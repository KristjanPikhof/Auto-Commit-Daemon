// message.go is the daemon-side adapter onto the Phase 5 ai package.
//
// Phase 1 owned a local rule-based generator; Phase 5 (this lane) moved
// the canonical implementation into internal/ai/deterministic.go so the
// replay path can swap providers without code churn here. This file is
// now a thin wrapper that:
//
//  1. translates the daemon's EventContext into ai.CommitContext;
//  2. invokes the ai.Provider's Generate;
//  3. composes the resulting Result.Subject + Result.Body into the
//     single-string message MessageFn returns.
//
// Output is **byte-identical** to the previous Phase 1 implementation:
// single-op events produce just the subject, multi-op events produce
// `subject + "\n\n" + bullets`. Existing replay tests pin the subject
// shape and continue to pass unchanged.
//
// Diff text reconstruction
// ------------------------
// Network-bound providers (openai-compat, plugin subprocess) want a
// unified diff describing the captured change so the model can produce a
// commit subject grounded in the actual delta. The diff is rebuilt from
// the per-op `before_oid` / `after_oid` blobs persisted at capture time
// (NOT from the live worktree, which may have moved on by the time the
// drain runs). Implementation choice: shell `git diff --no-color
// --no-ext-diff <before> <after>` per op and rewrite the synthetic
// `a/<oid>` `b/<oid>` paths with the captured path. This keeps us on the
// same git binary the rest of the daemon already drives — no second
// diff library, no bespoke text-diff implementation. For create/delete
// we substitute the well-known empty-blob OID
// (`e69de29bb2d1d6434b8b29ae775ad8c2e48c5391`) for the missing side.
// Diff egress is now governed solely by the selected provider: when
// ai.ProviderNeedsDiff(p) reports true (network-bound providers), the
// reconstructed diff is redacted and capped, then attached to
// CommitContext.DiffText. The deterministic provider declares
// NeedsDiff=false and therefore never sees DiffText. The legacy
// ACD_AI_SEND_DIFF env var is deprecated and ignored.
package daemon

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// emptyBlobOID is git's hard-coded SHA-1 of the empty blob. Used as the
// "missing side" OID when synthesising create/delete diffs so we can
// always pass two real OIDs to `git diff`.
const emptyBlobOID = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"

// DeterministicMessage produces a commit subject + optional body from the
// event + ops alone. Pure forwarder over ai.DeterministicProvider.
//
// The deterministic provider does not consult DiffText / RepoRoot, so we
// pass an empty repo root here — the daemon-side wiring (providerMessageFn)
// is what populates the diff for AI providers.
func DeterministicMessage(ctx context.Context, ec EventContext) (string, error) {
	return providerMessageFn(ai.DeterministicProvider{}, "")(ctx, ec)
}

// providerMessageFn adapts an ai.Provider into the daemon's MessageFn
// signature. Subject + Body are joined with a blank line so the run
// loop's commit-tree call gets a single string. Errors propagate so
// Compose'd fallback chains can surface their final outcome to the
// caller (which logs, marks the event failed, and continues).
//
// repoRoot is used to reconstruct the unified diff from captured blob
// OIDs; pass "" when the caller cannot supply a repo root (the
// deterministic provider tolerates an empty DiffText, and AI providers
// will simply receive an empty diff field).
func providerMessageFn(p ai.Provider, repoRoot string) MessageFn {
	return func(ctx context.Context, ec EventContext) (string, error) {
		effectiveRepoRoot := repoRoot
		if !ai.ProviderNeedsDiff(p) {
			effectiveRepoRoot = ""
		}
		cc := commitContextFromEvent(ctx, ec, effectiveRepoRoot)
		r, err := p.Generate(ctx, cc)
		if err != nil {
			return "", err
		}
		if r.Body == "" {
			return r.Subject, nil
		}
		return r.Subject + "\n\n" + r.Body, nil
	}
}

// commitContextFromEvent translates the daemon's EventContext into the
// ai package's CommitContext. Multi-op events are flattened into MultiOp;
// single-op events populate the top-level Path/Op/OldPath fields so the
// deterministic generator can take the single-op path.
//
// When repoRoot is non-empty and ACD_AI_SEND_DIFF is truthy, the captured
// before/after OIDs on each op are diffed via `git diff` and the resulting
// unified diff text is redacted, capped via ai.Truncate, and stitched into
// CommitContext.DiffText. Diff failures are
// swallowed: an empty DiffText still produces a working commit message
// (the deterministic fallback is unaffected).
func commitContextFromEvent(ctx context.Context, ec EventContext, repoRoot string) ai.CommitContext {
	cc := ai.CommitContext{
		Branch:   ec.Event.BranchRef,
		RepoRoot: repoRoot,
		Now:      time.Now(),
	}
	switch len(ec.Ops) {
	case 0:
		// no-op — Generator returns "Update files".
	case 1:
		op := ec.Ops[0]
		cc.Path = op.Path
		cc.Op = op.Op
		if op.OldPath.Valid {
			cc.OldPath = op.OldPath.String
		}
	default:
		cc.MultiOp = make([]ai.OpItem, 0, len(ec.Ops))
		for _, op := range ec.Ops {
			old := ""
			if op.OldPath.Valid {
				old = op.OldPath.String
			}
			cc.MultiOp = append(cc.MultiOp, ai.OpItem{
				Path:    op.Path,
				Op:      op.Op,
				OldPath: old,
			})
		}
	}
	if repoRoot != "" && len(ec.Ops) > 0 && aiSendDiffEnabled() {
		if diff, err := BuildOpsDiff(ctx, repoRoot, ec.Ops); err == nil && diff != "" {
			cc.DiffText = ai.Truncate(ai.RedactDiffSecrets(diff), ai.DiffCap)
		}
	}
	return cc
}

func aiSendDiffEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envAISendDiff))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// BuildOpsDiff reconstructs a unified diff for one event's ops by
// running `git diff` against the per-op `before_oid` / `after_oid`
// blobs. Each op contributes one diff section; sections are joined with
// a single newline. Soft errors per-op (missing blob, unknown op) are
// swallowed and the corresponding section is omitted; the function only
// returns an error when the underlying git binary is unreachable in a
// way that should surface up to the caller.
//
// The returned text is capped to ai.DiffCap while sections are appended,
// so large multi-op events stop rendering once the provider budget is
// consumed. Callers still apply redaction + ai.Truncate before handing the
// diff to a provider because redaction can change the final byte length.
func BuildOpsDiff(ctx context.Context, repoRoot string, ops []state.CaptureOp) (string, error) {
	if repoRoot == "" || len(ops) == 0 {
		return "", nil
	}
	var buf cappedDiffBuffer
	for _, op := range ops {
		if buf.Full() {
			break
		}
		section, err := buildOpDiff(ctx, repoRoot, op)
		if err != nil {
			// Soft per-op failure: skip this op but keep going so the
			// model still sees the other ops in a multi-op event.
			continue
		}
		if section == "" {
			continue
		}
		if buf.Len() > 0 && !buf.HasTrailingNewline() {
			buf.WriteString("\n")
		}
		buf.WriteString(section)
	}
	return buf.String(), nil
}

type cappedDiffBuffer struct {
	buf bytes.Buffer
}

func (b *cappedDiffBuffer) Len() int {
	return b.buf.Len()
}

func (b *cappedDiffBuffer) Full() bool {
	return b.buf.Len() >= ai.DiffCap
}

func (b *cappedDiffBuffer) HasTrailingNewline() bool {
	raw := b.buf.Bytes()
	return len(raw) > 0 && raw[len(raw)-1] == '\n'
}

func (b *cappedDiffBuffer) WriteString(s string) {
	remaining := ai.DiffCap - b.buf.Len()
	if remaining <= 0 {
		return
	}
	if len(s) > remaining {
		s = s[:remaining]
	}
	b.buf.WriteString(s)
}

func (b *cappedDiffBuffer) String() string {
	return b.buf.String()
}

// buildOpDiff produces a unified diff section for one captured op.
// Returns "" + nil when the op carries no usable OIDs (e.g. an oversize
// metadata-only op), or the textual diff with rewritten path headers
// for every other case.
func buildOpDiff(ctx context.Context, repoRoot string, op state.CaptureOp) (string, error) {
	path := op.Path
	oldPath := ""
	if op.OldPath.Valid {
		oldPath = op.OldPath.String
	}
	beforeOID := ""
	if op.BeforeOID.Valid {
		beforeOID = op.BeforeOID.String
	}
	afterOID := ""
	if op.AfterOID.Valid {
		afterOID = op.AfterOID.String
	}
	beforeMode := ""
	if op.BeforeMode.Valid {
		beforeMode = op.BeforeMode.String
	}
	afterMode := ""
	if op.AfterMode.Valid {
		afterMode = op.AfterMode.String
	}

	switch op.Op {
	case "create":
		return renderDiff(ctx, repoRoot, diffSpec{
			oldPath: path, newPath: path,
			beforeOID: emptyBlobOID, afterOID: afterOID,
			newFileMode: afterMode,
		})
	case "delete":
		return renderDiff(ctx, repoRoot, diffSpec{
			oldPath: path, newPath: path,
			beforeOID: beforeOID, afterOID: emptyBlobOID,
			deletedFileMode: beforeMode,
		})
	case "modify":
		return renderDiff(ctx, repoRoot, diffSpec{
			oldPath: path, newPath: path,
			beforeOID: beforeOID, afterOID: afterOID,
			oldMode: beforeMode, newMode: afterMode,
		})
	case "rename":
		from := oldPath
		if from == "" {
			from = path
		}
		return renderDiff(ctx, repoRoot, diffSpec{
			oldPath: from, newPath: path,
			beforeOID: beforeOID, afterOID: afterOID,
			renameFrom: from, renameTo: path,
			oldMode: beforeMode, newMode: afterMode,
		})
	case "mode":
		return renderDiff(ctx, repoRoot, diffSpec{
			oldPath: path, newPath: path,
			beforeOID: beforeOID, afterOID: afterOID,
			oldMode: beforeMode, newMode: afterMode,
			modeOnly: beforeOID == afterOID,
		})
	default:
		return "", nil
	}
}

// diffSpec carries the rendering knobs for one op's diff section.
type diffSpec struct {
	oldPath, newPath     string
	beforeOID, afterOID  string
	oldMode, newMode     string
	newFileMode          string // for "new file mode" header on create
	deletedFileMode      string // for "deleted file mode" header on delete
	renameFrom, renameTo string
	modeOnly             bool
}

// renderDiff stitches the op's header lines together with the body diff
// produced by `git diff <before> <after>`. The body's auto-generated
// `diff --git a/<oid>` / `--- a/<oid>` / `+++ b/<oid>` lines are
// stripped and replaced by header lines that reflect the captured path
// + mode. Anything past the first hunk header (`@@`) is forwarded
// verbatim so binary-file markers or "No newline" trailers survive.
func renderDiff(ctx context.Context, repoRoot string, s diffSpec) (string, error) {
	if err := ensureEmptyBlob(ctx, repoRoot, s); err != nil {
		return "", err
	}

	var hdr strings.Builder
	fmt.Fprintf(&hdr, "diff --git a/%s b/%s\n", s.oldPath, s.newPath)
	if s.newFileMode != "" {
		fmt.Fprintf(&hdr, "new file mode %s\n", s.newFileMode)
	}
	if s.deletedFileMode != "" {
		fmt.Fprintf(&hdr, "deleted file mode %s\n", s.deletedFileMode)
	}
	if s.renameFrom != "" && s.renameTo != "" && s.renameFrom != s.renameTo {
		fmt.Fprintf(&hdr, "rename from %s\nrename to %s\n", s.renameFrom, s.renameTo)
	}
	if s.oldMode != "" && s.newMode != "" && s.oldMode != s.newMode &&
		s.newFileMode == "" && s.deletedFileMode == "" {
		fmt.Fprintf(&hdr, "old mode %s\nnew mode %s\n", s.oldMode, s.newMode)
	}
	if s.modeOnly {
		// Pure mode change carries no content delta — header alone is
		// the entire section.
		return hdr.String(), nil
	}

	if s.beforeOID == "" || s.afterOID == "" {
		// Defensive: missing OID. Header alone is still informative.
		return hdr.String(), nil
	}

	body, err := git.DiffBlobs(ctx, repoRoot, s.beforeOID, s.afterOID)
	if err != nil {
		// Best-effort: when git refuses (missing blob, foreign archive),
		// fall back to header-only so the model still sees the change
		// shape.
		return hdr.String(), nil
	}
	body = stripGitDiffPreamble(body)

	if body == "" {
		return hdr.String(), nil
	}
	// Reattach our own `--- a/<path>` / `+++ b/<path>` lines so the
	// hunk(s) are anchored to a real path. `git diff` between two blob
	// OIDs emits these but with the OIDs in place of paths; we strip
	// those and supply the captured ones.
	fmt.Fprintf(&hdr, "--- a/%s\n+++ b/%s\n", s.oldPath, s.newPath)
	hdr.WriteString(body)
	if !strings.HasSuffix(body, "\n") {
		hdr.WriteByte('\n')
	}
	return hdr.String(), nil
}

// stripGitDiffPreamble drops git's auto-generated header (`diff --git`,
// `index`, `---`, `+++` lines) up to but not including the first hunk
// (`@@`) or "Binary files" marker. Returns the remainder.
func stripGitDiffPreamble(body string) string {
	// Common case: the body starts with `diff --git a/<oid> b/<oid>`.
	// Walk lines, dropping any that look like the synthetic header
	// emitted by `git diff <oidA> <oidB>`. Stop at the first `@@` or
	// `Binary files` line.
	lines := strings.SplitAfter(body, "\n")
	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "@@ "),
			strings.HasPrefix(line, "Binary files "):
			return strings.Join(lines[i:], "")
		}
	}
	// No hunks found — nothing useful to forward.
	return ""
}

// ensureEmptyBlob makes sure git's empty-blob OID exists in the object
// store before we ask `git diff` to read it. The first
// `git hash-object -w --stdin < /dev/null` is idempotent and cheap;
// subsequent calls are no-ops.
func ensureEmptyBlob(ctx context.Context, repoRoot string, s diffSpec) error {
	if s.beforeOID != emptyBlobOID && s.afterOID != emptyBlobOID {
		return nil
	}
	_, err := git.HashObjectStdin(ctx, repoRoot, nil)
	return err
}

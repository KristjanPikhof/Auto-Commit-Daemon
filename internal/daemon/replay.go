// replay.go drains pending capture_events into per-event commits per §8.3.
//
// Atomic-per-file: every event becomes ONE commit. Coalescing multi-file
// events into a single commit is OFF by default in v1 — even when an event
// happens to carry multiple ops, a single commit is produced via the legacy
// update-index --index-info path.
//
// AI commit messages land in Phase 5 (internal/ai). Phase 1 ships a
// deterministic message helper in this package; the run loop wires it via
// the MessageFn hook so Phase 5 can swap the implementation without
// touching the replay state machine.
package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// MessageFn produces a commit message for one event + its ops. Phase 1
// callers pass DeterministicMessage; Phase 5 swaps in an AI-backed
// implementation.
type MessageFn func(ctx context.Context, ec EventContext) (string, error)

// EventContext is the input handed to MessageFn. Mirrors the fields the
// legacy daemon passes to its message generator (event row + ops).
type EventContext struct {
	Event state.CaptureEvent
	Ops   []state.CaptureOp
}

// ReplayOpts configures one replay pass.
type ReplayOpts struct {
	// MessageFn produces the commit message. Nil falls back to
	// DeterministicMessage.
	MessageFn MessageFn

	// IndexFile is the GIT_INDEX_FILE path used for an isolated index. When
	// empty, defaults to <gitDir>/acd/replay.index. Caller-provided value
	// lets tests put it on a temp path.
	IndexFile string

	// GitDir is the absolute git dir for the worktree. Required to seed a
	// default IndexFile.
	GitDir string

	// Limit caps the number of events drained per call. 0 = no limit.
	Limit int
}

// ReplaySummary describes one drain.
type ReplaySummary struct {
	Published int // events that produced a new commit
	Conflicts int // events deferred via publish_state.status=conflict
	Failed    int // events marked failed (validation/commit errors)
}

// Replay drains pending capture_events for the active branch into commits.
//
// One pass per call: the run loop is expected to invoke this on every
// poll-tick. Coalescing OFF — each event becomes its own commit, with the
// previous event's commit as the new HEAD's parent.
//
// Conflict semantics: when the live index for any path touched by an event
// disagrees with the event's before-state, the event is left pending and
// publish_state.status is updated to "conflict". The daemon surfaces the
// conflict via daemon_meta.last_replay_conflict; resolution is the
// operator's job (out of scope for v1 automation).
func Replay(ctx context.Context, repoRoot string, db *state.DB, cctx CaptureContext, opts ReplayOpts) (ReplaySummary, error) {
	var sum ReplaySummary
	if repoRoot == "" || db == nil {
		return sum, fmt.Errorf("daemon: Replay: repoRoot and db required")
	}

	msgFn := opts.MessageFn
	if msgFn == nil {
		msgFn = DeterministicMessage
	}

	indexFile := opts.IndexFile
	if indexFile == "" {
		if opts.GitDir == "" {
			return sum, fmt.Errorf("daemon: Replay: IndexFile or GitDir required")
		}
		indexFile = filepath.Join(opts.GitDir, "acd", "replay.index")
	}
	if err := os.MkdirAll(filepath.Dir(indexFile), 0o700); err != nil {
		return sum, fmt.Errorf("daemon: replay: mkdir index parent: %w", err)
	}
	// Always start from a clean index: stale entries from a prior crashed
	// run would otherwise poison write-tree.
	_ = os.Remove(indexFile)

	pending, err := state.PendingEvents(ctx, db, opts.Limit)
	if err != nil {
		return sum, fmt.Errorf("daemon: load pending: %w", err)
	}
	if len(pending) == 0 {
		return sum, nil
	}

	// Seed the isolated index from the active HEAD so conflicts are checked
	// against the canonical baseline (not whatever stale index the worker
	// last left behind).
	if err := git.ReadTree(ctx, repoRoot, indexFile, cctx.BaseHead); err != nil {
		return sum, fmt.Errorf("daemon: replay seed read-tree: %w", err)
	}

	parent := cctx.BaseHead

	for _, ev := range pending {
		if err := ctx.Err(); err != nil {
			return sum, err
		}
		ops, err := state.LoadCaptureOps(ctx, db, ev.Seq)
		if err != nil {
			return sum, fmt.Errorf("daemon: load ops seq=%d: %w", ev.Seq, err)
		}
		if len(ops) == 0 {
			// No ops to apply — mark failed, do not block the queue.
			markFailed(ctx, db, ev, "no ops attached")
			sum.Failed++
			continue
		}

		// Validate before touching the index.
		if msg := validateOps(ops); msg != "" {
			markFailed(ctx, db, ev, msg)
			sum.Failed++
			continue
		}

		// Conflict probe: compare the per-replay scratch index (seeded
		// from BaseHead and advanced by every prior queued event) against
		// each op's before-state. The repo's live index is intentionally
		// NOT inspected — a busy worktree that has moved ahead of the
		// queue would otherwise spuriously reject valid sequenced events
		// (e.g. an A→B→C→D modify chain whose disk state already shows D).
		//
		// Mirrors snapshot-replay._verify_op against the in-memory state
		// dict seeded from snapshot_state_for_index over the GIT_INDEX_FILE
		// scratch index.
		if reason, err := detectConflict(ctx, repoRoot, indexFile, ops); err != nil {
			return sum, err
		} else if reason != "" {
			recordConflict(ctx, db, ev, reason, cctx)
			sum.Conflicts++
			continue
		}

		// Apply ops to the isolated index, write a tree, commit, advance HEAD.
		commitOID, err := commitOneEvent(ctx, repoRoot, indexFile, parent, ev, ops, msgFn)
		if err != nil {
			markFailed(ctx, db, ev, err.Error())
			sum.Failed++
			// Reset the in-memory index from `parent` so a partial apply
			// does not poison subsequent events.
			_ = git.ReadTree(ctx, repoRoot, indexFile, parent)
			continue
		}

		// Advance the branch ref via CAS against the prior parent.
		oldOID := parent
		if cctx.BaseHead == "" {
			// Initial commit case (no prior parent) -> non-CAS update.
			oldOID = ""
		}
		if err := git.UpdateRef(ctx, repoRoot, cctx.BranchRef, commitOID, oldOID); err != nil {
			// CAS failed: ref moved out from under us. Mark conflict.
			recordConflict(ctx, db, ev, "update-ref CAS failed: "+err.Error(), cctx)
			sum.Conflicts++
			_ = git.ReadTree(ctx, repoRoot, indexFile, parent)
			continue
		}

		// Settle the event row + publish_state.
		nowSec := float64(time.Now().UnixNano()) / 1e9
		if err := state.MarkEventPublished(ctx, db,
			ev.Seq, "published",
			sql.NullString{String: commitOID, Valid: true},
			sql.NullString{},
			ev.Message, nowSec,
		); err != nil {
			return sum, fmt.Errorf("daemon: mark published: %w", err)
		}
		_ = state.SavePublishState(ctx, db, state.Publish{
			EventSeq:         sql.NullInt64{Int64: ev.Seq, Valid: true},
			BranchRef:        sql.NullString{String: cctx.BranchRef, Valid: true},
			BranchGeneration: sql.NullInt64{Int64: cctx.BranchGeneration, Valid: true},
			SourceHead:       sql.NullString{String: parent, Valid: true},
			TargetCommitOID:  sql.NullString{String: commitOID, Valid: true},
			Status:           "published",
		})

		parent = commitOID
		sum.Published++
	}

	return sum, nil
}

// validateOps mirrors snapshot-replay._validate_op: every op kind must
// supply the right combination of after_oid/after_mode/before_*/old_path.
// Returns the empty string on success.
func validateOps(ops []state.CaptureOp) string {
	for _, op := range ops {
		if op.Path == "" {
			return fmt.Sprintf("missing path for op %q", op.Op)
		}
		switch op.Op {
		case "create", "modify", "mode", "rename":
			if !op.AfterOID.Valid || op.AfterOID.String == "" {
				return fmt.Sprintf("missing after_oid for %s %s", op.Op, op.Path)
			}
			if !op.AfterMode.Valid || op.AfterMode.String == "" {
				return fmt.Sprintf("missing after_mode for %s %s", op.Op, op.Path)
			}
		}
		switch op.Op {
		case "modify", "mode", "delete":
			if !op.BeforeOID.Valid || op.BeforeOID.String == "" {
				return fmt.Sprintf("missing before_oid for %s %s", op.Op, op.Path)
			}
			if !op.BeforeMode.Valid || op.BeforeMode.String == "" {
				return fmt.Sprintf("missing before_mode for %s %s", op.Op, op.Path)
			}
		case "rename":
			if !op.OldPath.Valid || op.OldPath.String == "" {
				return fmt.Sprintf("missing old_path for rename %s", op.Path)
			}
		}
	}
	return ""
}

// detectConflict checks the live staged index for every path touched by ops
// and flags a conflict when the live state disagrees with the op's
// before-state. Mirrors the legacy _verify_op against
// snapshot_state_for_index. Returns ("", nil) on success.
func detectConflict(ctx context.Context, repoRoot string, ops []state.CaptureOp) (string, error) {
	paths := touchedPaths(ops)
	if len(paths) == 0 {
		return "", nil
	}
	live, err := git.LsFilesStaged(ctx, repoRoot, paths...)
	if err != nil {
		return "", fmt.Errorf("ls-files staged: %w", err)
	}
	type entry struct {
		mode, oid string
	}
	idx := map[string]entry{}
	for _, le := range live {
		idx[le.Path] = entry{mode: le.Mode, oid: le.OID}
	}
	for _, op := range ops {
		switch op.Op {
		case "create":
			if e, ok := idx[op.Path]; ok {
				// "create on existing path" is fine only when the existing
				// entry already matches the after-state (idempotent replay).
				if e.mode != op.AfterMode.String || e.oid != op.AfterOID.String {
					return fmt.Sprintf("create conflict for %s", op.Path), nil
				}
			}
		case "modify", "mode":
			e, ok := idx[op.Path]
			if !ok {
				return fmt.Sprintf("%s missing-in-index for %s", op.Op, op.Path), nil
			}
			if e.mode != op.BeforeMode.String || e.oid != op.BeforeOID.String {
				return fmt.Sprintf("%s before-state mismatch for %s", op.Op, op.Path), nil
			}
		case "delete":
			e, ok := idx[op.Path]
			if !ok {
				return fmt.Sprintf("delete missing-in-index for %s", op.Path), nil
			}
			if e.mode != op.BeforeMode.String || e.oid != op.BeforeOID.String {
				return fmt.Sprintf("delete before-state mismatch for %s", op.Path), nil
			}
		case "rename":
			old := op.OldPath.String
			e, ok := idx[old]
			if !ok {
				return fmt.Sprintf("rename source missing-in-index for %s", old), nil
			}
			if e.mode != op.BeforeMode.String || e.oid != op.BeforeOID.String {
				return fmt.Sprintf("rename source mismatch for %s", old), nil
			}
			if _, exists := idx[op.Path]; exists {
				return fmt.Sprintf("rename target already exists for %s", op.Path), nil
			}
		}
	}
	return "", nil
}

// touchedPaths is the set of paths that an event's ops will read or write.
func touchedPaths(ops []state.CaptureOp) []string {
	seen := map[string]struct{}{}
	for _, op := range ops {
		if op.Path != "" {
			seen[op.Path] = struct{}{}
		}
		if op.OldPath.Valid && op.OldPath.String != "" {
			seen[op.OldPath.String] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// commitOneEvent applies ops via update-index --index-info on the isolated
// index, runs write-tree, composes the commit message, and runs commit-tree.
// Returns the new commit OID; the caller is responsible for update-ref.
func commitOneEvent(ctx context.Context, repoRoot, indexFile, parent string, ev state.CaptureEvent, ops []state.CaptureOp, msgFn MessageFn) (string, error) {
	// Build update-index payload. Mirrors snapshot_state.apply_ops_to_index.
	const zeroOID = "0000000000000000000000000000000000000000"
	var lines []string
	for _, op := range ops {
		switch op.Op {
		case "create", "modify", "mode":
			lines = append(lines, fmt.Sprintf("%s %s\t%s",
				op.AfterMode.String, op.AfterOID.String, op.Path))
		case "delete":
			lines = append(lines, fmt.Sprintf("0 %s\t%s", zeroOID, op.Path))
		case "rename":
			if op.OldPath.Valid && op.OldPath.String != "" {
				lines = append(lines, fmt.Sprintf("0 %s\t%s", zeroOID, op.OldPath.String))
			}
			lines = append(lines, fmt.Sprintf("%s %s\t%s",
				op.AfterMode.String, op.AfterOID.String, op.Path))
		default:
			return "", fmt.Errorf("unknown op %q", op.Op)
		}
	}
	if err := git.UpdateIndexInfo(ctx, repoRoot, indexFile, lines); err != nil {
		return "", fmt.Errorf("update-index: %w", err)
	}
	tree, err := git.WriteTree(ctx, repoRoot, indexFile)
	if err != nil {
		return "", fmt.Errorf("write-tree: %w", err)
	}

	msg, err := msgFn(ctx, EventContext{Event: ev, Ops: ops})
	if err != nil {
		return "", fmt.Errorf("message: %w", err)
	}
	if strings.TrimSpace(msg) == "" {
		// Defensive fallback so the commit never lands with an empty subject.
		msg = "Update files"
	}

	var parents []string
	if parent != "" {
		parents = []string{parent}
	}
	commitOID, err := git.CommitTree(ctx, repoRoot, tree, msg, parents...)
	if err != nil {
		return "", fmt.Errorf("commit-tree: %w", err)
	}
	return commitOID, nil
}

// markFailed flags an event as terminally failed and records the reason.
// Best-effort: persistence failures here do not propagate, the next pass
// will see the event still pending and re-attempt (with the same outcome).
func markFailed(ctx context.Context, db *state.DB, ev state.CaptureEvent, reason string) {
	nowSec := float64(time.Now().UnixNano()) / 1e9
	_ = state.MarkEventPublished(ctx, db,
		ev.Seq, "failed",
		sql.NullString{}, sql.NullString{String: reason, Valid: true},
		ev.Message, nowSec)
}

// recordConflict updates publish_state to flag the conflict but leaves the
// event row in `pending` so the next pass can retry once the operator has
// reconciled the index. Mirrors the legacy "blocked_conflict" surface but
// uses the simpler `conflict` status the v1 schema documents.
func recordConflict(ctx context.Context, db *state.DB, ev state.CaptureEvent, reason string, cctx CaptureContext) {
	_ = state.SavePublishState(ctx, db, state.Publish{
		EventSeq:         sql.NullInt64{Int64: ev.Seq, Valid: true},
		BranchRef:        sql.NullString{String: cctx.BranchRef, Valid: true},
		BranchGeneration: sql.NullInt64{Int64: cctx.BranchGeneration, Valid: true},
		SourceHead:       sql.NullString{String: cctx.BaseHead, Valid: true},
		Status:           "conflict",
		Error:            sql.NullString{String: reason, Valid: true},
	})
	_ = state.MetaSet(ctx, db, "last_replay_conflict",
		fmt.Sprintf("seq=%d: %s", ev.Seq, reason))
}

// errReplay is sentinel for fatal replay errors that should halt the pass.
// Non-fatal per-event problems are recorded against the event row and the
// pass continues.
var errReplay = errors.New("replay") //nolint:unused

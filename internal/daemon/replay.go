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
	// empty, Replay creates a per-pass tempfile under <gitDir>/acd and
	// removes it before returning. Caller-provided values are left in place
	// for tests that need to inspect the index.
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
	Conflicts int // events terminally settled in state.EventStateBlockedConflict
	Failed    int // events marked failed (validation/commit errors)
	BaseHead  string
}

// Replay drains pending capture_events for the active branch into commits.
//
// One pass per call: the run loop is expected to invoke this on every
// poll-tick. Coalescing OFF — each event becomes its own commit, with the
// previous event's commit as the new HEAD's parent.
//
// Conflict semantics: when the scratch replay index for any path touched by
// an event disagrees with the event's before-state, OR the branch ref CAS
// fails on update-ref, the event is settled in state.EventStateBlockedConflict
// (terminal — never retried automatically) and publish_state.status is set
// to "blocked_conflict". The daemon also stamps daemon_meta.last_replay_conflict
// so operators can spot a divergence at a glance. Resolution is the
// operator's job (out of scope for v1 automation).
//
// Batch halt: a conflict or commit-build failure short-circuits the rest of
// the pending queue. Subsequent events were captured assuming the broken
// predecessor would land first; replaying them on top of a stale parent
// would produce a tree that diverges from the operator's intent. The next
// poll tick sees those events still pending and re-attempts them only after
// the operator has reconciled the blocker (which advances BaseHead /
// branch_generation and lets the queue drain naturally).
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
	cleanupIndex := func() {}
	if indexFile == "" {
		if opts.GitDir == "" {
			return sum, fmt.Errorf("daemon: Replay: IndexFile or GitDir required")
		}
		indexDir := filepath.Join(opts.GitDir, "acd")
		if err := os.MkdirAll(indexDir, 0o700); err != nil {
			return sum, fmt.Errorf("daemon: replay: mkdir index parent: %w", err)
		}
		tmp, err := os.CreateTemp(indexDir, "replay-*.index")
		if err != nil {
			return sum, fmt.Errorf("daemon: replay: create temp index: %w", err)
		}
		indexFile = tmp.Name()
		if err := tmp.Close(); err != nil {
			_ = os.Remove(indexFile)
			return sum, fmt.Errorf("daemon: replay: close temp index: %w", err)
		}
		cleanupIndex = func() { _ = os.Remove(indexFile) }
		defer cleanupIndex()
	} else if err := os.MkdirAll(filepath.Dir(indexFile), 0o700); err != nil {
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
	activeCtx := cctx
	sum.BaseHead = parent

	for _, ev := range pending {
		if err := ctx.Err(); err != nil {
			return sum, err
		}

		// Branch-generation / ancestry guard. An event whose generation
		// no longer matches the active context was captured under a
		// branch state that has since been rewritten (rebase, reset,
		// branch switch). An event whose BaseHead is not reachable from
		// the current replay parent was captured against a HEAD that no
		// longer descends to the live worktree. Either case must NOT
		// silently replay — the resulting commit would chain off a stale
		// parent and diverge from the operator's intent. Block
		// terminally so operators can spot the mismatch and reconcile.
		if reason, err := checkEventGeneration(ctx, repoRoot, parent, ev, cctx); err != nil {
			return sum, err
		} else if reason != "" {
			recordConflict(ctx, db, ev, reason, activeCtx)
			sum.Conflicts++
			return sum, nil
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
			recordConflict(ctx, db, ev, reason, activeCtx)
			sum.Conflicts++
			// Halt the batch: subsequent events were captured assuming
			// this one would land first. Running them now would replay on
			// top of a broken predecessor.
			return sum, nil
		}

		// Apply ops to the isolated index, write a tree, commit, advance HEAD.
		commitOID, err := commitOneEvent(ctx, repoRoot, indexFile, parent, ev, ops, msgFn)
		if err != nil {
			markFailed(ctx, db, ev, err.Error())
			sum.Failed++
			// Halt the batch: a commit-build failure leaves `parent`
			// pointing at the prior commit, but later events will still
			// chain from a broken predecessor as soon as the operator
			// fixes the root cause. Stop here and let the next poll tick
			// re-attempt from a fresh seed.
			_ = git.ReadTree(ctx, repoRoot, indexFile, parent)
			return sum, nil
		}

		// Advance the branch ref via CAS against the prior parent.
		oldOID := parent
		if cctx.BaseHead == "" {
			// Initial commit case (no prior parent) -> non-CAS update.
			oldOID = ""
		}
		if err := git.UpdateRef(ctx, repoRoot, cctx.BranchRef, commitOID, oldOID); err != nil {
			// CAS failed: ref moved out from under us. Block terminally —
			// every queued event downstream was captured against the
			// stale ref and must wait for branch reconciliation.
			recordConflict(ctx, db, ev, "update-ref CAS failed: "+err.Error(), activeCtx)
			sum.Conflicts++
			_ = git.ReadTree(ctx, repoRoot, indexFile, parent)
			return sum, nil
		}

		// Settle the event row + publish_state.
		nowSec := float64(time.Now().UnixNano()) / 1e9
		if err := state.MarkEventPublished(ctx, db,
			ev.Seq, state.EventStatePublished,
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
		activeCtx.BaseHead = commitOID
		sum.BaseHead = commitOID
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

// detectConflict checks the scratch replay index for every path touched by
// ops and flags a conflict when the indexed state disagrees with the op's
// before-state. Mirrors the legacy _verify_op against the in-memory state
// dict seeded from snapshot_state_for_index over the GIT_INDEX_FILE scratch
// index. Returns ("", nil) on success.
//
// indexFile must be the per-replay scratch index (the same path passed to
// UpdateIndexInfo + WriteTree below); empty falls back to the live repo
// index but the run loop never relies on that — see the comment in Replay.
func detectConflict(ctx context.Context, repoRoot, indexFile string, ops []state.CaptureOp) (string, error) {
	paths := touchedPaths(ops)
	if len(paths) == 0 {
		return "", nil
	}
	staged, err := git.LsFilesIndex(ctx, repoRoot, indexFile, paths...)
	if err != nil {
		return "", fmt.Errorf("ls-files staged: %w", err)
	}
	type entry struct {
		mode, oid string
	}
	idx := map[string]entry{}
	for _, le := range staged {
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
// "failed" is terminal — PendingEvents excludes the row, so the next pass
// will not re-attempt it. Best-effort: persistence failures here do not
// propagate.
func markFailed(ctx context.Context, db *state.DB, ev state.CaptureEvent, reason string) {
	nowSec := float64(time.Now().UnixNano()) / 1e9
	_ = state.MarkEventPublished(ctx, db,
		ev.Seq, state.EventStateFailed,
		sql.NullString{}, sql.NullString{String: reason, Valid: true},
		ev.Message, nowSec)
}

// recordConflict terminally settles the event in
// state.EventStateBlockedConflict and synchronously upserts the singleton
// publish_state row to status="blocked_conflict" — both writes happen in one
// transaction via state.MarkEventBlocked so a status reader never observes a
// stale half-update.
//
// The event row leaves `pending` permanently. PendingEvents will skip it on
// every subsequent poll, so a stuck event no longer blocks the queue with
// retry churn. Operators see the row via `acd status` (blocked_conflicts
// count) and via daemon_meta.last_replay_conflict for the human message.
func recordConflict(ctx context.Context, db *state.DB, ev state.CaptureEvent, reason string, cctx CaptureContext) {
	nowSec := float64(time.Now().UnixNano()) / 1e9
	_ = state.MarkEventBlocked(ctx, db, ev.Seq, reason, nowSec,
		sql.NullString{String: cctx.BranchRef, Valid: true},
		sql.NullInt64{Int64: cctx.BranchGeneration, Valid: true},
		sql.NullString{String: cctx.BaseHead, Valid: true},
	)
	_ = state.MetaSet(ctx, db, "last_replay_conflict",
		fmt.Sprintf("seq=%d: %s", ev.Seq, reason))
}

// checkEventGeneration is the §8.9 stale-event guard. Returns a non-empty
// human-readable reason when the queued event must not be replayed against
// the current branch generation, or ("", nil) when the event is safe to
// publish.
//
// Two failure modes are distinguished in the returned reason so operators
// can tell why the queue stalled:
//
//  1. Generation mismatch: ev.BranchGeneration != cctx.BranchGeneration.
//     The branch token transitioned through a divergence (rebase, reset,
//     branch switch) since the event was captured. Replaying it would
//     resurrect work that the operator already rewrote.
//  2. Ancestry mismatch: ev.BaseHead is not an ancestor of the current
//     replay parent. Even if generations match (e.g. a daemon restart
//     missed the bump), the captured baseline is no longer reachable and
//     the resulting commit would chain off a stale parent.
//
// A branch_ref mismatch is also flagged — replaying an event captured for
// branch X onto branch Y would silently land it on the wrong ref.
//
// repoRoot is required for the merge-base ancestry probe. parent is the
// current replay HEAD (== cctx.BaseHead at the start of a pass, advancing
// per published commit). When parent or ev.BaseHead is empty we skip the
// ancestry probe — orphan repos and the very-first commit have no history
// to compare against.
func checkEventGeneration(ctx context.Context, repoRoot, parent string, ev state.CaptureEvent, cctx CaptureContext) (string, error) {
	if cctx.BranchRef != "" && ev.BranchRef != "" && ev.BranchRef != cctx.BranchRef {
		return fmt.Sprintf(
			"branch ref mismatch: event captured on %s but daemon is on %s",
			ev.BranchRef, cctx.BranchRef), nil
	}
	if ev.BranchGeneration != 0 && ev.BranchGeneration != cctx.BranchGeneration {
		return fmt.Sprintf(
			"branch generation mismatch: event captured at generation %d but daemon is at %d (branch was reset/rebased/switched since capture)",
			ev.BranchGeneration, cctx.BranchGeneration), nil
	}
	// Ancestry probe — even when generations match (e.g. daemon restart
	// missed the bump) we must refuse to chain off a parent that is no
	// longer reachable from HEAD. Both sides must be present for a
	// meaningful merge-base call.
	if parent == "" || ev.BaseHead == "" {
		return "", nil
	}
	if ev.BaseHead == parent {
		return "", nil
	}
	ok, err := git.MergeBaseIsAncestor(ctx, repoRoot, ev.BaseHead, parent)
	if err != nil {
		// merge-base failed — most often because ev.BaseHead is no
		// longer in the object database (gc'd reset). Treat as a
		// terminal block so the operator notices.
		return fmt.Sprintf(
			"ancestry probe failed for base %s: %v (branch likely rewritten since capture)",
			ev.BaseHead, err), nil
	}
	if !ok {
		return fmt.Sprintf(
			"event base %s is not an ancestor of replay head %s (branch was reset/rebased since capture)",
			ev.BaseHead, parent), nil
	}
	return "", nil
}

// errReplay is sentinel for fatal replay errors that should halt the pass.
// Non-fatal per-event problems are recorded against the event row and the
// pass continues.
var errReplay = errors.New("replay") //nolint:unused

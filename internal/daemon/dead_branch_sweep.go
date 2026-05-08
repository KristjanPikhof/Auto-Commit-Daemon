// dead_branch_sweep.go houses the helpers that prune unpublished
// capture_events rows (pending + blocked_conflict + failed) whose owning
// branch ref no longer resolves. Two callers live here:
//
//   - the runtime Diverged transition path (daemon.go) calls
//     pruneDeadBranchTerminals after the prior generation's pending rows
//     have already been swept by DeletePendingForGeneration. When the prior
//     branch ref is gone, its terminal rows (and any pending rows that
//     escaped the generation-only sweep, e.g. captured under a different
//     active generation) would otherwise accumulate forever — `acd status`
//     and the PendingEvents barrier path would surface phantom blocked
//     counts for a branch the operator has long since deleted.
//   - daemon Run init schedules runStartupDeadBranchSweep on a goroutine
//     after the running-mode publish so a daemon restart that discovers
//     pre-existing dead-branch rows cleans them up off the blocking startup
//     path (the sweep can shell out to git for-each-ref and walk the entire
//     terminal-pair set, neither of which we want on the start-latency
//     budget).
//
// Both paths honor EnvKeepDeadBranchBarriers as an operator opt-out — set it
// truthy when you want to keep the rows around for forensic inspection.
//
// Pending + terminal must drop together for the dead-branch case. Leaving
// pending rows behind while deleting their terminal predecessor lets
// PendingEvents re-expose them on the next replay pass; replay then
// re-evaluates them against the prior (now-irrelevant) generation,
// mismatches in checkEventGeneration, and stamps a fresh blocked_conflict.
// The state-layer helper PurgeUnpublishedForDeadBranch enforces this in
// one transaction.
package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

// EnvKeepDeadBranchBarriers, when truthy ("1"/"true"/"yes"/"on", case-insensitive),
// disables both the runtime Diverged-hook prune and the daemon-startup sweep.
// Operators set it when they want to inspect blocked_conflict / failed rows for
// branches that have since been deleted.
const EnvKeepDeadBranchBarriers = "ACD_KEEP_DEAD_BRANCH_BARRIERS"

// deadBranchSweepRefsCap caps how many distinct refs the startup sweep records
// in the trace event's `refs` field. The trace writer is best-effort and the
// payload is for operator forensics — bounding the slice keeps the JSONL row
// reasonable on a long-lived repo with many stale branches.
const deadBranchSweepRefsCap = 32

// MetaKeyDeadBranchPruneLastRunTS records the wall-clock unix-seconds of the
// most recent dead-branch prune action that actually deleted rows (in either
// the runtime Diverged-hook path or the startup sweep). Operators read this
// via `acd diagnose --json` to reason about whether stale-branch hygiene is
// keeping pace. No-op sweeps (zero rows pruned) do NOT update this key — the
// surface is intentionally "last action that did something" so a long quiet
// period after an operator deletes a branch remains visible.
const MetaKeyDeadBranchPruneLastRunTS = "dead_branch_prune.last_run_ts"

// MetaKeyDeadBranchPruneLastCount records the total number of capture_events
// rows pruned by the most recent non-empty dead-branch prune action. Stored as
// a base-10 string. Reset to the new total on every non-empty prune; not
// cumulative.
const MetaKeyDeadBranchPruneLastCount = "dead_branch_prune.last_count"

// MetaKeyDeadBranchPruneLastRefs is a JSON-encoded []string of the branch refs
// whose terminals were pruned in the most recent non-empty dead-branch prune
// action. The slice is bounded by deadBranchSweepRefsCap so a sweep across
// many stale branches does not balloon the meta payload.
const MetaKeyDeadBranchPruneLastRefs = "dead_branch_prune.last_refs"

// recordDeadBranchPruneMeta stamps the three dead-branch prune meta keys in a
// single MetaSetMany transaction so `acd diagnose` reads them atomically.
// Best-effort: any error is logged at warn level and the caller continues. The
// meta surface is forensic — never block prune progress on a meta write.
//
// rows must be > 0 (the caller guards on the no-op case so empty sweeps do not
// overwrite the previous "last action that did something" snapshot). refs is
// the already-capped slice of pruned refs.
func recordDeadBranchPruneMeta(ctx context.Context, db *state.DB, logger *slog.Logger, rows int, refs []string) {
	refsJSON, err := json.Marshal(refs)
	if err != nil {
		// Marshalling a []string cannot fail under normal conditions, but
		// log defensively and keep going — the count + ts are still useful.
		logger.Warn("dead-branch prune: marshal refs failed; recording empty refs",
			"err", err.Error())
		refsJSON = []byte("[]")
	}
	pairs := map[string]string{
		MetaKeyDeadBranchPruneLastRunTS: strconv.FormatInt(time.Now().Unix(), 10),
		MetaKeyDeadBranchPruneLastCount: strconv.Itoa(rows),
		MetaKeyDeadBranchPruneLastRefs:  string(refsJSON),
	}
	if err := state.MetaSetMany(ctx, db, pairs); err != nil {
		logger.Warn("dead-branch prune: stamp meta keys failed",
			"err", err.Error(),
			"rows", rows,
			"refs", len(refs))
	}
}

// isKeepDeadBranchBarriers reports whether the operator opted out of dead-branch
// terminal pruning via EnvKeepDeadBranchBarriers. Empty / falsy / unset -> false
// (default ON). Truthy -> true. Recognized truthy values (case-insensitive):
// "1", "true", "yes", "on".
func isKeepDeadBranchBarriers() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvKeepDeadBranchBarriers))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// deadBranchPair pairs a branch ref with its observed branch_generation so the
// sweep can emit a per-pair prune log line and skip the active pair.
type deadBranchPair struct {
	Ref        string
	Generation int64
}

// distinctTerminalBranchPairs reads the (branch_ref, branch_generation) pairs of
// terminal capture_events rows via the read-only handle. Routed through ReadSQL
// so a long-running replay drain holding the serialized writer connection does
// not block startup.
func distinctTerminalBranchPairs(ctx context.Context, db *state.DB) ([]deadBranchPair, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT DISTINCT branch_ref, branch_generation
  FROM capture_events
 WHERE state IN (?, ?) AND branch_ref != ''`,
		state.EventStateBlockedConflict, state.EventStateFailed)
	if err != nil {
		return nil, fmt.Errorf("daemon: distinct terminal branch pairs: %w", err)
	}
	defer rows.Close()
	var out []deadBranchPair
	for rows.Next() {
		var p deadBranchPair
		if err := rows.Scan(&p.Ref, &p.Generation); err != nil {
			return nil, fmt.Errorf("daemon: scan terminal branch pair: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("daemon: iterate terminal branch pairs: %w", err)
	}
	return out, nil
}

// runStartupDeadBranchSweep scans terminal capture_events rows at daemon Run
// init and prunes those whose branch_ref has since been deleted. The currently
// active (branch_ref, branch_generation) pair is always preserved — even if its
// terminals exist — so an in-flight blocked_conflict the operator is about to
// resolve via `acd recover` is not silently removed.
//
// Returns nothing (best-effort). Failures are logged and traced; the daemon
// continues to start.
func runStartupDeadBranchSweep(
	ctx context.Context,
	repoDir string,
	db *state.DB,
	cctx CaptureContext,
	logger *slog.Logger,
	tracer acdtrace.Logger,
) {
	if isKeepDeadBranchBarriers() {
		// The single startup info log already fired in Run; just no-op.
		return
	}
	pairs, err := distinctTerminalBranchPairs(ctx, db)
	if err != nil {
		logger.Warn("startup sweep: read terminal branch pairs failed",
			"err", err.Error())
		return
	}
	scanned := len(pairs)
	if scanned == 0 {
		return
	}
	// Enumerate live branch refs once up-front instead of per-pair `git
	// show-ref` shell-outs. With N distinct terminal pairs this collapses N
	// forks into 1; on a long-lived repo with many stale branches the
	// difference dominates the sweep's wall-clock cost. Fail closed: any
	// error here preserves terminals (callers see the same "could not prove
	// dead, leave it alone" semantics as the per-pair RefExists fail-open).
	liveRefs, err := git.LiveBranchSet(ctx, repoDir)
	if err != nil {
		logger.Warn("startup sweep: enumerate live refs failed; preserving terminals",
			"err", err.Error())
		return
	}
	prunedRefs := make([]string, 0, deadBranchSweepRefsCap)
	totalRows := 0
	prunedPairs := 0
	for _, p := range pairs {
		if p.Ref == "" {
			continue
		}
		// Never touch the active pair.
		if p.Ref == cctx.BranchRef && p.Generation == cctx.BranchGeneration {
			continue
		}
		if _, alive := liveRefs[p.Ref]; alive {
			continue
		}
		rows, dErr := state.PurgeUnpublishedForDeadBranch(ctx, db, p.Ref, p.Generation)
		if dErr != nil {
			logger.Warn("startup sweep: purge dead-branch unpublished failed",
				"ref", p.Ref,
				"generation", p.Generation,
				"err", dErr.Error())
			continue
		}
		if rows == 0 {
			continue
		}
		prunedPairs++
		totalRows += rows
		if len(prunedRefs) < deadBranchSweepRefsCap {
			prunedRefs = append(prunedRefs, p.Ref)
		}
		logger.Info("startup sweep pruned dead-branch unpublished",
			"ref", p.Ref,
			"generation", p.Generation,
			"rows", rows)
	}
	if totalRows > 0 {
		// Stamp the diagnose-visible meta keys before recording the trace
		// event so readers that wake on the trace see consistent meta.
		recordDeadBranchPruneMeta(ctx, db, logger, totalRows, prunedRefs)
	}
	recordTrace(tracer, acdtrace.Event{
		Repo:       repoDir,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: "daemon.dead_branch_sweep",
		Decision:   sweepDecision(prunedPairs),
		Reason:     "startup sweep",
		Output: map[string]any{
			"scanned":           scanned,
			"pruned_pairs":      prunedPairs,
			"total_rows_pruned": totalRows,
			"refs":              prunedRefs,
		},
		Generation: cctx.BranchGeneration,
	})
}

// sweepDecision distinguishes a no-op sweep from one that actually removed rows.
// Kept here (not in trace.go) so trace.go does not have to know about sweep
// semantics.
func sweepDecision(prunedPairs int) string {
	if prunedPairs > 0 {
		return "pruned"
	}
	return "skip"
}

// pruneDeadBranchTerminals is the runtime Diverged-hook helper. Called after
// state.DeletePendingForGeneration to drop terminal rows for the prior
// (branch_ref, branch_generation) when the ref has been deleted. Honors the
// EnvKeepDeadBranchBarriers opt-out.
//
// Inputs come from the Diverged caller's local scope: oldRef is
// tokenBranchRef(oldToken); prevGeneration is the pre-bump generation;
// cctx is the post-bump context (used for trace BranchRef / HeadSHA /
// Generation).
//
// Best-effort: errors from RefExists or DeleteTerminalForDeadBranch are logged
// and traced (Error field) but do not propagate to the caller.
func pruneDeadBranchTerminals(
	ctx context.Context,
	repoDir string,
	db *state.DB,
	cctx CaptureContext,
	oldRef string,
	prevGeneration int64,
	logger *slog.Logger,
	tracer acdtrace.Logger,
	reason string,
) {
	if oldRef == "" || isKeepDeadBranchBarriers() {
		return
	}
	exists, err := git.RefExists(ctx, repoDir, oldRef)
	if err != nil {
		logger.Warn("dead-branch ref probe failed; preserving terminals",
			"ref", oldRef,
			"err", err.Error())
		recordTrace(tracer, acdtrace.Event{
			Repo:       repoDir,
			BranchRef:  cctx.BranchRef,
			HeadSHA:    cctx.BaseHead,
			EventClass: "branch_token.dead_branch_pruned",
			Decision:   "skip",
			Reason:     reason,
			Input:      map[string]any{"branch_ref": oldRef, "prev_generation": prevGeneration},
			Output: map[string]any{
				"prev_generation": prevGeneration,
				"branch_ref":      oldRef,
				"rows_pruned":     0,
			},
			Error:      err.Error(),
			Generation: cctx.BranchGeneration,
		})
		return
	}
	if exists {
		// Live ref — no action, no log.
		return
	}
	rows, err := state.PurgeUnpublishedForDeadBranch(ctx, db, oldRef, prevGeneration)
	if err != nil {
		logger.Warn("purge dead-branch unpublished failed",
			"ref", oldRef,
			"generation", prevGeneration,
			"err", err.Error())
		recordTrace(tracer, acdtrace.Event{
			Repo:       repoDir,
			BranchRef:  cctx.BranchRef,
			HeadSHA:    cctx.BaseHead,
			EventClass: "branch_token.dead_branch_pruned",
			Decision:   "error",
			Reason:     reason,
			Input:      map[string]any{"branch_ref": oldRef, "prev_generation": prevGeneration},
			Output: map[string]any{
				"prev_generation": prevGeneration,
				"branch_ref":      oldRef,
				"rows_pruned":     0,
			},
			Error:      err.Error(),
			Generation: cctx.BranchGeneration,
		})
		return
	}
	if rows > 0 {
		logger.Info("pruned dead-branch terminal rows",
			"ref", oldRef,
			"generation", prevGeneration,
			"rows", rows)
		// Stamp the diagnose-visible meta keys for the operator-facing
		// "last action that did something" surface. Skipped on the
		// rows == 0 path (probe-was-dead-but-no-terminals-queued) so the
		// previous snapshot survives.
		recordDeadBranchPruneMeta(ctx, db, logger, rows, []string{oldRef})
	}
	recordTrace(tracer, acdtrace.Event{
		Repo:       repoDir,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: "branch_token.dead_branch_pruned",
		Decision:   pruneDecision(rows),
		Reason:     reason,
		Input:      map[string]any{"branch_ref": oldRef, "prev_generation": prevGeneration},
		Output: map[string]any{
			"prev_generation": prevGeneration,
			"branch_ref":      oldRef,
			"rows_pruned":     rows,
		},
		Generation: cctx.BranchGeneration,
	})
}

// pruneDecision distinguishes a real prune from a probe that returned a dead
// ref but had no terminal rows queued.
func pruneDecision(rows int) string {
	if rows > 0 {
		return "pruned"
	}
	return "skip"
}

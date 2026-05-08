// dead_branch_sweep.go houses the helpers that prune terminal capture_events
// rows whose owning branch ref no longer resolves. Two callers live here:
//
//   - the runtime Diverged transition path (daemon.go) calls
//     pruneDeadBranchTerminals after dropping pending rows for the prior
//     generation. When the prior branch ref is gone, its blocked_conflict /
//     failed rows would otherwise accumulate forever — `acd status` and the
//     PendingEvents barrier path would surface phantom blocked counts for a
//     branch the operator has long since deleted.
//   - daemon Run init calls runStartupDeadBranchSweep BEFORE the main loop
//     so a daemon restart that discovers pre-existing dead-branch terminals
//     prunes them up-front instead of waiting for the next Diverged hook
//     (which may never fire if the operator moved on to a different branch
//     entirely).
//
// Both paths honor EnvKeepDeadBranchBarriers as an operator opt-out — set it
// truthy when you want to keep the rows around for forensic inspection.
package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

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
	prunedRefs := make([]string, 0, len(pairs))
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
		exists, err := git.RefExists(ctx, repoDir, p.Ref)
		if err != nil {
			logger.Warn("startup sweep: dead-branch ref probe failed; preserving terminals",
				"ref", p.Ref,
				"generation", p.Generation,
				"err", err.Error())
			continue
		}
		if exists {
			continue
		}
		rows, dErr := state.DeleteTerminalForDeadBranch(ctx, db, p.Ref, p.Generation)
		if dErr != nil {
			logger.Warn("startup sweep: delete dead-branch terminals failed",
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
		logger.Info("startup sweep pruned dead-branch terminals",
			"ref", p.Ref,
			"generation", p.Generation,
			"rows", rows)
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
	rows, err := state.DeleteTerminalForDeadBranch(ctx, db, oldRef, prevGeneration)
	if err != nil {
		logger.Warn("delete dead-branch terminals failed",
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

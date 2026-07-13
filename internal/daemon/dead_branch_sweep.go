// dead_branch_sweep.go houses the helpers that recover unpublished
// capture_events rows (pending + blocked_conflict + failed) whose owning
// branch ref no longer resolves. Two callers live here:
//
//   - the runtime Diverged transition path reconciles the prior exact pair
//     before accepting the new token. pruneDeadBranchTerminals remains as a
//     compatibility helper for older dead-ref rows found outside that path.
//   - daemon Run init schedules runStartupDeadBranchSweep on a goroutine
//     after the running-mode publish so a daemon restart that discovers
//     pre-existing dead-branch rows cleans them up off the blocking startup
//     path (the sweep can shell out to git for-each-ref and walk the entire
//     terminal-pair set, neither of which we want on the start-latency
//     budget).
//
// Both paths honor EnvKeepDeadBranchBarriers as an operator opt-out. Recovery
// composes the exact pair's immutable capture chain, anchors it under
// refs/acd/recovery, and atomically transitions every row to recovered. No
// captured row is deleted merely because its branch disappeared.
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
// disables both the runtime dead-ref recovery and the daemon-startup sweep.
// Operators set it when they want to inspect blocked_conflict / failed rows for
// branches that have since been deleted.
const EnvKeepDeadBranchBarriers = "ACD_KEEP_DEAD_BRANCH_BARRIERS"

// deadBranchSweepRefsCap caps how many distinct refs the startup sweep records
// in the trace event's `refs` field. The trace writer is best-effort and the
// payload is for operator forensics — bounding the slice keeps the JSONL row
// reasonable on a long-lived repo with many stale branches.
const deadBranchSweepRefsCap = 32

// MetaKeyDeadBranchPruneLastRunTS records the wall-clock unix-seconds of the
// most recent dead-branch recovery action. The legacy key name is preserved
// for diagnose/API compatibility; no capture row is deleted. Operators read this
// via `acd diagnose --json` to reason about whether stale-branch hygiene is
// keeping pace. No-op sweeps (zero rows recovered) do NOT update this key — the
// surface is intentionally "last action that did something" so a long quiet
// period after an operator deletes a branch remains visible.
const MetaKeyDeadBranchPruneLastRunTS = "dead_branch_prune.last_run_ts"

// MetaKeyDeadBranchPruneLastCount records the total number of capture_events
// rows recovered by the most recent non-empty dead-branch action. The legacy
// key name is preserved for compatibility. Stored as
// a base-10 string. Reset to the new total on every non-empty prune; not
// cumulative.
const MetaKeyDeadBranchPruneLastCount = "dead_branch_prune.last_count"

// MetaKeyDeadBranchPruneLastRefs is a JSON-encoded []string of the branch refs
// whose unpublished chains were recovered. The legacy key name is preserved.
// The slice is bounded by deadBranchSweepRefsCap so a sweep across
// many stale branches does not balloon the meta payload.
const MetaKeyDeadBranchPruneLastRefs = "dead_branch_prune.last_refs"

// recordDeadBranchRecoveryMeta stamps the three legacy dead-branch meta keys in a
// single MetaSetMany transaction so `acd diagnose` reads them atomically.
// Best-effort: any error is logged at warn level and the caller continues. The
// meta surface is forensic — never block recovery progress on a meta write.
//
// rows must be > 0 (the caller guards on the no-op case so empty sweeps do not
// overwrite the previous "last action that did something" snapshot). refs is
// the already-capped slice of recovered refs.
func recordDeadBranchRecoveryMeta(ctx context.Context, db *state.DB, logger *slog.Logger, rows int, refs []string) {
	refsJSON, err := json.Marshal(refs)
	if err != nil {
		// Marshalling a []string cannot fail under normal conditions, but
		// log defensively and keep going — the count + ts are still useful.
		logger.Warn("dead-branch recovery: marshal refs failed; recording empty refs",
			"err", err.Error())
		refsJSON = []byte("[]")
	}
	pairs := map[string]string{
		MetaKeyDeadBranchPruneLastRunTS: strconv.FormatInt(time.Now().Unix(), 10),
		MetaKeyDeadBranchPruneLastCount: strconv.Itoa(rows),
		MetaKeyDeadBranchPruneLastRefs:  string(refsJSON),
	}
	if err := state.MetaSetMany(ctx, db, pairs); err != nil {
		logger.Warn("dead-branch recovery: stamp meta keys failed",
			"err", err.Error(),
			"rows", rows,
			"refs", len(refs))
	}
}

// isKeepDeadBranchBarriers reports whether the operator opted out of dead-branch
// recovery via EnvKeepDeadBranchBarriers. Empty / falsy / unset -> false
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
// sweep can emit a per-pair recovery log line and skip the active pair.
type deadBranchPair struct {
	Ref        string
	Generation int64
}

type startupDeadBranchSweepOptions struct {
	// Test-only hook used to create a pause or git-operation marker after the
	// sweep's initial gate but immediately before the per-pair safety recheck.
	beforePairSafetyCheck func(context.Context, deadBranchPair)
}

// distinctUnpublishedBranchPairs reads exact (branch_ref, branch_generation) pairs of
// unpublished capture_events rows via the read-only handle. Routed through ReadSQL
// so a long-running replay drain holding the serialized writer connection does
// not block startup.
func distinctUnpublishedBranchPairs(ctx context.Context, db *state.DB) ([]deadBranchPair, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT DISTINCT branch_ref, branch_generation
  FROM capture_events
 WHERE state IN (?, ?, ?) AND branch_ref != ''`,
		state.EventStatePending, state.EventStateBlockedConflict, state.EventStateFailed)
	if err != nil {
		return nil, fmt.Errorf("daemon: distinct unpublished branch pairs: %w", err)
	}
	defer rows.Close()
	var out []deadBranchPair
	for rows.Next() {
		var p deadBranchPair
		if err := rows.Scan(&p.Ref, &p.Generation); err != nil {
			return nil, fmt.Errorf("daemon: scan unpublished branch pair: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("daemon: iterate unpublished branch pairs: %w", err)
	}
	return out, nil
}

// runStartupDeadBranchSweep scans unpublished capture_events rows at daemon Run
// init and recovers those whose branch_ref has since been deleted. The currently
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
	runStartupDeadBranchSweepWithOptions(ctx, repoDir, db, cctx, logger, tracer,
		startupDeadBranchSweepOptions{})
}

func runStartupDeadBranchSweepWithOptions(
	ctx context.Context,
	repoDir string,
	db *state.DB,
	cctx CaptureContext,
	logger *slog.Logger,
	tracer acdtrace.Logger,
	opts startupDeadBranchSweepOptions,
) {
	if isKeepDeadBranchBarriers() {
		// The single startup info log already fired in Run; just no-op.
		return
	}
	pairs, err := distinctUnpublishedBranchPairs(ctx, db)
	if err != nil {
		logger.Warn("startup sweep: read unpublished branch pairs failed",
			"err", err.Error())
		return
	}
	scanned := len(pairs)
	if scanned == 0 {
		return
	}
	gitDir, err := git.AbsoluteGitDir(ctx, repoDir)
	if err != nil {
		logger.Warn("startup sweep: resolve git dir failed; preserving unpublished rows",
			"err", err.Error())
		return
	}
	liveToken, err := BranchGenerationToken(ctx, repoDir)
	if err != nil {
		logger.Warn("startup sweep: resolve live token failed; preserving unpublished rows",
			"err", err.Error())
		return
	}
	archiveOnly := tokenSHA(liveToken) == ""
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
	recoveredRefs := make([]string, 0, deadBranchSweepRefsCap)
	totalRows := 0
	recoveredPairs := 0
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
		// Recheck immediately before recovery. The batched live-ref snapshot
		// is only a candidate filter; a branch may be recreated during the
		// sweep and must then remain owned by normal replay.
		exists, probeErr := git.RefExists(ctx, repoDir, p.Ref)
		if probeErr != nil {
			logger.Warn("startup sweep: recheck dead branch failed; preserving unpublished rows",
				"ref", p.Ref, "generation", p.Generation, "err", probeErr.Error())
			continue
		}
		if exists {
			continue
		}
		if opts.beforePairSafetyCheck != nil {
			opts.beforePairSafetyCheck(ctx, p)
		}
		// The sweep runs asynchronously after daemon startup, so the initial
		// Run-level pause/git-operation check can become stale during a long
		// scan. Recheck immediately before each mutation and stop the sweep on
		// any unsafe or unreadable state.
		if operation, active := gitOperationInProgress(gitDir); active {
			logger.Info("startup sweep: git operation began during scan; preserving remaining unpublished rows",
				"operation", operation, "ref", p.Ref, "generation", p.Generation)
			return
		}
		pauseStatus, pauseErr := daemonPauseState(ctx, gitDir, db)
		if pauseErr != nil {
			logger.Warn("startup sweep: recheck pause failed; preserving remaining unpublished rows",
				"ref", p.Ref, "generation", p.Generation, "err", pauseErr.Error())
			return
		}
		if pauseStatus.Active {
			logger.Info("startup sweep: pause began during scan; preserving remaining unpublished rows",
				"source", pauseStatus.Source, "reason", pauseStatus.Reason,
				"ref", p.Ref, "generation", p.Generation)
			return
		}
		result, dErr := reconcileTransitionPair(ctx, repoDir, gitDir, db,
			p.Ref, p.Generation, archiveOnly, p.Ref, "startup_dead_branch_sweep", tracer)
		if dErr != nil {
			logger.Warn("startup sweep: recover dead-branch unpublished failed",
				"ref", p.Ref,
				"generation", p.Generation,
				"err", dErr.Error())
			continue
		}
		if !result.Handled || result.EventCount == 0 {
			continue
		}
		recoveredPairs++
		totalRows += result.EventCount
		if len(recoveredRefs) < deadBranchSweepRefsCap {
			recoveredRefs = append(recoveredRefs, p.Ref)
		}
		logger.Info("startup sweep recovered dead-branch unpublished",
			"ref", p.Ref,
			"generation", p.Generation,
			"rows", result.EventCount,
			"recovery_ref", result.RecoveryRef)
	}
	if totalRows > 0 {
		// Stamp the diagnose-visible meta keys before recording the trace
		// event so readers that wake on the trace see consistent meta.
		recordDeadBranchRecoveryMeta(ctx, db, logger, totalRows, recoveredRefs)
	}
	recordTrace(tracer, acdtrace.Event{
		Repo:       repoDir,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: "daemon.dead_branch_sweep",
		Decision:   sweepDecision(recoveredPairs),
		Reason:     "startup sweep",
		Output: map[string]any{
			"scanned":              scanned,
			"recovered_pairs":      recoveredPairs,
			"total_rows_recovered": totalRows,
			"refs":                 recoveredRefs,
		},
		Generation: cctx.BranchGeneration,
	})
}

// sweepDecision distinguishes a no-op sweep from one that recovered rows.
// Kept here (not in trace.go) so trace.go does not have to know about sweep
// semantics.
func sweepDecision(recoveredPairs int) string {
	if recoveredPairs > 0 {
		return "recovered"
	}
	return "skip"
}

// pruneDeadBranchTerminals is the runtime Diverged-hook helper. It preserves
// the prior exact (branch_ref, branch_generation) chain when its ref has been
// deleted. The legacy name remains for compatibility with focused tests and
// diagnose metadata.
//
// Inputs come from the Diverged caller's local scope: oldRef is
// tokenBranchRef(oldToken); prevGeneration is the pre-bump generation;
// cctx is the post-bump context (used for trace BranchRef / HeadSHA /
// Generation).
//
// Best-effort: errors from RefExists or reconciliation are logged
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
			EventClass: "branch_token.dead_branch_recovered",
			Decision:   "skip",
			Reason:     reason,
			Input:      map[string]any{"branch_ref": oldRef, "prev_generation": prevGeneration},
			Output: map[string]any{
				"prev_generation": prevGeneration,
				"branch_ref":      oldRef,
				"rows_recovered":  0,
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
	gitDir, err := git.AbsoluteGitDir(ctx, repoDir)
	if err != nil {
		logger.Warn("resolve git dir for dead-branch recovery failed",
			"ref", oldRef,
			"generation", prevGeneration,
			"err", err.Error())
		recordTrace(tracer, acdtrace.Event{
			Repo:       repoDir,
			BranchRef:  cctx.BranchRef,
			HeadSHA:    cctx.BaseHead,
			EventClass: "branch_token.dead_branch_recovered",
			Decision:   "error",
			Reason:     reason,
			Input:      map[string]any{"branch_ref": oldRef, "prev_generation": prevGeneration},
			Output: map[string]any{
				"prev_generation": prevGeneration,
				"branch_ref":      oldRef,
				"rows_recovered":  0,
			},
			Error:      err.Error(),
			Generation: cctx.BranchGeneration,
		})
		return
	}
	liveToken, err := BranchGenerationToken(ctx, repoDir)
	if err != nil {
		logger.Warn("resolve live token for dead-branch recovery failed",
			"ref", oldRef, "generation", prevGeneration, "err", err.Error())
		return
	}
	result, err := reconcileTransitionPair(ctx, repoDir, gitDir, db,
		oldRef, prevGeneration, tokenSHA(liveToken) == "", oldRef, "runtime_dead_branch_sweep", tracer)
	if err != nil {
		logger.Warn("recover dead-branch unpublished failed",
			"ref", oldRef,
			"generation", prevGeneration,
			"err", err.Error())
		recordTrace(tracer, acdtrace.Event{
			Repo:       repoDir,
			BranchRef:  cctx.BranchRef,
			HeadSHA:    cctx.BaseHead,
			EventClass: "branch_token.dead_branch_recovered",
			Decision:   "error",
			Reason:     reason,
			Input:      map[string]any{"branch_ref": oldRef, "prev_generation": prevGeneration},
			Output: map[string]any{
				"prev_generation": prevGeneration,
				"branch_ref":      oldRef,
				"rows_recovered":  0,
			},
			Error:      err.Error(),
			Generation: cctx.BranchGeneration,
		})
		return
	}
	rows := result.EventCount
	if rows > 0 {
		logger.Info("recovered dead-branch unpublished rows",
			"ref", oldRef,
			"generation", prevGeneration,
			"rows", rows,
			"recovery_ref", result.RecoveryRef)
		recordDeadBranchRecoveryMeta(ctx, db, logger, rows, []string{oldRef})
	}
	recordTrace(tracer, acdtrace.Event{
		Repo: repoDir, BranchRef: cctx.BranchRef, HeadSHA: cctx.BaseHead,
		EventClass: "branch_token.dead_branch_recovered",
		Decision:   sweepDecision(rows), Reason: reason,
		Input: map[string]any{"branch_ref": oldRef, "prev_generation": prevGeneration},
		Output: map[string]any{"prev_generation": prevGeneration, "branch_ref": oldRef,
			"rows_recovered": rows, "recovery_ref": result.RecoveryRef},
		Generation: cctx.BranchGeneration,
	})
}

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
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

// pauseWarnLimiter coalesces repeated "malformed pause marker" / "invalid
// rewind grace" warnings to one emission per minute per (process, key).
// daemonPauseState runs on every replay pass; without throttling, a single
// stale or corrupt value emits a warn line every tick (sub-second cadence
// under fsnotify churn) and floods the operator's logs.
//
// Mirrors the pendingCapWarnLimiter pattern in capture.go: time-based, NTP-
// safe (clock backward → reset gate and emit), test-overridable interval.
var (
	pauseWarnMu       sync.Mutex
	pauseWarnLastUnix sync.Map // key string → *atomic.Int64 (unix seconds)
	pauseWarnInterval atomic.Int64
	pauseWarnNowFn    atomic.Pointer[func() time.Time]
)

const pauseWarnDefaultInterval = 60 // seconds

func pauseWarnNow() time.Time {
	if fn := pauseWarnNowFn.Load(); fn != nil && *fn != nil {
		return (*fn)()
	}
	return time.Now()
}

func pauseWarnIntervalSeconds() int64 {
	if v := pauseWarnInterval.Load(); v > 0 {
		return v
	}
	return pauseWarnDefaultInterval
}

// shouldEmitPauseWarn returns true at most once per interval per key.
func shouldEmitPauseWarn(key string) bool {
	pauseWarnMu.Lock()
	defer pauseWarnMu.Unlock()
	now := pauseWarnNow().Unix()
	stored, _ := pauseWarnLastUnix.LoadOrStore(key, &atomic.Int64{})
	last := stored.(*atomic.Int64).Load()
	interval := pauseWarnIntervalSeconds()
	if now < last { // NTP backward step (strict; same-second still throttles)
		stored.(*atomic.Int64).Store(now)
		return true
	}
	if now-last < interval {
		return false
	}
	stored.(*atomic.Int64).Store(now)
	return true
}

// resetPauseWarnForTest clears all keys and overrides the interval. Test-only.
func resetPauseWarnForTest(t interface{ Helper() }, intervalSeconds int64) {
	t.Helper()
	pauseWarnMu.Lock()
	pauseWarnLastUnix.Range(func(k, _ any) bool {
		pauseWarnLastUnix.Delete(k)
		return true
	})
	pauseWarnInterval.Store(intervalSeconds)
	pauseWarnMu.Unlock()
}

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
	//
	// The run loop sets Limit = DefaultReplayLimit so each replay pass returns
	// to the daemon promptly enough to claim flush_requests, refresh the
	// heartbeat, and observe shutdown. ReplaySummary.HasMore signals whether
	// the queue still contains pending work so the run loop can schedule an
	// immediate follow-up wake instead of waiting for the next poll tick.
	Limit int
	// Trace receives best-effort decision records. Nil disables tracing.
	Trace acdtrace.Logger
}

// DefaultReplayLimit caps a single replay pass at 64 events. Beyond this
// budget the daemon yields control back to the run loop so flush_requests,
// heartbeat refreshes, and shutdown signals are not starved by a long queue.
// ReplaySummary.HasMore tells the run loop whether to fire another pass
// immediately.
const DefaultReplayLimit = 64

// ReplaySummary describes one drain.
type ReplaySummary struct {
	Published int // events that produced a new commit
	Conflicts int // events terminally settled in state.EventStateBlockedConflict
	Failed    int // events marked failed (validation/commit errors)
	BaseHead  string
	Skipped   bool // replay drain was intentionally skipped before reading events
	// HasMore is true when ReplayOpts.Limit capped the batch and at least one
	// additional pending event was visible beyond the cap. The run loop uses
	// this to schedule an immediate follow-up replay pass without waiting for
	// the next poll tick. Always false when Limit <= 0 (unbounded drain).
	HasMore bool
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

	if paused, err := daemonPauseState(ctx, opts.GitDir, db); err != nil {
		return sum, err
	} else if paused.Active {
		sum.BaseHead = cctx.BaseHead
		sum.Skipped = true
		traceReplayPaused(opts.Trace, repoRoot, cctx, paused)
		return sum, nil
	}

	indexFile := opts.IndexFile
	if indexFile == "" {
		if opts.GitDir == "" {
			return sum, fmt.Errorf("daemon: Replay: IndexFile or GitDir required")
		}
		// TOCTOU defense: create a private 0o700 directory and place the
		// scratch index inside it. Never hand a *path* to git that we have
		// pre-created and then unlinked — the open-after-unlink window
		// would let a same-uid attacker substitute a symlink/regular file
		// at the path before git's read-tree opens it. The directory is
		// owned by us for the lifetime of the pass; git creates the index
		// file inside it (no file pre-exists at the chosen path), and we
		// tear the whole tree down on return.
		indexParent := filepath.Join(opts.GitDir, "acd")
		if err := os.MkdirAll(indexParent, 0o700); err != nil {
			return sum, fmt.Errorf("daemon: replay: mkdir index parent: %w", err)
		}
		tmpDir, err := os.MkdirTemp(indexParent, "replay-")
		if err != nil {
			return sum, fmt.Errorf("daemon: replay: mkdir temp index dir: %w", err)
		}
		// 0o700 is a defense-in-depth tighten — MkdirTemp already creates
		// at 0o700 on POSIX, but be explicit so a future umask change or
		// platform variance cannot widen the bag.
		if err := os.Chmod(tmpDir, 0o700); err != nil {
			_ = os.RemoveAll(tmpDir)
			return sum, fmt.Errorf("daemon: replay: chmod temp index dir: %w", err)
		}
		indexFile = filepath.Join(tmpDir, "idx")
		// Do NOT pre-create the index file. Do NOT unlink it. Hand the
		// fresh path inside our private dir to git via GIT_INDEX_FILE on
		// the first read-tree below.
		defer os.RemoveAll(tmpDir)
	} else if err := os.MkdirAll(filepath.Dir(indexFile), 0o700); err != nil {
		return sum, fmt.Errorf("daemon: replay: mkdir index parent: %w", err)
	} else {
		// Caller-provided path: stale entries from a prior crashed run
		// would otherwise poison write-tree. The TOCTOU concern only
		// applies to default temp paths; caller-supplied paths are
		// assumed-trusted (used by tests that need to inspect the index).
		_ = os.Remove(indexFile)
	}

	// Per-pass batch budget. When opts.Limit > 0 we query one extra row so the
	// "is there more queued behind this batch?" question can be answered
	// without a follow-up COUNT — sum.HasMore tells the run loop to schedule
	// an immediate next pass instead of waiting for the poll tick.
	queryLimit := opts.Limit
	if queryLimit > 0 {
		queryLimit = opts.Limit + 1
	}
	pending, err := state.PendingEvents(ctx, db, queryLimit)
	if err != nil {
		return sum, fmt.Errorf("daemon: load pending: %w", err)
	}
	if opts.Limit > 0 && len(pending) > opts.Limit {
		pending = pending[:opts.Limit]
		sum.HasMore = true
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

	// Resolve the active branch ref ONCE per pass. Per-event re-resolution
	// would fork a `git symbolic-ref HEAD` (and `rev-parse HEAD`) for every
	// event in the queue — O(events) subprocess invocations for a
	// pass-stable value. A concurrent committer that moves the ref will
	// trip the CAS retry path below, where we refresh the branch+tree
	// state on demand.
	if branchRef, headOID := resolveBranch(ctx, repoRoot, slog.Default()); branchRef != "" {
		activeCtx.BranchRef = branchRef
		if headOID != "" && headOID == parent {
			activeCtx.BaseHead = headOID
		}
	}
	// Cache the parent's tree OID. After every successful commit we carry
	// forward `treeOID` (the tree write-tree just produced is, by
	// construction, the next parent's tree) so we never run a fresh
	// `rev-parse <commit>^{tree}` per event in the steady state.
	parentTree, err := resolveTreeOID(ctx, repoRoot, parent)
	if err != nil {
		return sum, err
	}

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
		if reason, err := checkEventGeneration(ctx, repoRoot, parent, ev, activeCtx); err != nil {
			return sum, err
		} else if reason != "" {
			errorClass := replayErrorValidation
			if strings.Contains(reason, "branch ref mismatch") {
				errorClass = replayErrorRefMissing
			}
			if err := recordConflict(ctx, db, ev, replayIssue{
				ErrorClass: errorClass,
				Message:    reason,
				Ref:        activeCtx.BranchRef,
				Path:       ev.Path,
			}, activeCtx); err != nil {
				return sum, err
			}
			traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.conflict", state.EventStateBlockedConflict, reason, nil)
			sum.Conflicts++
			return sum, nil
		}

		ops, err := state.LoadCaptureOps(ctx, db, ev.Seq)
		if err != nil {
			return sum, fmt.Errorf("daemon: load ops seq=%d: %w", ev.Seq, err)
		}
		if len(ops) == 0 {
			// No ops to apply — mark failed, do not block the queue.
			if err := markFailed(ctx, db, ev, replayIssue{
				ErrorClass: replayErrorValidation,
				Message:    "no ops attached",
				Ref:        activeCtx.BranchRef,
				Path:       ev.Path,
			}); err != nil {
				return sum, err
			}
			traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.failed", state.EventStateFailed, "no ops attached", nil)
			sum.Failed++
			continue
		}

		// Validate before touching the index.
		if msg := validateOps(ops); msg != "" {
			if err := markFailed(ctx, db, ev, replayIssue{
				ErrorClass: replayErrorValidation,
				Message:    msg,
				Ref:        activeCtx.BranchRef,
				Path:       ev.Path,
			}); err != nil {
				return sum, err
			}
			traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.failed", state.EventStateFailed, msg, nil)
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
			headOID, alreadyPublished, err := alreadyPublishedAtHEAD(ctx, repoRoot, parent, ops)
			if err != nil {
				return sum, err
			}
			if alreadyPublished {
				sourceHead := parent
				if err := settlePublishedEvent(ctx, db, ev, activeCtx, sourceHead, headOID); err != nil {
					return sum, err
				}
				if err := git.ReadTree(ctx, repoRoot, indexFile, headOID); err != nil {
					return sum, fmt.Errorf("daemon: replay reseed index after idempotent publish: %w", err)
				}
				parent = headOID
				// Parent OID changed to an external HEAD — its tree must
				// be re-resolved before the next event can use parentTree
				// for no-op detection. This is rare (only fires on a
				// successful idempotent-publish), so the per-incident
				// rev-parse cost stays O(parallel-publishers) rather than
				// O(events).
				parentTree, err = resolveTreeOID(ctx, repoRoot, parent)
				if err != nil {
					return sum, err
				}
				activeCtx.BaseHead = headOID
				sum.BaseHead = headOID
				sum.Published++
				traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.commit", state.EventStatePublished, "already_published_by_external_committer", map[string]any{
					"commit": headOID,
					"parent": sourceHead,
				})
				continue
			}
			if err := recordConflict(ctx, db, ev, replayIssue{
				ErrorClass: replayErrorBeforeStateMismatch,
				Message:    reason,
				Ref:        activeCtx.BranchRef,
				Path:       ev.Path,
			}, activeCtx); err != nil {
				return sum, err
			}
			traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.conflict", state.EventStateBlockedConflict, reason, nil)
			sum.Conflicts++
			// Halt the batch: subsequent events were captured assuming
			// this one would land first. Running them now would replay on
			// top of a broken predecessor.
			return sum, nil
		}

		// Per-event timeout. write-tree, commit-tree, and update-ref are
		// the heavy git ops in this loop; a pathological worktree (giant
		// rename, GC contention, network alternates) could otherwise stall
		// a single event for minutes and starve flush_requests / shutdown
		// signals waiting on the run loop. Each event gets a 60s budget
		// inherited from the caller's ctx; on timeout the event is marked
		// failed and the batch halts so the next pass starts fresh. Tests
		// override the budget via replayPerEventTimeoutForTest.
		eventCtx, cancelEvent := context.WithTimeout(ctx, perEventTimeout())
		treeOID, err := applyOpsAndWriteTree(eventCtx, repoRoot, indexFile, ops)
		if err != nil {
			cancelEvent()
			if markErr := markFailed(ctx, db, ev, replayIssue{
				ErrorClass: replayErrorCommitBuildFailure,
				Message:    err.Error(),
				Ref:        activeCtx.BranchRef,
				Path:       ev.Path,
			}); markErr != nil {
				return sum, markErr
			}
			traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.failed", state.EventStateFailed, err.Error(), nil)
			sum.Failed++
			return sum, nil
		}

		// Parallel-create no-op: if write-tree produced the same tree as
		// the current parent, the captured ops are already reflected in
		// HEAD's tree (an external committer landed an identical change
		// before we got here). Settle the event as published against
		// `parent` without committing an empty tree.
		//
		// `parentTree` was resolved once at pass start (or refreshed on
		// the CAS retry path) — re-resolving via `rev-parse <parent>^{tree}`
		// per event would fork an extra git subprocess for every queued
		// row in the steady state.
		if parentTree != "" && treeOID == parentTree {
			cancelEvent()
			if err := settlePublishedEvent(ctx, db, ev, activeCtx, parent, parent); err != nil {
				return sum, err
			}
			// Reseed the scratch index from `parent` so chained events
			// see a clean baseline (write-tree leaves stale entries
			// otherwise).
			if err := git.ReadTree(ctx, repoRoot, indexFile, parent); err != nil {
				return sum, fmt.Errorf("daemon: replay reseed index after no-op tree: %w", err)
			}
			activeCtx.BaseHead = parent
			sum.BaseHead = parent
			sum.Published++
			traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.commit", state.EventStatePublished, "already_published_no_op_tree", map[string]any{
				"commit": parent,
				"parent": parent,
				"tree":   treeOID,
			})
			continue
		}

		// Build the commit on top of the new tree.
		commitOID, err := buildCommitFromTree(eventCtx, repoRoot, treeOID, parent, ev, ops, msgFn)
		if err != nil {
			cancelEvent()
			if markErr := markFailed(ctx, db, ev, replayIssue{
				ErrorClass: replayErrorCommitBuildFailure,
				Message:    err.Error(),
				Ref:        activeCtx.BranchRef,
				Path:       ev.Path,
			}); markErr != nil {
				return sum, markErr
			}
			traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.failed", state.EventStateFailed, err.Error(), nil)
			sum.Failed++
			// Halt the batch: a commit-build failure leaves `parent`
			// pointing at the prior commit, but later events will still
			// chain from a broken predecessor as soon as the operator
			// fixes the root cause. Stop here and let the next poll tick
			// re-attempt from a fresh seed.
			return sum, nil
		}

		// Advance the branch ref via CAS against the prior parent.
		oldOID := parent
		if cctx.BaseHead == "" {
			// Initial commit case (no prior parent) -> non-CAS update.
			oldOID = ""
		}
		if err := updateReplayRefWithRetry(eventCtx, repoRoot, "HEAD", commitOID, oldOID, opts.Trace, activeCtx, ev); err != nil {
			// CAS exhausted. Before declaring conflict, give the
			// idempotent path one shot: an external committer may have
			// landed identical content between our write-tree and our
			// final update-ref attempt, leaving HEAD's tree already in
			// the desired shape. alreadyPublishedAtHEAD enforces an
			// ancestry guard against `parent` so we cannot mistakenly
			// settle on top of a HEAD that diverged from our anchor.
			//
			// Only one probe per CAS exhaustion (the helper is
			// itself idempotent and re-reads HEAD post-probe to defend
			// against further movement).
			headOID, alreadyPublished, probeErr := alreadyPublishedAtHEAD(ctx, repoRoot, parent, ops)
			if probeErr != nil {
				cancelEvent()
				return sum, probeErr
			}
			if alreadyPublished {
				cancelEvent()
				if err := settlePublishedEvent(ctx, db, ev, activeCtx, parent, headOID); err != nil {
					return sum, err
				}
				if err := git.ReadTree(ctx, repoRoot, indexFile, headOID); err != nil {
					return sum, fmt.Errorf("daemon: replay reseed index after cas idempotent publish: %w", err)
				}
				parent = headOID
				// CAS lost to a concurrent committer. Re-resolve the
				// branch ref + parent tree so subsequent events in this
				// pass observe the post-publish anchor instead of the
				// stale pre-CAS tree we cached at pass start.
				if branchRef, refHead := resolveBranch(ctx, repoRoot, slog.Default()); branchRef != "" {
					activeCtx.BranchRef = branchRef
					if refHead != "" && refHead == parent {
						activeCtx.BaseHead = refHead
					}
				}
				parentTree, err = resolveTreeOID(ctx, repoRoot, parent)
				if err != nil {
					return sum, err
				}
				activeCtx.BaseHead = headOID
				sum.BaseHead = headOID
				sum.Published++
				traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.commit", state.EventStatePublished, "already_published_after_cas_exhaustion", map[string]any{
					"commit": headOID,
					"parent": oldOID,
				})
				continue
			}
			// CAS failed: ref moved out from under us. Block terminally —
			// every queued event downstream was captured against the
			// stale ref and must wait for branch reconciliation.
			reason := "update-ref CAS failed: " + err.Error()
			actual, expected := parseUpdateRefCASReason(reason)
			if expected == "" {
				expected = oldOID
			}
			cancelEvent()
			if recErr := recordConflict(ctx, db, ev, replayIssue{
				ErrorClass: replayErrorCASFail,
				Expected:   expected,
				Actual:     actual,
				Message:    reason,
				Ref:        activeCtx.BranchRef,
				Path:       ev.Path,
			}, activeCtx); recErr != nil {
				return sum, recErr
			}
			traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.conflict", state.EventStateBlockedConflict, reason, map[string]any{
				"expected_sha": expected,
				"actual_sha":   actual,
			})
			sum.Conflicts++
			return sum, nil
		}

		// Settle the event row + publish_state.
		cancelEvent()
		if err := settlePublishedEvent(ctx, db, ev, activeCtx, parent, commitOID); err != nil {
			return sum, err
		}

		parent = commitOID
		// Carry forward `treeOID` as the new parent's tree. The commit we
		// just produced (commitOID) was built from this exact tree by
		// commit-tree, so an extra `git rev-parse <commitOID>^{tree}` would
		// be redundant. Steady-state branch+tree probes are now O(passes),
		// not O(events).
		parentTree = treeOID
		activeCtx.BaseHead = commitOID
		sum.BaseHead = commitOID
		sum.Published++
		traceReplay(opts.Trace, repoRoot, activeCtx, ev, "replay.commit", state.EventStatePublished, "event published", map[string]any{
			"commit": commitOID,
			"parent": oldOID,
		})
	}

	return sum, nil
}

var (
	replayUpdateRef      = git.UpdateRef
	replayUpdateRefSleep = sleepWithContext
)

var replayUpdateRefBackoffs = []time.Duration{
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
}

// DefaultReplayPerEventTimeout caps the heavy git work for a single replay
// event (write-tree, commit-tree, update-ref retries). A pathological
// worktree (multi-GB rename, foreign object database, GC contention) could
// otherwise stall the run loop for minutes per event and starve flush
// requests, heartbeat refreshes, and shutdown signals. On timeout the
// per-event deadline fires inside the inner git op, the event is marked
// failed/blocked, and the batch halts so the next pass starts fresh.
const DefaultReplayPerEventTimeout = 60 * time.Second

var replayPerEventTimeoutOverride atomic.Int64 // nanoseconds, 0 = use default

func perEventTimeout() time.Duration {
	if v := replayPerEventTimeoutOverride.Load(); v > 0 {
		return time.Duration(v)
	}
	return DefaultReplayPerEventTimeout
}

// replayUpdateRefJitter applies jittered backoff to update-ref retries.
// Co-located daemons retrying the same ref at fixed 50/100/200ms cadences
// re-collide on every retry; ±25% jitter (uniform) breaks the lockstep so
// neighbours fan out across the wall-clock window. Tests pin a
// deterministic source via replayUpdateRefJitterFn.
var replayUpdateRefJitterFn atomic.Pointer[func(time.Duration) time.Duration]

const replayUpdateRefJitterFraction = 0.25

func defaultUpdateRefJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// math/rand/v2.Float64() ∈ [0,1). Map to [-fraction, +fraction).
	delta := (rngFloat64()*2 - 1) * replayUpdateRefJitterFraction
	jittered := time.Duration(float64(d) * (1 + delta))
	if jittered <= 0 {
		jittered = d / 2 // floor: never sleep zero or negative
	}
	return jittered
}

// rngFloat64 indirects math/rand/v2.Float64 so tests can pin determinism.
var rngFloat64 = func() float64 {
	return v2randFloat64()
}

type replayPause struct {
	Active    bool
	Source    string
	Reason    string
	SetAt     string
	ExpiresAt string
	Remaining int64
}

// daemonPauseState reads the daemon pause gate that BOTH replay and capture
// honor. Sources, in priority order:
//
//  1. Manual pause marker at <gitDir>/acd/paused (durable JSON written by
//     `acd pause`, cleared by `acd resume`). Active when present and not
//     expired. Malformed markers fail open with a warning. Non-regular
//     markers (FIFO, socket, device, directory, symlink) also fail open
//     with a warning so a stale inode cannot wedge replay indefinitely.
//  2. Rewind grace under daemon_meta.replay.paused_until — set when the
//     daemon detects a same-branch rewind (newHead is an ancestor of the
//     previous head, e.g. operator ran `git reset --soft HEAD~1`). The
//     gate covers BOTH replay and capture so transient worktree state
//     observed during the rewind window is NOT captured into the queue;
//     otherwise the post-grace replay would resurrect work the operator
//     just rewound.
//
// Detached HEAD pauses are handled by a separate gate in the Run loop.
//
// Callers must skip the capture pass and the replay drain when
// `paused.Active` is true. The shared helper guarantees both lanes observe
// the same state (alias retained for replay-internal call sites).
func daemonPauseState(ctx context.Context, gitDir string, db *state.DB) (replayPause, error) {
	now := time.Now().UTC()
	if gitDir != "" {
		marker, ok, err := pausepkg.Read(gitDir)
		switch {
		case errors.Is(err, pausepkg.ErrMalformed):
			slog.Default().Warn("ignoring malformed pause marker", "err", err.Error())
		case errors.Is(err, pausepkg.ErrNonRegularSource):
			// A non-regular pause marker (FIFO, socket, device, directory,
			// symlink) would otherwise wedge replay forever — every pass
			// would re-surface the same error. Fail open with a warning so
			// the queue keeps draining; the operator can investigate the
			// stray inode at <gitDir>/acd/paused at their leisure.
			slog.Default().Warn("ignoring non-regular pause marker", "err", err.Error())
		case err != nil:
			return replayPause{}, fmt.Errorf("daemon: read pause marker: %w", err)
		case ok:
			paused, err := markerPauseState(marker, now)
			if err != nil {
				if shouldEmitPauseWarn("invalid_pause_marker") {
					slog.Default().Warn("ignoring invalid pause marker", "err", err.Error())
				}
			} else if paused.Active {
				return paused, nil
			}
		}
	}

	raw, ok, err := state.MetaGet(ctx, db, MetaKeyReplayPausedUntil)
	if err != nil {
		return replayPause{}, fmt.Errorf("daemon: read replay pause meta: %w", err)
	}
	if !ok || strings.TrimSpace(raw) == "" {
		return replayPause{}, nil
	}
	until, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		if shouldEmitPauseWarn("invalid_rewind_grace") {
			slog.Default().Warn("ignoring invalid rewind grace pause", "value", raw, "err", err.Error())
		}
		return replayPause{}, nil
	}
	if !until.After(now) {
		if _, err := state.MetaDelete(ctx, db, MetaKeyReplayPausedUntil); err != nil {
			return replayPause{}, fmt.Errorf("daemon: clear expired replay pause meta: %w", err)
		}
		return replayPause{}, nil
	}
	return replayPause{
		Active:    true,
		Source:    "rewind_grace",
		Reason:    "rewind grace",
		ExpiresAt: until.UTC().Format(time.RFC3339),
		Remaining: int64(until.Sub(now).Seconds()),
	}, nil
}

func markerPauseState(marker pausepkg.Marker, now time.Time) (replayPause, error) {
	paused := replayPause{
		Active: true,
		Source: "manual",
		Reason: marker.Reason,
		SetAt:  marker.SetAt,
	}
	if marker.ExpiresAt == nil || strings.TrimSpace(*marker.ExpiresAt) == "" {
		return paused, nil
	}
	expiresAt, err := time.Parse(time.RFC3339, strings.TrimSpace(*marker.ExpiresAt))
	if err != nil {
		return replayPause{}, fmt.Errorf("parse expires_at: %w", err)
	}
	if !expiresAt.After(now) {
		return replayPause{}, nil
	}
	paused.ExpiresAt = expiresAt.UTC().Format(time.RFC3339)
	paused.Remaining = int64(expiresAt.Sub(now).Seconds())
	return paused, nil
}

func updateReplayRefWithRetry(
	ctx context.Context,
	repoRoot, ref, commitOID, oldOID string,
	logger acdtrace.Logger,
	cctx CaptureContext,
	ev state.CaptureEvent,
) error {
	var lastErr error
	for attempt := 1; attempt <= len(replayUpdateRefBackoffs); attempt++ {
		err := replayUpdateRef(ctx, repoRoot, ref, commitOID, oldOID)
		if err == nil {
			traceReplayUpdateRef(logger, repoRoot, cctx, ev, state.EventStatePublished, "update-ref CAS succeeded", attempt, false, ref, commitOID, oldOID, nil)
			return nil
		}
		lastErr = err

		retryable := isTransientUpdateRefLockError(err)
		finalAttempt := attempt == len(replayUpdateRefBackoffs)
		decision := state.EventStateBlockedConflict
		if retryable && !finalAttempt {
			decision = "retry"
		}
		traceReplayUpdateRef(logger, repoRoot, cctx, ev, decision, err.Error(), attempt, retryable && !finalAttempt, ref, commitOID, oldOID, err)

		if !retryable {
			return err
		}
		if finalAttempt {
			return err
		}
		if sleepErr := replayUpdateRefSleep(ctx, replayUpdateRefBackoffs[attempt-1]); sleepErr != nil {
			return sleepErr
		}
	}
	return lastErr
}

func isTransientUpdateRefLockError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return !strings.Contains(msg, " is at ") &&
		(strings.Contains(msg, "cannot lock") || strings.Contains(msg, "unable to lock"))
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func traceReplayUpdateRef(
	logger acdtrace.Logger,
	repoRoot string,
	cctx CaptureContext,
	ev state.CaptureEvent,
	decision, reason string,
	attempt int,
	retry bool,
	ref, commitOID, oldOID string,
	err error,
) {
	output := map[string]any{
		"attempt":      attempt,
		"max_attempts": len(replayUpdateRefBackoffs),
		"retry":        retry,
		"ref":          ref,
		"commit":       commitOID,
		"expected_sha": oldOID,
	}
	if err != nil {
		actual, expected := parseUpdateRefCASReason("update-ref CAS failed: " + err.Error())
		if actual != "" {
			output["actual_sha"] = actual
		}
		if expected != "" {
			output["expected_sha"] = expected
		}
	}
	traceReplay(logger, repoRoot, cctx, ev, "replay.update_ref", decision, reason, output)
}

// alreadyPublishedAtHEAD reports whether HEAD's tree already reflects the
// captured ops, signalling that an external committer landed our intent
// before we got there. Returning (headOID, true, nil) tells the caller to
// settle the event as published against `headOID` without minting a new
// commit.
//
// Two guards keep idempotent settle from masking real divergence:
//
//  1. Ancestry guard: `sourceHead` (the replay parent the event was about
//     to chain off) MUST be an ancestor of the current HEAD. If HEAD has
//     diverged from our parent (operator hard-reset to an unrelated
//     branch, force-push, etc.) the matching tree state is coincidence,
//     not a successful parallel publish — return false and let the caller
//     block terminally. An empty `sourceHead` skips the probe (initial
//     commit / orphan repo).
//  2. HEAD-movement guard: HEAD is re-resolved AFTER the per-op tree
//     probes. If the resolved OID has shifted between the first read and
//     the post-probe re-read, the captured tree state is no longer
//     guaranteed to correspond to the live HEAD — return false so the
//     caller retries on the next replay pass with a fresh anchor.
func alreadyPublishedAtHEAD(ctx context.Context, repoRoot, sourceHead string, ops []state.CaptureOp) (string, bool, error) {
	// Defensive empty-ops guard. The replay loop only reaches this helper
	// after validateOps + LoadCaptureOps, but a future refactor could
	// hand us a zero-length slice — settle to "not published" rather than
	// silently confirming an empty event.
	if len(ops) == 0 {
		return "", false, nil
	}
	headOID, err := git.RevParse(ctx, repoRoot, "HEAD")
	if err != nil {
		if errors.Is(err, git.ErrRefNotFound) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("rev-parse HEAD: %w", err)
	}
	// Ancestry guard: an external HEAD that doesn't descend from our
	// replay parent means the matching tree state is coincidence, not a
	// successful parallel publish. Return (headOID, false) so the caller
	// can record a real conflict instead of silently chaining off a
	// stranger.
	if sourceHead != "" && sourceHead != headOID {
		descends, err := git.IsAncestor(ctx, repoRoot, sourceHead, headOID)
		if err != nil {
			return "", false, fmt.Errorf("ancestry probe %s..%s: %w", sourceHead, headOID, err)
		}
		if !descends {
			return headOID, false, nil
		}
	}
	for _, op := range ops {
		if op.Op == "delete" {
			// Delete is idempotent only when HEAD has NO entry at all
			// for this path. A path replaced by a directory (tree
			// entry) or a submodule (commit entry) is NOT absent —
			// settling as published would mask a real divergence.
			absent, err := isPathAbsentInTree(ctx, repoRoot, headOID, op.Path)
			if err != nil {
				return "", false, err
			}
			if !absent {
				return headOID, false, nil
			}
			continue
		}
		blobOID, err := git.LsTreeBlobOID(ctx, repoRoot, headOID, op.Path)
		if err != nil {
			return "", false, fmt.Errorf("ls-tree HEAD %s: %w", op.Path, err)
		}
		if !op.AfterOID.Valid || op.AfterOID.String == "" {
			return headOID, false, nil
		}
		if blobOID != op.AfterOID.String {
			return headOID, false, nil
		}
		if op.AfterMode.Valid && op.AfterMode.String != "" {
			entries, err := git.LsTree(ctx, repoRoot, headOID, false, op.Path)
			if err != nil {
				return "", false, fmt.Errorf("ls-tree HEAD %s: %w", op.Path, err)
			}
			if !treeEntryModeMatches(entries, op.Path, op.AfterMode.String) {
				return headOID, false, nil
			}
		}
		if op.Op == "rename" && op.OldPath.Valid && op.OldPath.String != "" {
			absent, err := isPathAbsentInTree(ctx, repoRoot, headOID, op.OldPath.String)
			if err != nil {
				return "", false, err
			}
			if !absent {
				return headOID, false, nil
			}
			// Rename source verify: before settling as already-published
			// we require the captured BeforeOID for the rename source to
			// still be present in the object database. If it's missing
			// (gc'd, partial fetch), we cannot prove the rename actually
			// matches the captured intent, so refuse to settle and let
			// the caller block.
			if op.BeforeOID.Valid && op.BeforeOID.String != "" {
				present, err := objectExists(ctx, repoRoot, op.BeforeOID.String)
				if err != nil {
					return "", false, err
				}
				if !present {
					return headOID, false, nil
				}
			}
		}
	}
	// HEAD-movement guard: the per-op probes above all read against the
	// `headOID` we resolved at the start. If HEAD has moved while we were
	// probing (an external committer landed something between the first
	// rev-parse and the last ls-tree), the matching tree state no longer
	// describes the live ref. Refuse to settle and let the caller try
	// again on the next pass with a fresh anchor.
	postHead, err := git.RevParse(ctx, repoRoot, "HEAD")
	if err != nil {
		if errors.Is(err, git.ErrRefNotFound) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("rev-parse HEAD post-probe: %w", err)
	}
	if postHead != headOID {
		return postHead, false, nil
	}
	return headOID, true, nil
}

// isPathAbsentInTree reports whether path is absent at ref. A path resolved
// to a non-blob entry (tree, submodule) is treated as NOT absent — the
// caller's idempotent check must not confuse a directory-replacement with
// a successful delete.
func isPathAbsentInTree(ctx context.Context, repoRoot, ref, path string) (bool, error) {
	entries, err := git.LsTree(ctx, repoRoot, ref, false, path)
	if err != nil {
		return false, fmt.Errorf("ls-tree %s %s: %w", ref, path, err)
	}
	for _, entry := range entries {
		if entry.Path == path {
			return false, nil
		}
	}
	return true, nil
}

// objectExists reports whether the given OID is present in the local
// object database via `git cat-file -e`. Used by the rename-source verify
// path so the daemon will not settle a rename as published when the
// captured BeforeOID is no longer reachable (shallow clone, gc'd ref).
func objectExists(ctx context.Context, repoRoot, oid string) (bool, error) {
	if oid == "" {
		return false, nil
	}
	_, _, err := git.RunWithStderr(ctx, git.RunOpts{Dir: repoRoot}, "cat-file", "-e", oid)
	if err == nil {
		return true, nil
	}
	var gerr *git.Error
	if errors.As(err, &gerr) && gerr.ExitCode == 1 {
		return false, nil
	}
	return false, fmt.Errorf("cat-file -e %s: %w", oid, err)
}

func treeEntryModeMatches(entries []git.TreeEntry, path, mode string) bool {
	for _, entry := range entries {
		if entry.Path == path && entry.Type == "blob" {
			return entry.Mode == mode
		}
	}
	return false
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

// applyOpsAndWriteTree applies ops via update-index --index-info on the
// isolated index and runs write-tree, returning the resulting tree OID. The
// caller decides whether to mint a commit (and is responsible for any
// follow-up update-ref).
//
// Mirrors snapshot_state.apply_ops_to_index. Split out from commitOneEvent
// so the replay loop can compare the new tree against the parent tree and
// skip commit-tree on a parallel-create no-op.
func applyOpsAndWriteTree(ctx context.Context, repoRoot, indexFile string, ops []state.CaptureOp) (string, error) {
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
	return tree, nil
}

// buildCommitFromTree composes the commit message and runs commit-tree on
// the supplied tree OID. Returns the new commit OID; the caller is
// responsible for update-ref.
func buildCommitFromTree(ctx context.Context, repoRoot, treeOID, parent string, ev state.CaptureEvent, ops []state.CaptureOp, msgFn MessageFn) (string, error) {
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
	commitOID, err := git.CommitTree(ctx, repoRoot, treeOID, msg, parents...)
	if err != nil {
		return "", fmt.Errorf("commit-tree: %w", err)
	}
	return commitOID, nil
}

// resolveTreeOID returns the tree OID at the given commit. Empty input
// returns ("", nil) so the caller can short-circuit. Missing refs are also
// surfaced as ("", nil) — there is no parent tree to compare against.
func resolveTreeOID(ctx context.Context, repoRoot, commit string) (string, error) {
	if commit == "" {
		return "", nil
	}
	tree, err := git.RevParse(ctx, repoRoot, commit+"^{tree}")
	if err != nil {
		if errors.Is(err, git.ErrRefNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("rev-parse %s^{tree}: %w", commit, err)
	}
	return tree, nil
}

func settlePublishedEvent(ctx context.Context, db *state.DB, ev state.CaptureEvent, cctx CaptureContext, sourceHead, commitOID string) error {
	nowSec := float64(time.Now().UnixNano()) / 1e9
	if err := state.MarkEventPublished(ctx, db,
		ev.Seq, state.EventStatePublished,
		sql.NullString{String: commitOID, Valid: true},
		sql.NullString{},
		ev.Message, nowSec,
	); err != nil {
		return fmt.Errorf("daemon: mark published: %w", err)
	}
	if err := state.SavePublishState(ctx, db, state.Publish{
		EventSeq:         sql.NullInt64{Int64: ev.Seq, Valid: true},
		BranchRef:        sql.NullString{String: cctx.BranchRef, Valid: true},
		BranchGeneration: sql.NullInt64{Int64: cctx.BranchGeneration, Valid: true},
		SourceHead:       sql.NullString{String: sourceHead, Valid: true},
		TargetCommitOID:  sql.NullString{String: commitOID, Valid: true},
		Status:           "published",
	}); err != nil {
		// Best-effort: the event row is already marked published via
		// MarkEventPublished above. publish_state is the operator-visible
		// breadcrumb singleton — surfacing the failure as a slog warn is
		// enough so the event row itself stays authoritative.
		slog.Default().Warn("save publish_state after MarkEventPublished",
			"seq", ev.Seq, "commit", commitOID, "err", err.Error())
	}
	return nil
}

// markFailed flags an event as terminally failed and records the reason.
// "failed" is terminal — PendingEvents excludes the row, so the next pass
// will not re-attempt it. Returns a non-nil error when the terminal-state
// write to capture_events fails: swallowing the error would leave the
// event in `pending` and the next pass would replay it forever. The
// caller is expected to surface the error to the run loop so the
// scheduler can back off rather than spin retry-storming the same row.
func markFailed(ctx context.Context, db *state.DB, ev state.CaptureEvent, issue replayIssue) error {
	if issue.Message == "" {
		issue.Message = "replay failed"
	}
	nowSec := float64(time.Now().UnixNano()) / 1e9
	if err := state.MarkEventPublished(ctx, db,
		ev.Seq, state.EventStateFailed,
		sql.NullString{}, sql.NullString{String: issue.Message, Valid: true},
		ev.Message, nowSec); err != nil {
		return fmt.Errorf("daemon: mark failed seq=%d: %w", ev.Seq, err)
	}
	recordReplayIssue(ctx, db, ev, issue, nowSec)
	return nil
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
//
// Returns a non-nil error when MarkEventBlocked fails. Swallowing it would
// leave the event in `pending`, so PendingEvents would resurface it on the
// next pass and replay would loop forever on a row the daemon already
// classified as terminally broken. The caller surfaces the error to the
// run loop so the scheduler can back off.
func recordConflict(ctx context.Context, db *state.DB, ev state.CaptureEvent, issue replayIssue, cctx CaptureContext) error {
	if issue.Message == "" {
		issue.Message = "replay conflict"
	}
	nowSec := float64(time.Now().UnixNano()) / 1e9
	if err := state.MarkEventBlocked(ctx, db, ev.Seq, issue.Message, nowSec,
		sql.NullString{String: cctx.BranchRef, Valid: true},
		sql.NullInt64{Int64: cctx.BranchGeneration, Valid: true},
		sql.NullString{String: cctx.BaseHead, Valid: true},
	); err != nil {
		return fmt.Errorf("daemon: mark blocked seq=%d: %w", ev.Seq, err)
	}
	recordReplayIssue(ctx, db, ev, issue, nowSec)
	return nil
}

const (
	metaKeyLastReplayConflict       = "last_replay_conflict"
	metaKeyLastReplayConflictLegacy = "last_replay_conflict_legacy"

	replayErrorCASFail             = "cas_fail"
	replayErrorBeforeStateMismatch = "before_state_mismatch"
	replayErrorCommitBuildFailure  = "commit_build_failure"
	replayErrorRefMissing          = "ref_missing"
	replayErrorValidation          = "validation"
)

type replayIssue struct {
	ErrorClass string
	Expected   string
	Actual     string
	Ref        string
	Path       string
	Message    string
}

type replayConflictMetadata struct {
	TS         string `json:"ts"`
	Seq        int64  `json:"seq"`
	ErrorClass string `json:"error_class"`
	Expected   string `json:"expected_sha,omitempty"`
	Actual     string `json:"actual_sha,omitempty"`
	Ref        string `json:"ref,omitempty"`
	Path       string `json:"path,omitempty"`
	Message    string `json:"message"`
}

func recordReplayIssue(ctx context.Context, db *state.DB, ev state.CaptureEvent, issue replayIssue, nowSec float64) {
	if issue.ErrorClass == "" {
		issue.ErrorClass = classifyReplayIssue(issue.Message)
	}
	meta := replayConflictMetadata{
		TS:         time.Unix(0, int64(nowSec*1e9)).UTC().Format(time.RFC3339Nano),
		Seq:        ev.Seq,
		ErrorClass: issue.ErrorClass,
		Expected:   issue.Expected,
		Actual:     issue.Actual,
		Ref:        issue.Ref,
		Path:       issue.Path,
		Message:    issue.Message,
	}
	_ = state.MetaSetJSON(ctx, db, metaKeyLastReplayConflict, meta)
	_ = state.MetaSet(ctx, db, metaKeyLastReplayConflictLegacy,
		fmt.Sprintf("seq=%d: %s", ev.Seq, issue.Message))
}

func classifyReplayIssue(message string) string {
	switch {
	case strings.Contains(message, "update-ref CAS failed"):
		return replayErrorCASFail
	case strings.Contains(message, "before-state mismatch"),
		strings.Contains(message, "missing-in-index"),
		strings.Contains(message, "create conflict"),
		strings.Contains(message, "rename source"),
		strings.Contains(message, "rename target"):
		return replayErrorBeforeStateMismatch
	case strings.Contains(message, "commit-tree"),
		strings.Contains(message, "write-tree"),
		strings.Contains(message, "update-index"):
		return replayErrorCommitBuildFailure
	case strings.Contains(message, "branch ref mismatch"):
		return replayErrorRefMissing
	default:
		return replayErrorValidation
	}
}

func parseUpdateRefCASReason(reason string) (actual, expected string) {
	const actualMarker = " is at "
	const expectedMarker = " but expected "
	actualStart := strings.Index(reason, actualMarker)
	expectedStart := strings.Index(reason, expectedMarker)
	if actualStart == -1 || expectedStart == -1 || expectedStart <= actualStart {
		return "", ""
	}
	actualFields := strings.Fields(reason[actualStart+len(actualMarker) : expectedStart])
	if len(actualFields) > 0 {
		actual = actualFields[0]
	}
	expectedFields := strings.Fields(reason[expectedStart+len(expectedMarker):])
	if len(expectedFields) > 0 {
		expected = expectedFields[0]
	}
	return actual, expected
}

func traceReplay(logger acdtrace.Logger, repoRoot string, cctx CaptureContext, ev state.CaptureEvent, class, decision, reason string, output map[string]any) {
	input := map[string]any{
		"operation": ev.Operation,
		"path":      ev.Path,
	}
	recordTrace(logger, acdtrace.Event{
		Repo:       repoRoot,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: class,
		Decision:   decision,
		Reason:     reason,
		Input:      input,
		Output:     output,
		Error:      traceError(decision, reason),
		Seq:        ev.Seq,
		Generation: cctx.BranchGeneration,
	})
}

func traceCapturePaused(logger acdtrace.Logger, repoRoot string, cctx CaptureContext, paused replayPause) {
	output := map[string]any{
		"source": paused.Source,
	}
	if paused.Reason != "" {
		output["reason"] = paused.Reason
	}
	if paused.SetAt != "" {
		output["set_at"] = paused.SetAt
	}
	if paused.ExpiresAt != "" {
		output["expires_at"] = paused.ExpiresAt
		output["remaining_seconds"] = paused.Remaining
	}
	recordTrace(logger, acdtrace.Event{
		Repo:       repoRoot,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: "capture.pause",
		Decision:   "skipped",
		Reason:     "capture_paused",
		Output:     output,
		Generation: cctx.BranchGeneration,
	})
}

func traceReplayPaused(logger acdtrace.Logger, repoRoot string, cctx CaptureContext, paused replayPause) {
	output := map[string]any{
		"source": paused.Source,
	}
	if paused.Reason != "" {
		output["reason"] = paused.Reason
	}
	if paused.SetAt != "" {
		output["set_at"] = paused.SetAt
	}
	if paused.ExpiresAt != "" {
		output["expires_at"] = paused.ExpiresAt
		output["remaining_seconds"] = paused.Remaining
	}
	recordTrace(logger, acdtrace.Event{
		Repo:       repoRoot,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: "replay.pause",
		Decision:   "skipped",
		Reason:     "replay_paused",
		Output:     output,
		Generation: cctx.BranchGeneration,
	})
}

func traceError(decision, reason string) string {
	if decision == state.EventStatePublished || reason == "" {
		return ""
	}
	return reason
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
	ok, err := git.IsAncestor(ctx, repoRoot, ev.BaseHead, parent)
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

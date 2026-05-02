// capture.go walks the worktree, hashes every captured file into the git
// object store, and emits classify ops persisted into capture_events +
// capture_ops per §8.2.
//
// Walk semantics carried over from the legacy daemon (snapshot-capture.py):
//   - filepath.WalkDir + manual symlink handling (do NOT call WalkDir on
//     followlinks=true — this is the regression CLAUDE.md calls out).
//   - Symlinks always emit mode 120000 regardless of target type. NEVER
//     descend into a symlinked directory.
//   - Skip nested .git (file or dir) and submodule (gitlink mode 160000).
//   - Skip ACD's own .git/acd state subdir.
//   - Sensitive default-deny via state.SensitiveMatcher.
//   - Gitignored paths via batch git.IgnoreChecker.
//   - Oversize regulars (> ACD_MAX_FILE_BYTES, default 5 MiB) -> meta-only.
//   - Regular files opened with O_NOFOLLOW + post-open lstat/fstat
//     ino+dev+mode verification (TOCTOU defense against symlink swap).
package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

// EnvMaxFileBytes is the per-file size cap. Mirrors the legacy
// SNAPSHOTD_MAX_FILE_BYTES knob with the new ACD_ prefix.
const EnvMaxFileBytes = "ACD_MAX_FILE_BYTES"

// DefaultMaxFileBytes is the default per-file size cap (5 MiB).
const DefaultMaxFileBytes int64 = 5 << 20

// EnvMaxPendingEvents bounds capture_events FIFO depth for the active
// (branch_ref, branch_generation). When the depth meets or exceeds the cap
// the new event is dropped (history is preserved; only the *new* tail is
// refused) and a rate-limited slog.Warn fires. 0 disables the cap.
const EnvMaxPendingEvents = "ACD_MAX_PENDING_EVENTS"

// DefaultMaxPendingEvents is the default per-generation pending-depth cap
// applied when EnvMaxPendingEvents is unset. 50_000 events is well above
// "normal capture" volume but small enough to bound memory + replay cost
// during a multi-day pause.
const DefaultMaxPendingEvents = 50_000

// MetaKeyPendingHighWater is the daemon_meta key under which the
// highest-observed pending depth (a.k.a. "watermark") is persisted for
// `acd diagnose --json`. Persisted as a base-10 integer string.
const MetaKeyPendingHighWater = "capture.pending_high_water"

// MetaKeyCaptureBackpressurePausedAt is the daemon_meta key whose presence
// signals that capture has entered durable backpressure: the pending FIFO
// for the active (branch_ref, branch_generation) reached
// ACD_MAX_PENDING_EVENTS and the daemon refuses to walk + classify until
// replay drains the queue below the high-water mark (or the operator
// explicitly accepts the loss via `acd resume --accept-overflow`). The
// value is the RFC3339 UTC timestamp of the FIRST observation; subsequent
// passes that re-encounter the saturated cap leave the timestamp untouched
// so operators can see how long backpressure has been active.
const MetaKeyCaptureBackpressurePausedAt = "capture.backpressure_paused_at"

// MetaKeyCaptureEventsDroppedTotal is a cumulative counter of capture ops
// that the backpressure gate refused to enqueue across the lifetime of the
// state.db. Persisted as a base-10 int64. Surfaced via `acd diagnose
// --json` so operators can detect silent loss without scraping logs.
const MetaKeyCaptureEventsDroppedTotal = "capture.events_dropped_total"

// CaptureBackpressureClearRatio is the high-water fraction of
// ACD_MAX_PENDING_EVENTS at which capture lifts the durable backpressure
// pause. Pending must drop strictly below cap*ratio before
// MetaKeyCaptureBackpressurePausedAt is cleared. 0.8 keeps capture
// suppressed until replay has made meaningful progress, avoiding a
// thrash where each pass alternates between paused and resumed.
const CaptureBackpressureClearRatio = 0.8

// CapDropReasonAtCap is the trace reason emitted when the pending-depth cap
// drops a captured op rather than appending it to capture_events.
const CapDropReasonAtCap = "pending depth at cap"

// CapDropReasonBackpressureEntry is the trace reason emitted on the pass
// that first observes saturation and skips walk+classify entirely. The
// dropped events count for the pass is unknown (we never walked) so the
// trace event records the cap and the cumulative dropped-total instead.
const CapDropReasonBackpressureEntry = "capture saturated; skipped walk"

// stateSubdir is the per-repo state directory name inside .git/. Keeping it
// here as a local constant avoids importing internal/state just for the
// string; the package-level helper in state/db.go is "acd" via
// AcdDirFromGitDir.
const stateSubdir = "acd"

// CaptureSummary describes one capture pass.
type CaptureSummary struct {
	EventsAppended   int   // number of capture_events rows inserted
	EventsDropped    int   // ops refused due to ACD_MAX_PENDING_EVENTS cap
	Oversize         int   // files skipped due to size cap
	Errors           int   // soft errors (per-file lstat/open failures)
	WalkedFiles      int64 // for diagnostics
	PendingDepth     int   // pending depth observed for the active generation at end of pass (0 if cap disabled)
	PendingHighWater int64 // updated daemon_meta.capture.pending_high_water value (0 if not bumped)
	// Skipped is true when Capture intentionally skipped the walk before
	// touching the worktree (e.g. an active manual pause marker or rewind
	// grace). Mirrors ReplaySummary.Skipped so direct callers can short-
	// circuit the same way the run loop does.
	Skipped bool
	// SkipReason is a short human-readable label populated alongside
	// Skipped. Empty when Skipped is false.
	SkipReason string
	// BackpressurePaused is true when the pass observed the durable
	// capture-backpressure gate as active (either entered this pass or
	// entered earlier and not yet cleared). The walk is skipped on entry;
	// the field is also true for the same pass that drops the gate to
	// describe the state across the transition.
	BackpressurePaused bool
	// BackpressureCleared is true when this pass observed the durable
	// capture-backpressure gate transitioning from active to inactive
	// (pending dropped below CaptureBackpressureClearRatio * cap).
	BackpressureCleared bool
	// EventsDroppedTotal mirrors daemon_meta.capture.events_dropped_total
	// after this pass. 0 when the cumulative counter has never advanced.
	EventsDroppedTotal int64
}

// CaptureContext carries the per-pass repository identity that the legacy
// daemon calls "ctx" (branch_ref, branch_generation, base_head). Phase 1
// keeps this struct small and lets the run loop populate it; the
// branch-generation token implementation lives elsewhere (§8.9).
type CaptureContext struct {
	BranchRef        string
	BranchGeneration int64
	BaseHead         string // HEAD OID at start of pass (or "" if no HEAD)
}

// CaptureOpts configures one capture pass. Zero-valued fields fall back to
// production defaults; tests inject lighter substitutes.
type CaptureOpts struct {
	// MaxFileBytes overrides EnvMaxFileBytes / DefaultMaxFileBytes.
	MaxFileBytes int64
	// IgnoreChecker batches gitignore checks. Caller owns the lifetime —
	// typically built once at daemon start and reused for the run.
	IgnoreChecker *git.IgnoreChecker
	// SensitiveMatcher precomputes the active sensitive glob set. Caller
	// owns the lifetime; nil falls back to a fresh matcher per pass (slow
	// but correct).
	SensitiveMatcher *state.SensitiveMatcher
	// SubmodulePaths is the set of repo-relative paths that are submodules
	// (mode 160000 in HEAD's tree). Capture must not descend into them.
	SubmodulePaths map[string]bool
	// Trace receives best-effort decision records. Nil disables tracing.
	Trace acdtrace.Logger
	// GitDir is the absolute .git directory. Required to consult the
	// daemon pause gate (manual marker + rewind grace meta). Empty
	// disables the in-Capture pause check entirely; callers that have
	// already gated externally (e.g. the run loop) should set
	// SkipPauseCheck instead so the gate is symmetric across direct and
	// run-loop invocations.
	GitDir string
	// SkipPauseCheck disables the daemon pause gate inside Capture. The
	// run loop already gates capture+replay on the same pause state and
	// emits a single trace event before either pass runs; setting this
	// flag avoids a double-trace. Direct callers (tests, future CLI
	// wrappers) leave it false to honor the gate.
	SkipPauseCheck bool
}

// resolveMaxFileBytes consults EnvMaxFileBytes, falls back to default.
func resolveMaxFileBytes(opt int64) int64 {
	if opt > 0 {
		return opt
	}
	if env := os.Getenv(EnvMaxFileBytes); env != "" {
		if n, err := strconv.ParseInt(env, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return DefaultMaxFileBytes
}

// resolveMaxPendingEvents consults EnvMaxPendingEvents and returns the
// effective cap. Negative values are clamped to 0 (disabled).
//
// Behavior:
//   - empty / unset -> DefaultMaxPendingEvents.
//   - parses to a non-negative int64 -> that value (0 disables).
//   - parse error -> DefaultMaxPendingEvents (fail safe to bounded).
func resolveMaxPendingEvents() int64 {
	env := os.Getenv(EnvMaxPendingEvents)
	if env == "" {
		return DefaultMaxPendingEvents
	}
	n, err := strconv.ParseInt(env, 10, 64)
	if err != nil {
		return DefaultMaxPendingEvents
	}
	if n < 0 {
		return 0
	}
	return n
}

// pendingCapWarnLimiter coalesces "capture pending depth at cap" warnings to
// at most one per minute per process. Tests can override the wall clock and
// minimum interval via the unexported helpers below.
var (
	pendingCapWarnMu       sync.Mutex
	pendingCapWarnLastUnix atomic.Int64
	pendingCapWarnInterval atomic.Int64 // seconds between warns; 0 = use default
	pendingCapNowFn        atomic.Pointer[func() time.Time]
)

const pendingCapWarnDefaultInterval = 60 // seconds

func pendingCapWarnNow() time.Time {
	if fn := pendingCapNowFn.Load(); fn != nil && *fn != nil {
		return (*fn)()
	}
	return time.Now()
}

func pendingCapWarnIntervalSeconds() int64 {
	if v := pendingCapWarnInterval.Load(); v > 0 {
		return v
	}
	return pendingCapWarnDefaultInterval
}

// shouldEmitPendingCapWarn returns true when the rate-limited token says it
// is time to emit a fresh slog.Warn. Concurrent capture passes serialize
// under pendingCapWarnMu so we never race two warns through the gate.
//
// NTP-safe: an NTP backward step would otherwise leave (now-last) negative,
// which compares as < interval forever and silences the warn. We clamp two
// ways:
//
//  1. now <= last → the clock ran backwards (or we're in the same second);
//     reset last to now and emit so a stuck warn becomes unstuck on the next
//     capture pass.
//  2. last is more than `2 * interval` in the future relative to now — this
//     can only happen when last was stamped before a bigger backward step.
//     Reset and emit.
func shouldEmitPendingCapWarn() bool {
	pendingCapWarnMu.Lock()
	defer pendingCapWarnMu.Unlock()
	now := pendingCapWarnNow().Unix()
	last := pendingCapWarnLastUnix.Load()
	interval := pendingCapWarnIntervalSeconds()
	// NTP backward step: clock went STRICTLY back so `last` is now in the
	// future. Reset the gate and emit so a stuck warn does not stay
	// suppressed. We use strict `<` (not `<=`) so same-second re-entry
	// still throttles correctly.
	if now < last {
		pendingCapWarnLastUnix.Store(now)
		return true
	}
	if now-last < interval {
		return false
	}
	pendingCapWarnLastUnix.Store(now)
	return true
}

// resetPendingCapWarnForTest clears the limiter so individual tests can
// observe a fresh warn without waiting a full minute. Test-only.
func resetPendingCapWarnForTest(t interface{ Helper() }, intervalSeconds int64) {
	t.Helper()
	pendingCapWarnMu.Lock()
	pendingCapWarnLastUnix.Store(0)
	pendingCapWarnInterval.Store(intervalSeconds)
	pendingCapWarnMu.Unlock()
}

// captureBackpressureActive returns true when the durable backpressure
// pause meta key is set. Best-effort: errors are returned to the caller so
// Capture can fail closed (treat the pass as paused) rather than walking
// while the gate is in an unknown state.
func captureBackpressureActive(ctx context.Context, db *state.DB) (bool, string, error) {
	if db == nil {
		return false, "", nil
	}
	v, ok, err := state.MetaGet(ctx, db, MetaKeyCaptureBackpressurePausedAt)
	if err != nil {
		return false, "", err
	}
	if !ok {
		return false, "", nil
	}
	return true, v, nil
}

// enterCaptureBackpressure stamps MetaKeyCaptureBackpressurePausedAt with
// an RFC3339 UTC timestamp on the first transition into backpressure.
// Idempotent: a second call while the key is already present leaves the
// original timestamp untouched so operators see how long the gate has been
// active.
func enterCaptureBackpressure(ctx context.Context, db *state.DB, now time.Time) (string, bool, error) {
	if db == nil {
		return "", false, nil
	}
	if v, ok, err := state.MetaGet(ctx, db, MetaKeyCaptureBackpressurePausedAt); err != nil {
		return "", false, err
	} else if ok && v != "" {
		return v, false, nil
	}
	stamp := now.UTC().Format(time.RFC3339)
	if err := state.MetaSet(ctx, db, MetaKeyCaptureBackpressurePausedAt, stamp); err != nil {
		return "", false, err
	}
	return stamp, true, nil
}

// clearCaptureBackpressure removes MetaKeyCaptureBackpressurePausedAt.
// Returns whether a row was actually deleted so the caller can decide
// whether to emit a "cleared" trace event.
func clearCaptureBackpressure(ctx context.Context, db *state.DB) (bool, error) {
	if db == nil {
		return false, nil
	}
	return state.MetaDelete(ctx, db, MetaKeyCaptureBackpressurePausedAt)
}

// readEventsDroppedTotal returns the cumulative dropped-events counter
// stored in daemon_meta. Returns 0 when the key is unset or unparseable —
// the counter is for visibility only, never a correctness anchor.
func readEventsDroppedTotal(ctx context.Context, db *state.DB) (int64, error) {
	if db == nil {
		return 0, nil
	}
	v, ok, err := state.MetaGet(ctx, db, MetaKeyCaptureEventsDroppedTotal)
	if err != nil {
		return 0, err
	}
	if !ok || v == "" {
		return 0, nil
	}
	n, perr := strconv.ParseInt(v, 10, 64)
	if perr != nil {
		// Corrupt counter — treat as 0 and let the next bump overwrite.
		return 0, nil
	}
	return n, nil
}

// bumpEventsDroppedTotal adds delta to the cumulative dropped-events
// counter. Best-effort but surfaces parse/write errors so the caller can
// trace them. Negative deltas are clamped to 0 (the counter is monotonic).
func bumpEventsDroppedTotal(ctx context.Context, db *state.DB, delta int64) (int64, error) {
	if db == nil || delta <= 0 {
		cur, err := readEventsDroppedTotal(ctx, db)
		return cur, err
	}
	cur, err := readEventsDroppedTotal(ctx, db)
	if err != nil {
		return 0, err
	}
	next := cur + delta
	if err := state.MetaSet(ctx, db, MetaKeyCaptureEventsDroppedTotal, strconv.FormatInt(next, 10)); err != nil {
		return cur, err
	}
	return next, nil
}

// updatePendingHighWater bumps daemon_meta.capture.pending_high_water when
// depth strictly exceeds the persisted value. Best-effort: errors are
// swallowed because the capture pipeline must keep running.
func updatePendingHighWater(ctx context.Context, db *state.DB, depth int) {
	if db == nil || depth <= 0 {
		return
	}
	cur, _, err := state.MetaGet(ctx, db, MetaKeyPendingHighWater)
	if err != nil {
		return
	}
	prev := int64(0)
	if cur != "" {
		if v, perr := strconv.ParseInt(cur, 10, 64); perr == nil {
			prev = v
		}
	}
	if int64(depth) <= prev {
		return
	}
	_ = state.MetaSet(ctx, db, MetaKeyPendingHighWater, strconv.FormatInt(int64(depth), 10))
}

// Capture walks the repo, builds the live map, classifies vs the persisted
// shadow_paths for this (branch, generation), persists capture events +
// updates shadow rows, and returns a summary. The caller is expected to
// have bootstrapped the shadow against HEAD before the first capture; this
// helper does not own the bootstrap path.
//
// Callers must pass a stable cctx — the (branch, generation) tuple keys both
// the shadow_paths read AND the capture_events insert, so a concurrent
// branch swap mid-walk would emit events keyed to the new generation while
// the live map was sampled under the old one.
func Capture(ctx context.Context, repoRoot string, db *state.DB, cctx CaptureContext, opts CaptureOpts) (CaptureSummary, error) {
	if repoRoot == "" {
		return CaptureSummary{}, fmt.Errorf("daemon: Capture: empty repoRoot")
	}
	if db == nil {
		return CaptureSummary{}, fmt.Errorf("daemon: Capture: nil db")
	}
	if cctx.BranchRef == "" {
		return CaptureSummary{}, fmt.Errorf("daemon: Capture: empty branch_ref")
	}
	if cctx.BaseHead == "" {
		return CaptureSummary{}, fmt.Errorf("daemon: Capture: empty base_head")
	}

	// Daemon pause gate — symmetric with Replay. Manual pause marker and
	// rewind grace pause BOTH lanes; otherwise a direct caller (test, CLI,
	// future automation) could resurrect rewound work by walking the
	// worktree while the operator is mid-surgery. The run loop sets
	// SkipPauseCheck because it already gates on the same state and emits
	// the trace event itself; direct callers leave it false.
	if !opts.SkipPauseCheck && opts.GitDir != "" {
		paused, err := daemonPauseState(ctx, opts.GitDir, db)
		if err != nil {
			return CaptureSummary{}, err
		}
		if paused.Active {
			traceCapturePaused(opts.Trace, repoRoot, cctx, paused)
			reason := paused.Reason
			if reason == "" {
				reason = paused.Source
			}
			return CaptureSummary{Skipped: true, SkipReason: reason}, nil
		}
	}

	// Backpressure gate. The pending-depth cap is a per-(branch_ref,
	// branch_generation) FIFO bound: under a long pause the queue would
	// otherwise grow without bound and starve replay/memory. We refuse to
	// walk + classify while the queue is saturated so the daemon stops
	// burning fsnotify-driven walks on a state that cannot accept new
	// events. The gate is durable in daemon_meta — once tripped it stays
	// active until either replay drains pending below
	// CaptureBackpressureClearRatio*cap, or the operator explicitly
	// accepts the loss via `acd resume --accept-overflow`.
	pendingCap := resolveMaxPendingEvents()
	var summary CaptureSummary
	pending := -1
	if pendingCap > 0 {
		n, perr := state.CountPendingEventsForGeneration(ctx, db, cctx.BranchRef, cctx.BranchGeneration)
		if perr != nil {
			return summary, fmt.Errorf("daemon: count pending events: %w", perr)
		}
		pending = n
		bpActive, bpSetAt, perr := captureBackpressureActive(ctx, db)
		if perr != nil {
			return summary, fmt.Errorf("daemon: read capture backpressure: %w", perr)
		}

		// Entry: pending at or above the cap. Stamp the durable gate (no-op
		// if already set), refresh the high-water mark, increment the
		// cumulative dropped-total by the *current overflow* (best-effort —
		// we have not walked, so the precise per-pass drop count is
		// unknown; charging at least 1 keeps the counter strictly
		// monotonic across saturated passes), and skip walk + classify.
		if int64(pending) >= pendingCap {
			summary.BackpressurePaused = true
			summary.PendingDepth = pending
			updatePendingHighWater(ctx, db, pending)
			if !bpActive {
				if stamp, _, perr := enterCaptureBackpressure(ctx, db, time.Now()); perr != nil {
					return summary, fmt.Errorf("daemon: enter capture backpressure: %w", perr)
				} else {
					bpSetAt = stamp
				}
			}
			delta := int64(1)
			if int64(pending) > pendingCap {
				delta = int64(pending) - pendingCap + 1
			}
			total, perr := bumpEventsDroppedTotal(ctx, db, delta)
			if perr != nil {
				return summary, fmt.Errorf("daemon: bump events dropped total: %w", perr)
			}
			summary.EventsDroppedTotal = total
			summary.EventsDropped = int(delta)
			if v, ok, _ := state.MetaGet(ctx, db, MetaKeyPendingHighWater); ok && v != "" {
				if hw, perr := strconv.ParseInt(v, 10, 64); perr == nil {
					summary.PendingHighWater = hw
				}
			}
			if shouldEmitPendingCapWarn() {
				slog.Default().Warn(
					"capture pending depth at cap; skipping walk and entering backpressure pause. Use acd resume --accept-overflow to clear, or wait for replay to drain.",
					slog.String("branch_ref", cctx.BranchRef),
					slog.Int64("branch_generation", cctx.BranchGeneration),
					slog.Int64("cap", pendingCap),
					slog.Int("pending_depth", pending),
					slog.String("env", EnvMaxPendingEvents),
					slog.String("backpressure_set_at", bpSetAt),
					slog.Int64("events_dropped_total", total),
				)
			}
			recordTrace(opts.Trace, acdtrace.Event{
				Repo:       repoRoot,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "capture.event",
				Decision:   "dropped",
				Reason:     CapDropReasonBackpressureEntry,
				Output: map[string]any{
					"pending_depth":            pending,
					"cap":                      pendingCap,
					"backpressure_set_at":      bpSetAt,
					"events_dropped_total":     total,
					"events_dropped_this_pass": delta,
				},
				Generation: cctx.BranchGeneration,
			})
			return summary, nil
		}

		// Drain crossing: backpressure was active and pending has dropped
		// strictly below the high-water mark. Clear the gate and emit a
		// trace event so operators can correlate "backpressure ended" with
		// the replay drain that did the work.
		if bpActive {
			highWater := int64(float64(pendingCap) * CaptureBackpressureClearRatio)
			if highWater < 1 {
				highWater = 1
			}
			if int64(pending) < highWater {
				removed, perr := clearCaptureBackpressure(ctx, db)
				if perr != nil {
					return summary, fmt.Errorf("daemon: clear capture backpressure: %w", perr)
				}
				if removed {
					summary.BackpressureCleared = true
					recordTrace(opts.Trace, acdtrace.Event{
						Repo:       repoRoot,
						BranchRef:  cctx.BranchRef,
						HeadSHA:    cctx.BaseHead,
						EventClass: "capture.pause",
						Decision:   "cleared",
						Reason:     "pending drained below high-water mark",
						Output: map[string]any{
							"pending_depth": pending,
							"cap":           pendingCap,
							"high_water":    highWater,
							"prior_set_at":  bpSetAt,
							"source":        "drained",
						},
						Generation: cctx.BranchGeneration,
					})
				}
			} else {
				// Still above the high-water mark even though we are
				// strictly below the cap — keep the gate active and skip
				// the walk to give replay more headroom.
				summary.BackpressurePaused = true
				summary.PendingDepth = pending
				if v, ok, _ := state.MetaGet(ctx, db, MetaKeyPendingHighWater); ok && v != "" {
					if hw, perr := strconv.ParseInt(v, 10, 64); perr == nil {
						summary.PendingHighWater = hw
					}
				}
				if total, perr := readEventsDroppedTotal(ctx, db); perr == nil {
					summary.EventsDroppedTotal = total
				}
				recordTrace(opts.Trace, acdtrace.Event{
					Repo:       repoRoot,
					BranchRef:  cctx.BranchRef,
					HeadSHA:    cctx.BaseHead,
					EventClass: "capture.pause",
					Decision:   "skipped",
					Reason:     "capture saturated; awaiting drain below high-water",
					Output: map[string]any{
						"pending_depth": pending,
						"cap":           pendingCap,
						"high_water":    highWater,
						"set_at":        bpSetAt,
						"source":        "backpressure",
					},
					Generation: cctx.BranchGeneration,
				})
				return summary, nil
			}
		}
	}

	matcher := opts.SensitiveMatcher
	if matcher == nil {
		matcher = state.NewSensitiveMatcher()
	}
	maxBytes := resolveMaxFileBytes(opts.MaxFileBytes)

	live, walkSummary, err := walkLive(ctx, repoRoot, walkOpts{
		matcher:       matcher,
		ignoreChecker: opts.IgnoreChecker,
		submodules:    opts.SubmodulePaths,
		maxBytes:      maxBytes,
		db:            db,
	})
	if err != nil {
		// walkLive populates Errors/Oversize/WalkedFiles in its own summary;
		// merge those into the active summary before returning.
		summary.Errors += walkSummary.Errors
		summary.Oversize += walkSummary.Oversize
		summary.WalkedFiles += walkSummary.WalkedFiles
		return summary, err
	}
	summary.Errors += walkSummary.Errors
	summary.Oversize += walkSummary.Oversize
	summary.WalkedFiles += walkSummary.WalkedFiles

	shadow, err := loadShadow(ctx, db, cctx)
	if err != nil {
		return summary, fmt.Errorf("daemon: load shadow: %w", err)
	}

	ops := Classify(shadow, live)
	recordTrace(opts.Trace, acdtrace.Event{
		Repo:       repoRoot,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: "capture.classify",
		Decision:   "classified",
		Reason:     "compared live worktree to shadow state",
		Output: map[string]any{
			"ops":          len(ops),
			"walked_files": summary.WalkedFiles,
			"oversize":     summary.Oversize,
			"errors":       summary.Errors,
		},
		Generation: cctx.BranchGeneration,
	})

	// Persist each classified op as its own capture_events row + capture_ops
	// child. Atomic-per-file commits (§8.3) means one event = one op. We do
	// NOT batch multiple ops into a single event in v1 — keeping the schema
	// flexible is fine, but the replay invariant is "1 commit per event".
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			return summary, err
		}

		if pendingCap > 0 && int64(pending) >= pendingCap {
			// Mid-pass saturation: the entry-time gate let this pass run
			// (pending was below cap) but appending classified ops pushed
			// the queue at or above the cap. Stamp the durable gate so the
			// NEXT pass early-returns ahead of walk, increment the
			// dropped-total once for this op, emit a single rate-limited
			// warn + trace per pass, and stop processing further ops to
			// keep walk cost bounded.
			summary.EventsDropped++
			summary.BackpressurePaused = true
			summary.PendingDepth = pending
			updatePendingHighWater(ctx, db, pending)
			if _, _, perr := enterCaptureBackpressure(ctx, db, time.Now()); perr != nil {
				return summary, fmt.Errorf("daemon: enter capture backpressure: %w", perr)
			}
			total, perr := bumpEventsDroppedTotal(ctx, db, 1)
			if perr != nil {
				return summary, fmt.Errorf("daemon: bump events dropped total: %w", perr)
			}
			summary.EventsDroppedTotal = total
			if v, ok, _ := state.MetaGet(ctx, db, MetaKeyPendingHighWater); ok && v != "" {
				if hw, perr := strconv.ParseInt(v, 10, 64); perr == nil {
					summary.PendingHighWater = hw
				}
			}
			if shouldEmitPendingCapWarn() {
				slog.Default().Warn(
					"capture pending depth at cap mid-pass; entering backpressure pause. Use acd resume --accept-overflow to clear, or wait for replay to drain.",
					slog.String("branch_ref", cctx.BranchRef),
					slog.Int64("branch_generation", cctx.BranchGeneration),
					slog.Int64("cap", pendingCap),
					slog.Int("pending_depth", pending),
					slog.String("env", EnvMaxPendingEvents),
				)
			}
			recordTrace(opts.Trace, acdtrace.Event{
				Repo:       repoRoot,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "capture.event",
				Decision:   "dropped",
				Reason:     CapDropReasonAtCap,
				Input: map[string]any{
					"op":       op.Op,
					"path":     op.Path,
					"old_path": op.OldPath,
					"fidelity": op.Fidelity,
				},
				Output: map[string]any{
					"pending_depth":        pending,
					"cap":                  pendingCap,
					"events_dropped_total": total,
				},
				Generation: cctx.BranchGeneration,
			})
			// Stop the pass — further ops cannot land while saturated, and
			// re-classifying them next pass after a drain is the
			// self-healing behavior we want.
			return summary, nil
		}

		ev := state.CaptureEvent{
			BranchRef:        cctx.BranchRef,
			BranchGeneration: cctx.BranchGeneration,
			BaseHead:         cctx.BaseHead,
			Operation:        op.Op,
			Path:             op.Path,
			Fidelity:         op.Fidelity,
			OldPath:          nullString(op.OldPath),
		}
		stateOps := []state.CaptureOp{toStateOp(op)}
		seq, err := state.AppendCaptureEvent(ctx, db, ev, stateOps)
		if err != nil {
			return summary, fmt.Errorf("daemon: append capture event %s %s: %w", op.Op, op.Path, err)
		}
		summary.EventsAppended++
		if pendingCap > 0 {
			pending++
		}
		recordTrace(opts.Trace, acdtrace.Event{
			Repo:       repoRoot,
			BranchRef:  cctx.BranchRef,
			HeadSHA:    cctx.BaseHead,
			EventClass: "capture.event",
			Decision:   "appended",
			Reason:     "classified op persisted to capture_events",
			Input: map[string]any{
				"op":       op.Op,
				"path":     op.Path,
				"old_path": op.OldPath,
				"fidelity": op.Fidelity,
			},
			Output:     map[string]any{"seq": seq},
			Seq:        seq,
			Generation: cctx.BranchGeneration,
		})

		// Update shadow_paths to reflect the new live state. Renames erase
		// the old path; deletes erase the path; everything else upserts.
		if err := updateShadow(ctx, db, cctx, op); err != nil {
			return summary, fmt.Errorf("daemon: update shadow %s: %w", op.Path, err)
		}
	}

	if pendingCap > 0 {
		if pending >= 0 {
			summary.PendingDepth = pending
			updatePendingHighWater(ctx, db, pending)
		}
		// Reflect the post-update high water in the summary regardless of
		// whether we just bumped it; readers want the current persisted
		// value, not a delta.
		if v, ok, _ := state.MetaGet(ctx, db, MetaKeyPendingHighWater); ok && v != "" {
			if hw, perr := strconv.ParseInt(v, 10, 64); perr == nil {
				summary.PendingHighWater = hw
			}
		}
	}
	// Surface the cumulative dropped-total regardless of whether the cap is
	// active this pass. Readers want the running counter, not a delta.
	if total, perr := readEventsDroppedTotal(ctx, db); perr == nil && total > 0 {
		summary.EventsDroppedTotal = total
	}

	return summary, nil
}

func toStateOp(op ClassifiedOp) state.CaptureOp {
	return state.CaptureOp{
		Ord:        0,
		Op:         op.Op,
		Path:       op.Path,
		OldPath:    nullString(op.OldPath),
		BeforeOID:  nullString(op.BeforeOID),
		BeforeMode: nullString(op.BeforeMode),
		AfterOID:   nullString(op.AfterOID),
		AfterMode:  nullString(op.AfterMode),
		Fidelity:   op.Fidelity,
	}
}

// nullString wraps an empty/non-empty string as sql.NullString.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// walkOpts bundles inputs to walkLive so the function signature stays
// readable.
type walkOpts struct {
	matcher       *state.SensitiveMatcher
	ignoreChecker *git.IgnoreChecker
	submodules    map[string]bool
	maxBytes      int64
	db            *state.DB
}

// ignoreCheckBatchSize caps how many paths walkLive sends per
// IgnoreChecker.Check call. The cap is by file count (not byte size) so
// pathological worktrees with very long path names cannot wedge the
// long-lived `git check-ignore --stdin` subprocess on a single huge write.
// Layers larger than the cap are sliced into multiple round-trips; the
// results are concatenated in order before survivors descend.
const ignoreCheckBatchSize = 1000

// classifyIgnoredBatched calls ig.Check in slices of at most batchSize
// paths and concatenates the boolean results so the returned slice is
// 1:1 with the input. Empty input returns a non-nil empty slice for
// caller convenience. Any error from the underlying Check call aborts
// the loop and surfaces immediately.
func classifyIgnoredBatched(ctx context.Context, ig *git.IgnoreChecker, paths []string, batchSize int) ([]bool, error) {
	out := make([]bool, 0, len(paths))
	if len(paths) == 0 {
		return out, nil
	}
	if batchSize <= 0 {
		batchSize = ignoreCheckBatchSize
	}
	for start := 0; start < len(paths); start += batchSize {
		end := start + batchSize
		if end > len(paths) {
			end = len(paths)
		}
		results, err := ig.Check(ctx, paths[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	return out, nil
}

// walkLive walks the worktree and returns the live map.
//
// Implementation notes:
//   - We BFS the worktree by directory layer instead of letting
//     filepath.WalkDir DFS the whole tree. Each layer batches the cheap
//     filters (symlink, .git, ACD subdir, nested repo, submodule, sensitive)
//     locally; survivors are then run through IgnoreChecker.Check in
//     batches of ignoreCheckBatchSize so a top-level gitignored directory
//     like build/, node_modules/, or DerivedData is pruned before we
//     readdir its 100k+ children. This mirrors fsnotify_watcher.preWalk
//     (commit f647b92): per-layer batched ignore-classify + parent-prune.
//   - Symlinks-to-directory are NEVER descended into (followlinks=false
//     equivalent), preserving the legacy CLAUDE.md regression. They are
//     captured as mode-120000 candidates instead.
//   - Sensitive + ignore checks short-circuit before O_NOFOLLOW + read.
//   - All errors except context cancellation are soft: the daemon must keep
//     running across permission errors or file races.
func walkLive(ctx context.Context, repoRoot string, opts walkOpts) (map[string]LiveEntry, CaptureSummary, error) {
	live := map[string]LiveEntry{}
	var summary CaptureSummary

	// First pass: BFS the worktree, collecting (a) regular-file + symlink
	// candidates that survived the cheap filters and (b) the directory
	// frontier we still need to descend. Both are then batch-classified
	// against IgnoreChecker; ignored directories are pruned before their
	// children are read.
	type candidate struct {
		rel  string
		full string
		fi   os.FileInfo
	}
	var pending []candidate

	type dirEntry struct {
		rel  string // worktree-relative slashed path; "" for the root
		full string
	}
	frontier := []dirEntry{{rel: "", full: repoRoot}}

	for len(frontier) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, summary, err
		}

		var nextDirs []dirEntry
		var fileCands []candidate

		// layerHadError coalesces per-entry soft errors into a single
		// summary.Errors bump for the whole BFS layer. Previously the DFS
		// path bumped Errors at most once per pass; the BFS rewrite would
		// otherwise inflate the counter linearly with the number of
		// unreadable entries (a corrupt subtree with 10k bad children
		// reported Errors=10k instead of Errors=1), breaking comparison
		// across versions for triage. The trace event_class table still
		// emits the per-entry detail; only the aggregate counter is
		// coalesced here.
		layerHadError := false
		bumpLayerError := func() {
			if !layerHadError {
				summary.Errors++
				layerHadError = true
			}
		}

		for _, parent := range frontier {
			children, err := os.ReadDir(parent.full)
			if err != nil {
				// Soft error: the directory vanished or is unreadable.
				bumpLayerError()
				continue
			}
			for _, d := range children {
				name := d.Name()
				var childRel string
				if parent.rel == "" {
					childRel = name
				} else {
					childRel = parent.rel + "/" + name
				}
				if hasControlPathChar(childRel) {
					recordInvalidPath(ctx, opts.db, childRel, "control_chars")
					bumpLayerError()
					continue
				}

				// Always step around .git, regardless of depth. The token
				// only ever exists as a top-level component, but the cheap
				// topComponent slice keeps the check identical to the
				// previous DFS implementation. Our own state lives at
				// <gitDir>/acd, which is inside .git and never reachable as
				// a worktree-rooted top component, so we deliberately do
				// NOT prune top-level "acd/" — that would silently delete
				// user repos containing a literal acd/ directory.
				// TODO: if state ever lives outside .git, prune by
				// comparing the absolute path against gitDir/stateSubdir.
				topComponent := childRel
				if i := strings.IndexByte(childRel, '/'); i >= 0 {
					topComponent = childRel[:i]
				}
				if topComponent == ".git" {
					continue
				}

				childFull := filepath.Join(parent.full, name)
				fi, lstatErr := os.Lstat(childFull)
				if lstatErr != nil {
					bumpLayerError()
					continue
				}
				mode := fi.Mode()

				// Symlinks: capture as 120000 candidate, never descend.
				if mode&os.ModeSymlink != 0 {
					if opts.matcher != nil && opts.matcher.Match(childRel) {
						continue
					}
					fileCands = append(fileCands, candidate{rel: childRel, full: childFull, fi: fi})
					continue
				}

				if fi.IsDir() {
					// Nested-repo / submodule: a directory containing .git
					// is a boundary we never cross.
					if _, err := os.Stat(filepath.Join(childFull, ".git")); err == nil {
						continue
					}
					if opts.submodules != nil && opts.submodules[childRel] {
						continue
					}
					if opts.matcher != nil && opts.matcher.MatchDirectory(childRel) {
						continue
					}
					nextDirs = append(nextDirs, dirEntry{rel: childRel, full: childFull})
					continue
				}

				// Regular files only — sockets/FIFOs/devices skipped quietly.
				if !mode.IsRegular() {
					continue
				}
				if opts.matcher != nil && opts.matcher.Match(childRel) {
					continue
				}
				fileCands = append(fileCands, candidate{rel: childRel, full: childFull, fi: fi})
			}
		}

		// Per-layer batched ignore check: classify the directory frontier
		// AND the file candidates collected at this layer in one pass so
		// ignored top-level subtrees never get read on the next iteration.
		// Batches are capped at ignoreCheckBatchSize paths to keep any
		// single check-ignore round-trip bounded; larger layers are sliced
		// into successive Check calls.
		if opts.ignoreChecker != nil && (len(nextDirs) > 0 || len(fileCands) > 0) {
			origDirCount := len(nextDirs)
			paths := make([]string, 0, origDirCount+len(fileCands))
			for _, e := range nextDirs {
				paths = append(paths, e.rel)
			}
			for _, c := range fileCands {
				paths = append(paths, c.rel)
			}
			results, ierr := classifyIgnoredBatched(ctx, opts.ignoreChecker, paths, ignoreCheckBatchSize)
			if ierr != nil {
				// Fail-closed: if check-ignore is busted, abort the pass
				// rather than silently committing files git considers
				// ignored.
				return nil, summary, fmt.Errorf("daemon: check-ignore: %w", ierr)
			}

			survivorDirs := make([]dirEntry, 0, len(nextDirs))
			for i, e := range nextDirs {
				if results[i] {
					continue
				}
				survivorDirs = append(survivorDirs, e)
			}
			nextDirs = survivorDirs

			survivorFiles := make([]candidate, 0, len(fileCands))
			for j, c := range fileCands {
				if results[origDirCount+j] {
					continue
				}
				survivorFiles = append(survivorFiles, c)
			}
			fileCands = survivorFiles
		}

		pending = append(pending, fileCands...)
		frontier = nextDirs
	}

	if err := ctx.Err(); err != nil {
		return nil, summary, err
	}

	for _, c := range pending {
		if err := ctx.Err(); err != nil {
			return nil, summary, err
		}
		summary.WalkedFiles++
		entry, ok, err := hashCandidate(ctx, repoRoot, c, opts)
		if err != nil {
			summary.Errors++
			continue
		}
		if !ok {
			summary.Oversize++
			continue
		}
		live[c.rel] = entry
	}

	return live, summary, nil
}

// hashCandidate hashes one candidate path into the git object store. For
// symlinks: read target bytes, hash with mode 120000. For regulars: open
// O_NOFOLLOW, verify ino+dev+mode unchanged across the open, enforce the
// size cap (recording oversize via daemon_meta), then hash via stdin.
//
// Returns:
//   - (entry, true,  nil) — captured ok.
//   - (zero,  false, nil) — skipped (oversize, vanished, type changed).
//   - (zero,  _,     err) — hard error worth recording in summary.
func hashCandidate(ctx context.Context, repoRoot string, c candidateLike, opts walkOpts) (LiveEntry, bool, error) {
	mode := c.fi.Mode()
	if mode&os.ModeSymlink != 0 {
		target, rerr := os.Readlink(c.full)
		if rerr != nil {
			return LiveEntry{}, false, rerr
		}
		oid, _, herr := git.HashSymlinkBlob(ctx, repoRoot, target)
		if herr != nil {
			return LiveEntry{}, false, herr
		}
		return LiveEntry{Path: c.rel, Mode: git.SymlinkMode, OID: oid}, true, nil
	}

	// Regular file: O_NOFOLLOW + verify ino/dev/mode (TOCTOU defense).
	flags := os.O_RDONLY | syscall.O_NOFOLLOW
	f, err := os.OpenFile(c.full, flags, 0)
	if err != nil {
		return LiveEntry{}, false, err
	}
	defer f.Close()

	post, err := f.Stat()
	if err != nil {
		return LiveEntry{}, false, err
	}
	if !sameFile(c.fi, post) {
		// Swapped between lstat and open — discard.
		return LiveEntry{}, false, nil
	}
	if !post.Mode().IsRegular() {
		return LiveEntry{}, false, nil
	}
	if post.Size() > opts.maxBytes {
		recordOversize(ctx, opts.db, c.rel, post.Size(), opts.maxBytes)
		return LiveEntry{}, false, nil
	}
	// Read up to maxBytes+1 to detect truncation/grow during read; if we
	// exceed, record oversize and discard.
	buf, err := io.ReadAll(f)
	if err != nil {
		return LiveEntry{}, false, err
	}
	if int64(len(buf)) > opts.maxBytes {
		recordOversize(ctx, opts.db, c.rel, int64(len(buf)), opts.maxBytes)
		return LiveEntry{}, false, nil
	}
	oid, herr := git.HashObjectStdin(ctx, repoRoot, buf)
	if herr != nil {
		return LiveEntry{}, false, herr
	}
	return LiveEntry{
		Path: c.rel,
		Mode: gitModeFor(post.Mode()),
		OID:  oid,
	}, true, nil
}

// candidateLike is the minimal shape hashCandidate needs. Aliasing the
// closure-captured candidate type keeps walkLive's pending slice unboxed.
type candidateLike = struct {
	rel  string
	full string
	fi   os.FileInfo
}

// sameFile compares ino+dev+mode-type to defend against symlink swaps and
// inode swaps between lstat and open. Mirrors the legacy
// _open_regular_file_safely check.
func sameFile(pre, post os.FileInfo) bool {
	preStat, ok1 := pre.Sys().(*syscall.Stat_t)
	postStat, ok2 := post.Sys().(*syscall.Stat_t)
	if !ok1 || !ok2 {
		// Cannot verify on this OS — best-effort: require type bits to match.
		return pre.Mode().Type() == post.Mode().Type()
	}
	if preStat.Ino != postStat.Ino || preStat.Dev != postStat.Dev {
		return false
	}
	if pre.Mode().Type() != post.Mode().Type() {
		return false
	}
	return true
}

// gitModeFor maps a Go fs.Mode onto a git tree mode for regular files.
// Symlinks are handled separately via SymlinkMode.
func gitModeFor(m os.FileMode) string {
	if m&0o111 != 0 {
		return git.ExecutableFileMode
	}
	return git.RegularFileMode
}

// recordOversize stores a daemon_meta breadcrumb so operators can see why a
// path was skipped without having to grep the daemon log. Best-effort:
// errors are dropped because the capture pipeline must keep running.
func recordOversize(ctx context.Context, db *state.DB, rel string, size, cap int64) {
	if db == nil {
		return
	}
	key := "capture-skip-large:" + rel
	val := fmt.Sprintf("size=%d>cap=%d", size, cap)
	_ = state.MetaSet(ctx, db, key, val)
}

func hasControlPathChar(rel string) bool {
	return strings.ContainsAny(rel, "\x00\t\n\r")
}

func recordInvalidPath(ctx context.Context, db *state.DB, rel, reason string) {
	if db == nil {
		return
	}
	key := "capture-skip-invalid-path:" + metaPathKey(rel)
	_ = state.MetaSet(ctx, db, key, "reason="+reason)
}

func metaPathKey(rel string) string {
	replacer := strings.NewReplacer(
		"\x00", "\\0",
		"\t", "\\t",
		"\n", "\\n",
		"\r", "\\r",
	)
	return replacer.Replace(rel)
}

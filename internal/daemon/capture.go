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
//   - Generated dependency/cache tree pruning via state.SafeIgnoreMatcher.
//   - Gitignored paths via batch git.IgnoreChecker.
//   - Oversize regulars (> ACD_MAX_FILE_BYTES, default 5 MiB) -> meta-only.
//   - Regular files opened with O_NOFOLLOW + post-open lstat/fstat
//     ino+dev+mode verification (TOCTOU defense against symlink swap).
package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
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

// EnvPathQuiescenceSeconds names the operator knob that defers planner offers
// for a captured path until that path has been quiet for the configured
// number of seconds. The capture row itself is still persisted to
// capture_events immediately for durability — the gate only changes WHEN the
// pending event becomes visible to the intent planner. Default 0 disables
// the gate so existing deployments behave unchanged. Restart the daemon for
// changes to apply.
const EnvPathQuiescenceSeconds = "ACD_PATH_QUIESCENCE_SECONDS"

// DefaultPathQuiescenceSeconds is the default ACD_PATH_QUIESCENCE_SECONDS
// when the env var is unset or unparseable. Zero preserves current
// behavior — the gate is opt-in.
const DefaultPathQuiescenceSeconds = 0

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
	// SafeIgnoreMatcher precomputes generated dependency/cache trees that
	// ACD skips internally even when they are not gitignored. Nil falls
	// back to a fresh matcher per pass.
	SafeIgnoreMatcher *state.SafeIgnoreMatcher
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
	// SortByPath, when true, reorders the slice returned by Classify into
	// lexicographic ascending Path order BEFORE the per-op
	// AppendCaptureEvent insert loop runs and BEFORE the pending-depth
	// cap is applied; events that overflow the cap mid-pass are
	// therefore the lex-largest paths, not the most recently edited.
	// Daemon run-loop callers leave this false; the live walk's iteration
	// order is preserved exactly as before. Used by tests and tooling
	// (`acd commit-all`) that need deterministic seq ordering across passes.
	SortByPath bool

	// DisablePendingCap, when true, disables the per-generation
	// pending-depth cap (EnvMaxPendingEvents) for THIS Capture call only.
	// Daemon run-loop callers leave this false so the documented
	// backpressure invariant holds. Single-shot tools like
	// `acd commit-all` set it true so a cold-start dirty worktree can
	// drain in one pass without the mid-walk drop fence kicking in.
	// Mutually exclusive with MaxPendingEventsOverride: when both are
	// set, DisablePendingCap wins. The process-wide env var is NOT
	// touched.
	DisablePendingCap bool

	// MaxPendingEventsOverride overrides the per-generation pending-depth
	// cap for this Capture call only when > 0. -1 (or any negative value)
	// is treated as unset and falls back to resolveMaxPendingEvents().
	// Zero falls through to the env-derived default; use
	// DisablePendingCap to actually turn the cap off. The process-wide
	// env var is NOT touched.
	MaxPendingEventsOverride int64
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

// resolveCaptureMaxPending honours the per-call CaptureOpts override
// chain ahead of the process-wide env var. DisablePendingCap forces the
// cap off; a strictly-positive MaxPendingEventsOverride wins next; any
// other case falls through to the documented env/default behavior in
// resolveMaxPendingEvents.
func resolveCaptureMaxPending(opts CaptureOpts) int64 {
	if opts.DisablePendingCap {
		return 0
	}
	if opts.MaxPendingEventsOverride > 0 {
		return opts.MaxPendingEventsOverride
	}
	return resolveMaxPendingEvents()
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
	pendingCap := resolveCaptureMaxPending(opts)
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
	safeIgnore := opts.SafeIgnoreMatcher
	if safeIgnore == nil {
		safeIgnore = state.NewSafeIgnoreMatcher()
	}
	maxBytes := resolveMaxFileBytes(opts.MaxFileBytes)

	live, protectedSkips, walkSummary, err := walkLive(ctx, repoRoot, walkOpts{
		matcher:       matcher,
		safeIgnore:    safeIgnore,
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
	protectedSkipCount := protectShadowFromSkippedPresent(ctx, repoRoot, db, cctx, shadow, protectedSkips)

	ops := Classify(shadow, live)
	if opts.SortByPath {
		// Reorder ops in lexicographic ascending Path order BEFORE the
		// per-op AppendCaptureEvent loop runs and BEFORE the
		// pending-depth cap is applied. If the cap drops a tail mid-pass
		// (see below), the events that overflow are therefore the
		// lex-LARGEST paths, not the most recently edited ones. Tooling
		// that wants newest-first drop semantics must leave SortByPath
		// false and rely on the live walk's iteration order.
		sort.SliceStable(ops, func(i, j int) bool { return ops[i].Path < ops[j].Path })
	}
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
			"protected":    protectedSkipCount,
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
		recordCapturedDecision(ctx, db, cctx, seq, op)
		// Stamp the per-path quiescence tracker AFTER the row durably
		// lands. The planner-offer gate keyed by ACD_PATH_QUIESCENCE_SECONDS
		// reads from this map; the capture row itself is unaffected by the
		// gate and is already pending in the FIFO. A rename also touches
		// the OldPath so a deferred rename source/dest pair stays paired
		// from the gate's perspective.
		nowQ := pathQuiescenceNow()
		RecordPathWrite(op.Path, nowQ)
		if op.OldPath != "" {
			RecordPathWrite(op.OldPath, nowQ)
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

func protectShadowFromSkippedPresent(ctx context.Context, repoRoot string, db *state.DB, cctx CaptureContext, shadow map[string]ShadowEntry, protected map[string]skippedPresent) int {
	if len(shadow) == 0 || len(protected) == 0 {
		return 0
	}
	index := newProtectedSkipIndex(protected)
	count := 0
	for path := range shadow {
		reason, ok, needsPresenceCheck := index.reasonForPath(path)
		if !ok {
			continue
		}
		if needsPresenceCheck && !shadowPathPresentOrIndeterminate(repoRoot, path) {
			continue
		}
		delete(shadow, path)
		count++
		recordProtectedSkipDecision(ctx, db, cctx, path, reason)
	}
	return count
}

func protectedReasonForPath(path string, protected map[string]skippedPresent) (string, bool) {
	reason, ok, _ := newProtectedSkipIndex(protected).reasonForPath(path)
	return reason, ok
}

type protectedSkipIndex struct {
	exact map[string]string
	dirs  []protectedDirPrefix
}

type protectedDirPrefix struct {
	path   string
	reason string
}

func newProtectedSkipIndex(protected map[string]skippedPresent) protectedSkipIndex {
	index := protectedSkipIndex{
		exact: make(map[string]string, len(protected)),
	}
	for path, skip := range protected {
		if path == "" || skip.Reason == "" {
			continue
		}
		if skip.Dir {
			index.dirs = append(index.dirs, protectedDirPrefix{path: path, reason: skip.Reason})
			continue
		}
		index.exact[path] = skip.Reason
	}
	sort.Slice(index.dirs, func(i, j int) bool {
		return len(index.dirs[i].path) > len(index.dirs[j].path)
	})
	return index
}

func (index protectedSkipIndex) reasonForPath(path string) (reason string, ok bool, needsPresenceCheck bool) {
	if reason, ok := index.exact[path]; ok {
		return reason, true, false
	}
	for _, prefix := range index.dirs {
		if path == prefix.path || strings.HasPrefix(path, prefix.path+"/") {
			return prefix.reason, true, true
		}
	}
	return "", false, false
}

func shadowPathPresentOrIndeterminate(repoRoot, path string) bool {
	_, err := os.Lstat(filepath.Join(repoRoot, filepath.FromSlash(path)))
	if err == nil {
		return true
	}
	return !errors.Is(err, os.ErrNotExist)
}

func recordProtectedSkipDecision(ctx context.Context, db *state.DB, cctx CaptureContext, path, reason string) {
	if db == nil || path == "" {
		return
	}
	kind := skippedDecisionKind(reason)
	if latest, err := state.DecisionsForPath(ctx, db, path, 1); err == nil && len(latest) == 1 {
		d := latest[0]
		if d.Kind == kind &&
			d.Reason.Valid && d.Reason.String == reason &&
			d.ActionTaken.Valid && d.ActionTaken.String == "no_delete_generated" &&
			d.HeadSHA.Valid && d.HeadSHA.String == cctx.BaseHead &&
			d.BranchRef.Valid && d.BranchRef.String == cctx.BranchRef &&
			d.BranchGeneration.Valid && d.BranchGeneration.Int64 == cctx.BranchGeneration {
			return
		}
	}
	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		Kind:             kind,
		Path:             sql.NullString{String: path, Valid: true},
		Reason:           sql.NullString{String: reason, Valid: reason != ""},
		HeadSHA:          sql.NullString{String: cctx.BaseHead, Valid: cctx.BaseHead != ""},
		BranchRef:        sql.NullString{String: cctx.BranchRef, Valid: cctx.BranchRef != ""},
		BranchGeneration: sql.NullInt64{Int64: cctx.BranchGeneration, Valid: true},
		ActionTaken:      sql.NullString{String: "no_delete_generated", Valid: true},
		UserMessage:      sql.NullString{String: fmt.Sprintf("Skipped present protected path %s without generating a delete.", path), Valid: true},
	}); err != nil {
		slog.Default().Warn("append capture protected decision", "path", path, "reason", reason, "err", err.Error())
	}
}

func skippedDecisionKind(reason string) string {
	switch reason {
	case "sensitive", "safe_ignore", "gitignore":
		return state.DecisionKindProtected
	default:
		return state.DecisionKindSkipped
	}
}

func recordCapturedDecision(ctx context.Context, db *state.DB, cctx CaptureContext, seq int64, op ClassifiedOp) {
	if db == nil || op.Path == "" {
		return
	}
	action := "queued"
	message := fmt.Sprintf("Captured %s for %s and queued it for replay.", op.Op, op.Path)
	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		Kind:             state.DecisionKindCaptured,
		Path:             sql.NullString{String: op.Path, Valid: true},
		Reason:           sql.NullString{String: op.Fidelity, Valid: op.Fidelity != ""},
		EventSeq:         sql.NullInt64{Int64: seq, Valid: seq > 0},
		HeadSHA:          sql.NullString{String: cctx.BaseHead, Valid: cctx.BaseHead != ""},
		BranchRef:        sql.NullString{String: cctx.BranchRef, Valid: cctx.BranchRef != ""},
		BranchGeneration: sql.NullInt64{Int64: cctx.BranchGeneration, Valid: true},
		ActionTaken:      sql.NullString{String: action, Valid: true},
		UserMessage:      sql.NullString{String: message, Valid: true},
	}); err != nil {
		slog.Default().Warn("append capture decision", "path", op.Path, "seq", seq, "err", err.Error())
	}
}

// walkOpts bundles inputs to walkLive so the function signature stays
// readable.
type walkOpts struct {
	matcher       *state.SensitiveMatcher
	safeIgnore    *state.SafeIgnoreMatcher
	ignoreChecker *git.IgnoreChecker
	submodules    map[string]bool
	maxBytes      int64
	db            *state.DB
}

type skippedPresent struct {
	Reason string
	Dir    bool
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
func walkLive(ctx context.Context, repoRoot string, opts walkOpts) (map[string]LiveEntry, map[string]skippedPresent, CaptureSummary, error) {
	live := map[string]LiveEntry{}
	protected := map[string]skippedPresent{}
	var summary CaptureSummary
	markProtected := func(rel, reason string, dir bool) {
		if rel == "" || reason == "" {
			return
		}
		protected[rel] = skippedPresent{Reason: reason, Dir: dir}
	}

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
			return nil, protected, summary, err
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
				markProtected(parent.rel, "unreadable", true)
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
					markProtected(childRel, "invalid_path", false)
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
					markProtected(childRel, "lstat_error", false)
					bumpLayerError()
					continue
				}
				mode := fi.Mode()

				// Symlinks: capture as 120000 candidate, never descend.
				if mode&os.ModeSymlink != 0 {
					if opts.matcher != nil && opts.matcher.Match(childRel) {
						markProtected(childRel, "sensitive", false)
						continue
					}
					if opts.safeIgnore != nil && opts.safeIgnore.MatchFile(childRel) {
						markProtected(childRel, "safe_ignore", false)
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
						markProtected(childRel, "sensitive", true)
						continue
					}
					if opts.safeIgnore != nil && opts.safeIgnore.MatchDirectory(childRel) {
						markProtected(childRel, "safe_ignore", true)
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
					markProtected(childRel, "sensitive", false)
					continue
				}
				if opts.safeIgnore != nil && opts.safeIgnore.MatchFile(childRel) {
					markProtected(childRel, "safe_ignore", false)
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
				return nil, protected, summary, fmt.Errorf("daemon: check-ignore: %w", ierr)
			}

			survivorDirs := make([]dirEntry, 0, len(nextDirs))
			for i, e := range nextDirs {
				if results[i] {
					markProtected(e.rel, "gitignore", true)
					continue
				}
				survivorDirs = append(survivorDirs, e)
			}
			nextDirs = survivorDirs

			survivorFiles := make([]candidate, 0, len(fileCands))
			for j, c := range fileCands {
				if results[origDirCount+j] {
					markProtected(c.rel, "gitignore", false)
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
		return nil, protected, summary, err
	}

	for _, c := range pending {
		if err := ctx.Err(); err != nil {
			return nil, protected, summary, err
		}
		summary.WalkedFiles++
		entry, ok, reason, err := hashCandidate(ctx, repoRoot, c, opts)
		if err != nil {
			markProtected(c.rel, "unreadable", false)
			summary.Errors++
			continue
		}
		if !ok {
			markProtected(c.rel, reason, false)
			summary.Oversize++
			continue
		}
		live[c.rel] = entry
	}

	return live, protected, summary, nil
}

// hashCandidate hashes one candidate path into the git object store. For
// symlinks: read target bytes, hash with mode 120000. For regulars: open
// O_NOFOLLOW, verify ino+dev+mode unchanged across the open, enforce the
// size cap (recording oversize via daemon_meta), then hash via stdin.
//
// Returns:
//   - (entry, true,  "",     nil) — captured ok.
//   - (zero,  false, reason, nil) — skipped (oversize, vanished, type changed).
//   - (zero,  _,     "",     err) — hard error worth recording in summary.
func hashCandidate(ctx context.Context, repoRoot string, c candidateLike, opts walkOpts) (LiveEntry, bool, string, error) {
	mode := c.fi.Mode()
	if mode&os.ModeSymlink != 0 {
		target, rerr := os.Readlink(c.full)
		if rerr != nil {
			return LiveEntry{}, false, "", rerr
		}
		oid, _, herr := git.HashSymlinkBlob(ctx, repoRoot, target)
		if herr != nil {
			return LiveEntry{}, false, "", herr
		}
		return LiveEntry{Path: c.rel, Mode: git.SymlinkMode, OID: oid}, true, "", nil
	}

	// Regular file: O_NOFOLLOW + verify ino/dev/mode (TOCTOU defense).
	flags := os.O_RDONLY | syscall.O_NOFOLLOW
	f, err := os.OpenFile(c.full, flags, 0)
	if err != nil {
		return LiveEntry{}, false, "", err
	}
	defer f.Close()

	post, err := f.Stat()
	if err != nil {
		return LiveEntry{}, false, "", err
	}
	if !sameFile(c.fi, post) {
		// Swapped between lstat and open — discard.
		return LiveEntry{}, false, "unstable", nil
	}
	if !post.Mode().IsRegular() {
		return LiveEntry{}, false, "non_regular", nil
	}
	if post.Size() > opts.maxBytes {
		recordOversize(ctx, opts.db, c.rel, post.Size(), opts.maxBytes)
		return LiveEntry{}, false, "oversize", nil
	}
	// Read up to maxBytes+1 to detect truncation/grow during read; if we
	// exceed, record oversize and discard.
	buf, err := io.ReadAll(f)
	if err != nil {
		return LiveEntry{}, false, "", err
	}
	if int64(len(buf)) > opts.maxBytes {
		recordOversize(ctx, opts.db, c.rel, int64(len(buf)), opts.maxBytes)
		return LiveEntry{}, false, "oversize", nil
	}
	oid, herr := git.HashObjectStdin(ctx, repoRoot, buf)
	if herr != nil {
		return LiveEntry{}, false, "", herr
	}
	return LiveEntry{
		Path: c.rel,
		Mode: gitModeFor(post.Mode()),
		OID:  oid,
	}, true, "", nil
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

// pathQuiescenceTracker tracks the most recent capture timestamp per path
// across the daemon's lifetime. The intent planner consults the tracker
// before deciding whether a captured path is "ready" to ship: a path that
// just received a write is held back until ACD_PATH_QUIESCENCE_SECONDS of
// silence has elapsed. The capture row itself is persisted immediately so
// durability is unchanged; only the planner-offer is gated.
//
// The map is intentionally process-local: persisting per-path timestamps to
// SQLite would add a write per capture event and cross-process status
// queries already see the gated counts via the `path_quiescence.*`
// daemon_meta keys we stamp at the end of each replay pass.
//
// pathQuiescenceEnabled is a hot-path gate: capture stamps a path on every
// captured op, so when ACD_PATH_QUIESCENCE_SECONDS is unset (the default)
// we want RecordPathWrite to return without acquiring the mutex. The
// daemon flips the gate via SetPathQuiescenceEnabled at startup once the
// env value is resolved; resolvePathQuiescenceSeconds also flips it on
// when it parses a positive value so a test that wires only the env var
// (no daemon startup) still exercises the tracker.
//
// pathQuiescenceMaxEntries bounds the map at ~4096 entries. When the
// limit is exceeded, RecordPathWrite evicts entries older than 2x the
// current quiescence window (or 1 hour when quiescence is zero) under the
// already-held write lock. Worst-case memory: ~4096 entries * 256B/entry
// (path + time.Time + map overhead) ≈ 1 MiB.
//
// Tests can substitute the wall clock via SetPathQuiescenceClockForTest and
// reset the tracker via ResetPathQuiescenceForTest.
const pathQuiescenceMaxEntries = 4096

var (
	pathQuiescenceMu        sync.RWMutex
	pathQuiescenceWrites    = map[string]time.Time{}
	pathQuiescenceNowFn     atomic.Pointer[func() time.Time]
	pathQuiescenceEnabled   atomic.Bool
	pathQuiescenceWindowSec atomic.Int64 // current ACD_PATH_QUIESCENCE_SECONDS for eviction
)

func pathQuiescenceNow() time.Time {
	if fn := pathQuiescenceNowFn.Load(); fn != nil && *fn != nil {
		return (*fn)()
	}
	return time.Now()
}

// SetPathQuiescenceClockForTest swaps the clock the tracker consults. Pass
// nil to restore time.Now. Test-only.
func SetPathQuiescenceClockForTest(t interface{ Helper() }, fn func() time.Time) {
	t.Helper()
	if fn == nil {
		pathQuiescenceNowFn.Store(nil)
		return
	}
	cp := fn
	pathQuiescenceNowFn.Store(&cp)
}

// ResetPathQuiescenceForTest empties the tracker so adjacent tests do not
// share path state. Test-only. Also resets the enable gate so the next
// test starts from a known-disabled state.
func ResetPathQuiescenceForTest(t interface{ Helper() }) {
	t.Helper()
	pathQuiescenceMu.Lock()
	pathQuiescenceWrites = map[string]time.Time{}
	pathQuiescenceMu.Unlock()
	pathQuiescenceEnabled.Store(false)
	pathQuiescenceWindowSec.Store(0)
}

// SetPathQuiescenceEnabled flips the hot-path gate. The daemon calls this
// at startup once it has resolved ACD_PATH_QUIESCENCE_SECONDS so capture
// can short-circuit RecordPathWrite when the gate is off. Idempotent.
func SetPathQuiescenceEnabled(enabled bool) {
	pathQuiescenceEnabled.Store(enabled)
}

// PathQuiescenceTrackerSize returns the current number of tracked paths.
// Surface for diagnose/observability and tests.
func PathQuiescenceTrackerSize() int {
	pathQuiescenceMu.RLock()
	n := len(pathQuiescenceWrites)
	pathQuiescenceMu.RUnlock()
	return n
}

// RecordPathWrite stamps `now` as the most recent capture timestamp for
// `path`. Empty paths are ignored. Concurrent capture passes serialize on
// the tracker's write lock. Exported so callers outside this package
// (currently none, but the intent-planner replay loop reads it) can keep
// the API discoverable.
//
// Callers MUST pass repo-relative slash-separated paths matching the
// canonicalization used for capture_ops.path (see touchedPaths). Defensive
// normalization here would mask caller bugs; we trust the producer.
//
// Hot-path short-circuit: when SetPathQuiescenceEnabled has not been
// flipped on (the default for ACD_PATH_QUIESCENCE_SECONDS=0 deployments),
// RecordPathWrite returns immediately without acquiring the mutex. Callers
// in capture.go run this on every captured op, so the cheap atomic Load
// keeps the no-gate steady state at the same cost as the prior code path.
func RecordPathWrite(path string, now time.Time) {
	if path == "" {
		return
	}
	if !pathQuiescenceEnabled.Load() {
		return
	}
	pathQuiescenceMu.Lock()
	pathQuiescenceWrites[path] = now
	if len(pathQuiescenceWrites) > pathQuiescenceMaxEntries {
		evictStalePathQuiescenceLocked(now)
	}
	pathQuiescenceMu.Unlock()
}

// evictStalePathQuiescenceLocked prunes tracker entries older than 2x the
// configured quiescence window (or 1 hour when the window is zero). Caller
// must hold pathQuiescenceMu in write mode.
func evictStalePathQuiescenceLocked(now time.Time) {
	windowSec := pathQuiescenceWindowSec.Load()
	cutoff := time.Hour
	if windowSec > 0 {
		cutoff = 2 * time.Duration(windowSec) * time.Second
	}
	threshold := now.Add(-cutoff)
	for k, v := range pathQuiescenceWrites {
		if v.Before(threshold) {
			delete(pathQuiescenceWrites, k)
		}
	}
	// If eviction was insufficient (every entry is fresh), drop the
	// oldest 25% to keep the cap honored. This only fires under
	// pathological churn where 4k distinct paths are stamped within the
	// quiescence window — practically: nothing falls out of the eviction
	// branch under normal load.
	if len(pathQuiescenceWrites) > pathQuiescenceMaxEntries {
		type kv struct {
			k string
			t time.Time
		}
		entries := make([]kv, 0, len(pathQuiescenceWrites))
		for k, v := range pathQuiescenceWrites {
			entries = append(entries, kv{k: k, t: v})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].t.Before(entries[j].t) })
		drop := len(entries) / 4
		if drop < 1 {
			drop = 1
		}
		for i := 0; i < drop; i++ {
			delete(pathQuiescenceWrites, entries[i].k)
		}
	}
}

// PathLastWrite returns the most recent recorded write timestamp for `path`,
// and `ok=false` when no capture has stamped the path during this daemon
// lifetime. Used by the planner-offer gate to compute quiescence.
func PathLastWrite(path string) (time.Time, bool) {
	if path == "" {
		return time.Time{}, false
	}
	pathQuiescenceMu.RLock()
	ts, ok := pathQuiescenceWrites[path]
	pathQuiescenceMu.RUnlock()
	return ts, ok
}

// IsPathQuiescent returns true when `path` is eligible to be offered to the
// planner under a `quiescence` gate. Quiescence == 0 short-circuits to true
// (gate disabled). When the path has no recorded write (e.g. captured by a
// previous daemon run before restart) we treat it as quiescent so a stale
// pending row never gets stranded behind a tracker miss.
func IsPathQuiescent(path string, quiescence time.Duration, now time.Time) bool {
	if quiescence <= 0 || path == "" {
		return true
	}
	last, ok := PathLastWrite(path)
	if !ok {
		return true
	}
	return now.Sub(last) >= quiescence
}

// resolvePathQuiescenceSeconds parses ACD_PATH_QUIESCENCE_SECONDS into a
// time.Duration. Negative or unparseable values fall back to the default
// (zero, i.e. gate disabled).
func resolvePathQuiescenceSeconds() time.Duration {
	env := os.Getenv(EnvPathQuiescenceSeconds)
	if env == "" {
		return time.Duration(DefaultPathQuiescenceSeconds) * time.Second
	}
	n, err := strconv.Atoi(env)
	if err != nil || n < 0 {
		return time.Duration(DefaultPathQuiescenceSeconds) * time.Second
	}
	return time.Duration(n) * time.Second
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

// Package daemon implements the long-running per-repo capture+replay loop.
//
// The exported entry point is Run, which composes all the Phase 1 building
// blocks (capture, replay, refcount, prune, lock, signals, scheduler) into
// the loop body §8.1 specifies.
//
// Run is single-goroutine: every per-tick mutation happens on the run-loop
// goroutine. Signals dispatch via os/signal in a small helper goroutine but
// only push notifications onto buffered channels — the loop itself reads
// those channels and never holds shared state outside its own stack.
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
	"runtime/debug"
	"strconv"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

// Default knobs the run loop uses when Options leaves them zero.
const (
	// DefaultClientSweepInterval matches the legacy daemon's
	// CLIENT_SWEEP_INTERVAL_SECONDS — sweep refcount roughly every 5
	// seconds. Cheap operation.
	DefaultClientSweepInterval = 5 * time.Second
	// DefaultPruneInterval matches the legacy PRUNE_INTERVAL_SECONDS —
	// run the capture_events pruner roughly once per minute.
	DefaultPruneInterval = 60 * time.Second
	// DefaultRollupInterval is the minimum gap between RunDailyRollup
	// attempts. The aggregator is also forced once per UTC-day boundary
	// crossing regardless of this floor.
	DefaultRollupInterval = 5 * time.Minute
	// DefaultFlushLimit caps how many flush_requests are drained per
	// run-loop iteration. A bursty enqueue (1500+ rows) must not starve
	// other Run-loop work, and the inner drain must remain context-
	// cancelable. Tests can override Options.FlushLimit (e.g. 1) for
	// tighter control.
	DefaultFlushLimit = 256
	// OrphanFlushAckThreshold is how long a flush_request may stay in the
	// "acknowledged" state before the daemon's startup sweep marks it
	// "failed". Acknowledged-but-never-completed rows are an orphan from a
	// prior daemon crash between ClaimNextFlushRequest and
	// CompleteFlushRequest. Sweeping them at startup keeps `acd status` /
	// queue depth metrics from accumulating ghosts forever.
	OrphanFlushAckThreshold = 5 * time.Minute
)

// EnvClientTTLSeconds is the environment knob for ACD_CLIENT_TTL_SECONDS
// (D21). The default is DefaultClientTTL (30 minutes).
const EnvClientTTLSeconds = "ACD_CLIENT_TTL_SECONDS"

// Options configures one Run invocation.
//
// Required: RepoPath, GitDir, DB. Everything else has a usable default.
type Options struct {
	// RepoPath is the absolute path to the worktree root.
	RepoPath string
	// GitDir is the absolute .git directory.
	GitDir string
	// DB is the already-open per-repo state database. Run does NOT close
	// the DB on exit — caller owns the lifetime.
	DB *state.DB

	// Logger emits all run-loop progress. Nil falls back to slog.Default().
	Logger *slog.Logger

	// Scheduler is the backoff helper. Zero-valued struct = production
	// defaults; tests pass a Scheduler with smaller bases/ceilings to keep
	// the suite fast.
	Scheduler Scheduler

	// BootGrace is the post-start window during which empty refcount
	// sweeps do not count toward self-termination. Zero falls back to
	// DefaultBootGrace.
	BootGrace time.Duration

	// EventRetention overrides the capture_events retention window. Zero
	// falls back to DefaultEventRetention (with EnvEventRetentionDays
	// honored).
	EventRetention time.Duration

	// ClientTTL overrides the daemon_clients TTL. Zero falls back to
	// DefaultClientTTL (or EnvClientTTLSeconds if set).
	ClientTTL time.Duration

	// EmptySweepThreshold overrides the consecutive-empty-sweeps gate.
	// Zero falls back to DefaultEmptySweepThreshold.
	EmptySweepThreshold int

	// ClientSweepInterval throttles refcount sweeps. Zero falls back to
	// DefaultClientSweepInterval.
	ClientSweepInterval time.Duration

	// PruneInterval throttles the capture_events pruner. Zero falls back
	// to DefaultPruneInterval.
	PruneInterval time.Duration

	// RollupInterval caps how often the daily rollup hook may run. Zero
	// falls back to DefaultRollupInterval. The aggregator is also fired
	// immediately when a UTC-day boundary crossing is detected.
	RollupInterval time.Duration

	// CentralStatsDBPath, when non-empty, opens the central stats.db at
	// daemon start and pushes per-repo daily_rollups into it after each
	// rollup pass. Empty means "skip central push" — only the per-repo
	// daily_rollups table is updated. Tests typically leave this empty.
	CentralStatsDBPath string

	// CentralStats, when non-nil, is used as the central stats handle
	// instead of opening one from CentralStatsDBPath. Tests inject a
	// pre-opened *central.StatsDB this way to avoid filesystem coupling.
	CentralStats *central.StatsDB

	// RepoHash is the stable cross-repo identifier used when pushing
	// per-repo daily_rollups into the central stats.db. Empty disables
	// the central push (logged but non-fatal).
	RepoHash string

	// MessageFn produces commit messages. Nil falls back to a MessageFn
	// derived from MessageProvider (or, when MessageProvider is also nil,
	// from ai.BuildProvider(ai.LoadProviderConfigFromEnv())). Tests may
	// pin a deterministic MessageFn here directly without involving the
	// ai package at all.
	MessageFn MessageFn

	// MessageProvider, when non-nil, is the ai.Provider used to compose
	// commit messages on the replay path. Nil triggers env-driven
	// selection via ai.LoadProviderConfigFromEnv + ai.BuildProvider —
	// production callers leave this nil and rely on ACD_AI_*. Tests can
	// inject a stub Provider to assert the message reaches the commit.
	MessageProvider ai.Provider

	// MessageProviderCloser, when non-nil, is closed on Run shutdown.
	// Pair this with MessageProvider when the provider holds OS
	// resources (currently only ai.SubprocessProvider). When Run
	// constructs the provider itself from env vars, the closer returned
	// by ai.BuildProvider is captured automatically.
	MessageProviderCloser io.Closer

	// Now lets tests inject a fake clock. Nil falls back to time.Now.
	Now func() time.Time

	// WakeCh is an optional injection point so tests can trigger wakes
	// without sending real OS signals. Production callers leave this nil
	// and the loop relies on InstallSignalHandlers' SIGUSR1 channel.
	WakeCh <-chan struct{}

	// ShutdownCh is the test-side equivalent for SIGTERM/SIGINT. Nil
	// falls back to InstallSignalHandlers' shutdown channel.
	ShutdownCh <-chan struct{}

	// SkipSignals disables the real os/signal registration. Tests that
	// inject WakeCh / ShutdownCh set this to true so the test goroutine
	// has full control over wake + shutdown.
	SkipSignals bool

	// FlushLimit caps how many flush_requests are drained per iteration.
	// Zero falls back to DefaultFlushLimit (256). Tests set it to 1 for
	// tighter control.
	FlushLimit int

	// FsnotifyEnabled turns on the recursive fsnotify watcher (D11 hybrid).
	// Default is false so the existing test suite keeps deterministic
	// poll-only timing; production callers (and the integration test) opt
	// in by setting this true. Even when true, ACD_DISABLE_FSNOTIFY=1
	// forces poll-only mode at watcher construction time.
	FsnotifyEnabled bool

	// FsnotifyDebounce overrides the trailing-edge debounce on fsnotify
	// wakes. Zero falls back to DefaultDebounce.
	FsnotifyDebounce time.Duration

	// FsnotifyMaxWatches caps the OS watch budget. Zero asks the watcher
	// to derive a sensible default from the platform.
	FsnotifyMaxWatches int

	// Trace receives best-effort decision records. Nil uses ACD_TRACE env
	// wiring; disabled env returns a no-op logger.
	Trace acdtrace.Logger
}

// resolveClientTTL honors EnvClientTTLSeconds + opt.
func resolveClientTTL(opt time.Duration) time.Duration {
	if opt > 0 {
		return opt
	}
	if env := os.Getenv(EnvClientTTLSeconds); env != "" {
		if secs, err := strconv.ParseFloat(env, 64); err == nil && secs > 0 {
			return time.Duration(secs * float64(time.Second))
		}
	}
	return DefaultClientTTL
}

// Run executes the per-repo daemon run loop. Returns nil on graceful
// shutdown (SIGTERM/SIGINT, ctx.Done, self-terminate). Returns
// ErrDaemonLockHeld when another daemon already owns daemon.lock — the
// caller should map this onto exit ExitTempFail (75).
//
// Run does NOT close opts.DB; the caller owns the database lifetime.
func Run(ctx context.Context, opts Options) error {
	if opts.RepoPath == "" {
		return fmt.Errorf("daemon: Run: empty RepoPath")
	}
	if opts.GitDir == "" {
		return fmt.Errorf("daemon: Run: empty GitDir")
	}
	if opts.DB == nil {
		return fmt.Errorf("daemon: Run: nil DB")
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// Top-level panic recover. The daemon owns long-lived resources whose
	// cleanup runs through subsequent defers (IgnoreChecker subprocess,
	// fsnotify watcher, central stats DB, AI provider closer, trace
	// writer). An unrecovered panic would skip those defers entirely and
	// leak the check-ignore subprocess; recovering here lets the deferred
	// Close calls run before we re-panic so the operator sees the original
	// trace and the orphan process is reaped.
	defer func() {
		if r := recover(); r != nil {
			logger.Error("daemon panic; deferred cleanup will run before re-raise",
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()))
			// Re-raise so the harness/test runner observes the failure.
			panic(r)
		}
	}()
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	tracer := opts.Trace
	if tracer == nil {
		tracer = acdtrace.FromEnv(opts.RepoPath, opts.GitDir)
	}
	defer func() {
		if err := tracer.Close(); err != nil {
			logger.Warn("close trace writer", "err", err.Error())
		}
	}()
	// MessageFn precedence: explicit MessageFn > injected MessageProvider
	// > env-driven ai.BuildProvider > deterministic. The closer returned
	// by ai.BuildProvider (only non-nil for subprocess plugins) is owned
	// by Run and Closed on graceful shutdown.
	var providerCloser io.Closer
	closeProviderOnce := func() {
		if providerCloser != nil {
			if err := providerCloser.Close(); err != nil {
				logger.Warn("close ai provider", "err", err.Error())
			}
			providerCloser = nil
		}
	}
	defer closeProviderOnce()

	if _, ok := os.LookupEnv("ACD_AI_SEND_DIFF"); ok {
		logger.Warn("ACD_AI_SEND_DIFF is deprecated and ignored; diff egress is now controlled by ACD_AI_PROVIDER",
			slog.String("env", "ACD_AI_SEND_DIFF"))
	}

	msgFn := opts.MessageFn
	if msgFn == nil {
		provider := opts.MessageProvider
		if provider == nil {
			cfg := ai.LoadProviderConfigFromEnv()
			cfg.Logger = logger
			built, closer, err := ai.BuildProvider(cfg)
			if err != nil {
				logger.Warn("build ai provider; falling back to deterministic",
					"err", err.Error())
				provider = ai.DeterministicProvider{}
			} else {
				provider = built
				providerCloser = closer
			}
			logger.Info("ai provider selected",
				"provider", provider.Name(),
				"mode", cfg.Mode)
		} else if opts.MessageProviderCloser != nil {
			providerCloser = opts.MessageProviderCloser
		}
		msgFn = providerMessageFn(provider, opts.RepoPath)
	}
	bootGrace := opts.BootGrace
	if bootGrace <= 0 {
		bootGrace = DefaultBootGrace
	}
	clientSweepEvery := opts.ClientSweepInterval
	if clientSweepEvery <= 0 {
		clientSweepEvery = DefaultClientSweepInterval
	}
	pruneEvery := opts.PruneInterval
	if pruneEvery <= 0 {
		pruneEvery = DefaultPruneInterval
	}
	rollupEvery := opts.RollupInterval
	if rollupEvery <= 0 {
		rollupEvery = DefaultRollupInterval
	}
	emptyThreshold := opts.EmptySweepThreshold
	if emptyThreshold <= 0 {
		emptyThreshold = DefaultEmptySweepThreshold
	}
	clientTTL := resolveClientTTL(opts.ClientTTL)
	eventRetention := opts.EventRetention // resolved inside PruneCaptureEvents

	// 1. Acquire daemon.lock. Sentinel returned on contention.
	dlock, err := AcquireDaemonLock(opts.GitDir)
	if err != nil {
		if errors.Is(err, ErrDaemonLockHeld) {
			logger.Warn("daemon.lock contended; another daemon is alive",
				"git_dir", opts.GitDir)
			return err
		}
		return fmt.Errorf("daemon: acquire daemon.lock: %w", err)
	}
	defer func() { _ = dlock.Release() }()

	pid := os.Getpid()
	bootTime := now()

	// 2. Stamp daemon_state.mode = "running" + identity.
	//
	// Use identity.Capture(pid) — the ps-form hash — so the persisted
	// fingerprint is byte-symmetric with what `acd stop` / `acd wake`
	// recompute when verifying the pid before delivering a signal.
	// identity.CaptureSelf() hashes the unjoined os.Args, which is more
	// precise but cannot be reproduced by an external observer reading
	// `ps`, so a CaptureSelf-stamped fingerprint always mismatches at
	// verify time and signalProcess silently swallows SIGTERM/SIGKILL.
	fp, fpErr := identity.Capture(pid)
	var fpToken string
	if fpErr == nil {
		fpToken = FingerprintToken(fp)
	}
	heartbeatNow := func(mode, note string) {
		ts := float64(now().UnixNano()) / 1e9
		st := state.DaemonState{
			PID:         pid,
			Mode:        mode,
			HeartbeatTS: ts,
			UpdatedTS:   ts,
		}
		if note != "" {
			st.Note = sql.NullString{String: note, Valid: true}
		}
		if fpToken != "" {
			st.DaemonFingerprint = sql.NullString{String: fpToken, Valid: true}
		}
		if err := state.SaveDaemonState(ctx, opts.DB, st); err != nil {
			logger.Warn("save daemon_state", "err", err.Error())
		}
	}
	heartbeatNow("running", "daemon started")

	// 3. Install signal handlers (unless tests opt out).
	var sig *Signals
	var sigCleanup func()
	if !opts.SkipSignals {
		sig, sigCleanup = InstallSignalHandlers(ctx)
		defer sigCleanup()
	}
	wakeCh := opts.WakeCh
	if wakeCh == nil && sig != nil {
		wakeCh = sig.Wake
	}
	shutdownCh := opts.ShutdownCh
	if shutdownCh == nil && sig != nil {
		shutdownCh = sig.Shutdown
	}

	// Resolve the active branch ref / generation up-front. The generation
	// counter is loaded from daemon_meta so a daemon restart preserves the
	// last-known value — otherwise queued events captured under generation
	// N would look fresh against an in-memory seed of 1, defeating the
	// stale-event guard at replay time. The first daemon run on a new
	// repo gets the legacy default (1).
	branchRef, headOID := resolveBranch(ctx, opts.RepoPath, logger)
	persistedGen, err := LoadBranchGeneration(ctx, opts.DB)
	if err != nil {
		logger.Warn("load persisted branch generation", "err", err.Error())
		persistedGen = 1
	}
	persistedHead, err := LoadBranchHead(ctx, opts.DB)
	if err != nil {
		logger.Warn("load persisted branch head", "err", err.Error())
	}
	currentToken, terr := BranchGenerationToken(ctx, opts.RepoPath)
	if terr != nil {
		logger.Warn("seed branch token", "err", terr.Error())
		currentToken = ""
	}
	branchTransitionBlocked := false
	if persistedHead != "" && currentToken != "" {
		prevToken := "rev:" + persistedHead
		if persistedToken, ok, err := state.MetaGet(ctx, opts.DB, MetaKeyBranchToken); err != nil {
			logger.Warn("load persisted branch token", "err", err.Error())
		} else if ok && persistedToken != "" {
			prevToken = persistedToken
		}
		transition, cErr := ClassifyTokenTransition(ctx, opts.RepoPath, prevToken, currentToken)
		if cErr != nil {
			logger.Warn("classify startup branch transition; will retry",
				"err", cErr.Error())
			currentToken = prevToken
			branchTransitionBlocked = true
		} else if transition == TokenTransitionDiverged {
			prevGeneration := persistedGen
			rewindPaused, rewindUntil, rewindErr := maybeSetRewindGrace(ctx, opts.RepoPath, opts.DB, prevToken, currentToken, now())
			if rewindErr != nil {
				logger.Warn("detect startup rewind grace", "err", rewindErr.Error())
			} else if rewindPaused {
				logger.Info("replay paused after startup branch rewind", "until", rewindUntil)
			}
			persistedGen++
			droppedPending, dropErr := state.DeletePendingForGeneration(ctx, opts.DB, prevGeneration)
			if dropErr != nil {
				logger.Warn("drop pending events for previous branch generation at startup",
					"generation", prevGeneration, "err", dropErr.Error())
			}
			ts := strconv.FormatFloat(float64(now().UnixNano())/1e9, 'f', -1, 64)
			_ = state.MetaSet(ctx, opts.DB, MetaKeyBranchTokenChangedAt, ts)
			logger.Info("branch generation bumped at startup",
				"old", prevToken, "new", currentToken,
				"generation", persistedGen,
				"transition", transition.String())
			recordTrace(tracer, acdtrace.Event{
				Repo:       opts.RepoPath,
				BranchRef:  branchRef,
				HeadSHA:    headOID,
				EventClass: "branch_token.transition",
				Decision:   transition.String(),
				Reason:     "startup token transition classified",
				Input:      map[string]any{"previous": prevToken, "current": currentToken},
				Output: map[string]any{
					"prev_generation": prevGeneration,
					"new_generation":  persistedGen,
					"dropped_pending": droppedPending,
				},
				Error:      traceErrString(dropErr),
				Generation: persistedGen,
			})
		}
	}
	cctx := CaptureContext{
		BranchRef:        branchRef,
		BranchGeneration: persistedGen,
		BaseHead:         headOID,
	}
	// Persist the seed observation so the next divergence has a baseline
	// to compare against. Best-effort — log but do not fail on write.
	if err := SaveBranchGeneration(ctx, opts.DB, cctx.BranchGeneration, headOID); err != nil {
		logger.Warn("seed branch generation", "err", err.Error())
	}
	if !branchTransitionBlocked {
		if err := state.MetaSet(ctx, opts.DB, MetaKeyBranchToken, currentToken); err != nil {
			logger.Warn("seed branch token", "err", err.Error())
		}
	}

	// Seed shadow_paths from HEAD before the first capture so files
	// already at HEAD don't generate spurious creates.
	if cctx.BranchRef != "" {
		if _, ok, _ := state.MetaGet(ctx, opts.DB, MetaKeyDetachedHeadPaused); ok {
			_, _ = state.MetaDelete(ctx, opts.DB, MetaKeyDetachedHeadPaused)
		}
	}
	if cctx.BranchRef != "" && cctx.BaseHead != "" {
		if seeded, err := BootstrapShadow(ctx, opts.RepoPath, opts.DB, cctx); err != nil {
			logger.Warn("bootstrap shadow", "err", err.Error())
			traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", err.Error(), 0)
		} else {
			traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), "startup shadow bootstrap", seeded)
			if seeded > 0 {
				logger.Info("shadow bootstrapped", "rows", seeded)
			}
			if pruned, pErr := pruneShadowGenerations(ctx, opts.DB, cctx); pErr != nil {
				logger.Warn("prune old shadow generations", "err", pErr.Error())
			} else if pruned > 0 {
				logger.Info("pruned old shadow generations", "rows", pruned)
			}
		}
	}

	ignoreChecker := git.NewIgnoreChecker(opts.RepoPath)
	defer func() { _ = ignoreChecker.Close() }()
	matcher := state.NewSensitiveMatcher()

	// 3a. fsnotify watcher (D11 hybrid). Disabled by default so existing
	// poll-only tests stay deterministic; the run loop subscribes to a
	// dedicated wake channel that the watcher drives via WakeFn.
	var (
		fsWatcher    *FsnotifyWatcher
		fsWakeCh     chan struct{}
		fsWakeReader <-chan struct{} // nil-when-disabled receive view
	)
	if opts.FsnotifyEnabled {
		fsWakeCh = make(chan struct{}, 1)
		fsWakeReader = fsWakeCh
		wakeFn := func() {
			select {
			case fsWakeCh <- struct{}{}:
			default:
				// channel already full — wake is coalesced.
			}
		}
		diagFn := func(d WatcherDiagnostics) {
			_ = state.MetaSet(ctx, opts.DB, "fsnotify.mode", d.Mode)
			_ = state.MetaSet(ctx, opts.DB, "fsnotify.watch_count",
				strconv.Itoa(d.WatchCount))
			_ = state.MetaSet(ctx, opts.DB, "fsnotify.dropped_events",
				strconv.Itoa(d.DroppedEvents))
			_ = state.MetaSet(ctx, opts.DB, "fsnotify.fallback_reason",
				d.FallbackReason)
		}
		w, err := NewFsnotifyWatcher(FsnotifyOptions{
			RepoPath:      opts.RepoPath,
			GitDir:        opts.GitDir,
			IgnoreChecker: ignoreChecker,
			Sensitive:     matcher,
			Debounce:      opts.FsnotifyDebounce,
			MaxWatches:    opts.FsnotifyMaxWatches,
			WakeFn:        wakeFn,
			Logger:        logger,
			DiagnosticsFn: diagFn,
		})
		if err != nil {
			logger.Warn("fsnotify watcher init failed; running poll-only",
				"err", err.Error())
		} else {
			fsWatcher = w
			if startErr := fsWatcher.Start(ctx); startErr != nil {
				logger.Warn("fsnotify watcher start failed",
					"err", startErr.Error())
			}
		}
	}
	defer func() {
		if fsWatcher != nil {
			_ = fsWatcher.Stop()
		}
	}()

	// Central stats handle for the rollup hook (§8.10). Caller may
	// inject a pre-opened *central.StatsDB via opts.CentralStats; if not
	// and CentralStatsDBPath is set, open one here and own its lifetime.
	// Empty / nil = skip central push (per-repo aggregation still runs).
	statsDB := opts.CentralStats
	closeStats := false
	if statsDB == nil && opts.CentralStatsDBPath != "" {
		s, sErr := openCentralStats(ctx, opts.CentralStatsDBPath)
		if sErr != nil {
			logger.Warn("open central stats db", "err", sErr.Error(),
				"path", opts.CentralStatsDBPath)
		} else {
			statsDB = s
			closeStats = true
		}
	}
	defer func() {
		if closeStats && statsDB != nil {
			_ = statsDB.Close()
		}
	}()

	// Loop state.
	var (
		consecutiveErrors int
		emptyCount        int
		currentDelay      = opts.Scheduler.Reset()
		lastSweep         = time.Time{}
		lastPrune         = time.Time{}
		lastRollup        = time.Time{}
		lastRollupUTCDay  = ""
		stopped           bool

		// operation_in_progress staleness tracking. opMarkerSetAt is the
		// monotonic-ish wall-clock observation of when the current marker
		// first appeared (in this process). opMarkerHead is the HEAD SHA at
		// that point. Both reset to zero/empty when the marker disappears.
		// opMarkerWarnedAt rate-limits the "marker may be stale" warning.
		opMarkerSetAt    time.Time
		opMarkerHead     string
		opMarkerWarnedAt time.Time
	)
	const (
		// staleOpMarkerThreshold: how long an operation_in_progress marker
		// must stay present (with HEAD motionless) before we surface a
		// "marker may be stale" warning.
		staleOpMarkerThreshold = 15 * time.Minute
		// staleOpMarkerWarnInterval: throttle for the periodic warning.
		staleOpMarkerWarnInterval = 5 * time.Minute
	)

	graceful := func(reason string) {
		stopped = true
		st := state.DaemonState{
			PID:         pid,
			Mode:        "stopped",
			HeartbeatTS: float64(now().UnixNano()) / 1e9,
			UpdatedTS:   float64(now().UnixNano()) / 1e9,
			Note:        sql.NullString{String: reason, Valid: true},
		}
		if fpToken != "" {
			st.DaemonFingerprint = sql.NullString{String: fpToken, Valid: true}
		}
		// Use a fresh background context with a short timeout — the
		// run-loop's ctx is already canceled in the most common path
		// (ctx.Done shutdown). We must still stamp mode=stopped so
		// controllers can see the daemon left cleanly.
		shutdownCtx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		if err := state.SaveDaemonState(shutdownCtx, opts.DB, st); err != nil {
			logger.Warn("stamp stopped state", "err", err.Error())
		}
		logger.Info("daemon stopping", "reason", reason)
	}

	logger.Info("daemon running",
		"repo", opts.RepoPath, "pid", pid, "branch", branchRef,
		"head", headOID, "token", currentToken)

	processBranchTokenChange := func(logPrefix string) bool {
		newToken, terr := BranchGenerationToken(ctx, opts.RepoPath)
		if terr != nil {
			logger.Warn(logPrefix+" resolve failed", "err", terr.Error())
			return false
		}
		if SameGeneration(currentToken, newToken) {
			// Cross-tick same-SHA rewind probe.
			//
			// `git reset --hard HEAD~1` followed by `git reset --hard ORIG_HEAD`
			// between two daemon ticks leaves the token byte-identical, so the
			// SameGeneration short-circuit hides a real rewind from
			// maybeSetRewindGrace and capture would otherwise enqueue any
			// transient worktree changes the operator just rewound.
			//
			// We persist the live HEAD on every tick (see the unconditional
			// stamp at the bottom of this function). If the persisted HEAD
			// differs from BOTH the in-memory token's SHA and the freshly-read
			// live HEAD, an out-of-band observer recorded a different HEAD
			// between ticks. Probe ancestry: when the live HEAD is an ancestor
			// of the persisted (i.e. backward), classify as a same-SHA rewind
			// and set the grace gate just like the explicit divergence path.
			liveHead := tokenSHA(newToken)
			tokenHead := tokenSHA(currentToken)
			liveBranchRef := tokenBranchRef(newToken)
			if liveHead != "" && tokenHead != "" && liveBranchRef != "" {
				persistedHead, lhErr := LoadBranchHead(ctx, opts.DB)
				if lhErr != nil {
					logger.Warn(logPrefix+" load persisted head for cross-tick probe",
						"err", lhErr.Error())
				} else if persistedHead != "" &&
					persistedHead != tokenHead &&
					persistedHead != liveHead {
					ok, aErr := git.IsAncestor(ctx, opts.RepoPath, liveHead, persistedHead)
					if aErr != nil {
						logger.Warn(logPrefix+" cross-tick ancestry probe failed",
							"err", aErr.Error())
					} else if ok {
						synthesizedPrev := branchTokenRev(persistedHead, liveBranchRef)
						rewindPaused, rewindUntil, rewindErr := maybeSetRewindGrace(
							ctx, opts.RepoPath, opts.DB, synthesizedPrev, newToken, now())
						if rewindErr != nil {
							logger.Warn(logPrefix+" cross-tick rewind grace failed",
								"err", rewindErr.Error())
						} else if rewindPaused {
							logger.Info("replay paused after cross-tick same-SHA rewind",
								"persisted_head", persistedHead,
								"live_head", liveHead,
								"until", rewindUntil)
							recordTrace(tracer, acdtrace.Event{
								Repo:       opts.RepoPath,
								BranchRef:  liveBranchRef,
								HeadSHA:    liveHead,
								EventClass: "branch_token.transition",
								Decision:   "diverged",
								Reason:     "cross-tick same-SHA rewind detected",
								Input: map[string]any{
									"persisted": persistedHead,
									"current":   tokenHead,
									"live":      liveHead,
								},
								Output: map[string]any{
									"rewind_until": rewindUntil,
								},
								Generation: cctx.BranchGeneration,
							})
						}
					}
				}
			}
			// Unconditional stamp: keep persisted MetaKeyBranchHead in sync
			// with the freshly-observed live HEAD so the next tick's probe
			// has a current baseline rather than a stale value written by an
			// old transition. Cheap meta upsert; no error abort — a failure
			// just means the next probe sees the same stale value, which is
			// what the previous code did unconditionally.
			if liveHead != "" {
				if err := state.MetaSet(ctx, opts.DB, MetaKeyBranchHead, liveHead); err != nil {
					logger.Warn(logPrefix+" stamp branch head per-tick",
						"err", err.Error())
				}
			}
			return false
		}
		transition, cErr := ClassifyTokenTransition(ctx, opts.RepoPath, currentToken, newToken)
		if cErr != nil {
			logger.Warn(logPrefix+" classify failed; will retry",
				"err", cErr.Error())
			return true
		}
		ts := strconv.FormatFloat(float64(now().UnixNano())/1e9, 'f', -1, 64)
		_ = state.MetaSet(ctx, opts.DB, MetaKeyBranchTokenChangedAt, ts)
		_ = state.MetaSet(ctx, opts.DB, MetaKeyBranchToken, newToken)
		// Refresh HEAD for capture/replay regardless of transition kind.
		branchRef, headOID = resolveBranch(ctx, opts.RepoPath, logger)
		cctx.BranchRef = branchRef
		cctx.BaseHead = headOID
		oldToken := currentToken
		currentToken = newToken
		if transition == TokenTransitionDiverged {
			prevGeneration := cctx.BranchGeneration
			rewindPaused, rewindUntil, rewindErr := maybeSetRewindGrace(ctx, opts.RepoPath, opts.DB, oldToken, newToken, now())
			if rewindErr != nil {
				logger.Warn(logPrefix+" detect rewind grace failed", "err", rewindErr.Error())
			} else if rewindPaused {
				logger.Info("replay paused after branch rewind", "until", rewindUntil)
			}
			cctx.BranchGeneration++
			logger.Info("branch generation bumped",
				"old", oldToken, "new", newToken,
				"generation", cctx.BranchGeneration,
				"transition", transition.String())
			droppedPending, dropErr := state.DeletePendingForGeneration(ctx, opts.DB, prevGeneration)
			if dropErr != nil {
				logger.Warn("drop pending events for previous branch generation",
					"generation", prevGeneration, "err", dropErr.Error())
			}
			recordTrace(tracer, acdtrace.Event{
				Repo:       opts.RepoPath,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "branch_token.transition",
				Decision:   transition.String(),
				Reason:     "run-loop token transition classified",
				Input:      map[string]any{"previous": oldToken, "current": newToken},
				Output: map[string]any{
					"prev_generation": prevGeneration,
					"new_generation":  cctx.BranchGeneration,
					"dropped_pending": droppedPending,
				},
				Error:      traceErrString(dropErr),
				Generation: cctx.BranchGeneration,
			})
			if err := SaveBranchGeneration(ctx, opts.DB,
				cctx.BranchGeneration, headOID); err != nil {
				logger.Warn("persist bumped branch generation",
					"err", err.Error())
			}
			// shadow_paths is keyed by (branch_ref, branch_generation).
			// After a divergence the new key is empty; without
			// reseeding from HEAD the next capture would classify every
			// tracked file as a phantom `create`.
			if cctx.BranchRef == "" {
				_ = state.MetaSet(ctx, opts.DB, MetaKeyDetachedHeadPaused, ts)
				logger.Warn("detached HEAD detected; capture/replay paused")
			} else if seeded, err := BootstrapShadow(ctx, opts.RepoPath, opts.DB, cctx); err != nil {
				logger.Warn("reseed shadow after generation bump",
					"err", err.Error())
				traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", err.Error(), 0)
			} else {
				traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), "generation bump shadow reseed", seeded)
				if seeded > 0 {
					logger.Info("shadow reseeded",
						"rows", seeded,
						"generation", cctx.BranchGeneration)
				}
				if pruned, pErr := pruneShadowGenerations(ctx, opts.DB, cctx); pErr != nil {
					logger.Warn("prune old shadow generations", "err", pErr.Error())
				} else if pruned > 0 {
					logger.Info("pruned old shadow generations", "rows", pruned)
				}
			}
		} else {
			// Fast-forward: persist the new HEAD so the next transition
			// compares against the latest baseline, but keep the generation
			// counter put.
			//
			// Exception: if a rewind-grace marker is currently active, the
			// previous tick's same-branch rewind reseeded shadow_paths from
			// the rewound (lower) HEAD. A fast-forward landing inside that
			// window must NOT just bump BaseHead; the next post-grace capture
			// pass would otherwise compare the live HEAD's tracked files
			// against shadow rows seeded at the rewound HEAD and emit phantom
			// `create` events for content that is already published. Treat
			// this FF as a generation boundary: bump the generation, reseed
			// shadow from the new HEAD, and clear the grace gate so the
			// resumed capture/replay drain sees a clean shadow.
			graceActive, until, gErr := rewindGraceActive(ctx, opts.DB, now())
			if gErr != nil {
				logger.Warn(logPrefix+" probe rewind grace failed",
					"err", gErr.Error())
			}
			if graceActive {
				prevGeneration := cctx.BranchGeneration
				cctx.BranchGeneration++
				logger.Info("fast-forward inside rewind grace; reseeding shadow",
					"old", oldToken, "new", newToken,
					"generation", cctx.BranchGeneration,
					"grace_until", until)
				recordTrace(tracer, acdtrace.Event{
					Repo:       opts.RepoPath,
					BranchRef:  cctx.BranchRef,
					HeadSHA:    cctx.BaseHead,
					EventClass: "branch_token.transition",
					Decision:   transition.String(),
					Reason:     "fast-forward inside rewind grace; reseeding shadow",
					Input:      map[string]any{"previous": oldToken, "current": newToken},
					Output: map[string]any{
						"prev_generation": prevGeneration,
						"new_generation":  cctx.BranchGeneration,
						"grace_until":     until,
					},
					Generation: cctx.BranchGeneration,
				})
				if err := SaveBranchGeneration(ctx, opts.DB,
					cctx.BranchGeneration, headOID); err != nil {
					logger.Warn("persist branch generation after FF-in-grace",
						"err", err.Error())
				}
				if cctx.BranchRef != "" {
					if seeded, err := BootstrapShadow(ctx, opts.RepoPath, opts.DB, cctx); err != nil {
						logger.Warn("reseed shadow after FF-in-grace",
							"err", err.Error())
						traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", err.Error(), 0)
					} else {
						traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), "FF-in-grace shadow reseed", seeded)
						if seeded > 0 {
							logger.Info("shadow reseeded after FF-in-grace",
								"rows", seeded,
								"generation", cctx.BranchGeneration)
						}
						if pruned, pErr := pruneShadowGenerations(ctx, opts.DB, cctx); pErr != nil {
							logger.Warn("prune old shadow generations", "err", pErr.Error())
						} else if pruned > 0 {
							logger.Info("pruned old shadow generations", "rows", pruned)
						}
					}
				}
				// Clear the rewind grace gate now that shadow is consistent
				// with the current HEAD. Capture/replay can resume on the
				// next tick.
				clearRewindGraceMeta(ctx, opts.DB, opts.RepoPath, cctx, tracer, logger,
					"fast-forward inside rewind grace")
			} else {
				logger.Debug("branch fast-forwarded",
					"old", oldToken, "new", newToken,
					"generation", cctx.BranchGeneration)
				recordTrace(tracer, acdtrace.Event{
					Repo:       opts.RepoPath,
					BranchRef:  cctx.BranchRef,
					HeadSHA:    cctx.BaseHead,
					EventClass: "branch_token.transition",
					Decision:   transition.String(),
					Reason:     "run-loop token transition classified",
					Input:      map[string]any{"previous": oldToken, "current": newToken},
					Output:     map[string]any{"generation": cctx.BranchGeneration},
					Generation: cctx.BranchGeneration,
				})
				if err := SaveBranchGeneration(ctx, opts.DB,
					cctx.BranchGeneration, headOID); err != nil {
					logger.Warn("persist branch head", "err", err.Error())
				}
			}
		}
		return false
	}

	for {
		branchTransitionBlocked = false

		// 4a/b. Honor ctx + shutdown signal.
		if err := ctx.Err(); err != nil {
			graceful("context canceled")
			return nil
		}
		select {
		case <-shutdownCh:
			graceful("signal shutdown")
			return nil
		default:
		}

		// 4c. Drain any pending wake (the wake channel is buffered cap=1
		// so we just non-blocking receive once; a real wake is observed
		// either here or in the sleep select below). The fsnotify wake
		// channel is drained in the same way so a queued event from
		// before the previous tick doesn't double-fire.
		select {
		case <-wakeCh:
		default:
		}
		if fsWakeReader != nil {
			select {
			case <-fsWakeReader:
			default:
			}
		}

		// 4d. Branch-generation token check. The token alone cannot
		// distinguish an ACD-driven fast-forward (the daemon just landed
		// a commit and HEAD advanced) from an external rewrite (operator
		// ran `git reset` / rebased / switched branches). We re-resolve
		// HEAD's ancestry against the previously observed HEAD: if the
		// new HEAD descends from the old, it is a fast-forward and the
		// generation counter stays put — queued events with the prior
		// BaseHead are still safe because their parent is still in
		// HEAD's history. A divergence (rebase / reset / orphan switch)
		// bumps the generation; the bump is persisted so a daemon
		// restart picks up the same value, and the next replay pass
		// terminally blocks any queued events captured under the prior
		// generation (their BaseHead is no longer reachable).
		operationName, operationPaused := gitOperationInProgress(opts.GitDir)
		if operationPaused {
			_ = state.MetaSet(ctx, opts.DB, MetaKeyOperationInProgress, operationName)
			// Stale-marker tracking: stamp the wall-clock + HEAD the first
			// time we see this marker, persist for diagnose, then warn
			// periodically when both have been motionless past threshold.
			currentHead, _ := git.RevParse(ctx, opts.RepoPath, "HEAD")
			nowTS := now()
			if opMarkerSetAt.IsZero() {
				opMarkerSetAt = nowTS
				opMarkerHead = currentHead
				stamp := strconv.FormatFloat(float64(nowTS.UnixNano())/1e9, 'f', -1, 64)
				_ = state.MetaSet(ctx, opts.DB, MetaKeyOperationInProgressSetAt, stamp)
				_ = state.MetaSet(ctx, opts.DB, MetaKeyOperationInProgressHead, currentHead)
			}
			logger.Warn("git operation in progress; capture/replay paused",
				"operation", operationName)
			recordTrace(tracer, acdtrace.Event{
				Repo:       opts.RepoPath,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "daemon.pause",
				Decision:   "paused",
				Reason:     "git operation marker present",
				Input:      map[string]any{"operation": operationName},
				Generation: cctx.BranchGeneration,
			})
			// Stale heuristic: marker present > threshold AND HEAD has not
			// moved since we first saw it. We never auto-clear — operator
			// must run `git rebase --abort` (or remove the marker) by hand.
			elapsed := nowTS.Sub(opMarkerSetAt)
			if elapsed >= staleOpMarkerThreshold && currentHead == opMarkerHead {
				if opMarkerWarnedAt.IsZero() || nowTS.Sub(opMarkerWarnedAt) >= staleOpMarkerWarnInterval {
					logger.Warn("operation_in_progress marker may be stale; verify git status",
						"operation", operationName,
						"head", currentHead,
						"duration", elapsed.Round(time.Second).String())
					opMarkerWarnedAt = nowTS
				}
			}
		} else if _, ok, _ := state.MetaGet(ctx, opts.DB, MetaKeyOperationInProgress); ok {
			_, _ = state.MetaDelete(ctx, opts.DB, MetaKeyOperationInProgress)
			_, _ = state.MetaDelete(ctx, opts.DB, MetaKeyOperationInProgressSetAt)
			_, _ = state.MetaDelete(ctx, opts.DB, MetaKeyOperationInProgressHead)
			opMarkerSetAt = time.Time{}
			opMarkerHead = ""
			opMarkerWarnedAt = time.Time{}
			// Operation cleared is an explicit operator transition. A stale
			// rewind-grace marker from before the operation must NOT survive
			// it — otherwise capture/replay stay muted up to
			// ACD_REWIND_GRACE_SECONDS post-resume. Best-effort: log on
			// failure, don't abort the resume path.
			clearRewindGraceMeta(ctx, opts.DB, opts.RepoPath, cctx, tracer, logger,
				"git operation cleared")
			recordTrace(tracer, acdtrace.Event{
				Repo:       opts.RepoPath,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "daemon.pause",
				Decision:   "resumed",
				Reason:     "git operation marker cleared",
				Generation: cctx.BranchGeneration,
			})
		}

		if !operationPaused {
			if cctx.BranchRef == "" {
				branchRef, headOID = resolveBranch(ctx, opts.RepoPath, logger)
				if branchRef != "" {
					cctx.BranchRef = branchRef
					cctx.BaseHead = headOID
					if err := SaveBranchGeneration(ctx, opts.DB,
						cctx.BranchGeneration, headOID); err != nil {
						logger.Warn("persist reattached branch head",
							"err", err.Error())
					}
					if _, ok, _ := state.MetaGet(ctx, opts.DB, MetaKeyDetachedHeadPaused); ok {
						_, _ = state.MetaDelete(ctx, opts.DB, MetaKeyDetachedHeadPaused)
					}
					// Reattach is an explicit operator transition. Like the
					// operation-cleared path above, a stale rewind-grace marker
					// from before the detach must NOT survive the reattach —
					// otherwise capture/replay stay muted up to
					// ACD_REWIND_GRACE_SECONDS post-reattach.
					clearRewindGraceMeta(ctx, opts.DB, opts.RepoPath, cctx, tracer, logger,
						"detached HEAD reattached")
					if headOID != "" {
						if seeded, err := BootstrapShadow(ctx, opts.RepoPath, opts.DB, cctx); err != nil {
							logger.Warn("bootstrap shadow after reattach",
								"err", err.Error())
							traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", err.Error(), 0)
						} else {
							traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), "reattach shadow bootstrap", seeded)
							if seeded > 0 {
								logger.Info("shadow bootstrapped after reattach",
									"rows", seeded)
							}
							if pruned, pErr := pruneShadowGenerations(ctx, opts.DB, cctx); pErr != nil {
								logger.Warn("prune old shadow generations", "err", pErr.Error())
							} else if pruned > 0 {
								logger.Info("pruned old shadow generations", "rows", pruned)
							}
						}
					}
				}
			}
			if processBranchTokenChange("branch token") {
				branchTransitionBlocked = true
			}
		}

		// 4e. Drain pending flush_requests; each one triggers an immediate
		// capture+replay cycle. We advance through the queue until empty
		// (or FlushLimit is exceeded).
		flushed := 0
		for {
			fr, ok, err := state.ClaimNextFlushRequest(ctx, opts.DB)
			if err != nil {
				logger.Warn("claim flush request", "err", err.Error())
				break
			}
			if !ok {
				break
			}
			flushed++
			logger.Info("flush request acked",
				"id", fr.ID, "command", fr.Command)
			if err := state.CompleteFlushRequest(ctx, opts.DB, fr.ID, true,
				sql.NullString{String: "flushed", Valid: true}); err != nil {
				logger.Warn("complete flush", "err", err.Error())
			}
			if opts.FlushLimit > 0 && flushed >= opts.FlushLimit {
				break
			}
		}
		if !operationPaused && processBranchTokenChange("pre-capture branch token") {
			branchTransitionBlocked = true
		}

		// 4f. Capture pass.
		//
		// Manual pause + rewind grace pause BOTH capture and replay. This is
		// symmetric with the detached-HEAD pause and the git-operation pause:
		// while the operator's repo is in a transient state (mid-rewind, mid-
		// rebase, paused for surgery), capture must NOT enqueue events that
		// reflect that transient state. Otherwise the post-pause replay drain
		// would resurrect work the operator just rewound. Detached HEAD has
		// its own dedicated gate above.
		var (
			capSum     CaptureSummary
			capErr     error
			daemonPaus replayPause
			pauseErr   error
		)
		detachedHeadPaused := cctx.BranchRef == ""
		if !branchTransitionBlocked && !operationPaused && !detachedHeadPaused {
			daemonPaus, pauseErr = daemonPauseState(ctx, opts.GitDir, opts.DB)
			if pauseErr != nil {
				logger.Warn("read daemon pause state", "err", pauseErr.Error())
			}
		}
		daemonPaused := pauseErr == nil && daemonPaus.Active
		if branchTransitionBlocked {
			logger.Warn("capture/replay paused until branch transition is classified")
		} else if operationPaused {
			logger.Warn("git operation in progress; capture/replay paused",
				"operation", operationName)
		} else if detachedHeadPaused {
			ts := strconv.FormatFloat(float64(now().UnixNano())/1e9, 'f', -1, 64)
			_ = state.MetaSet(ctx, opts.DB, MetaKeyDetachedHeadPaused, ts)
			logger.Warn("detached HEAD detected; capture/replay paused")
		} else if daemonPaused {
			logger.Warn("daemon paused; capture skipped",
				"source", daemonPaus.Source, "reason", daemonPaus.Reason)
			traceCapturePaused(tracer, opts.RepoPath, cctx, daemonPaus)
		} else if cctx.BaseHead != "" {
			// The run loop has already evaluated the pause gate above and
			// emitted the trace event when paused; SkipPauseCheck=true
			// prevents Capture from re-tracing the same decision. GitDir
			// is still wired through so that direct callers (tests,
			// future CLI wrappers) honor the same gate symmetrically.
			capSum, capErr = Capture(ctx, opts.RepoPath, opts.DB, cctx, CaptureOpts{
				IgnoreChecker:    ignoreChecker,
				SensitiveMatcher: matcher,
				Trace:            tracer,
				GitDir:           opts.GitDir,
				SkipPauseCheck:   true,
			})
		}

		var (
			repSum ReplaySummary
			repErr error
		)
		if capErr == nil && !branchTransitionBlocked && !operationPaused && !detachedHeadPaused && !daemonPaused && cctx.BaseHead != "" {
			// 4g. Replay pass.
			repSum, repErr = Replay(ctx, opts.RepoPath, opts.DB, cctx, ReplayOpts{
				MessageFn: msgFn,
				GitDir:    opts.GitDir,
				Trace:     tracer,
			})
			if repErr == nil && repSum.Published > 0 {
				// Refresh BaseHead to the exact commit replay just wrote.
				cctx.BaseHead = repSum.BaseHead
				currentToken = branchTokenRev(cctx.BaseHead, cctx.BranchRef)
				if err := SaveBranchGeneration(ctx, opts.DB,
					cctx.BranchGeneration, cctx.BaseHead); err != nil {
					logger.Warn("persist replay head", "err", err.Error())
				}
				if err := state.MetaSet(ctx, opts.DB, MetaKeyBranchToken, currentToken); err != nil {
					logger.Warn("persist replay branch token", "err", err.Error())
				}
			}
		}

		// Tick error counters.
		if capErr != nil {
			consecutiveErrors++
			logger.Warn("capture error", "n", consecutiveErrors, "err", capErr.Error())
			_ = state.MetaSet(ctx, opts.DB, "last_capture_error", capErr.Error())
		} else if repErr != nil {
			consecutiveErrors++
			logger.Warn("replay error", "n", consecutiveErrors, "err", repErr.Error())
			_ = state.MetaSet(ctx, opts.DB, "last_replay_error", repErr.Error())
		} else {
			consecutiveErrors = 0
			_ = state.MetaSet(ctx, opts.DB, "last_capture_error", "")
		}

		hadWork := flushed > 0 || capSum.EventsAppended > 0 || repSum.Published > 0

		// Heartbeat refresh — visible to controllers between iterations.
		heartbeatNow("running", "")

		// 4h. Refcount sweep, throttled to ClientSweepInterval.
		nowTS := now()
		if nowTS.Sub(lastSweep) >= clientSweepEvery {
			alive, sErr := SweepClients(ctx, opts.DB, nowTS, SweepOpts{TTL: clientTTL})
			if sErr != nil {
				logger.Warn("client sweep", "err", sErr.Error())
			} else {
				if alive == 0 {
					emptyCount++
				} else {
					emptyCount = 0
				}
			}
			lastSweep = nowTS

			// 4i. Self-terminate gate.
			if ShouldSelfTerminate(emptyCount, nowTS.Sub(bootTime), SelfTerminateOpts{
				BootGrace:           bootGrace,
				EmptySweepThreshold: emptyThreshold,
			}) {
				graceful(fmt.Sprintf("no live clients for %d sweeps", emptyCount))
				return nil
			}
		}

		// 4j. Prune capture_events opportunistically.
		if nowTS.Sub(lastPrune) >= pruneEvery {
			if n, pErr := PruneCaptureEvents(ctx, opts.DB, nowTS, eventRetention); pErr != nil {
				logger.Warn("prune events", "err", pErr.Error())
			} else if n > 0 {
				logger.Info("pruned events", "rows", n)
			}
			lastPrune = nowTS
		}

		// 4k. Phase 3 daily rollup hook (§8.10). Throttled to
		// RollupInterval, force-fired once when the UTC day changes
		// underneath the loop. Failure logs + records last_error_at
		// but never crashes the loop.
		curUTCDay := nowTS.UTC().Format(dayLayout)
		dayBoundaryCrossed := lastRollupUTCDay != "" && curUTCDay != lastRollupUTCDay
		if curUTCDay != lastRollupUTCDay && lastRollupUTCDay == "" {
			lastRollupUTCDay = curUTCDay
		}
		if dayBoundaryCrossed || nowTS.Sub(lastRollup) >= rollupEvery {
			n, rErr := RunDailyRollup(ctx, opts.DB, RunDailyRollupOpts{
				RepoPath: opts.RepoPath,
				Now:      now,
			})
			if rErr != nil {
				logger.Warn("daily rollup", "err", rErr.Error())
				_ = state.MetaSet(ctx, opts.DB, metaRollupLastErrorAt,
					strconv.FormatFloat(float64(nowTS.UnixNano())/1e9, 'f', -1, 64))
			} else if n > 0 {
				logger.Info("daily rollup", "rows", n)
			}
			// Central push is best-effort. Skip when stats handle or
			// repo_hash is missing — log + continue without erroring.
			if rErr == nil && statsDB != nil && opts.RepoHash != "" {
				if pushed, pErr := central.PushRollupsToCentral(
					ctx, opts.DB, statsDB, opts.RepoHash, opts.RepoPath,
				); pErr != nil {
					logger.Warn("central rollup push", "err", pErr.Error())
					_ = state.MetaSet(ctx, opts.DB, metaRollupLastErrorAt,
						strconv.FormatFloat(float64(nowTS.UnixNano())/1e9, 'f', -1, 64))
				} else if pushed > 0 {
					logger.Info("central rollup pushed", "rows", pushed)
				}
			}
			lastRollup = nowTS
			lastRollupUTCDay = curUTCDay
		}

		// 4l. Compute next delay.
		switch {
		case consecutiveErrors > 0:
			currentDelay = opts.Scheduler.NextError(currentDelay)
		case hadWork:
			currentDelay = opts.Scheduler.Reset()
		default:
			currentDelay = opts.Scheduler.NextIdle(currentDelay)
		}

		// 4m. Sleep until the next tick or wake/shutdown/ctx event.
		if stopped {
			return nil
		}
		timer := time.NewTimer(currentDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			graceful("context canceled")
			return nil
		case <-shutdownCh:
			timer.Stop()
			graceful("signal shutdown")
			return nil
		case <-wakeCh:
			timer.Stop()
			currentDelay = opts.Scheduler.Reset()
		case <-fsWakeReader:
			// fsWakeReader is nil when fsnotify is disabled; a nil
			// receive blocks forever, so this arm is effectively
			// inactive when there's no watcher.
			timer.Stop()
			currentDelay = opts.Scheduler.Reset()
		case <-timer.C:
		}
	}
}

// resolveBranch returns (branchRef, headOID) for the current HEAD. A detached
// HEAD returns an empty branchRef so the run loop pauses capture/replay instead
// of inventing a branch target.
func resolveBranch(ctx context.Context, repoDir string, logger *slog.Logger) (string, string) {
	branch, err := git.RunBranchRef(ctx, repoDir)
	if err != nil {
		logger.Warn("symbolic-ref HEAD failed", "err", err.Error())
		return "", ""
	}
	head, err := git.RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		if !errors.Is(err, git.ErrRefNotFound) {
			logger.Warn("rev-parse HEAD failed", "err", err.Error())
		}
		return branch, ""
	}
	return branch, head
}

func gitOperationInProgress(gitDir string) (string, bool) {
	for _, marker := range []struct {
		path string
		name string
	}{
		{path: "rebase-merge", name: "rebase-merge"},
		{path: "rebase-apply", name: "rebase-apply"},
		{path: "MERGE_HEAD", name: "merge"},
		{path: "CHERRY_PICK_HEAD", name: "cherry-pick"},
		{path: "BISECT_LOG", name: "bisect"},
	} {
		if _, err := os.Stat(filepath.Join(gitDir, marker.path)); err == nil {
			return marker.name, true
		} else if !errors.Is(err, os.ErrNotExist) {
			return marker.name, true
		}
	}
	return "", false
}

// gitDirEnsureSubdir is a tiny helper to ensure a subdir exists under
// .git/acd. Used by callers that want to write auxiliary files alongside
// daemon.lock without re-implementing the mkdir-then-open dance.
func gitDirEnsureSubdir(gitDir, sub string) (string, error) {
	dir := filepath.Join(gitDir, stateSubdir, sub)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return dir, nil
}

// openCentralStats opens (or creates) the central stats.db at the given
// absolute path. Wraps central.OpenAt so the daemon package owns the
// "open + log + skip" policy without re-implementing the bootstrap dance.
func openCentralStats(ctx context.Context, dbPath string) (*central.StatsDB, error) {
	return central.OpenAt(ctx, dbPath)
}

// clearRewindGraceMeta removes a stale daemon_meta.replay.paused_until row.
//
// It is a best-effort helper invoked on explicit operator transitions where
// the rewind heuristic must NOT survive: detached-HEAD reattach and
// operation-in-progress clear. The marker persists across restarts (it is a
// row in daemon_meta) so a transition that lifts an unrelated pause must
// also strip the rewind-grace gate, otherwise capture/replay stay muted for
// up to ACD_REWIND_GRACE_SECONDS after the operator-driven resume.
//
// Failures are logged but do not abort the caller — `daemonPauseState` will
// fall through to the next tick and clear an expired value naturally. When a
// row was actually removed we emit a `replay.pause` trace with decision
// "cleared" so operator-facing tooling can see the reason.
func clearRewindGraceMeta(ctx context.Context, db *state.DB, repoPath string, cctx CaptureContext, tracer acdtrace.Logger, logger *slog.Logger, reason string) {
	prev, ok, err := state.MetaGet(ctx, db, MetaKeyReplayPausedUntil)
	if err != nil {
		logger.Warn("read rewind grace meta",
			"err", err.Error(), "reason", reason)
		return
	}
	if !ok || prev == "" {
		return
	}
	if _, err := state.MetaDelete(ctx, db, MetaKeyReplayPausedUntil); err != nil {
		logger.Warn("clear rewind grace meta",
			"err", err.Error(), "reason", reason)
		return
	}
	recordTrace(tracer, acdtrace.Event{
		Repo:       repoPath,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: "replay.pause",
		Decision:   "cleared",
		Reason:     reason,
		Input:      map[string]any{"previous_until": prev},
		Generation: cctx.BranchGeneration,
	})
	logger.Info("rewind grace cleared on operator transition",
		"reason", reason, "previous_until", prev)
}

// (un)used helpers retained for future phases — keep the symbol exported so
// the test build doesn't drop them on compile.
var _ = gitDirEnsureSubdir

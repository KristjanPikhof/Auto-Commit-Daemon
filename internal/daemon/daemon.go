// Package daemon protects worktree changes in durable checkpoints.
// Run owns protection, retention, locking and wakeups; explicit finish owns
// branch publication. A joined helper keeps heartbeat fresh during scans.
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
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
	// DefaultCheckpointRetentionInterval keeps the object inventory out of
	// the minute-level event maintenance path. Checkpoint age and budget
	// policies operate on days, so hourly evaluation is prompt without making
	// idle repositories repeatedly traverse Git history.
	DefaultCheckpointRetentionInterval = time.Hour
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
	// manualResumeResyncWindow is the grace period after `acd resume` during
	// which a same-branch fast-forward preserves shadow state so paused local
	// edits can self-heal against an external commit that landed while paused.
	manualResumeResyncWindow = 30 * time.Second
	// branchTransitionSettleDelay gives external git operations a short window
	// to finish updating the worktree after HEAD moves. Ref updates and
	// worktree writes are not observed atomically by the daemon; without this
	// pause, a fast tick can reseed shadow from the new HEAD and then capture a
	// still-missing upstream file as a local delete.
	branchTransitionSettleDelay = 100 * time.Millisecond
)

// EnvClientTTLSeconds is the environment knob for ACD_CLIENT_TTL_SECONDS
// (D21). The default is DefaultClientTTL (30 minutes).
const EnvClientTTLSeconds = "ACD_CLIENT_TTL_SECONDS"

// providerCloseTimeout bounds how long Run waits for the configured AI
// provider's Closer to return before falling back to force-kill (when the
// closer exposes Process()) and proceeding with daemon shutdown. The
// budget is generous enough for an LSP-style flush yet short enough that
// SIGTERM is observed within the documented ~6s envelope.
const providerCloseTimeout = 5 * time.Second
const daemonProgressHeartbeatInterval = time.Second

// processExposer is implemented by subprocess-backed AI provider closers
// so Run can force-kill the underlying process when Close hangs past
// providerCloseTimeout.
type processExposer interface {
	Process() *os.Process
}

// Options configures one Run invocation.
//
// Required: RepoPath, GitDir, DB. Everything else has a usable default.
// Legacy publication, planner and provider fields remain compile bridges;
// Run ignores them. Their callers are removed in the surface-removal task.
type Options struct {
	// RepoPath is the absolute path to the worktree root.
	RepoPath string
	// GitDir is the absolute .git directory.
	GitDir string
	// DB is the already-open per-repo state database. Run does NOT close
	// the DB on exit — caller owns the lifetime.
	DB *state.DB

	// SkipDaemonLock is reserved for the supervisor's repository worker. The
	// worker acquires the one canonical common-directory lock before starting
	// all linked-worktree loops in its process. Direct daemon entry points must
	// leave this false.
	SkipDaemonLock bool

	// OperationGate serializes protection/publication passes with an explicit
	// worker mutation such as restore. The supervisor worker owns the gate;
	// ordinary direct daemon callers leave it nil.
	OperationGate *sync.RWMutex

	// ProtectionGate serializes full scans with explicit finish protection.
	ProtectionGate *sync.Mutex
	// FinishRecoveryDone means the owner recovered finish before accepting requests.
	FinishRecoveryDone bool

	// PublicationHeld is consulted at every safe boundary. Setup workers use
	// it to prove checkpoint coverage before a global cutover commits. A true
	// result never pauses protection and never mutates the user's Git history.
	PublicationHeld func() bool

	// Protection settings are resolved per worktree by the supervisor. They
	// must not be overlaid through process environment because one worker may
	// host multiple linked worktrees with different repository settings.
	MaxFileBytes               int64
	SensitiveMatcher           *state.SensitiveMatcher
	SafeIgnoreMatcher          *state.SafeIgnoreMatcher
	RewindGrace                *time.Duration
	ShadowRetentionGenerations *int64

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

	// providerCloseTimeout is a test-only override for the bounded provider
	// shutdown wait. Zero keeps the production providerCloseTimeout.
	providerCloseTimeout time.Duration
	// progressHeartbeatEvery is a test-only acceleration of the fixed
	// controller-liveness heartbeat used during long replay work.
	progressHeartbeatEvery time.Duration
	// replay is a test-only seam for blocking long-pass and cancellation
	// coverage. Production always uses Replay.
	replay func(context.Context, string, *state.DB, CaptureContext, ReplayOpts) (ReplaySummary, error)

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
	// PromptTrace is the repository-scoped sensitive prompt trace sink. Nil
	// preserves the direct-daemon compatibility path that reads the legacy
	// process environment.
	PromptTrace prompttrace.Logger

	// IntentPlanner overrides the intent planner used by Replay. Production
	// leaves this nil; tests inject a recorder to assert run-loop gate
	// behavior without involving network or subprocess providers.
	IntentPlanner ai.IntentPlanner
	// runtimeBuildProvider is a test-only seam for constructing providers
	// from immutable runtime revisions. It preserves the production cutover
	// contract while letting run-loop tests assert provider reuse and health
	// continuity without executing a real subprocess or network request.
	runtimeBuildProvider runtimeBundleBuildFunc

	// beforeBranchTransitionAccept is a test-only synchronization point after
	// prospective shadow preparation and before the final token/pause CAS.
	beforeBranchTransitionAccept func()
	// afterBranchTransitionRollback fires after prospective shadow
	// invalidation and in-memory context restoration.
	afterBranchTransitionRollback func()
	// beforeBranchTokenCheck is a test-only synchronization point immediately
	// before the run loop samples the live branch token.
	beforeBranchTokenCheck func()
	// afterSelfPublicationAdoption is a test-only observation point after an
	// ACD-journal-proved target and all in-memory/durable token fields agree.
	// The historical name is retained for existing synchronization tests.
	afterSelfPublicationAdoption func(CaptureContext, string, string, string)
	// afterRunLoopWorkDecision is a test-only observation point for the
	// scheduler input after one complete pass.
	afterRunLoopWorkDecision func(hadWork, recoveryFollowup bool)
	// recoverSelfPublications is a test-only seam for deterministic
	// cancellation coverage around startup and active recovery passes.
	recoverSelfPublications func(
		context.Context, string, *state.DB, CaptureContext, ReplayOpts,
	) (SelfPublicationRecoverySummary, error)
	// selfPublicationCheckpoint is the run-loop wiring for Replay's
	// deterministic publication-boundary fault seam.
	selfPublicationCheckpoint func(SelfPublicationCheckpointEvent) error
	// branchGenerationToken is a test-only resolver seam for publication
	// adoption and transition retry coverage.
	branchGenerationToken func(context.Context, string) (string, error)
	// afterExternalRepairBridgeAdoption is a test-only synchronization point
	// after atomic bridge state is adopted and before the pass reaches capture.
	afterExternalRepairBridgeAdoption func(CaptureContext)
	// beforeStartupDeadBranchPairSafetyCheck is a test-only synchronization
	// point immediately before a startup dead-branch pair's final safety gate.
	// The sweep-owned context lets blocking tests release on daemon shutdown.
	beforeStartupDeadBranchPairSafetyCheck func(context.Context, deadBranchPair)
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
	if opts.RepoPath == "" || opts.GitDir == "" || opts.DB == nil {
		return errors.New("daemon: Run requires RepoPath, GitDir and DB")
	}
	if !opts.SkipDaemonLock {
		lock, err := AcquireDaemonLock(opts.GitDir)
		if err != nil {
			return err
		}
		defer lock.Release()
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger, logContext := newDaemonLogger(logger, opts)
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	boot := now()
	store := checkpointpkg.Store{DB: opts.DB}
	if err := store.RecoverPrepared(ctx, opts.RepoPath); err != nil {
		logger.Warn("checkpoint recovery requires attention; new protection remains active", "err", err)
	}
	if !opts.FinishRecoveryDone {
		worker := FinishWorker{RepoPath: opts.RepoPath, DB: opts.DB, OperationGate: opts.OperationGate, ProtectionGate: opts.ProtectionGate}
		if _, err := worker.RecoverPrepared(ctx); err != nil {
			logger.Warn("finish recovery requires attention; protection remains active", "err", err)
		}
	}
	checker := git.NewIgnoreChecker(opts.RepoPath)
	defer checker.Close()
	tracer := opts.Trace
	if tracer == nil {
		tracer = acdtrace.FromEnv(opts.RepoPath, opts.GitDir)
	}
	defer tracer.Close()
	wake, shutdown := opts.WakeCh, opts.ShutdownCh
	if !opts.SkipSignals {
		signals, cleanup := InstallSignalHandlers(ctx)
		defer cleanup()
		if wake == nil {
			wake = signals.Wake
		}
		if shutdown == nil {
			shutdown = signals.Shutdown
		}
	}
	ctx, cancelRun := context.WithCancel(ctx)
	shutdownJoined := make(chan struct{})
	go func() {
		defer close(shutdownJoined)
		select {
		case <-shutdown:
			cancelRun()
		case <-ctx.Done():
		}
	}()
	defer func() { cancelRun(); <-shutdownJoined }()
	var fsWake <-chan struct{}
	if opts.FsnotifyEnabled {
		ch := make(chan struct{}, 1)
		fsWake = ch
		watcher, err := NewFsnotifyWatcher(FsnotifyOptions{
			RepoPath: opts.RepoPath, GitDir: opts.GitDir, IgnoreChecker: checker,
			Sensitive: opts.SensitiveMatcher, SafeIgnore: opts.SafeIgnoreMatcher,
			Debounce: opts.FsnotifyDebounce, MaxWatches: opts.FsnotifyMaxWatches, Logger: logger,
			WakeFn: func() {
				select {
				case ch <- struct{}{}:
				default:
				}
			},
			DiagnosticsFn: func(d WatcherDiagnostics) {
				_ = state.MetaSetMany(ctx, opts.DB, map[string]string{
					"fsnotify.mode": d.Mode, "fsnotify.watch_count": strconv.Itoa(d.WatchCount),
					"fsnotify.dropped_events": strconv.Itoa(d.DroppedEvents), "fsnotify.fallback_reason": d.FallbackReason,
				})
			},
		})
		if err == nil {
			err = watcher.Start(ctx)
		}
		if err != nil {
			logger.Warn("watcher unavailable; polling remains active", "err", err)
		}
		if watcher != nil {
			defer func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = watcher.Stop(stopCtx)
			}()
		}
	}
	stats := opts.CentralStats
	if stats == nil && opts.CentralStatsDBPath != "" {
		var err error
		stats, err = openCentralStats(ctx, opts.CentralStatsDBPath)
		if err != nil {
			logger.Warn("open central stats", "err", err)
		} else {
			defer stats.Close()
		}
	}
	maintenance, err := store.LoadMaintenance(ctx)
	if err != nil {
		logger.Warn("load checkpoint maintenance", "err", err)
	}
	if maintenance.State == "" {
		maintenance.NextAttemptTS = boot.Add(DefaultCheckpointRetentionInterval).Unix()
	}
	var indexGitlinks protectionIndexCache
	var submoduleHead string
	var submodules map[string]bool
	protect := func(passCtx context.Context, reason string, maintain bool) (CaptureSummary, error) {
		if opts.OperationGate != nil {
			opts.OperationGate.RLock()
			defer opts.OperationGate.RUnlock()
		}
		if opts.ProtectionGate != nil {
			opts.ProtectionGate.Lock()
			defer opts.ProtectionGate.Unlock()
		}
		checker.Invalidate()
		branch, head := observeProtectionHead(passCtx, opts.RepoPath, logger)
		logContext.SetBranch(branch, 0)
		if submodules == nil || head != submoduleHead {
			submodules = map[string]bool{}
			if head != "" {
				entries, err := git.LsTree(passCtx, opts.RepoPath, head, true)
				if err != nil {
					return CaptureSummary{}, fmt.Errorf("daemon: inspect protection Gitlinks: %w", err)
				}
				for _, entry := range entries {
					if entry.Mode == "160000" {
						submodules[entry.Path] = true
					}
				}
			}
			submoduleHead = head
		}

		paths, err := protectionSubmodulePaths(passCtx, opts.RepoPath, submodules, &indexGitlinks)
		if err != nil {
			return CaptureSummary{}, fmt.Errorf("daemon: inspect index Gitlinks: %w", err)
		}

		epoch, err := BeginProtectionObservation(passCtx, opts.DB)
		if err != nil {
			return CaptureSummary{}, err
		}
		sum, captureErr := ProtectWorktree(passCtx, opts.RepoPath, opts.DB, CaptureContext{BranchRef: branch, BaseHead: head}, CaptureOpts{
			IgnoreChecker: checker, SensitiveMatcher: opts.SensitiveMatcher, SafeIgnoreMatcher: opts.SafeIgnoreMatcher,
			SubmodulePaths: paths, Trace: tracer, MaxFileBytes: opts.MaxFileBytes, CheckpointStore: &store, WorktreeID: checkpointpkg.WorktreeID(opts.RepoPath),
			ObservationEpoch: epoch, CheckpointReason: reason,
		})
		if maintain && now().Unix() >= maintenance.NextAttemptTS {
			var maintenanceErr error
			maintenance, maintenanceErr = store.Maintain(passCtx, opts.RepoPath, checkpointpkg.WorktreeID(opts.RepoPath), now(), maintenance)
			if maintenanceErr != nil {
				logger.Warn("checkpoint maintenance", "err", maintenanceErr)
			}
		}
		return sum, captureErr
	}
	var healthRecorded bool
	var healthCheckpoint, healthFailure string
	recordHealth := func(passCtx context.Context, sum CaptureSummary, captureErr error, nextRetry time.Time) {
		failure := ""
		if captureErr != nil {
			failure = captureErr.Error()
		}
		if healthRecorded && failure == "" && healthFailure == "" && sum.CheckpointID == healthCheckpoint {
			return
		}
		var retry []time.Time
		if !nextRetry.IsZero() {
			retry = []time.Time{nextRetry}
		}
		if err := state.RecordCaptureHealth(passCtx, opts.DB, failure, sum.CheckpointID, now(), retry...); err != nil {
			logger.Warn("record checkpoint health", "err", err)
			return
		}
		healthRecorded, healthCheckpoint, healthFailure = true, sum.CheckpointID, failure
	}
	pid := os.Getpid()
	fingerprint := ""
	if fp, err := identity.Capture(pid); err == nil {
		fingerprint = FingerprintToken(fp)
	}
	heartbeat := func(stampCtx context.Context, mode, note string) {
		ts := float64(now().UnixNano()) / 1e9
		stamp := state.DaemonState{PID: pid, Mode: mode, HeartbeatTS: ts, UpdatedTS: ts,
			Note: sql.NullString{String: note, Valid: note != ""}, DaemonFingerprint: sql.NullString{String: fingerprint, Valid: fingerprint != ""}}
		if err := state.SaveDaemonState(stampCtx, opts.DB, stamp); err != nil {
			logger.Warn("save worker heartbeat", "err", err)
		}
	}
	heartbeat(ctx, "running", "checkpoint protection active")
	sweepEvery, rollupEvery := opts.ClientSweepInterval, opts.RollupInterval
	if sweepEvery <= 0 {
		sweepEvery = DefaultClientSweepInterval
	}
	if rollupEvery <= 0 {
		rollupEvery = DefaultRollupInterval
	}
	var lastSweep, lastRollup time.Time
	var lastDay, previousCheckpoint string
	emptyCount := 0
	delay := opts.Scheduler.Reset()
	reason := state.CheckpointReasonPoll
	runWithProgressHeartbeat(ctx, progressHeartbeatInterval(opts), func() { heartbeat(ctx, "running", "checkpoint protection active") }, func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-shutdown:
				return
			default:
			}
			sum, err := protect(ctx, reason, true)
			switch {
			case err != nil:
				if ctx.Err() == nil {
					logger.Warn("checkpoint protection incomplete", "err", err)
				}
				delay = opts.Scheduler.NextError(delay)
			case sum.CheckpointID != previousCheckpoint:
				delay = opts.Scheduler.Reset()
			default:
				delay = opts.Scheduler.NextIdle(delay)
			}
			recordHealth(ctx, sum, err, now().Add(delay))
			previousCheckpoint = sum.CheckpointID
			current := now()
			if current.Sub(lastSweep) >= sweepEvery {
				alive, err := SweepClients(ctx, opts.DB, current, SweepOpts{TTL: resolveClientTTL(opts.ClientTTL)})
				if err != nil {
					logger.Warn("sweep clients", "err", err)
				} else if alive == 0 {
					emptyCount++
				} else {
					emptyCount = 0
				}
				lastSweep = current
				if ShouldSelfTerminate(emptyCount, current.Sub(boot), SelfTerminateOpts{BootGrace: opts.BootGrace, EmptySweepThreshold: opts.EmptySweepThreshold}) {
					return
				}
			}
			day := current.UTC().Format("2006-01-02")
			if day != lastDay || current.Sub(lastRollup) >= rollupEvery {
				_, err := RunDailyRollup(ctx, opts.DB, RunDailyRollupOpts{RepoPath: opts.RepoPath, Now: now})
				if err != nil {
					logger.Warn("daily rollup", "err", err)
				}
				if err == nil && stats != nil && opts.RepoHash != "" {
					if _, err := central.PushRollupsToCentral(ctx, opts.DB, stats, opts.RepoHash, opts.RepoPath); err != nil {
						logger.Warn("central rollup", "err", err)
					}
				}
				lastRollup, lastDay = current, day
			}
			timer := time.NewTimer(delay)
			reason = state.CheckpointReasonPoll
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-shutdown:
				timer.Stop()
				return
			case <-wake:
				timer.Stop()
				delay = opts.Scheduler.Reset()
				reason = state.CheckpointReasonHint
			case <-fsWake:
				timer.Stop()
				delay = opts.Scheduler.Reset()
				reason = state.CheckpointReasonWatch
			case <-timer.C:
			}
		}
	})
	// Cancellation ends the running heartbeat before the final fresh-context
	// checkpoint. Stop protects current bytes and never drains publication.
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var stopErr error
	var stopSum CaptureSummary
	runWithProgressHeartbeat(stopCtx, progressHeartbeatInterval(opts), func() { heartbeat(stopCtx, "running", "final checkpoint") }, func() {
		stopSum, stopErr = protect(stopCtx, state.CheckpointReasonHint, false)
	})
	note := "checkpoint protection stopped"
	if stopErr != nil {
		note = "stopped with incomplete checkpoint"
	}
	stampCtx, cancelStamp := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStamp()
	recordHealth(stampCtx, stopSum, stopErr, time.Time{})
	heartbeat(stampCtx, "stopped", note)
	return stopErr
}

// observeProtectionHead records the literal attachment, including unborn and
// detached HEAD. Unsafe publication state never disables checkpoint protection.
func observeProtectionHead(ctx context.Context, repo string, logger *slog.Logger) (string, string) {
	branch := ""
	if raw, err := git.Run(ctx, git.RunOpts{Dir: repo}, "symbolic-ref", "--quiet", "--no-recurse", "HEAD"); err == nil {
		branch = strings.TrimSpace(string(raw))
	} else {
		var gitErr *git.Error
		if !errors.As(err, &gitErr) || gitErr.ExitCode != 1 {
			logger.Warn("observe HEAD attachment", "err", err)
		}
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil && !errors.Is(err, git.ErrRefNotFound) {
		logger.Warn("observe HEAD commit", "err", err)
	}
	return branch, head
}

func recordCompletedTransitionProofAttention(
	ctx context.Context,
	db *state.DB,
	proofErr error,
) error {
	if !errors.Is(proofErr, state.ErrCompletedBranchTransitionProof) {
		return nil
	}
	detail := ai.SanitizePlannerError(proofErr.Error())
	return state.MetaSet(ctx, db, MetaKeyBranchTransitionNeedsAttention,
		"Durable ACD transition proof needs attention: "+detail)
}

const intentRepairRecoveryAttentionPrefix = "Durable Intent repair recovery needs attention: "

func hasRecoverableIntentRepairForPair(
	repairs []state.IntentRepair,
	branchRef string,
	branchGeneration int64,
) bool {
	for _, repair := range repairs {
		if repair.BranchRef == branchRef &&
			repair.BranchGeneration == branchGeneration {
			return true
		}
	}
	return false
}

func recordIntentRepairRecoveryAttention(
	ctx context.Context,
	db *state.DB,
	recoveryErr error,
) error {
	if !errors.Is(recoveryErr, ErrIntentRepairRecoveryProof) {
		return nil
	}
	detail := ai.SanitizePlannerError(recoveryErr.Error())
	return state.MetaSet(ctx, db, MetaKeyBranchTransitionNeedsAttention,
		intentRepairRecoveryAttentionPrefix+detail)
}

func clearIntentRepairRecoveryAttention(
	ctx context.Context,
	db *state.DB,
) error {
	value, ok, err := state.MetaGet(
		ctx, db, MetaKeyBranchTransitionNeedsAttention)
	if err != nil || !ok ||
		!strings.HasPrefix(value, intentRepairRecoveryAttentionPrefix) {
		return err
	}
	return state.MetaSet(
		ctx, db, MetaKeyBranchTransitionNeedsAttention, "")
}

func withIntentRejectsWriter(ctx context.Context, gitDir string) context.Context {
	return ai.WithIntentRejectsWriter(ctx,
		ai.NewIntentRejectsWriter(filepath.Join(gitDir, "acd"), time.Now))
}

type daemonLogContext struct {
	mu         sync.RWMutex
	branchRef  string
	generation int64
}

func newDaemonLogger(logger *slog.Logger, opts Options) (*slog.Logger, *daemonLogContext) {
	repoHash := opts.RepoHash
	if repoHash == "" {
		var hashErr error
		repoHash, hashErr = paths.RepoHash(opts.RepoPath)
		if hashErr != nil {
			repoHash = central.CanonicalID(opts.RepoPath)
		}
	}
	logContext := &daemonLogContext{}
	return slog.New(&daemonContextHandler{
		next: logger.With(
			"repo_hash", repoHash,
			"worktree", opts.RepoPath,
			"git_dir", opts.GitDir,
		).Handler(),
		context: logContext,
	}), logContext
}

func logPublicationDrainTransition(
	logger *slog.Logger,
	from state.PublicationDrain,
	drain state.PublicationDrain,
) {
	if logger == nil || (drain.Phase == from.Phase &&
		drain.FallbackMode == from.FallbackMode) {
		return
	}
	logger.Info("publication drain recovery transition",
		"drain_id", drain.ID,
		"from_phase", from.Phase,
		"to_phase", drain.Phase,
		"from_mode", from.FallbackMode,
		"to_mode", drain.FallbackMode,
		"resolved_events", drain.PublishedEventCount,
		"target_events", drain.TargetEventCount,
		"reason", drain.LastError)
}

func reconcileResolvedPublicationDrains(
	ctx context.Context,
	db *state.DB,
	logger *slog.Logger,
	trigger string,
	now time.Time,
) (int, error) {
	candidates, err := state.ResolvedPublicationDrainCandidates(ctx, db.ReadSQL())
	if err != nil {
		return 0, err
	}
	if len(candidates) == 0 {
		return 0, nil
	}
	reconciled, err := state.ReconcileResolvedPublicationDrains(
		ctx, db, float64(now.UnixNano())/1e9)
	if err != nil {
		return 0, err
	}
	for _, drain := range reconciled {
		if logger != nil {
			logger.Info("completed resolved publication drain",
				"drain_id", drain.ID,
				"from_phase", drain.PreviousPhase,
				"resolved_events", drain.ResolvedEvents,
				"target_events", drain.TargetEvents,
				"trigger", trigger)
		}
	}
	return len(reconciled), nil
}

func (c *daemonLogContext) SetBranch(branchRef string, generation int64) {
	c.mu.Lock()
	c.branchRef = branchRef
	c.generation = generation
	c.mu.Unlock()
}

func (c *daemonLogContext) Attrs() []slog.Attr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	attrs := make([]slog.Attr, 0, 2)
	if c.branchRef != "" {
		attrs = append(attrs, slog.String("branch_ref", c.branchRef))
	}
	if c.generation > 0 {
		attrs = append(attrs, slog.Int64("branch_generation", c.generation))
	}
	return attrs
}

type daemonContextHandler struct {
	next    slog.Handler
	context *daemonLogContext
}

func (h *daemonContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *daemonContextHandler) Handle(ctx context.Context, record slog.Record) error {
	present := make(map[string]struct{}, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		present[attr.Key] = struct{}{}
		return true
	})
	for _, attr := range h.context.Attrs() {
		if _, exists := present[attr.Key]; !exists {
			record.AddAttrs(attr)
		}
	}
	return h.next.Handle(ctx, record)
}

func (h *daemonContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &daemonContextHandler{next: h.next.WithAttrs(attrs), context: h.context}
}

func (h *daemonContextHandler) WithGroup(name string) slog.Handler {
	return &daemonContextHandler{next: h.next.WithGroup(name), context: h.context}
}

func progressHeartbeatInterval(opts Options) time.Duration {
	if opts.progressHeartbeatEvery > 0 {
		return opts.progressHeartbeatEvery
	}
	return daemonProgressHeartbeatInterval
}

// runWithProgressHeartbeat keeps the controller-visible heartbeat fresh while
// work blocks inside protection or recovery. The deferred stop always joins the
// helper goroutine, including error returns, cancellation, and panic unwinds.
func runWithProgressHeartbeat(
	ctx context.Context,
	interval time.Duration,
	heartbeat func(),
	work func(),
) {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				heartbeat()
			}
		}
	}()
	defer func() {
		close(done)
		<-stopped
	}()
	work()
}

func replayNeedsImmediateFollowup(sum ReplaySummary) bool {
	if sum.Published > 0 || sum.RecaptureRequired {
		return true
	}
	// A transient wait can retain a frozen publication target and therefore
	// report HasMore, but rerunning the full capture/replay loop immediately
	// cannot advance it. Let the idle scheduler back off until the provider,
	// verification resource, settle window, or another wake becomes ready.
	return sum.HasMore && sum.Disposition != ReplayDispositionTransientWait
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

// GitOperationInProgress is the exported wrapper around gitOperationInProgress
// used by CLI helpers that need to refuse running while a git operation is
// active in <gitDir>. Returns the human-readable marker name (e.g. "merge",
// "rebase-merge") and true when a marker is present.
func GitOperationInProgress(gitDir string) (string, bool) {
	return gitOperationInProgress(gitDir)
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
			// A non-ErrNotExist stat error (EACCES, EIO, transient
			// filesystem hiccup) MUST NOT latch capture/replay into
			// permanent pause. The previous implementation returned
			// (name, true) on any such error, which meant a single
			// EACCES on .git/MERGE_HEAD wedged the daemon forever (no
			// auto-clear path; only the reverse "marker absent" branch
			// in the run loop clears the operation gate). Treat the
			// marker as absent for this tick — the next tick will
			// re-stat and observe whatever state actually exists.
			//
			// We log via slog.Default rather than a closure-bound
			// logger so this helper stays pure and easy to call from
			// tests; the run loop's own pause/resume warns will still
			// surface a real operation if the marker really is there.
			slog.Default().Warn("git operation marker stat error; treating as absent for this tick",
				"marker", marker.path, "err", err.Error())
			continue
		}
	}
	return "", false
}

// openCentralStats opens (or creates) the central stats.db at the given
// absolute path. Wraps central.OpenAt so the daemon package owns the
// "open + log + skip" policy without re-implementing the bootstrap dance.
func openCentralStats(ctx context.Context, dbPath string) (*central.StatsDB, error) {
	return central.OpenAt(ctx, dbPath)
}

func manualPauseRecentlyResumed(ctx context.Context, db *state.DB, now time.Time) (bool, string) {
	raw, ok, err := state.MetaGet(ctx, db, MetaKeyManualPauseResumedAt)
	if err != nil || !ok || raw == "" {
		return false, ""
	}
	sec, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		_, _ = state.MetaDelete(ctx, db, MetaKeyManualPauseResumedAt)
		return false, raw
	}
	resumedAt := time.Unix(0, int64(sec*1e9))
	if now.Before(resumedAt) || now.Sub(resumedAt) <= manualResumeResyncWindow {
		return true, raw
	}
	_, _ = state.MetaDelete(ctx, db, MetaKeyManualPauseResumedAt)
	return false, raw
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

// sweepOrphanAckedFlushRequests marks any flush_request that has been in the
// "acknowledged" state past `threshold` as failed. It is invoked once per
// daemon boot.
//
// The drain in the run loop transitions a row pending -> acknowledged ->
// completed atomically: if the daemon dies between those two writes the row
// is left in "acknowledged" with no completed_ts, and `acd status` /
// PendingFlushDepth-style probes count it forever. The sweep is the only
// release valve since CompleteFlushRequest doesn't expose a "fail by id +
// timeout" API and the task forbids touching the state package.
//
// Returns the number of rows updated. Best-effort: log + continue on err.
func sweepOrphanAckedFlushRequests(ctx context.Context, db *state.DB, now time.Time, threshold time.Duration) (int64, error) {
	if db == nil || threshold <= 0 {
		return 0, nil
	}
	cutoff := float64(now.Add(-threshold).UnixNano()) / 1e9
	const q = `
UPDATE flush_requests
SET status = 'failed',
    completed_ts = ?,
    note = COALESCE(note, '') || ' [orphan-acked sweep]'
WHERE status = 'acknowledged'
  AND acknowledged_ts IS NOT NULL
  AND acknowledged_ts <= ?`
	res, err := db.SQL().ExecContext(ctx, q, float64(now.UnixNano())/1e9, cutoff)
	if err != nil {
		return 0, fmt.Errorf("daemon: sweep orphan acked flush requests: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("daemon: rows affected: %w", err)
	}
	return n, nil
}

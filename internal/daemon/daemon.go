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
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
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
	// afterSelfPublicationAdoption is a test-only observation point after the
	// journal-proved target and all in-memory/durable token fields agree.
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
	logger, logContext := newDaemonLogger(logger, opts)
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
	promptTracer := opts.PromptTrace
	if promptTracer == nil {
		var err error
		promptTracer, err = prompttrace.NewFromEnv(opts.RepoPath, opts.GitDir)
		if err != nil {
			logger.Warn("initialize prompt trace writer", "err", err.Error())
		}
	}
	if promptTracer != nil {
		defer func() {
			if err := promptTracer.Close(); err != nil {
				logger.Warn("close prompt trace writer", "err", err.Error())
			}
		}()
	}
	// MessageFn precedence: explicit MessageFn > injected MessageProvider
	// > env-driven ai.BuildProvider > deterministic. The closer returned
	// by ai.BuildProvider (only non-nil for subprocess plugins) is owned
	// by Run and Closed on graceful shutdown.
	//
	// closeProviderOnce bounds the closer at providerCloseTimeout. A
	// subprocess AI provider that wedges on Close (deadlocked stdin
	// drain, hung handshake, etc.) must not stall daemon shutdown. On
	// timeout we attempt Process.Kill via the optional processExposer
	// interface (subprocess plugins should expose it); otherwise we log
	// and proceed. Either way the caller observes a bounded shutdown.
	var providerCloser io.Closer
	closeTimeout := opts.providerCloseTimeout
	if closeTimeout <= 0 {
		closeTimeout = providerCloseTimeout
	}
	closeProviderOnce := func() {
		if providerCloser == nil {
			return
		}
		closer := providerCloser
		providerCloser = nil
		closeDone := make(chan error, 1)
		go func() { closeDone <- closer.Close() }()
		select {
		case err := <-closeDone:
			if err != nil {
				logger.Warn("close ai provider", "err", err.Error())
			}
		case <-time.After(closeTimeout):
			logger.Warn("close ai provider timed out; force-killing if possible",
				"timeout", closeTimeout.String())
			if pe, ok := closer.(processExposer); ok {
				if proc := pe.Process(); proc != nil {
					if err := proc.Kill(); err != nil {
						logger.Warn("ai provider kill failed",
							"err", err.Error())
					}
				}
			}
			// Don't wait for closeDone — by definition the goroutine
			// is still hung. Leak it; the kernel reaps once the
			// underlying syscall unblocks.
		}
	}
	defer closeProviderOnce()

	if _, ok := os.LookupEnv("ACD_AI_SEND_DIFF"); ok {
		logger.Warn("ACD_AI_SEND_DIFF is deprecated and ignored; diff egress is now opt-in via ACD_AI_DIFF_EGRESS=1",
			slog.String("env", "ACD_AI_SEND_DIFF"))
	}

	providerCfg := ai.LoadProviderConfigFromEnv()
	providerCfg.Logger = logger
	var runtimeCredentialStore *credentials.Store
	runtimeRoots, rootsErr := paths.Resolve()
	if rootsErr != nil {
		logger.Warn("resolve runtime configuration roots",
			"err", ai.SanitizePlannerError(rootsErr.Error()))
	} else {
		store := credentials.NewStore(runtimeRoots)
		runtimeCredentialStore = &store
		key, _, credentialErr := credentials.Resolve(store, os.LookupEnv)
		if credentialErr != nil {
			logger.Warn("load protected provider credential",
				"err", ai.SanitizePlannerError(credentialErr.Error()))
		} else {
			providerCfg.APIKey = key
		}
	}
	// Bind this run's reject writer to its context before any planner call can
	// fire. Linked worktrees share a process but retain distinct Git-dir logs.
	// Writes remain best-effort and require no provider reconfiguration.
	ctx = withIntentRejectsWriter(ctx, opts.GitDir)
	// Resolve ACD_PATH_QUIESCENCE_SECONDS once at startup so capture's
	// hot-path RecordPathWrite gate is set before any capture pass runs.
	// The replay loop also resolves the env per-pass, but doing it here
	// ensures a daemon spawned with a positive value never sees a window
	// where capture skips the stamp before the first replay tick.
	_ = resolvePathQuiescenceSeconds()
	if err := state.MetaSetMany(ctx, opts.DB, map[string]string{
		"commit.format":          string(providerCfg.CommitFormat),
		"intent.window":          strconv.Itoa(providerCfg.IntentWindow),
		"intent.min_pending":     strconv.Itoa(providerCfg.IntentMinPending),
		"intent.settle_window":   providerCfg.IntentSettleWindow.String(),
		"intent.max_pending_age": providerCfg.IntentMaxPendingAge.String(),
		"intent.recent_commits":  strconv.Itoa(providerCfg.IntentRecentCommits),
		"intent.defer_limit":     strconv.Itoa(providerCfg.IntentDeferLimit),
		"intent.diff_egress":     strconv.FormatBool(diffEgressOptIn()),
	}); err != nil {
		logger.Warn("stamp commit strategy metadata", "err", err.Error())
	}

	msgFn := opts.MessageFn
	provider := opts.MessageProvider
	needsProvider := msgFn == nil || (providerCfg.CommitStrategy == ai.CommitStrategyIntent && opts.IntentPlanner == nil)
	if provider == nil && needsProvider {
		built, closer, err := ai.BuildProvider(providerCfg)
		if err != nil {
			logger.Warn("build ai provider; falling back to deterministic",
				"err", err.Error())
			provider = ai.DeterministicProvider{CommitFormat: providerCfg.CommitFormat}
		} else {
			provider = built
			providerCloser = closer
		}
		logger.Info("ai provider selected",
			"provider", provider.Name(),
			"mode", providerCfg.Mode)
	} else if provider != nil && opts.MessageProviderCloser != nil {
		providerCloser = opts.MessageProviderCloser
	}
	if provider == nil {
		provider = ai.DeterministicProvider{CommitFormat: providerCfg.CommitFormat}
	}

	if msgFn == nil {
		// Diff egress is OFF by default. Network-bound providers receive
		// only metadata (paths + op kinds + branch + timestamp) unless the
		// operator explicitly opts in via ACD_AI_DIFF_EGRESS. Reason: the
		// reconstructed unified diff carries source bytes; redaction is
		// pattern-based and best-effort; an unset default that silently
		// transmits diffs would be a privacy regression on upgrade. When
		// the opt-in is missing for a provider that wants diffs, surface a
		// one-shot warn so operators see what they need to set.
		effectiveRepoRoot := opts.RepoPath
		if ai.ProviderNeedsDiff(provider) && !diffEgressOptIn() {
			effectiveRepoRoot = ""
			logger.Warn("AI provider supports diff context but ACD_AI_DIFF_EGRESS=1 is not set; sending metadata only",
				"provider", provider.Name())
		}
		msgFn = providerMessageFnWithPromptTrace(provider, effectiveRepoRoot, promptTracer)
	}

	// Build or inject the intent planner once per Run. Replay receives this
	// same instance on every pass, so subprocess sessions and HTTP transports
	// are reused and closed exactly once with the message provider.
	runIntentPlanner := opts.IntentPlanner
	if providerCfg.CommitStrategy == ai.CommitStrategyIntent && runIntentPlanner == nil {
		if planner, ok := provider.(ai.IntentPlanner); ok {
			runIntentPlanner = planner
		} else {
			logger.Warn("AI provider does not implement intent planning; falling back to deterministic",
				"provider", provider.Name())
			runIntentPlanner = ai.DeterministicProvider{CommitFormat: providerCfg.CommitFormat}
		}
	}
	var (
		intentHealth          *IntentPlannerHealth
		intentHealthOptions   IntentPlannerHealthOptions
		intentPlannerProvider string
		intentPlannerModel    string
	)
	if runIntentPlanner != nil {
		intentPlannerProvider = ai.PrimaryProviderName(runIntentPlanner)
		if intentPlannerProvider == "openai-compat" {
			intentPlannerModel = providerCfg.Model
		}
		deterministic := intentPlannerProvider == (ai.DeterministicProvider{}).Name()
		intentHealthOptions = IntentPlannerHealthOptions{
			Provider: IntentPlannerProviderIdentity{
				Provider:         intentPlannerProvider,
				Model:            intentPlannerModel,
				Endpoint:         providerCfg.BaseURL,
				TrustFingerprint: runtimeTrustFingerprint(providerCfg.CAFile),
				Deterministic:    deterministic,
			},
			Now: now,
		}
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

	// 1. Acquire daemon.lock. A supervisor worker may already own the one
	// common-directory lock for every linked worktree it manages.
	if !opts.SkipDaemonLock {
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
	}
	cutoverBlock := ""
	if rootsErr == nil {
		if cutover, cutoverErr := EnsureIntentV2RuntimeCutover(
			ctx, opts.DB, opts.RepoPath, runtimeRoots, os.LookupEnv); cutoverErr != nil {
			cutoverBlock = runtimeConfigureReason(
				"the Intent v2 runtime cutover could not be completed")
			logger.Warn("Intent v2 runtime cutover needs attention",
				"err", ai.SanitizePlannerError(cutoverErr.Error()))
			_ = setRuntimeMetaIfChanged(ctx, opts.DB, map[string]string{
				metaIntentV2MigrationState:  "needs_attention",
				"intent.v2.needs_attention": cutoverBlock,
			})
		} else if cutover.Migrated {
			logger.Info("materialized Intent v2 runtime revision",
				"revision_id", cutover.RevisionID,
				"preset_id", cutover.PresetID,
				"preset_version", cutover.PresetVersion,
				"customized", cutover.Customized)
		}
	} else if required, ok, metaErr := state.MetaGet(
		ctx, opts.DB, metaIntentV2CutoverRequired,
	); metaErr != nil || ok && parseRuntimeBool(required) {
		cutoverBlock = runtimeConfigureReason(
			"the Intent v2 runtime cutover could not resolve configuration storage")
		_ = setRuntimeMetaIfChanged(ctx, opts.DB, map[string]string{
			metaIntentV2MigrationState:  "needs_attention",
			"intent.v2.needs_attention": cutoverBlock,
		})
	}
	if runIntentPlanner != nil {
		intentHealth = NewIntentPlannerHealth(ctx, opts.DB, intentHealthOptions)
	}
	initialIdentity := intentHealthOptions.Provider
	initialFingerprint := IntentPlannerProviderFingerprint(initialIdentity)
	initialPreset := config.PresetFast
	initialPresetID := "event.fast"
	initialReplayBlock := configuredRuntimeReplayBlock(ctx, opts.DB)
	if providerCfg.CommitStrategy == ai.CommitStrategyIntent {
		initialPreset = config.PresetBalanced
		initialPresetID = "intent.balanced"
		if initialReplayBlock == "" {
			initialReplayBlock = runtimeConfigureReason(
				"an immutable Intent v2 runtime revision is not active")
		}
	}
	if cutoverBlock != "" {
		initialReplayBlock = cutoverBlock
	}
	initialBundle := &RuntimeBundle{
		Provider: provider, ProviderCloser: providerCloser, MessageFn: msgFn,
		IntentPlanner: runIntentPlanner, IntentHealth: intentHealth,
		HealthIdentity: initialIdentity, HealthFingerprint: initialFingerprint,
		Model: intentPlannerModel, DiffEgress: providerCfg.DiffEgress,
		CommitStrategy:       providerCfg.CommitStrategy,
		CommitFormat:         providerCfg.CommitFormat,
		PresetID:             initialPresetID,
		PresetVersion:        config.PresetCatalogVersion,
		IntentPreset:         initialPreset,
		ReplayBlockedReason:  initialReplayBlock,
		IntentRetryLimit:     resolvedIntentRetryLimit(),
		IntentWindow:         providerCfg.IntentWindow,
		IntentMinPending:     providerCfg.IntentMinPending,
		IntentSettleWindow:   providerCfg.IntentSettleWindow,
		IntentMaxPendingAge:  providerCfg.IntentMaxPendingAge,
		IntentRecentCommits:  providerCfg.IntentRecentCommits,
		IntentDeferLimit:     providerCfg.IntentDeferLimit,
		IntentPathCoalescing: pathCoalesceEnabled(),
	}
	runtimeBundles := NewRuntimeBundleManager(initialBundle, RuntimeBundleBuilder{
		DB: opts.DB, RepoRoot: opts.RepoPath, PromptTrace: promptTracer,
		Logger: logger, Now: now,
		BuildProvider:   opts.runtimeBuildProvider,
		CredentialStore: runtimeCredentialStore, LookupEnv: os.LookupEnv,
	}, closeTimeout)
	// RuntimeBundleManager now owns the initial closer. Leave the legacy
	// deferred guard installed with a nil target for compatibility with early
	// returns above this point.
	providerCloser = nil
	defer runtimeBundles.Close()
	if cutoverBlock == "" {
		if err := runtimeBundles.ActivateDesired(ctx); err != nil {
			logger.Warn("activate Intent v2 runtime revision",
				"err", ai.SanitizePlannerError(err.Error()))
		}
	}
	currentRuntime := runtimeBundles.Current()
	if currentRuntime != nil && currentRuntime.ReplayBlockedReason != "" {
		_ = setRuntimeMetaIfChanged(ctx, opts.DB, map[string]string{
			"intent.v2.needs_attention": currentRuntime.ReplayBlockedReason,
			metaIntentV2MigrationState:  "needs_attention",
			"intent.v2.preset_id":       currentRuntime.PresetID,
			"intent.v2.preset_version":  strconv.Itoa(currentRuntime.PresetVersion),
		})
	} else if currentRuntime != nil && currentRuntime.RevisionID > 0 {
		_ = setRuntimeMetaIfChanged(ctx, opts.DB, map[string]string{
			"intent.v2.needs_attention": "",
			metaIntentV2MigrationState:  "active",
			"intent.v2.preset_id":       currentRuntime.PresetID,
			"intent.v2.preset_version":  strconv.Itoa(currentRuntime.PresetVersion),
		})
	}

	// 1a. Orphan flush_request sweep. Rows that sat in "acknowledged" past
	// OrphanFlushAckThreshold are presumed orphans from a previous daemon
	// crash between ClaimNextFlushRequest and CompleteFlushRequest. Mark
	// them failed so `acd status` / queue depth probes do not show ghost
	// requests forever. Best-effort — log and continue on failure.
	if n, err := sweepOrphanAckedFlushRequests(ctx, opts.DB, now(), OrphanFlushAckThreshold); err != nil {
		logger.Warn("sweep orphan acked flush requests", "err", err.Error())
	} else if n > 0 {
		logger.Info("swept orphan acknowledged flush requests", "rows", n)
	}

	// 1b. Rewind-grace clamp. The pause marker is persisted as a wall-clock
	// RFC3339 timestamp; if the host clock jumped forward between
	// maybeSetRewindGrace writing the marker and the daemon restarting (or
	// jumped backward and was later corrected), the marker can sit far
	// beyond the configured grace window. Cap to 2*grace; legitimate values
	// stay untouched.
	rewindGrace := resolveRewindGrace()
	if opts.RewindGrace != nil {
		rewindGrace = *opts.RewindGrace
	}
	shadowRetention := resolveShadowRetentionGenerations()
	if opts.ShadowRetentionGenerations != nil {
		shadowRetention = *opts.ShadowRetentionGenerations
	}
	if clamped, original, replacement, err := clampRewindGraceAtStartupWithDuration(ctx, opts.DB, now(), rewindGrace); err != nil {
		logger.Warn("clamp rewind grace meta at startup", "err", err.Error())
	} else if clamped {
		logger.Warn("clamped wall-clock-skewed rewind grace marker at startup",
			"original", original, "replacement", replacement)
	}

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
	checkpointStore := checkpointpkg.Store{DB: opts.DB}
	if err := checkpointStore.RecoverPrepared(ctx, opts.RepoPath); err != nil {
		return fmt.Errorf("daemon: recover protection checkpoints: %w", err)
	}
	if err := checkpointStore.RecoverRetention(ctx, opts.RepoPath); err != nil {
		return fmt.Errorf("daemon: recover checkpoint retention: %w", err)
	}
	if _, err := reconcileResolvedPublicationDrains(
		ctx, opts.DB, logger, "worker_startup", now(),
	); err != nil {
		return fmt.Errorf("daemon: reconcile resolved publication drains: %w", err)
	}
	// Do not advertise running until every crash journal has been recovered;
	// setup and status use this stamp as the worker readiness barrier.
	heartbeatNow("running", "daemon started")
	var shutdownCh <-chan struct{}
	recoveryRootCtx := ctx
	recoverSelfPublicationsPass := func(
		rootCtx context.Context,
		recoveryCtx CaptureContext,
	) (SelfPublicationRecoverySummary, error) {
		// One journal per invocation keeps the run loop available to drain
		// flush requests between recovery attempts. The timeout bounds even
		// a slow Git proof, while the progress heartbeat preserves the
		// controller's three-second liveness contract during that proof.
		passCtx, passCancel := context.WithTimeout(
			rootCtx, 5*time.Second)
		defer passCancel()
		recover := RecoverSelfPublications
		if opts.recoverSelfPublications != nil {
			recover = opts.recoverSelfPublications
		}
		var summary SelfPublicationRecoverySummary
		var recoverErr error
		runWithProgressHeartbeat(passCtx, progressHeartbeatInterval(opts), func() {
			heartbeatNow("running", "")
		}, func() {
			summary, recoverErr = recover(
				passCtx, opts.RepoPath, opts.DB, recoveryCtx,
				ReplayOpts{Limit: 1})
		})
		heartbeatNow("running", "")
		return summary, recoverErr
	}

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
	rawShutdownCh := opts.ShutdownCh
	if rawShutdownCh == nil && sig != nil {
		rawShutdownCh = sig.Shutdown
	}
	shutdownPendingAtStart := false
	select {
	case <-rawShutdownCh:
		shutdownPendingAtStart = true
	default:
	}
	recoveryRootCtx, recoveryCancel := context.WithCancel(ctx)
	shutdownBroadcast := make(chan struct{})
	shutdownBridgeStop := make(chan struct{})
	shutdownBridgeDone := make(chan struct{})
	go func() {
		defer close(shutdownBridgeDone)
		if shutdownPendingAtStart {
			recoveryCancel()
			close(shutdownBroadcast)
			return
		}
		select {
		case <-rawShutdownCh:
			recoveryCancel()
			close(shutdownBroadcast)
		case <-ctx.Done():
			recoveryCancel()
			close(shutdownBroadcast)
		case <-shutdownBridgeStop:
		}
	}()
	defer func() {
		close(shutdownBridgeStop)
		recoveryCancel()
		<-shutdownBridgeDone
	}()
	shutdownCh = shutdownBroadcast
	validationWakeCh := make(chan struct{}, 1)

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
	branchGenerationToken := BranchGenerationToken
	if opts.branchGenerationToken != nil {
		branchGenerationToken = opts.branchGenerationToken
	}
	currentToken, terr := branchGenerationToken(ctx, opts.RepoPath)
	if terr != nil {
		logger.Warn("seed branch token", "err", terr.Error())
		currentToken = ""
	} else {
		// Derive startup context from the same token observation. A checkout
		// can race the earlier resolveBranch call; mixing those two samples
		// would accept an attached token with a detached capture context (or
		// vice versa).
		branchRef = tokenBranchRef(currentToken)
		headOID = tokenSHA(currentToken)
	}
	logContext.SetBranch(branchRef, persistedGen)
	startupPublicationBlocked := false
	startupReattachedFromDetached := false
	if branchRef != "" && headOID != "" {
		// A detached marker alongside an attached current token means HEAD
		// reattached before startup finished sampling it. In that narrow
		// case, the rewind-grace row belongs to the stale detached state.
		// Let exact self-publication proof run, but retain both markers until
		// the branch transition is accepted below.
		_, detachedMarkerExists, detachedErr :=
			state.MetaGet(ctx, opts.DB, MetaKeyDetachedHeadPaused)
		if detachedErr != nil {
			logger.Warn("read detached marker before startup self-publication recovery",
				"err", detachedErr.Error())
			startupPublicationBlocked = true
		}
		if detachedMarkerExists {
			persistedToken, tokenOK, tokenErr :=
				state.MetaGet(ctx, opts.DB, MetaKeyBranchToken)
			if tokenErr != nil {
				logger.Warn("read branch token before startup self-publication recovery",
					"err", tokenErr.Error())
				startupPublicationBlocked = true
			} else {
				startupReattachedFromDetached = tokenOK &&
					tokenBranchRef(persistedToken) == "" &&
					tokenSHA(persistedToken) != ""
				if tokenOK && tokenBranchRef(persistedToken) != "" {
					// An attached durable token proves this marker is stale.
					// Remove only the detached marker; any active rewind grace
					// belongs to the attached history and must remain intact.
					_, _ = state.MetaDelete(
						ctx, opts.DB, MetaKeyDetachedHeadPaused)
				}
			}
		}
		if opts.PublicationHeld != nil && opts.PublicationHeld() {
			logger.Info("startup self-publication recovery deferred by publication hold")
			startupPublicationBlocked = true
		} else if operation, active := gitOperationInProgress(opts.GitDir); active {
			logger.Info("startup self-publication recovery deferred during git operation",
				"operation", operation)
			startupPublicationBlocked = true
		} else if pauseStatus, pauseErr :=
			daemonPauseState(ctx, opts.GitDir, opts.DB); pauseErr != nil {
			logger.Warn("read pause state before startup self-publication recovery",
				"err", pauseErr.Error())
			startupPublicationBlocked = true
		} else if pauseStatus.Active &&
			!(pauseStatus.Source == "rewind_grace" &&
				startupReattachedFromDetached) {
			logger.Info("startup self-publication recovery deferred while paused",
				"source", pauseStatus.Source, "reason", pauseStatus.Reason)
			startupPublicationBlocked = true
		} else if !startupPublicationBlocked {
			recoveryCtx := CaptureContext{
				BranchRef: branchRef, BranchGeneration: persistedGen,
				BaseHead: persistedHead,
			}
			// A shutdown already pending before startup still permits crash
			// convergence. A shutdown arriving during this proof cancels it
			// promptly through recoveryRootCtx.
			startupRecoveryRootCtx := recoveryRootCtx
			if shutdownPendingAtStart {
				startupRecoveryRootCtx = ctx
			}
			recovered, recoverErr := recoverSelfPublicationsPass(
				startupRecoveryRootCtx, recoveryCtx)
			if recoverErr != nil {
				logger.Warn("recover self-publications before startup branch transition",
					"err", recoverErr.Error())
				startupPublicationBlocked = true
			} else if recovered.FinalTargetOID != "" {
				// CompleteSelfPublication already persisted the exact journal
				// target and branch token in the same SQLite transaction. Seed
				// startup's previous state from that proved target so any
				// subsequent external movement is classified from the internal
				// publication boundary instead of the stale pre-crash head.
				persistedHead = recovered.FinalTargetOID
				logger.Info("recovered self-publication at startup",
					"target", recovered.FinalTargetOID,
					"completed", recovered.Completed,
					"abandoned", recovered.Abandoned)
			}
			if recovered.HasMore {
				logger.Info("startup self-publication recovery has more work")
				startupPublicationBlocked = true
			}
		}
	}
	startupRepairBlocked := startupPublicationBlocked
	if !startupPublicationBlocked && branchRef != "" && headOID != "" {
		recoverable, recoverErr := state.RecoverableIntentRepairs(
			ctx, opts.DB, intentRepairBackupCap)
		if recoverErr != nil {
			logger.Warn("inspect recoverable intent repairs at startup",
				"err", recoverErr.Error())
			startupRepairBlocked = true
		} else if len(recoverable) > 0 {
			recoveryCtx := CaptureContext{
				BranchRef: branchRef, BranchGeneration: persistedGen,
				BaseHead: headOID,
			}
			recovered, recoverErr := RecoverIntentRepairs(
				ctx, opts.RepoPath, opts.GitDir, opts.DB, recoveryCtx)
			if recoverErr != nil {
				logger.Warn("recover intent repairs before branch transition",
					"err", recoverErr.Error())
				startupRepairBlocked = true
			} else {
				completed := false
				for _, repair := range recovered {
					if repair.Status == state.IntentRepairCompleted {
						completed = true
						logger.Info("recovered intent repair at startup",
							"repair_id", repair.ID, "new_head", repair.NewHead)
					}
				}
				if completed {
					persistedHead = headOID
					if err := state.MetaSetMany(ctx, opts.DB, map[string]string{
						MetaKeyBranchGeneration: strconv.FormatInt(persistedGen, 10),
						MetaKeyBranchHead:       headOID,
						MetaKeyBranchToken:      currentToken,
					}); err != nil {
						logger.Warn("accept recovered intent repair metadata",
							"err", err.Error())
						startupRepairBlocked = true
					}
				}
			}
		}
	}
	branchTransitionBlocked := startupRepairBlocked
	startupPreviousToken := currentToken
	startupPreviousGeneration := persistedGen
	startupTokenChanged := false
	startupTransition := TokenTransitionUnchanged
	startupChangedAt := ""
	startupShadowMutated := false
	startupShadowRefreshRequired := false
	if persistedHead != "" && currentToken != "" {
		prevToken := "rev:" + persistedHead
		if persistedToken, ok, err := state.MetaGet(ctx, opts.DB, MetaKeyBranchToken); err != nil {
			logger.Warn("load persisted branch token", "err", err.Error())
		} else if ok && persistedToken != "" {
			prevToken = persistedToken
		}
		startupPreviousToken = prevToken
		transition, cErr := ClassifyTokenTransition(ctx, opts.RepoPath, prevToken, currentToken)
		if cErr == nil {
			startupTransition = transition
		}
		if cErr != nil {
			logger.Warn("classify startup branch transition; will retry",
				"err", cErr.Error())
			currentToken = prevToken
			branchTransitionBlocked = true
		} else if transition != TokenTransitionUnchanged {
			if startupPublicationBlocked {
				logger.Info("startup branch transition deferred for ambiguous self-publication")
			} else if operation, active := gitOperationInProgress(opts.GitDir); active {
				logger.Info("startup branch transition deferred during git operation",
					"operation", operation)
				branchTransitionBlocked = true
			} else if pauseStatus, perr := daemonPauseState(ctx, opts.GitDir, opts.DB); perr != nil {
				logger.Warn("read pause state before startup branch reconciliation; will retry",
					"err", perr.Error())
				branchTransitionBlocked = true
			} else if pauseStatus.Active && pauseStatus.Source == "manual" {
				logger.Info("startup branch transition deferred while manually paused",
					"reason", pauseStatus.Reason)
				branchTransitionBlocked = true
			} else if result, rErr := reconcileTransitionPair(ctx, opts.RepoPath, opts.GitDir,
				opts.DB, tokenBranchRef(prevToken), persistedGen,
				tokenSHA(currentToken) == "", "", "startup_branch_transition", tracer); rErr != nil {
				logger.Warn("reconcile unpublished chain before startup branch transition; will retry",
					"branch_ref", tokenBranchRef(prevToken),
					"generation", persistedGen,
					"err", rErr.Error())
				branchTransitionBlocked = true
			} else if result.Handled {
				logger.Info("reconciled unpublished chain before startup branch transition",
					"branch_ref", tokenBranchRef(prevToken),
					"generation", persistedGen,
					"outcome", result.Outcome,
					"events", result.EventCount,
					"recovery_ref", result.RecoveryRef)
			}
			if !branchTransitionBlocked {
				verifiedToken, verifyErr := branchGenerationToken(ctx, opts.RepoPath)
				if verifyErr != nil {
					logger.Warn("verify startup branch token after reconciliation; will retry",
						"err", verifyErr.Error())
					branchTransitionBlocked = true
				} else if verifiedToken != currentToken {
					logger.Info("startup branch token changed during reconciliation; will retry",
						"observed", currentToken, "verified", verifiedToken)
					branchTransitionBlocked = true
				}
			}
			if branchTransitionBlocked {
				branchRef = tokenBranchRef(prevToken)
				headOID = persistedHead
				currentToken = prevToken
			}
		}
		if !branchTransitionBlocked && transition == TokenTransitionDiverged {
			rewindPaused, rewindUntil, rewindErr := maybeSetRewindGraceWithDuration(ctx, opts.RepoPath, opts.DB, prevToken, currentToken, now(), rewindGrace)
			if rewindErr != nil {
				logger.Warn("detect startup rewind grace", "err", rewindErr.Error())
			} else if rewindPaused {
				logger.Info("replay paused after startup branch rewind", "until", rewindUntil)
			}
			persistedGen++
			ts := strconv.FormatFloat(float64(now().UnixNano())/1e9, 'f', -1, 64)
			startupTokenChanged = true
			startupChangedAt = ts
			logger.Info("branch generation prepared at startup",
				"old", prevToken, "new", currentToken,
				"generation", persistedGen,
				"transition", transition.String())
		} else if !branchTransitionBlocked && transition == TokenTransitionFastForward {
			startupTokenChanged = true
			startupChangedAt = strconv.FormatFloat(float64(now().UnixNano())/1e9, 'f', -1, 64)
		}
	}
	cctx := CaptureContext{
		BranchRef:        branchRef,
		BranchGeneration: persistedGen,
		BaseHead:         headOID,
	}
	logContext.SetBranch(cctx.BranchRef, cctx.BranchGeneration)
	// Seed shadow_paths from HEAD before the first capture so files
	// already at HEAD don't generate spurious creates.
	if !branchTransitionBlocked && cctx.BranchRef != "" && cctx.BaseHead != "" {
		seedShadow := BootstrapShadow
		seedReason := "startup shadow bootstrap"
		if startupTokenChanged {
			seedShadow = ReseedShadowFromHead
			seedReason = "startup branch transition shadow reseed"
			startupShadowMutated = true
		}
		if seeded, err := seedShadow(ctx, opts.RepoPath, opts.DB, cctx); err != nil {
			logger.Warn("bootstrap shadow", "err", err.Error())
			traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", err.Error(), 0)
			branchTransitionBlocked = true
		} else {
			traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), seedReason, seeded)
			if seeded > 0 {
				logger.Info("shadow bootstrapped", "rows", seeded)
			}
		}
	}
	if !branchTransitionBlocked {
		verifiedToken, verifyErr := branchGenerationToken(ctx, opts.RepoPath)
		if verifyErr != nil {
			logger.Warn("verify startup branch token before metadata accept; will retry",
				"err", verifyErr.Error())
			branchTransitionBlocked = true
		} else if verifiedToken != currentToken {
			logger.Info("startup branch token changed during shadow seed; will retry",
				"observed", currentToken, "verified", verifiedToken)
			branchTransitionBlocked = true
		}
	}
	if !branchTransitionBlocked {
		meta := map[string]string{
			MetaKeyBranchGeneration: strconv.FormatInt(cctx.BranchGeneration, 10),
			MetaKeyBranchHead:       cctx.BaseHead,
			MetaKeyBranchToken:      currentToken,
		}
		if startupTokenChanged {
			meta[MetaKeyBranchTokenChangedAt] = startupChangedAt
		}
		if err := state.MetaSetMany(ctx, opts.DB, meta); err != nil {
			logger.Warn("accept startup branch transition metadata; will retry", "err", err.Error())
			branchTransitionBlocked = true
		} else if startupTokenChanged {
			recordTrace(tracer, acdtrace.Event{
				Repo:       opts.RepoPath,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "branch_token.transition",
				Decision:   startupTransition.String(),
				Reason:     "startup branch transition accepted",
				Input:      map[string]any{"previous": startupPreviousToken, "current": currentToken},
				Output: map[string]any{
					"accepted":        true,
					"prev_generation": startupPreviousGeneration,
					"new_generation":  cctx.BranchGeneration,
				},
				Generation: cctx.BranchGeneration,
			})
		}
	}
	if branchTransitionBlocked && startupPreviousToken != "" {
		if startupShadowMutated && cctx.BranchRef != "" {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			cleanupErr := state.InvalidateShadowGeneration(cleanupCtx, opts.DB,
				cctx.BranchRef, cctx.BranchGeneration,
				ShadowBootstrappedKey(cctx.BranchRef, cctx.BranchGeneration))
			cleanupCancel()
			if cleanupErr != nil {
				logger.Warn("invalidate prospective startup shadow; will force refresh",
					"err", cleanupErr.Error())
				startupShadowRefreshRequired = true
			}
		}
		if startupTokenChanged {
			recordTrace(tracer, acdtrace.Event{
				Repo:       opts.RepoPath,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "branch_token.transition",
				Decision:   "rolled_back",
				Reason:     "startup branch transition was not accepted",
				Input:      map[string]any{"previous": startupPreviousToken, "current": currentToken},
				Output: map[string]any{
					"accepted":        false,
					"prev_generation": startupPreviousGeneration,
					"new_generation":  cctx.BranchGeneration,
				},
				Generation: cctx.BranchGeneration,
			})
		}
		currentToken = startupPreviousToken
		persistedGen = startupPreviousGeneration
		branchRef = tokenBranchRef(startupPreviousToken)
		headOID = tokenSHA(startupPreviousToken)
		if headOID == "" {
			headOID = persistedHead
		}
		cctx = CaptureContext{BranchRef: branchRef, BranchGeneration: persistedGen, BaseHead: headOID}
		logContext.SetBranch(cctx.BranchRef, cctx.BranchGeneration)
	}
	if !branchTransitionBlocked && cctx.BranchRef != "" {
		if transition, detachErr := syncDetachedHeadState(ctx, opts.DB, false, now()); detachErr != nil {
			logger.Warn("sync detached HEAD state at startup", "err", detachErr.Error())
		} else if transition == detachedHeadReattached {
			logger.Info("HEAD reattached; capture and publication resumed for this worktree")
			if startupReattachedFromDetached {
				clearRewindGraceMeta(ctx, opts.DB, opts.RepoPath, cctx, tracer, logger,
					"detached HEAD reattached before startup acceptance")
			}
		}
	}
	if !branchTransitionBlocked {
		if startupTokenChanged {
			_, _ = state.MetaDelete(ctx, opts.DB, MetaKeyManualPauseResumedAt)
		}
		if cctx.BranchRef != "" {
			if pruned, pErr := pruneShadowGenerationsWithRetention(ctx, opts.DB, cctx, shadowRetention); pErr != nil {
				logger.Warn("prune old shadow generations", "err", pErr.Error())
			} else if pruned > 0 {
				logger.Info("pruned old shadow generations", "rows", pruned)
			}
		}
	}
	if !branchTransitionBlocked && cctx.BranchRef != "" && cctx.BaseHead != "" {
		if repaired, err := RepairPublishedLiveIndex(ctx, opts.RepoPath, opts.DB, cctx.BaseHead, DefaultLiveIndexRepairLimit); err != nil {
			logger.Warn("repair published live index", "err", err.Error())
		} else if repaired.Applied > 0 || len(repaired.Skipped) > 0 {
			logger.Info("published live index repair checked", "candidates", repaired.Candidates, "applied", repaired.Applied, "skipped", len(repaired.Skipped))
		}
	}

	// The dead-branch sweep used to run synchronously here, before the
	// main loop. That put two costs on the blocking startup path: a
	// `git for-each-ref` shell-out and an O(distinct-terminal-pairs) walk
	// over capture_events. The sweep is now scheduled below as a one-shot
	// goroutine fired AFTER the running-mode publish so neither cost
	// counts against the start-latency budget. The env opt-out log still
	// fires synchronously so operators see the knob even on a no-op
	// startup.
	if isKeepDeadBranchBarriers() {
		logger.Info("dead-branch unpublished recovery disabled by env",
			"env", EnvKeepDeadBranchBarriers)
	}

	ignoreChecker := git.NewIgnoreChecker(opts.RepoPath)
	defer func() { _ = ignoreChecker.Close() }()
	matcher := opts.SensitiveMatcher
	if matcher == nil {
		matcher = state.NewSensitiveMatcher()
	}
	safeIgnore := opts.SafeIgnoreMatcher
	if safeIgnore == nil {
		safeIgnore = state.NewSafeIgnoreMatcher()
	}

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
			// Single-tx batch so the four diagnostics are observed
			// atomically by readers and a contending writer cannot
			// amplify N×busy_timeout into a tick stall.
			_ = state.MetaSetMany(ctx, opts.DB, map[string]string{
				"fsnotify.mode":            d.Mode,
				"fsnotify.watch_count":     strconv.Itoa(d.WatchCount),
				"fsnotify.dropped_events":  strconv.Itoa(d.DroppedEvents),
				"fsnotify.fallback_reason": d.FallbackReason,
			})
		}
		w, err := NewFsnotifyWatcher(FsnotifyOptions{
			RepoPath:      opts.RepoPath,
			GitDir:        opts.GitDir,
			IgnoreChecker: ignoreChecker,
			Sensitive:     matcher,
			SafeIgnore:    safeIgnore,
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
			// Bound the teardown so a wedged IgnoreChecker subprocess
			// cannot stall daemon shutdown. shutdown-lane will likely
			// thread the outer shutdown ctx here; until then, a fixed
			// 5s budget matches the per-layer preWalk timeout.
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = fsWatcher.Stop(stopCtx)
			stopCancel()
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
		replayErrorLogs   replayErrorLogLimiter

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

	graceful := func(reason string) error {
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
		shutdownCtx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer scancel()
		if err := state.SaveDaemonState(shutdownCtx, opts.DB, st); err != nil {
			logger.Warn("stamp stopped state", "err", err.Error())
			return fmt.Errorf("daemon: stamp stopped state: %w", err)
		}
		logger.Info("daemon stopping", "reason", reason)
		return nil
	}

	logger.Info("daemon running",
		"repo", opts.RepoPath, "pid", pid, "branch", branchRef,
		"head", headOID, "token", currentToken)

	// The startup sweep runs off the latency-sensitive startup path, but it
	// remains owned by this Run invocation. Cancel and join it before graceful
	// shutdown stamps stopped state, and keep the defer as a safety net for any
	// later non-graceful return. Because this defer was registered after the
	// daemon lock's defer, the sweep can never outlive lock ownership.
	startupSweepCtx, startupSweepCancel := context.WithCancel(ctx)
	var startupSweepWG sync.WaitGroup
	stopStartupSweep := func() {
		startupSweepCancel()
		startupSweepWG.Wait()
	}
	defer stopStartupSweep()
	gracefulWithSweep := func(reason string) error {
		stopStartupSweep()
		return graceful(reason)
	}

	// Schedule the dead-branch sweep on a one-shot goroutine, AFTER the
	// "daemon running" log lands, so neither the for-each-ref shell-out
	// nor the O(N) walk over distinct terminal pairs counts against the
	// start-latency budget. The goroutine reads ctx — when the daemon
	// stops the sweep is short-circuited.
	//
	// Honor the shared pause state first: an operator pause or rewind grace
	// expects no background mutation to capture_events. A read failure on the
	// pause state is logged and the sweep is skipped (fail closed — same posture
	// as the run-loop pause gate).
	startupSweepCctx := cctx
	startupSweepWG.Add(1)
	go func(sweepCctx CaptureContext) {
		defer startupSweepWG.Done()
		if operation, active := gitOperationInProgress(opts.GitDir); active {
			logger.Info("startup dead-branch sweep skipped during git operation",
				"operation", operation)
			return
		}
		if pauseStatus, perr := daemonPauseState(startupSweepCtx, opts.GitDir, opts.DB); perr != nil {
			logger.Warn("read pause state before startup dead-branch sweep; skipping",
				"err", perr.Error())
			return
		} else if pauseStatus.Active {
			logger.Info("startup dead-branch sweep skipped while paused",
				"source", pauseStatus.Source,
				"reason", pauseStatus.Reason)
			return
		}
		runStartupDeadBranchSweepWithOptions(startupSweepCtx, opts.RepoPath, opts.DB,
			sweepCctx, logger, tracer, startupDeadBranchSweepOptions{
				beforePairSafetyCheck: opts.beforeStartupDeadBranchPairSafetyCheck,
			})
	}(startupSweepCctx)

	// lastStampedBranchHead is the most recent value the run loop has
	// written to MetaKeyBranchHead through the SameGeneration "per-tick
	// keep-alive" path inside processBranchTokenChange. The previous
	// implementation called state.MetaSet on every tick regardless of the
	// value, which produced steady write churn on otherwise-idle daemons.
	// We now skip the upsert when the live HEAD matches what we last
	// stamped; the cross-tick rewind probe still runs because it reads
	// persisted via LoadBranchHead, not via lastStampedBranchHead.
	//
	// Seed lastStampedBranchHead from the persisted value at startup so
	// the very first idle tick does not re-stamp an unchanged value.
	lastStampedBranchHead := persistedHead
	var branchTransitionSettleUntil time.Time
	forceShadowRefresh := startupShadowRefreshRequired
	pendingSelfPublicationTarget := ""

	adoptSelfPublicationTarget := func(targetOID, observedToken string) error {
		if targetOID == "" {
			return fmt.Errorf("daemon: adopt self-publication: empty target OID")
		}
		if tokenSHA(observedToken) != targetOID ||
			tokenBranchRef(observedToken) != cctx.BranchRef {
			return fmt.Errorf(
				"daemon: adopt self-publication: observed token %q does not match target %s on %s",
				observedToken, targetOID, cctx.BranchRef)
		}
		nextToken := branchTokenRev(targetOID, cctx.BranchRef)
		if err := SaveBranchPublicationToken(
			ctx, opts.DB, cctx.BranchGeneration, targetOID, nextToken); err != nil {
			return fmt.Errorf("daemon: persist self-publication token: %w", err)
		}

		// This is one run-loop boundary: no capture, flush, wake, or branch
		// transition check can run between the durable transaction above and
		// these in-memory assignments.
		cctx.BaseHead = targetOID
		currentToken = nextToken
		headOID = targetOID
		lastStampedBranchHead = targetOID
		pendingSelfPublicationTarget = ""
		if opts.afterSelfPublicationAdoption != nil {
			opts.afterSelfPublicationAdoption(
				cctx, currentToken, headOID, lastStampedBranchHead)
		}
		return nil
	}

	recoverActiveSelfPublications := func() (blocked, hasMore bool) {
		if cctx.BranchRef == "" {
			return false, false
		}
		if opts.PublicationHeld != nil && opts.PublicationHeld() {
			return true, false
		}
		recovered, recoverErr := recoverSelfPublicationsPass(recoveryRootCtx, cctx)
		if recoverErr != nil {
			logger.Warn("self-publication recovery needs attention",
				"branch_ref", cctx.BranchRef,
				"generation", cctx.BranchGeneration,
				"err", recoverErr.Error())
			return true, false
		}
		if recovered.FinalTargetOID == "" {
			return recovered.HasMore, recovered.HasMore
		}
		targetOID := recovered.FinalTargetOID
		pendingSelfPublicationTarget = targetOID
		// Recovery proved and locked the literal branch at target through
		// SQLite completion. Adopt that exact internal boundary first. A
		// branch move immediately after lock release remains external and is
		// classified by the token check that follows this closure.
		if err := adoptSelfPublicationTarget(
			targetOID, branchTokenRev(targetOID, cctx.BranchRef)); err != nil {
			logger.Warn("adopt recovered self-publication target",
				"target", targetOID, "err", err.Error())
			return true, false
		}
		logger.Info("recovered self-publication during run-loop pass",
			"target", targetOID,
			"completed", recovered.Completed,
			"abandoned", recovered.Abandoned)
		return recovered.HasMore, recovered.HasMore
	}

	processBranchTokenChange := func(logPrefix string) bool {
		if opts.beforeBranchTokenCheck != nil {
			opts.beforeBranchTokenCheck()
		}
		newToken, terr := branchGenerationToken(ctx, opts.RepoPath)
		if terr != nil {
			logger.Warn(logPrefix+" resolve failed", "err", terr.Error())
			// A completed journal target that has not yet been adopted is an
			// exact internal transition in progress. Fail closed until HEAD
			// can be sampled again; otherwise capture/replay would run against
			// the pre-publication cctx and strand new work on a stale base.
			return pendingSelfPublicationTarget != ""
		}
		if pendingSelfPublicationTarget != "" {
			if tokenSHA(newToken) == pendingSelfPublicationTarget &&
				tokenBranchRef(newToken) == cctx.BranchRef {
				if err := adoptSelfPublicationTarget(
					pendingSelfPublicationTarget, newToken); err != nil {
					logger.Warn(logPrefix+" retry self-publication adoption",
						"target", pendingSelfPublicationTarget,
						"err", err.Error())
					return true
				}
				return false
			}
			// HEAD no longer names the completed journal target. It is now an
			// unknown transition and must retain the existing reconciliation
			// and generation-classification path below.
			pendingSelfPublicationTarget = ""
		}
		if SameGeneration(currentToken, newToken) {
			if forceShadowRefresh && cctx.BranchRef != "" {
				if seeded, seedErr := ReseedShadowFromHead(ctx, opts.RepoPath, opts.DB, cctx); seedErr != nil {
					logger.Warn(logPrefix+" refresh shadow after rolled-back transition",
						"err", seedErr.Error())
					return true
				} else {
					forceShadowRefresh = false
					traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded),
						"rolled-back transition shadow refresh", seeded)
					return true
				}
			}
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
			crossTickRewindDetected := false
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
						rewindPaused, rewindUntil, rewindErr := maybeSetRewindGraceWithDuration(
							ctx, opts.RepoPath, opts.DB, synthesizedPrev, newToken, now(), rewindGrace)
						if rewindErr != nil {
							logger.Warn(logPrefix+" cross-tick rewind grace failed",
								"err", rewindErr.Error())
						} else if rewindPaused {
							crossTickRewindDetected = true
							seeded, seedErr := ReseedShadowFromHead(ctx, opts.RepoPath, opts.DB, cctx)
							if seedErr != nil {
								logger.Warn(logPrefix+" reseed shadow after cross-tick rewind",
									"err", seedErr.Error())
								traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", seedErr.Error(), 0)
								return true
							}
							traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded),
								"cross-tick rewind shadow reseed", seeded)
							logger.Info("replay paused after cross-tick same-SHA rewind",
								"persisted_head", persistedHead,
								"live_head", liveHead,
								"until", rewindUntil,
								"shadow_rows", seeded)
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
			// Conditional stamp: keep persisted MetaKeyBranchHead in sync
			// with the freshly-observed live HEAD so the next tick's probe
			// has a current baseline rather than a stale value written by an
			// old transition. Skip the upsert when liveHead matches what we
			// last stamped — otherwise an idle daemon writes a meta row on
			// every tick. A failure here just means the next probe sees the
			// same stale value, matching the previous unconditional behaviour.
			// A self-publication journal can update MetaKeyBranchHead before
			// the run loop adopts its target in memory. If the user rewinds in
			// that small window, lastStampedBranchHead may still equal the live
			// HEAD even though the durable value is the abandoned publication
			// target. A detected cross-tick rewind must therefore overwrite the
			// durable value unconditionally; otherwise every later tick sees the
			// same stale target and re-arms the grace window forever.
			if liveHead != "" && (liveHead != lastStampedBranchHead || crossTickRewindDetected) {
				if err := state.MetaSet(ctx, opts.DB, MetaKeyBranchHead, liveHead); err != nil {
					logger.Warn(logPrefix+" stamp branch head per-tick",
						"err", err.Error())
				} else {
					lastStampedBranchHead = liveHead
				}
			}
			if cctx.BranchRef != "" && cctx.BaseHead != "" {
				bootstrapped, bErr := IsShadowBootstrapped(ctx, opts.DB, cctx.BranchRef, cctx.BranchGeneration)
				if bErr != nil {
					logger.Warn(logPrefix+" read shadow bootstrap marker",
						"err", bErr.Error())
					return true
				}
				if !bootstrapped {
					seeded, seedErr := BootstrapShadow(ctx, opts.RepoPath, opts.DB, cctx)
					if seedErr != nil {
						logger.Warn(logPrefix+" bootstrap missing shadow",
							"err", seedErr.Error())
						traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", seedErr.Error(), 0)
						return true
					}
					traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), "missing shadow bootstrap", seeded)
					if seeded > 0 {
						logger.Info("shadow bootstrapped after missing marker",
							"rows", seeded,
							"generation", cctx.BranchGeneration)
					}
					return true
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
		pauseStatus, pauseErr := daemonPauseState(ctx, opts.GitDir, opts.DB)
		if pauseErr != nil {
			logger.Warn(logPrefix+" read pause state before branch reconciliation; will retry",
				"err", pauseErr.Error())
			return true
		}
		if pauseStatus.Active && pauseStatus.Source == "manual" {
			logger.Info("branch transition deferred while manually paused",
				"old", currentToken,
				"new", newToken,
				"reason", pauseStatus.Reason)
			return true
		}
		oldToken := currentToken
		result, reconcileErr := reconcileTransitionPair(ctx, opts.RepoPath, opts.GitDir,
			opts.DB, cctx.BranchRef, cctx.BranchGeneration,
			tokenSHA(newToken) == "", "", "runtime_branch_transition", tracer)
		if reconcileErr != nil {
			logger.Warn(logPrefix+" reconcile unpublished chain before branch transition; will retry",
				"branch_ref", cctx.BranchRef,
				"generation", cctx.BranchGeneration,
				"err", reconcileErr.Error())
			return true
		}
		if result.Handled {
			logger.Info("reconciled unpublished chain before branch transition",
				"branch_ref", cctx.BranchRef,
				"generation", cctx.BranchGeneration,
				"outcome", result.Outcome,
				"events", result.EventCount,
				"recovery_ref", result.RecoveryRef)
		}
		verifiedToken, verifyErr := branchGenerationToken(ctx, opts.RepoPath)
		if verifyErr != nil {
			logger.Warn(logPrefix+" verify branch token after reconciliation; will retry",
				"err", verifyErr.Error())
			return true
		}
		if verifiedToken != newToken {
			logger.Info("branch token changed during reconciliation; will retry",
				"observed", newToken, "verified", verifiedToken)
			return true
		}
		branchTransitionSettleUntil = now().Add(branchTransitionSettleDelay)
		ts := strconv.FormatFloat(float64(now().UnixNano())/1e9, 'f', -1, 64)
		oldCctx := cctx
		oldBranchRef := branchRef
		oldHeadOID := headOID
		prospectiveShadowMutated := false
		var pendingTransitionTraces []acdtrace.Event
		rollbackTraced := false
		rollbackTransition := func() {
			if prospectiveShadowMutated && cctx.BranchRef != "" {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				cleanupErr := state.InvalidateShadowGeneration(cleanupCtx, opts.DB,
					cctx.BranchRef, cctx.BranchGeneration,
					ShadowBootstrappedKey(cctx.BranchRef, cctx.BranchGeneration))
				cleanupCancel()
				if cleanupErr != nil {
					logger.Warn(logPrefix+" invalidate prospective shadow; will force refresh",
						"err", cleanupErr.Error())
					forceShadowRefresh = true
				}
			}
			if !rollbackTraced {
				recordTrace(tracer, acdtrace.Event{
					Repo:       opts.RepoPath,
					BranchRef:  cctx.BranchRef,
					HeadSHA:    cctx.BaseHead,
					EventClass: "branch_token.transition",
					Decision:   "rolled_back",
					Reason:     "run-loop branch transition was not accepted",
					Input:      map[string]any{"previous": oldToken, "current": newToken},
					Output: map[string]any{
						"accepted":        false,
						"prev_generation": oldCctx.BranchGeneration,
						"new_generation":  cctx.BranchGeneration,
					},
					Generation: cctx.BranchGeneration,
				})
				rollbackTraced = true
			}
			currentToken = oldToken
			cctx = oldCctx
			logContext.SetBranch(cctx.BranchRef, cctx.BranchGeneration)
			branchRef = oldBranchRef
			headOID = oldHeadOID
			if opts.afterBranchTransitionRollback != nil {
				opts.afterBranchTransitionRollback()
			}
		}
		acceptTransition := func() bool {
			if opts.beforeBranchTransitionAccept != nil {
				opts.beforeBranchTransitionAccept()
			}
			pauseStatus, pauseErr := daemonPauseState(ctx, opts.GitDir, opts.DB)
			if pauseErr != nil {
				logger.Warn(logPrefix+" recheck pause before metadata accept; will retry",
					"err", pauseErr.Error())
				rollbackTransition()
				return false
			}
			if pauseStatus.Active && pauseStatus.Source == "manual" {
				logger.Info("branch transition acceptance deferred while manually paused",
					"reason", pauseStatus.Reason)
				rollbackTransition()
				return false
			}
			verifiedToken, verifyErr := branchGenerationToken(ctx, opts.RepoPath)
			if verifyErr != nil {
				logger.Warn(logPrefix+" verify branch token before metadata accept; will retry",
					"err", verifyErr.Error())
				rollbackTransition()
				return false
			}
			if verifiedToken != newToken {
				logger.Info("branch token changed during shadow seed; will retry",
					"observed", newToken, "verified", verifiedToken)
				rollbackTransition()
				return false
			}
			if err := state.MetaSetMany(ctx, opts.DB, map[string]string{
				MetaKeyBranchTokenChangedAt: ts,
				MetaKeyBranchToken:          newToken,
				MetaKeyBranchGeneration:     strconv.FormatInt(cctx.BranchGeneration, 10),
				MetaKeyBranchHead:           cctx.BaseHead,
			}); err != nil {
				logger.Warn(logPrefix+" accept branch transition metadata; will retry",
					"err", err.Error())
				rollbackTransition()
				return false
			}
			lastStampedBranchHead = cctx.BaseHead
			logContext.SetBranch(cctx.BranchRef, cctx.BranchGeneration)
			for _, event := range pendingTransitionTraces {
				recordTrace(tracer, event)
			}
			return true
		}
		// Derive the prospective capture context from the exact token sample
		// already classified and checked. A second resolve can race checkout
		// and mix a branch/head pair from a different observation.
		branchRef = tokenBranchRef(newToken)
		headOID = tokenSHA(newToken)
		cctx.BranchRef = branchRef
		cctx.BaseHead = headOID
		currentToken = newToken
		reattaching := false
		clearGraceAfterAccept := false
		clearManualResumeAfterAccept := false
		pruneShadowAfterAccept := false
		if transition == TokenTransitionDiverged {
			prevGeneration := cctx.BranchGeneration
			rewindPaused, rewindUntil, rewindErr := maybeSetRewindGraceWithDuration(ctx, opts.RepoPath, opts.DB, oldToken, newToken, now(), rewindGrace)
			if rewindErr != nil {
				logger.Warn(logPrefix+" detect rewind grace failed", "err", rewindErr.Error())
			} else if rewindPaused {
				logger.Info("replay paused after branch rewind", "until", rewindUntil)
			}
			cctx.BranchGeneration++
			pruneShadowAfterAccept = true
			logger.Info("branch generation bumped",
				"old", oldToken, "new", newToken,
				"generation", cctx.BranchGeneration,
				"transition", transition.String())
			pendingTransitionTraces = append(pendingTransitionTraces, acdtrace.Event{
				Repo:       opts.RepoPath,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "branch_token.transition",
				Decision:   transition.String(),
				Reason:     "run-loop token transition classified",
				Input:      map[string]any{"previous": oldToken, "current": newToken},
				Output: map[string]any{
					"accepted":          true,
					"prev_generation":   prevGeneration,
					"new_generation":    cctx.BranchGeneration,
					"reconciled_events": result.EventCount,
					"reconcile_outcome": result.Outcome,
				},
				Generation: cctx.BranchGeneration,
			})
			// shadow_paths is keyed by (branch_ref, branch_generation).
			// After a divergence the new key is empty; without
			// reseeding from HEAD the next capture would classify every
			// tracked file as a phantom `create`.
			if cctx.BranchRef == "" {
			} else {
				// Detached -> attached transition can land here when the
				// reattach branch at line 1057 races with this Diverged
				// path: if iteration N's resolveBranch ran before the
				// operator's checkout but processBranchTokenChange ran
				// after, cctx.BranchRef is set right here and the line
				// 1057 reattach clear is skipped on iteration N+1. Clear
				// the detached-HEAD marker and any stale rewind grace
				// from the prior detach window symmetrically with the
				// dedicated reattach branch above so capture/replay
				// resume immediately rather than staying muted up to
				// ACD_REWIND_GRACE_SECONDS.
				if tokenBranchRef(oldToken) == "" {
					reattaching = true
				}
				prospectiveShadowMutated = true
				if seeded, err := ReseedShadowFromHead(ctx, opts.RepoPath, opts.DB, cctx); err != nil {
					logger.Warn("reseed shadow after generation bump",
						"err", err.Error())
					traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", err.Error(), 0)
					rollbackTransition()
					return true
				} else {
					traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), "generation bump shadow reseed", seeded)
					if seeded > 0 {
						logger.Info("shadow reseeded",
							"rows", seeded,
							"generation", cctx.BranchGeneration)
					}
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
				pruneShadowAfterAccept = true
				logger.Info("fast-forward inside rewind grace; reseeding shadow",
					"old", oldToken, "new", newToken,
					"generation", cctx.BranchGeneration,
					"grace_until", until)
				pendingTransitionTraces = append(pendingTransitionTraces, acdtrace.Event{
					Repo:       opts.RepoPath,
					BranchRef:  cctx.BranchRef,
					HeadSHA:    cctx.BaseHead,
					EventClass: "branch_token.transition",
					Decision:   transition.String(),
					Reason:     "fast-forward inside rewind grace; reseeding shadow",
					Input:      map[string]any{"previous": oldToken, "current": newToken},
					Output: map[string]any{
						"accepted":        true,
						"prev_generation": prevGeneration,
						"new_generation":  cctx.BranchGeneration,
						"grace_until":     until,
					},
					Generation: cctx.BranchGeneration,
				})
				if cctx.BranchRef != "" {
					prospectiveShadowMutated = true
					if seeded, err := ReseedShadowFromHead(ctx, opts.RepoPath, opts.DB, cctx); err != nil {
						logger.Warn("reseed shadow after FF-in-grace",
							"err", err.Error())
						traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", err.Error(), 0)
						rollbackTransition()
						return true
					} else {
						traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), "FF-in-grace shadow reseed", seeded)
						if seeded > 0 {
							logger.Info("shadow reseeded after FF-in-grace",
								"rows", seeded,
								"generation", cctx.BranchGeneration)
						}
					}
				}
				clearGraceAfterAccept = true
			} else {
				fastForwardPaused, pauseErr := daemonPauseState(ctx, opts.GitDir, opts.DB)
				if pauseErr != nil {
					logger.Warn("read pause state before branch fast-forward resync",
						"err", pauseErr.Error())
					rollbackTransition()
					return true
				}
				if fastForwardPaused.Active && fastForwardPaused.Source == "manual" {
					logger.Info("branch fast-forward deferred while manually paused",
						"old", oldToken, "new", newToken,
						"reason", fastForwardPaused.Reason)
					rollbackTransition()
					return true
				}
				if resumed, stamp := manualPauseRecentlyResumed(ctx, opts.DB, now()); resumed {
					logger.Info("branch fast-forward observed after manual resume; reseeding shadow",
						"old", oldToken,
						"new", newToken,
						"generation", cctx.BranchGeneration,
						"resumed_at", stamp)
					pendingTransitionTraces = append(pendingTransitionTraces, acdtrace.Event{
						Repo:       opts.RepoPath,
						BranchRef:  cctx.BranchRef,
						HeadSHA:    cctx.BaseHead,
						EventClass: "branch_token.transition",
						Decision:   transition.String(),
						Reason:     "fast-forward observed after manual resume; reseeding shadow",
						Input:      map[string]any{"previous": oldToken, "current": newToken},
						Output: map[string]any{
							"accepted":   true,
							"generation": cctx.BranchGeneration,
							"resumed_at": stamp,
						},
						Generation: cctx.BranchGeneration,
					})
					clearManualResumeAfterAccept = true
				}
				seeded := 0
				if cctx.BranchRef != "" {
					var seedErr error
					prospectiveShadowMutated = true
					seeded, seedErr = ReseedShadowFromHead(ctx, opts.RepoPath, opts.DB, cctx)
					if seedErr != nil {
						logger.Warn("reseed shadow after branch fast-forward",
							"err", seedErr.Error())
						traceBootstrapShadow(tracer, opts.RepoPath, cctx, "error", seedErr.Error(), 0)
						rollbackTransition()
						return true
					} else {
						traceBootstrapShadow(tracer, opts.RepoPath, cctx, traceSeedDecision(seeded), "fast-forward shadow reseed", seeded)
						logger.Info("shadow reseeded after branch fast-forward",
							"rows", seeded,
							"generation", cctx.BranchGeneration)
					}
				}
				logger.Debug("branch fast-forwarded",
					"old", oldToken, "new", newToken,
					"generation", cctx.BranchGeneration,
					"shadow_rows", seeded)
				pendingTransitionTraces = append(pendingTransitionTraces, acdtrace.Event{
					Repo:       opts.RepoPath,
					BranchRef:  cctx.BranchRef,
					HeadSHA:    cctx.BaseHead,
					EventClass: "branch_token.transition",
					Decision:   transition.String(),
					Reason:     "run-loop token transition classified",
					Input:      map[string]any{"previous": oldToken, "current": newToken},
					Output: map[string]any{
						"accepted":             true,
						"generation":           cctx.BranchGeneration,
						"reconciled_events":    result.EventCount,
						"reconcile_outcome":    result.Outcome,
						"shadow_rows_reseeded": seeded,
					},
					Generation: cctx.BranchGeneration,
				})
			}
		}
		if acceptTransition() {
			if result.Handled && result.EventCount > 0 && oldCctx.BranchRef != "" {
				exists, refErr := git.RefExists(ctx, opts.RepoPath, oldCctx.BranchRef)
				if refErr != nil {
					logger.Warn("recheck prior ref after branch recovery",
						"ref", oldCctx.BranchRef, "err", refErr.Error())
				} else if !exists {
					recordDeadBranchRecoveryMeta(ctx, opts.DB, logger,
						result.EventCount, []string{oldCctx.BranchRef})
				}
			}
			if reattaching {
				clearGraceAfterAccept = true
			}
			if clearGraceAfterAccept {
				clearRewindGraceMeta(ctx, opts.DB, opts.RepoPath, cctx, tracer, logger,
					"accepted branch transition")
			}
			if clearManualResumeAfterAccept {
				_, _ = state.MetaDelete(ctx, opts.DB, MetaKeyManualPauseResumedAt)
			}
			if pruneShadowAfterAccept {
				if pruned, pErr := pruneShadowGenerationsWithRetention(ctx, opts.DB, cctx, shadowRetention); pErr != nil {
					logger.Warn("prune old shadow generations", "err", pErr.Error())
				} else if pruned > 0 {
					logger.Info("pruned old shadow generations", "rows", pruned)
				}
			}
		}
		return true
	}

	wakeAckLogNext := now().Add(time.Minute)
	var wakeAckCount int
	var wakeAckLastID int64
	// A logical flush is acknowledged when its durable queue row is drained,
	// but its intent batch-wait bypass is consumed only by a replay-eligible
	// pass. Keep the signal sticky across activation/validation gates and other
	// skipped passes so configuration convergence cannot silently discard it.
	logicalFlushPending := false

	// Start setup validation only after startup branch reconciliation has
	// established the exact branch generation recorded by configure.
	runtimeBundles.StartValidationWorker(ctx, validationWakeCh)

	for {
		branchTransitionBlocked = false
		recoveryFollowup := false

		// 4a/b. Honor ctx + shutdown signal.
		if err := ctx.Err(); err != nil {
			return gracefulWithSweep("context canceled")
		}
		select {
		case <-shutdownCh:
			return gracefulWithSweep("signal shutdown")
		default:
		}
		// The prior capture/replay pass has fully returned. Converge desired
		// runtime revisions before any new pass can obtain a lease.
		if cutoverBlock != "" {
			retryRoots, retryRootsErr := paths.Resolve()
			if retryRootsErr == nil {
				if cutover, retryErr := EnsureIntentV2RuntimeCutover(
					ctx, opts.DB, opts.RepoPath, retryRoots,
					os.LookupEnv); retryErr == nil {
					runtimeRoots = retryRoots
					if runtimeCredentialStore == nil {
						store := credentials.NewStore(runtimeRoots)
						runtimeCredentialStore = &store
						runtimeBundles.SetCredentialStore(
							runtimeCredentialStore)
					}
					cutoverBlock = ""
					logger.Info("Intent v2 runtime cutover recovered",
						"revision_id", cutover.RevisionID,
						"preset_id", cutover.PresetID)
				}
			}
		}
		if cutoverBlock == "" {
			if err := runtimeBundles.ActivateDesired(ctx); err != nil {
				logger.Warn("activate desired runtime config; retaining last-known-good",
					"err", ai.SanitizePlannerError(err.Error()))
			}
			if queued, err := runtimeBundles.QueueExperimentRevert(ctx, now()); err != nil {
				logger.Warn("queue experiment baseline revert", "err", ai.SanitizePlannerError(err.Error()))
			} else if queued {
				if err := runtimeBundles.ActivateDesired(ctx); err != nil {
					logger.Warn("activate experiment baseline revert", "err", ai.SanitizePlannerError(err.Error()))
				}
			}
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
		select {
		case <-validationWakeCh:
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
			// Stale-marker tracking: stamp the wall-clock + HEAD the first
			// time we see this marker, persist for diagnose, then warn
			// periodically when both have been motionless past threshold.
			currentHead, _ := git.RevParse(ctx, opts.RepoPath, "HEAD")
			nowTS := now()
			if opMarkerSetAt.IsZero() {
				opMarkerSetAt = nowTS
				opMarkerHead = currentHead
				stamp := strconv.FormatFloat(float64(nowTS.UnixNano())/1e9, 'f', -1, 64)
				// First-observation transition: stamp marker name +
				// set_at + head_at atomically.
				_ = setRuntimeMetaIfChanged(ctx, opts.DB, map[string]string{
					MetaKeyOperationInProgress:      operationName,
					MetaKeyOperationInProgressSetAt: stamp,
					MetaKeyOperationInProgressHead:  currentHead,
				})
			} else {
				// Steady-state: only the marker name needs refreshing
				// (no-op upsert when unchanged); set_at/head_at remain
				// pinned to the first-observation tick.
				_ = state.MetaSet(ctx, opts.DB, MetaKeyOperationInProgress, operationName)
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
				// NTP-safe: nowTS.Before(opMarkerWarnedAt) catches a backward
				// step that would otherwise leave Sub() negative and silence
				// the warn forever. time.Time arithmetic uses monotonic
				// readings when both operands have them, but a Time stored
				// across boundaries that strip the monotonic clock (e.g. JSON
				// round-trips, t.Round(0)) falls back to wall-clock and is
				// vulnerable. Clamping cheaply covers that case.
				sinceWarn := nowTS.Sub(opMarkerWarnedAt)
				if opMarkerWarnedAt.IsZero() || nowTS.Before(opMarkerWarnedAt) || sinceWarn >= staleOpMarkerWarnInterval {
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

		if opts.OperationGate != nil {
			opts.OperationGate.RLock()
		}
		if !operationPaused && cctx.BranchRef != "" {
			if pauseStatus, pauseErr :=
				daemonPauseState(ctx, opts.GitDir, opts.DB); pauseErr != nil {
				logger.Warn("read pause state before self-publication recovery",
					"err", pauseErr.Error())
				branchTransitionBlocked = true
			} else if !pauseStatus.Active {
				branchTransitionBlocked, recoveryFollowup =
					recoverActiveSelfPublications()
			}
		}

		if !operationPaused && !branchTransitionBlocked {
			if processBranchTokenChange("branch token") {
				branchTransitionBlocked = true
			}
		}
		if opts.OperationGate != nil {
			opts.OperationGate.RUnlock()
		}

		// 4e. Drain pending flush_requests; each bounded batch triggers one
		// immediate capture+replay cycle. Claiming and completing each row in
		// separate transactions made a 5000-wake burst exceed the 60-second
		// acknowledgement contract under -race. DrainFlushRequests atomically
		// owns and completes the oldest flushLimit rows in one statement while
		// returning their commands for logical/wake accounting.
		//
		// The batch remains capped at DefaultFlushLimit=256, preserving the
		// run-loop heartbeat/capture/shutdown budget. Check shutdown before and
		// after the statement so a non-cancelable ctx controlled only by
		// shutdownCh waits for at most one bounded SQLite operation.
		flushLimit := opts.FlushLimit
		if flushLimit <= 0 {
			flushLimit = DefaultFlushLimit
		}
		flushedTotal := 0
		flushedLogical := 0
		flushedWake := 0
		if ctx.Err() == nil {
			select {
			case <-shutdownCh:
				return gracefulWithSweep("signal shutdown")
			default:
			}
			flushed, err := state.DrainFlushRequests(
				ctx,
				opts.DB,
				flushLimit,
				sql.NullString{String: "flushed", Valid: true},
			)
			if err != nil {
				logger.Warn("drain flush requests", "err", err.Error())
			} else {
				flushedTotal = len(flushed)
				for _, fr := range flushed {
					if fr.Command == "flush_logical" {
						flushedLogical++
					}
					if fr.Command == "wake" {
						flushedWake++
						wakeAckCount++
						wakeAckLastID = fr.ID
					}
					logger.Debug("flush request acked",
						"id", fr.ID, "command", fr.Command)
				}
				logicalFlushPending = logicalFlushPending || flushedLogical > 0
			}
		}
		select {
		case <-shutdownCh:
			return gracefulWithSweep("signal shutdown")
		default:
		}
		if flushedWake > 0 && flushedLogical == 0 {
			recordTrace(tracer, acdtrace.Event{
				Repo:       opts.RepoPath,
				BranchRef:  cctx.BranchRef,
				HeadSHA:    cctx.BaseHead,
				EventClass: "replay.intent.wake_drained",
				Decision:   "wake_drained",
				Reason:     "wake flush_requests drained without logical flush",
				Input: map[string]any{
					"flushed_total": flushedTotal,
					"flushed_wake":  flushedWake,
				},
				Generation: cctx.BranchGeneration,
			})
		}
		if tickNow := now(); !tickNow.Before(wakeAckLogNext) {
			if wakeAckCount > 0 {
				logger.Info("wake flush requests acked",
					"count", wakeAckCount,
					"last_id", wakeAckLastID,
					"window", time.Minute.String())
				wakeAckCount = 0
				wakeAckLastID = 0
			}
			wakeAckLogNext = tickNow.Add(time.Minute)
		}
		// Re-check the branch token AFTER the flush drain. The drain can
		// iterate up to DefaultFlushLimit (256) rows, and operator git
		// surgery (`git reset/rebase/checkout`) is NOT serialized through
		// wakeCh — HEAD can move during the drain. Without this re-check,
		// Capture/Replay would run with a stale BranchRef/BaseHead/generation,
		// risking events keyed under the wrong shadow generation, missed
		// rewind grace, or replay anchored to a stale BaseHead. If a
		// transition is observed, mark the iteration blocked and let the
		// next tick re-evaluate after Capture/Replay are skipped.
		if !branchTransitionBlocked && !operationPaused {
			if processBranchTokenChange("post-flush branch token") {
				branchTransitionBlocked = true
			}
		}
		if !branchTransitionBlocked && !branchTransitionSettleUntil.IsZero() {
			if now().Before(branchTransitionSettleUntil) {
				branchTransitionBlocked = true
			} else {
				branchTransitionSettleUntil = time.Time{}
			}
		}
		if _, err := reconcileResolvedPublicationDrains(
			ctx, opts.DB, logger, "run_loop", now(),
		); err != nil {
			logger.Warn("reconcile resolved publication drains", "err", err.Error())
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
		runtimeLease := runtimeBundles.Lease()
		passBundle := runtimeLease.Bundle()
		_ = setRuntimeMetaIfChanged(ctx, opts.DB, map[string]string{
			"commit.strategy": string(passBundle.CommitStrategy),
		})
		passCtx := withRuntimeTelemetry(ctx, passBundle)
		var (
			capSum     CaptureSummary
			capErr     error
			daemonPaus replayPause
			pauseErr   error
		)
		detachedHeadPaused := cctx.BranchRef == ""
		if transition, detachErr := syncDetachedHeadState(
			ctx, opts.DB, detachedHeadPaused, now(),
		); detachErr != nil {
			logger.Warn("sync detached HEAD state", "err", detachErr.Error())
		} else {
			switch transition {
			case detachedHeadEntered:
				logger.Warn("detached HEAD detected; capture and publication paused for this worktree")
			case detachedHeadReattached:
				logger.Info("HEAD reattached; capture and publication resumed for this worktree")
			}
		}
		if !branchTransitionBlocked && !operationPaused && !detachedHeadPaused {
			daemonPaus, pauseErr = daemonPauseStateFn(ctx, opts.GitDir, opts.DB)
			if pauseErr != nil {
				logger.Warn("read daemon pause state", "err", pauseErr.Error())
			}
		}
		// Fail CLOSED on pause-state read errors. A transient SQLite read
		// error on daemon_meta.replay.paused_until — or any other error
		// surfaced by daemonPauseState that wasn't already softened to a
		// fail-open warning inside the helper (ErrMalformed,
		// ErrNonRegularSource on the disk marker) — must NOT be treated as
		// "no pause active". Otherwise a flaky DB read or a partial-write
		// during operator surgery could let capture/replay silently chew
		// through the queue while the operator believes replay is paused.
		// The existing on-disk-marker softening in daemonPauseState is
		// intentional (a corrupt JSON marker should not wedge replay
		// forever); the SQLite-side fail-closed here is the dual: when we
		// genuinely cannot answer "is replay paused?", assume yes.
		daemonPaused := pauseErr != nil || daemonPaus.Active
		if branchTransitionBlocked {
			logger.Warn("publication paused for branch transition settle; protection remains active")
		} else if operationPaused {
			logger.Warn("git operation in progress; publication paused and protection remains active",
				"operation", operationName)
		} else if daemonPaused && !detachedHeadPaused {
			logger.Warn("publication paused; protection remains active",
				"source", daemonPaus.Source, "reason", daemonPaus.Reason)
			traceCapturePaused(tracer, opts.RepoPath, cctx, daemonPaus)
		}
		if opts.OperationGate != nil {
			opts.OperationGate.RLock()
		}
		publicationHeld := opts.PublicationHeld != nil && opts.PublicationHeld()
		if publicationHeld {
			logger.Info("publication held by transactional setup; protection remains active")
		}
		unsafePublication := branchTransitionBlocked || operationPaused || detachedHeadPaused || daemonPaused || publicationHeld
		observationEpoch, observationErr := BeginProtectionObservation(passCtx, opts.DB)
		if observationErr != nil {
			capErr = observationErr
		}
		if capErr == nil && unsafePublication {
			ignoreChecker.Invalidate()
			capSum, capErr = ProtectWorktree(passCtx, opts.RepoPath, opts.DB, cctx, CaptureOpts{
				IgnoreChecker:     ignoreChecker,
				SensitiveMatcher:  matcher,
				SafeIgnoreMatcher: safeIgnore,
				Trace:             tracer,
				GitDir:            opts.GitDir,
				CheckpointStore:   &checkpointStore,
				WorktreeID:        checkpointpkg.WorktreeID(opts.RepoPath),
				CheckpointReason:  state.CheckpointReasonPoll,
				MaxFileBytes:      opts.MaxFileBytes,
				ObservationEpoch:  observationEpoch,
			})
		} else if capErr == nil && cctx.BaseHead != "" {
			// git check-ignore keeps ignore files loaded for the lifetime
			// of its --stdin process. Refresh once per capture pass so
			// newly-created or edited .gitignore files are honored even
			// when fsnotify is disabled or misses the ignore-file event.
			ignoreChecker.Invalidate()
			// The run loop has already evaluated the pause gate above and
			// emitted the trace event when paused; SkipPauseCheck=true
			// prevents Capture from re-tracing the same decision. GitDir
			// is still wired through so that direct callers (tests,
			// future CLI wrappers) honor the same gate symmetrically.
			capSum, capErr = Capture(passCtx, opts.RepoPath, opts.DB, cctx, CaptureOpts{
				IgnoreChecker:     ignoreChecker,
				SensitiveMatcher:  matcher,
				SafeIgnoreMatcher: safeIgnore,
				Trace:             tracer,
				GitDir:            opts.GitDir,
				SkipPauseCheck:    true,
				CheckpointStore:   &checkpointStore,
				WorktreeID:        checkpointpkg.WorktreeID(opts.RepoPath),
				CheckpointReason:  state.CheckpointReasonPoll,
				MaxFileBytes:      opts.MaxFileBytes,
				ObservationEpoch:  observationEpoch,
			})
		}

		var (
			repSum        ReplaySummary
			repErr        error
			replayChecked bool
		)
		if capErr == nil && !branchTransitionBlocked && !operationPaused && !detachedHeadPaused &&
			!daemonPaused && !publicationHeld && cctx.BaseHead != "" {
			replayChecked = true
			// 4g. Replay pass. Bounded by DefaultReplayLimit so a large
			// pending queue cannot starve flush_request claims, heartbeat
			// refresh, or shutdown observation. ReplaySummary.HasMore is
			// folded into hadWork below so the scheduler resets to the base
			// poll interval and an immediate follow-up pass drains the rest
			// without waiting for the idle ceiling.
			activeDrain, drainErr := ActivePublicationDrainForPair(
				passCtx, opts.DB, cctx.BranchRef, cctx.BranchGeneration)
			if drainErr == nil && activeDrain == nil {
				recoveredDrain, recoverErr := RecoverSupersededCandidatePublicationDrain(
					passCtx, opts.DB, cctx.BranchRef, cctx.BranchGeneration,
					time.Now().UTC())
				if recoverErr != nil {
					drainErr = recoverErr
				} else if recoveredDrain != nil {
					activeDrain = recoveredDrain
					logPublicationDrainTransition(
						logger, state.PublicationDrain{
							Phase: state.PublicationDrainNeedsAction,
						}, *recoveredDrain)
				}
			}
			setupValidation, validationPending, validationErr :=
				state.DesiredConfigValidation(ctx, opts.DB)
			if drainErr != nil {
				repErr = drainErr
			} else if validationErr != nil {
				repErr = validationErr
			} else if validationPending &&
				setupValidation.Status != state.ConfigValidationPassed &&
				activeDrain == nil {
				repSum = ReplaySummary{
					Skipped: true,
					SkippedReason: "configuration_validation_" +
						setupValidation.Status,
					BaseHead: cctx.BaseHead,
				}
				_ = setRuntimeMetaIfChanged(ctx, opts.DB, map[string]string{
					"config.validation.status": setupValidation.Status,
					"config.validation.attempt": strconv.Itoa(
						setupValidation.Attempt),
				})
			} else if passBundle.ReplayBlockedReason != "" && activeDrain == nil {
				repSum = ReplaySummary{
					Skipped: true, SkippedReason: "intent_v2_needs_attention",
					BaseHead: cctx.BaseHead,
				}
				_ = setRuntimeMetaIfChanged(ctx, opts.DB, map[string]string{
					"intent.v2.needs_attention": passBundle.ReplayBlockedReason,
					metaIntentV2MigrationState:  "needs_attention",
					"intent.v2.preset_id":       passBundle.PresetID,
					"intent.v2.preset_version":  strconv.Itoa(passBundle.PresetVersion),
				})
			} else {
				var candidateVerify IntentCandidateVerifier
				var repairCommitVerify git.IntentRepairCommitVerifier
				if passBundle.IntentVerificationReady {
					candidateVerify = runtimeIntentCandidateVerifier(
						opts.RepoPath, opts.GitDir, cctx.BaseHead,
						passBundle.RevisionID,
						passBundle.IntentVerificationCommand)
					repairCommitVerify = runtimeIntentRepairCommitVerifier(
						opts.RepoPath, passBundle.RevisionID,
						passBundle.IntentVerificationCommand)
				}
				replay := Replay
				if opts.replay != nil {
					replay = opts.replay
				}
				if activeDrain != nil &&
					activeDrain.Phase == state.PublicationDrainCheckpointing {
					resumedDrain, resumeErr := ResumePublicationDrainCheckpointing(
						passCtx, opts.RepoPath, opts.DB, *activeDrain, time.Now().UTC())
					if resumeErr != nil {
						repErr = resumeErr
					} else {
						activeDrain = &resumedDrain
					}
				}
				if repErr == nil && activeDrain != nil &&
					activeDrain.Phase == state.PublicationDrainNormalizing {
					previousDrain := *activeDrain
					resumedDrain, resumeErr := ResumePublicationDrainNormalization(
						passCtx, opts.DB, *activeDrain, time.Now().UTC())
					if resumeErr != nil {
						repErr = resumeErr
					} else {
						activeDrain = &resumedDrain
						logPublicationDrainTransition(
							logger, previousDrain, resumedDrain)
					}
				}
				if repErr == nil && (activeDrain == nil ||
					activeDrain.Phase != state.PublicationDrainNeedsAction) {
					runWithProgressHeartbeat(passCtx, progressHeartbeatInterval(opts), func() {
						heartbeatNow("running", "")
					}, func() {
						repSum, repErr = replay(passCtx, opts.RepoPath, opts.DB, cctx, ReplayOpts{
							MessageFn:                  passBundle.MessageFn,
							GitDir:                     opts.GitDir,
							Trace:                      tracer,
							PromptTrace:                promptTracer,
							Limit:                      DefaultReplayLimit,
							CommitStrategy:             passBundle.CommitStrategy,
							IntentWindow:               passBundle.IntentWindow,
							IntentMinPending:           passBundle.IntentMinPending,
							IntentSettleWindow:         passBundle.IntentSettleWindow,
							IntentMaxPendingAge:        passBundle.IntentMaxPendingAge,
							IntentRecentCommits:        passBundle.IntentRecentCommits,
							IntentDeferLimit:           passBundle.IntentDeferLimit,
							IntentRetryLimit:           &passBundle.IntentRetryLimit,
							IntentPathCoalescing:       &passBundle.IntentPathCoalescing,
							IntentBypassBatchWait:      logicalFlushPending || activeDrain != nil,
							IntentPlanner:              passBundle.IntentPlanner,
							IntentHealth:               passBundle.IntentHealth,
							IntentPlannerProvider:      passBundle.HealthIdentity.Provider,
							IntentPlannerModel:         passBundle.Model,
							IntentIncludeDiffs:         passBundle.IntentIncludeDiffs,
							IntentPreset:               passBundle.IntentPreset,
							IntentVerificationMode:     passBundle.IntentVerificationMode,
							IntentCandidateVerify:      candidateVerify,
							IntentRepairCommitVerify:   repairCommitVerify,
							IntentRepairEnabled:        passBundle.IntentRepairEnabled,
							IntentRepairHorizon:        passBundle.IntentRepairHorizon,
							IntentRepairMaxCommits:     passBundle.IntentRepairMaxCommits,
							SelfPublicationCheckpoint:  opts.selfPublicationCheckpoint,
							RequireCompletedCheckpoint: true,
							PublicationDrain:           activeDrain,
						})
					})
					if activeDrain != nil {
						updatedDrain, updateErr := UpdatePublicationDrainAfterReplay(
							passCtx, opts.DB, *activeDrain, repSum, repErr, time.Now().UTC())
						if updateErr != nil {
							repErr = errors.Join(repErr, updateErr)
						} else {
							logPublicationDrainTransition(
								logger, *activeDrain, updatedDrain)
							if updatedDrain.Phase != state.PublicationDrainCompleted &&
								updatedDrain.Phase != state.PublicationDrainNeedsAction {
								repErr = nil
								repSum.HasMore = true
							}
						}
					}
				}
				if logicalFlushPending && repErr == nil && !repSum.Skipped {
					logicalFlushPending = false
				}
				_ = updateIntentV2EvaluationMeta(
					ctx, opts.DB, passBundle, repSum, repErr)
			}
			if repErr == nil && repSum.SelfPublicationTargetOID != "" {
				targetOID := repSum.SelfPublicationTargetOID
				if repSum.BaseHead != targetOID {
					repErr = fmt.Errorf(
						"daemon: replay self-publication target %s disagrees with base head %s",
						targetOID, repSum.BaseHead)
				} else {
					pendingSelfPublicationTarget = targetOID
					observedToken, tokenErr := branchGenerationToken(
						passCtx, opts.RepoPath)
					if tokenErr != nil {
						repErr = fmt.Errorf(
							"daemon: resolve self-publication target: %w",
							tokenErr)
					} else if tokenSHA(observedToken) == targetOID &&
						tokenBranchRef(observedToken) == cctx.BranchRef {
						if adoptErr := adoptSelfPublicationTarget(
							targetOID, observedToken); adoptErr != nil {
							repErr = adoptErr
						}
					} else {
						// An external writer moved HEAD after the journal
						// completed. Do not bless that movement as ours; the
						// next token check must classify it normally.
						pendingSelfPublicationTarget = ""
						logger.Info("HEAD moved after self-publication; deferring to transition classifier",
							"journal_target", targetOID,
							"observed_token", observedToken)
					}
				}
			}
			if _, reconcileErr := reconcileResolvedPublicationDrains(
				passCtx, opts.DB, logger, "replay_settled", now(),
			); reconcileErr != nil {
				repErr = errors.Join(repErr, reconcileErr)
			}
		}
		if opts.OperationGate != nil {
			opts.OperationGate.RUnlock()
		}
		runtimeLease.Release()
		if _, err := runtimeBundles.QueueExperimentRevert(ctx, now()); err != nil {
			logger.Warn("queue settled experiment baseline revert", "err", ai.SanitizePlannerError(err.Error()))
		}

		// Tick error counters.
		if capErr != nil {
			consecutiveErrors++
			logger.Warn("capture error", "n", consecutiveErrors, "err", capErr.Error())
			_ = state.MetaSet(ctx, opts.DB, "last_capture_error", capErr.Error())
		} else if repErr != nil {
			consecutiveErrors++
			clean, repeats, metaErr := recordReplayErrorObservability(
				ctx, opts.DB, repErr, now())
			if metaErr != nil {
				logger.Warn("persist replay error observability",
					"err", metaErr.Error())
			}
			if emit, suppressed := replayErrorLogs.observe(clean, now()); emit {
				logger.Warn("replay error", "n", repeats, "err", clean,
					"suppressed", suppressed)
			}
		} else {
			consecutiveErrors = 0
			_ = state.MetaSet(ctx, opts.DB, "last_capture_error", "")
			if replayChecked {
				previous, repeats, metaErr := clearReplayErrorObservability(
					ctx, opts.DB)
				if metaErr != nil {
					logger.Warn("clear replay error observability",
						"err", metaErr.Error())
				} else if previous != "" {
					_, suppressed := replayErrorLogs.recover()
					logger.Info("replay recovered", "previous_err", previous,
						"repeats", repeats, "suppressed", suppressed)
				} else {
					replayErrorLogs.recover()
				}
			}
		}

		// A recovered active chain invalidates shadow and needs another pass to
		// reseed + recapture, just as a bounded replay needs another pass to
		// drain HasMore. Treat both as work so idle backoff cannot delay
		// convergence.
		hadWork := recoveryFollowup || flushedTotal > 0 ||
			capSum.EventsAppended > 0 || replayNeedsImmediateFollowup(repSum)
		if opts.afterRunLoopWorkDecision != nil {
			opts.afterRunLoopWorkDecision(hadWork, recoveryFollowup)
		}

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
			// Bound the unresolved-fingerprint dedup map. Tied to the
			// sweep tick so growth is bounded by sweep cadence rather
			// than letting the map drift forever in long-lived daemons.
			_ = sweepFingerprintWarnMap()
			lastSweep = nowTS

			// 4i. Self-terminate gate.
			if ShouldSelfTerminate(emptyCount, nowTS.Sub(bootTime), SelfTerminateOpts{
				BootGrace:           bootGrace,
				EmptySweepThreshold: emptyThreshold,
			}) {
				return gracefulWithSweep(fmt.Sprintf("no live clients for %d sweeps", emptyCount))
			}
		}

		// 4j. Prune capture_events opportunistically.
		if nowTS.Sub(lastPrune) >= pruneEvery {
			if n, pErr := PruneCaptureEvents(ctx, opts.RepoPath, opts.DB, nowTS, eventRetention); pErr != nil {
				logger.Warn("prune events", "err", pErr.Error())
			} else if n > 0 {
				logger.Info("pruned events", "rows", n)
			}
			retention, retentionErr := checkpointStore.ApplyRetention(ctx, opts.RepoPath,
				checkpointpkg.WorktreeID(opts.RepoPath), nowTS)
			if retentionErr != nil {
				logger.Warn("prune checkpoints", "err", retentionErr.Error())
				_ = state.MetaSet(context.Background(), opts.DB, MetaKeyProtectionRetentionOverBudget, "needs_action")
			} else {
				value := "false"
				if retention.OverBudget {
					value = "true"
					logger.Warn("checkpoint content exceeds soft budget",
						"bytes", retention.ContentBytes, "protected_bytes", retention.ProtectedBytes)
				}
				_ = state.MetaSet(context.Background(), opts.DB, MetaKeyProtectionRetentionOverBudget, value)
				if retention.Pruned > 0 {
					logger.Info("pruned published checkpoints", "checkpoints", retention.Pruned)
				}
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
			return gracefulWithSweep("context canceled")
		case <-shutdownCh:
			timer.Stop()
			return gracefulWithSweep("signal shutdown")
		case <-wakeCh:
			timer.Stop()
			currentDelay = opts.Scheduler.Reset()
		case <-validationWakeCh:
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
// work blocks inside replay or recovery. The deferred stop always joins the
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
	return sum.Published > 0 || sum.HasMore || sum.RecaptureRequired
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

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
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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

	// MessageFn produces commit messages. Nil falls back to
	// DeterministicMessage.
	MessageFn MessageFn

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
	// Zero means "drain them all". Tests set it to 1 for tighter control.
	FlushLimit int
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
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	msgFn := opts.MessageFn
	if msgFn == nil {
		msgFn = DeterministicMessage
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
	fp, fpErr := identity.CaptureSelf()
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

	// Resolve the active branch ref / generation up-front. Production
	// callers will eventually rebuild this on a HEAD change; Phase 1
	// keeps it stable across the loop.
	branchRef, headOID := resolveBranch(ctx, opts.RepoPath, logger)
	cctx := CaptureContext{
		BranchRef:        branchRef,
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	currentToken, _ := BranchGenerationToken(ctx, opts.RepoPath)

	// Seed shadow_paths from HEAD before the first capture so files
	// already at HEAD don't generate spurious creates.
	if cctx.BaseHead != "" {
		if seeded, err := BootstrapShadow(ctx, opts.RepoPath, opts.DB, cctx); err != nil {
			logger.Warn("bootstrap shadow", "err", err.Error())
		} else if seeded > 0 {
			logger.Info("shadow bootstrapped", "rows", seeded)
		}
	}

	ignoreChecker := git.NewIgnoreChecker(opts.RepoPath)
	defer func() { _ = ignoreChecker.Close() }()
	matcher := state.NewSensitiveMatcher()

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
		consecutiveErrors    int
		consecutiveIdleTicks int
		emptyCount           int
		currentDelay         = opts.Scheduler.Reset()
		lastSweep            = time.Time{}
		lastPrune            = time.Time{}
		lastRollup           = time.Time{}
		lastRollupUTCDay     = ""
		stopped              bool
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

	for {
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
		// either here or in the sleep select below).
		select {
		case <-wakeCh:
		default:
		}

		// 4d. Branch-generation token check.
		newToken, terr := BranchGenerationToken(ctx, opts.RepoPath)
		if terr != nil {
			logger.Warn("branch token resolve failed", "err", terr.Error())
		} else if !SameGeneration(currentToken, newToken) {
			logger.Info("branch generation bumped",
				"old", currentToken, "new", newToken)
			ts := strconv.FormatFloat(float64(now().UnixNano())/1e9, 'f', -1, 64)
			_ = state.MetaSet(ctx, opts.DB, "branch_token_changed_at", ts)
			_ = state.MetaSet(ctx, opts.DB, "branch_token", newToken)
			currentToken = newToken
			// Refresh HEAD for capture/replay.
			branchRef, headOID = resolveBranch(ctx, opts.RepoPath, logger)
			cctx.BranchRef = branchRef
			cctx.BaseHead = headOID
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

		// 4f. Capture pass.
		var (
			capSum CaptureSummary
			capErr error
		)
		if cctx.BaseHead != "" {
			capSum, capErr = Capture(ctx, opts.RepoPath, opts.DB, cctx, CaptureOpts{
				IgnoreChecker:    ignoreChecker,
				SensitiveMatcher: matcher,
			})
		}

		var (
			repSum ReplaySummary
			repErr error
		)
		if capErr == nil && cctx.BaseHead != "" {
			// 4g. Replay pass.
			repSum, repErr = Replay(ctx, opts.RepoPath, opts.DB, cctx, ReplayOpts{
				MessageFn: msgFn,
				GitDir:    opts.GitDir,
			})
			if repErr == nil && repSum.Published > 0 {
				// Refresh BaseHead — replay advanced HEAD.
				_, head := resolveBranch(ctx, opts.RepoPath, logger)
				cctx.BaseHead = head
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
		if hadWork {
			consecutiveIdleTicks = 0
		} else {
			consecutiveIdleTicks++
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
			// Reset the idle counter so the next tick starts fresh.
			currentDelay = opts.Scheduler.Reset()
			consecutiveIdleTicks = 0
		case <-timer.C:
		}
	}
}

// resolveBranch returns (branchRef, headOID) for the current HEAD. Errors
// are logged + degrade to empty strings; the run loop tolerates a missing
// HEAD (orphan repo) by skipping capture/replay until HEAD resolves.
func resolveBranch(ctx context.Context, repoDir string, logger *slog.Logger) (string, string) {
	// branch ref via symbolic-ref HEAD; fall back to refs/heads/main on
	// detached/orphan states for v1 (CLI lane will tighten this later).
	branch, _ := git.RunBranchRef(ctx, repoDir)
	if branch == "" {
		branch = "refs/heads/main"
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

// (un)used helpers retained for future phases — keep the symbol exported so
// the test build doesn't drop them on compile.
var _ = gitDirEnsureSubdir

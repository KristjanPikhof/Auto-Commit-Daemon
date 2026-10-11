// Package daemon protects worktree changes in durable checkpoints.
// Run owns protection, retention, locking and wakeups; explicit finish owns
// branch publication. A joined helper keeps heartbeat fresh during scans.
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
	"strings"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

const (
	DefaultCheckpointRetentionInterval = time.Hour
	DefaultRollupInterval              = 5 * time.Minute
	daemonProgressHeartbeatInterval    = time.Second
)

// RepoPath, GitDir and DB are required.
// Options configures checkpoint protection. Finish has its own explicit configuration.
type Options struct {
	RepoPath, GitDir             string
	DB                           *state.DB
	SkipDaemonLock               bool
	OperationGate                *sync.RWMutex
	ProtectionGate               *sync.Mutex
	FinishRecoveryDone           bool
	MaxFileBytes                 int64
	SensitiveMatcher             *state.SensitiveMatcher
	SafeIgnoreMatcher            *state.SafeIgnoreMatcher
	Logger                       *slog.Logger
	Scheduler                    Scheduler
	RollupInterval               time.Duration
	CentralStatsDBPath, RepoHash string
	CentralStats                 *central.StatsDB
	Now                          func() time.Time
	WakeCh, ShutdownCh           <-chan struct{}
	SkipSignals                  bool
	FsnotifyEnabled              bool
	FsnotifyDebounce             time.Duration
	FsnotifyMaxWatches           int
	Trace                        acdtrace.Logger
	progressHeartbeatEvery       time.Duration
}

// Run executes the per-repo daemon run loop. Returns nil on graceful
// shutdown (SIGTERM/SIGINT or context cancellation). Returns
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
	func() {
		if opts.OperationGate != nil {
			opts.OperationGate.RLock()
			defer opts.OperationGate.RUnlock()
		}
		if opts.ProtectionGate != nil {
			opts.ProtectionGate.Lock()
			defer opts.ProtectionGate.Unlock()
		}
		if err := store.RecoverPrepared(ctx, opts.RepoPath); err != nil {
			logger.Warn("checkpoint recovery requires attention; new protection remains active", "err", err)
		}
	}()
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
	rollupEvery := opts.RollupInterval
	if rollupEvery <= 0 {
		rollupEvery = DefaultRollupInterval
	}
	var lastRollup time.Time
	var lastDay, previousCheckpoint string
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

type daemonContextHandler struct {
	next    slog.Handler
	context *daemonLogContext
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
			// filesystem hiccup) must not latch checkpoint observations into
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

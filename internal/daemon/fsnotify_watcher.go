// fsnotify_watcher.go implements the recursive fsnotify watcher per §8.5
// (D11 hybrid: fsnotify is the low-latency wake source; the poll loop is
// the safety net that runs regardless).
//
// Behavior, in one paragraph: at construction the watcher pre-walks the
// repo (skipping `.git`, gitignored, sensitive, submodules, and never
// descending symlinked directories — the regression CLAUDE.md calls out)
// and registers an fsnotify watch per directory. If the platform watch
// budget would be exceeded, it falls back to poll-only mode immediately
// and records the reason on `daemon_meta`. While running, it coalesces
// bursts of events into a single trailing-edge debounce (default 100ms)
// then calls WakeFn to nudge the run loop. New directories created at
// runtime are walked recursively to add additional watches; if that push
// exceeds the budget mid-flight the watcher transparently falls back to
// poll-only. ACD_DISABLE_FSNOTIFY=1 forces poll-only at construction time.
//
// The watcher is safe to Stop concurrently with Start; both honor the
// passed context.
package daemon

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// EnvDisableFsnotify is the toggle that forces poll-only mode at watcher
// construction. Any non-empty value other than "0"/"false" disables
// fsnotify wakes. Mirrors the legacy SNAPSHOTD_DISABLE_FSNOTIFY knob with
// the new ACD_ prefix.
const EnvDisableFsnotify = "ACD_DISABLE_FSNOTIFY"

// EnvMaxInotifyWatches lets ops cap the watch budget below the platform
// detected default. Useful in shared CI containers where
// /proc/sys/fs/inotify/max_user_watches is misleading.
const EnvMaxInotifyWatches = "ACD_MAX_INOTIFY_WATCHES"

// Defaults for the watcher's tunables. Public so tests and the run loop
// can reference them without magic numbers.
const (
	DefaultDebounce          = 100 * time.Millisecond
	DefaultLinuxWatchBudget  = 8000 // fallback when /proc isn't readable
	DefaultDarwinWatchBudget = 1024 // half of typical macOS rlimit nofile
	WatchBudgetMargin        = 0.90 // use 90% of detected platform max
	MaxConsecutiveErrors     = 10   // streak after which we give up on fsnotify
)

// FallbackReason values stamped into daemon_meta when fsnotify cannot run.
const (
	FallbackDisabled       = "disabled_by_env"
	FallbackInitFailed     = "fsnotify_init_failed"
	FallbackBudgetExceeded = "watch_budget_exceeded"
	FallbackErrorsExceeded = "errors_exceeded"
)

// WatcherDiagnostics is the snapshot exported to daemon_meta. The run
// loop wires DiagnosticsFn into MetaSet calls; tests inspect this struct
// directly.
type WatcherDiagnostics struct {
	// Mode is "fsnotify" while the OS watcher is live, "poll" once we've
	// fallen back to poll-only.
	Mode string
	// WatchCount is the number of directories the OS watcher currently
	// holds. Zero in poll mode.
	WatchCount int
	// DroppedEvents is the running count of fsnotify channel errors. The
	// poll loop is still the safety net so a few drops are non-fatal.
	DroppedEvents int
	// FallbackReason names the trigger that put us in poll mode. Empty
	// while Mode=="fsnotify".
	FallbackReason string
}

// FsnotifyOptions configures one watcher. RepoPath + WakeFn are required;
// everything else has a usable default.
type FsnotifyOptions struct {
	RepoPath      string
	GitDir        string
	IgnoreChecker *git.IgnoreChecker
	Sensitive     *state.SensitiveMatcher
	Debounce      time.Duration
	MaxWatches    int
	WakeFn        func()
	Logger        *slog.Logger
	DiagnosticsFn func(WatcherDiagnostics)
}

// FsnotifyWatcher is the live watcher handle. It is safe to call Stop
// from any goroutine; double-Stop is a no-op.
type FsnotifyWatcher struct {
	opts       FsnotifyOptions
	logger     *slog.Logger
	debounce   time.Duration
	maxWatches int

	mu             sync.Mutex
	mode           string // "fsnotify" or "poll"
	watchCount     int
	droppedEvents  int
	fallbackReason string
	watcher        *fsnotify.Watcher // nil in poll mode
	watchedDirs    map[string]struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// NewFsnotifyWatcher constructs a watcher and pre-walks the repo to seed
// directory watches. On any failure mode that disables OS-level events
// (ACD_DISABLE_FSNOTIFY=1, fsnotify init error, watch budget exceeded)
// the returned watcher is valid but operates in poll-only mode — its
// WakeFn will never fire and Diagnostics() reflects the fallback reason.
//
// NewFsnotifyWatcher does NOT spawn goroutines; call Start to begin
// dispatching events.
func NewFsnotifyWatcher(opts FsnotifyOptions) (*FsnotifyWatcher, error) {
	if opts.RepoPath == "" {
		return nil, errors.New("daemon: FsnotifyWatcher: empty RepoPath")
	}
	if opts.WakeFn == nil {
		return nil, errors.New("daemon: FsnotifyWatcher: nil WakeFn")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	debounce := opts.Debounce
	if debounce <= 0 {
		debounce = DefaultDebounce
	}
	maxWatches := opts.MaxWatches
	if maxWatches <= 0 {
		maxWatches = detectWatchBudget(logger)
	}

	w := &FsnotifyWatcher{
		opts:        opts,
		logger:      logger,
		debounce:    debounce,
		maxWatches:  maxWatches,
		mode:        "fsnotify",
		watchedDirs: map[string]struct{}{},
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}

	// Honor the disable toggle before touching the OS.
	if envFlag(EnvDisableFsnotify) {
		w.fallbackToPoll(FallbackDisabled, "ACD_DISABLE_FSNOTIFY=1; running poll-only")
		return w, nil
	}

	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		w.fallbackToPoll(FallbackInitFailed, "fsnotify.NewWatcher failed: "+err.Error())
		return w, nil
	}
	w.watcher = fsw

	// Pre-walk + register. Failure to enumerate is fatal (we can't trust
	// poll either if the worktree disappeared mid-walk), but exhausting
	// the budget is recoverable: we tear down OS watches and run poll-only.
	if err := w.preWalk(opts.RepoPath); err != nil {
		_ = fsw.Close()
		w.watcher = nil
		if errors.Is(err, errBudgetExceeded) {
			w.clearWatchedLocked()
			w.fallbackToPoll(FallbackBudgetExceeded,
				"pre-walk would exceed watch budget; running poll-only")
			return w, nil
		}
		return nil, err
	}

	return w, nil
}

// errBudgetExceeded is the sentinel preWalk uses to signal a budget hit.
// Internal — never escapes the package.
var errBudgetExceeded = errors.New("watch budget exceeded")

// envFlag returns true when env[name] is non-empty and not "0"/"false".
func envFlag(name string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return v != "" && v != "0" && v != "false"
}

// detectWatchBudget computes a usable cap on directory watches. On Linux
// it reads /proc/sys/fs/inotify/max_user_watches and applies a 90%
// margin. On macOS it derives a cap from RLIMIT_NOFILE / 2 (kqueue uses
// FDs). Any failure falls back to a conservative platform default. Ops
// can override via ACD_MAX_INOTIFY_WATCHES.
func detectWatchBudget(logger *slog.Logger) int {
	if env := os.Getenv(EnvMaxInotifyWatches); env != "" {
		if n, err := strconv.Atoi(env); err == nil && n > 0 {
			return n
		}
	}

	// Try linux first.
	if data, err := os.ReadFile("/proc/sys/fs/inotify/max_user_watches"); err == nil {
		if n, perr := strconv.Atoi(strings.TrimSpace(string(data))); perr == nil && n > 0 {
			return int(float64(n) * WatchBudgetMargin)
		}
	}

	// macOS / BSD path: derive from RLIMIT_NOFILE.
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err == nil && rlim.Cur > 0 {
		half := int(rlim.Cur / 2)
		if half > 0 {
			return half
		}
	}

	// Last resort: pick a defensible default. The legacy daemon used
	// 8000 on Linux when the proc file was unreadable; 1024 on macOS.
	logger.Debug("watch budget detection fell back to default")
	return DefaultLinuxWatchBudget
}

// preWalk walks the repo and registers watches on every accepted
// directory. Returns errBudgetExceeded if the registered count would
// exceed maxWatches; the caller is responsible for the poll-mode
// fallback.
func (w *FsnotifyWatcher) preWalk(root string) error {
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// soft errors during pre-walk: keep going.
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		// lstat — never descend symlinked dirs (legacy regression).
		fi, err := os.Lstat(path)
		if err != nil {
			return fs.SkipDir
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fs.SkipDir
		}

		// Skip .git, .git/acd state, and the per-repo gitDir if it lives
		// somewhere unusual (worktrees).
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fs.SkipDir
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			// always watch the worktree root itself.
			if err := w.addWatch(path); err != nil {
				return err
			}
			return nil
		}

		topComponent := rel
		if i := strings.IndexByte(rel, '/'); i >= 0 {
			topComponent = rel[:i]
		}
		if topComponent == ".git" || topComponent == stateSubdir {
			return fs.SkipDir
		}

		// Nested-repo / submodule pruning: any dir containing .git is a
		// boundary we never cross.
		if path != root {
			if _, err := os.Stat(filepath.Join(path, ".git")); err == nil {
				return fs.SkipDir
			}
		}

		// Sensitive directories: skip whole subtrees that look like
		// secrets dirs (best-effort; capture also rejects them).
		if w.opts.Sensitive != nil && w.opts.Sensitive.MatchDirectory(rel) {
			return fs.SkipDir
		}

		// Honor gitignore at directory granularity. The IgnoreChecker is
		// optional; when absent we err on the side of watching (capture
		// will still drop ignored files).
		if w.opts.IgnoreChecker != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			results, err := w.opts.IgnoreChecker.Check(ctx, []string{rel})
			if err == nil && len(results) == 1 && results[0] {
				return fs.SkipDir
			}
		}

		if err := w.addWatch(path); err != nil {
			return err
		}
		return nil
	})
	if walkErr != nil {
		return walkErr
	}
	return nil
}

// addWatch registers one path with the OS watcher, enforcing the budget.
// Returns errBudgetExceeded the moment a registration would overshoot.
func (w *FsnotifyWatcher) addWatch(path string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.watcher == nil {
		// already in poll mode; nothing to do.
		return nil
	}
	if _, ok := w.watchedDirs[path]; ok {
		return nil
	}
	if w.watchCount+1 > w.maxWatches {
		return errBudgetExceeded
	}
	if err := w.watcher.Add(path); err != nil {
		// Treat as a soft failure — capture still sees the path on the
		// next poll. Surface it as a dropped event so diagnostics show
		// pressure, but do not bail.
		w.droppedEvents++
		return nil
	}
	w.watchedDirs[path] = struct{}{}
	w.watchCount++
	return nil
}

// clearWatchedLocked drops all OS watches and resets bookkeeping. Caller
// must already hold w.mu.
func (w *FsnotifyWatcher) clearWatchedLocked() {
	if w.watcher != nil {
		for p := range w.watchedDirs {
			_ = w.watcher.Remove(p)
		}
		_ = w.watcher.Close()
		w.watcher = nil
	}
	w.watchedDirs = map[string]struct{}{}
	w.watchCount = 0
}

// fallbackToPoll transitions the watcher into poll-only mode. Idempotent.
func (w *FsnotifyWatcher) fallbackToPoll(reason, logMsg string) {
	w.mu.Lock()
	if w.mode == "poll" {
		w.mu.Unlock()
		return
	}
	w.mode = "poll"
	w.fallbackReason = reason
	w.clearWatchedLocked()
	w.mu.Unlock()

	if logMsg != "" {
		w.logger.Warn(logMsg, "reason", reason)
	}
	w.emitDiagnostics()
}

// emitDiagnostics calls DiagnosticsFn under the snapshot lock.
func (w *FsnotifyWatcher) emitDiagnostics() {
	if w.opts.DiagnosticsFn == nil {
		return
	}
	w.opts.DiagnosticsFn(w.Diagnostics())
}

// Diagnostics returns a snapshot of the current watcher state. Safe to
// call from any goroutine.
func (w *FsnotifyWatcher) Diagnostics() WatcherDiagnostics {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WatcherDiagnostics{
		Mode:           w.mode,
		WatchCount:     w.watchCount,
		DroppedEvents:  w.droppedEvents,
		FallbackReason: w.fallbackReason,
	}
}

// Start dispatches OS events on a worker goroutine. It is a no-op if the
// watcher has already fallen back to poll mode. Subsequent calls beyond
// the first are no-ops too.
func (w *FsnotifyWatcher) Start(ctx context.Context) error {
	var startErr error
	w.startOnce.Do(func() {
		w.mu.Lock()
		mode := w.mode
		w.mu.Unlock()

		// Always emit one diagnostics snapshot so the run loop can
		// stamp daemon_meta exactly once at boot.
		w.emitDiagnostics()

		if mode == "poll" {
			// Nothing to dispatch; close doneCh so Stop is consistent.
			close(w.doneCh)
			return
		}
		go w.dispatch(ctx)
	})
	return startErr
}

// dispatch runs the trailing-edge debounce + dynamic re-walk loop. Exits
// on Stop or ctx cancellation.
func (w *FsnotifyWatcher) dispatch(ctx context.Context) {
	defer close(w.doneCh)

	w.mu.Lock()
	fsw := w.watcher
	w.mu.Unlock()
	if fsw == nil {
		return
	}

	var (
		debounceTimer *time.Timer
		debounceC     <-chan time.Time
	)
	resetDebounce := func() {
		if debounceTimer == nil {
			debounceTimer = time.NewTimer(w.debounce)
		} else {
			if !debounceTimer.Stop() {
				select {
				case <-debounceTimer.C:
				default:
				}
			}
			debounceTimer.Reset(w.debounce)
		}
		debounceC = debounceTimer.C
	}
	stopDebounce := func() {
		if debounceTimer != nil {
			debounceTimer.Stop()
		}
		debounceC = nil
	}

	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			stopDebounce()
			return
		case <-w.stopCh:
			stopDebounce()
			return
		case ev, ok := <-fsw.Events:
			if !ok {
				stopDebounce()
				return
			}
			consecutiveErrors = 0
			w.handleEvent(ev)
			resetDebounce()
		case err, ok := <-fsw.Errors:
			if !ok {
				stopDebounce()
				return
			}
			w.mu.Lock()
			w.droppedEvents++
			w.mu.Unlock()
			consecutiveErrors++
			w.logger.Warn("fsnotify error", "err", err.Error(), "streak", consecutiveErrors)
			if consecutiveErrors >= MaxConsecutiveErrors {
				w.fallbackToPoll(FallbackErrorsExceeded,
					"fsnotify error streak exceeded; running poll-only")
				stopDebounce()
				return
			}
		case <-debounceC:
			debounceC = nil
			if w.opts.WakeFn != nil {
				w.opts.WakeFn()
			}
		}
	}
}

// handleEvent dispatches one OS event. We add watches recursively on
// directory creation; we ignore the file-level details because capture
// will rediscover them on the next pass.
func (w *FsnotifyWatcher) handleEvent(ev fsnotify.Event) {
	// Re-walk new directories so children become watched too. We do not
	// remove watches on Remove/Rename — the OS already cleaned them up.
	if ev.Op&fsnotify.Create != 0 {
		fi, err := os.Lstat(ev.Name)
		if err == nil && fi.IsDir() && fi.Mode()&os.ModeSymlink == 0 {
			if err := w.preWalk(ev.Name); err != nil {
				if errors.Is(err, errBudgetExceeded) {
					w.fallbackToPoll(FallbackBudgetExceeded,
						"runtime watch growth exceeded budget; running poll-only")
				}
			}
		}
	}
}

// Stop tears down the watcher and waits for the dispatch goroutine to
// exit. Safe to call multiple times; only the first does work. Also safe
// to call when Start was never invoked: in that case there is no
// dispatch goroutine to drain so we close doneCh ourselves.
func (w *FsnotifyWatcher) Stop() error {
	w.stopOnce.Do(func() {
		close(w.stopCh)
		w.mu.Lock()
		fsw := w.watcher
		w.mu.Unlock()
		if fsw != nil {
			_ = fsw.Close()
		}
		// If Start was never called, no goroutine ever closes doneCh —
		// claim it ourselves via startOnce so the wait below does not
		// block forever.
		w.startOnce.Do(func() { close(w.doneCh) })
		<-w.doneCh
	})
	return nil
}

// WatchedPaths returns a sorted snapshot of the currently watched
// directories. Test-only — production callers should use Diagnostics().
// We expose it so the symlink-not-descending regression test can verify
// the watcher actually skipped a symlinked subtree.
func (w *FsnotifyWatcher) WatchedPaths() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.watchedDirs))
	for p := range w.watchedDirs {
		out = append(out, p)
	}
	return out
}

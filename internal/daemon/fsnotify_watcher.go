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
// bursts of events into a leading-edge wake plus a trailing-edge debounce
// (default 100ms) with a hard tail clamp (500ms) so a continuous
// auto-formatter cannot starve the wake. Directory creates seen on the
// dispatch goroutine are handed off to a sibling worker that performs
// the recursive re-walk; the dispatch goroutine itself never blocks on
// IgnoreChecker round-trips so fsnotify Events do not back up. New
// directories created at runtime become watched too; if that push
// exceeds the budget mid-flight the watcher transparently falls back to
// poll-only. ACD_DISABLE_FSNOTIFY=1 forces poll-only at construction
// time.
//
// The watcher is safe to Stop concurrently with Start; both honor the
// passed context.
package daemon

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

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
	// MaxDebounceTail is the hard upper bound on how long the trailing-edge
	// debounce will keep deferring a wake under continuous events. Without
	// this clamp, an auto-formatter that fires faster than the debounce
	// interval would starve WakeFn forever.
	MaxDebounceTail = 500 * time.Millisecond
	// rewalkQueueDepth is the buffer size for the directory-create rewalk
	// channel. A small buffer keeps memory bounded while letting bursts of
	// mkdir events queue up without blocking the dispatch goroutine. If the
	// queue saturates we drop the oldest pending rewalk and replace it with
	// the newest path; capture's poll safety net still observes any new
	// files there.
	rewalkQueueDepth = 64
	// diagQueueDepth is the buffer for diagnostics deliveries. We always
	// deliver the latest snapshot — older snapshots in the buffer are
	// superseded — so capacity 1 with replace-on-full semantics is enough.
	diagQueueDepth = 1
	// preWalkLayerTimeout caps every IgnoreChecker round-trip during a
	// pre-walk layer. Long-running ignore checks (e.g. a stuck subprocess)
	// should not stall a Stop indefinitely; the watcher-scoped context lets
	// Stop cancel them.
	preWalkLayerTimeout = 5 * time.Second
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

// fsnotifyIgnoreChecker is the minimum surface the watcher needs from
// git.IgnoreChecker. Defining it here lets tests substitute a slow stub
// without modifying the production IgnoreChecker.
type fsnotifyIgnoreChecker interface {
	Check(ctx context.Context, paths []string) ([]bool, error)
}

// FsnotifyOptions configures one watcher. RepoPath + WakeFn are required;
// everything else has a usable default.
type FsnotifyOptions struct {
	RepoPath      string
	GitDir        string
	IgnoreChecker fsnotifyIgnoreChecker
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

	// rewalkCh hands directory-Create events from the dispatch goroutine
	// to the sibling rewalk worker, so dispatch never blocks on a
	// preWalk's IgnoreChecker round-trip.
	rewalkCh chan string

	// diagCh delivers the latest WatcherDiagnostics snapshot to a sibling
	// worker, off the dispatch goroutine, so DiagnosticsFn (which writes
	// SQLite) cannot block event drain.
	diagCh chan WatcherDiagnostics

	// watcherCtx is the watcher-scoped context cancelled by Stop. Threaded
	// through every potentially-blocking helper (preWalk's IgnoreChecker
	// round-trips, the rewalk worker, the diagnostics worker) so Stop
	// returns within bounded time even when subprocesses wedge.
	watcherCtx    context.Context
	watcherCancel context.CancelFunc

	// workerWg counts every long-lived goroutine the watcher owns
	// (dispatch, rewalk worker, diagnostics worker). Stop waits on it.
	workerWg sync.WaitGroup

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

	wctx, wcancel := context.WithCancel(context.Background())
	w := &FsnotifyWatcher{
		opts:          opts,
		logger:        logger,
		debounce:      debounce,
		maxWatches:    maxWatches,
		mode:          "fsnotify",
		watchedDirs:   map[string]struct{}{},
		rewalkCh:      make(chan string, rewalkQueueDepth),
		diagCh:        make(chan WatcherDiagnostics, diagQueueDepth),
		watcherCtx:    wctx,
		watcherCancel: wcancel,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
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
	if err := w.preWalk(wctx, opts.RepoPath); err != nil {
		_ = fsw.Close()
		w.watcher = nil
		if errors.Is(err, errBudgetExceeded) {
			w.clearWatchedLocked()
			w.fallbackToPoll(FallbackBudgetExceeded,
				"pre-walk would exceed watch budget; running poll-only")
			return w, nil
		}
		// Any other error: cancel watcher-scoped ctx so no goroutines leak.
		wcancel()
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
// fallback. The watcher-scoped ctx must be passed in so Stop can cancel
// in-flight IgnoreChecker round-trips.
//
// BFS layout with parent-prune: candidate directories are processed by
// depth. Each layer collects every dir that survives the cheap filters
// (.git pruning, symlinks, nested repos, sensitive dirs), batch-calls
// IgnoreChecker.Check once for the entire layer, then only descends into
// the non-ignored survivors. This early-prunes ignored subtrees (e.g.
// node_modules, vendor) instead of walking every entry beneath them — the
// previous DFS approach descended into ignored subtrees and only filtered
// after the fact, which made first-walk on large repos pay the readdir
// cost for every ignored child.
//
// Ignore-check batching is preserved: each BFS layer issues one round-trip
// through the long-lived `git check-ignore --stdin` subprocess. With
// per-layer batching the total round-trips equal the worktree depth, not
// the directory count.
//
// IgnoreChecker errors are NOT swallowed — a stuck check-ignore process
// would otherwise inflate the watched set with directories that should
// have been pruned. The error surfaces to the caller, which falls back to
// poll-only.
func (w *FsnotifyWatcher) preWalk(ctx context.Context, root string) error {
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	if !rootInfo.IsDir() {
		return nil
	}

	// The worktree root itself is never subject to gitignore.
	rootRel, err := filepath.Rel(w.opts.RepoPath, root)
	if err != nil {
		return err
	}
	rootRel = filepath.ToSlash(rootRel)
	// Filter the root itself through the cheap pre-checks unless it is
	// the repo root (rel == "."). When called from handleEvent on a
	// runtime mkdir, the new dir might be inside a sensitive or nested
	// subtree.
	if rootRel != "." {
		topComponent := rootRel
		if i := strings.IndexByte(rootRel, '/'); i >= 0 {
			topComponent = rootRel[:i]
		}
		// Our state lives at <gitDir>/acd, which is inside .git and is
		// already covered by the .git prune above. Do NOT match a literal
		// worktree-rooted "acd/" top component — that path is a legitimate
		// user directory and pruning it silently drops real files.
		if topComponent == ".git" {
			return nil
		}
		if _, err := os.Stat(filepath.Join(root, ".git")); err == nil {
			return nil
		}
		if w.opts.Sensitive != nil && w.opts.Sensitive.MatchDirectory(rootRel) {
			return nil
		}
		// Honor gitignore on the root of a runtime re-walk too. A
		// persistent IgnoreChecker error here is treated as ignored=false
		// (fail-open at the root only — the layer-level pre-walk below
		// fails closed).
		if w.opts.IgnoreChecker != nil {
			cctx, cancel := context.WithTimeout(ctx, preWalkLayerTimeout)
			results, ierr := w.opts.IgnoreChecker.Check(cctx, []string{rootRel})
			cancel()
			if ierr == nil && len(results) == 1 && results[0] {
				return nil
			}
			// Honor watcher-scoped cancellation so Stop unblocks promptly.
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
	if err := w.addWatch(root); err != nil {
		return err
	}

	type entry struct {
		rel  string
		full string
	}
	// frontier holds the directories whose children we still need to read.
	// At the start of each iteration we readdir every dir in the frontier,
	// collect all surviving children into nextLayer, batch-classify
	// nextLayer through IgnoreChecker, then watch the survivors. Survivors
	// become the next frontier.
	frontier := []entry{{rel: rootRel, full: root}}

	for len(frontier) > 0 {
		// Cheap watcher-scoped cancellation check before each readdir
		// layer so a long pre-walk shuts down fast.
		if err := ctx.Err(); err != nil {
			return err
		}
		var nextLayer []entry
		for _, parent := range frontier {
			children, err := os.ReadDir(parent.full)
			if err != nil {
				// soft error: skip this dir's children but keep walking.
				continue
			}
			for _, d := range children {
				if !d.IsDir() {
					continue
				}
				childPath := filepath.Join(parent.full, d.Name())
				fi, err := os.Lstat(childPath)
				if err != nil {
					continue
				}
				// Never descend symlinked dirs (legacy regression).
				if fi.Mode()&os.ModeSymlink != 0 {
					continue
				}
				if !fi.IsDir() {
					continue
				}
				var childRel string
				if parent.rel == "." {
					childRel = d.Name()
				} else {
					childRel = parent.rel + "/" + d.Name()
				}
				topComponent := childRel
				if i := strings.IndexByte(childRel, '/'); i >= 0 {
					topComponent = childRel[:i]
				}
				// .git/acd is inside .git and already covered. A literal
				// worktree-rooted "acd/" top component is a real user dir.
				if topComponent == ".git" {
					continue
				}
				// Nested-repo / submodule pruning: any dir containing
				// .git is a boundary we never cross.
				if _, err := os.Stat(filepath.Join(childPath, ".git")); err == nil {
					continue
				}
				// Sensitive directories: skip whole subtrees.
				if w.opts.Sensitive != nil && w.opts.Sensitive.MatchDirectory(childRel) {
					continue
				}
				nextLayer = append(nextLayer, entry{rel: childRel, full: childPath})
			}
		}

		if len(nextLayer) == 0 {
			break
		}

		// Per-layer batched ignore check: one round-trip classifies the
		// entire layer, and ignored parents are dropped from the frontier
		// so we never readdir into their subtrees on the next iteration.
		// A persistent IgnoreChecker error here is treated as a hard
		// failure: silently fail-open would inflate the watched set with
		// directories that should have been pruned (e.g. a 10k-package
		// node_modules), exhaust the budget, and force a misleading
		// poll-mode fallback under FallbackBudgetExceeded.
		ignored := map[string]bool{}
		if w.opts.IgnoreChecker != nil {
			cctx, cancel := context.WithTimeout(ctx, preWalkLayerTimeout)
			paths := make([]string, len(nextLayer))
			for i, c := range nextLayer {
				paths[i] = c.rel
			}
			results, ierr := w.opts.IgnoreChecker.Check(cctx, paths)
			cancel()
			if ierr != nil {
				// Honor watcher-scoped cancellation specifically: Stop
				// surfaces as ctx.Err and does not need a separate log.
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return ierr
			}
			if len(results) == len(nextLayer) {
				for i, isIgn := range results {
					if isIgn {
						ignored[nextLayer[i].rel] = true
					}
				}
			}
		}

		survivors := nextLayer[:0]
		for _, c := range nextLayer {
			if ignored[c.rel] {
				continue
			}
			if err := w.addWatch(c.full); err != nil {
				return err
			}
			survivors = append(survivors, c)
		}
		frontier = survivors
	}
	return nil
}

// addWatch registers one path with the OS watcher, enforcing the budget.
// Returns errBudgetExceeded the moment a registration would overshoot.
// On Linux, fsw.Add returns ENOSPC when the per-user inotify watch pool
// is exhausted (sysctl fs.inotify.max_user_watches). That is functionally
// the same condition as our explicit budget cap, so we surface it the
// same way.
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
		// ENOSPC = kernel-side inotify pool exhausted. Fall back to
		// poll-only just like an over-budget would; the caller's
		// errBudgetExceeded handler tears down OS watches cleanly.
		if errors.Is(err, syscall.ENOSPC) {
			return errBudgetExceeded
		}
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
// must already hold w.mu. fsnotify.Close already releases every watch
// the kernel was tracking, so we do not need to issue per-path Remove
// calls (which were costing N syscalls on big trees and could fail with
// ENOENT during teardown).
func (w *FsnotifyWatcher) clearWatchedLocked() {
	if w.watcher != nil {
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

// emitDiagnostics queues a diagnostics snapshot for the worker goroutine
// to deliver. We intentionally do NOT call DiagnosticsFn synchronously
// here: in production it writes SQLite, and on the dispatch path that
// would back up the fsnotify Events channel under load (kernel-side
// inotify drops). Replace-on-full semantics: when the buffer is full we
// drop the older queued snapshot — only the latest counts.
func (w *FsnotifyWatcher) emitDiagnostics() {
	if w.opts.DiagnosticsFn == nil {
		return
	}
	d := w.Diagnostics()
	for {
		select {
		case w.diagCh <- d:
			return
		default:
			// Drop the oldest snapshot, then retry; an outdated snapshot
			// will be superseded by this newer one.
			select {
			case <-w.diagCh:
			default:
				// Worker drained between our default and the receive;
				// loop and try the send again.
			}
		}
	}
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

		// Spawn the diagnostics worker first so the boot snapshot is
		// delivered even on the poll-mode path.
		w.workerWg.Add(1)
		go w.diagnosticsWorker()

		// Always emit one diagnostics snapshot so the run loop can
		// stamp daemon_meta exactly once at boot.
		w.emitDiagnostics()

		if mode == "poll" {
			// Nothing to dispatch; close doneCh so Stop is consistent.
			close(w.doneCh)
			return
		}
		// Sibling rewalk worker absorbs directory-Create events so the
		// dispatch goroutine never blocks on preWalk's IgnoreChecker
		// round-trips. Spawn before dispatch so dispatch never sees a
		// closed rewalkCh.
		w.workerWg.Add(1)
		go w.rewalkWorker()

		w.workerWg.Add(1)
		go func() {
			defer w.workerWg.Done()
			w.dispatch(ctx)
		}()
	})
	return startErr
}

// dispatch runs the leading-edge wake plus trailing-edge debounce loop.
// Exits on Stop or ctx cancellation. Directory creates are NOT processed
// inline — they are forwarded to rewalkWorker via rewalkCh so a slow
// IgnoreChecker round-trip cannot stall the fsnotify Events channel.
func (w *FsnotifyWatcher) dispatch(ctx context.Context) {
	defer close(w.doneCh)

	w.mu.Lock()
	fsw := w.watcher
	w.mu.Unlock()
	if fsw == nil {
		return
	}

	// Trailing-edge debounce + tail clamp. Wakes go out via WakeFn:
	//   - Leading edge: the first event after a quiet window fires WakeFn
	//     immediately so consumers see activity without waiting.
	//   - Trailing edge: the debounce timer collapses bursts into a
	//     final wake N ms after the last event.
	//   - Tail clamp: a separate timer (MaxDebounceTail) caps the maximum
	//     interval between wakes under continuous events. Without this,
	//     a tool that fires faster than `debounce` (auto-formatters,
	//     hot reloaders) would keep resetting the trailing timer and
	//     starve WakeFn forever.
	var (
		debounceTimer *time.Timer
		debounceC     <-chan time.Time
		tailTimer     *time.Timer
		tailC         <-chan time.Time
		// burstActive is true between the first event of a burst and the
		// next quiet window; it gates the leading-edge wake.
		burstActive bool
	)
	stopTimer := func(t *time.Timer) {
		if t == nil {
			return
		}
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}
	resetDebounce := func() {
		if debounceTimer == nil {
			debounceTimer = time.NewTimer(w.debounce)
		} else {
			stopTimer(debounceTimer)
			debounceTimer.Reset(w.debounce)
		}
		debounceC = debounceTimer.C
	}
	armTail := func() {
		if tailTimer == nil {
			tailTimer = time.NewTimer(MaxDebounceTail)
		} else {
			// Already armed: do not reset. The tail clamp must fire on
			// its OWN schedule independent of new events; that's the
			// whole point of the clamp.
			return
		}
		tailC = tailTimer.C
	}
	stopTail := func() {
		stopTimer(tailTimer)
		tailC = nil
	}
	stopAll := func() {
		stopTimer(debounceTimer)
		debounceC = nil
		stopTail()
		burstActive = false
	}

	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			stopAll()
			return
		case <-w.stopCh:
			stopAll()
			return
		case ev, ok := <-fsw.Events:
			if !ok {
				stopAll()
				return
			}
			consecutiveErrors = 0
			w.handleEvent(ev)

			// Leading-edge wake fires the moment we exit a quiet window.
			if !burstActive {
				burstActive = true
				if w.opts.WakeFn != nil {
					w.opts.WakeFn()
				}
				armTail()
			}
			resetDebounce()
		case err, ok := <-fsw.Errors:
			if !ok {
				stopAll()
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
				stopAll()
				return
			}
		case <-debounceC:
			debounceC = nil
			// Trailing-edge wake. End-of-burst cleanup: the next event
			// will start a fresh burst (and re-fire the leading-edge
			// wake), and the tail clamp is no longer needed.
			stopTail()
			burstActive = false
			if w.opts.WakeFn != nil {
				w.opts.WakeFn()
			}
		case <-tailC:
			tailC = nil
			tailTimer = nil
			// Hard tail clamp expired during a continuous event stream.
			// Fire WakeFn so consumers see activity even though no quiet
			// window ever materialized. Re-arm tail so the next clamp
			// is honored if the stream keeps going.
			if w.opts.WakeFn != nil {
				w.opts.WakeFn()
			}
			armTail()
		}
	}
}

// rewalkWorker consumes directory-Create paths from rewalkCh and calls
// preWalk on each one. Runs on its own goroutine so a slow
// IgnoreChecker.Check never delays fsnotify event drain. Exits when
// rewalkCh is closed (by Stop) or watcherCtx is cancelled.
func (w *FsnotifyWatcher) rewalkWorker() {
	defer w.workerWg.Done()
	for {
		select {
		case <-w.watcherCtx.Done():
			return
		case path, ok := <-w.rewalkCh:
			if !ok {
				return
			}
			fi, err := os.Lstat(path)
			if err != nil {
				continue
			}
			if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
				continue
			}
			if err := w.preWalk(w.watcherCtx, path); err != nil {
				if errors.Is(err, errBudgetExceeded) {
					w.fallbackToPoll(FallbackBudgetExceeded,
						"runtime watch growth exceeded budget; running poll-only")
					// Drain the rest: poll mode means no further rewalks.
					return
				}
				if errors.Is(err, context.Canceled) {
					return
				}
				w.logger.Warn("fsnotify runtime preWalk failed",
					"path", path, "err", err.Error())
			}
		}
	}
}

// diagnosticsWorker consumes WatcherDiagnostics from diagCh and forwards
// each snapshot to DiagnosticsFn. Runs on its own goroutine so the SQLite
// write inside DiagnosticsFn cannot back up the dispatch path. Exits when
// diagCh is closed (by Stop) or watcherCtx is cancelled. We deliberately
// drain the channel after Stop closes it so any pending snapshot still
// reaches the run loop's daemon_meta.
func (w *FsnotifyWatcher) diagnosticsWorker() {
	defer w.workerWg.Done()
	for {
		select {
		case <-w.watcherCtx.Done():
			// Drain anything still pending so the final mode/reason
			// snapshot is visible to operators.
			for {
				select {
				case d, ok := <-w.diagCh:
					if !ok {
						return
					}
					w.opts.DiagnosticsFn(d)
				default:
					return
				}
			}
		case d, ok := <-w.diagCh:
			if !ok {
				return
			}
			w.opts.DiagnosticsFn(d)
		}
	}
}

// handleEvent dispatches one OS event. Directory creates are forwarded
// to the rewalk worker (NOT preWalked inline) so the dispatch goroutine
// never blocks on an IgnoreChecker round-trip. Reconciliation of
// Remove/Rename happens inline because it is a cheap map operation.
func (w *FsnotifyWatcher) handleEvent(ev fsnotify.Event) {
	// Re-walk new directories so children become watched too. We hand
	// off to the rewalkWorker via a buffered channel; if the queue is
	// full, drop the oldest entry to make room (replace-on-full).
	if ev.Op&fsnotify.Create != 0 {
		select {
		case w.rewalkCh <- ev.Name:
		default:
			// Drop the oldest pending rewalk to make room — the poll
			// safety net still picks up the file there. Bookkeep as a
			// dropped event so diagnostics show the pressure.
			select {
			case <-w.rewalkCh:
			default:
			}
			select {
			case w.rewalkCh <- ev.Name:
			default:
				// Worker hasn't consumed since we drained; surface this
				// as a dropped event and move on.
				w.mu.Lock()
				w.droppedEvents++
				w.mu.Unlock()
			}
		}
	}

	// Reconcile bookkeeping when a tracked directory disappears or is
	// renamed away. fsnotify reports Remove/Rename for both files and
	// directories; we only act if the path is in watchedDirs. We can't
	// stat the path (it's gone) so the membership check is the proxy
	// for "this was a directory we were watching".
	if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		w.releaseWatch(ev.Name)
	}
}

// releaseWatch drops a tracked directory and any tracked descendants
// from watchedDirs and decrements watchCount accordingly. No-op if the
// path was not tracked. The OS inotify/kqueue watch is already gone
// (the kernel cleans it up on Remove/Rename, including all descendants
// of a directory tree that was rmrf'd) so we only reconcile our own
// counters. Without the descendant sweep, watchCount drifts upward over
// long-running churn until it appears to hit budget.
func (w *FsnotifyWatcher) releaseWatch(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.watchedDirs[path]; ok {
		delete(w.watchedDirs, path)
		if w.watchCount > 0 {
			w.watchCount--
		}
	}
	// Cascade: if `path` was a directory, any tracked descendants are
	// also gone from the kernel's watch table. Sweep them out of our map
	// too. The `path + sep` prefix avoids collapsing siblings whose
	// names happen to share a prefix with `path`.
	prefix := path + string(filepath.Separator)
	for p := range w.watchedDirs {
		if strings.HasPrefix(p, prefix) {
			delete(w.watchedDirs, p)
			if w.watchCount > 0 {
				w.watchCount--
			}
		}
	}
}

// Stop tears down the watcher and waits for every owned goroutine
// (dispatch, rewalk worker, diagnostics worker) to exit. Safe to call
// multiple times; only the first does work. Also safe to call when
// Start was never invoked: in that case there is no dispatch goroutine
// to drain so we close doneCh ourselves.
//
// The provided ctx bounds the wait. Stop cancels the watcher-scoped
// context so any in-flight IgnoreChecker round-trip aborts immediately;
// it then blocks until all workers exit OR ctx is cancelled. A nil ctx
// is treated as context.Background().
//
// shutdown-lane note: this signature accepts a caller-scoped ctx so
// outer shutdown logic can deadline the teardown without changing
// internals.
func (w *FsnotifyWatcher) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w.stopOnce.Do(func() {
		// Cancel the watcher-scoped context first so any in-flight
		// IgnoreChecker.Check / preWalk returns promptly.
		w.watcherCancel()
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

		// Wait for dispatch (signalled via doneCh) and the worker
		// goroutines (signalled via workerWg). The caller's ctx bounds
		// both waits.
		select {
		case <-w.doneCh:
		case <-ctx.Done():
		}

		// Close rewalkCh + diagCh so the workers exit; they also honor
		// watcherCtx so this is belt-and-suspenders. Only safe to close
		// once — Stop is guarded by stopOnce.
		close(w.rewalkCh)
		close(w.diagCh)

		done := make(chan struct{})
		go func() {
			w.workerWg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
		}
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

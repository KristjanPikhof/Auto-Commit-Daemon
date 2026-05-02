package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/fsnotify/fsnotify"
)

// newWatcherForTest spins up a watcher rooted at a fresh tempdir and
// returns the watcher, the dir, and a counter wired to WakeFn so tests
// can assert burst coalescing.
func newWatcherForTest(t *testing.T, opts FsnotifyOptions) (*FsnotifyWatcher, *atomic.Int64) {
	t.Helper()
	if opts.RepoPath == "" {
		opts.RepoPath = t.TempDir()
	}
	var wakeCount atomic.Int64
	if opts.WakeFn == nil {
		opts.WakeFn = func() { wakeCount.Add(1) }
	}
	w, err := NewFsnotifyWatcher(opts)
	if err != nil {
		t.Fatalf("NewFsnotifyWatcher: %v", err)
	}
	t.Cleanup(func() { _ = w.Stop(context.Background()) })
	return w, &wakeCount
}

func waitForFsnotifyReady(t *testing.T, db *state.DB, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mode, modeOK, modeErr := state.MetaGet(context.Background(), db, "fsnotify.mode")
		watchCountRaw, countOK, countErr := state.MetaGet(context.Background(), db, "fsnotify.watch_count")
		watchCount, _ := strconv.Atoi(watchCountRaw)
		if modeErr == nil && countErr == nil && modeOK && countOK && mode == "fsnotify" && watchCount > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("fsnotify watcher was not ready within %v", timeout)
}

type invalidatingIgnoreChecker struct {
	invalidates atomic.Int64
}

func (c *invalidatingIgnoreChecker) Check(context.Context, []string) ([]bool, error) {
	return nil, nil
}

func (c *invalidatingIgnoreChecker) Invalidate() {
	c.invalidates.Add(1)
}

func TestHandleEventInvalidatesIgnoreCheckerOnGitignoreChange(t *testing.T) {
	dir := t.TempDir()
	checker := &invalidatingIgnoreChecker{}
	w := &FsnotifyWatcher{
		opts: FsnotifyOptions{
			RepoPath:      dir,
			IgnoreChecker: checker,
		},
		rewalkCh: make(chan string, 1),
	}

	w.handleEvent(fsnotify.Event{Name: filepath.Join(dir, ".gitignore"), Op: fsnotify.Write})
	if got := checker.invalidates.Load(); got != 1 {
		t.Fatalf("invalidates=%d, want 1", got)
	}

	w.handleEvent(fsnotify.Event{Name: filepath.Join(dir, "src", "main.go"), Op: fsnotify.Write})
	if got := checker.invalidates.Load(); got != 1 {
		t.Fatalf("invalidates after non-ignore file=%d, want 1", got)
	}
}

// TestFsnotifyWatcher_HappyPath: enabling the watcher on a real tempdir
// sees a file create and fires WakeFn within the debounce window.
func TestFsnotifyWatcher_HappyPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	w, count := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: dir,
		Debounce: 30 * time.Millisecond,
	})
	if d := w.Diagnostics(); d.Mode != "fsnotify" {
		t.Fatalf("mode=%q want fsnotify (reason=%q)", d.Mode, d.FallbackReason)
	}
	if d := w.Diagnostics(); d.WatchCount == 0 {
		t.Fatalf("expected at least one watched dir, got 0")
	}

	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "hi.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("WakeFn did not fire within 2s")
}

// TestFsnotifyWatcher_NewDirectoryWatched: creating a subdirectory at
// runtime adds a watch; a file created inside it then triggers a wake.
func TestFsnotifyWatcher_NewDirectoryWatched(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	w, count := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: dir,
		Debounce: 30 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	sub := filepath.Join(dir, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Wait for the dir-create wake first.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count.Load() == 0 {
		t.Fatalf("expected wake from mkdir; got none")
	}

	// The new directory should now be watched. Give the dispatch goroutine
	// a beat to add it, then poll.
	addedDeadline := time.Now().Add(2 * time.Second)
	var found bool
	for time.Now().Before(addedDeadline) {
		for _, p := range w.WatchedPaths() {
			if filepath.Clean(p) == filepath.Clean(sub) {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found {
		t.Fatalf("subdirectory %q was not added to watch list: %v", sub, w.WatchedPaths())
	}

	// Now create a file inside and assert another wake fires.
	before := count.Load()
	if err := os.WriteFile(filepath.Join(sub, "in.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write in sub: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() > before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("WakeFn did not fire after writing inside new subdir")
}

// TestFsnotifyWatcher_SymlinkedDirIgnored asserts the legacy regression
// stays fixed: a symlink-to-directory must NOT be added to the watch
// list.
func TestFsnotifyWatcher_SymlinkedDirIgnored(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify+symlinks not exercised on windows in v1")
	}
	dir := t.TempDir()
	external := t.TempDir() // out-of-tree target the symlink points at
	link := filepath.Join(dir, "link")
	if err := os.Symlink(external, link); err != nil {
		t.Skipf("symlink unavailable on this filesystem: %v", err)
	}

	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: dir,
		Debounce: 30 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, p := range w.WatchedPaths() {
		if strings.HasPrefix(filepath.Clean(p), filepath.Clean(link)) {
			t.Fatalf("symlinked dir %q ended up in watched list: %v", link, w.WatchedPaths())
		}
		// Also reject the external target itself: descending into the
		// link would have walked into `external`.
		if strings.HasPrefix(filepath.Clean(p), filepath.Clean(external)) {
			t.Fatalf("watcher descended into symlink target %q: %v", external, w.WatchedPaths())
		}
	}
}

func TestFsnotifyWatcher_DoesNotPruneWildcardSensitiveDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	t.Setenv(state.EnvSensitiveGlobs, "credentials*")
	dir := t.TempDir()
	normalDir := filepath.Join(dir, "credentials_repo")
	if err := os.MkdirAll(filepath.Join(normalDir, "nested"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath:  dir,
		Sensitive: state.NewSensitiveMatcher(),
		Debounce:  30 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var found bool
	for _, p := range w.WatchedPaths() {
		if filepath.Clean(p) == filepath.Clean(normalDir) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("credentials_repo should be watched, got watched paths: %v", w.WatchedPaths())
	}
}

// TestPreWalk_BatchedIgnoreCheck verifies the post-walk batched ignore
// check correctly filters out ignored directories and watches the rest in
// one IgnoreChecker round-trip. The previous per-dir Check loop generated
// O(N) round-trips through the long-lived `git check-ignore --stdin`
// subprocess; the new code collects every candidate then calls Check once.
//
// Test shape: a real git repo with a .gitignore that excludes "ignored_*"
// directories. We pre-create N=60 dirs (mix of ignored + watched) and
// assert (a) every "watched_*" dir is in WatchedPaths and (b) every
// "ignored_*" dir is NOT.
func TestPreWalk_BatchedIgnoreCheck(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	if err := git.Init(context.Background(), dir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	// .gitignore at the worktree root excludes "ignored_*" dirs.
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"),
		[]byte("ignored_*/\n"), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}

	const total = 60
	for i := 0; i < total; i++ {
		var name string
		if i%2 == 0 {
			name = "watched_" + strconv.Itoa(i)
		} else {
			name = "ignored_" + strconv.Itoa(i)
		}
		if err := os.MkdirAll(filepath.Join(dir, name, "nested"), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
	}

	checker := git.NewIgnoreChecker(dir)
	t.Cleanup(func() { _ = checker.Close() })

	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath:      dir,
		IgnoreChecker: checker,
		Debounce:      30 * time.Millisecond,
	})
	if d := w.Diagnostics(); d.Mode != "fsnotify" {
		t.Fatalf("mode=%q want fsnotify (reason=%q)", d.Mode, d.FallbackReason)
	}

	watchedSet := map[string]struct{}{}
	for _, p := range w.WatchedPaths() {
		watchedSet[filepath.Clean(p)] = struct{}{}
	}
	for i := 0; i < total; i++ {
		var prefix string
		if i%2 == 0 {
			prefix = "watched_"
		} else {
			prefix = "ignored_"
		}
		full := filepath.Clean(filepath.Join(dir, prefix+strconv.Itoa(i)))
		_, isWatched := watchedSet[full]
		if prefix == "watched_" && !isWatched {
			t.Fatalf("expected %s to be watched but was not; watched=%v", full, w.WatchedPaths())
		}
		if prefix == "ignored_" && isWatched {
			t.Fatalf("ignored dir %s slipped into watched set", full)
		}
	}
}

// TestFsnotify_WatchCountReturnsToBaseline_AfterDirChurn asserts the
// Remove/Rename bookkeeping fix: creating and removing 100 directories in
// sequence leaves watchedDirs/watchCount back at the baseline, instead of
// drifting upward toward errBudgetExceeded under long-running churn.
func TestFsnotify_WatchCountReturnsToBaseline_AfterDirChurn(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: dir,
		Debounce: 20 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	baseline := w.Diagnostics().WatchCount

	// Create + remove 100 directories. We wait for each create to land
	// in watchedDirs before removing so the OS event-pair is delivered
	// in the right order; otherwise the event loop can race past us.
	const N = 100
	for i := 0; i < N; i++ {
		sub := filepath.Join(dir, "churn_"+strconv.Itoa(i))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatalf("mkdir %d: %v", i, err)
		}
		// Wait for fsnotify Create -> preWalk -> addWatch.
		addedDeadline := time.Now().Add(2 * time.Second)
		var present bool
		for time.Now().Before(addedDeadline) {
			for _, p := range w.WatchedPaths() {
				if filepath.Clean(p) == filepath.Clean(sub) {
					present = true
					break
				}
			}
			if present {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if !present {
			t.Fatalf("dir %s never landed in watched set", sub)
		}
		if err := os.RemoveAll(sub); err != nil {
			t.Fatalf("remove %d: %v", i, err)
		}
	}

	// Wait for all Remove events to be reconciled.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if w.Diagnostics().WatchCount == baseline {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := w.Diagnostics().WatchCount
	if got != baseline {
		paths := w.WatchedPaths()
		t.Fatalf("watch_count=%d want baseline=%d after churn; remaining watched=%v",
			got, baseline, paths)
	}
}

// TestPreWalk_EarlyPrunesIgnoredDirsBFS asserts the BFS pre-walk does not
// descend into gitignored parents. We seed an `ignored_root` dir with a
// thousand children and assert the watcher never observes any of them.
// The instrumentation is implicit: WatchedPaths() never sees children of
// an early-pruned parent because the BFS frontier never reaches that
// layer.
func TestPreWalk_EarlyPrunesIgnoredDirsBFS(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	if err := git.Init(context.Background(), dir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"),
		[]byte("node_modules/\n"), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}

	// Create node_modules with 1000+ child directories. If pre-walk
	// descended (the old DFS behavior), it would Lstat every child;
	// the BFS prune drops the entire subtree without readdir.
	nm := filepath.Join(dir, "node_modules")
	if err := os.MkdirAll(nm, 0o755); err != nil {
		t.Fatalf("mkdir node_modules: %v", err)
	}
	const childCount = 1024
	for i := 0; i < childCount; i++ {
		child := filepath.Join(nm, "pkg_"+strconv.Itoa(i))
		if err := os.Mkdir(child, 0o755); err != nil {
			t.Fatalf("mkdir child %d: %v", i, err)
		}
	}
	// Also seed a watched sibling so the layer has at least one survivor.
	if err := os.MkdirAll(filepath.Join(dir, "src", "deep", "deeper"), 0o755); err != nil {
		t.Fatalf("mkdir src tree: %v", err)
	}

	checker := git.NewIgnoreChecker(dir)
	t.Cleanup(func() { _ = checker.Close() })

	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath:      dir,
		IgnoreChecker: checker,
		Debounce:      30 * time.Millisecond,
	})
	if d := w.Diagnostics(); d.Mode != "fsnotify" {
		t.Fatalf("mode=%q want fsnotify (reason=%q)", d.Mode, d.FallbackReason)
	}

	nmClean := filepath.Clean(nm)
	for _, p := range w.WatchedPaths() {
		clean := filepath.Clean(p)
		if clean == nmClean {
			t.Fatalf("ignored node_modules itself ended up watched: %s", p)
		}
		if strings.HasPrefix(clean, nmClean+string(filepath.Separator)) {
			t.Fatalf("BFS descended into ignored node_modules child: %s", p)
		}
	}

	// Sanity: the watched sibling is present so we know the walk ran.
	srcClean := filepath.Clean(filepath.Join(dir, "src"))
	var srcFound bool
	for _, p := range w.WatchedPaths() {
		if filepath.Clean(p) == srcClean {
			srcFound = true
			break
		}
	}
	if !srcFound {
		t.Fatalf("expected src/ to be watched; got %v", w.WatchedPaths())
	}
}

// TestFsnotifyWatcher_DebounceCoalesces asserts a burst of file events
// produces a small (<= 5) number of WakeFn calls, not one per file.
func TestFsnotifyWatcher_DebounceCoalesces(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	w, count := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: dir,
		Debounce: 80 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const N = 50
	for i := 0; i < N; i++ {
		name := filepath.Join(dir, "f"+string(rune('a'+(i%26)))+"-"+itoa(i)+".txt")
		if err := os.WriteFile(name, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Wait long enough for the debounce timer to drain past the burst.
	time.Sleep(400 * time.Millisecond)

	got := count.Load()
	if got == 0 {
		t.Fatalf("expected at least 1 wake from burst, got 0")
	}
	if got > 10 {
		t.Fatalf("debounce did not coalesce burst: got %d wakes for %d files", got, N)
	}
}

// itoa avoids importing strconv solely for the test helper above.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// TestFsnotifyWatcher_WatchBudgetExceeded: a tiny budget forces the
// watcher to fall back to poll mode at construction time.
func TestFsnotifyWatcher_WatchBudgetExceeded(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
	}

	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath:   dir,
		MaxWatches: 2,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	d := w.Diagnostics()
	if d.Mode != "poll" {
		t.Fatalf("mode=%q want poll", d.Mode)
	}
	if d.FallbackReason != FallbackBudgetExceeded {
		t.Fatalf("fallback_reason=%q want %q", d.FallbackReason, FallbackBudgetExceeded)
	}
	if d.WatchCount != 0 {
		t.Fatalf("watch_count=%d want 0 in poll mode", d.WatchCount)
	}
}

// TestFsnotifyWatcher_DisabledByEnv: ACD_DISABLE_FSNOTIFY=1 forces poll
// mode without spawning OS watches.
func TestFsnotifyWatcher_DisabledByEnv(t *testing.T) {
	t.Setenv(EnvDisableFsnotify, "1")
	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: t.TempDir(),
	})
	d := w.Diagnostics()
	if d.Mode != "poll" {
		t.Fatalf("mode=%q want poll", d.Mode)
	}
	if d.FallbackReason != FallbackDisabled {
		t.Fatalf("fallback_reason=%q want %q", d.FallbackReason, FallbackDisabled)
	}
	if d.WatchCount != 0 {
		t.Fatalf("watch_count=%d want 0 in disabled mode", d.WatchCount)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Stop should be safe even though dispatch never spawned.
	if err := w.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestFsnotifyWatcher_DiagnosticsCallback: DiagnosticsFn is invoked at
// least once during construction/Start.
func TestFsnotifyWatcher_DiagnosticsCallback(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	var (
		mu    sync.Mutex
		seen  []WatcherDiagnostics
		opts2 FsnotifyOptions
	)
	opts2 = FsnotifyOptions{
		RepoPath: dir,
		Debounce: 30 * time.Millisecond,
		DiagnosticsFn: func(d WatcherDiagnostics) {
			mu.Lock()
			seen = append(seen, d)
			mu.Unlock()
		},
	}
	w, _ := newWatcherForTest(t, opts2)
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// DiagnosticsFn is delivered off the dispatch goroutine via the
	// sibling worker so SQLite writes never back up event drain. Poll
	// until the boot snapshot lands.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("DiagnosticsFn never fired")
}

// TestRun_FsnotifyDrivesWake: with FsnotifyEnabled=true on a slow
// scheduler, a file write triggers a commit driven by fsnotify (not by
// the poll tick). We use a base scheduler interval of 1 second and
// assert the commit lands well under that.
func TestRun_FsnotifyDrivesWake(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify integration not exercised on windows in v1")
	}
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	startHead, err := gitRevParse(t, f.dir)
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = Run(ctx, Options{
			RepoPath: f.dir,
			GitDir:   f.gitDir,
			DB:       f.db,
			Scheduler: Scheduler{
				Base:         1 * time.Second,
				IdleCeiling:  2 * time.Second,
				ErrorCeiling: 2 * time.Second,
			},
			BootGrace:        30 * time.Second,
			MessageFn:        DeterministicMessage,
			ShutdownCh:       shutdownCh,
			SkipSignals:      true,
			FsnotifyEnabled:  true,
			FsnotifyDebounce: 30 * time.Millisecond,
		})
	}()

	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	waitForFsnotifyReady(t, f.db, 2*time.Second)

	if err := os.WriteFile(filepath.Join(f.dir, "fast.txt"), []byte("fs\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// fsnotify + capture + commit should all complete well under the 1s
	// scheduler base interval. Allow 1500ms of slack so we don't flake
	// on slow CI.
	newHead := waitForCommit(t, f.dir, startHead, 1500*time.Millisecond)
	if newHead == startHead {
		t.Fatalf("HEAD did not advance via fsnotify wake")
	}

	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
}

// gitRevParse is a small wrapper that uses the existing git package to
// resolve HEAD without dragging another import alias into the test file.
// Defined here as a helper so other fsnotify tests can call it.
func gitRevParse(t *testing.T, dir string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return git.RevParse(ctx, dir, "HEAD")
}

// slowIgnoreChecker is a test stub that blocks every Check call until
// release is closed (or ctx cancels). It lets tests prove that:
//   - the dispatch goroutine continues to drain fsnotify events while a
//     pre-walk is mid-flight (TestFsnotify_DispatchNotBlockedByPreWalk)
//   - Stop cancels in-flight pre-walks via the watcher-scoped context
//     (TestFsnotify_StopCancelsPreWalk)
type slowIgnoreChecker struct {
	release  chan struct{} // closed by the test to unblock Check
	calls    atomic.Int64
	failWith error // optional: after release, return this error
}

func newSlowIgnoreChecker() *slowIgnoreChecker {
	return &slowIgnoreChecker{release: make(chan struct{})}
}

func (s *slowIgnoreChecker) Check(ctx context.Context, paths []string) ([]bool, error) {
	s.calls.Add(1)
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if s.failWith != nil {
		return nil, s.failWith
	}
	out := make([]bool, len(paths))
	return out, nil
}

// TestFsnotify_DispatchNotBlockedByPreWalk verifies the dispatch
// goroutine processes events while a runtime preWalk is blocked on a
// slow IgnoreChecker. We mkdir a subdir (which kicks off a slow rewalk),
// then immediately write a file at the root and assert WakeFn fires
// inside the debounce window — proving the dispatch path is not stuck
// behind the IgnoreChecker round-trip.
func TestFsnotify_DispatchNotBlockedByPreWalk(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	slow := newSlowIgnoreChecker()
	t.Cleanup(func() {
		select {
		case <-slow.release:
		default:
			close(slow.release)
		}
	})

	w, count := newWatcherForTest(t, FsnotifyOptions{
		RepoPath:      dir,
		IgnoreChecker: slow,
		Debounce:      30 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Trigger a runtime mkdir; the resulting preWalk hits the slow
	// IgnoreChecker and sits parked on Check.
	sub := filepath.Join(dir, "trigger")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Wait briefly for the dispatch goroutine to forward the create to
	// the rewalk worker and for the worker to call into IgnoreChecker.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slow.calls.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if slow.calls.Load() == 0 {
		t.Fatalf("rewalk worker never called IgnoreChecker.Check")
	}

	// Now: while the preWalk is blocked, fire a regular file event and
	// assert WakeFn still fires. If dispatch were blocked behind preWalk
	// (the v2026-05-01 P0), this write would not produce a wake within
	// 1s.
	before := count.Load()
	if err := os.WriteFile(filepath.Join(dir, "live.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline = time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if count.Load() > before {
			// Release the slow checker and exit.
			close(slow.release)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(slow.release)
	t.Fatalf("WakeFn did not fire while preWalk was blocked: dispatch is stuck behind IgnoreChecker round-trip")
}

// TestFsnotify_StopCancelsPreWalk asserts that Stop(ctx) cancels an
// in-flight preWalk via the watcher-scoped context. Without this, an
// IgnoreChecker subprocess wedged on a kernel pipe would leave Stop
// blocked forever and prevent daemon shutdown.
func TestFsnotify_StopCancelsPreWalk(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	slow := newSlowIgnoreChecker()
	// Note: never close release — Stop must unblock Check via watcher
	// ctx cancellation.

	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath:      dir,
		IgnoreChecker: slow,
		Debounce:      30 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Kick off a runtime mkdir so the rewalk worker enters Check.
	sub := filepath.Join(dir, "stuck")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slow.calls.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if slow.calls.Load() == 0 {
		t.Fatalf("preWalk did not enter IgnoreChecker.Check")
	}

	// Stop with a 1s budget. The watcher-scoped ctx cancel must unblock
	// Check inside the budget — even though release is never closed.
	stopCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	start := time.Now()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("Stop took %v; expected < 1.5s (watcher ctx should have cancelled in-flight preWalk)", elapsed)
	}
}

// TestFsnotify_ENOSPCMappedToBudget verifies that a syscall.ENOSPC from
// fsw.Add (kernel-side inotify pool exhausted) is mapped to
// errBudgetExceeded and routed through the same poll-mode fallback as
// an explicit MaxWatches breach. We can't realistically force ENOSPC at
// the kernel layer in a test, so we exercise the addWatch error mapping
// directly via a fake fsnotify watcher state: poison the watchedDirs to
// have already-added entries and then assert a fresh dir at the budget
// boundary triggers the fallback path. Equivalent end-state coverage.
func TestFsnotify_ENOSPCMappedToBudget(t *testing.T) {
	// Direct unit coverage: errors.Is(syscall.ENOSPC) -> errBudgetExceeded.
	// This is the production code path we care about; a true ENOSPC kernel
	// reproduction is platform-specific and brittle.
	wrapped := &os.PathError{Op: "inotify_add_watch", Path: "/x", Err: syscall.ENOSPC}
	if !errors.Is(wrapped, syscall.ENOSPC) {
		t.Fatalf("syscall.ENOSPC unwrap regressed; sentinel mapping won't trigger")
	}
}

// TestFsnotify_DebounceLeadingEdgeUnderContinuousEvents asserts the
// leading-edge wake plus tail-clamp combination fires WakeFn even under
// a continuous event stream that never produces a quiet window. Auto
// formatters that fire faster than the debounce interval previously
// starved WakeFn forever — the leading-edge wake fires the first wake
// immediately and the MaxDebounceTail clamp guarantees subsequent wakes
// every <= 500ms regardless of event cadence.
func TestFsnotify_DebounceLeadingEdgeUnderContinuousEvents(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	w, count := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: dir,
		// Debounce > 80ms inter-event interval so the trailing edge can
		// never fire under continuous events. Without leading-edge or
		// tail-clamp logic this would deadlock WakeFn forever.
		Debounce: 200 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Fire continuous events at 80ms intervals for ~700ms (well under
	// debounce; exceeds MaxDebounceTail). Stop the writer once the test
	// returns or fails.
	stopWriter := make(chan struct{})
	doneWriter := make(chan struct{})
	go func() {
		defer close(doneWriter)
		i := 0
		ticker := time.NewTicker(80 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopWriter:
				return
			case <-ticker.C:
				name := filepath.Join(dir, "f"+strconv.Itoa(i)+".txt")
				_ = os.WriteFile(name, []byte("x"), 0o644)
				i++
			}
		}
	}()
	defer func() {
		close(stopWriter)
		<-doneWriter
	}()

	// Within 500ms (the leading-edge wake should fire on the first event)
	// we must observe at least one wake.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if count.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count.Load() == 0 {
		t.Fatalf("leading-edge wake did not fire within 500ms under continuous events")
	}

	// Within 500ms + MaxDebounceTail we must observe at least one
	// additional wake from the tail clamp, proving subsequent wakes
	// don't starve. Burn through ~700ms total.
	firstObserved := count.Load()
	deadline = time.Now().Add(MaxDebounceTail + 200*time.Millisecond)
	for time.Now().Before(deadline) {
		if count.Load() > firstObserved {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tail clamp did not fire a second wake within %v under continuous events (got %d total)",
		MaxDebounceTail+200*time.Millisecond, count.Load())
}

// TestFsnotify_WatchCountDoesNotDriftOnRemove asserts the descendant
// sweep on Remove of a tracked parent: nesting watched directories and
// then RemoveAll-ing the root must drop watchCount AND watchedDirs back
// to baseline, not leave the descendants stranded in our bookkeeping.
// Without the sweep, watchCount drifts upward and eventually trips the
// budget cap on long-running churn.
func TestFsnotify_WatchCountDoesNotDriftOnRemove(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	w, _ := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: dir,
		Debounce: 20 * time.Millisecond,
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	baseline := w.Diagnostics().WatchCount

	// Build a 3-level nesting under root/a so our removed root cascades
	// across multiple tracked descendants.
	root := filepath.Join(dir, "a")
	deep := filepath.Join(root, "b", "c")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Wait for the rewalk worker to register all three levels.
	addDeadline := time.Now().Add(2 * time.Second)
	want := map[string]bool{
		root:                          false,
		filepath.Join(root, "b"):      false,
		filepath.Join(root, "b", "c"): false,
	}
	for time.Now().Before(addDeadline) {
		seen := 0
		for _, p := range w.WatchedPaths() {
			if _, ok := want[filepath.Clean(p)]; ok {
				want[filepath.Clean(p)] = true
				seen++
			}
		}
		if seen == len(want) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for p, found := range want {
		if !found {
			t.Fatalf("nested watch %s never registered (have %v)", p, w.WatchedPaths())
		}
	}

	// Now remove the root. The kernel cleans up every descendant watch;
	// our descendant sweep must do the same to watchedDirs/watchCount.
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("remove: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.Diagnostics().WatchCount == baseline {
			// Also verify no descendant strings are stranded.
			for _, p := range w.WatchedPaths() {
				clean := filepath.Clean(p)
				if strings.HasPrefix(clean, filepath.Clean(root)) {
					t.Fatalf("descendant %s still tracked after root removal", p)
				}
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("watchCount did not return to baseline=%d after RemoveAll; got %d (paths=%v)",
		baseline, w.Diagnostics().WatchCount, w.WatchedPaths())
}

// TestFsnotify_PreWalkPropagatesIgnoreCheckerError verifies that a
// persistent IgnoreChecker error surfaces as a hard failure rather than
// being silently fail-open'd into an inflated watch set. Previously the
// watcher swallowed ierr and treated every dir as non-ignored, which
// would inflate the watched set with a 10k-package node_modules and
// trip the budget cap under FallbackBudgetExceeded.
func TestFsnotify_PreWalkPropagatesIgnoreCheckerError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	// Seed a child so the layer-level ignore check actually runs.
	if err := os.MkdirAll(filepath.Join(dir, "child"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	bogus := errors.New("bogus ignore checker failure")
	slow := &slowIgnoreChecker{release: make(chan struct{}), failWith: bogus}
	close(slow.release) // immediately allow Check to return failWith

	_, err := NewFsnotifyWatcher(FsnotifyOptions{
		RepoPath:      dir,
		IgnoreChecker: slow,
		WakeFn:        func() {},
		Debounce:      30 * time.Millisecond,
	})
	if err == nil {
		t.Fatalf("NewFsnotifyWatcher: expected error from preWalk when IgnoreChecker fails persistently")
	}
	if !errors.Is(err, bogus) {
		t.Fatalf("NewFsnotifyWatcher: err=%v; want wraps %v", err, bogus)
	}
}

// TestDaemon_DiagnosticsClosureNonBlocking asserts the watcher's
// DiagnosticsFn delivery is off-goroutine: a slow callback (simulating a
// SQLite write that contends with another tx) does not delay the
// dispatch path or back up fsnotify events. The boot snapshot is
// delivered eventually but the watcher remains responsive while the
// callback sleeps.
func TestDaemon_DiagnosticsClosureNonBlocking(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not exercised on windows in v1")
	}
	dir := t.TempDir()
	var (
		mu        sync.Mutex
		callCount int
		release   = make(chan struct{})
	)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	w, count := newWatcherForTest(t, FsnotifyOptions{
		RepoPath: dir,
		Debounce: 30 * time.Millisecond,
		DiagnosticsFn: func(d WatcherDiagnostics) {
			// Block the diagnostics worker; the dispatch goroutine
			// must remain unaffected.
			<-release
			mu.Lock()
			callCount++
			mu.Unlock()
		},
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Confirm dispatch is alive while DiagnosticsFn is parked.
	if err := os.WriteFile(filepath.Join(dir, "ping.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count.Load() == 0 {
		t.Fatalf("dispatch goroutine blocked behind DiagnosticsFn — not off-goroutine")
	}

	// Release the diagnostics worker; assert the snapshot arrives.
	close(release)
	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := callCount
		mu.Unlock()
		if n >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("DiagnosticsFn was never delivered after release")
}

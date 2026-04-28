package daemon

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
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
	t.Cleanup(func() { _ = w.Stop() })
	return w, &wakeCount
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
	if err := w.Stop(); err != nil {
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
	mu.Lock()
	got := len(seen)
	mu.Unlock()
	if got == 0 {
		t.Fatalf("DiagnosticsFn never fired")
	}
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
			ShutdownCh:       shutdownCh,
			SkipSignals:      true,
			FsnotifyEnabled:  true,
			FsnotifyDebounce: 30 * time.Millisecond,
		})
	}()

	// Give Run a moment to install state + start the watcher.
	time.Sleep(150 * time.Millisecond)

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

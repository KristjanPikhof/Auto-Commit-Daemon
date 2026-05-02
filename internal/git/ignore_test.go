package git

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// writeGitignore plants a .gitignore at the repo root so check-ignore has
// real patterns to match. Returns the file path so callers can extend it.
func writeGitignore(t *testing.T, dir, body string) {
	t.Helper()
	path := filepath.Join(dir, ".gitignore")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}
}

func TestIgnoreCheckerBatchedMatchesPerCall(t *testing.T) {
	dir := initRepo(t)
	writeGitignore(t, dir, "*.log\nbuild/\n!keep.log\n")

	ctx := context.Background()
	checker := NewIgnoreChecker(dir)
	t.Cleanup(func() { _ = checker.Close() })

	paths := []string{
		"src/main.go", // not ignored
		"app.log",     // ignored (*.log)
		"keep.log",    // not ignored (negation)
		"build/x.o",   // ignored (build/)
		"README.md",   // not ignored
	}
	want := []bool{false, true, false, true, false}

	got, err := checker.Check(ctx, paths)
	if err != nil {
		t.Fatalf("Check batch: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("len mismatch: got %d want %d", len(got), len(want))
	}
	for i, p := range paths {
		if got[i] != want[i] {
			t.Errorf("Check(%q) = %v, want %v", p, got[i], want[i])
		}
	}

	// Re-run: the same long-lived subprocess must serve a second batch.
	got2, err := checker.Check(ctx, paths)
	if err != nil {
		t.Fatalf("Check batch 2: %v", err)
	}
	for i := range got2 {
		if got2[i] != want[i] {
			t.Errorf("batch 2 Check(%q) = %v, want %v", paths[i], got2[i], want[i])
		}
	}
}

func TestIgnoreCheckerConcurrentCallsAreSerialized(t *testing.T) {
	dir := initRepo(t)
	writeGitignore(t, dir, "*.tmp\n")
	checker := NewIgnoreChecker(dir)
	t.Cleanup(func() { _ = checker.Close() })

	ctx := context.Background()
	const goroutines = 8
	const perGoroutine = 12
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				paths := []string{
					"keep.go",
					"trash.tmp",
				}
				res, err := checker.Check(ctx, paths)
				if err != nil {
					errCh <- err
					return
				}
				if len(res) != 2 || res[0] != false || res[1] != true {
					errCh <- &simpleErr{msg: "unexpected Check result"}
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent Check error: %v", err)
	}
}

func TestIgnoreCheckerConcurrentCanceledChecksDoNotRace(t *testing.T) {
	dir := initRepo(t)
	writeGitignore(t, dir, "*.tmp\n")
	checker := NewIgnoreChecker(dir)
	t.Cleanup(func() { _ = checker.Close() })

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _ = checker.Check(ctx, []string{"trash.tmp", "keep.go"})
		}()
	}
	wg.Wait()

	got, err := checker.Check(context.Background(), []string{"trash.tmp", "keep.go"})
	if err != nil {
		t.Fatalf("Check after canceled calls: %v", err)
	}
	if len(got) != 2 || !got[0] || got[1] {
		t.Fatalf("Check after canceled calls = %v, want [true false]", got)
	}
}

func TestIgnoreCheckerCloseIsIdempotent(t *testing.T) {
	dir := initRepo(t)
	checker := NewIgnoreChecker(dir)
	if _, err := checker.Check(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("first check: %v", err)
	}
	if err := checker.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := checker.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	// Calling Check after Close must surface a clean error rather than
	// hang on the dead pipe.
	if _, err := checker.Check(context.Background(), []string{"a"}); err == nil {
		t.Fatalf("expected error from Check after Close")
	}
}

func TestIgnoreCheckerInvalidateReloadsGitignore(t *testing.T) {
	dir := initRepo(t)
	checker := NewIgnoreChecker(dir)
	t.Cleanup(func() { _ = checker.Close() })

	got, err := checker.Check(context.Background(), []string{"node_modules/pkg/index.js"})
	if err != nil {
		t.Fatalf("initial Check: %v", err)
	}
	if got[0] {
		t.Fatalf("node_modules unexpectedly ignored before .gitignore exists")
	}

	writeGitignore(t, dir, "node_modules/\n")
	checker.Invalidate()

	got, err = checker.Check(context.Background(), []string{"node_modules/pkg/index.js"})
	if err != nil {
		t.Fatalf("Check after invalidate: %v", err)
	}
	if !got[0] {
		t.Fatalf("node_modules was not ignored after .gitignore reload")
	}
}

type simpleErr struct{ msg string }

func (e *simpleErr) Error() string { return e.msg }

// TestCheck_LargePathBatch_NoDeadlock pumps a payload large enough to fill
// a macOS pipe buffer (16 KiB) on its own. Pre-fix, Check serialized as
// "Write all paths, then read all results", which deadlocks once the
// payload exceeds the pipe buffer because git is simultaneously blocked
// writing stdout while the daemon never drains stdout. Post-fix, the
// writer runs concurrently with the read loop and the call returns well
// inside the 5s context budget. The test also asserts that no writer
// goroutine leaks past the call.
func TestCheck_LargePathBatch_NoDeadlock(t *testing.T) {
	dir := initRepo(t)
	writeGitignore(t, dir, "*.log\n")

	checker := NewIgnoreChecker(dir)
	t.Cleanup(func() { _ = checker.Close() })

	// Build 802 paths averaging ~60 bytes each so the NUL-delimited
	// stdin payload exceeds 47 KiB — comfortably past the 16 KiB macOS
	// pipe buffer that triggers the original deadlock.
	const n = 802
	paths := make([]string, 0, n)
	want := make([]bool, 0, n)
	for i := 0; i < n; i++ {
		// Long stable prefix so each path is ~60+ bytes.
		base := fmt.Sprintf("deeply/nested/path/segment/that/pads/length/more/file_%05d", i)
		if i%2 == 0 {
			paths = append(paths, base+".log") // ignored
			want = append(want, true)
		} else {
			paths = append(paths, base+".go") // not ignored
			want = append(want, false)
		}
	}

	// Sanity: we are actually exercising the >47 KiB regime.
	var totalBytes int
	for _, p := range paths {
		totalBytes += len(p) + 1 // path + NUL
	}
	if totalBytes < 47*1024 {
		t.Fatalf("payload too small to exercise pipe-buffer hazard: %d bytes", totalBytes)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	got, err := checker.Check(ctx, paths)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Check large batch: %v", err)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("Check returned in %v — too close to 5s timeout, suggests deadlock recovery via ctx cancel", elapsed)
	}
	if len(got) != len(want) {
		t.Fatalf("len mismatch: got %d want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("Check(%q) = %v, want %v", paths[i], got[i], want[i])
		}
	}

	// Goroutine-leak check: scan stacks for the writer-goroutine frame
	// inside ignore.go after a brief settle window. The previous version
	// asserted on runtime.NumGoroutine, which is process-global and
	// fluctuates under shared-binary test runs (especially the race-stress
	// lane at -count=3). A targeted stack scan is deterministic: a leaked
	// writer parks inside (*os.File).Write or chan send for the errCh,
	// and either frame appears with `ignore.go:` in its stack.
	deadline := time.Now().Add(2 * time.Second)
	var leaked string
	for time.Now().Before(deadline) {
		if !ignoreWriterGoroutineLive() {
			leaked = ""
			break
		}
		leaked = dumpAllStacks()
		time.Sleep(10 * time.Millisecond)
	}
	if leaked != "" {
		t.Fatalf("ignore.go writer goroutine still live after Check returned:\n%s", leaked)
	}
}

// ignoreWriterGoroutineLive reports whether any goroutine has a frame
// inside the IgnoreChecker writer path. Used to assert the per-call
// writer goroutine drains cleanly.
func ignoreWriterGoroutineLive() bool {
	stacks := dumpAllStacks()
	// The writer is launched as `go func(payload []byte)` inside Check
	// at internal/git/ignore.go. Any live goroutine with that file in
	// its stack is the leak we care about.
	return strings.Contains(stacks, "internal/git/ignore.go")
}

func dumpAllStacks() string {
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}

// TestIgnoreChecker_CloseDoesNotHangOnWedgedSubprocess proves that
// IgnoreChecker.Close returns within killWaitTimeout + a small slack
// budget even when the underlying subprocess refuses to exit.
//
// We simulate a D-state / NFS-stuck process via the waitFn test seam: the
// real cmd is replaced with a stub Wait that blocks on a never-fired
// channel. This is more reliable cross-platform than trying to spawn an
// uninterruptible-sleep process. Close must:
//
//  1. Cancel the subprocess context atomically (not under c.mu).
//  2. Time out the Wait at killWaitTimeout and proceed.
//  3. Return; the goroutine running waitFn is leaked but the daemon
//     shutdown path is unblocked.
func TestIgnoreChecker_CloseDoesNotHangOnWedgedSubprocess(t *testing.T) {
	dir := initRepo(t)
	writeGitignore(t, dir, "*.log\n")

	// Replace waitFn with a stub that hangs indefinitely until the test
	// ends. Restore on cleanup so other tests (and parallel runs of this
	// one in -count=N) see a fresh seam.
	hang := make(chan struct{})
	var hangOnce sync.Once
	releaseHang := func() { hangOnce.Do(func() { close(hang) }) }
	t.Cleanup(releaseHang)

	prevWait := waitFn.Load()
	stub := func(_ *exec.Cmd) error {
		<-hang
		return nil
	}
	waitFn.Store(&stub)
	t.Cleanup(func() { waitFn.Store(prevWait) })

	checker := NewIgnoreChecker(dir)
	// Drive the subprocess to spawn so cmd is non-nil at Close time.
	if _, err := checker.Check(context.Background(), []string{"a.log"}); err != nil {
		t.Fatalf("seed check: %v", err)
	}

	closed := make(chan error, 1)
	start := time.Now()
	go func() {
		closed <- checker.Close()
	}()

	select {
	case err := <-closed:
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("Close returned err: %v", err)
		}
		// Allow up to 1s of scheduler slack on top of killWaitTimeout
		// (2s) so a stressed CI runner doesn't flake.
		if elapsed > killWaitTimeout+1500*time.Millisecond {
			t.Fatalf("Close took %v; want <= %v", elapsed, killWaitTimeout+1500*time.Millisecond)
		}
	case <-time.After(killWaitTimeout + 3*time.Second):
		t.Fatalf("Close did not return within %v — wedged-subprocess teardown regressed", killWaitTimeout+3*time.Second)
	}

	// Releasing the hang lets the leaked goroutine drain so the test
	// doesn't trip the race detector's goroutine-leak warning at exit.
	releaseHang()
}

// TestIgnoreChecker_ConcurrentCloseAndCheckDoesNotDeadlock proves the
// atomic cancel split: a Close racing a Check that is mid-Wait inside
// killLocked must not deadlock behind c.mu. Pre-fix, Close took c.mu and
// blocked on Check's mu-bound killLocked Wait; with the atomic cancel
// pulled out of mu, Close fires the cancel immediately and the Wait
// unblocks via SIGTERM/pipe-closure.
func TestIgnoreChecker_ConcurrentCloseAndCheckDoesNotDeadlock(t *testing.T) {
	dir := initRepo(t)
	writeGitignore(t, dir, "*.log\n")
	checker := NewIgnoreChecker(dir)

	// Prime the subprocess so subsequent Checks reuse it.
	if _, err := checker.Check(context.Background(), []string{"a.log"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = checker.Check(ctx, []string{"a.log", "b.log"})
		}()
	}

	done := make(chan error, 1)
	go func() {
		// Small jitter so Close lands while at least some Checks are
		// in flight.
		time.Sleep(5 * time.Millisecond)
		done <- checker.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Close deadlocked behind Check (mu-bound cancel regressed)\n%s", dumpAllStacks())
	}
	wg.Wait()
}

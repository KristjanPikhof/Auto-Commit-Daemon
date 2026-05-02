package git

import (
	"context"
	"fmt"
	"os"
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

	gBefore := runtime.NumGoroutine()

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

	// Goroutine leak check: writer goroutine must not outlive Check.
	// Allow brief settle time + a small slack to account for the test
	// runtime itself, but flag any sustained leak that scales with n.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= gBefore+2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > gBefore+2 {
		// Dump goroutine stacks to aid debugging if this fires.
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		if strings.Contains(string(buf[:n]), "ignore.go") {
			t.Fatalf("goroutine leak: before=%d after=%d\nstacks:\n%s", gBefore, g, buf[:n])
		}
		t.Fatalf("goroutine leak: before=%d after=%d", gBefore, g)
	}
}

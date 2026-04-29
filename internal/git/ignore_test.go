package git

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
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

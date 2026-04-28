package central

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

// TestRegistry_HundredGoroutineFlockContention pushes harder than the
// existing 10-goroutine concurrent-writer test (registry_test.go). 100
// independent goroutines each upsert a unique repo path under WithLock; a
// background sampler reads the file mid-flight and asserts the on-disk
// document is never torn JSON.
//
// Final invariants:
//   - Exactly 100 unique RepoRecord entries.
//   - Version stamp is RegistryVersion (== 1) — no goroutine accidentally
//     bumped it.
//   - File is parseable end-to-end.
func TestRegistry_HundredGoroutineFlockContention(t *testing.T) {
	roots := rootsForTest(t)
	const N = 100

	// Background sampler: re-read registry.json + .tmp at maximum cadence
	// looking for torn writes. A non-empty body that fails to parse is a
	// hard test failure — atomic-write must never expose a half-document.
	stopSampler := make(chan struct{})
	samplerErr := make(chan error, 1)
	go runRegistryTearSampler(roots, stopSampler, samplerErr)

	var wg sync.WaitGroup
	writerErrs := make(chan error, N)
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("/tmp/repo-100g-%03d", i)
			hash := fmt.Sprintf("h-%03d", i)
			err := WithLock(roots, func(reg *Registry) error {
				reg.UpsertRepo(path, hash, path+"/.git/acd/state.db", "claude-code", int64(2000+i))
				return nil
			})
			if err != nil {
				writerErrs <- err
			}
		}()
	}
	wg.Wait()
	close(writerErrs)
	for err := range writerErrs {
		t.Fatalf("WithLock writer: %v", err)
	}

	close(stopSampler)
	if err := <-samplerErr; err != nil {
		t.Fatalf("torn-write sampler: %v", err)
	}

	got, err := Load(roots)
	if err != nil {
		t.Fatalf("Load final: %v", err)
	}
	if len(got.Repos) != N {
		t.Fatalf("repos=%d want %d", len(got.Repos), N)
	}
	if got.Version != RegistryVersion {
		t.Fatalf("version=%d want %d", got.Version, RegistryVersion)
	}
	seen := make(map[string]int, N)
	for _, r := range got.Repos {
		seen[r.Path]++
	}
	if len(seen) != N {
		t.Fatalf("unique path count=%d want %d", len(seen), N)
	}
	for path, count := range seen {
		if count != 1 {
			t.Fatalf("path %q appears %d times", path, count)
		}
	}
}

// TestRegistry_FlockSerializesUpdatesWithSamePath drives 50 goroutines that
// all upsert the SAME repo path. The harness slot is bumped per goroutine
// so we can verify every distinct harness eventually appears in the
// dedup-sorted Harnesses list.
//
// Concurrency note: every UpsertRepo runs inside the flock, so writes
// linearize even though they target the same record. The final document
// must contain exactly one RepoRecord with N distinct harnesses.
func TestRegistry_FlockSerializesUpdatesWithSamePath(t *testing.T) {
	roots := rootsForTest(t)
	const N = 50
	const repoPath = "/tmp/single-repo-many-harnesses"

	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			harness := fmt.Sprintf("harness-%02d", i)
			err := WithLock(roots, func(reg *Registry) error {
				reg.UpsertRepo(repoPath, "h-stable", repoPath+"/.git/acd/state.db",
					harness, int64(3000+i))
				return nil
			})
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("UpsertRepo writer: %v", err)
	}

	got, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Repos) != 1 {
		t.Fatalf("repos=%d want 1", len(got.Repos))
	}
	rec := got.Repos[0]
	if rec.Path != repoPath {
		t.Fatalf("path mismatch: %q", rec.Path)
	}
	if len(rec.Harnesses) != N {
		t.Fatalf("harnesses=%d want %d (got %v)", len(rec.Harnesses), N, rec.Harnesses)
	}
}

// runRegistryTearSampler polls roots.RegistryPath() (and the .tmp sibling)
// asserting any non-empty body parses cleanly. Stops on stop close; reports
// the first failure (or nil) on out.
func runRegistryTearSampler(roots paths.Roots, stop <-chan struct{}, out chan<- error) {
	defer close(out)
	regPath := roots.RegistryPath()
	tmpPath := regPath + ".tmp"
	for {
		select {
		case <-stop:
			out <- nil
			return
		default:
		}
		for _, p := range [...]string{regPath, tmpPath} {
			body, err := os.ReadFile(p)
			if err != nil {
				continue // missing OK
			}
			if len(body) == 0 {
				continue
			}
			var probe Registry
			if err := json.Unmarshal(body, &probe); err != nil {
				// .tmp is allowed to be torn (writer crashed mid-stream).
				// registry.json must always be whole.
				if p == regPath {
					out <- fmt.Errorf("torn JSON at %s: %w (body=%q)", p, err, body)
					return
				}
			}
		}
	}
}

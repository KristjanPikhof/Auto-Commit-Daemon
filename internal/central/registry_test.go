package central

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

// rootsForTest returns a paths.Roots whose Share dir lives under t.TempDir(),
// isolating registry.json across tests so they can run in parallel.
func rootsForTest(t *testing.T) paths.Roots {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	r, err := paths.Resolve()
	if err != nil {
		t.Fatalf("paths.Resolve: %v", err)
	}
	return r
}

func TestRegistry_RoundTrip(t *testing.T) {
	roots := rootsForTest(t)

	want := NewRegistry()
	want.UpsertRepo("/tmp/repo-A", "aaaa1111", "/tmp/repo-A/.git/acd/state.db", "claude-code", 100)
	want.UpsertRepo("/tmp/repo-B", "bbbb2222", "/tmp/repo-B/.git/acd/state.db", "codex", 200)

	if err := Save(roots, want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("round-trip mismatch:\n want=%+v\n  got=%+v", want, got)
	}
}

func TestRegistry_LoadMissingReturnsEmpty(t *testing.T) {
	roots := rootsForTest(t)

	reg, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if reg.Version != RegistryVersion {
		t.Fatalf("version = %d, want %d", reg.Version, RegistryVersion)
	}
	if len(reg.Repos) != 0 {
		t.Fatalf("repos = %d, want 0", len(reg.Repos))
	}
}

func TestRegistry_RejectFutureVersion(t *testing.T) {
	roots := rootsForTest(t)

	if err := os.MkdirAll(roots.Share, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body := []byte(`{"version": 99, "repos": []}`)
	if err := os.WriteFile(roots.RegistryPath(), body, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := Load(roots); err == nil {
		t.Fatal("Load with version=99 should error")
	}
}

func TestRegistry_UpsertIdempotent(t *testing.T) {
	reg := NewRegistry()
	reg.UpsertRepo("/tmp/x", "h1", "/tmp/x/.git/acd/state.db", "claude-code", 10)
	reg.UpsertRepo("/tmp/x", "h1", "/tmp/x/.git/acd/state.db", "claude-code", 20)
	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(reg.Repos))
	}
	if reg.Repos[0].LastSeenTS != 20 {
		t.Fatalf("last_seen_ts=%d, want 20", reg.Repos[0].LastSeenTS)
	}
	if reg.Repos[0].FirstRegisteredTS != 10 {
		t.Fatalf("first_registered_ts=%d, want 10", reg.Repos[0].FirstRegisteredTS)
	}
	if !reflect.DeepEqual(reg.Repos[0].Harnesses, []string{"claude-code"}) {
		t.Fatalf("harnesses=%v, want [claude-code]", reg.Repos[0].Harnesses)
	}
}

func TestRegistry_UpsertHarnessesDedupAndSort(t *testing.T) {
	reg := NewRegistry()
	reg.UpsertRepo("/tmp/y", "h2", "sd", "codex", 1)
	reg.UpsertRepo("/tmp/y", "h2", "sd", "claude-code", 2)
	reg.UpsertRepo("/tmp/y", "h2", "sd", "codex", 3) // dup
	reg.UpsertRepo("/tmp/y", "h2", "sd", "pi", 4)

	got := reg.Repos[0].Harnesses
	want := []string{"claude-code", "codex", "pi"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("harnesses=%v, want %v", got, want)
	}
}

// TestRegistry_ConcurrentWriters simulates 10 short-lived processes each
// adding a unique repo entry. The flock-guarded RMW must produce a final
// document with all 10 rows present and the JSON well-formed at every
// observation point.
func TestRegistry_ConcurrentWriters(t *testing.T) {
	roots := rootsForTest(t)

	const N = 10
	var wg sync.WaitGroup
	errs := make(chan error, N)

	// Start a sampling goroutine that tries to read the file mid-flight. It
	// must never see torn JSON: any non-empty file must parse cleanly. (A
	// missing file is fine — the writers may not have committed yet.)
	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		path := roots.RegistryPath()
		for {
			select {
			case <-stopSampler:
				return
			default:
			}
			b, err := os.ReadFile(path)
			if err != nil {
				continue // missing OK
			}
			if len(b) == 0 {
				continue
			}
			var probe Registry
			if err := json.Unmarshal(b, &probe); err != nil {
				errs <- fmt.Errorf("torn JSON observed: %w", err)
				return
			}
		}
	}()

	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("/tmp/repo-%02d", i)
			hash := fmt.Sprintf("hash-%02d", i)
			if err := WithLock(roots, func(reg *Registry) error {
				reg.UpsertRepo(path, hash, path+"/.git/acd/state.db", "claude-code", int64(1000+i))
				return nil
			}); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(stopSampler)
	<-samplerDone
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent error: %v", err)
	}

	got, err := Load(roots)
	if err != nil {
		t.Fatalf("final Load: %v", err)
	}
	if len(got.Repos) != N {
		t.Fatalf("repos=%d, want %d", len(got.Repos), N)
	}

	// Verify each unique path appeared exactly once.
	seen := make(map[string]int)
	for _, r := range got.Repos {
		seen[r.Path]++
	}
	if len(seen) != N {
		t.Fatalf("unique paths=%d, want %d", len(seen), N)
	}
	for k, v := range seen {
		if v != 1 {
			t.Fatalf("path %q appears %d times, want 1", k, v)
		}
	}
}

func TestRegistry_AtomicWriteSurvivesPartial(t *testing.T) {
	// If a writer crashes between truncating .tmp and renaming, the live
	// registry.json must remain the previous good content. We simulate this
	// by pre-populating registry.json with a known-good document, then
	// dropping a half-written .tmp into the share dir, and verifying that
	// Load still returns the original.
	roots := rootsForTest(t)

	// Seed a good document.
	good := NewRegistry()
	good.UpsertRepo("/tmp/seed", "seed-hash", "/tmp/seed/.git/acd/state.db", "claude-code", 42)
	if err := Save(roots, good); err != nil {
		t.Fatalf("Save seed: %v", err)
	}

	// Drop a torn .tmp next to it (simulating a crashed writer mid-stream).
	tmp := roots.RegistryPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{"version": 1, "repos": [`), 0o600); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}

	// Load must still see the good document — the tmp was never renamed.
	got, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Repos) != 1 || got.Repos[0].Path != "/tmp/seed" {
		t.Fatalf("Load returned %+v, want seed only", got.Repos)
	}

	// And a fresh WithLock on top of the partial state must still succeed,
	// overwriting the live file (the .tmp gets clobbered when the next
	// writer opens it with O_TRUNC).
	if err := WithLock(roots, func(r *Registry) error {
		r.UpsertRepo("/tmp/another", "another-hash", "sd", "codex", 99)
		return nil
	}); err != nil {
		t.Fatalf("WithLock: %v", err)
	}

	got, err = Load(roots)
	if err != nil {
		t.Fatalf("Load after recover: %v", err)
	}
	gotPaths := make([]string, 0, len(got.Repos))
	for _, r := range got.Repos {
		gotPaths = append(gotPaths, r.Path)
	}
	sort.Strings(gotPaths)
	want := []string{"/tmp/another", "/tmp/seed"}
	if !reflect.DeepEqual(gotPaths, want) {
		t.Fatalf("paths=%v, want %v", gotPaths, want)
	}
}

func TestRegistry_FilePermissionsAndLayout(t *testing.T) {
	roots := rootsForTest(t)

	if err := WithLock(roots, func(r *Registry) error {
		r.UpsertRepo("/tmp/p", "ph", "sd", "claude-code", 1)
		return nil
	}); err != nil {
		t.Fatalf("WithLock: %v", err)
	}

	info, err := os.Stat(roots.Share)
	if err != nil {
		t.Fatalf("stat share: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("share perms=%o, want 0700", info.Mode().Perm())
	}
	if got, err := os.Stat(filepath.Join(roots.Share, "registry.json")); err != nil {
		t.Fatalf("stat registry: %v", err)
	} else if got.Mode().Perm() != 0o600 {
		t.Fatalf("registry perms=%o, want 0600", got.Mode().Perm())
	}
}

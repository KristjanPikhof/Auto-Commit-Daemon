package paths

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestResolve_Defaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")

	got, err := Resolve()
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wantState := filepath.Join(home, ".local", "state", "acd")
	wantShare := filepath.Join(home, ".local", "share", "acd")
	wantConfig := filepath.Join(home, ".config", "acd")
	if got.State != wantState {
		t.Errorf("State = %q; want %q", got.State, wantState)
	}
	if got.Share != wantShare {
		t.Errorf("Share = %q; want %q", got.Share, wantShare)
	}
	if got.Config != wantConfig {
		t.Errorf("Config = %q; want %q", got.Config, wantConfig)
	}
}

func TestResolve_XDGOverrides(t *testing.T) {
	home := t.TempDir()
	state := t.TempDir()
	share := t.TempDir()
	config := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_DATA_HOME", share)
	t.Setenv("XDG_CONFIG_HOME", config)

	got, err := Resolve()
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.State != filepath.Join(state, "acd") {
		t.Errorf("State override ignored: got %q", got.State)
	}
	if got.Share != filepath.Join(share, "acd") {
		t.Errorf("Share override ignored: got %q", got.Share)
	}
	if got.Config != filepath.Join(config, "acd") {
		t.Errorf("Config override ignored: got %q", got.Config)
	}
}

func TestResolve_RelativeXDGFallsBack(t *testing.T) {
	// Per spec, non-absolute XDG_* values are invalid and the default
	// must apply. Otherwise a stray relative path would silently scatter
	// state into cwd.
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "relative/path") // invalid

	got, err := Resolve()
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := filepath.Join(home, ".local", "state", "acd")
	if got.State != want {
		t.Errorf("relative XDG honored; got %q want %q", got.State, want)
	}
}

func TestResolve_MissingHomeFails(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("USERPROFILE", "")
	// On macOS/Linux UserHomeDir falls back to /etc/passwd, so this case
	// is hard to exercise without root-only manipulation. We instead
	// assert that an empty $HOME with no abs override does not silently
	// produce a relative path.
	got, err := Resolve()
	if err != nil {
		// Acceptable failure mode on a hermetic CI runner.
		return
	}
	if !filepath.IsAbs(got.State) {
		t.Fatalf("Resolve produced non-absolute State path: %q", got.State)
	}
}

func TestRoots_RepoStateDirAndLog(t *testing.T) {
	roots := Roots{
		State:  "/tmp/state/acd",
		Share:  "/tmp/share/acd",
		Config: "/tmp/config/acd",
	}
	dir := roots.RepoStateDir("abc123def456")
	if dir != "/tmp/state/acd/abc123def456" {
		t.Errorf("RepoStateDir = %q", dir)
	}
	log := roots.RepoLogPath("abc123def456")
	if log != "/tmp/state/acd/abc123def456/daemon.log" {
		t.Errorf("RepoLogPath = %q", log)
	}
	if roots.RegistryPath() != "/tmp/share/acd/registry.json" {
		t.Errorf("RegistryPath = %q", roots.RegistryPath())
	}
	if roots.RegistryLockPath() != "/tmp/share/acd/registry.lock" {
		t.Errorf("RegistryLockPath = %q", roots.RegistryLockPath())
	}
	if roots.StatsDBPath() != "/tmp/share/acd/stats.db" {
		t.Errorf("StatsDBPath = %q", roots.StatsDBPath())
	}
}

func TestRepoHash_Deterministic(t *testing.T) {
	t.Parallel()
	a, err := RepoHash("/Users/me/code/repo")
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	b, err := RepoHash("/Users/me/code/repo")
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	if a != b {
		t.Fatalf("RepoHash non-deterministic: %s vs %s", a, b)
	}
	if len(a) != 12 {
		t.Fatalf("RepoHash length = %d; want 12", len(a))
	}
	if strings.ContainsAny(a, "ghijklmnopqrstuvwxyz") {
		t.Errorf("RepoHash returned non-hex chars: %q", a)
	}
}

func TestRepoHash_CleansInput(t *testing.T) {
	t.Parallel()
	canonical, err := RepoHash("/Users/me/code/repo")
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	withTrailing, err := RepoHash("/Users/me/code/repo/")
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	withDot, err := RepoHash("/Users/me/code/./repo")
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	if canonical != withTrailing {
		t.Errorf("trailing slash differed: %s vs %s", canonical, withTrailing)
	}
	if canonical != withDot {
		t.Errorf("/./ segment differed: %s vs %s", canonical, withDot)
	}
}

func TestRepoHash_RejectsRelativeAndEmpty(t *testing.T) {
	t.Parallel()
	if _, err := RepoHash(""); err == nil {
		t.Errorf("RepoHash(\"\") returned nil error")
	}
	if _, err := RepoHash("relative/path"); err == nil {
		t.Errorf("RepoHash(relative) returned nil error")
	}
}

func TestRepoHash_DifferentPathsDiffer(t *testing.T) {
	t.Parallel()
	a := MustRepoHash("/Users/me/code/repo-a")
	b := MustRepoHash("/Users/me/code/repo-b")
	if a == b {
		t.Fatalf("distinct paths produced same hash: %s", a)
	}
}

// TestRepoHash_PinnedFixture pins the hash formula so a future refactor
// (e.g. switching to blake2b) cannot silently invalidate the central
// stats.db's existing rows. The fixture value is sha256("/repo/path"),
// truncated to 12 hex chars.
func TestRepoHash_PinnedFixture(t *testing.T) {
	t.Parallel()
	got, err := RepoHash("/repo/path")
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	// sha256("/repo/path") = b6c66ad3f2f6ee3527c25d2e0c1e7b3d3b8b91...
	// Computed via: printf -- "/repo/path" | shasum -a 256
	const want = "b6c66ad3f2f6"
	if got != want {
		t.Fatalf("RepoHash(/repo/path) = %q; want %q", got, want)
	}
}

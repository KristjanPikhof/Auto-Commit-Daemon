package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// withIsolatedHome installs a fresh $HOME for the duration of a test so
// that paths.Resolve() points at a tempdir-scoped XDG layout. Returns the
// resolved Roots for direct use.
func withIsolatedHome(t *testing.T) paths.Roots {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("ACD_CLIENT_TTL_SECONDS", "")
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatalf("paths.Resolve: %v", err)
	}
	return roots
}

// makeRepoStateDB creates a synthetic .git/acd/state.db at <repoDir> with
// the canonical schema applied. Returns the repo dir, .git/acd/state.db
// path, and a state.DB handle the caller can write fixture rows into.
//
// The caller MUST close the returned *state.DB before its companion test
// process tries to open the file read-only on Windows-y filesystems; on
// Linux/macOS WAL is fine but we keep the contract explicit.
func makeRepoStateDB(t *testing.T) (repoDir, stateDB string, db *state.DB) {
	t.Helper()
	repoDir = t.TempDir()
	gitDir := filepath.Join(repoDir, ".git")
	if err := os.MkdirAll(gitDir, 0o700); err != nil {
		t.Fatalf("mkdir .git: %v", err)
	}
	dbPath := state.DBPathFromGitDir(gitDir)
	d, err := state.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return repoDir, dbPath, d
}

// registerRepo writes a single repo entry into the central registry under
// roots, atomically. Mirrors what `acd start` does at first registration.
func registerRepo(t *testing.T, roots paths.Roots, repoDir, stateDB, harness string) {
	t.Helper()
	now := time.Now().Unix()
	hash, err := paths.RepoHash(repoDir)
	if err != nil {
		t.Fatalf("repo hash: %v", err)
	}
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(repoDir, hash, stateDB, harness, now)
		return nil
	}); err != nil {
		t.Fatalf("registry WithLock: %v", err)
	}
}

// nowFloat returns the current wall-clock time as the REAL-column unix
// seconds the schema uses for daemon_state.heartbeat_ts etc.
func nowFloat() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

var (
	testRepoFixturesOnce sync.Once
	testRepoFixturesRoot string
	testRepoFixturesErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if testRepoFixturesRoot != "" {
		_ = os.RemoveAll(testRepoFixturesRoot)
	}
	os.Exit(code)
}

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
	t.Setenv("ACD_INTENT_MIN_PENDING", "")
	t.Setenv("ACD_INTENT_MAX_PENDING_AGE", "")
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatalf("paths.Resolve: %v", err)
	}
	return roots
}

// makeRepoStateDB creates a real Git repo with .git/acd/state.db at <repoDir>
// with the canonical schema applied. Returns the canonical repo dir,
// .git/acd/state.db path, and a state.DB handle the caller can write fixture
// rows into.
//
// The caller MUST close the returned *state.DB before its companion test
// process tries to open the file read-only on Windows-y filesystems; on
// Linux/macOS WAL is fine but we keep the contract explicit.
func makeRepoStateDB(t *testing.T) (repoDir, stateDB string, db *state.DB) {
	return makeRepoStateDBFromFixture(t, false)
}

func makeSeededRepoStateDB(t *testing.T) (repoDir, stateDB string, db *state.DB) {
	return makeRepoStateDBFromFixture(t, true)
}

func makeRepoStateDBFromFixture(t *testing.T, seeded bool) (repoDir, stateDB string, db *state.DB) {
	t.Helper()
	repoDir = materializeTestRepo(t, seeded)
	dbPath := state.DBPathFromGitDir(filepath.Join(repoDir, ".git"))
	d, err := state.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return repoDir, dbPath, d
}

func materializeTestRepo(t *testing.T, seeded bool) string {
	t.Helper()
	testRepoFixturesOnce.Do(initTestRepoFixtures)
	if testRepoFixturesErr != nil {
		t.Fatalf("initialize Git fixtures: %v", testRepoFixturesErr)
	}

	name := "empty"
	if seeded {
		name = "seeded"
	}
	repoDir := t.TempDir()
	if err := copyFixtureTree(filepath.Join(testRepoFixturesRoot, name), repoDir); err != nil {
		t.Fatalf("materialize %s Git fixture: %v", name, err)
	}
	realRepoDir, err := filepath.EvalSymlinks(repoDir)
	if err != nil {
		t.Fatalf("canonicalize temp repo: %v", err)
	}
	return realRepoDir
}

func initTestRepoFixtures() {
	root, err := os.MkdirTemp("", "acd-cli-repo-fixtures-")
	if err != nil {
		testRepoFixturesErr = err
		return
	}
	testRepoFixturesRoot = root

	empty := filepath.Join(root, "empty")
	if err := os.MkdirAll(empty, 0o700); err != nil {
		testRepoFixturesErr = err
		return
	}
	ctx := context.Background()
	if _, err := git.Run(ctx, git.RunOpts{Dir: empty}, "init", "-q", "-b", "main"); err != nil {
		testRepoFixturesErr = err
		return
	}
	for _, setting := range [][2]string{
		{"user.name", "ACD Test"},
		{"user.email", "acd@example.invalid"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: empty}, "config", setting[0], setting[1]); err != nil {
			testRepoFixturesErr = err
			return
		}
	}
	dbPath := state.DBPathFromGitDir(filepath.Join(empty, ".git"))
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		testRepoFixturesErr = err
		return
	}
	if _, err := db.SQL().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		_ = db.Close()
		testRepoFixturesErr = err
		return
	}
	if err := db.Close(); err != nil {
		testRepoFixturesErr = err
		return
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(dbPath + suffix); err != nil && !os.IsNotExist(err) {
			testRepoFixturesErr = err
			return
		}
	}

	seeded := filepath.Join(root, "seeded")
	if err := os.MkdirAll(seeded, 0o700); err != nil {
		testRepoFixturesErr = err
		return
	}
	if err := copyFixtureTree(empty, seeded); err != nil {
		testRepoFixturesErr = err
		return
	}
	if err := os.WriteFile(filepath.Join(seeded, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		testRepoFixturesErr = err
		return
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: seeded}, "add", "seed.txt"); err != nil {
		testRepoFixturesErr = err
		return
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: seeded}, "commit", "-q", "-m", "seed"); err != nil {
		testRepoFixturesErr = err
	}
}

func copyFixtureTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if entry.Type()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(linkTarget, target)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			_ = in.Close()
			return err
		}
		_, copyErr := io.Copy(out, in)
		return errors.Join(copyErr, out.Close(), in.Close())
	})
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

func upsertActivatedRepoFixture(
	registry *central.Registry,
	root, commonDir, stateDB, harness string,
	now int64,
) {
	repositoryID := central.CanonicalID(commonDir)
	registry.UpsertRepo(root, repositoryID, stateDB, harness, now)
	for i := range registry.Repos {
		if central.SameRepoPath(registry.Repos[i].Path, root) {
			registry.Repos[i].RepositoryID = repositoryID
			registry.Repos[i].WorktreeID = central.CanonicalID(root)
			registry.Repos[i].CommonDir = commonDir
			return
		}
	}
}

// nowFloat returns the current wall-clock time as the REAL-column unix
// seconds the schema uses for daemon_state.heartbeat_ts etc.
func nowFloat() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

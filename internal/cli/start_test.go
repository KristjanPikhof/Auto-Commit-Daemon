package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)





func makeStartRepo(t *testing.T) string {
	t.Helper()
	repoDir := makeUnregisteredStartRepo(t)
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	registerStartRepoFixture(t, roots, repoDir)
	return repoDir
}

// registerStartRepoFixture gives start and pause tests explicit repository
// consent without paying the fsync cost of a production registry mutation.
// These tests use isolated roots and exercise registry durability separately.
func registerStartRepoFixture(t *testing.T, roots paths.Roots, repoDir string) {
	t.Helper()
	registry, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry fixture: %v", err)
	}
	root, err := filepath.EvalSymlinks(repoDir)
	if err != nil {
		t.Fatalf("resolve repository fixture: %v", err)
	}
	gitDir := filepath.Join(root, ".git")
	upsertActivatedRepoFixture(registry, root, gitDir,
		state.DBPathFromGitDir(gitDir), "", time.Now().Unix())
	body, err := json.Marshal(registry)
	if err != nil {
		t.Fatalf("marshal registry fixture: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(roots.RegistryPath()), 0o700); err != nil {
		t.Fatalf("create registry fixture directory: %v", err)
	}
	if err := os.WriteFile(roots.RegistryPath(), body, 0o600); err != nil {
		t.Fatalf("write registry fixture: %v", err)
	}
}

func makeUnregisteredStartRepo(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	repoDir := t.TempDir()
	if err := git.Init(ctx, repoDir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	return repoDir
}

func registerEnabledStartRepo(t *testing.T, repoDir string) {
	t.Helper()
	ctx := context.Background()
	wt, err := git.ResolveWorktree(ctx, repoDir)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		registration, err := registry.RegisterResolvedRepo(wt, "", time.Now().Unix())
		if err != nil {
			return err
		}
		registry.EnableRepo(central.RepoRemovalTarget{
			Path: registration.Record.Path, StateDB: registration.Record.StateDB,
		}, time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatalf("register enabled repository: %v", err)
	}
}

func openStartDB(t *testing.T, repoDir string) *state.DB {
	t.Helper()
	ctx := context.Background()
	db, err := state.Open(ctx, state.DBPathFromGitDir(filepath.Join(repoDir, ".git")))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}











func commitStartRepoSeed(t *testing.T, repoDir string) string {
	t.Helper()
	ctx := context.Background()
	for _, kv := range [][]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	if err := os.WriteFile(filepath.Join(repoDir, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "commit", "-q", "-m", "seed"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	head, err := git.RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return head
}





























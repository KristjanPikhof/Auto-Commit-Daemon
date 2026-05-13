package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveWorktreeExplicitRepoPath(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	wt, err := ResolveWorktree(ctx, dir)
	if err != nil {
		t.Fatalf("ResolveWorktree: %v", err)
	}
	wantRoot := canonicalTestPath(t, dir)
	if wt.Root != wantRoot {
		t.Fatalf("root=%q want %q", wt.Root, wantRoot)
	}
	wantGitDir := filepath.Join(wantRoot, ".git")
	if wt.GitDir != wantGitDir {
		t.Fatalf("gitDir=%q want %q", wt.GitDir, wantGitDir)
	}
}

func TestResolveWorktreeNestedSubdir(t *testing.T) {
	dir := initRepo(t)
	nested := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	wt, err := ResolveWorktree(context.Background(), nested)
	if err != nil {
		t.Fatalf("ResolveWorktree nested: %v", err)
	}
	wantRoot := canonicalTestPath(t, dir)
	if wt.Root != wantRoot {
		t.Fatalf("root=%q want %q", wt.Root, wantRoot)
	}
}

func TestResolveWorktreeRejectsNonGitDirectory(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()

	_, err := ResolveWorktree(context.Background(), dir)
	if err == nil {
		t.Fatal("expected non-Git directory error")
	}
	if !errors.Is(err, ErrNotWorktree) {
		t.Fatalf("expected ErrNotWorktree, got %v", err)
	}
}

func TestResolveWorktreeRejectsMissingPath(t *testing.T) {
	requireGit(t)
	missing := filepath.Join(t.TempDir(), "missing")

	_, err := ResolveWorktree(context.Background(), missing)
	if err == nil {
		t.Fatal("expected missing path error")
	}
	if errors.Is(err, ErrNotWorktree) {
		t.Fatalf("missing path should be a path error, got %v", err)
	}
}

func TestResolveWorktreeLinkedWorktreeGitFile(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	if _, err := Run(ctx, RunOpts{Dir: dir}, "commit", "--allow-empty", "-m", "init"); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	linked := filepath.Join(t.TempDir(), "linked")
	if _, err := Run(ctx, RunOpts{Dir: dir}, "worktree", "add", "-q", "-b", "linked-test", linked); err != nil {
		t.Fatalf("git worktree add: %v", err)
	}
	gitFile, err := os.Stat(filepath.Join(linked, ".git"))
	if err != nil {
		t.Fatalf("stat linked .git file: %v", err)
	}
	if gitFile.IsDir() {
		t.Fatalf("expected linked worktree .git to be a file")
	}

	wt, err := ResolveWorktree(ctx, linked)
	if err != nil {
		t.Fatalf("ResolveWorktree linked: %v", err)
	}
	wantRoot := canonicalTestPath(t, linked)
	if wt.Root != wantRoot {
		t.Fatalf("root=%q want %q", wt.Root, wantRoot)
	}
	if wt.GitDir == filepath.Join(wantRoot, ".git") {
		t.Fatalf("git dir used literal .git file path: %q", wt.GitDir)
	}
	if _, err := os.Stat(wt.GitDir); err != nil {
		t.Fatalf("resolved git dir does not exist: %s: %v", wt.GitDir, err)
	}
}

func canonicalTestPath(t *testing.T, path string) string {
	t.Helper()
	clean := filepath.Clean(path)
	if realPath, err := filepath.EvalSymlinks(clean); err == nil {
		return filepath.Clean(realPath)
	}
	return clean
}

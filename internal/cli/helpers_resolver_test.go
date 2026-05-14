package cli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func TestResolveRepoUsesCurrentWorkingDirectory(t *testing.T) {
	dir := initCLIResolverRepo(t)
	chdirForTest(t, dir)

	got, err := resolveRepo("")
	if err != nil {
		t.Fatalf("resolveRepo cwd: %v", err)
	}
	want := canonicalCLIResolverTestPath(t, dir)
	if got != want {
		t.Fatalf("repo=%q want %q", got, want)
	}
}

func chdirForTest(t *testing.T, dir string) {
	t.Helper()
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldwd) })
}

func TestResolveRepoUsesExplicitRepoPath(t *testing.T) {
	dir := initCLIResolverRepo(t)

	got, err := resolveRepo(dir)
	if err != nil {
		t.Fatalf("resolveRepo explicit: %v", err)
	}
	want := canonicalCLIResolverTestPath(t, dir)
	if got != want {
		t.Fatalf("repo=%q want %q", got, want)
	}
}

func TestResolveRepoCanonicalizesNestedSubdir(t *testing.T) {
	dir := initCLIResolverRepo(t)
	nested := filepath.Join(dir, "nested", "deeper")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	got, err := resolveRepo(nested)
	if err != nil {
		t.Fatalf("resolveRepo nested: %v", err)
	}
	want := canonicalCLIResolverTestPath(t, dir)
	if got != want {
		t.Fatalf("repo=%q want %q", got, want)
	}
}

func TestResolveRepoRejectsNonGitDirectory(t *testing.T) {
	requireGitForCLIResolverTest(t)
	dir := t.TempDir()

	_, err := resolveRepo(dir)
	if err == nil {
		t.Fatal("expected non-Git directory error")
	}
	if !strings.Contains(err.Error(), "not inside a Git worktree") {
		t.Fatalf("expected clear not-in-worktree message, got %v", err)
	}
}

func initCLIResolverRepo(t *testing.T) string {
	t.Helper()
	requireGitForCLIResolverTest(t)
	dir := t.TempDir()
	if err := gitpkg.Init(context.Background(), dir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	return dir
}

func requireGitForCLIResolverTest(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not on PATH; skipping")
	}
}

func canonicalCLIResolverTestPath(t *testing.T, path string) string {
	t.Helper()
	clean := filepath.Clean(path)
	if realPath, err := filepath.EvalSymlinks(clean); err == nil {
		return filepath.Clean(realPath)
	}
	return clean
}

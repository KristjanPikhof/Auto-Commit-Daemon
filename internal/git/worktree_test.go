package git

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkResolveWorktree(b *testing.B) {
	dir := b.TempDir()
	ctx := context.Background()
	if err := Init(ctx, dir); err != nil {
		b.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: dir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		if _, err := ResolveWorktree(ctx, dir); err != nil {
			b.Fatal(err)
		}
	}
}

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
	if wt.CommonDir != wantGitDir {
		t.Fatalf("commonDir=%q want %q", wt.CommonDir, wantGitDir)
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
	if want := filepath.Join(canonicalTestPath(t, dir), ".git"); wt.CommonDir != want {
		t.Fatalf("commonDir=%q want %q", wt.CommonDir, want)
	}
}

func TestResolveWorktreeUsesOneGitCallWithNewlineFallback(t *testing.T) {
	for _, name := range []string{"space ' and \" quotes", "line\nbreak"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), name)
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := Init(context.Background(), dir); err != nil {
				t.Fatal(err)
			}
			if _, err := Run(context.Background(), RunOpts{Dir: dir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
				t.Fatal(err)
			}
			realGit, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			wrapperDir := t.TempDir()
			calls := filepath.Join(wrapperDir, "calls")
			quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
			script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + quote(calls) + "\nexec " + quote(realGit) + " \"$@\"\n"
			if err := os.WriteFile(filepath.Join(wrapperDir, "git"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", wrapperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			wt, err := ResolveWorktree(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			wantRoot := canonicalTestPath(t, dir)
			if want := (Worktree{Root: wantRoot, GitDir: filepath.Join(wantRoot, ".git"), CommonDir: filepath.Join(wantRoot, ".git")}); wt != want {
				t.Fatalf("worktree=%+v want %+v", wt, want)
			}
			data, err := os.ReadFile(calls)
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if strings.Contains(name, "\n") {
				wantCalls = 4
			}
			if got := strings.Count(string(data), "\n"); got != wantCalls {
				t.Fatalf("Git calls=%d want %d: %s", got, wantCalls, data)
			}
		})
	}
}

func TestResolveWorktreeSubmoduleAndSymlink(t *testing.T) {
	dir := initRepo(t)
	source := initRepo(t)
	ctx := context.Background()
	if _, err := Run(ctx, RunOpts{Dir: source}, "commit", "--allow-empty", "-m", "seed"); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: dir}, "-c", "protocol.file.allow=always", "submodule", "add", "-q", source, "module"); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "module-link")
	if err := os.Symlink(filepath.Join(dir, "module"), link); err != nil {
		t.Fatal(err)
	}
	wt, err := ResolveWorktree(ctx, link)
	if err != nil {
		t.Fatal(err)
	}
	root := canonicalTestPath(t, dir)
	gitDir := filepath.Join(root, ".git", "modules", "module")
	if want := (Worktree{Root: filepath.Join(root, "module"), GitDir: gitDir, CommonDir: gitDir}); wt != want {
		t.Fatalf("worktree=%+v want %+v", wt, want)
	}
}

func TestResolveWorktreeRejectsBareAndMalformedGitFile(t *testing.T) {
	for _, bare := range []bool{false, true} {
		name := "malformed git file"
		if bare {
			name = "bare repository"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if bare {
				if _, err := Run(context.Background(), RunOpts{Dir: dir}, "init", "--bare", "-q"); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(filepath.Join(dir, ".git"), []byte("not a git directory\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := ResolveWorktree(context.Background(), dir); !errors.Is(err, ErrNotWorktree) {
				t.Fatalf("expected ErrNotWorktree, got %v", err)
			}
		})
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

package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNotWorktree identifies paths that exist but are not inside a Git worktree.
var ErrNotWorktree = errors.New("git: path is not inside a worktree")

// Worktree identifies the canonical Git worktree root and its absolute git dir.
type Worktree struct {
	Root   string
	GitDir string
}

// ResolveWorktree resolves repoPath (or the current working directory when
// repoPath is empty) to the canonical Git worktree root and absolute git dir.
// It validates that the starting path exists, then relies on Git's own
// rev-parse output so linked worktrees, submodules, and .git files are handled
// according to Git semantics rather than filesystem heuristics.
func ResolveWorktree(ctx context.Context, repoPath string) (Worktree, error) {
	if repoPath == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return Worktree{}, fmt.Errorf("git: get cwd: %w", err)
		}
		repoPath = cwd
	}
	abs, err := filepath.Abs(repoPath)
	if err != nil {
		return Worktree{}, fmt.Errorf("git: abs %q: %w", repoPath, err)
	}
	abs = filepath.Clean(abs)
	if realAbs, err := filepath.EvalSymlinks(abs); err == nil {
		abs = filepath.Clean(realAbs)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return Worktree{}, fmt.Errorf("git: stat %s: %w", abs, err)
	}
	if !info.IsDir() {
		return Worktree{}, fmt.Errorf("git: %s is not a directory", abs)
	}

	root, err := ShowToplevel(ctx, abs)
	if err != nil {
		return Worktree{}, fmt.Errorf("%w: %s", ErrNotWorktree, abs)
	}
	gitDir, err := AbsoluteGitDir(ctx, abs)
	if err != nil {
		return Worktree{}, fmt.Errorf("git: resolve absolute git dir for %s: %w", abs, err)
	}
	root = filepath.Clean(root)
	if realRoot, err := filepath.EvalSymlinks(root); err == nil {
		root = filepath.Clean(realRoot)
	}
	gitDir = filepath.Clean(gitDir)
	if realGitDir, err := filepath.EvalSymlinks(gitDir); err == nil {
		gitDir = filepath.Clean(realGitDir)
	}
	return Worktree{Root: root, GitDir: gitDir}, nil
}

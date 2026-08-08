package git

import (
	"bytes"
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
	Root      string
	GitDir    string
	CommonDir string
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
		return Worktree{}, fmt.Errorf("%w: %s: %v", ErrNotWorktree, abs, err)
	}
	gitDir, err := AbsoluteGitDir(ctx, abs)
	if err != nil {
		return Worktree{}, fmt.Errorf("git: resolve absolute git dir for %s: %w", abs, err)
	}
	commonDir, err := GitCommonDir(ctx, abs)
	if err != nil {
		return Worktree{}, fmt.Errorf("git: resolve common dir for %s: %w", abs, err)
	}
	root = filepath.Clean(root)
	if realRoot, err := filepath.EvalSymlinks(root); err == nil {
		root = filepath.Clean(realRoot)
	}
	// `rev-parse --absolute-git-dir` already returns an absolute path, but a
	// linked-worktree .git file or a .git symlink can point at a dir that
	// is itself behind a symlink; resolve so registry identity stays stable.
	gitDir = filepath.Clean(gitDir)
	if realGitDir, err := filepath.EvalSymlinks(gitDir); err == nil {
		gitDir = filepath.Clean(realGitDir)
	}
	commonDir = filepath.Clean(commonDir)
	if realCommonDir, err := filepath.EvalSymlinks(commonDir); err == nil {
		commonDir = filepath.Clean(realCommonDir)
	}
	return Worktree{Root: root, GitDir: gitDir, CommonDir: commonDir}, nil
}

// GitCommonDir returns the canonical absolute Git common directory shared by
// linked worktrees. Ordinary repositories return their .git directory.
func GitCommonDir(ctx context.Context, dir string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultReadTimeout},
		"rev-parse", "--git-common-dir")
	if err != nil {
		return "", err
	}
	common := filepath.Clean(string(bytes.TrimSpace(out)))
	if !filepath.IsAbs(common) {
		common = filepath.Join(dir, common)
	}
	common, err = filepath.Abs(common)
	if err != nil {
		return "", err
	}
	return filepath.Clean(common), nil
}

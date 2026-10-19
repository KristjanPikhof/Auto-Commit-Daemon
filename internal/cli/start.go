package cli

import (
	"context"

	"path/filepath"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

// resolveGitDir resolves the .git directory for a repo. Falls back to
// <repo>/.git when the git binary fails (common in synthetic test repos).
func resolveGitDir(ctx context.Context, repo string) (string, error) {
	resolved, err := git.AbsoluteGitDir(ctx, repo)
	if err == nil {
		return resolved, nil
	}
	fallback := filepath.Join(repo, ".git")
	if fileExists(fallback) {
		return fallback, nil
	}
	return "", err
}

package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

// startResult is the JSON payload returned by `acd start --json`.
type startResult struct {
	Started     bool   `json:"started"`
	Duplicate   bool   `json:"duplicate"`
	DaemonPID   int    `json:"daemon_pid,omitempty"`
	Skipped     bool   `json:"skipped,omitempty"`
	SkipReason  string `json:"skipped_reason,omitempty"`
	Repo        string `json:"repo"`
	RepoHash    string `json:"repo_hash"`
	SessionID   string `json:"session_id"`
	Harness     string `json:"harness"`
	ClientCount int    `json:"client_count"`
}

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

func ensureAttachedHEAD(ctx context.Context, repo string) error {
	branchRef, err := git.RunBranchRef(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd start: resolve HEAD branch: %w", err)
	}
	if branchRef == "" {
		return errors.New("acd start: detached HEAD is not supported; checkout a branch before starting")
	}
	return nil
}

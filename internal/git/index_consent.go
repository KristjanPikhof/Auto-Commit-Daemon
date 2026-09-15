package git

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrStagingChanged = errors.New("staging changed after approval; the current staging selection was preserved")

// IndexContentDigest describes index entries and their user-visible flags,
// excluding stat-cache refreshes that do not change the staging selection.
func IndexContentDigest(ctx context.Context, repoDir string) (string, error) {
	return indexContentDigest(ctx, repoDir, "")
}

func indexContentDigest(ctx context.Context, repoDir, indexFile string) (string, error) {
	env := map[string]string{}
	if indexFile != "" {
		env["GIT_INDEX_FILE"] = indexFile
	}
	body, err := Run(ctx, RunOpts{Dir: repoDir, ExtraEnv: env}, "ls-files", "--stage", "-v", "-z")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(body)), nil
}

// ConsumeApprovedIndex serializes comparison and replacement with Git writers.
// An already-clean index is the idempotent recovery state after a prior rename
// succeeded but recording staged_consumed was interrupted.
func ConsumeApprovedIndex(ctx context.Context, repoDir, expectedDigest, expectedHead string) error {
	worktree, err := ResolveWorktree(ctx, repoDir)
	if err != nil {
		return err
	}
	indexPath := filepath.Join(worktree.GitDir, "index")
	lockPath := indexPath + ".lock"
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("lock approved index: %w", err)
	}
	owned := true
	defer func() {
		_ = lock.Close()
		if owned {
			_ = os.Remove(lockPath)
		}
	}()
	current, err := IndexContentDigest(ctx, repoDir)
	if err != nil {
		return err
	}
	temporary, err := os.MkdirTemp(worktree.GitDir, "acd-approved-index-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporary)
	replacement := filepath.Join(temporary, "index")
	if err := ReadTree(ctx, repoDir, replacement, expectedHead); err != nil {
		return err
	}
	clean, err := indexContentDigest(ctx, repoDir, replacement)
	if err != nil {
		return err
	}
	if current == clean {
		return nil
	}
	if expectedDigest == "" {
		return errors.New("staging approval has no saved index identity; review commit-all again")
	}
	if current != expectedDigest {
		return ErrStagingChanged
	}
	head, err := Run(ctx, RunOpts{Dir: repoDir}, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(head)) != expectedHead {
		return errors.New("branch changed before consuming approved staging")
	}
	body, err := os.ReadFile(replacement)
	if err != nil {
		return err
	}
	if _, err := lock.Write(body); err != nil {
		return err
	}
	if err := lock.Sync(); err != nil {
		return err
	}
	if err := lock.Close(); err != nil {
		return err
	}
	if err := os.Rename(lockPath, indexPath); err != nil {
		return err
	}
	owned = false
	directory, err := os.Open(worktree.GitDir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

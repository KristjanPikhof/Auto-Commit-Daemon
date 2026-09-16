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

var ErrStagingApprovalMissing = errors.New("staging approval has no saved index identity; review commit-all again")

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
	// Intent-to-add has the same empty-blob entry as a staged empty file.
	// Its exclusion from this diff preserves that distinct staging choice.
	// Hashing (without -w) computes the object-format-aware empty tree without
	// writing Git objects; a fixed tree keeps this independent of HEAD moves.
	emptyTree, err := Run(ctx, RunOpts{Dir: repoDir}, "hash-object", "-t", "tree", "--stdin")
	if err != nil {
		return "", err
	}
	staged, err := Run(ctx, RunOpts{Dir: repoDir, ExtraEnv: env}, "diff", "--cached", "--raw", "--no-abbrev", "--no-renames", "--no-ext-diff", "--ita-invisible-in-index", "-z", strings.TrimSpace(string(emptyTree)))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(append(body, staged...))), nil
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
	if err := preserveIndexFlags(ctx, repoDir, replacement); err != nil {
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
		return ErrStagingApprovalMissing
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

// Consuming staged content must retain sparse-checkout and assume-unchanged
// choices for entries that remain in HEAD.
func preserveIndexFlags(ctx context.Context, repoDir, replacement string) error {
	flags, err := Run(ctx, RunOpts{Dir: repoDir}, "ls-files", "-v", "-z")
	if err != nil {
		return err
	}
	env := map[string]string{"GIT_INDEX_FILE": replacement}
	paths, err := Run(ctx, RunOpts{Dir: repoDir, ExtraEnv: env}, "ls-files", "-z")
	if err != nil {
		return err
	}
	retained := make(map[string]bool)
	for _, path := range strings.Split(string(paths), "\x00") {
		retained[path] = true
	}
	var skip, assume []string
	for _, entry := range strings.Split(string(flags), "\x00") {
		if len(entry) < 3 || !retained[entry[2:]] {
			continue
		}
		if entry[0] == 'S' || entry[0] == 's' {
			skip = append(skip, entry[2:])
		}
		if entry[0] >= 'a' && entry[0] <= 'z' {
			assume = append(assume, entry[2:])
		}
	}
	for _, choice := range []struct {
		flag  string
		paths []string
	}{{"--skip-worktree", skip}, {"--assume-unchanged", assume}} {
		if len(choice.paths) == 0 {
			continue
		}
		_, err := Run(ctx, RunOpts{Dir: repoDir, ExtraEnv: env, Stdin: strings.NewReader(strings.Join(choice.paths, "\x00") + "\x00")}, "update-index", choice.flag, "-z", "--stdin")
		if err != nil {
			return err
		}
	}
	return nil
}

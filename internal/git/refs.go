package git

import (
	"context"
	"errors"
	"strings"
)

// ErrRefNotFound is returned by RevParse when the requested rev does not
// resolve. Callers use it to distinguish "missing ref" (expected, e.g. on
// an initial commit) from a real git failure.
var ErrRefNotFound = errors.New("git: ref not found")

// RevParse resolves rev (any acceptable revision spec — HEAD, refs/...,
// short hash, etc.) to a full SHA. Returns ErrRefNotFound when the rev does
// not exist; other failures surface as *Error.
func RevParse(ctx context.Context, repoDir, rev string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir}, "rev-parse", "--verify", "--quiet", rev)
	if err != nil {
		var gerr *Error
		if errors.As(err, &gerr) && gerr.ExitCode == 1 {
			return "", ErrRefNotFound
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// ShowToplevel returns the absolute path of the worktree root.
func ShowToplevel(ctx context.Context, dir string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: dir}, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// AbsoluteGitDir returns the absolute path of the worktree's git dir
// (`.git` for normal repos, the linked git dir for worktrees).
func AbsoluteGitDir(ctx context.Context, dir string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: dir}, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// UpdateRef sets ref to newOID. If oldOID is non-empty, it is supplied as
// the expected current value (compare-and-swap); pass the empty string for
// a new ref or an unconditional update.
//
// Mirrors the legacy replay's compare-and-swap update-ref invocation in
// snapshot-replay.py. --no-deref makes named refs explicit instead of
// following symbolic refs; literal HEAD is allowed to dereference so linked
// worktrees update their active branch rather than detaching HEAD.
func UpdateRef(ctx context.Context, repoDir, ref, newOID, oldOID string) error {
	args := []string{"update-ref"}
	if ref != "HEAD" {
		args = append(args, "--no-deref")
	}
	args = append(args, ref, newOID)
	if oldOID != "" {
		args = append(args, oldOID)
	}
	_, err := Run(ctx, RunOpts{Dir: repoDir}, args...)
	return err
}

// RunBranchRef returns the symbolic ref the worktree's HEAD points at,
// e.g. "refs/heads/main". Returns ("", nil) on a detached HEAD; surfaces
// any other git failure verbatim.
//
// The shell-out is `git symbolic-ref --quiet HEAD`. Detached HEAD makes
// git exit 1 with no stderr; we map that to ("", nil) so the run loop can
// fall back to a default branch name.
func RunBranchRef(ctx context.Context, repoDir string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir}, "symbolic-ref", "--quiet", "HEAD")
	if err != nil {
		var gerr *Error
		if errors.As(err, &gerr) && gerr.ExitCode == 1 {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// IsAncestor reports whether ancestor is an ancestor of descendant.
// Returns (true, nil) when ancestor, (false, nil) when not. A real git
// failure (e.g. unresolved oid) returns a non-nil error.
func IsAncestor(ctx context.Context, repoDir, ancestor, descendant string) (bool, error) {
	_, _, err := RunWithStderr(ctx, RunOpts{Dir: repoDir}, "merge-base", "--is-ancestor", ancestor, descendant)
	if err == nil {
		return true, nil
	}
	var gerr *Error
	if errors.As(err, &gerr) && gerr.ExitCode == 1 {
		return false, nil
	}
	return false, err
}


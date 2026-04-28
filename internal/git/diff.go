package git

import (
	"context"
	"strings"
)

// DiffBlobs runs `git diff --no-color --no-ext-diff` between two blob OIDs
// and returns the unified diff output. Used by the replay's commit-message
// AI plugin path (§8.3).
//
// Either OID may be the empty tree's all-zero SHA to represent
// creation/deletion, but most callers will use git's special "/dev/null"
// path semantics by passing the empty string for the missing side; this
// helper sticks to two real OIDs for now and the higher-level diff helpers
// are introduced when the replay path lands (phase 5).
func DiffBlobs(ctx context.Context, repoDir, oidA, oidB string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir},
		"diff", "--no-color", "--no-ext-diff", oidA, oidB,
	)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// DiffPath runs `git diff --no-color --no-ext-diff -- <path>` against the
// working tree and returns the unified diff. Empty output means the path
// matches the index.
func DiffPath(ctx context.Context, repoDir, path string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir},
		"diff", "--no-color", "--no-ext-diff", "--", path,
	)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// CatFileBlob returns the bytes of a blob OID. Useful for replay's
// "resolve many blob OIDs" path (snapshot-replay.py uses cat-file --batch
// for this; one-off lookups go through this helper).
func CatFileBlob(ctx context.Context, repoDir, oid string) ([]byte, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir}, "cat-file", "blob", oid)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EmptyTreeOID is git's hard-coded empty-tree object id; useful as a
// "before" tree for initial-commit diffs and replay recovery.
const EmptyTreeOID = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

// TrimSHA returns oid with any trailing whitespace stripped. Convenience
// for callers that want to defend against a stray newline from upstream.
func TrimSHA(oid string) string { return strings.TrimSpace(oid) }

package git

import (
	"context"
	"errors"
	"strings"
)

// DiffBlobs runs `git diff --no-color --no-ext-diff` between two blob OIDs
// and returns the unified diff output. Used by the replay's commit-message
// AI plugin path (§8.3).
//
// The output is capped at DefaultDiffCap; on overflow the partial prefix is
// returned alongside ErrStdoutOverflow so callers can surface a truncated
// payload or fall back to metadata-only flows.
//
// Either OID may be the empty tree's all-zero SHA to represent
// creation/deletion, but most callers will use git's special "/dev/null"
// path semantics by passing the empty string for the missing side; this
// helper sticks to two real OIDs for now and the higher-level diff helpers
// are introduced when the replay path lands (phase 5).
func DiffBlobs(ctx context.Context, repoDir, oidA, oidB string) (string, error) {
	return DiffBlobsLimited(ctx, repoDir, oidA, oidB, DefaultDiffCap)
}

// DiffBlobsLimited is DiffBlobs with an explicit stdout byte cap. A
// maxBytes <= 0 disables the cap.
func DiffBlobsLimited(ctx context.Context, repoDir, oidA, oidB string, maxBytes int64) (string, error) {
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, maxBytes,
		"diff", "--no-color", "--no-ext-diff", oidA, oidB,
	)
	if err != nil {
		if errors.Is(err, ErrStdoutOverflow) {
			return string(out), err
		}
		return "", err
	}
	return string(out), nil
}

// DiffPath runs `git diff --no-color --no-ext-diff -- <path>` against the
// working tree and returns the unified diff. Empty output means the path
// matches the index. Output is capped at DefaultDiffCap.
func DiffPath(ctx context.Context, repoDir, path string) (string, error) {
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, DefaultDiffCap,
		"diff", "--no-color", "--no-ext-diff", "--", path,
	)
	if err != nil {
		if errors.Is(err, ErrStdoutOverflow) {
			return string(out), err
		}
		return "", err
	}
	return string(out), nil
}

// CatFileBlob returns the bytes of a blob OID. Useful for replay's
// "resolve many blob OIDs" path (snapshot-replay.py uses cat-file --batch
// for this; one-off lookups go through this helper). Output is capped at
// DefaultDiffCap; on overflow the prefix is returned with ErrStdoutOverflow.
func CatFileBlob(ctx context.Context, repoDir, oid string) ([]byte, error) {
	return CatFileBlobLimited(ctx, repoDir, oid, DefaultDiffCap)
}

// CatFileBlobLimited is CatFileBlob with an explicit stdout byte cap. A
// maxBytes <= 0 disables the cap.
func CatFileBlobLimited(ctx context.Context, repoDir, oid string, maxBytes int64) ([]byte, error) {
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, maxBytes,
		"cat-file", "blob", oid,
	)
	if err != nil {
		if errors.Is(err, ErrStdoutOverflow) {
			return out, err
		}
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

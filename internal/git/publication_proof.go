package git

import (
	"context"
	"errors"
	"fmt"
)

// PublicationOp is the immutable object evidence needed to prove one capture.
// Empty optional fields mean no captured evidence was supplied.
type PublicationOp struct {
	Operation string
	Path      string
	OldPath   string
	BeforeOID string
	AfterOID  string
	AfterMode string
}

// PublicationProofPolicy keeps caller-specific recovery limits explicit.
type PublicationProofPolicy struct {
	AllowDeletes          bool
	MissingPathIsMismatch bool
}

// ProvePublicationAtHEAD checks that the expected HEAD descends from sourceHead,
// contains every operation, and remains HEAD after all probes. It never writes.
// A false result means the caller must preserve its existing recovery boundary.
func ProvePublicationAtHEAD(ctx context.Context, repo, sourceHead, headOID string, ops []PublicationOp, policy PublicationProofPolicy) (string, bool, error) {
	if len(ops) == 0 || headOID == "" {
		return "", false, nil
	}
	// Ancestry guard: an external HEAD that doesn't descend from our
	// replay parent means the matching tree state is coincidence, not a
	// successful parallel publish. Return (headOID, false) so the caller
	// can record a real conflict instead of silently chaining off a
	// stranger.
	if sourceHead != "" && sourceHead != headOID {
		descends, err := IsAncestor(ctx, repo, sourceHead, headOID)
		if err != nil {
			return "", false, fmt.Errorf("ancestry probe %s..%s: %w", sourceHead, headOID, err)
		}
		if !descends {
			return headOID, false, nil
		}
	}
	for _, op := range ops {
		if op.Operation == "delete" {
			if !policy.AllowDeletes {
				return headOID, false, nil
			}
			// Delete is idempotent only when HEAD has NO entry at all
			// for this path. A path replaced by a directory (tree
			// entry) or a submodule (commit entry) is NOT absent —
			// settling as published would mask a real divergence.
			absent, err := PathAbsentInTree(ctx, repo, headOID, op.Path)
			if err != nil {
				return "", false, err
			}
			if !absent {
				return headOID, false, nil
			}
			continue
		}
		blobOID, err := LsTreeBlobOID(ctx, repo, headOID, op.Path)
		if err != nil {
			if policy.MissingPathIsMismatch && errors.Is(err, ErrRefNotFound) {
				return headOID, false, nil
			}
			return "", false, fmt.Errorf("ls-tree HEAD %s: %w", op.Path, err)
		}
		if op.AfterOID == "" {
			return headOID, false, nil
		}
		if blobOID != op.AfterOID {
			return headOID, false, nil
		}
		if op.AfterMode != "" {
			entries, err := LsTree(ctx, repo, headOID, false, op.Path)
			if err != nil {
				return "", false, fmt.Errorf("ls-tree HEAD %s: %w", op.Path, err)
			}
			if !treeEntryModeMatches(entries, op.Path, op.AfterMode) {
				return headOID, false, nil
			}
		}
		if op.Operation == "rename" && op.OldPath != "" {
			absent, err := PathAbsentInTree(ctx, repo, headOID, op.OldPath)
			if err != nil {
				return "", false, err
			}
			if !absent {
				return headOID, false, nil
			}
			// Rename source verify: before settling as already-published
			// we require the captured BeforeOID for the rename source to
			// still be present in the object database. If it's missing
			// (gc'd, partial fetch), we cannot prove the rename actually
			// matches the captured intent, so refuse to settle and let
			// the caller block.
			if op.BeforeOID != "" {
				present, err := publicationObjectExists(ctx, repo, op.BeforeOID)
				if err != nil {
					return "", false, err
				}
				if !present {
					return headOID, false, nil
				}
			}
		}
	}
	// HEAD-movement guard: the per-op probes above all read against the
	// `headOID` we resolved at the start. If HEAD has moved while we were
	// probing (an external committer landed something between the first
	// rev-parse and the last ls-tree), the matching tree state no longer
	// describes the live ref. Refuse to settle and let the caller try
	// again on the next pass with a fresh anchor.
	postHead, err := RevParse(ctx, repo, "HEAD")
	if err != nil {
		if errors.Is(err, ErrRefNotFound) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("rev-parse HEAD post-probe: %w", err)
	}
	if postHead != headOID {
		return postHead, false, nil
	}
	return headOID, true, nil
}

// PathAbsentInTree reports whether path is absent at ref. A path resolved
// to a non-blob entry (tree, submodule) is treated as NOT absent — the
// caller's idempotent check must not confuse a directory-replacement with
// a successful delete.
func PathAbsentInTree(ctx context.Context, repo, ref, path string) (bool, error) {
	entries, err := LsTree(ctx, repo, ref, false, path)
	if err != nil {
		return false, fmt.Errorf("ls-tree %s %s: %w", ref, path, err)
	}
	for _, entry := range entries {
		if entry.Path == path {
			return false, nil
		}
	}
	return true, nil
}

// publicationObjectExists reports whether the given OID is present in the local
// object database via `git cat-file -e`. Used by the rename-source verify
// path so the daemon will not settle a rename as published when the
// captured BeforeOID is no longer reachable (shallow clone, gc'd ref).
func publicationObjectExists(ctx context.Context, repo, oid string) (bool, error) {
	if oid == "" {
		return false, nil
	}
	_, _, err := RunWithStderr(ctx, RunOpts{Dir: repo}, "cat-file", "-e", oid)
	if err == nil {
		return true, nil
	}
	var gerr *Error
	if errors.As(err, &gerr) && gerr.ExitCode == 1 {
		return false, nil
	}
	return false, fmt.Errorf("cat-file -e %s: %w", oid, err)
}

func treeEntryModeMatches(entries []TreeEntry, path, mode string) bool {
	for _, entry := range entries {
		if entry.Path == path && entry.Type == "blob" {
			return entry.Mode == mode
		}
	}
	return false
}

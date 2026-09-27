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
	AllowDeletes         bool
	MissingRefIsMismatch bool
}

// ProvePublicationAtHEAD checks that the expected HEAD descends from sourceHead,
// contains every operation, and remains HEAD after all probes. It never writes.
// A false result means the caller must preserve its existing recovery boundary.
func ProvePublicationAtHEAD(ctx context.Context, repo, sourceHead, headOID string, ops []PublicationOp, policy PublicationProofPolicy) (string, bool, error) {
	if len(ops) == 0 || headOID == "" {
		return "", false, nil
	}
	// Matching content on unrelated history is not publication proof.
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
			// A directory or submodule at the old path is not a completed delete.
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
			if policy.MissingRefIsMismatch && errors.Is(err, ErrRefNotFound) {
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
			// Keep the captured source object available as rename evidence.
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
	// Reject a proof whose expected commit is no longer HEAD.
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

// PathAbsentInTree treats files, directories and submodules as present.
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

// publicationObjectExists checks whether captured rename evidence still exists.
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

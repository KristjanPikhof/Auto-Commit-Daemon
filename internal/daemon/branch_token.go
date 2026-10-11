// branch_token.go reads literal HEAD identity and retained legacy generation metadata.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// BranchTokenMissing is the canonical "no HEAD" token.
const BranchTokenMissing = "missing"

// Retained legacy generation metadata used by recovery and restore.
const (
	// MetaKeyBranchGeneration stores the capture generation recorded by the old runtime.
	MetaKeyBranchGeneration = "branch.generation"
	// MetaKeyBranchHead stores the HEAD recorded alongside that generation.
	MetaKeyBranchHead = "branch.head"
)

// BranchGenerationToken returns the current generation token by resolving
// HEAD and the symbolic branch ref. ErrRefNotFound from git is mapped to a
// missing token; any other error is surfaced verbatim.
func BranchGenerationToken(ctx context.Context, repoDir string) (string, error) {
	if repoDir == "" {
		return "", fmt.Errorf("daemon: BranchGenerationToken: empty repoDir")
	}
	branchRef, err := git.RunBranchRef(ctx, repoDir)
	if err != nil {
		return "", err
	}
	sha, err := git.RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		if errors.Is(err, git.ErrRefNotFound) {
			return branchTokenMissing(branchRef), nil
		}
		return "", err
	}
	return branchTokenRev(sha, branchRef), nil
}

// tokenSHA strips the "rev:" prefix, returning "" for "missing" / "" / any
// shape we don't recognize.
func tokenSHA(token string) string {
	const prefix = "rev:"
	rest, ok := strings.CutPrefix(token, prefix)
	if !ok || rest == "" {
		return ""
	}
	sha, _, _ := strings.Cut(rest, " ")
	return sha
}

func tokenBranchRef(token string) string {
	if rest, ok := strings.CutPrefix(token, "rev:"); ok {
		_, branchRef, ok := strings.Cut(rest, " ")
		if ok {
			return branchRef
		}
		return ""
	}
	if rest, ok := strings.CutPrefix(token, BranchTokenMissing+" "); ok {
		return rest
	}
	return ""
}

func branchTokenRev(sha, branchRef string) string {
	if branchRef == "" {
		return "rev:" + sha
	}
	return "rev:" + sha + " " + branchRef
}

func branchTokenMissing(branchRef string) string {
	if branchRef == "" {
		return BranchTokenMissing
	}
	return BranchTokenMissing + " " + branchRef
}

// LoadBranchGeneration reads the persisted branch_generation from
// daemon_meta. Returns (1, nil) when the key is absent — the legacy default
// — so repositories without legacy metadata use generation 1.
// Bad / unparseable values fall back to (1, nil) with no error so
// malformed legacy metadata does not prevent recovery.
func LoadBranchGeneration(ctx context.Context, db *state.DB) (int64, error) {
	v, ok, err := state.MetaGet(ctx, db, MetaKeyBranchGeneration)
	if err != nil {
		return 0, fmt.Errorf("daemon: load branch generation: %w", err)
	}
	if !ok || v == "" {
		return 1, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		return 1, nil
	}
	return n, nil
}

// LoadBranchHead reads the last-known HEAD OID stored alongside the
// generation counter. Returns ("", nil) when the key is absent.
func LoadBranchHead(ctx context.Context, db *state.DB) (string, error) {
	v, ok, err := state.MetaGet(ctx, db, MetaKeyBranchHead)
	if err != nil {
		return "", fmt.Errorf("daemon: load branch head: %w", err)
	}
	if !ok {
		return "", nil
	}
	return v, nil
}

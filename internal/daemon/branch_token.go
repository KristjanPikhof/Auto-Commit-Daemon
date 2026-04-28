// branch_token.go implements the branch-generation token per §8.9.
//
// Token shape:
//   - "rev:<sha>" when HEAD resolves to a commit. Same generation between
//     iterations means the branch fast-forwarded (no rebase, no force-push).
//   - "missing"   when HEAD does not resolve (orphan repo, just-init'd).
//
// A bumped token signals a force-push or reset; the daemon records the
// transition in daemon_meta so operators can spot the divergence.
package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

// BranchTokenMissing is the canonical "no HEAD" token.
const BranchTokenMissing = "missing"

// BranchGenerationToken returns the current generation token by resolving
// HEAD. ErrRefNotFound from git is mapped to BranchTokenMissing; any other
// error is surfaced verbatim.
func BranchGenerationToken(ctx context.Context, repoDir string) (string, error) {
	if repoDir == "" {
		return "", fmt.Errorf("daemon: BranchGenerationToken: empty repoDir")
	}
	sha, err := git.RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		if errors.Is(err, git.ErrRefNotFound) {
			return BranchTokenMissing, nil
		}
		return "", err
	}
	return "rev:" + sha, nil
}

// SameGeneration reports whether two tokens describe the same generation.
// Two empty tokens compare equal (boot-time bootstrap); two non-empty
// tokens compare by exact-string equality, which captures both the
// rev-vs-missing transition and the rev-vs-different-rev transition.
func SameGeneration(a, b string) bool {
	return a == b
}

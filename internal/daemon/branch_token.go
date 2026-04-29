// branch_token.go implements the branch-generation token per §8.9.
//
// Token shape:
//   - "rev:<sha>" when HEAD resolves to a commit. Same generation between
//     iterations means the branch fast-forwarded (no rebase, no force-push).
//   - "missing"   when HEAD does not resolve (orphan repo, just-init'd).
//
// A bumped token signals a force-push or reset; the daemon records the
// transition in daemon_meta so operators can spot the divergence.
//
// # Generation semantics
//
// The token alone cannot distinguish a normal ACD fast-forward (the daemon
// just landed a commit and HEAD advanced one step) from an external rewrite
// (operator ran `git reset` / `git rebase` / branch switch). Both look like
// "the rev token changed". The daemon disambiguates by re-resolving HEAD's
// ancestry against the previously observed HEAD:
//
//   - newHead descends from prevHead (or prevHead == ""): ACD-style
//     fast-forward. Generation is preserved; queued events captured against
//     the prior HEAD remain valid because their BaseHead is still an
//     ancestor of HEAD.
//   - newHead does NOT descend from prevHead, OR transitioned to/from
//     "missing": the branch was rewritten under us. Generation bumps,
//     daemon_meta records the transition, and any queued events captured
//     under the old generation are terminally blocked at replay time.
//
// The generation counter is persisted in daemon_meta under
// MetaKeyBranchGeneration so a daemon restart picks up the last-known value
// instead of resetting to 1 (which would cause stale events to look fresh).
package daemon

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// BranchTokenMissing is the canonical "no HEAD" token.
const BranchTokenMissing = "missing"

// daemon_meta keys for the branch-generation machinery.
const (
	// MetaKeyBranchGeneration stores the integer branch_generation value
	// the run loop is currently using. Persisted across daemon restarts so
	// queued events captured under generation N remain comparable to a
	// freshly-booted daemon.
	MetaKeyBranchGeneration = "branch.generation"
	// MetaKeyBranchHead stores the last HEAD OID the run loop observed.
	// Used to drive the ancestry-based ACD-vs-external classification on
	// the next token transition.
	MetaKeyBranchHead = "branch.head"
	// MetaKeyBranchToken stores the raw last-known generation token
	// ("rev:<sha>" / "missing"). Operator-facing breadcrumb.
	MetaKeyBranchToken = "branch_token"
	// MetaKeyBranchTokenChangedAt stamps the wall-clock seconds at which
	// the token last changed. Operator breadcrumb only — the loop never
	// reads it back.
	MetaKeyBranchTokenChangedAt = "branch_token_changed_at"
)

// TokenTransition classifies how the active branch ref moved between two
// observations of HEAD.
type TokenTransition int

const (
	// TokenTransitionUnchanged means the token is identical (same SHA or
	// both "missing"). The run loop does nothing.
	TokenTransitionUnchanged TokenTransition = iota
	// TokenTransitionFastForward means newHead is a descendant of prevHead
	// (or prevHead was empty). Compatible with queued events captured at
	// prevHead because prevHead is still in the new HEAD's history. The
	// daemon's own commits land here; an operator running `git pull`
	// against an upstream that fast-forwards also lands here.
	TokenTransitionFastForward
	// TokenTransitionDiverged means newHead does not descend from prevHead
	// (rebase, reset, branch-switch, force-push, transition to/from
	// "missing"). Queued events captured under the prior generation are
	// no longer safe to replay — their BaseHead is no longer reachable
	// from HEAD.
	TokenTransitionDiverged
)

// String lets logs and tests render the transition without a switch.
func (t TokenTransition) String() string {
	switch t {
	case TokenTransitionUnchanged:
		return "unchanged"
	case TokenTransitionFastForward:
		return "fast-forward"
	case TokenTransitionDiverged:
		return "diverged"
	default:
		return "unknown"
	}
}

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

// ClassifyTokenTransition reports whether the move from prev->new HEAD is a
// fast-forward (newHead descends from prevHead) or a divergence (rebase,
// reset, branch-switch). prevHead == "" is treated as fast-forward — there
// was no prior history to descend from. A transition to or from
// BranchTokenMissing is always a divergence.
//
// repoDir is required for the merge-base ancestry probe. Callers that
// already know prevHead == newHead should short-circuit with
// TokenTransitionUnchanged; this helper does that anyway, but the caller
// can avoid the git-shellout in the common case.
func ClassifyTokenTransition(ctx context.Context, repoDir, prevToken, newToken string) (TokenTransition, error) {
	if SameGeneration(prevToken, newToken) {
		return TokenTransitionUnchanged, nil
	}
	// Token shape transition: missing<->rev. Always a divergence. The
	// queue's BaseHead is either the empty string (orphan) or a SHA that
	// is no longer reachable from HEAD.
	if prevToken == BranchTokenMissing || newToken == BranchTokenMissing {
		return TokenTransitionDiverged, nil
	}
	prevHead := tokenSHA(prevToken)
	newHead := tokenSHA(newToken)
	if prevHead == "" {
		// Boot-time first observation: no history to compare.
		return TokenTransitionFastForward, nil
	}
	if newHead == "" {
		// Defensive — shouldn't happen given the missing checks above.
		return TokenTransitionDiverged, nil
	}
	ok, err := git.MergeBaseIsAncestor(ctx, repoDir, prevHead, newHead)
	if err != nil {
		return TokenTransitionDiverged, fmt.Errorf("daemon: classify token: %w", err)
	}
	if ok {
		return TokenTransitionFastForward, nil
	}
	return TokenTransitionDiverged, nil
}

// tokenSHA strips the "rev:" prefix, returning "" for "missing" / "" / any
// shape we don't recognize.
func tokenSHA(token string) string {
	const prefix = "rev:"
	if len(token) > len(prefix) && token[:len(prefix)] == prefix {
		return token[len(prefix):]
	}
	return ""
}

// LoadBranchGeneration reads the persisted branch_generation from
// daemon_meta. Returns (1, nil) when the key is absent — the legacy default
// — so a fresh repo starts at generation 1 just like the in-memory seed.
// Bad / unparseable values fall back to (1, nil) with no error so an
// operator who hand-edits the row can't crash the daemon; the next bump
// will overwrite the row anyway.
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

// SaveBranchGeneration upserts the persisted branch_generation alongside
// the last-known HEAD OID. Both writes are best-effort from the run loop's
// perspective — the loop logs a warn but does not abort on failure. We
// nonetheless surface the underlying error so tests can assert it.
func SaveBranchGeneration(ctx context.Context, db *state.DB, generation int64, head string) error {
	if err := state.MetaSet(ctx, db, MetaKeyBranchGeneration,
		strconv.FormatInt(generation, 10)); err != nil {
		return err
	}
	if err := state.MetaSet(ctx, db, MetaKeyBranchHead, head); err != nil {
		return err
	}
	return nil
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

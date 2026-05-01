// branch_token.go implements the branch-generation token per §8.9.
//
// Token shape:
//   - "rev:<sha> <branch-ref>" when HEAD resolves to a commit on a branch.
//     Same generation between iterations means the branch fast-forwarded
//     (no rebase, no force-push, no branch switch).
//   - "rev:<sha>" when HEAD resolves while detached.
//   - "missing <branch-ref>" when HEAD does not resolve (orphan repo,
//     just-init'd).
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
	"os"
	"strconv"
	"strings"
	"time"

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
	// MetaKeyDetachedHeadPaused is stamped when the daemon sees a detached
	// HEAD and pauses capture/replay instead of inventing a branch ref.
	MetaKeyDetachedHeadPaused = "detached_head_paused"
	// MetaKeyOperationInProgress stores the active git operation name when
	// capture/replay are paused for rebase, merge, cherry-pick, or bisect.
	MetaKeyOperationInProgress = "operation_in_progress"
	// MetaKeyOperationInProgressSetAt stamps the wall-clock seconds at which
	// MetaKeyOperationInProgress was first observed. Used to detect stale
	// markers (rebase aborted but the marker file lingered) and warn the
	// operator without auto-clearing.
	MetaKeyOperationInProgressSetAt = "operation_in_progress.set_at"
	// MetaKeyOperationInProgressHead stamps the HEAD SHA observed when the
	// marker first appeared. Used together with MetaKeyOperationInProgressSetAt
	// to decide whether HEAD has moved since the marker showed up — the
	// "stale" heuristic only fires when both the marker AND HEAD have been
	// motionless for the threshold.
	MetaKeyOperationInProgressHead = "operation_in_progress.head_at"
	// MetaKeyReplayPausedUntil stores an RFC3339 UTC timestamp until which
	// replay should skip drain passes after a detected branch rewind.
	MetaKeyReplayPausedUntil = "replay.paused_until"
)

// EnvRewindGraceSeconds controls the post-rewind replay pause window. The
// default is intentionally short: enough for an operator's reset/revert flow
// to settle, but not long enough to surprise a normal daemon session.
const EnvRewindGraceSeconds = "ACD_REWIND_GRACE_SECONDS"

const defaultRewindGrace = 60 * time.Second

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
	prevMissing := tokenMissing(prevToken)
	newMissing := tokenMissing(newToken)
	prevBranchRef := tokenBranchRef(prevToken)
	newBranchRef := tokenBranchRef(newToken)
	// Asymmetric ref presence: exactly one of the two tokens carries a branch
	// ref. Covers legacy-token-to-named-ref upgrades (prev has no ref, new
	// has ref) and the inverse (new has no ref while prev did — detached-HEAD
	// or legacy-token downgrade). Both directions indicate a generation
	// boundary.
	if (prevBranchRef == "") != (newBranchRef == "") && prevToken != "" {
		return TokenTransitionDiverged, nil
	}
	if prevBranchRef != "" && newBranchRef != "" && prevBranchRef != newBranchRef {
		return TokenTransitionDiverged, nil
	}
	// Token shape transition: missing<->rev. Always a divergence. The
	// queue's BaseHead is either the empty string (orphan) or a SHA that
	// is no longer reachable from HEAD.
	if prevMissing || newMissing {
		if prevMissing && newMissing {
			// Compatibility with old persisted "missing" tokens that did
			// not carry the branch ref. With no commits there is no
			// ancestry to prove or disprove, so the first observation is
			// treated like boot-time initialization unless both refs above
			// proved a concrete branch switch.
			return TokenTransitionFastForward, nil
		}
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
	ok, err := git.IsAncestor(ctx, repoDir, prevHead, newHead)
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

func tokenMissing(token string) bool {
	return token == BranchTokenMissing || strings.HasPrefix(token, BranchTokenMissing+" ")
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

// maybeSetRewindGrace is called after ClassifyTokenTransition returns Diverged
// on a same branch-ref pair; it distinguishes a backward rewind (set grace
// marker) from any other divergence (return false).
//
// When newHead is an ancestor of prevHead (operator ran `git reset --soft
// HEAD~1` or similar rewinding op), the daemon writes
// daemon_meta.replay.paused_until = now+grace so the next capture/replay tick
// observes a paused gate via daemonPauseState. Non-rewind divergences (e.g.
// branch-switch, force-push, sibling commit) return (false, "", nil) without
// touching the meta key.
//
// During the grace window BOTH capture and replay are paused. fsnotify fires
// as untracked files reappear after a rewind, and a post-grace replay drain
// would otherwise resurrect work the operator just rewound. The Run loop gates
// capture via daemonPauseState; the Replay drain uses the same helper.
//
// Returns (active, expiresRFC3339, error).
func maybeSetRewindGrace(ctx context.Context, repoDir string, db *state.DB, prevToken, newToken string, now time.Time) (bool, string, error) {
	prevHead := tokenSHA(prevToken)
	newHead := tokenSHA(newToken)
	prevBranchRef := tokenBranchRef(prevToken)
	newBranchRef := tokenBranchRef(newToken)
	if prevHead == "" || newHead == "" || prevBranchRef == "" || newBranchRef == "" || prevBranchRef != newBranchRef {
		return false, "", nil
	}
	grace := resolveRewindGrace()
	if grace <= 0 {
		return false, "", nil
	}
	ok, err := git.IsAncestor(ctx, repoDir, newHead, prevHead)
	if err != nil {
		return false, "", err
	}
	if !ok {
		return false, "", nil
	}
	until := now.Add(grace).UTC().Format(time.RFC3339)
	if err := state.MetaSet(ctx, db, MetaKeyReplayPausedUntil, until); err != nil {
		return false, "", err
	}
	return true, until, nil
}

// rewindGraceActive reports whether daemon_meta.replay.paused_until carries a
// timestamp that is still in the future relative to now. It does NOT consult
// the manual pause marker — callers that need the full pause picture should
// call daemonPauseState. This helper exists so the run loop can detect a
// fast-forward landing inside an active rewind grace window: in that case the
// FF path must reseed shadow_paths from the rewound HEAD before clearing the
// gate, otherwise post-grace capture compares live HEAD against stale shadow
// rows seeded at the rewound (lower) HEAD and emits phantom create events.
//
// Returns (active, expiresAt, error). expiresAt is the parsed RFC3339 string
// from daemon_meta when active, "" otherwise. A malformed value is treated as
// inactive — daemonPauseState already warns about that case.
func rewindGraceActive(ctx context.Context, db *state.DB, now time.Time) (bool, string, error) {
	raw, ok, err := state.MetaGet(ctx, db, MetaKeyReplayPausedUntil)
	if err != nil {
		return false, "", err
	}
	if !ok || strings.TrimSpace(raw) == "" {
		return false, "", nil
	}
	until, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		return false, "", nil
	}
	if !until.After(now.UTC()) {
		return false, "", nil
	}
	return true, until.UTC().Format(time.RFC3339), nil
}

func resolveRewindGrace() time.Duration {
	raw := strings.TrimSpace(os.Getenv(EnvRewindGraceSeconds))
	if raw == "" {
		return defaultRewindGrace
	}
	secs, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || secs < 0 {
		return defaultRewindGrace
	}
	return time.Duration(secs) * time.Second
}

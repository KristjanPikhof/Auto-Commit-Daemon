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

// Current HEAD observations and read-only legacy transition markers.
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
	// MetaKeyBranchTransitionNeedsAttention records contradictory durable
	// evidence for an ACD-authored ref move. It keeps status honest when the
	// daemon must fail closed instead of retrying an invalid journal forever.
	MetaKeyBranchTransitionNeedsAttention = "branch.transition.needs_attention"
	// MetaKeyDetachedHeadPaused is stamped when the daemon sees a detached
	// HEAD. Checkpoint protection continues while publication is unavailable.
	MetaKeyDetachedHeadPaused = "detached_head_paused"
	// MetaKeyOperationInProgress stores the active git operation name when
	// publication is unavailable during rebase, merge, cherry-pick, or bisect.
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
	// MetaKeyManualPauseResumedAt stores the wall-clock seconds when
	// `acd resume` removed a manual pause marker. The run loop uses this
	// short-lived breadcrumb to distinguish a fast-forward that landed during
	// a manual pause from an ordinary upstream pull observed while unpaused.
	MetaKeyManualPauseResumedAt = "manual_pause.resumed_at"
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
// the last-known HEAD OID. Both values move in one transaction so readers
// cannot observe a new generation paired with the prior HEAD (or vice versa).
// The write is best-effort from the run loop's perspective — the loop logs a
// warning but does not abort on failure. We nonetheless surface the underlying
// error so tests can assert it.
func SaveBranchGeneration(ctx context.Context, db *state.DB, generation int64, head string) error {
	return state.MetaSetMany(ctx, db, map[string]string{
		MetaKeyBranchGeneration: strconv.FormatInt(generation, 10),
		MetaKeyBranchHead:       head,
	})
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

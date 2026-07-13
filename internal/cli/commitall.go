package cli

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// errCommitAllAborted is returned by runCommitAll when the user declines
// the interactive confirmation prompt. cobra's RunE maps any non-nil
// error to a non-zero exit code, so callers can distinguish "user said
// no" from "ran successfully with nothing to do".
var errCommitAllAborted = errors.New("acd commit-all: aborted by user")

// commitAllIncompleteError reports that commit-all stopped safely but did not
// drain every unpublished event. The rendered result carries the detailed
// counts; the typed error gives scripts a reliable non-zero outcome.
type commitAllIncompleteError struct {
	PendingAfter int
	Conflicts    int
	Failed       int
}

type commitAllReplayError struct {
	Cause        error
	PendingAfter int
}

func (e *commitAllReplayError) Error() string {
	return fmt.Sprintf("acd commit-all: replay stopped with %d unpublished event(s): %v", e.PendingAfter, e.Cause)
}

func (e *commitAllReplayError) Unwrap() error { return e.Cause }

// commitAllRecoveryError reports a failure while preserving pre-existing
// provenance pairs. Earlier pairs may already be protected and transitioned,
// so callers must receive the rendered partial result as well as a non-zero
// error.
type commitAllRecoveryError struct {
	Cause        error
	PendingAfter int
}

func (e *commitAllRecoveryError) Error() string {
	return fmt.Sprintf("acd commit-all: pre-existing recovery stopped with %d unpublished event(s): %v", e.PendingAfter, e.Cause)
}

func (e *commitAllRecoveryError) Unwrap() error { return e.Cause }

func (e *commitAllIncompleteError) Error() string {
	return fmt.Sprintf(
		"acd commit-all: incomplete (pending=%d conflicts=%d failed=%d)",
		e.PendingAfter, e.Conflicts, e.Failed,
	)
}

// commitAllResult is the JSON payload returned by `acd commit-all --json`.
type commitAllResult struct {
	OK                  bool     `json:"ok"`
	Repo                string   `json:"repo"`
	BranchRef           string   `json:"branch_ref"`
	HeadBefore          string   `json:"head_before"`
	HeadAfter           string   `json:"head_after,omitempty"`
	Strategy            string   `json:"strategy"`
	Provider            string   `json:"provider"`
	IntentWindow        int      `json:"intent_window,omitempty"`
	IntentDeferLim      int      `json:"intent_defer_limit,omitempty"`
	PendingBefore       int      `json:"pending_before"`
	PendingAfter        int      `json:"pending_after"`
	EstimatedPass       int      `json:"estimated_passes"`
	Commits             int      `json:"commits"`
	Drained             int      `json:"drained"`
	Conflicts           int      `json:"conflicts,omitempty"`
	Failed              int      `json:"failed,omitempty"`
	Incomplete          bool     `json:"incomplete,omitempty"`
	Error               string   `json:"error,omitempty"`
	DryRun              bool     `json:"dry_run,omitempty"`
	Confirmed           bool     `json:"confirmed,omitempty"`
	DroppedStalePending int      `json:"dropped_stale_pending,omitempty"`
	PreservedPending    int      `json:"preserved_pending,omitempty"`
	RecoveryRefs        []string `json:"recovery_refs,omitempty"`
	DurationMillis      int64    `json:"duration_ms"`
	Notes               []string `json:"notes,omitempty"`
}

func newCommitAllCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "commit-all",
		Short: "Commit every uncommitted file in this worktree (one-shot)",
		Long: `Commit each uncommitted file even when the daemon was off, ordered by
lexicographic path so the planner sees coherent sibling windows.

The active commit strategy is read from existing config (daemon meta first,
then ACD_COMMIT_STRATEGY env, then the canonical default). There is no
--strategy override: commit-all matches what the daemon would do on its own.

	Detached HEAD, an in-progress git operation
	(rebase/merge/cherry-pick/bisect), and a manual pause marker are refused for
	both previews and apply. If an authorized run reaches its mutation phase, it
	also refuses while the per-repo daemon is alive; dry-run and a clean no-op do
	not acquire daemon.lock.`,
		Example: `  acd commit-all --dry-run
  acd commit-all --yes
  acd commit-all --repo /path/to/repo --yes --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			yes, _ := cmd.Flags().GetBool("yes")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			jsonOut, _ := cmd.Flags().GetBool("json")
			err := runCommitAll(cmd.Context(), cmd.OutOrStdout(), cmd.InOrStdin(), repo, yes, dryRun, jsonOut)
			if errors.Is(err, errCommitAllAborted) {
				// The JSON / human payload has already been rendered by
				// runCommitAll; suppress cobra's secondary "Error:"
				// banner but keep the non-zero exit code so scripts
				// can distinguish a decline from a clean no-op.
				cmd.SilenceErrors = true
				cmd.SilenceUsage = true
			}
			return err
		},
	}
	cmd.Flags().Bool("yes", false, "Skip the interactive confirmation prompt")
	cmd.Flags().Bool("dry-run", false, "Plan and show summary without committing")
	return cmd
}

const (
	commitAllZeroProgressLimit = 3
	commitAllRecaptureLimit    = 3
)

type commitAllMutationStage string

const (
	commitAllStagePostLock         commitAllMutationStage = "post_lock"
	commitAllStageRecoveryPair     commitAllMutationStage = "recovery_pair"
	commitAllStageReseed           commitAllMutationStage = "reseed"
	commitAllStageCapture          commitAllMutationStage = "capture"
	commitAllStageReplay           commitAllMutationStage = "replay"
	commitAllStageRecaptureReseed  commitAllMutationStage = "recapture_reseed"
	commitAllStageRecaptureCapture commitAllMutationStage = "recapture_capture"
)

// commitAllHooks is a per-invocation test seam. It deliberately lives on the
// call stack instead of in package globals so parallel CLI tests cannot change
// another invocation's mutation timing.
type commitAllHooks struct {
	BeforeMutationCheck func(context.Context, commitAllMutationStage) error
}

type commitAllMutationGuard struct {
	repo           string
	gitDir         string
	expectedBranch string
	hooks          commitAllHooks
}

func (g commitAllMutationGuard) check(
	ctx context.Context,
	stage commitAllMutationStage,
	expectedHead string,
) error {
	if g.hooks.BeforeMutationCheck != nil {
		if err := g.hooks.BeforeMutationCheck(ctx, stage); err != nil {
			return fmt.Errorf("acd commit-all: %s pre-mutation hook: %w", stage, err)
		}
	}
	return revalidateCommitAllMutationAnchor(ctx, g.repo, g.gitDir, g.expectedBranch, expectedHead)
}

func runCommitAll(ctx context.Context, out io.Writer, in io.Reader, repoFlag string, yes, dryRun, jsonOut bool) error {
	return runCommitAllWithHooks(ctx, out, in, repoFlag, yes, dryRun, jsonOut, commitAllHooks{})
}

func runCommitAllWithHooks(
	ctx context.Context,
	out io.Writer,
	in io.Reader,
	repoFlag string,
	yes, dryRun, jsonOut bool,
	hooks commitAllHooks,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	if jsonOut && !yes && !dryRun {
		return errors.New("acd commit-all: --json requires --yes (no interactive prompt available)")
	}

	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd commit-all: resolve git dir: %w", err)
	}

	branchRef, err := git.RunBranchRef(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd commit-all: resolve HEAD branch: %w", err)
	}
	if branchRef == "" {
		return errors.New("acd commit-all: detached HEAD; checkout a branch before running commit-all")
	}
	if name, active := daemon.GitOperationInProgress(gitDir); active {
		return fmt.Errorf("acd commit-all: refusing while git operation %q is in progress", name)
	}
	if _, present, err := pausepkg.Read(gitDir); err != nil {
		return fmt.Errorf("acd commit-all: read pause marker: %w", err)
	} else if present {
		return fmt.Errorf("acd commit-all: refusing while manual pause marker is present at %s; run `acd resume` first", pausepkg.Path(gitDir))
	}

	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		if errors.Is(err, git.ErrRefNotFound) {
			return errors.New("acd commit-all: no commits on branch yet; create the initial commit first")
		}
		return fmt.Errorf("acd commit-all: resolve HEAD: %w", err)
	}
	if strings.TrimSpace(head) == "" {
		return errors.New("acd commit-all: no commits on branch yet; create the initial commit first")
	}
	guard := commitAllMutationGuard{
		repo:           repo,
		gitDir:         gitDir,
		expectedBranch: branchRef,
		hooks:          hooks,
	}
	dbPath := state.DBPathFromGitDir(gitDir)
	preflight, err := inspectCommitAllPreflight(ctx, repo, dbPath, branchRef, head)
	if err != nil {
		return err
	}
	cfg := ai.LoadProviderConfigFromEnv()
	cfg.CommitStrategy = preflight.Strategy
	estimatedPending := preflight.ExistingCount + preflight.WorktreeChanges
	res := commitAllResult{
		OK:               true,
		Repo:             repo,
		BranchRef:        branchRef,
		HeadBefore:       head,
		Strategy:         string(preflight.Strategy),
		Provider:         commitAllConfiguredProviderName(cfg),
		IntentWindow:     cfg.IntentWindow,
		IntentDeferLim:   cfg.IntentDeferLimit,
		PendingBefore:    estimatedPending,
		EstimatedPass:    commitAllEstimatePasses(preflight.Strategy, estimatedPending, cfg.IntentWindow),
		DryRun:           dryRun,
		PreservedPending: preflight.ExistingCount,
	}
	if preflight.ExistingCount > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"%swould preserve %d pre-existing event(s) across %d exact pair(s) before reseeding shadow",
			commitAllDryRunPrefix(dryRun), preflight.ExistingCount, len(preflight.ExistingPairs),
		))
	}
	if preflight.WorktreeChanges > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"%sread-only worktree estimate found %d changed path(s)",
			commitAllDryRunPrefix(dryRun), preflight.WorktreeChanges,
		))
	}
	if dryRun {
		res.Notes = append(res.Notes, "dry-run: no capture, recovery ref, or replay state was written")
		res.DurationMillis = time.Since(start).Milliseconds()
		return renderCommitAll(out, res, jsonOut)
	}
	if estimatedPending == 0 {
		res.Notes = append(res.Notes, "no pending events; worktree already clean")
		res.HeadAfter = head
		res.DurationMillis = time.Since(start).Milliseconds()
		return renderCommitAll(out, res, jsonOut)
	}
	if !yes {
		ok, err := promptCommitAllConfirm(out, in, res)
		if err != nil {
			return err
		}
		if !ok {
			res.Notes = append(res.Notes, "aborted by user; no state changes were made")
			res.OK = false
			res.DurationMillis = time.Since(start).Milliseconds()
			if rerr := renderCommitAll(out, res, jsonOut); rerr != nil {
				return rerr
			}
			return errCommitAllAborted
		}
	}
	res.Confirmed = true

	// Acquire the daemon lock only after the operator authorizes mutation. Hold
	// it through the state reload and replay so a daemon cannot start between
	// the post-prompt safety checks and capture.
	dlock, err := daemon.AcquireDaemonLock(gitDir)
	if err != nil {
		if errors.Is(err, daemon.ErrDaemonLockHeld) {
			return errors.New("acd commit-all: refusing while the per-repo daemon is alive (stop it first with `acd stop`)")
		}
		return fmt.Errorf("acd commit-all: acquire daemon.lock: %w", err)
	}
	defer func() { _ = dlock.Release() }()
	if err := guard.check(ctx, commitAllStagePostLock, head); err != nil {
		return err
	}

	// Writable state is opened only after dry-run and confirmation paths have
	// returned. Reload the state-derived values so the mutation phase acts on
	// the exact queue and generation it is about to reconcile.
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd commit-all: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()
	gen, err := loadCommitAllGeneration(ctx, db)
	if err != nil {
		return err
	}
	cctx := daemon.CaptureContext{
		BranchRef:        branchRef,
		BranchGeneration: gen,
		BaseHead:         head,
	}
	strategy, err := ResolveEffectiveCommitStrategy(ctx, db.SQL())
	if err != nil {
		return fmt.Errorf("acd commit-all: resolve strategy: %w", err)
	}
	cfg.CommitStrategy = strategy
	res.Strategy = string(strategy)
	provider, providerCloser, err := ai.BuildProvider(cfg)
	if err != nil {
		return fmt.Errorf("acd commit-all: build provider: %w", err)
	}
	if providerCloser != nil {
		defer func() { _ = providerCloser.Close() }()
	}
	res.Provider = ai.PrimaryProviderName(provider)
	if err := guard.check(ctx, commitAllStagePostLock, head); err != nil {
		return err
	}
	existingPairs, err := inspectCommitAllExistingPairs(ctx, db, cctx)
	if err != nil {
		return err
	}
	existingCount := commitAllExistingPairCount(existingPairs)
	res.PreservedPending = existingCount
	res.PendingBefore = existingCount

	// Preserve every pre-existing exact-pair queue before reseeding shadow. The
	// shared reconciler makes each pair reachable independently before active
	// capture begins.
	for _, pair := range existingPairs {
		if err := guard.check(ctx, commitAllStageRecoveryPair, head); err != nil {
			res.DurationMillis = time.Since(start).Milliseconds()
			return finishCommitAllRecoveryError(ctx, out, repo, db, cctx, res, jsonOut, err)
		}
		result, err := reconcileCommitAllExistingPair(ctx, repo, gitDir, db, pair)
		if err != nil {
			res.DurationMillis = time.Since(start).Milliseconds()
			return finishCommitAllRecoveryError(ctx, out, repo, db, cctx, res, jsonOut, err)
		}
		if result.Handled {
			appendCommitAllRecoveryResult(&res, result)
		}
	}

	// commit-all is the "cold start, dirty repo, daemon was off" entrypoint.
	// Users expect the captured events to reflect a diff of live worktree
	// vs HEAD, not a diff vs whatever stale shadow happens to remain from a
	// previous (potentially failed) daemon session. BootstrapShadow is
	// idempotent on the (branch_ref, branch_generation) marker — if a prior
	// capture pass absorbed those edits into shadow without successful
	// replay, the marker is still present and BootstrapShadow returns 0,
	// leaving the next Capture to compare live worktree against a shadow
	// that already mirrors live state and emit zero events. Force a reseed
	// so the diff is always vs HEAD's tree.
	if err := guard.check(ctx, commitAllStageReseed, head); err != nil {
		return err
	}
	if _, err := daemon.ReseedShadowFromHead(ctx, repo, db, cctx); err != nil {
		return fmt.Errorf("acd commit-all: reseed shadow from HEAD: %w", err)
	}
	checker := git.NewIgnoreChecker(repo)
	defer func() { _ = checker.Close() }()
	sensitive := state.NewSensitiveMatcher()
	safeIgnore := state.NewSafeIgnoreMatcher()

	// DisablePendingCap (NOT os.Setenv) lifts the pending-depth cap for
	// this single Capture call. Cold-start dirty trees can otherwise trip
	// the mid-pass cap; using a typed CaptureOpts field keeps the daemon's
	// process-wide invariant (DefaultMaxPendingEvents) untouched and
	// avoids racing the env with concurrent goroutines.
	if err := guard.check(ctx, commitAllStageCapture, head); err != nil {
		return err
	}
	if _, err := daemon.Capture(ctx, repo, db, cctx, daemon.CaptureOpts{
		IgnoreChecker:     checker,
		SensitiveMatcher:  sensitive,
		SafeIgnoreMatcher: safeIgnore,
		GitDir:            gitDir,
		SortByPath:        true,
		DisablePendingCap: true,
	}); err != nil {
		return fmt.Errorf("acd commit-all: capture: %w", err)
	}

	pendingCount, err := countCommitAllUnpublishedPair(ctx, db, cctx)
	if err != nil {
		return fmt.Errorf("acd commit-all: count pending: %w", err)
	}

	res.PendingBefore = pendingCount
	res.EstimatedPass = commitAllEstimatePasses(strategy, pendingCount, cfg.IntentWindow)
	res.Notes = append(res.Notes, "shadow reseeded from HEAD")

	if pendingCount == 0 {
		res.Notes = append(res.Notes, "no pending events; worktree already clean")
		res.DurationMillis = time.Since(start).Milliseconds()
		return renderCommitAll(out, res, jsonOut)
	}

	commits, drained, conflicts, failed, after, err := commitAllReplayLoop(ctx, repo, gitDir, db, cctx, strategy, cfg, provider, pendingCount, &res.Notes, &guard)
	res.Commits = commits
	res.Drained = drained
	res.Conflicts = conflicts
	res.Failed = failed
	res.PendingAfter = after
	res.DurationMillis = time.Since(start).Milliseconds()
	return finishCommitAllReplay(ctx, out, repo, db, cctx, res, jsonOut, err)
}

type commitAllPreflight struct {
	Generation      int64
	Strategy        ai.CommitStrategy
	ExistingPairs   []commitAllExistingPair
	ExistingCount   int
	WorktreeChanges int
}

// inspectCommitAllPreflight gathers enough information to render a dry-run or
// confirmation prompt without opening the writable state handle. A missing
// state DB is a valid cold-start case and contributes no queued events.
func inspectCommitAllPreflight(
	ctx context.Context,
	repo, stateDB, branchRef, head string,
) (commitAllPreflight, error) {
	preflight := commitAllPreflight{Generation: 1}
	strategy, err := ResolveEffectiveCommitStrategy(ctx, nil)
	if err != nil {
		return commitAllPreflight{}, fmt.Errorf("acd commit-all: resolve strategy: %w", err)
	}
	preflight.Strategy = strategy

	if _, statErr := os.Stat(stateDB); statErr == nil {
		conn, openErr := openStateDBReadOnly(ctx, stateDB)
		if openErr != nil {
			return commitAllPreflight{}, fmt.Errorf("acd commit-all: open state.db read-only: %w", openErr)
		}
		defer func() { _ = conn.Close() }()

		preflight.Generation, err = loadCommitAllGenerationSQL(ctx, conn)
		if err != nil {
			return commitAllPreflight{}, err
		}
		cctx := daemon.CaptureContext{
			BranchRef:        branchRef,
			BranchGeneration: preflight.Generation,
			BaseHead:         head,
		}
		preflight.ExistingPairs, err = inspectCommitAllExistingPairsSQL(ctx, conn, cctx)
		if err != nil {
			return commitAllPreflight{}, err
		}
		preflight.ExistingCount = commitAllExistingPairCount(preflight.ExistingPairs)
		preflight.Strategy, err = ResolveEffectiveCommitStrategy(ctx, conn)
		if err != nil {
			return commitAllPreflight{}, fmt.Errorf("acd commit-all: resolve strategy: %w", err)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return commitAllPreflight{}, fmt.Errorf("acd commit-all: stat state.db: %w", statErr)
	}

	preflight.WorktreeChanges, err = estimateCommitAllWorktreeChanges(ctx, repo)
	if err != nil {
		return commitAllPreflight{}, err
	}
	return preflight, nil
}

// estimateCommitAllWorktreeChanges counts tracked worktree differences from
// HEAD plus untracked, non-ignored paths. It reads Git only; capture remains the
// authority for the exact event count after the operator authorizes mutation.
func estimateCommitAllWorktreeChanges(ctx context.Context, repo string) (int, error) {
	tracked, err := git.Run(ctx, git.RunOpts{Dir: repo},
		"diff", "--no-ext-diff", "--name-only", "-z", "HEAD", "--")
	if err != nil {
		return 0, fmt.Errorf("acd commit-all: estimate tracked worktree changes: %w", err)
	}
	untracked, err := git.Run(ctx, git.RunOpts{Dir: repo},
		"ls-files", "--others", "--exclude-standard", "-z", "--")
	if err != nil {
		return 0, fmt.Errorf("acd commit-all: estimate untracked worktree changes: %w", err)
	}

	sensitive := state.NewSensitiveMatcher()
	safeIgnore := state.NewSafeIgnoreMatcher()
	paths := make(map[string]struct{})
	add := func(raw []byte) {
		for _, rel := range strings.Split(string(raw), "\x00") {
			if rel == "" || sensitive.Match(rel) || safeIgnore.MatchFile(rel) {
				continue
			}
			paths[rel] = struct{}{}
		}
	}
	add(tracked)
	add(untracked)
	return len(paths), nil
}

func commitAllDryRunPrefix(dryRun bool) string {
	if dryRun {
		return "dry-run: "
	}
	return ""
}

func commitAllConfiguredProviderName(cfg ai.ProviderConfig) string {
	switch {
	case cfg.Mode == "openai-compat" && cfg.APIKey != "":
		return "openai-compat"
	case strings.HasPrefix(cfg.Mode, "subprocess:") &&
		strings.TrimSpace(strings.TrimPrefix(cfg.Mode, "subprocess:")) != "":
		return cfg.Mode
	default:
		return "deterministic"
	}
}

func revalidateCommitAllMutationAnchor(
	ctx context.Context,
	repo, gitDir, expectedBranch, expectedHead string,
) error {
	branchRef, err := git.RunBranchRef(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd commit-all: recheck HEAD branch: %w", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		return fmt.Errorf("acd commit-all: recheck HEAD: %w", err)
	}
	if branchRef != expectedBranch || head != expectedHead {
		return fmt.Errorf(
			"acd commit-all: refusing because HEAD changed after preflight (%s @ %s -> %s @ %s)",
			expectedBranch, shortenSHA(expectedHead), branchRef, shortenSHA(head),
		)
	}
	if name, active := daemon.GitOperationInProgress(gitDir); active {
		return fmt.Errorf("acd commit-all: refusing while git operation %q is in progress", name)
	}
	if _, present, err := pausepkg.Read(gitDir); err != nil {
		return fmt.Errorf("acd commit-all: recheck pause marker: %w", err)
	} else if present {
		return fmt.Errorf("acd commit-all: refusing while manual pause marker is present at %s; run `acd resume` first", pausepkg.Path(gitDir))
	}
	return nil
}

func loadCommitAllGeneration(ctx context.Context, db *state.DB) (int64, error) {
	return loadCommitAllGenerationSQL(ctx, db.ReadSQL())
}

func loadCommitAllGenerationSQL(ctx context.Context, conn *sql.DB) (int64, error) {
	v, ok, err := metaLookup(ctx, conn, daemon.MetaKeyBranchGeneration)
	if err != nil {
		return 0, fmt.Errorf("acd commit-all: load branch generation: %w", err)
	}
	// Genuinely absent meta = fresh DB; default to generation 1.
	if !ok || strings.TrimSpace(v) == "" {
		return 1, nil
	}
	// Meta is present but unparseable / non-positive: refuse rather than
	// silently fabricating generation=1. The daemon may have advanced the
	// generation past 1 (rebase, divergence, ref switch) and overwriting
	// it would build a parallel shadow the daemon will not consume.
	parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("acd commit-all: branch generation meta %q is not a valid integer; run `acd fix --clear-pause` to repair state", v)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("acd commit-all: branch generation meta %q is non-positive; run `acd fix --clear-pause` to repair state", v)
	}
	return parsed, nil
}

type commitAllExistingPair struct {
	BranchRef  string
	Generation int64
	FirstSeq   int64
	EventCount int
	Active     bool
}

func inspectCommitAllExistingPairs(
	ctx context.Context,
	db *state.DB,
	cctx daemon.CaptureContext,
) ([]commitAllExistingPair, error) {
	return inspectCommitAllExistingPairsSQL(ctx, db.ReadSQL(), cctx)
}

func inspectCommitAllExistingPairsSQL(
	ctx context.Context,
	conn *sql.DB,
	cctx daemon.CaptureContext,
) ([]commitAllExistingPair, error) {
	rows, err := conn.QueryContext(ctx, `
SELECT branch_ref, branch_generation, MIN(seq), COUNT(*)
FROM capture_events
WHERE state IN (?, ?, ?)
GROUP BY branch_ref, branch_generation
ORDER BY MIN(seq) ASC`,
		state.EventStatePending, state.EventStateBlockedConflict, state.EventStateFailed,
	)
	if err != nil {
		return nil, fmt.Errorf("acd commit-all: inspect pre-existing unpublished pairs: %w", err)
	}
	defer rows.Close()
	var pairs []commitAllExistingPair
	for rows.Next() {
		var pair commitAllExistingPair
		if err := rows.Scan(&pair.BranchRef, &pair.Generation, &pair.FirstSeq, &pair.EventCount); err != nil {
			return nil, fmt.Errorf("acd commit-all: scan pre-existing unpublished pair: %w", err)
		}
		pair.Active = pair.BranchRef == cctx.BranchRef && pair.Generation == cctx.BranchGeneration
		pairs = append(pairs, pair)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("acd commit-all: iterate pre-existing unpublished pairs: %w", err)
	}
	return pairs, nil
}

func commitAllExistingPairCount(pairs []commitAllExistingPair) int {
	total := 0
	for _, pair := range pairs {
		total += pair.EventCount
	}
	return total
}

func reconcileCommitAllExistingPair(
	ctx context.Context,
	repo, gitDir string,
	db *state.DB,
	pair commitAllExistingPair,
) (daemon.RecoveryChainResult, error) {
	result, err := daemon.ReconcileUnpublishedChain(ctx, repo, db, daemon.RecoveryReconcileOptions{
		GitDir:           gitDir,
		BranchRef:        pair.BranchRef,
		BranchGeneration: pair.Generation,
		FirstSeq:         pair.FirstSeq,
		Trigger:          "commit_all_preflight",
		InvalidateShadow: pair.Active,
	})
	if err != nil {
		return daemon.RecoveryChainResult{}, fmt.Errorf("acd commit-all: preserve pre-existing unpublished pair %s generation %d: %w", pair.BranchRef, pair.Generation, err)
	}
	return result, nil
}

func countCommitAllUnpublishedPair(ctx context.Context, db *state.DB, cctx daemon.CaptureContext) (int, error) {
	var count int
	if err := db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND state IN (?, ?, ?)`, cctx.BranchRef, cctx.BranchGeneration,
		state.EventStatePending, state.EventStateBlockedConflict, state.EventStateFailed,
	).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func commitAllEstimatePasses(strategy ai.CommitStrategy, pending, window int) int {
	if pending <= 0 {
		return 0
	}
	if strategy == ai.CommitStrategyIntent && window > 0 {
		return int(math.Ceil(float64(pending) / float64(window)))
	}
	return pending
}

func promptCommitAllConfirm(out io.Writer, in io.Reader, res commitAllResult) (bool, error) {
	renderCommitAllConfirmation(out, res)
	fmt.Fprint(out, "Proceed? [y/N]: ")
	if in == nil {
		in = os.Stdin
	}
	reader := bufio.NewReader(in)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("acd commit-all: read confirmation: %w", err)
	}
	answer := strings.TrimSpace(strings.ToLower(line))
	return answer == "y" || answer == "yes", nil
}

func renderCommitAllConfirmation(out io.Writer, res commitAllResult) {
	fmt.Fprintf(out, "Repo: %s (%s @ %s)\n", res.Repo, res.BranchRef, shortenSHA(res.HeadBefore))
	fmt.Fprintf(out, "Pending events: %d\n", res.PendingBefore)
	fmt.Fprintf(out, "Strategy: %s (provider %s)\n", res.Strategy, valueOrUnset(res.Provider))
	if res.Strategy == string(ai.CommitStrategyIntent) {
		fmt.Fprintf(out, "Intent window: %d, defer limit: %d\n", res.IntentWindow, res.IntentDeferLim)
	}
	fmt.Fprintf(out, "Estimated passes: %d\n", res.EstimatedPass)
}

// commitAllReplayer abstracts daemon.Replay so unit tests can inject a
// fake that controls the per-pass return without spinning a real git
// repo. Production calls go through commitAllRunReplayDefault.
type commitAllReplayer func(ctx context.Context, repo string, db *state.DB, cctx daemon.CaptureContext, opts daemon.ReplayOpts) (daemon.ReplaySummary, error)

func commitAllRunReplayDefault(ctx context.Context, repo string, db *state.DB, cctx daemon.CaptureContext, opts daemon.ReplayOpts) (daemon.ReplaySummary, error) {
	return daemon.Replay(ctx, repo, db, cctx, opts)
}

func commitAllReplayLoop(
	ctx context.Context,
	repo, gitDir string,
	db *state.DB,
	cctx daemon.CaptureContext,
	strategy ai.CommitStrategy,
	cfg ai.ProviderConfig,
	provider ai.Provider,
	startingPending int,
	noteSink *[]string,
	guard *commitAllMutationGuard,
) (commits, drained, conflicts, failed, after int, err error) {
	return commitAllReplayLoopWithSafety(ctx, repo, gitDir, db, cctx, strategy, cfg, provider, startingPending, commitAllRunReplayDefault, noteSink, guard)
}

// commitAllReplayLoopWith is the testable form of commitAllReplayLoop;
// production code goes through the public signature above. `replayFn`
// lets unit tests inject deterministic per-pass results; `noteSink`, if
// non-nil, captures Notes appended during the loop (e.g. zero-progress
// escape) so callers don't have to wrap.
func commitAllReplayLoopWith(
	ctx context.Context,
	repo, gitDir string,
	db *state.DB,
	cctx daemon.CaptureContext,
	strategy ai.CommitStrategy,
	cfg ai.ProviderConfig,
	provider ai.Provider,
	startingPending int,
	replayFn commitAllReplayer,
	noteSink *[]string,
) (commits, drained, conflicts, failed, after int, err error) {
	return commitAllReplayLoopWithSafety(
		ctx, repo, gitDir, db, cctx, strategy, cfg, provider,
		startingPending, replayFn, noteSink, nil,
	)
}

func commitAllReplayLoopWithSafety(
	ctx context.Context,
	repo, gitDir string,
	db *state.DB,
	cctx daemon.CaptureContext,
	strategy ai.CommitStrategy,
	cfg ai.ProviderConfig,
	provider ai.Provider,
	startingPending int,
	replayFn commitAllReplayer,
	noteSink *[]string,
	guard *commitAllMutationGuard,
) (commits, drained, conflicts, failed, after int, err error) {
	zeroProgress := 0
	recapturePasses := 0
	prevHead := cctx.BaseHead

	// Provider-driven message fn matches what the daemon's run loop
	// would do for any non-deterministic provider. The daemon's
	// internal providerMessageFn is exposed via daemon.ProviderMessageFn
	// so commit-all routes per-event subjects through the same path.
	var msgFn daemon.MessageFn
	if provider != nil {
		msgFn = daemon.ProviderMessageFn(provider, repo)
	}

	var (
		planner         ai.IntentPlanner
		intentHealth    *daemon.IntentPlannerHealth
		plannerProvider string
		plannerModel    string
	)
	if strategy == ai.CommitStrategyIntent {
		if p, ok := provider.(ai.IntentPlanner); ok {
			planner = p
		} else {
			planner = ai.DeterministicProvider{CommitFormat: cfg.CommitFormat}
		}
		plannerProvider = ai.PrimaryProviderName(planner)
		if plannerProvider == "openai-compat" {
			plannerModel = cfg.Model
		}
	}

	for {
		if guard != nil {
			if safetyErr := guard.check(ctx, commitAllStageReplay, cctx.BaseHead); safetyErr != nil {
				err = safetyErr
				return
			}
		}
		// Construct one circuit for this commit-all process after the first
		// pass safety check succeeds. Reusing it in every ReplayOpts prevents a
		// failing remote planner from being retried on each bounded pass.
		if strategy == ai.CommitStrategyIntent && intentHealth == nil {
			intentHealth = daemon.NewIntentPlannerHealth(ctx, db, daemon.IntentPlannerHealthOptions{
				Provider: daemon.IntentPlannerProviderIdentity{
					Provider:      plannerProvider,
					Model:         plannerModel,
					Endpoint:      cfg.BaseURL,
					Deterministic: plannerProvider == (ai.DeterministicProvider{}).Name(),
				},
			})
		}
		opts := daemon.ReplayOpts{
			GitDir:                gitDir,
			Limit:                 daemon.DefaultReplayLimit,
			MessageFn:             msgFn,
			CommitStrategy:        strategy,
			IntentPlanner:         planner,
			IntentHealth:          intentHealth,
			IntentPlannerProvider: plannerProvider,
			IntentPlannerModel:    plannerModel,
			IntentWindow:          cfg.IntentWindow,
			IntentMinPending:      cfg.IntentMinPending,
			IntentMaxPendingAge:   cfg.IntentMaxPendingAge,
			IntentRecentCommits:   cfg.IntentRecentCommits,
			IntentDeferLimit:      cfg.IntentDeferLimit,
			IntentBypassBatchWait: true,
		}
		sum, rerr := replayFn(ctx, repo, db, cctx, opts)
		commits += sum.Published
		conflicts += sum.Conflicts
		failed += sum.Failed
		if rerr != nil {
			err = fmt.Errorf("acd commit-all: replay: %w", rerr)
			return
		}
		// Refresh BaseHead so the next pass sees the just-committed HEAD.
		// A failure here is fatal: a stale BaseHead would target the
		// wrong CAS base and produce spurious supersede / conflict
		// outcomes on the next pass.
		newHead, herr := git.RevParse(ctx, repo, "HEAD")
		if herr != nil {
			err = fmt.Errorf("acd commit-all: refresh HEAD between passes: %w", herr)
			return
		}
		cctx.BaseHead = newHead
		if sum.RecaptureRequired {
			recapturePasses++
			if recapturePasses > commitAllRecaptureLimit {
				err = fmt.Errorf("acd commit-all: recovery recapture did not converge after %d passes; captured work remains protected in recovery refs", commitAllRecaptureLimit)
				return
			}
			captureSummary, captureErr := recaptureCommitAllWorktreeWithSafety(ctx, repo, gitDir, db, cctx, guard)
			if captureErr != nil {
				err = captureErr
				return
			}
			if noteSink != nil {
				*noteSink = append(*noteSink, fmt.Sprintf(
					"recovery invalidated shadow; recaptured %d event(s) before continuing",
					captureSummary.EventsAppended))
			}
		}

		remaining, perr := countCommitAllUnpublishedPair(ctx, db, cctx)
		if perr != nil {
			err = fmt.Errorf("acd commit-all: count pending after pass: %w", perr)
			return
		}
		after = remaining

		if sum.RecaptureRequired {
			zeroProgress = 0
		} else if sum.Published == 0 && cctx.BaseHead == prevHead {
			zeroProgress++
		} else {
			zeroProgress = 0
		}
		prevHead = cctx.BaseHead

		if after == 0 {
			break
		}
		if zeroProgress >= commitAllZeroProgressLimit {
			if noteSink != nil {
				*noteSink = append(*noteSink, fmt.Sprintf("replay made no progress for %d consecutive passes; stopping with %d pending", commitAllZeroProgressLimit, after))
			}
			break
		}
		if !sum.RecaptureRequired && (sum.Conflicts > 0 || sum.Failed > 0) {
			break
		}
	}
	if startingPending > 0 {
		drained = startingPending - after
		if drained < 0 {
			drained = 0
		}
	}
	return
}

func recaptureCommitAllWorktreeWithSafety(
	ctx context.Context,
	repo, gitDir string,
	db *state.DB,
	cctx daemon.CaptureContext,
	guard *commitAllMutationGuard,
) (daemon.CaptureSummary, error) {
	if guard != nil {
		if err := guard.check(ctx, commitAllStageRecaptureReseed, cctx.BaseHead); err != nil {
			return daemon.CaptureSummary{}, err
		}
	}
	if _, err := daemon.ReseedShadowFromHead(ctx, repo, db, cctx); err != nil {
		return daemon.CaptureSummary{}, fmt.Errorf("acd commit-all: reseed shadow after recovery: %w", err)
	}
	checker := git.NewIgnoreChecker(repo)
	defer func() { _ = checker.Close() }()
	if guard != nil {
		if err := guard.check(ctx, commitAllStageRecaptureCapture, cctx.BaseHead); err != nil {
			return daemon.CaptureSummary{}, err
		}
	}
	summary, err := daemon.Capture(ctx, repo, db, cctx, daemon.CaptureOpts{
		IgnoreChecker:     checker,
		SensitiveMatcher:  state.NewSensitiveMatcher(),
		SafeIgnoreMatcher: state.NewSafeIgnoreMatcher(),
		GitDir:            gitDir,
		SortByPath:        true,
		DisablePendingCap: true,
	})
	if err != nil {
		return daemon.CaptureSummary{}, fmt.Errorf("acd commit-all: recapture after recovery: %w", err)
	}
	return summary, nil
}

func finishCommitAll(out io.Writer, res commitAllResult, jsonOut bool) error {
	if res.PendingAfter == 0 {
		return renderCommitAll(out, res, jsonOut)
	}

	res.OK = false
	res.Incomplete = true
	res.Notes = append(res.Notes, fmt.Sprintf(
		"commit-all stopped incomplete with pending=%d conflicts=%d failed=%d; captured work remains protected",
		res.PendingAfter, res.Conflicts, res.Failed,
	))
	if err := renderCommitAll(out, res, jsonOut); err != nil {
		return err
	}
	return &commitAllIncompleteError{
		PendingAfter: res.PendingAfter,
		Conflicts:    res.Conflicts,
		Failed:       res.Failed,
	}
}

func appendCommitAllRecoveryResult(res *commitAllResult, result daemon.RecoveryChainResult) {
	if res == nil {
		return
	}
	res.RecoveryRefs = append(res.RecoveryRefs, result.RecoveryRef)
	res.Notes = append(res.Notes, fmt.Sprintf(
		"preserved %d pre-existing event(s) as %s at %s",
		result.EventCount, result.Outcome, result.RecoveryRef))
}

func finishCommitAllRecoveryError(
	ctx context.Context,
	out io.Writer,
	repo string,
	db *state.DB,
	cctx daemon.CaptureContext,
	res commitAllResult,
	jsonOut bool,
	recoveryErr error,
) error {
	pairs, err := inspectCommitAllExistingPairs(ctx, db, cctx)
	if err != nil {
		res.Notes = append(res.Notes, fmt.Sprintf("post-error unpublished recount failed: %v", err))
	} else {
		res.PendingAfter = commitAllExistingPairCount(pairs)
		if res.PendingBefore >= res.PendingAfter {
			res.Drained = res.PendingBefore - res.PendingAfter
		}
	}
	if newHead, err := git.RevParse(ctx, repo, "HEAD"); err == nil {
		res.HeadAfter = newHead
	} else {
		res.Notes = append(res.Notes, fmt.Sprintf("post-error HEAD lookup failed: %v", err))
	}
	res.OK = false
	res.Incomplete = true
	res.Error = recoveryErr.Error()
	res.Notes = append(res.Notes,
		"pre-existing chain recovery stopped with an error; completed recovery refs remain valid and remaining captures are unchanged")
	if err := renderCommitAll(out, res, jsonOut); err != nil {
		return err
	}
	return &commitAllRecoveryError{Cause: recoveryErr, PendingAfter: res.PendingAfter}
}

func finishCommitAllReplay(
	ctx context.Context,
	out io.Writer,
	repo string,
	db *state.DB,
	cctx daemon.CaptureContext,
	res commitAllResult,
	jsonOut bool,
	replayErr error,
) error {
	if replayErr != nil {
		pending, err := countCommitAllUnpublishedPair(ctx, db, cctx)
		if err != nil {
			res.Notes = append(res.Notes, fmt.Sprintf("post-error pending recount failed: %v", err))
		} else {
			res.PendingAfter = pending
			if res.PendingBefore >= pending {
				res.Drained = res.PendingBefore - pending
			}
		}
		res.OK = false
		res.Incomplete = true
		res.Error = replayErr.Error()
		res.Notes = append(res.Notes, "replay stopped with an error; captured work remains protected")
	}
	if newHead, err := git.RevParse(ctx, repo, "HEAD"); err == nil {
		res.HeadAfter = newHead
	} else {
		slog.Default().Warn("acd commit-all: post-loop HEAD lookup failed", slog.String("err", err.Error()))
		res.Notes = append(res.Notes, fmt.Sprintf("post-loop HEAD lookup failed: %v", err))
	}
	if replayErr == nil {
		return finishCommitAll(out, res, jsonOut)
	}
	if err := renderCommitAll(out, res, jsonOut); err != nil {
		return err
	}
	return &commitAllReplayError{Cause: replayErr, PendingAfter: res.PendingAfter}
}

func renderCommitAll(out io.Writer, res commitAllResult, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if res.DryRun {
		fmt.Fprintf(out, "commit-all DRY RUN for %s (%s @ %s)\n", res.Repo, res.BranchRef, shortenSHA(res.HeadBefore))
	} else if res.Incomplete {
		fmt.Fprintf(out, "commit-all incomplete for %s (%s)\n", res.Repo, res.BranchRef)
	} else if !res.OK {
		fmt.Fprintf(out, "commit-all stopped for %s (%s)\n", res.Repo, res.BranchRef)
	} else {
		fmt.Fprintf(out, "commit-all complete for %s (%s)\n", res.Repo, res.BranchRef)
	}
	fmt.Fprintf(out, "Strategy: %s (provider %s)\n", res.Strategy, valueOrUnset(res.Provider))
	fmt.Fprintf(out, "Pending: before=%d after=%d\n", res.PendingBefore, res.PendingAfter)
	if !res.DryRun {
		fmt.Fprintf(out, "Commits: %d (drained=%d)\n", res.Commits, res.Drained)
		if res.Conflicts > 0 || res.Failed > 0 {
			fmt.Fprintf(out, "Issues: conflicts=%d failed=%d (use `acd diagnose` to inspect)\n", res.Conflicts, res.Failed)
		}
		if res.HeadAfter != "" && res.HeadAfter != res.HeadBefore {
			fmt.Fprintf(out, "HEAD: %s -> %s\n", shortenSHA(res.HeadBefore), shortenSHA(res.HeadAfter))
		}
	}
	for _, note := range res.Notes {
		fmt.Fprintf(out, "- %s\n", note)
	}
	fmt.Fprintf(out, "Duration: %s\n", formatDurationCompact(time.Duration(res.DurationMillis)*time.Millisecond))
	return nil
}

func shortenSHA(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "(none)"
	}
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

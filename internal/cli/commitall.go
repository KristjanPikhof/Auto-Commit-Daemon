package cli

import (
	"bufio"
	"context"
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

// commitAllResult is the JSON payload returned by `acd commit-all --json`.
type commitAllResult struct {
	OK             bool     `json:"ok"`
	Repo           string   `json:"repo"`
	BranchRef      string   `json:"branch_ref"`
	HeadBefore     string   `json:"head_before"`
	HeadAfter      string   `json:"head_after,omitempty"`
	Strategy       string   `json:"strategy"`
	Provider       string   `json:"provider"`
	IntentWindow   int      `json:"intent_window,omitempty"`
	IntentDeferLim int      `json:"intent_defer_limit,omitempty"`
	PendingBefore  int      `json:"pending_before"`
	PendingAfter   int      `json:"pending_after"`
	EstimatedPass  int      `json:"estimated_passes"`
	Commits        int      `json:"commits"`
	Drained        int      `json:"drained"`
	Conflicts      int      `json:"conflicts,omitempty"`
	Failed         int      `json:"failed,omitempty"`
	DryRun         bool     `json:"dry_run,omitempty"`
	Confirmed      bool     `json:"confirmed,omitempty"`
	DurationMillis int64    `json:"duration_ms"`
	Notes          []string `json:"notes,omitempty"`
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

Refuses to run on detached HEAD, while a git operation is in progress
(rebase/merge/cherry-pick/bisect), while a manual pause marker is present,
or while the per-repo daemon is alive.`,
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

const commitAllZeroProgressLimit = 3

func runCommitAll(ctx context.Context, out io.Writer, in io.Reader, repoFlag string, yes, dryRun, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()

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

	// daemon.lock acquire is the live-daemon refusal gate: if the daemon
	// owns the lock our acquire fails with ErrDaemonLockHeld. We hold it
	// across the entire run so a daemon that starts mid-flight cannot
	// race the capture/replay loop. The previous control.lock
	// acquire+release dance was no-op (released before any work) and has
	// been dropped — daemon.lock already covers the only real exclusion.
	dlock, err := daemon.AcquireDaemonLock(gitDir)
	if err != nil {
		if errors.Is(err, daemon.ErrDaemonLockHeld) {
			return errors.New("acd commit-all: refusing while the per-repo daemon is alive (stop it first with `acd stop`)")
		}
		return fmt.Errorf("acd commit-all: acquire daemon.lock: %w", err)
	}
	defer func() { _ = dlock.Release() }()

	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd commit-all: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

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
	gen, err := loadCommitAllGeneration(ctx, db)
	if err != nil {
		return err
	}
	cctx := daemon.CaptureContext{
		BranchRef:        branchRef,
		BranchGeneration: gen,
		BaseHead:         head,
	}

	if _, err := daemon.BootstrapShadow(ctx, repo, db, cctx); err != nil {
		return fmt.Errorf("acd commit-all: bootstrap shadow: %w", err)
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

	pending, err := state.PendingEvents(ctx, db, 0)
	if err != nil {
		return fmt.Errorf("acd commit-all: count pending: %w", err)
	}
	pendingCount := len(pending)

	strategy, err := ResolveEffectiveCommitStrategy(ctx, db.SQL())
	if err != nil {
		return fmt.Errorf("acd commit-all: resolve strategy: %w", err)
	}
	cfg := ai.LoadProviderConfigFromEnv()
	cfg.CommitStrategy = strategy
	provider, providerCloser, err := ai.BuildProvider(cfg)
	if err != nil {
		return fmt.Errorf("acd commit-all: build provider: %w", err)
	}
	if providerCloser != nil {
		defer func() { _ = providerCloser.Close() }()
	}

	estimated := commitAllEstimatePasses(strategy, pendingCount, cfg.IntentWindow)

	res := commitAllResult{
		OK:             true,
		Repo:           repo,
		BranchRef:      branchRef,
		HeadBefore:     head,
		Strategy:       string(strategy),
		Provider:       ai.PrimaryProviderName(provider),
		IntentWindow:   cfg.IntentWindow,
		IntentDeferLim: cfg.IntentDeferLimit,
		PendingBefore:  pendingCount,
		EstimatedPass:  estimated,
		DryRun:         dryRun,
	}

	if pendingCount == 0 {
		res.Notes = append(res.Notes, "no pending events; worktree already clean")
		res.DurationMillis = time.Since(start).Milliseconds()
		return renderCommitAll(out, res, jsonOut)
	}

	if dryRun {
		previewIntentDryRun(ctx, repo, db, cctx, strategy, cfg, provider, &res)
		res.DurationMillis = time.Since(start).Milliseconds()
		return renderCommitAll(out, res, jsonOut)
	}

	if !yes {
		if jsonOut {
			return errors.New("acd commit-all: --json requires --yes (no interactive prompt available)")
		}
		ok, err := promptCommitAllConfirm(out, in, res)
		if err != nil {
			return err
		}
		if !ok {
			res.Notes = append(res.Notes, "aborted by user")
			res.OK = false
			res.DurationMillis = time.Since(start).Milliseconds()
			if rerr := renderCommitAll(out, res, jsonOut); rerr != nil {
				return rerr
			}
			// Return a sentinel error so cobra exits non-zero. Scripts
			// can grep for "aborted by user" in JSON output and rely
			// on the exit code matching the human signal.
			return errCommitAllAborted
		}
	}
	res.Confirmed = true

	commits, drained, conflicts, failed, after, err := commitAllReplayLoop(ctx, repo, gitDir, db, cctx, strategy, cfg, provider, pendingCount)
	if err != nil {
		return err
	}
	res.Commits = commits
	res.Drained = drained
	res.Conflicts = conflicts
	res.Failed = failed
	res.PendingAfter = after
	if newHead, herr := git.RevParse(ctx, repo, "HEAD"); herr == nil {
		res.HeadAfter = newHead
	} else {
		slog.Default().Warn("acd commit-all: post-loop HEAD lookup failed", slog.String("err", herr.Error()))
		res.Notes = append(res.Notes, fmt.Sprintf("post-loop HEAD lookup failed: %v", herr))
	}
	res.DurationMillis = time.Since(start).Milliseconds()
	return renderCommitAll(out, res, jsonOut)
}

func loadCommitAllGeneration(ctx context.Context, db *state.DB) (int64, error) {
	v, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyBranchGeneration)
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
		return 0, fmt.Errorf("acd commit-all: branch generation meta %q is not a valid integer; run `acd recover --auto` to repair state", v)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("acd commit-all: branch generation meta %q is non-positive; run `acd recover --auto` to repair state", v)
	}
	return parsed, nil
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
) (commits, drained, conflicts, failed, after int, err error) {
	return commitAllReplayLoopWith(ctx, repo, gitDir, db, cctx, strategy, cfg, provider, startingPending, commitAllRunReplayDefault, nil)
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
	zeroProgress := 0
	prevHead := cctx.BaseHead

	// Provider-driven message fn matches what the daemon's run loop
	// would do for any non-deterministic provider. The daemon's
	// internal providerMessageFn is exposed via daemon.ProviderMessageFn
	// so commit-all routes per-event subjects through the same path.
	var msgFn daemon.MessageFn
	if provider != nil {
		msgFn = daemon.ProviderMessageFn(provider, repo)
	}

	var planner ai.IntentPlanner
	if strategy == ai.CommitStrategyIntent {
		if p, ok := provider.(ai.IntentPlanner); ok {
			planner = p
		}
	}

	for {
		opts := daemon.ReplayOpts{
			GitDir:                gitDir,
			Limit:                 daemon.DefaultReplayLimit,
			MessageFn:             msgFn,
			CommitStrategy:        strategy,
			IntentPlanner:         planner,
			IntentWindow:          cfg.IntentWindow,
			IntentMinPending:      cfg.IntentMinPending,
			IntentMaxPendingAge:   cfg.IntentMaxPendingAge,
			IntentRecentCommits:   cfg.IntentRecentCommits,
			IntentDeferLimit:      cfg.IntentDeferLimit,
			IntentBypassBatchWait: true,
		}
		sum, rerr := replayFn(ctx, repo, db, cctx, opts)
		if rerr != nil {
			err = fmt.Errorf("acd commit-all: replay: %w", rerr)
			return
		}
		commits += sum.Published
		conflicts += sum.Conflicts
		failed += sum.Failed
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

		remaining, perr := state.PendingEvents(ctx, db, 0)
		if perr != nil {
			err = fmt.Errorf("acd commit-all: count pending after pass: %w", perr)
			return
		}
		after = len(remaining)

		if sum.Published == 0 && cctx.BaseHead == prevHead {
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
		if sum.Conflicts > 0 || sum.Failed > 0 {
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

func previewIntentDryRun(
	ctx context.Context,
	repo string,
	db *state.DB,
	cctx daemon.CaptureContext,
	strategy ai.CommitStrategy,
	cfg ai.ProviderConfig,
	provider ai.Provider,
	res *commitAllResult,
) {
	res.Notes = append(res.Notes, fmt.Sprintf("dry-run: %d events would be processed", res.PendingBefore))
	if strategy != ai.CommitStrategyIntent {
		return
	}
	planner, ok := provider.(ai.IntentPlanner)
	if !ok {
		res.Notes = append(res.Notes, "dry-run: provider does not implement intent planning; would fall back to deterministic single-event grouping")
		return
	}
	// Refuse to call a network provider during dry-run. Users reasonably
	// expect --dry-run to be airgapped; the planner request still leaks
	// captured paths/ops to the configured AI endpoint even though no
	// diff egress is involved. Skip the planner peek when the provider
	// declares NeedsDiff (network-bound) or is anything other than the
	// always-local deterministic provider.
	if ai.ProviderNeedsDiff(provider) || cfg.Provider != ai.ProviderDeterministic {
		res.Notes = append(res.Notes, fmt.Sprintf("dry-run: planner peek skipped (network provider %q; would call out otherwise)", ai.PrimaryProviderName(provider)))
		return
	}
	pending, err := state.PendingEvents(ctx, db, cfg.IntentWindow)
	if err != nil || len(pending) == 0 {
		return
	}
	offered := make([]ai.OfferedCapture, 0, len(pending))
	for _, ev := range pending {
		offered = append(offered, ai.OfferedCapture{
			Seq:       ev.Seq,
			Path:      ev.Path,
			Op:        ev.Operation,
			Timestamp: time.Unix(0, int64(ev.CapturedTS*1e9)),
			Fidelity:  ev.Fidelity,
		})
	}
	req, rerr := ai.NewIntentPlanRequest(ai.IntentPlanRequestOptions{OfferedCaptures: offered})
	if rerr != nil {
		res.Notes = append(res.Notes, fmt.Sprintf("dry-run: build planner request: %v", rerr))
		return
	}
	plan, perr := planner.PlanIntent(ctx, req)
	if perr != nil {
		res.Notes = append(res.Notes, fmt.Sprintf("dry-run: planner preview failed: %v", perr))
		return
	}
	if len(plan.SelectedSeqs) > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf("dry-run: planner would select %d capture(s) for the next commit", len(plan.SelectedSeqs)))
	}
	if len(plan.DeferredSeqs) > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf("dry-run: planner would defer %d capture(s)", len(plan.DeferredSeqs)))
	}
	if subj := strings.TrimSpace(plan.Subject); subj != "" {
		res.Notes = append(res.Notes, "dry-run: planner subject preview: "+subj)
	}
}

func renderCommitAll(out io.Writer, res commitAllResult, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if res.DryRun {
		fmt.Fprintf(out, "commit-all DRY RUN for %s (%s @ %s)\n", res.Repo, res.BranchRef, shortenSHA(res.HeadBefore))
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

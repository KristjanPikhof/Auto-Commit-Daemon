package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type productCommitAllResult struct {
	Repo               string  `json:"repo"`
	CheckpointID       string  `json:"checkpoint_id,omitempty"`
	DrainID            string  `json:"drain_id,omitempty"`
	Phase              string  `json:"phase,omitempty"`
	Protected          bool    `json:"protected"`
	PublicationDrained bool    `json:"publication_drained"`
	TargetEventSeq     int64   `json:"target_event_seq"`
	TargetEvents       int64   `json:"target_events"`
	PublishedEvents    int64   `json:"published_events"`
	RemainingEvents    int64   `json:"remaining_events"`
	RecoveredEvents    int64   `json:"recovered_events,omitempty"`
	TerminalEvents     int64   `json:"terminal_events,omitempty"`
	CommitsCreated     int64   `json:"commits_created"`
	WaitingReason      string  `json:"waiting_reason,omitempty"`
	FallbackMode       string  `json:"fallback_mode,omitempty"`
	LastError          string  `json:"last_error,omitempty"`
	SemanticAttempts   int64   `json:"semantic_rebuild_attempts,omitempty"`
	EventFallbackCount int64   `json:"event_fallback_count,omitempty"`
	LastProgressTS     float64 `json:"last_progress_ts,omitempty"`
	StagedConsumed     bool    `json:"staged_consumed,omitempty"`
	WorktreeChanges    int     `json:"worktree_changes,omitempty"`
	PendingEvents      int     `json:"pending_events,omitempty"`
	DryRun             bool    `json:"dry_run,omitempty"`
}

func newProductCommitAllCmd() *cobra.Command {
	var yes, dryRun bool
	cmd := &cobra.Command{
		Use:   "commit-all",
		Short: "Checkpoint and publish all current changes",
		Long: `Checkpoint all current eligible changes durably, then ask the managed
worker to publish the protected target as normal local Git commits.

The target is frozen at the checkpoint barrier, so later edits do not keep the
command running forever. Event mode preserves one capture per commit; Intent
mode may publish several atomic commits. This command never creates one giant
commit merely because it is named commit-all.`,
		Example: `  acd commit-all --dry-run
  acd commit-all --yes
  acd commit-all --repo /path/to/repo --yes --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			quiet, _ := cmd.Flags().GetBool("quiet")
			return runProductCommitAll(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(),
				cmd.InOrStdin(), repo, yes, dryRun, jsonOut, quiet)
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Apply without an interactive confirmation")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview without checkpointing or publishing")
	return cmd
}

func runProductCommitAll(
	ctx context.Context,
	out, progressOut io.Writer,
	in io.Reader,
	repoFlag string,
	yes, dryRun, jsonOut, quiet bool,
) error {
	if jsonOut && !yes && !dryRun {
		return invalidCommandError("acd commit-all: --json apply requires --yes")
	}
	if !yes && !dryRun && !productCommitAllInteractive(in) {
		return invalidCommandError("acd commit-all: non-interactive apply requires --yes")
	}
	lookup, err := loadControlRepo(ctx, repoFlag)
	if err != nil {
		return controlWorktreeError("commit-all", repoFlag, err)
	}
	if !lookup.Registered || lookup.Record.RepositoryID == "" || lookup.Record.WorktreeID == "" {
		return actionRequiredError("setup_required", "acd commit-all: run `acd setup` before publishing")
	}
	status, err := inspectControl(ctx, lookup.Worktree.Root)
	if err != nil {
		return err
	}
	changes, err := productWorktreeChangeCount(ctx, lookup.Worktree.Root)
	if err != nil {
		return fmt.Errorf("acd commit-all: inspect worktree: %w", err)
	}
	preview := productCommitAllResult{
		Repo: lookup.Worktree.Root, WorktreeChanges: changes,
		PendingEvents: status.PendingEvents, DryRun: dryRun,
	}
	if dryRun {
		return renderProductCommitAll(out, preview, productStateWaiting, jsonOut)
	}
	if !yes {
		fmt.Fprintf(out, "Checkpoint and publish %d changed path(s) with %d pending event(s)? [y/N] ",
			changes, status.PendingEvents)
		answer, readErr := bufio.NewReader(in).ReadString('\n')
		if readErr != nil && strings.TrimSpace(answer) == "" {
			return readErr
		}
		if normalized := strings.ToLower(strings.TrimSpace(answer)); normalized != "y" && normalized != "yes" {
			return invalidCommandError("acd commit-all: cancelled; no changes were made")
		}
	}

	params, _ := json.Marshal(map[string]any{
		"kind": "checkpoint", "drain_publication": true,
		"consume_staged": true,
	})
	type callResult struct {
		result productCommitAllResult
		err    error
	}
	resultCh := make(chan callResult, 1)
	startedAt := time.Now()
	go func() {
		result, callErr := startProductPublicationDrain(
			ctx, lookup, params, startedAt)
		resultCh <- callResult{result: result, err: callErr}
	}()

	if !quiet && !jsonOut {
		fmt.Fprintln(progressOut, "Commit all: checkpointing current changes")
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var call callResult
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case call = <-resultCh:
			if call.err != nil {
				var commandErr *CommandError
				if errors.As(call.err, &commandErr) && commandErr.Code == "publication_needs_action" {
					return actionRequiredError(commandErr.Code, commandErr.Message)
				}
				return fmt.Errorf("acd commit-all: %w", call.err)
			}
			goto completed
		case <-ticker.C:
			if quiet || jsonOut {
				continue
			}
			statusResponse, statusErr := callSupervisor(
				ctx, lookup, "publication_drain_status", nil, 2*time.Second)
			if statusErr == nil {
				projection, decodeErr := decodeProductData[state.PublicationDrainReadOnlyProjection](statusResponse.Data)
				if decodeErr == nil && projection.Latest != nil {
					drain := projection.Latest
					remaining := drain.TargetEventCount - drain.PublishedEventCount
					fmt.Fprintf(progressOut,
						"Commit all: phase=%s remaining=%d commits=%d fallback=%s error=%s\n",
						drain.Phase, remaining, drain.CommitCount,
						commitAllValueOr(drain.FallbackMode, "none"),
						commitAllValueOr(drain.LastError, "none"))
					continue
				}
			}
			current, inspectErr := inspectControl(ctx, lookup.Worktree.Root)
			if inspectErr == nil {
				fmt.Fprintf(progressOut,
					"Commit all: protected=%s pending=%d; starting durable drain\n",
					yesNo(current.Protected), current.PendingEvents)
			}
		}
	}

completed:
	result := call.result
	result.Repo = lookup.Worktree.Root
	if !result.PublicationDrained {
		result, err = waitForProductPublicationDrain(
			ctx, lookup, result, progressOut, quiet || jsonOut)
		if err != nil {
			return err
		}
	}
	return finishProductCommitAll(out, result, jsonOut)
}

func startProductPublicationDrain(
	ctx context.Context,
	lookup controlRepoLookup,
	params json.RawMessage,
	startedAt time.Time,
) (productCommitAllResult, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		response, err := callSupervisor(ctx, lookup, "publication_drain_start", params,
			supervisor.CheckpointBarrierTimeout)
		if err == nil {
			result, decodeErr := decodeProductData[productCommitAllResult](response.Data)
			if decodeErr != nil {
				return productCommitAllResult{}, fmt.Errorf(
					"acd commit-all: decode worker result: %w", decodeErr)
			}
			return result, nil
		}
		lastErr = err
		var commandErr *CommandError
		if errors.As(err, &commandErr) && !commandErr.Retryable {
			return productCommitAllResult{}, err
		}
		if result, found := reconnectProductPublicationDrain(
			ctx, lookup, startedAt); found {
			return result, nil
		}
		if _, restartErr := callSupervisor(
			ctx, lookup, "restart_repository", nil, 30*time.Second); restartErr != nil {
			lastErr = errors.Join(lastErr, restartErr)
			continue
		}
		if readyErr := waitControlWorkerReady(
			ctx, lookup, 15*time.Second); readyErr != nil {
			lastErr = errors.Join(lastErr, readyErr)
			continue
		}
		if result, found := reconnectProductPublicationDrain(
			ctx, lookup, startedAt); found {
			return result, nil
		}
	}
	return productCommitAllResult{}, fmt.Errorf(
		"acd commit-all: worker recovery did not restore the durable publication drain: %w",
		lastErr)
}

func reconnectProductPublicationDrain(
	ctx context.Context,
	lookup controlRepoLookup,
	startedAt time.Time,
) (productCommitAllResult, bool) {
	response, err := callSupervisor(
		ctx, lookup, "publication_drain_status", nil, 2*time.Second)
	if err != nil {
		return productCommitAllResult{}, false
	}
	projection, err := decodeProductData[state.PublicationDrainReadOnlyProjection](
		response.Data)
	if err != nil {
		return productCommitAllResult{}, false
	}
	drain := selectReconnectPublicationDrain(
		projection, lookup.Record.WorktreeID, startedAt)
	if drain == nil {
		return productCommitAllResult{}, false
	}
	return productCommitAllResultFromDrain(*drain), true
}

func selectReconnectPublicationDrain(
	projection state.PublicationDrainReadOnlyProjection,
	worktreeID string,
	startedAt time.Time,
) *state.PublicationDrain {
	var matches []state.PublicationDrain
	for _, drain := range projection.Active {
		if drain.WorktreeID == worktreeID {
			matches = append(matches, drain)
		}
	}
	var drain *state.PublicationDrain
	for index := range matches {
		if matches[index].CreatedTS >= float64(startedAt.Add(-time.Second).UnixNano())/1e9 &&
			(drain == nil || matches[index].CreatedTS > drain.CreatedTS) {
			drain = &matches[index]
		}
	}
	if drain == nil && projection.Latest != nil &&
		projection.Latest.WorktreeID == worktreeID &&
		projection.Latest.CreatedTS >= float64(startedAt.Add(-time.Second).UnixNano())/1e9 {
		drain = projection.Latest
	}
	return drain
}

func productCommitAllResultFromDrain(drain state.PublicationDrain) productCommitAllResult {
	var maxSeq int64
	for _, seq := range drain.EventSeqs {
		maxSeq = max(maxSeq, seq)
	}
	return productCommitAllResult{
		CheckpointID: drain.CheckpointID, DrainID: drain.ID, Phase: drain.Phase,
		Protected: true, PublicationDrained: drain.Phase == state.PublicationDrainCompleted,
		TargetEventSeq: maxSeq, TargetEvents: drain.TargetEventCount,
		PublishedEvents: drain.PublishedEventCount,
		RemainingEvents: drain.TargetEventCount - drain.PublishedEventCount,
		CommitsCreated:  drain.CommitCount, WaitingReason: drain.LastError,
		FallbackMode: drain.FallbackMode, LastError: drain.LastError,
		SemanticAttempts:   drain.SemanticRebuildAttempts,
		EventFallbackCount: drain.EventFallbackCount,
		LastProgressTS:     drain.LastProgressTS, StagedConsumed: drain.StagedConsumed,
	}
}

func waitForProductPublicationDrain(
	ctx context.Context,
	lookup controlRepoLookup,
	result productCommitAllResult,
	progressOut io.Writer,
	quiet bool,
) (productCommitAllResult, error) {
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	var latest *state.PublicationDrain
	var lastStatusErr error
	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-report.C:
			if quiet {
				continue
			}
			if latest == nil {
				fmt.Fprintf(progressOut,
					"Commit all: reconnecting to durable drain %s: %v\n",
					result.DrainID, lastStatusErr)
				continue
			}
			remaining := latest.TargetEventCount - latest.PublishedEventCount
			fmt.Fprintf(progressOut,
				"Commit all: phase=%s remaining=%d commits=%d fallback=%s error=%s\n",
				latest.Phase, remaining, latest.CommitCount,
				commitAllValueOr(latest.FallbackMode, "none"),
				commitAllValueOr(latest.LastError, "none"))
		case <-poll.C:
			response, err := callSupervisor(
				ctx, lookup, "publication_drain_status", nil, 2*time.Second)
			if err != nil {
				lastStatusErr = err
				continue
			}
			projection, err := decodeProductData[state.PublicationDrainReadOnlyProjection](
				response.Data)
			if err != nil {
				lastStatusErr = err
				continue
			}
			latest = productPublicationDrainByID(projection, result.DrainID)
			if latest == nil {
				lastStatusErr = errors.New("durable publication drain is not visible yet")
				continue
			}
			lastStatusErr = nil
			result.Phase = latest.Phase
			result.TargetEvents = latest.TargetEventCount
			result.PublishedEvents = latest.PublishedEventCount
			result.RemainingEvents = latest.TargetEventCount - latest.PublishedEventCount
			result.CommitsCreated = latest.CommitCount
			result.FallbackMode = latest.FallbackMode
			result.LastError = latest.LastError
			result.SemanticAttempts = latest.SemanticRebuildAttempts
			result.EventFallbackCount = latest.EventFallbackCount
			result.LastProgressTS = latest.LastProgressTS
			result.StagedConsumed = latest.StagedConsumed
			switch latest.Phase {
			case state.PublicationDrainCompleted:
				result.PublicationDrained = true
				result.RemainingEvents = 0
				return result, nil
			case state.PublicationDrainNeedsAction:
				return result, actionRequiredError(
					"publication_needs_action", latest.LastError)
			}
		}
	}
}

func productPublicationDrainByID(
	projection state.PublicationDrainReadOnlyProjection,
	id string,
) *state.PublicationDrain {
	if projection.Latest != nil && projection.Latest.ID == id {
		return projection.Latest
	}
	for index := range projection.Active {
		if projection.Active[index].ID == id {
			return &projection.Active[index]
		}
	}
	return nil
}

func commitAllValueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func finishProductCommitAll(out io.Writer, result productCommitAllResult, jsonOut bool) error {
	stateName := productStateProtected
	if !result.PublicationDrained {
		stateName = productStateWaiting
	}
	if err := renderProductCommitAll(out, result, stateName, jsonOut); err != nil {
		return err
	}
	if !result.PublicationDrained {
		return &CommandError{
			Code: "publication_incomplete", Message: "acd commit-all: target publication is incomplete",
			Exit: ExitActionRequired, rendered: true,
		}
	}
	return nil
}

func productCommitAllInteractive(in io.Reader) bool {
	file, ok := in.(*os.File)
	return ok && (isatty.IsTerminal(file.Fd()) || isatty.IsCygwinTerminal(file.Fd()))
}

func productWorktreeChangeCount(ctx context.Context, repo string) (int, error) {
	body, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return 0, err
	}
	count := 0
	items := strings.Split(string(body), "\x00")
	for index := 0; index < len(items); index++ {
		item := items[index]
		if item == "" {
			continue
		}
		count++
		if len(item) >= 2 && strings.ContainsAny(item[:2], "RC") && index+1 < len(items) {
			index++
		}
	}
	return count, nil
}

func renderProductCommitAll(out io.Writer, result productCommitAllResult, stateName productState, jsonOut bool) error {
	if jsonOut {
		return renderAnyProductEnvelope(out, productEnvelope{
			OK: true, State: stateName, Changed: result.TargetEvents > 0,
			Actions:    []productAction{{Kind: "commit_all", Status: string(stateName), Target: result.Repo}},
			NextAction: nil, Data: result,
		}, true)
	}
	if result.DryRun {
		fmt.Fprintf(out, "Commit-all preview: %d changed path(s), %d pending event(s).\n",
			result.WorktreeChanges, result.PendingEvents)
		fmt.Fprintln(out, "No checkpoint or Git commit was created.")
		return nil
	}
	fmt.Fprintf(out, "Protected checkpoint: %s\n", result.CheckpointID)
	fmt.Fprintf(out, "Published target events: %d/%d in %d Git commit(s).\n",
		result.PublishedEvents, result.TargetEvents, result.CommitsCreated)
	if result.PublicationDrained {
		fmt.Fprintln(out, "Commit all complete.")
	} else {
		fmt.Fprintf(out, "Publication is waiting: %s\n", result.WaitingReason)
		fmt.Fprintln(out, "Your changes are protected; ACD will keep retrying.")
	}
	return nil
}

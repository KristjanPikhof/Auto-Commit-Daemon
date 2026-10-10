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
	Preview            *commitAllScope `json:"preview,omitempty"`
	Repo               string          `json:"repo"`
	CheckpointID       string          `json:"checkpoint_id,omitempty"`
	DrainID            string          `json:"drain_id,omitempty"`
	Phase              string          `json:"phase,omitempty"`
	Protected          bool            `json:"protected"`
	PublicationDrained bool            `json:"publication_drained"`
	TargetEventSeq     int64           `json:"target_event_seq"`
	TargetEvents       int64           `json:"target_events"`
	PublishedEvents    int64           `json:"published_events"`
	RemainingEvents    int64           `json:"remaining_events"`
	RecoveredEvents    int64           `json:"recovered_events,omitempty"`
	TerminalEvents     int64           `json:"terminal_events,omitempty"`
	CommitsCreated     int64           `json:"commits_created"`
	WaitingReason      string          `json:"waiting_reason,omitempty"`
	FallbackMode       string          `json:"fallback_mode,omitempty"`
	LastError          string          `json:"last_error,omitempty"`
	SemanticAttempts   int64           `json:"semantic_rebuild_attempts,omitempty"`
	EventFallbackCount int64           `json:"event_fallback_count,omitempty"`
	LastProgressTS     float64         `json:"last_progress_ts,omitempty"`
	StagedConsumed     bool            `json:"staged_consumed,omitempty"`
	WorktreeChanges    int             `json:"worktree_changes,omitempty"`
	PendingEvents      int             `json:"pending_events,omitempty"`
	DryRun             bool            `json:"dry_run,omitempty"`
	SemanticGroupCount int             `json:"semantic_group_count,omitempty"`
	PlanAttempt        int             `json:"plan_attempt,omitempty"`
	PlanAttemptLimit   int             `json:"plan_attempt_limit,omitempty"`
	ResolutionMode     string          `json:"resolution_mode,omitempty"`
	SingletonCount     int             `json:"singleton_count,omitempty"`
}

func newProductCommitAllCmd() *cobra.Command {
	var yes, dryRun bool
	cmd := &cobra.Command{
		Use:   "commit-all",
		Short: "Protect and publish all current changes",
		Long: `Protect all current eligible changes in a durable checkpoint, then
publish that fixed set as normal local Git commits.

The command does not squash everything into one commit. ACD keeps separate
changes separate when needed and may create several commits. Changes made after
the checkpoint are not added to the current run.

Start with --dry-run. Apply with --yes. If the terminal closes, ACD keeps the
checkpoint and continues safely; running the command again reconnects to the
same work.`,
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
	cmd.Flags().BoolVar(&yes, "yes", false, "Protect and publish without asking for confirmation")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be included without changing anything")
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
	scope, err := inspectCommitAllScope(ctx, lookup.Worktree.Root, lookup.Record.StateDB)
	if err != nil {
		return fmt.Errorf("acd commit-all: inspect preview: %w", err)
	}
	preview.Preview = &scope
	if dryRun {
		return renderProductCommitAll(out, preview, productStateWaiting, jsonOut)
	}
	input := bufio.NewReader(in)
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
reviewScope:
	for !yes {
		renderCommitAllScope(out, scope)
		fmt.Fprint(out, "Protect and publish this work? [y/N] ")
		answer, readErr := input.ReadString('\n')
		if readErr != nil && strings.TrimSpace(answer) == "" {
			return readErr
		}
		if normalized := strings.ToLower(strings.TrimSpace(answer)); normalized != "y" && normalized != "yes" {
			return invalidCommandError("acd commit-all: cancelled; no changes were made")
		}
		current, err := inspectCommitAllScope(ctx, lookup.Worktree.Root, lookup.Record.StateDB)
		if err != nil {
			return err
		}
		if current.Digest == scope.Digest {
			break
		}
		scope = current
		fmt.Fprintln(out, "The paths, staged content, or queued work changed. Review the refreshed preview.")
	}

	// --yes explicitly accepts current scope. Interactive approval is pinned
	// to the reviewed paths and staging, including the worker-side recheck.
	previewDigest := ""
	if !yes {
		previewDigest = scope.Digest
	}
	params, _ := json.Marshal(map[string]any{
		"kind": "checkpoint", "drain_publication": true,
		"consume_staged":        true,
		"preview_digest":        previewDigest,
		"expected_index_digest": scope.IndexDigest,
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
		fmt.Fprintln(progressOut, "Commit all: saving a checkpoint for current changes")
	}
	ticker = time.NewTicker(5 * time.Second)
	var call callResult
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case call = <-resultCh:
			if call.err != nil {
				var commandErr *CommandError
				if !yes && errors.As(call.err, &commandErr) && commandErr.Code == "plan_changed" {
					ticker.Stop()
					scope, err = inspectCommitAllScope(ctx, lookup.Worktree.Root, lookup.Record.StateDB)
					if err != nil {
						return err
					}
					fmt.Fprintln(out, "The paths, staged content, or queued work changed. Review the refreshed preview.")
					goto reviewScope
				}
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
			projection, _, statusErr := readProductPublicationDrain(ctx, lookup)
			if statusErr == nil {
				if drain := productPublicationDrainForProgress(projection,
					lookup.Record.WorktreeID, startedAt); drain != nil {
					remaining := drain.TargetEventCount - drain.PublishedEventCount
					writeProductCommitAllProgress(progressOut, *drain, remaining)
					continue
				}
			}
			current, inspectErr := inspectControl(ctx, lookup.Worktree.Root)
			if inspectErr == nil {
				fmt.Fprintf(progressOut,
					"Commit all: still saving current changes; protected=%s, queued=%d\n",
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
	if result.PublicationDrained && result.RecoveredEvents > 0 {
		current, inspectErr := inspectControl(ctx, lookup.Worktree.Root)
		changes, changeErr := productWorktreeChangeCount(ctx, lookup.Worktree.Root)
		if inspectErr != nil {
			return inspectErr
		}
		if changeErr != nil {
			return fmt.Errorf("acd commit-all: inspect recovered worktree: %w", changeErr)
		}
		if current.PendingEvents > 0 || changes > 0 {
			if !quiet && !jsonOut {
				fmt.Fprintf(progressOut,
					"Commit all: recovery found %d current change(s); protecting and publishing them now\n",
					current.PendingEvents)
			}
			prior := result
			follow, followErr := startProductPublicationDrain(
				ctx, lookup, params, time.Now())
			if followErr != nil {
				return fmt.Errorf("acd commit-all: start recovered follow-up drain: %w", followErr)
			}
			if !follow.PublicationDrained {
				follow, followErr = waitForProductPublicationDrain(
					ctx, lookup, follow, progressOut, quiet || jsonOut)
				if followErr != nil {
					return followErr
				}
			}
			follow.Repo = lookup.Worktree.Root
			follow.TargetEvents += prior.TargetEvents
			follow.PublishedEvents += prior.PublishedEvents
			follow.RecoveredEvents += prior.RecoveredEvents
			follow.TerminalEvents += prior.TerminalEvents
			follow.CommitsCreated += prior.CommitsCreated
			result = follow
		}
	}
	if result.PublicationDrained && fileExists(lookup.Record.StateDB) {
		conn, openErr := openStateDBReadOnly(ctx, lookup.Record.StateDB)
		if openErr == nil {
			if win, loadErr := loadLastIntentPlannerWindowSQL(ctx, conn); loadErr == nil && win != nil {
				result.SemanticGroupCount = len(win.SelectedGroups)
				result.PlanAttempt = win.PlanAttempt
				result.PlanAttemptLimit = win.PlanAttemptLimit
				result.ResolutionMode = win.ResolutionMode
				for _, group := range win.SelectedGroups {
					if len(group.SelectedSeqs) == 1 {
						result.SingletonCount++
					}
				}
			}
			_ = conn.Close()
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
	projection, workerAvailable, err := readProductPublicationDrain(ctx, lookup)
	if err != nil {
		return productCommitAllResult{}, false
	}
	drain := selectReconnectPublicationDrain(
		projection, lookup.Record.WorktreeID, startedAt)
	if drain == nil {
		return productCommitAllResult{}, false
	}
	// A persisted active drain proves protection, not that a missing worker
	// resumed publication. Let the caller restart the worker first.
	if !publicationDrainCanReconnect(*drain, workerAvailable) {
		return productCommitAllResult{}, false
	}
	return productCommitAllResultFromDrain(*drain), true
}

func publicationDrainCanReconnect(drain state.PublicationDrain, workerAvailable bool) bool {
	return workerAvailable || drain.Phase == state.PublicationDrainCompleted ||
		drain.Phase == state.PublicationDrainNeedsAction
}

// The durable drain remains readable when a worker socket disappears. Keep the
// request-time filter at the call site so an unrelated earlier run cannot be
// mistaken for the current commit-all request.
func readProductPublicationDrain(ctx context.Context, lookup controlRepoLookup, drainIDs ...string) (
	state.PublicationDrainReadOnlyProjection, bool, error,
) {
	var params json.RawMessage
	if len(drainIDs) > 0 && drainIDs[0] != "" {
		params, _ = json.Marshal(map[string]string{"drain_id": drainIDs[0]})
	}
	response, err := callSupervisor(ctx, lookup, "publication_drain_status", params, 2*time.Second)
	if err == nil {
		projection, decodeErr := decodeProductData[state.PublicationDrainReadOnlyProjection](response.Data)
		return projection, decodeErr == nil, decodeErr
	}
	if ctx.Err() != nil {
		return state.PublicationDrainReadOnlyProjection{}, false, ctx.Err()
	}
	return readProductPublicationDrainAfterWorkerFailure(ctx, lookup.Record.StateDB, err, drainIDs...)
}

func readProductPublicationDrainAfterWorkerFailure(
	ctx context.Context, dbPath string, workerErr error,
	drainIDs ...string,
) (state.PublicationDrainReadOnlyProjection, bool, error) {
	// A runtime compatibility failure is not a worker disconnect. Do not let
	// an older local projection make an incompatible command look successful.
	if !productDrainReadOnlyFallbackAllowed(workerErr) {
		return state.PublicationDrainReadOnlyProjection{}, false, workerErr
	}
	if !fileExists(dbPath) {
		return state.PublicationDrainReadOnlyProjection{}, false, workerErr
	}
	projection, readErr := state.ReadPublicationDrainProjection(ctx, dbPath, drainIDs...)
	if readErr != nil {
		return state.PublicationDrainReadOnlyProjection{}, false, errors.Join(workerErr, readErr)
	}
	return projection, false, nil
}

func productDrainReadOnlyFallbackAllowed(err error) bool {
	var commandErr *CommandError
	return errors.As(err, &commandErr) && commandErr.Retryable &&
		!strings.Contains(err.Error(), "acd setup") &&
		!strings.Contains(err.Error(), "compatibility contract")
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

func productPublicationDrainForProgress(
	projection state.PublicationDrainReadOnlyProjection,
	worktreeID string,
	startedAt time.Time,
) *state.PublicationDrain {
	if drain := selectReconnectPublicationDrain(projection, worktreeID, startedAt); drain != nil {
		return drain
	}
	// A repeated command may be waiting for a drain created hours ago. This
	// older target is useful for display, but not proof that this request began.
	if projection.Latest != nil && projection.Latest.WorktreeID == worktreeID &&
		projection.Latest.Phase != state.PublicationDrainCompleted {
		return projection.Latest
	}
	return nil
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
	// Supervisor restart backoff reaches 30 seconds; allow startup time too.
	const workerReconnectGrace = 45 * time.Second
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	var latest *state.PublicationDrain
	var workerAvailable bool
	var workerUnavailableSince time.Time
	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-report.C:
			if quiet {
				continue
			}
			if !workerAvailable {
				fmt.Fprintln(progressOut,
					"Commit all: worker unavailable; reconnecting to the protected publication run")
			}
			if latest == nil {
				fmt.Fprintln(progressOut,
					"Commit all: reconnecting to the existing publication run")
				continue
			}
			remaining := latest.TargetEventCount - latest.PublishedEventCount
			writeProductCommitAllProgress(progressOut, *latest, remaining)
		case <-poll.C:
			projection, live, err := readProductPublicationDrain(ctx, lookup, result.DrainID)
			if err != nil {
				if workerUnavailableSince.IsZero() {
					workerUnavailableSince = time.Now()
				}
				if time.Since(workerUnavailableSince) >= workerReconnectGrace &&
					!productWorkerRunning(ctx, lookup) {
					return result, unavailableError(fmt.Sprintf(
						"acd commit-all: protected publication drain %s could not reconnect to its worker; run `acd doctor`",
						result.DrainID))
				}
				continue
			}
			workerAvailable = live
			if live {
				workerUnavailableSince = time.Time{}
			} else if workerUnavailableSince.IsZero() {
				workerUnavailableSince = time.Now()
			}
			latest = productPublicationDrainByID(projection, result.DrainID)
			if latest == nil {
				continue
			}
			if !live && time.Since(workerUnavailableSince) >= workerReconnectGrace &&
				!productWorkerRunning(ctx, lookup) &&
				latest.Phase != state.PublicationDrainCompleted &&
				latest.Phase != state.PublicationDrainNeedsAction {
				return result, unavailableError(fmt.Sprintf(
					"acd commit-all: protected publication drain %s could not reconnect to its worker; run `acd doctor`",
					result.DrainID))
			}
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

func productWorkerRunning(ctx context.Context, lookup controlRepoLookup) bool {
	worker, ok := readSupervisorWorkerStatus(ctx, lookup.Roots, lookup.Record.RepositoryID)
	return ok && (worker.State == "running" || worker.State == "starting")
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

func writeProductCommitAllProgress(out io.Writer, drain state.PublicationDrain, remaining int64) {
	fmt.Fprintf(out, "Commit all: %d of %d protected change(s) left; %d commit(s) created; %s\n",
		remaining, drain.TargetEventCount, drain.CommitCount,
		publicationPhaseLabel(drain.Phase, drain.FallbackMode))
	if drain.LastError != "" {
		fmt.Fprintf(out, "Commit all: last issue: %s\n", drain.LastError)
	}
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
		fmt.Fprintln(out, "Commit-all preview")
		fmt.Fprintf(out, "Changes found: %d path(s)\n", result.WorktreeChanges)
		fmt.Fprintf(out, "Already protected and waiting: %d change(s)\n", result.PendingEvents)
		if result.Preview != nil {
			renderCommitAllScope(out, *result.Preview)
		}
		fmt.Fprintln(out, "Changed: no")
		fmt.Fprintln(out, "Next: Run `acd commit-all --yes` to protect and publish this work.")
		return nil
	}
	fmt.Fprintf(out, "Checkpoint: %s\n", result.CheckpointID)
	fmt.Fprintf(out, "Published to Git: %d of %d protected change(s) in %d commit(s)\n",
		result.PublishedEvents, result.TargetEvents, result.CommitsCreated)
	if result.RecoveredEvents > 0 {
		fmt.Fprintf(out, "Recovered and protected again: %d change(s)\n",
			result.RecoveredEvents)
	}
	if result.PublicationDrained {
		fmt.Fprintln(out, "Status: Complete. All selected changes are protected and published.")
		fmt.Fprintln(out, "Next: No action needed.")
	} else {
		fmt.Fprintf(out, "Status: Waiting. %s\n", result.WaitingReason)
		fmt.Fprintln(out, "Next: No action needed now. Your work is protected and ACD will keep trying.")
	}
	return nil
}

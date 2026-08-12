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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type productCommitAllResult struct {
	Repo               string `json:"repo"`
	CheckpointID       string `json:"checkpoint_id,omitempty"`
	Protected          bool   `json:"protected"`
	PublicationDrained bool   `json:"publication_drained"`
	TargetEventSeq     int64  `json:"target_event_seq"`
	TargetEvents       int64  `json:"target_events"`
	PublishedEvents    int64  `json:"published_events"`
	RemainingEvents    int64  `json:"remaining_events"`
	RecoveredEvents    int64  `json:"recovered_events,omitempty"`
	TerminalEvents     int64  `json:"terminal_events,omitempty"`
	CommitsCreated     int64  `json:"commits_created"`
	WaitingReason      string `json:"waiting_reason,omitempty"`
	WorktreeChanges    int    `json:"worktree_changes,omitempty"`
	PendingEvents      int    `json:"pending_events,omitempty"`
	DryRun             bool   `json:"dry_run,omitempty"`
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
	})
	type callResult struct {
		response supervisor.Response
		err      error
	}
	resultCh := make(chan callResult, 1)
	go func() {
		response, callErr := callSupervisor(ctx, lookup, "checkpoint_barrier", params,
			supervisor.CheckpointBarrierTimeout)
		resultCh <- callResult{response: response, err: callErr}
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
			current, inspectErr := inspectControl(ctx, lookup.Worktree.Root)
			if inspectErr == nil {
				fmt.Fprintf(progressOut, "Commit all: protected=%s pending=%d; still publishing\n",
					yesNo(current.Protected), current.PendingEvents)
			}
		}
	}

completed:
	result, err := decodeProductData[productCommitAllResult](call.response.Data)
	if err != nil {
		return fmt.Errorf("acd commit-all: decode worker result: %w", err)
	}
	result.Repo = lookup.Worktree.Root
	return finishProductCommitAll(out, result, jsonOut)
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

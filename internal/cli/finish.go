package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/spf13/cobra"
)

func newFinishCmd() *cobra.Command {
	var params finishStartParams
	cmd := &cobra.Command{Use: "finish [PURPOSE]", Short: "Create Git commits from your saved changes",
		Long: `Create local Git commits from a fresh snapshot of this working copy.

Local mode creates one commit. Intent mode uses the approved AI provider to
group completed work into commits. Later edits stay saved for your next
finish. ACD never pushes.

Supply PURPOSE as context describing what changed and why. Intent uses it to
explain decisions and choose useful steps. Intent requires approved redacted
code diffs. Binary-only changes also need context. --message requests one batch
with that exact message and disables automatic splitting, including in Intent.

Use --dry-run to plan and check without adding commits to your branch. It can
save private snapshots, call AI in Intent mode, and run configured checks. Add
--include-staged to include your exact current staged selection. Use --retry
ID to reconnect or retry the same saved request; it keeps its original
changes, purpose, message, and staged selection.`,
		Example: `  acd finish "Add offline checkout"
  acd finish --message "Update setup guidance"
  acd finish --dry-run
  acd finish --include-staged
  acd finish --retry REQUEST_ID`,
		Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				params.Purpose = args[0]
			}
			if params.RetryID != "" && (len(args) > 0 || cmd.Flags().Changed("message") || cmd.Flags().Changed("include-staged")) {
				return invalidCommandError("acd finish: --retry cannot change purpose, message or staged consent")
			}
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			quiet, _ := cmd.Flags().GetBool("quiet")
			return runFinish(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), repo, params, jsonOut, quiet)
		}}
	cmd.Flags().BoolVar(&params.DryRun, "dry-run", false, "Plan and check without moving the branch or index")
	cmd.Flags().BoolVar(&params.IncludeStaged, "include-staged", false, "Include the exact current staged selection")
	cmd.Flags().StringVar(&params.RetryID, "retry", "", "Retry or reconnect to a saved request")
	cmd.Flags().StringVar(&params.Message, "message", "", "Create one batch commit with this exact message (disables splitting)")
	return cmd
}

type finishCLIResult struct {
	state.FinishRequest
	Repo                string               `json:"repo"`
	WorktreeID          *string              `json:"worktree_id"`
	BranchRef           *string              `json:"branch_ref"`
	ExpectedHead        *string              `json:"expected_head"`
	CheckpointID        *string              `json:"checkpoint_id"`
	CheckpointTree      *string              `json:"checkpoint_tree"`
	TargetTree          *string              `json:"target_tree"`
	Mode                string               `json:"mode"`
	Style               string               `json:"style"`
	Verification        []daemon.FinishCheck `json:"verification"`
	EvidenceFingerprint string               `json:"evidence_fingerprint"`
	Protected           bool                 `json:"protected"`
	Published           bool                 `json:"published"`
	NoChanges           bool                 `json:"no_changes"`
	CommitsCreated      int                  `json:"commits_created"`
	CommitOIDs          []string             `json:"commit_oids"`
	NewHead             string               `json:"new_head"`
	StagedConsumed      bool                 `json:"staged_consumed"`
	WorktreeChanges     int                  `json:"worktree_changes"`
	SnapshotSavedAt     string               `json:"snapshot_saved_at,omitempty"`
}

func runFinish(ctx context.Context, out, progress io.Writer, repo string, params finishStartParams, jsonOut, quiet bool) error {
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil {
		return controlWorktreeError("finish", repo, err)
	}
	if !lookup.Registered || lookup.Record.LifecycleDisabled() {
		return actionRequiredError("protection_required", "acd finish: enable protection first with `acd on`")
	}
	if !quiet && !jsonOut {
		if params.RetryID != "" {
			fmt.Fprintln(progress, "Reconnecting to the saved finish request; later edits stay for the next finish.")
		} else {
			fmt.Fprintln(progress, "Saving current changes for this finish. Later edits stay for the next finish.")
		}
	}
	r, err := finishRequest(ctx, lookup, "finish_start", params)
	if err != nil {
		return err
	}
	interval := 100 * time.Millisecond
	lastStage := ""
	for r.State == state.FinishRunning || r.State == state.FinishPrepared {
		if !quiet && !jsonOut && r.Stage != lastStage {
			fmt.Fprintln(progress, finishStageLabel(r.Stage))
			lastStage = r.Stage
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return renderFinish(out, lookup, r, jsonOut, "finish_running")
		case <-timer.C:
		}
		next, readErr := finishRequest(ctx, lookup, "finish_status", map[string]string{"request_id": r.ID})
		if readErr != nil {
			return renderFinish(out, lookup, r, jsonOut, "finish_status_unavailable")
		}
		r = next
		if interval < time.Second {
			interval *= 2
			if interval > time.Second {
				interval = time.Second
			}
		}
	}
	return renderFinish(out, lookup, r, jsonOut, "")
}

func renderFinish(out io.Writer, lookup controlRepoLookup, r state.FinishRequest, jsonOut bool, waitCode string) error {
	detached := waitCode != ""
	result := finishCLIResult{FinishRequest: r, Repo: lookup.Worktree.Root, WorktreeID: finishIdentity(r.Target.WorktreeID), BranchRef: finishIdentity(r.Target.BranchRef), ExpectedHead: finishIdentity(r.Target.ExpectedHead), CheckpointID: finishIdentity(r.Target.CheckpointID), CheckpointTree: finishIdentity(r.Target.CheckpointTree), TargetTree: finishIdentity(r.Target.TargetTree), Mode: r.Target.Mode, Style: r.Target.Style, Verification: []daemon.FinishCheck{}, Protected: r.Target.CheckpointID != "", CommitOIDs: []string{}}
	var plan daemon.FinishSavedPlan
	if len(r.Plan) > 0 {
		if err := json.Unmarshal(r.Plan, &plan); err != nil {
			return fmt.Errorf("acd finish: invalid saved plan\nDetails: %w", err)
		}
		for _, step := range plan.Steps {
			result.Verification = append(result.Verification, step.Check)
		}
	}
	targetJSON, err := json.Marshal(r.Target)
	if err != nil {
		return err
	}
	fingerprint := sha256.Sum256(targetJSON)
	result.EvidenceFingerprint = hex.EncodeToString(fingerprint[:])
	var saved daemon.FinishResult
	if len(r.Result) > 0 {
		if err := json.Unmarshal(r.Result, &saved); err != nil {
			return fmt.Errorf("acd finish: invalid saved result\nDetails: %w", err)
		}
	}
	result.Published, result.NoChanges, result.CommitsCreated, result.NewHead, result.StagedConsumed = saved.Published, saved.NoChanges, saved.CommitsCreated, saved.NewHead, saved.StagedConsumed
	if saved.Published {
		var proof state.FinishPublicationProof
		if err := json.Unmarshal(r.Publication, &proof); err != nil {
			return fmt.Errorf("acd finish: invalid publication proof\nDetails: %w", err)
		}
		for _, commit := range proof.Commits {
			result.CommitOIDs = append(result.CommitOIDs, commit.OID)
		}
	}
	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count, countErr := productWorktreeChangeCount(readCtx, lookup.Worktree.Root)
	result.WorktreeChanges = count
	if r.Target.CheckpointID != "" {
		if cp, err := state.ResolveCheckpoint(readCtx, state.DBPathFromGitDir(lookup.Worktree.GitDir), r.Target.CheckpointID); err == nil {
			result.SnapshotSavedAt = time.Unix(0, int64(cp.CreatedTS*float64(time.Second))).Local().Format(time.RFC3339)
		}
	}
	ok := r.State == state.FinishCompleted && !detached
	retryable := (r.State == state.FinishFailed || detached) && r.ReasonCode != "provider_context_required" && r.ReasonCode != "provider_diffs_required" && r.ReasonCode != "provider_input_too_large"
	exit, failureCode := finishExit(r, waitCode)
	next := ""
	if !ok {
		if r.State == state.FinishNeedsAttention {
			next = "acd doctor"
		} else if r.ReasonCode == "provider_context_required" {
			next = `acd finish "what changed and why"`
		} else if r.ReasonCode == "provider_input_too_large" {
			next = "Review ai.max_input_bytes in acd config, then start a new acd finish"
		} else if r.ReasonCode == "provider_diffs_required" {
			next = "Approve code sharing with acd config, then start a new acd finish"
		} else {
			next = "acd finish --retry " + r.ID
		}
		if r.DryRun && r.State != state.FinishNeedsAttention && r.ReasonCode != "provider_diffs_required" && r.ReasonCode != "provider_input_too_large" {
			next += " --dry-run"
		}
	}
	stateName := productStateProtected
	if !ok {
		stateName = productStateNeedsAction
	}
	if detached {
		stateName = productStatePublishing
	}
	if jsonOut {
		var nextAction *string
		if next != "" {
			nextAction = &next
		}
		var failure *productError
		if !ok {
			failure = &productError{Code: failureCode, Message: r.Reason, Retryable: retryable}
			if detached {
				failure.Message = "Work is saved; reconnect to inspect this request"
			}
		}
		if err := renderAnyProductEnvelope(out, productEnvelope{OK: ok, State: stateName, Changed: true, Actions: []productAction{{Kind: "finish", Status: r.State, Target: lookup.Worktree.Root}}, NextAction: nextAction, Data: result, Error: failure}, true); err != nil {
			return err
		}
	} else {
		switch {
		case detached:
			fmt.Fprintln(out, "Your saved finish request is still available.")
		case r.State != state.FinishCompleted:
			if r.State == state.FinishNeedsAttention {
				fmt.Fprintln(out, "Finish needs attention.")
			} else {
				fmt.Fprintln(out, "Finish failed.")
			}
			fmt.Fprintf(out, "Reason: %s\n", consoleProse(consoleCause(r.Reason)))
		case saved.NoChanges:
			fmt.Fprintln(out, "No changes to commit.")
		case r.DryRun:
			fmt.Fprintln(out, "Finish preview\nNo commits were added to your branch.")
			if plan.Disclosure.MetadataIncluded && len(plan.Disclosure.TextChangeIDs) == 0 {
				fmt.Fprintln(out, "Plan based on supplied context and file metadata; code diffs were not shared.")
			}
			for _, step := range plan.Steps {
				fmt.Fprintf(out, "%s\n  %s\n", consoleProse(step.Message), finishCheckLabel(step.Check))
				if step.Boundary != "" {
					fmt.Fprintf(out, "  Boundary: %s\n", consoleProse(step.Boundary))
				}
			}
			fmt.Fprintln(out, "Optional: Run `acd finish` when the current work is ready.")
		default:
			fmt.Fprintln(out, commitCountLabel(result.CommitsCreated))
		}
		if r.EvidenceCalls > 0 {
			fmt.Fprintf(out, "Evidence review calls: %d. Global planning calls: %d.\n", r.EvidenceCalls, r.PlanningCalls)
		}
		if !ok {
			fmt.Fprintf(out, "Request: %s\n", safePreviewText(r.ID, 0))
			if result.Protected {
				fmt.Fprintln(out, "Changes for this request: saved.")
			}
		}
		if !r.DryRun {
			shown := make(map[string]bool)
			for _, check := range result.Verification {
				label := finishCheckLabel(check)
				if !shown[label] {
					fmt.Fprintln(out, label)
					shown[label] = true
				}
			}
		}
		if countErr == nil && count > 0 {
			fmt.Fprintf(out, "Uncommitted files remaining: %d.\n", count)
			if saved.Published {
				if result.SnapshotSavedAt != "" {
					fmt.Fprintf(out, "This finish used the snapshot saved at %s.\n", result.SnapshotSavedAt)
				}
				fmt.Fprintln(out, "Later edits and excluded files stay in your working copy.")
				fmt.Fprintln(out, "Inspect them with `git status --short`. Run `acd finish` when the remaining work is ready.")
			}
		} else if countErr == nil && saved.Published {
			fmt.Fprintln(out, "Working copy: clean.")
		}
		if next != "" {
			label := "Optional"
			if r.State == state.FinishNeedsAttention {
				label = "Action required"
			}
			fmt.Fprintf(out, "\n%s: %s\n", label, next)
			if strings.Contains(next, "--retry") {
				fmt.Fprintln(out, "Retry reconnects to this request or restarts it after failure, using the same saved changes.")
			}
		}
	}
	if !ok {
		return &CommandError{Code: failureCode, Message: "Finish requires attention. Next: " + next, Exit: exit, Retryable: retryable, rendered: true}
	}
	return nil
}

func finishIdentity(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func finishExit(r state.FinishRequest, waitCode string) (int, string) {
	if waitCode != "" {
		return ExitUnavailable, waitCode
	}
	if r.State == state.FinishCompleted {
		return ExitOK, ""
	}
	if r.ReasonCode == "provider_deadline" || r.ReasonCode == "provider_failure" {
		return ExitUnavailable, "finish_unavailable"
	}
	if r.State == state.FinishNeedsAttention {
		return ExitActionRequired, "finish_needs_attention"
	}
	if r.ReasonCode == "provider_input_too_large" || r.ReasonCode == "provider_context_required" || r.ReasonCode == "provider_diffs_required" {
		return ExitActionRequired, r.ReasonCode
	}
	return ExitActionRequired, "finish_failed"
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

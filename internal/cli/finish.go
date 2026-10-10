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
	cmd := &cobra.Command{Use: "finish [PURPOSE]", Short: "Commit the current protected work", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
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
	cmd.Flags().StringVar(&params.Message, "message", "", "Use this exact commit message")
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
}

func runFinish(ctx context.Context, out, progress io.Writer, repo string, params finishStartParams, jsonOut, quiet bool) error {
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil {
		return controlWorktreeError("finish", repo, err)
	}
	if !lookup.Registered || lookup.Record.LifecycleDisabled() {
		return actionRequiredError("protection_required", "acd finish: enable protection first with `acd on`")
	}
	r, err := finishRequest(ctx, lookup, "finish_start", params)
	if err != nil {
		return err
	}
	interval := 100 * time.Millisecond
	lastStage := ""
	for r.State == state.FinishRunning || r.State == state.FinishPrepared {
		if !quiet && !jsonOut && r.Stage != lastStage {
			fmt.Fprintf(progress, "Finish %s: %s (work saved in checkpoint %s)\n", r.ID, r.Stage, r.Target.CheckpointID)
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
			return fmt.Errorf("acd finish: invalid saved plan: %w", err)
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
			return fmt.Errorf("acd finish: invalid saved result: %w", err)
		}
	}
	result.Published, result.NoChanges, result.CommitsCreated, result.NewHead, result.StagedConsumed = saved.Published, saved.NoChanges, saved.CommitsCreated, saved.NewHead, saved.StagedConsumed
	if saved.Published {
		var proof state.FinishPublicationProof
		if err := json.Unmarshal(r.Publication, &proof); err != nil {
			return fmt.Errorf("acd finish: invalid publication proof: %w", err)
		}
		for _, commit := range proof.Commits {
			result.CommitOIDs = append(result.CommitOIDs, commit.OID)
		}
	}
	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count, countErr := productWorktreeChangeCount(readCtx, lookup.Worktree.Root)
	result.WorktreeChanges = count
	ok := r.State == state.FinishCompleted && !detached
	exit, failureCode := finishExit(r, waitCode)
	next := ""
	if !ok {
		next = "acd finish --retry " + r.ID
		if r.DryRun {
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
			failure = &productError{Code: failureCode, Message: r.Reason, Retryable: r.State == state.FinishFailed || detached}
			if detached {
				failure.Message = "Work is saved; reconnect to inspect this request"
			}
		}
		if err := renderAnyProductEnvelope(out, productEnvelope{OK: ok, State: stateName, Changed: true, Actions: []productAction{{Kind: "finish", Status: r.State, Target: lookup.Worktree.Root}}, NextAction: nextAction, Data: result, Error: failure}, true); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(out, "Finish request: %s (attempt %d)\nWork saved in checkpoint: %s\n", r.ID, r.Attempt, r.Target.CheckpointID)
		switch {
		case detached:
			fmt.Fprintln(out, "Work is saved; reconnect to inspect this request.")
		case r.State != state.FinishCompleted:
			fmt.Fprintf(out, "Finish %s: %s\n", r.State, r.Reason)
		case saved.NoChanges:
			fmt.Fprintln(out, "No changes to finish.\nPublished: 0 commit(s)")
		case r.DryRun:
			fmt.Fprintf(out, "Finish preview\nTarget: %s\nPublished: no\n", r.Target.TargetTree)
			for _, step := range plan.Steps {
				fmt.Fprintf(out, "%s\n  Tree: %s; verification: %s; command executed: %t\n", step.Message, step.TreeOID, step.Check.Policy, step.Check.Executed)
			}
			fmt.Fprintln(out, "Run `acd finish` to freeze and finish the current work.")
		default:
			fmt.Fprintf(out, "Published: %d commit(s)\n", result.CommitsCreated)
			for _, oid := range result.CommitOIDs {
				fmt.Fprintln(out, oid)
			}
		}
		if !r.DryRun {
			for _, check := range result.Verification {
				fmt.Fprintf(out, "Verification: %s; tree: %s; command executed: %t\n", check.Policy, check.TreeOID, check.Executed)
			}
		}
		if countErr == nil && count > 0 {
			fmt.Fprintf(out, "Current worktree: %d changed path(s); later work stays separate from this request.\n", count)
		}
		if next != "" {
			fmt.Fprintf(out, "Next: %s\n", next)
		}
	}
	if !ok {
		return &CommandError{Code: failureCode, Message: "acd finish: work is saved; " + next, Exit: exit, Retryable: detached || r.State == state.FinishFailed, rendered: true}
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
	if r.ReasonCode == "provider_input_too_large" {
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

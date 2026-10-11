package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func readIntentHistoryPlanRef(ctx context.Context, repo, ref string) (state.IntentHistoryPlan, bool, error) {
	var plan state.IntentHistoryPlan
	if file, err := os.Open(ref); err == nil {
		defer file.Close()
		raw, err := io.ReadAll(io.LimitReader(file, state.IntentHistoryPlanByteCap+1))
		if err != nil {
			return plan, false, err
		}
		if len(raw) > state.IntentHistoryPlanByteCap {
			return plan, false, errors.New("history rewrite: saved plan exceeds size limit")
		}
		if err := json.Unmarshal(raw, &plan); err != nil {
			return plan, false, err
		}
		return plan, plan.SourceBranchRef != "" && (plan.Version == 1 || plan.Version == state.IntentHistoryPlanVersion), nil
	}
	path, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return plan, false, err
	}
	db, err := state.OpenReadOnly(ctx, path)
	if err != nil {
		return plan, false, err
	}
	defer db.Close()
	return state.LoadIntentHistoryPlan(ctx, db, ref)
}

func generateFinishHistoryPlan(ctx context.Context, out io.Writer, repo string, selection git.RewriteSelection, opts rewriteCommitsOptions, jsonOut bool) error {
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil {
		return err
	}
	if workerPublicationHeld(lookup.Roots.SetupPublicationHoldPath(), nil) {
		return errors.New("history rewrite: setup currently holds publication; finish setup first")
	}
	runtime := &workerRuntime{roots: lookup.Roots, record: lookup.Record, worktree: lookup.Worktree}
	path, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return err
	}
	if db, openErr := state.OpenReadOnly(ctx, path); openErr == nil {
		runtime.db = db
		defer db.Close()
	} else if !errors.Is(openErr, os.ErrNotExist) {
		return openErr
	}
	if opts.planOut == "" && (!lookup.Registered || lookup.Record.LifecycleDisabled()) {
		return errors.New("history rewrite: use --plan-out FILE for a standalone plan, or run `acd on`")
	}
	cfg, err := resolveFinishWorkerConfigInputs(ctx, lookup.Roots, runtime, true)
	if err != nil {
		return err
	}
	var transport ai.FinishTransport
	if cfg.Mode == "intent" {
		var closer io.Closer
		transport, closer, err = ai.BuildFinishTransport(cfg.Provider, cfg.Approved)
		if err != nil {
			return err
		}
		if closer != nil {
			defer closer.Close()
		}
	}
	var chain, suffix []string
	for _, commit := range selection.Selected {
		chain = append(chain, commit.OID)
	}
	for _, commit := range selection.RecreateUnchanged {
		suffix = append(suffix, commit.OID)
	}
	timeout := cfg.Provider.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	plan, err := daemon.PlanFinishHistory(ctx, repo, selection.BranchRef, chain, suffix, transport, daemon.FinishHistoryOptions{Mode: cfg.Mode, Style: cfg.Style, MessagesOnly: opts.messagesOnly, DiffEgress: cfg.Provider.DiffEgress, Deadline: time.Now().Add(timeout), VerificationConfigDigest: cfg.ConfigDigest})
	if err != nil {
		return err
	}
	if opts.newBranch != "" {
		target := opts.newBranch
		if !strings.HasPrefix(target, "refs/heads/") {
			target = "refs/heads/" + target
		}
		if target == selection.BranchRef {
			return errors.New("history rewrite: new branch must differ from source")
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "check-ref-format", target); err != nil {
			return err
		}
		if _, err := git.RevParse(ctx, repo, target); err == nil {
			return errors.New("history rewrite: target branch already exists")
		}
		plan.TargetBranchRef = target
	}
	ref := plan.ID
	if opts.planOut != "" {
		raw, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return err
		}
		if len(raw) > state.IntentHistoryPlanByteCap {
			return errors.New("history rewrite: saved plan exceeds size limit")
		}
		if err := os.WriteFile(opts.planOut, append(raw, '\n'), 0600); err != nil {
			return err
		}
		ref = opts.planOut
	} else if _, err := historyRequest(ctx, lookup, "history_save", finishHistoryParams{Plan: plan}); err != nil {
		return err
	}
	if !opts.planOnly && opts.yes && !opts.dryRun {
		applyOut := out
		if jsonOut {
			applyOut = io.Discard
		}
		if err := applyIntentHistoryPlan(ctx, applyOut, repo, plan, false); err != nil {
			return err
		}
		if !jsonOut {
			return nil
		}
	}
	if jsonOut {
		return json.NewEncoder(out).Encode(plan)
	}
	printIntentHistoryPlan(out, plan)
	fmt.Fprintf(out, "Plan saved: %s\nPreview: acd history rewrite --apply %s --dry-run\n", ref, rewritePlanRefArg(ref))
	return nil
}

func printIntentHistoryPlan(out io.Writer, plan state.IntentHistoryPlan) {
	fmt.Fprintf(out, "History rewrite: %d original commits -> %d proposed commits\nOriginal branch: %s\nResult branch: %s\n", len(plan.SourceChain), len(plan.Goals), plan.SourceBranchRef, plan.TargetBranchRef)
	for _, goal := range plan.Goals {
		fmt.Fprintf(out, "- %s\n  Purpose: %s\n  Why this is one commit: %s\n", strings.SplitN(goal.Message, "\n", 2)[0], goal.Purpose, goal.Reason)
	}
}

func showIntentHistoryPlan(ctx context.Context, out io.Writer, repo string, plan state.IntentHistoryPlan, jsonOut, previewPassed bool) error {
	view := struct {
		state.IntentHistoryPlan
		WorkerRequest *state.FinishHistoryRequest `json:"worker_request,omitempty"`
		PreviewPassed bool                        `json:"preview_passed,omitempty"`
	}{IntentHistoryPlan: plan, PreviewPassed: previewPassed}
	if path, err := rewriteStateDBPath(ctx, repo); err == nil {
		if db, err := state.OpenReadOnly(ctx, path); err == nil {
			defer db.Close()
			if r, ok, err := state.LoadFinishHistoryRequest(ctx, db, plan.ID); err == nil && ok {
				view.WorkerRequest = &r
			}
		}
	}
	if jsonOut {
		return json.NewEncoder(out).Encode(view)
	}
	printIntentHistoryPlan(out, plan)
	if r := view.WorkerRequest; r != nil {
		fmt.Fprintf(out, "Rewrite status: %s\n", strings.ReplaceAll(r.Status, "_", " "))
		if r.Error != "" {
			fmt.Fprintln(out, "Details:", consoleProse(consoleCause(r.Error)))
		}
	}
	return nil
}

func applyIntentHistoryPlan(ctx context.Context, out io.Writer, repo string, plan state.IntentHistoryPlan, dryRun bool) error {
	if err := daemon.ValidateIntentHistoryPlan(ctx, repo, plan); err != nil {
		return err
	}
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil {
		return err
	}
	if !lookup.Registered || lookup.Record.LifecycleDisabled() {
		return errors.New("history rewrite: approved tree verification requires the active protection worker; run `acd on`")
	}
	r, err := historyRequest(ctx, lookup, "history_start", finishHistoryParams{Plan: plan, DryRun: dryRun})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "History request saved: %s (attempt %d)\n", r.PlanID, r.Attempt)
	for r.Status == "running" || r.Status == "prepared" {
		select {
		case <-ctx.Done():
			return fmt.Errorf("history rewrite: request %s is saved; reconnect with --show-plan %s\nDetails: %w", r.PlanID, r.PlanID, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
		r, err = historyRequest(ctx, lookup, "history_status", finishHistoryParams{PlanID: plan.ID})
		if err != nil {
			return fmt.Errorf("history rewrite: request %s is saved; inspect --show-plan %s\nDetails: %w", plan.ID, plan.ID, err)
		}
	}
	if r.Status != "completed" {
		action := "Review the saved plan with `acd history rewrite --show-plan " + plan.ID + "` and address the cause before applying it again."
		summary := "History rewrite failed."
		if r.Status == "needs_attention" {
			summary = "History rewrite needs attention."
			action = "Run `acd doctor`."
		}
		return errors.New(summary + "\nReason: " + safePreviewText(r.Error, 0) + "\nNext: " + action)
	}
	if dryRun {
		fmt.Fprintln(out, "Preview passed. Each proposed commit passed the configured verification.\nThe branches were unchanged.")
	} else {
		fmt.Fprintln(out, "History rewrite completed.\nThe original history is kept in a private backup.")
		var proof state.FinishHistoryProof
		if json.Unmarshal(r.Proof, &proof) == nil && proof.BackupRef != "" {
			fmt.Fprintf(out, "Original history backup: %s\n", safePreviewText(proof.BackupRef, 0))
		}
	}
	return nil
}

// editFinishHistoryMessages creates a new identity for reviewed messages while
// retaining every original tree, author and membership boundary.
func editFinishHistoryMessages(ctx context.Context, out io.Writer, repo string, plan state.IntentHistoryPlan, opts rewriteCommitsOptions, jsonOut bool) error {
	if plan.Mode != "local" && !plan.MessagesOnly {
		return errors.New("history rewrite: immutable goal reconstruction requires a new plan")
	}
	if err := daemon.ValidateIntentHistoryPlan(ctx, repo, plan); err != nil {
		return err
	}
	editable := state.RewritePlan{ID: plan.ID, CommitFormat: plan.CommitFormat}
	for _, goal := range plan.Goals {
		editable.Groups = append(editable.Groups, state.RewritePlanGroup{Members: []state.RewritePlanMember{{OldOID: goal.AuthorOID, OriginalMessage: goal.Message}}, ProposedMessage: goal.Message, GroupingReason: goal.Reason})
	}
	groups, changed, err := editRewritePlanWithEditor(editable, opts.editFormat)
	if err != nil {
		return err
	}
	if len(groups) != len(plan.Goals) {
		return errors.New("history rewrite: message editing cannot regroup original commits")
	}
	for index, group := range groups {
		if len(group.Members) != 1 || group.Members[0].OldOID != plan.Goals[index].AuthorOID {
			return errors.New("history rewrite: message editing cannot change membership or order")
		}
		plan.Goals[index].Message, plan.Goals[index].Reason = group.ProposedMessage, group.GroupingReason
	}
	ref := opts.editPlan
	if changed {
		plan.ID = ""
		plan, err = state.PrepareIntentHistoryPlan(plan)
		if err != nil {
			return err
		}
		// Keep the same worktree namespace but give the reviewed revision its own source pin.
		prefix := strings.Split(plan.SourceRef, "/history/")[0]
		plan.SourceRef = prefix + "/history/" + plan.ID + "/source"
		if _, err := git.EnsurePrivateRefDurable(ctx, repo, git.FinishRefPrefix, plan.SourceRef, plan.ExpectedHead); err != nil {
			return err
		}
		if err := daemon.ValidateIntentHistoryPlan(ctx, repo, plan); err != nil {
			return err
		}
		if file, err := os.Open(opts.editPlan); err == nil {
			file.Close()
			raw, err := json.MarshalIndent(plan, "", "  ")
			if err != nil {
				return err
			}
			if err := os.WriteFile(opts.editPlan, append(raw, '\n'), 0600); err != nil {
				return err
			}
		} else {
			lookup, err := loadControlRepo(ctx, repo)
			if err != nil {
				return err
			}
			if _, err := historyRequest(ctx, lookup, "history_save", finishHistoryParams{Plan: plan}); err != nil {
				return err
			}
			ref = plan.ID
		}
	}
	if opts.yes && !opts.planOnly && !opts.dryRun {
		applyOut := out
		if jsonOut {
			applyOut = io.Discard
		}
		if err := applyIntentHistoryPlan(ctx, applyOut, repo, plan, false); err != nil {
			return err
		}
		if !jsonOut {
			return nil
		}
	}
	if jsonOut {
		return json.NewEncoder(out).Encode(plan)
	}
	fmt.Fprintf(out, "Reviewed plan saved: %s\n", ref)
	return nil
}

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func readIntentHistoryPlanRef(ctx context.Context, repo, ref string) (state.IntentHistoryPlan, bool, error) {
	var plan state.IntentHistoryPlan
	if file, err := os.Open(ref); err == nil {
		defer file.Close()
		if err := json.NewDecoder(io.LimitReader(file, state.IntentHistoryPlanByteCap+1)).Decode(&plan); err != nil { return plan, false, err }
		return plan, plan.SourceBranchRef != "" && plan.Version == state.IntentHistoryPlanVersion, nil
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil { return plan, false, err }
	db, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil { return plan, false, err }
	defer db.Close()
	return state.LoadIntentHistoryPlan(ctx, db, ref)
}

func generateIntentHistoryPlan(ctx context.Context, out io.Writer, repo string, selection git.RewriteSelection, opts rewriteCommitsOptions, provider ai.Provider, cfg ai.ProviderConfig) error {
	if len(selection.RecreateUnchanged) != 0 { return errors.New("acd history rewrite: new-branch reconstruction must select through HEAD") }
	target := opts.newBranch
	if !strings.HasPrefix(target, "refs/heads/") { target = "refs/heads/"+target }
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "check-ref-format", target); err != nil { return err }
	var chain []string
	for _, commit := range selection.Selected { chain = append(chain, commit.OID) }
	plan, err := daemon.PlanIntentHistory(ctx, repo, selection.BranchRef, chain, provider, cfg.CommitFormat, cfg.DiffEgress)
	if err != nil { return err }
	plan.TargetBranchRef = target
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil { return err }
	db, err := state.Open(ctx, dbPath)
	if err != nil { return err }
	defer db.Close()
	plan, err = state.SaveIntentHistoryPlan(ctx, db, plan)
	if err != nil { return err }
	ref := plan.ID
	if opts.planOut != "" {
		file, err := os.OpenFile(opts.planOut, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil { return err }
		encoder := json.NewEncoder(file); encoder.SetIndent("", "  ")
		writeErr := encoder.Encode(plan); closeErr := file.Close()
		if writeErr != nil { return writeErr }; if closeErr != nil { return closeErr }
		ref = opts.planOut
	}
	printIntentHistoryPlan(out, plan)
	if opts.planOnly || opts.dryRun || !opts.yes {
		fmt.Fprintf(out, "Plan saved: %s\nPreview: acd history rewrite --apply %s --dry-run\n", ref, rewritePlanRefArg(ref))
		return nil
	}
	return applyIntentHistoryPlan(ctx, out, repo, plan, false)
}

func printIntentHistoryPlan(out io.Writer, plan state.IntentHistoryPlan) {
	fmt.Fprintf(out, "Intent history reconstruction: %d commits -> %d goals\nNew branch: %s\nOriginal branch: %s (preserved)\n", len(plan.SourceChain), len(plan.Goals), plan.TargetBranchRef, plan.SourceBranchRef)
	for _, goal := range plan.Goals { fmt.Fprintf(out, "- %s\n  Purpose: %s\n  Boundary: %s\n", strings.SplitN(goal.Message, "\n", 2)[0], goal.Purpose, goal.Reason) }
}

func applyIntentHistoryPlan(ctx context.Context, out io.Writer, repo string, plan state.IntentHistoryPlan, dryRun bool) error {
	replacements, err := daemon.ValidateIntentHistoryPlan(ctx, repo, plan)
	if err != nil { return err }
	if _, err := git.ApplyIntentHistoryReconstruction(ctx, repo, git.IntentHistoryReconstructionOptions{SourceBranchRef: plan.SourceBranchRef, TargetBranchRef: plan.TargetBranchRef, ExpectedHead: plan.ExpectedHead, OldChain: plan.SourceChain, Replacements: replacements, PlanID: plan.ID, DryRun: true}); err != nil { return err }
	printIntentHistoryPlan(out, plan)
	if dryRun { fmt.Fprintln(out, "Preview passed. The worker will verify each exact goal before creating the branch."); return nil }
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil { return err }
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil { return err }
	db, err := state.Open(ctx, dbPath)
	if err != nil { return err }
	defer db.Close()
	plan, err = state.SaveIntentHistoryPlan(ctx, db, plan)
	if err != nil { return err }
	if err := state.EnqueueIntentHistoryRequest(ctx, db, plan.ID); err != nil { return err }
	if lookup.Registered && !lookup.Record.LifecycleDisabled() {
		worker, _, loadErr := state.LoadDaemonState(ctx, db)
		if loadErr != nil { return loadErr }
		if worker.PID > 0 { _ = signalProcess(worker.PID, syscall.SIGUSR1, daemonFingerprintToken(worker)) }
		fmt.Fprintf(out, "Queued for the active worker: %s\nCapture protection continues. Run acd history rewrite --show-plan %s to inspect progress.\n", plan.ID, plan.ID)
		return nil
	}
	lock, err := daemon.AcquireDaemonLock(lookup.Worktree.GitDir)
	if err != nil { return err }
	defer lock.Release()
	if _, err := daemon.ProcessIntentHistoryRequest(ctx, repo, db, nil); err != nil { return err }
	request, _, err := state.LoadIntentHistoryRequest(ctx, db)
	if err != nil { return err }
	fmt.Fprintf(out, "Reconstruction complete: %s\nBackup: %s\n", request.NewHead, request.BackupRef)
	return nil
}

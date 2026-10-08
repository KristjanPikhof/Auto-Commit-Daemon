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
		raw, err := io.ReadAll(io.LimitReader(file, state.IntentHistoryPlanByteCap+1))
		if err != nil {
			return plan, false, err
		}
		if len(raw) > state.IntentHistoryPlanByteCap {
			return plan, false, errors.New("acd history rewrite: saved plan exceeds size limit")
		}
		if err := json.Unmarshal(raw, &plan); err != nil {
			return plan, false, err
		}
		return plan, plan.SourceBranchRef != "" && plan.Version == state.IntentHistoryPlanVersion, nil
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return plan, false, err
	}
	db, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return plan, false, err
	}
	defer db.Close()
	return state.LoadIntentHistoryPlan(ctx, db, ref)
}

func generateIntentHistoryPlan(ctx context.Context, out io.Writer, repo string, selection git.RewriteSelection, opts rewriteCommitsOptions, provider ai.Provider, cfg ai.ProviderConfig, jsonOut bool) error {
	if len(selection.RecreateUnchanged) != 0 {
		return errors.New("acd history rewrite: new-branch reconstruction must select through HEAD")
	}
	target := opts.newBranch
	if !strings.HasPrefix(target, "refs/heads/") {
		target = "refs/heads/" + target
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "check-ref-format", target); err != nil {
		return err
	}
	if target == selection.BranchRef {
		return errors.New("acd history rewrite: --new-branch must preserve the source branch")
	}
	if _, err := git.RevParse(ctx, repo, target); err == nil {
		return errors.New("acd history rewrite: target branch already exists; choose a new branch")
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return err
	}
	if opts.planOut == "" {
		version, err := state.ReadUserVersion(ctx, dbPath)
		if err != nil || version != state.SchemaVersion {
			return errors.New("acd history rewrite: use --plan-out FILE for a standalone preview until ACD is set up at the current schema; planning does not migrate an active worker")
		}
	}
	var chain []string
	for _, commit := range selection.Selected {
		chain = append(chain, commit.OID)
	}
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}
	plan, err := daemon.PlanIntentHistory(ctx, repo, selection.BranchRef, chain, provider, cfg.CommitFormat, cfg.DiffEgress)
	if err != nil {
		return err
	}
	plan.TargetBranchRef = target
	if opts.planOut != "" {
		plan, err = state.PrepareIntentHistoryPlan(plan)
	} else {
		db, openErr := state.OpenRuntime(ctx, dbPath)
		if openErr != nil {
			return openErr
		}
		plan, err = state.SaveIntentHistoryPlan(ctx, db, plan)
		db.Close()
	}
	if err != nil {
		return err
	}
	ref := plan.ID
	if opts.planOut != "" {
		file, err := os.OpenFile(opts.planOut, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		writeErr := encoder.Encode(plan)
		closeErr := file.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
		ref = opts.planOut
	}
	if !jsonOut {
		printIntentHistoryPlan(out, plan)
	}
	if opts.planOnly || opts.dryRun || !opts.yes {
		if jsonOut {
			return json.NewEncoder(out).Encode(plan)
		}
		fmt.Fprintf(out, "Plan saved: %s\nPreview: acd history rewrite --apply %s --dry-run\n", ref, rewritePlanRefArg(ref))
		return nil
	}
	if jsonOut {
		if err := applyIntentHistoryPlan(ctx, io.Discard, repo, plan, false); err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(plan)
	}
	return applyIntentHistoryPlan(ctx, out, repo, plan, false)
}

func printIntentHistoryPlan(out io.Writer, plan state.IntentHistoryPlan) {
	fmt.Fprintf(out, "Intent history reconstruction: %d commits -> %d goals\nNew branch: %s\nOriginal branch: %s (preserved)\n", len(plan.SourceChain), len(plan.Goals), plan.TargetBranchRef, plan.SourceBranchRef)
	for _, goal := range plan.Goals {
		fmt.Fprintf(out, "- %s\n  Purpose: %s\n  Boundary: %s\n", strings.SplitN(goal.Message, "\n", 2)[0], goal.Purpose, goal.Reason)
	}
}

func showIntentHistoryPlan(ctx context.Context, out io.Writer, repo string, plan state.IntentHistoryPlan, jsonOut, previewPassed bool) error {
	view := struct {
		state.IntentHistoryPlan
		WorkerRequest *state.IntentHistoryRequest `json:"worker_request,omitempty"`
		PreviewPassed bool                        `json:"preview_passed,omitempty"`
	}{IntentHistoryPlan: plan, PreviewPassed: previewPassed}
	if path, err := rewriteStateDBPath(ctx, repo); err == nil {
		if db, err := state.OpenReadOnly(ctx, path); err == nil {
			defer db.Close()
			if request, ok, err := state.LoadIntentHistoryRequest(ctx, db); err == nil && ok && request.PlanID == plan.ID {
				view.WorkerRequest = &request
			}
		}
	}
	if jsonOut {
		return json.NewEncoder(out).Encode(view)
	}
	printIntentHistoryPlan(out, plan)
	if request := view.WorkerRequest; request != nil {
		fmt.Fprintf(out, "Worker status: %s\n", request.Status)
		if request.Error != "" {
			fmt.Fprintln(out, request.Error)
		}
		if request.NewHead != "" {
			fmt.Fprintf(out, "New HEAD: %s\nBackup: %s\n", request.NewHead, request.BackupRef)
		}
	}
	return nil
}

func applyIntentHistoryPlan(ctx context.Context, out io.Writer, repo string, plan state.IntentHistoryPlan, dryRun bool) error {
	replacements, err := daemon.ValidateIntentHistoryPlan(ctx, repo, plan)
	if err != nil {
		return err
	}
	if _, err := git.ApplyIntentHistoryReconstruction(ctx, repo, git.IntentHistoryReconstructionOptions{SourceBranchRef: plan.SourceBranchRef, TargetBranchRef: plan.TargetBranchRef, ExpectedHead: plan.ExpectedHead, OldChain: plan.SourceChain, Replacements: replacements, PlanID: plan.ID, DryRun: true}); err != nil {
		return err
	}
	printIntentHistoryPlan(out, plan)
	if dryRun {
		fmt.Fprintln(out, "Preview passed. The worker will verify each exact goal before creating the branch.")
		return nil
	}
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil {
		return err
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return err
	}
	if !lookup.Registered || lookup.Record.LifecycleDisabled() {
		return errors.New("acd history rewrite: goal reconstruction requires an active worker to run the repository's approved verification; run acd on first")
	}
	readDB, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return err
	}
	defer readDB.Close()
	worker, _, err := state.LoadDaemonState(ctx, readDB)
	if err != nil {
		return err
	}
	var capability state.IntentHistoryWorker
	ok, err := state.MetaGetJSON(ctx, readDB, state.MetaKeyIntentHistoryWorker, &capability)
	if err != nil {
		return err
	}
	if !ok || capability.Protocol != state.IntentHistoryPlanVersion || capability.PID != worker.PID || capability.Fingerprint != daemonFingerprintToken(worker) || worker.PID <= 0 {
		return errors.New("acd history rewrite: the active worker does not support goal reconstruction; rebuild and restart ACD before applying")
	}
	version, err := state.ReadUserVersion(ctx, dbPath)
	if err != nil {
		return err
	}
	if version != state.SchemaVersion {
		return errors.New("acd history rewrite: active worker schema does not match the reconstruction protocol; run setup and restart first")
	}
	db, err := state.OpenRuntime(ctx, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	plan, err = state.SaveIntentHistoryPlan(ctx, db, plan)
	if err != nil {
		return err
	}
	if err := state.EnqueueIntentHistoryRequest(ctx, db, plan.ID); err != nil {
		return err
	}
	if err := signalProcess(worker.PID, syscall.SIGUSR1, daemonFingerprintToken(worker)); err != nil {
		return err
	}
	fmt.Fprintf(out, "Queued for the active worker: %s\nCapture protection continues. Run acd history rewrite --show-plan %s to inspect progress.\n", plan.ID, plan.ID)
	return nil
}

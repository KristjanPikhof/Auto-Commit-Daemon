package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

// PlanIntentHistory uses the live goal contract on exact historical versions.
// It does not alter the worktree, index, branch, or capture ledger.
func PlanIntentHistory(ctx context.Context, repo, branch string, chain []string, planner interface{ Name() string }, format ai.CommitFormat, includeDiffs bool) (state.IntentHistoryPlan, error) {
	var result state.IntentHistoryPlan
	units, err := git.ReadIntentHistoryUnits(ctx, repo, chain)
	if err != nil { return result, err }
	if len(units) == 0 { return result, errors.New("intent history: selection has no captured path changes") }
	parent, err := git.RevParse(ctx, repo, chain[0]+"^")
	if err != nil { return result, err }
	baseTree, err := git.RevParse(ctx, repo, parent+"^{tree}")
	if err != nil { return result, err }
	// A large history is represented by complete path chains, not by equally
	// clipped commit messages. Every underlying transition retains ownership.
	batches := make([][]int, 0, len(units))
	if len(units) <= ai.IntentCandidateCaptureCap {
		for i := range units { batches = append(batches, []int{i}) }
	} else {
		byPath := make(map[string]int)
		for i, unit := range units {
			index, ok := byPath[unit.Path]
			if !ok { index = len(batches); byPath[unit.Path] = index; batches = append(batches, nil) }
			batches[index] = append(batches[index], i)
		}
	}
	if len(batches) > ai.IntentCandidateCaptureCap { return result, fmt.Errorf("intent history: %d independent path chains exceed the focused planning limit %d", len(batches), ai.IntentCandidateCaptureCap) }
	var captures []IntentCandidateCapture
	var offered []ai.OfferedCapture
	diffBudget := ai.HistoryRewriteTotalDiffCap
	for i, batch := range batches {
		first, last := units[batch[0]], units[batch[len(batch)-1]]
		op := "modify"
		if first.Before.OID == "" { op = "create" }
		if last.After.OID == "" { op = "delete" }
		diff := ""
		if includeDiffs && diffBudget > 0 {
			before := parent
			if len(batch) == 1 { before = first.OldOID+"^" }
			raw, readErr := git.RunWithLimit(ctx, git.RunOpts{Dir: repo}, 256<<10,
				"diff", "--no-ext-diff", "--unified=3", before, last.OldOID, "--", first.Path)
			if readErr != nil { return result, readErr }
			cap := min(ai.IntentStageDiffCap, max(256, diffBudget/(len(batches)-i)))
			diff = ai.Truncate(ai.RedactDiffSecrets(string(raw)), cap)
			diffBudget -= len(diff)
		}
		seq := int64(i+1)
		capture := IntentCandidateCapture{Event: state.CaptureEvent{Seq: seq, BranchRef: branch, BranchGeneration: 0, Path: first.Path, Operation: op}, CapturedDiff: diff}
		captures = append(captures, capture)
		offered = append(offered, ai.OfferedCapture{Seq: seq, Path: first.Path, Op: op, Fidelity: "recorded_history", CapturedDiff: diff})
	}
	edges, err := BuildIntentCandidateDependencies(branch, 0, captures, runtimeIntentDependencyHints(captures), time.Now())
	if err != nil { return result, err }
	var dependencies []ai.IntentCaptureDependency
	for _, edge := range edges { dependencies = append(dependencies, ai.IntentCaptureDependency{FromSeq: edge.PrerequisiteSeq, ToSeq: edge.DependentSeq, Strength: ai.IntentDependencyStrength(edge.Strength), Kind: edge.Kind, EvidenceHash: edge.Evidence}) }
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{OfferedCaptures: offered, Dependencies: dependencies, IncludeCapturedDiffs: includeDiffs, CommitFormat: format})
	if err != nil { return result, err }
	var plan ai.IntentPlanV2
	var trees []string
	var selected [][]git.IntentHistoryUnit
	for attempt := 0; attempt < 3; attempt++ {
		plan, err = ai.PlanIntentV2WithCompatibility(ctx, planner, req)
		if err == nil { err = ValidateIntentGoalPlan(req, plan) }
		selected = nil
		if err == nil {
			for _, candidate := range plan.Candidates {
				if candidate.Readiness != ai.IntentCandidateReady || len(candidate.MissingCompanions) != 0 { err = errors.New("intent history: a proposed goal remains incomplete"); break }
				var indexes []int
				for _, seq := range candidate.SelectedSeqs { indexes = append(indexes, batches[int(seq)-1]...) }
				sort.Ints(indexes)
				var group []git.IntentHistoryUnit
				for _, index := range indexes { group = append(group, units[index]) }
				selected = append(selected, group)
			}
		}
		if err == nil { trees, err = git.MaterializeIntentHistoryUnits(ctx, repo, baseTree, units, selected) }
		if err == nil { break }
		if ctx.Err() != nil { return result, ctx.Err() }
		if kind, _ := classifyIntentPlannerFailure(err); kind == IntentPlannerFailureTransport { return result, err }
		req.RetryCorrection = ai.SanitizePlannerError(err.Error())
	}
	if err != nil { return result, err }
	finalTree, err := git.RevParse(ctx, repo, chain[len(chain)-1]+"^{tree}")
	if err != nil { return result, err }
	if len(trees) == 0 || trees[len(trees)-1] != finalTree { return result, errors.New("intent history: proposed goals do not preserve the final tree") }
	result = state.IntentHistoryPlan{Version: state.IntentHistoryPlanVersion, SourceBranchRef: branch, ExpectedHead: chain[len(chain)-1], BaseTree: baseTree, SourceChain: append([]string(nil), chain...)}
	for _, unit := range units { result.Units = append(result.Units, stateHistoryUnit(unit)) }
	for i, candidate := range plan.Candidates {
		var indexes []int
		for _, seq := range candidate.SelectedSeqs { indexes = append(indexes, batches[int(seq)-1]...) }
		sort.Ints(indexes)
		message := candidate.Subject
		if candidate.Body != "" { message += "\n\n"+candidate.Body }
		result.Goals = append(result.Goals, state.IntentHistoryGoal{ID: candidate.CandidateID, Purpose: candidate.Purpose, Message: message, Reason: candidate.GroupingReason, Units: indexes, TreeOID: trees[i]})
	}
	return result, nil
}

func stateHistoryUnit(unit git.IntentHistoryUnit) state.IntentHistoryUnit {
	return state.IntentHistoryUnit{OldOID: unit.OldOID, Path: unit.Path, Before: state.IntentHistoryVersion{OID: unit.Before.OID, Mode: unit.Before.Mode}, After: state.IntentHistoryVersion{OID: unit.After.OID, Mode: unit.After.Mode}}
}

// ValidateIntentHistoryPlan rederives original units and exact candidate trees.
// Saved plans cannot inject versions, change membership, or hide a stale HEAD.
func ValidateIntentHistoryPlan(ctx context.Context, repo string, plan state.IntentHistoryPlan) ([]git.IntentRepairReplacement, error) {
	if plan.Version != state.IntentHistoryPlanVersion || len(plan.Goals) == 0 { return nil, errors.New("intent history: invalid saved plan") }
	original, err := git.ReadIntentHistoryUnits(ctx, repo, plan.SourceChain)
	if err != nil { return nil, err }
	var recorded []state.IntentHistoryUnit
	for _, unit := range original { recorded = append(recorded, stateHistoryUnit(unit)) }
	if !reflect.DeepEqual(recorded, plan.Units) { return nil, errors.New("intent history: source provenance differs from the saved plan") }
	var groups [][]git.IntentHistoryUnit
	var replacements []git.IntentRepairReplacement
	for _, goal := range plan.Goals {
		var group []git.IntentHistoryUnit
		var replaces []string
		seen := make(map[string]bool)
		for _, index := range goal.Units {
			if index < 0 || index >= len(original) { return nil, errors.New("intent history: unknown recorded path transition") }
			unit := original[index]; group = append(group, unit)
			if !seen[unit.OldOID] { replaces = append(replaces, unit.OldOID); seen[unit.OldOID] = true }
		}
		if goal.ID == "" || strings.TrimSpace(goal.Purpose) == "" || strings.TrimSpace(goal.Message) == "" { return nil, errors.New("intent history: goal purpose and message are required") }
		groups = append(groups, group)
		replacements = append(replacements, git.IntentRepairReplacement{Replaces: replaces, TreeOID: goal.TreeOID, Message: goal.Message})
	}
	trees, err := git.MaterializeIntentHistoryUnits(ctx, repo, plan.BaseTree, original, groups)
	if err != nil { return nil, err }
	for i, tree := range trees { if tree != plan.Goals[i].TreeOID { return nil, errors.New("intent history: saved goal tree differs from its recorded transitions") } }
	return replacements, nil
}

// ProcessIntentHistoryRequest is called by the sole writer after capture.
// Verification suspends publication through evaluatePublication while the
// worker continues checkpointing later changes outside the frozen request.
func ProcessIntentHistoryRequest(ctx context.Context, repo string, db *state.DB, bundle *RuntimeBundle) (bool, error) {
	request, ok, err := state.LoadIntentHistoryRequest(ctx, db)
	if err != nil || !ok || request.Status != "pending" && request.Status != "running" { return false, err }
	plan, ok, err := state.LoadIntentHistoryPlan(ctx, db, request.PlanID)
	if err == nil && !ok { err = errors.New("intent history: queued plan is missing") }
	var result git.IntentRepairApplyResult
	if err == nil {
		request.Status = "running"
		if err = state.SaveIntentHistoryRequest(ctx, db, request); err != nil { return true, err }
		var replacements []git.IntentRepairReplacement
		replacements, err = ValidateIntentHistoryPlan(ctx, repo, plan)
		if err == nil {
			verify := func(callCtx context.Context, oid string, index int) error {
				_, checkErr := evaluatePublication(callCtx, func(jobCtx context.Context) (verification.Result, error) {
					var check verification.Result
					var runErr error
					if bundle != nil && bundle.IntentVerificationReady {
						check, runErr = (verification.Runner{}).Run(jobCtx, verification.Request{RepoPath: repo, CandidateID: plan.Goals[index].ID, CommitOID: oid, Command: bundle.IntentVerificationCommand})
					} else {
						if bundle != nil && bundle.IntentVerificationMode == "full" { return check, errors.New("intent history: required verification command is unavailable") }
						check, runErr = (verification.Runner{}).CheckStructural(jobCtx, verification.StructuralRequest{RepoPath: repo, CandidateID: plan.Goals[index].ID, CommitOID: oid})
					}
					if runErr == nil && check.NeedsAttention { runErr = fmt.Errorf("intent history: goal verification failed: %s", check.Output) }
					return check, runErr
				})
				return checkErr
			}
			result, err = git.ApplyIntentHistoryReconstruction(ctx, repo, git.IntentHistoryReconstructionOptions{SourceBranchRef: plan.SourceBranchRef, TargetBranchRef: plan.TargetBranchRef, ExpectedHead: plan.ExpectedHead, OldChain: plan.SourceChain, Replacements: replacements, PlanID: plan.ID, VerifyCommit: verify})
		}
	}
	if ctx.Err() != nil { return true, ctx.Err() }
	request.Status = "completed"
	if err != nil { request.Status = "failed"; request.Error = ai.SanitizePlannerError(err.Error()) } else { request.NewHead = result.NewHead; request.BackupRef = result.BackupRef }
	if saveErr := state.SaveIntentHistoryRequest(ctx, db, request); saveErr != nil { return true, saveErr }
	return true, err
}

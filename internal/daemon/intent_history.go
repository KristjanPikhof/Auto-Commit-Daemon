package daemon

import (
	"context"
	"database/sql"
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
	budget := runtimeTelemetryFromContext(ctx).providerTimeout
	if budget <= 0 {
		budget = ai.LoadProviderConfigFromEnv().Timeout
	}
	if budget <= 0 {
		budget = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	units, err := git.ReadIntentHistoryUnits(ctx, repo, chain)
	if err != nil {
		return result, err
	}
	if len(units) == 0 {
		return result, errors.New("intent history: selection has no captured path changes")
	}
	baseTree, err := git.IntentHistoryBaseTree(ctx, repo, chain[0])
	if err != nil {
		return result, err
	}
	batches, req, err := intentHistoryEvidence(ctx, repo, branch, baseTree, chain[len(chain)-1], units, format, includeDiffs)
	if err != nil {
		return result, err
	}
	var plan ai.IntentPlanV2
	var trees []string
	var selected [][]git.IntentHistoryUnit
	for attempt := 0; attempt < 3; attempt++ {
		plan, err = ai.PlanIntentV2WithCompatibility(ctx, planner, req)
		plannerCallFailed := err != nil
		if err == nil {
			err = ValidateIntentGoalPlan(req, plan)
		}
		selected = nil
		if err == nil {
			for _, candidate := range plan.Candidates {
				if candidate.Readiness != ai.IntentCandidateReady || len(candidate.MissingCompanions) != 0 {
					err = errors.New("intent history: a proposed goal remains incomplete")
					break
				}
				var indexes []int
				for _, seq := range candidate.SelectedSeqs {
					indexes = append(indexes, batches[int(seq)-1]...)
				}
				sort.Ints(indexes)
				var group []git.IntentHistoryUnit
				for _, index := range indexes {
					group = append(group, units[index])
				}
				selected = append(selected, group)
			}
		}
		if err == nil {
			trees, err = git.MaterializeIntentHistoryUnits(ctx, repo, baseTree, units, selected)
		}
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if kind, _ := classifyIntentPlannerFailure(classifyIntentPlannerHealthFailure(err, plannerCallFailed)); kind == IntentPlannerFailureTransport {
			return result, classifyIntentPlannerHealthFailure(err, plannerCallFailed)
		}
		err = classifyIntentPlannerHealthFailure(err, plannerCallFailed)
		req.RetryCorrection = ai.SanitizePlannerError(err.Error())
	}
	if err != nil {
		return result, err
	}
	finalTree, err := git.RevParse(ctx, repo, chain[len(chain)-1]+"^{tree}")
	if err != nil {
		return result, err
	}
	if len(trees) == 0 || trees[len(trees)-1] != finalTree {
		return result, errors.New("intent history: proposed goals do not preserve the final tree")
	}
	result = state.IntentHistoryPlan{Version: state.IntentHistoryPlanVersion, SourceBranchRef: branch, ExpectedHead: chain[len(chain)-1], BaseTree: baseTree, SourceChain: append([]string(nil), chain...), CommitFormat: string(format)}
	for _, unit := range units {
		result.Units = append(result.Units, stateHistoryUnit(unit))
	}
	for i, candidate := range plan.Candidates {
		var indexes []int
		for _, seq := range candidate.SelectedSeqs {
			indexes = append(indexes, batches[int(seq)-1]...)
		}
		sort.Ints(indexes)
		message := candidate.Subject
		if candidate.Body != "" {
			message += "\n\n" + candidate.Body
		}
		result.Goals = append(result.Goals, state.IntentHistoryGoal{ID: candidate.CandidateID, Purpose: candidate.Purpose, Message: message, Reason: candidate.GroupingReason, Units: indexes, TreeOID: trees[i], DependsOnCandidates: append([]string(nil), candidate.DependsOnCandidates...)})
	}
	return result, nil
}

func intentHistoryEvidence(ctx context.Context, repo, branch, baseTree, sourceHead string, units []git.IntentHistoryUnit, format ai.CommitFormat, includeDiffs bool) ([][]int, ai.IntentPlanRequestV2, error) {
	// A large history is represented by complete path chains, not by equally
	// clipped commit messages. Every underlying transition retains ownership.
	batches := make([][]int, 0, len(units))
	if len(units) <= ai.IntentCandidateCaptureCap {
		for i := range units {
			batches = append(batches, []int{i})
		}
	} else {
		byPath := make(map[string]int)
		for i, unit := range units {
			index, ok := byPath[unit.Path]
			if !ok {
				index = len(batches)
				byPath[unit.Path] = index
				batches = append(batches, nil)
			}
			batches[index] = append(batches[index], i)
		}
	}
	if len(batches) > ai.IntentCandidateCaptureCap {
		return nil, ai.IntentPlanRequestV2{}, fmt.Errorf("intent history: %d independent path chains exceed the focused planning limit %d", len(batches), ai.IntentCandidateCaptureCap)
	}
	var captures []IntentCandidateCapture
	var offered []ai.OfferedCapture
	var err error
	var offeredPaths []string
	for _, batch := range batches {
		offeredPaths = append(offeredPaths, units[batch[0]].Path)
	}
	diffs := make([]string, len(batches))
	for i, batch := range batches {
		if !includeDiffs {
			break
		}
		first, last := units[batch[0]], units[batch[len(batch)-1]]
		before := baseTree
		if len(batch) == 1 {
			before, err = git.IntentHistoryBaseTree(ctx, repo, first.OldOID)
			if err != nil {
				return nil, ai.IntentPlanRequestV2{}, err
			}
		}
		raw, err := git.RunWithLimit(ctx, git.RunOpts{Dir: repo}, 256<<10, "diff", "--no-ext-diff", "--unified=3", before, last.OldOID, "--", first.Path)
		if err != nil {
			return nil, ai.IntentPlanRequestV2{}, err
		}
		references, err := loadIntentRecordedReferenceContext(ctx, repo, last.Path, last.After.OID, last.After.Mode, offeredPaths)
		if err != nil {
			return nil, ai.IntentPlanRequestV2{}, err
		}
		diffs[i] = prependIntentRecordedReferenceContext(string(raw), references)
	}
	var relationshipCaptures []IntentCandidateCapture
	for i, batch := range batches {
		relationshipCaptures = append(relationshipCaptures, IntentCandidateCapture{
			Event: state.CaptureEvent{Seq: int64(i + 1), Path: units[batch[0]].Path}, CapturedDiff: diffs[i],
		})
	}
	if includeDiffs {
		names := intentOtherCaptureReferenceNames(relationshipCaptures)
		for i, batch := range batches {
			last := units[batch[len(batch)-1]]
			if !strings.HasSuffix(last.Path, ".go") {
				continue
			}
			references, err := loadIntentRecordedReferenceContext(ctx, repo, last.Path, last.After.OID, last.After.Mode, offeredPaths, names)
			if err != nil {
				return nil, ai.IntentPlanRequestV2{}, err
			}
			relationshipCaptures[i].CapturedDiff = prependIntentRecordedReferenceContext(relationshipCaptures[i].CapturedDiff, references)
		}
	}
	diffs = prioritizeIntentRelationshipEvidence(relationshipCaptures)
	diffs = allocateIntentEvidenceDiffs(diffs, ai.HistoryRewriteTotalDiffCap)
	for i, batch := range batches {
		first, last := units[batch[0]], units[batch[len(batch)-1]]
		op := "modify"
		if first.Before.OID == "" {
			op = "create"
		}
		if last.After.OID == "" {
			op = "delete"
		}
		diff := diffs[i]
		seq := int64(i + 1)
		capture := IntentCandidateCapture{Event: state.CaptureEvent{Seq: seq, BranchRef: branch, BranchGeneration: 0, Path: first.Path, Operation: op}, CapturedDiff: diff}
		// Only individual recorded transitions can prove a normalization.
		// A net path change must not hide substantive intermediate versions.
		if len(batch) == 1 {
			capture.Event.State = state.EventStatePending
			capture.Ops = []state.CaptureOp{{EventSeq: seq, Op: op, Path: first.Path,
				BeforeOID:  sql.NullString{String: first.Before.OID, Valid: first.Before.OID != ""},
				AfterOID:   sql.NullString{String: last.After.OID, Valid: last.After.OID != ""},
				BeforeMode: sql.NullString{String: first.Before.Mode, Valid: first.Before.Mode != ""},
				AfterMode:  sql.NullString{String: last.After.Mode, Valid: last.After.Mode != ""}}}
		}
		captures = append(captures, capture)
		offered = append(offered, ai.OfferedCapture{Seq: seq, Path: first.Path, Op: op, Fidelity: "recorded_history", CapturedDiff: diff})
	}
	captures, err = proveIntentSwiftBlankLineMaintenance(ctx, repo, captures)
	if err != nil {
		return nil, ai.IntentPlanRequestV2{}, err
	}
	// CLI references describe the final registered command. Unlike a
	// maintenance proof, they can use a complete, continuous path chain.
	cliCaptures := append([]IntentCandidateCapture(nil), captures...)
	for i, batch := range batches {
		first, last := units[batch[0]], units[batch[len(batch)-1]]
		cliCaptures[i].Event.State = state.EventStatePending
		cliCaptures[i].Ops = []state.CaptureOp{{EventSeq: cliCaptures[i].Event.Seq, Op: cliCaptures[i].Event.Operation, Path: first.Path,
			BeforeOID:  sql.NullString{String: first.Before.OID, Valid: first.Before.OID != ""},
			AfterOID:   sql.NullString{String: last.After.OID, Valid: last.After.OID != ""},
			BeforeMode: sql.NullString{String: first.Before.Mode, Valid: first.Before.Mode != ""},
			AfterMode:  sql.NullString{String: last.After.Mode, Valid: last.After.Mode != ""}}}
	}
	cliCaptures, err = proveIntentCobraCLIReferences(ctx, repo, sourceHead, cliCaptures)
	if err != nil {
		return nil, ai.IntentPlanRequestV2{}, err
	}
	for i := range captures {
		captures[i].FileMetadata = cliCaptures[i].FileMetadata
		offered[i].FileMetadata = captures[i].FileMetadata
	}
	edges, err := BuildIntentCandidateDependencies(branch, 0, captures, runtimeIntentDependencyHints(captures), time.Now())
	if err != nil {
		return nil, ai.IntentPlanRequestV2{}, err
	}
	var chain []string
	seenCommits := map[string]bool{}
	unitSeq := map[string]int64{}
	for i, batch := range batches {
		for _, index := range batch {
			unitSeq[units[index].OldOID+"\x00"+units[index].Path] = int64(i + 1)
		}
	}
	for _, unit := range units {
		if !seenCommits[unit.OldOID] {
			chain = append(chain, unit.OldOID)
			seenCommits[unit.OldOID] = true
		}
	}
	renames, err := git.ReadIntentHistoryRenamePairs(ctx, repo, chain)
	if err != nil {
		return nil, ai.IntentPlanRequestV2{}, err
	}
	for _, rename := range renames {
		left, right := unitSeq[rename.OldOID+"\x00"+rename.BeforePath], unitSeq[rename.OldOID+"\x00"+rename.AfterPath]
		if left == right {
			continue
		}
		if left > right {
			left, right = right, left
		}
		edges = append(edges, state.IntentCaptureDependency{PrerequisiteSeq: left, DependentSeq: right, Strength: state.IntentDependencyHard, Kind: "recorded_rename", Evidence: intentEvidenceHash(rename.OldOID + rename.BeforePath + rename.AfterPath)})
	}
	var dependencies []ai.IntentCaptureDependency
	for _, edge := range edges {
		dependencies = append(dependencies, ai.IntentCaptureDependency{FromSeq: edge.PrerequisiteSeq, ToSeq: edge.DependentSeq, Strength: ai.IntentDependencyStrength(edge.Strength), Kind: edge.Kind, EvidenceHash: edge.Evidence})
	}
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{OfferedCaptures: offered, Dependencies: dependencies, IncludeCapturedDiffs: includeDiffs, CommitFormat: format})
	if err != nil {
		return nil, ai.IntentPlanRequestV2{}, err
	}
	return batches, req, err
}

// Keep small companion changes complete before spending the remaining budget
// on larger changes. Equal clipping can hide an import or a late correction.
func allocateIntentEvidenceDiffs(raw []string, budget int) []string {
	result := make([]string, len(raw))
	priorities := make([]string, len(raw))
	bodies := make([]string, len(raw))
	limits := make([]int, len(raw))
	order := make([]int, len(raw))
	for i, diff := range raw {
		order[i] = i
		priorities[i], bodies[i] = splitIntentEvidencePriority(diff)
		if len(priorities[i]) <= max(0, budget) {
			budget -= len(priorities[i])
		} else {
			// Never emit a partial ownership/import signature. Missing evidence
			// remains an honest planning wait, not permission to exceed the cap.
			priorities[i] = ""
		}
	}
	for i, body := range bodies {
		limits[i] = min(len(body), 512, max(0, budget))
		budget -= limits[i]
	}
	sort.SliceStable(order, func(i, j int) bool { return len(bodies[order[i]]) < len(bodies[order[j]]) })
	for _, i := range order {
		extra := min(len(bodies[i])-limits[i], max(0, budget))
		limits[i] += extra
		budget -= extra
	}
	for i, limit := range limits {
		result[i] = priorities[i] + truncateIntentEvidenceDiff(bodies[i], limit)
	}
	return result
}

func splitIntentEvidencePriority(diff string) (string, string) {
	if !strings.HasPrefix(diff, "Recorded post-image references:\n") &&
		!strings.HasPrefix(diff, "Recorded relationship witnesses:\n") &&
		!strings.HasPrefix(diff, "Recorded changed relationship witnesses:\n") {
		return "", diff
	}
	const separator = "\nRecorded diff:\n"
	if end := strings.LastIndex(diff, separator); end >= 0 {
		end += len(separator)
		return diff[:end], diff[end:]
	}
	return "", diff
}

// Keep witnessed references and early declarations as well as late corrections.
// Generic message clipping retains metadata and the tail, which can erase every
// relationship in a large captured change. Never create a partial code witness.
func truncateIntentEvidenceDiff(diff string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(diff) <= limit {
		return diff
	}
	const marker = "\n... <truncated> ...\n"
	if limit <= len(marker) {
		return ""
	}
	headLimit := (limit - len(marker)) / 2
	head := diff[:headLimit]
	if end := strings.LastIndexByte(head, '\n'); end >= 0 {
		head = head[:end+1]
	} else {
		head = ""
	}
	tail := diff[len(diff)-(limit-len(head)-len(marker)):]
	if start := strings.IndexByte(tail, '\n'); start >= 0 {
		tail = tail[start+1:]
	} else {
		tail = ""
	}
	return head + marker + tail
}

func stateHistoryUnit(unit git.IntentHistoryUnit) state.IntentHistoryUnit {
	return state.IntentHistoryUnit{OldOID: unit.OldOID, Path: unit.Path, Before: state.IntentHistoryVersion{OID: unit.Before.OID, Mode: unit.Before.Mode}, After: state.IntentHistoryVersion{OID: unit.After.OID, Mode: unit.After.Mode}}
}

// ValidateIntentHistoryPlan rederives original units and exact candidate trees.
// Saved plans cannot inject versions, change membership, or hide a stale HEAD.
func ValidateIntentHistoryPlan(ctx context.Context, repo string, plan state.IntentHistoryPlan) ([]git.IntentRepairReplacement, error) {
	if plan.Version != state.IntentHistoryPlanVersion || len(plan.Goals) == 0 {
		return nil, errors.New("intent history: invalid saved plan")
	}
	original, err := git.ReadIntentHistoryUnits(ctx, repo, plan.SourceChain)
	if err != nil {
		return nil, err
	}
	var recorded []state.IntentHistoryUnit
	for _, unit := range original {
		recorded = append(recorded, stateHistoryUnit(unit))
	}
	if !reflect.DeepEqual(recorded, plan.Units) {
		return nil, errors.New("intent history: source provenance differs from the saved plan")
	}
	if len(plan.SourceChain) == 0 || plan.ExpectedHead != plan.SourceChain[len(plan.SourceChain)-1] {
		return nil, errors.New("intent history: invalid frozen source head")
	}
	baseTree, err := git.IntentHistoryBaseTree(ctx, repo, plan.SourceChain[0])
	if err != nil {
		return nil, err
	}
	if baseTree != plan.BaseTree {
		return nil, errors.New("intent history: saved base differs from source parent")
	}
	batches, req, err := intentHistoryEvidence(ctx, repo, plan.SourceBranchRef, baseTree, plan.ExpectedHead, original, ai.CommitFormat(plan.CommitFormat), true)
	if err != nil {
		return nil, err
	}
	unitBatch := make(map[int]int64, len(original))
	for i, batch := range batches {
		for _, index := range batch {
			unitBatch[index] = int64(i + 1)
		}
	}
	semantic := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	var groups [][]git.IntentHistoryUnit
	var replacements []git.IntentRepairReplacement
	for _, goal := range plan.Goals {
		var group []git.IntentHistoryUnit
		var replaces []string
		seen := make(map[string]bool)
		for _, index := range goal.Units {
			if index < 0 || index >= len(original) {
				return nil, errors.New("intent history: unknown recorded path transition")
			}
			unit := original[index]
			group = append(group, unit)
			if !seen[unit.OldOID] {
				replaces = append(replaces, unit.OldOID)
				seen[unit.OldOID] = true
			}
		}
		if goal.ID == "" || strings.TrimSpace(goal.Purpose) == "" || strings.TrimSpace(goal.Message) == "" {
			return nil, errors.New("intent history: goal purpose and message are required")
		}
		selected := make(map[int64]int)
		for _, index := range goal.Units {
			selected[unitBatch[index]]++
		}
		var seqs []int64
		for seq, count := range selected {
			if count != len(batches[seq-1]) {
				return nil, errors.New("intent history: saved goal splits a focused path chain")
			}
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		parts := strings.SplitN(goal.Message, "\n\n", 2)
		body := ""
		if len(parts) == 2 {
			body = parts[1]
		}
		semantic.Candidates = append(semantic.Candidates, ai.IntentCandidateAssignment{CandidateID: goal.ID, SelectedSeqs: seqs, Purpose: goal.Purpose, Readiness: ai.IntentCandidateReady, Subject: parts[0], Body: body, GroupingReason: goal.Reason, DependsOnCandidates: append([]string(nil), goal.DependsOnCandidates...)})
		groups = append(groups, group)
		replacements = append(replacements, git.IntentRepairReplacement{Replaces: replaces, TreeOID: goal.TreeOID, Message: goal.Message})
	}
	if err := ValidateIntentGoalPlan(req, semantic); err != nil {
		return nil, err
	}
	trees, err := git.MaterializeIntentHistoryUnits(ctx, repo, plan.BaseTree, original, groups)
	if err != nil {
		return nil, err
	}
	for i, tree := range trees {
		if tree != plan.Goals[i].TreeOID {
			return nil, errors.New("intent history: saved goal tree differs from its recorded transitions")
		}
	}
	return replacements, nil
}

// ProcessIntentHistoryRequest is called by the sole writer after capture.
// Verification suspends publication through evaluatePublication while the
// worker continues checkpointing later changes outside the frozen request.
func ProcessIntentHistoryRequest(ctx context.Context, repo string, db *state.DB, bundle *RuntimeBundle) (bool, error) {
	request, ok, err := state.LoadIntentHistoryRequest(ctx, db)
	if err != nil || !ok || request.Status != "pending" && request.Status != "running" {
		return false, err
	}
	plan, ok, err := state.LoadIntentHistoryPlan(ctx, db, request.PlanID)
	if err == nil && !ok {
		err = errors.New("intent history: queued plan is missing")
	}
	var result git.IntentRepairApplyResult
	if err == nil {
		request.Status = "running"
		if err = state.SaveIntentHistoryRequest(ctx, db, request); err != nil {
			return true, err
		}
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
						if bundle != nil && bundle.IntentVerificationMode == "full" {
							return check, errors.New("intent history: required verification command is unavailable")
						}
						check, runErr = (verification.Runner{}).CheckStructural(jobCtx, verification.StructuralRequest{RepoPath: repo, CandidateID: plan.Goals[index].ID, CommitOID: oid})
					}
					if runErr == nil && check.NeedsAttention {
						runErr = fmt.Errorf("intent history: goal verification failed: %s", check.Output)
					}
					return check, runErr
				})
				return checkErr
			}
			result, err = git.ApplyIntentHistoryReconstruction(ctx, repo, git.IntentHistoryReconstructionOptions{SourceBranchRef: plan.SourceBranchRef, TargetBranchRef: plan.TargetBranchRef, ExpectedHead: plan.ExpectedHead, OldChain: plan.SourceChain, Replacements: replacements, PlanID: plan.ID, VerifyCommit: verify})
		}
	}
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	request.Status = "completed"
	if err != nil {
		request.Status = "failed"
		request.Error = ai.SanitizePlannerError(err.Error())
	} else {
		request.NewHead = result.NewHead
		request.BackupRef = result.BackupRef
	}
	if saveErr := state.SaveIntentHistoryRequest(ctx, db, request); saveErr != nil {
		return true, saveErr
	}
	return true, err
}

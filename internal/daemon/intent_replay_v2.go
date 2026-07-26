package daemon

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// replayIntentCandidateBatch connects the durable v2 candidate engine to the
// existing proven Git publication path. Decisions are consumed in the
// planner-validated topological order; a failed/non-publishable prerequisite
// stops its dependants from reaching publishIntentSelection.
func replayIntentCandidateBatch(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	activeCtx CaptureContext,
	opts ReplayOpts,
	cfg intentReplayConfig,
	indexFile string,
	items []intentReplayItem,
	legacyRequest ai.IntentPlanRequest,
	parent, parentTree string,
	forced bool,
	sum ReplaySummary,
) (ReplaySummary, error) {
	diffBySeq := make(map[int64]string, len(legacyRequest.OfferedCaptures))
	for _, offered := range legacyRequest.OfferedCaptures {
		diffBySeq[offered.Seq] = offered.CapturedDiff
	}
	captures := make([]IntentCandidateCapture, 0, len(items))
	for _, item := range items {
		var covered []state.CaptureEvent
		if item.coalesce != nil {
			covered = append(covered, item.coalesce.Covered...)
		}
		captures = append(captures, IntentCandidateCapture{
			Event: item.event, Ops: item.ops, CapturedDiff: diffBySeq[item.event.Seq],
			CoveredEvents: covered,
		})
	}
	retryLimit := resolvedIntentRetryLimit()
	if cfg.retryLimit != nil {
		retryLimit = *cfg.retryLimit
	}
	telemetry := runtimeTelemetryFromContext(ctx)
	evaluationStartedTS := float64(time.Now().UnixNano()) / 1e9
	plannerCtx := ctx
	if opts.PromptTrace != nil {
		plannerCtx = prompttrace.With(ctx, opts.PromptTrace, prompttrace.Metadata{
			Strategy: "intent_v2", Protocol: ai.IntentPlannerProtocolV2,
			Provider: cfg.plannerProvider, Model: cfg.plannerModel,
			OfferedSeqs: intentOfferedSeqs(legacyRequest),
			BranchRef:   activeCtx.BranchRef, Generation: activeCtx.BranchGeneration,
			DiffIncluded:     intentRequestIncludesDiff(legacyRequest),
			DiffCap:          ai.IntentStageDiffCap,
			ConfigRevisionID: telemetry.revisionID,
			ConfigProfile:    telemetry.profile,
		})
	}
	plannerCtx, attemptCounter := ai.WithIntentAttemptCounter(plannerCtx)
	if !forced {
		traceIntentPlannerInput(opts.Trace, repoRoot, activeCtx, items,
			legacyRequest, cfg)
	}
	evaluation, err := EvaluateIntentCandidates(plannerCtx, db, IntentCandidateEvaluation{
		BranchRef: activeCtx.BranchRef, BranchGeneration: activeCtx.BranchGeneration,
		Captures: captures, Planner: cfg.planner, Health: opts.IntentHealth,
		RetryLimit: retryLimit, RetryLimitSet: true,
		Preset:       opts.IntentPreset,
		CommitFormat: cfg.commitFormat, IncludeDiffs: cfg.includeDiffs,
		ForcedAging: forced, Provider: cfg.plannerProvider, Model: cfg.plannerModel,
		ConfigRevisionID: sql.NullInt64{
			Int64: telemetry.revisionID, Valid: telemetry.revisionID > 0,
		},
		ConfigProfile: telemetry.profile,
		PresetID:      "intent." + string(opts.IntentPreset),
		PresetVersion: 2,
		LatestCommit:  legacyRequest.LatestCommit,
		PathContext:   legacyRequest.PathCommitContext,
		Materialize: intentCandidateScratchMaterializer(
			repoRoot, opts.GitDir, parent),
		Verify: opts.IntentCandidateVerify,
		Now:    time.Now().UTC(),
	})
	if err != nil {
		return sum, err
	}
	if counted := attemptCounter.RetryCount(); counted > evaluation.RetryCount {
		evaluation.RetryCount = counted
	}
	windowCfg := cfg
	windowCfg.retryCount = evaluation.RetryCount
	windowCfg.fallbackUsed = evaluation.Fallback != ""
	windowPlan := intentCandidatePlannerWindowPlan(
		legacyRequest, evaluation)
	if evaluation.PlannerFailure != "" {
		traceIntentPlannerValidationFailure(opts.Trace, repoRoot, activeCtx,
			items, evaluation.PlannerFailure)
	}
	traceIntentPlannerOutput(opts.Trace, repoRoot, activeCtx, items, windowPlan)
	if err := recordIntentPlannerWindow(ctx, db, windowCfg, legacyRequest,
		windowPlan, items, activeCtx, evaluationStartedTS, forced,
		evaluation.PlannerFailure); err != nil {
		return sum, err
	}
	if evaluation.PlannerFailure != "" {
		nowSec := float64(time.Now().UnixNano()) / 1e9
		for _, item := range items {
			if err := state.RecordPlannerError(ctx, db, item.event.Seq, nowSec,
				evaluation.PlannerFailure); err != nil {
				return sum, err
			}
			if err := appendIntentPlannerDecision(
				ctx, db, item.event, activeCtx, nowSec,
				state.DecisionKindIntentPlannerError,
				evaluation.PlannerFailure,
				"Intent v2 planner failed",
				"Intent v2 applied the configured preset fallback policy.",
			); err != nil {
				return sum, err
			}
		}
	}

	itemBySeq := make(map[int64]intentReplayItem, len(items))
	for _, item := range items {
		itemBySeq[item.event.Seq] = item
	}
	publishedCandidates := make(map[string]struct{})
	currentParent, currentTree := parent, parentTree
	publishedAny := false
	for _, decision := range evaluation.Decisions {
		if !decision.Publishable {
			continue
		}
		for _, prerequisite := range decision.Assignment.DependsOnCandidates {
			if _, ok := publishedCandidates[prerequisite]; !ok {
				sum.Skipped = true
				sum.SkippedReason = "intent_v2_prerequisite_pending"
				return sum, nil
			}
		}
		selected := make([]intentReplayItem, 0, len(decision.Candidate.Events))
		hasPublished := false
		for _, member := range decision.Candidate.Events {
			if member.EventRole == "coalesced" {
				continue
			}
			item, ok := itemBySeq[member.EventSeq]
			if !ok {
				event, loadErr := loadIntentCaptureEvent(ctx, db, member.EventSeq)
				if loadErr != nil {
					return sum, loadErr
				}
				ops, loadErr := state.LoadCaptureOps(ctx, db, member.EventSeq)
				if loadErr != nil {
					return sum, loadErr
				}
				item = intentReplayItem{event: event, ops: ops}
				itemBySeq[member.EventSeq] = item
			}
			if item.event.State != state.EventStatePending {
				hasPublished = true
			}
			selected = append(selected, item)
		}
		sort.Slice(selected, func(i, j int) bool {
			return selected[i].event.Seq < selected[j].event.Seq
		})
		plan := ai.IntentPlan{
			SelectedSeqs: append([]int64(nil), decision.Assignment.SelectedSeqs...),
			Subject:      decision.Assignment.Subject, Body: decision.Assignment.Body,
			GroupingReason: decision.Assignment.GroupingReason,
			Source:         evaluation.ProtocolVersion,
		}
		if hasPublished {
			if !opts.IntentRepairEnabled {
				sum.Skipped = true
				sum.SkippedReason = "intent_v2_repair_required"
				return sum, nil
			}
			repaired, publishedCount, repairErr := repairIntentCandidateDecision(
				ctx, repoRoot, opts.GitDir, db, activeCtx, opts, decision,
				selected, currentParent)
			if repairErr != nil {
				return sum, repairErr
			}
			if repaired.Status != state.IntentRepairCompleted {
				sum.Skipped = true
				sum.SkippedReason = "intent_v2_repair_" + repaired.Status
				return sum, nil
			}
			sum.Published += publishedCount
			sum.BaseHead = repaired.NewHead
			publishedCandidates[decision.Candidate.ID] = struct{}{}
			publishedAny = true
			currentParent = repaired.NewHead
			currentTree, err = resolveTreeOID(ctx, repoRoot, currentParent)
			if err != nil {
				return sum, err
			}
			continue
		}
		before := sum
		sum, err = publishIntentSelection(ctx, repoRoot, db, activeCtx, opts,
			indexFile, selected, plan, currentParent, currentTree, sum)
		if err != nil {
			return sum, err
		}
		if sum.Conflicts > before.Conflicts || sum.Failed > before.Failed ||
			sum.Published == before.Published {
			return sum, nil
		}
		if err := markIntentCandidatePublished(ctx, db, decision.Candidate.ID,
			sum.BaseHead, opts.IntentRepairEnabled, opts.IntentRepairHorizon); err != nil {
			return sum, err
		}
		publishedCandidates[decision.Candidate.ID] = struct{}{}
		publishedAny = true
		currentParent = sum.BaseHead
		currentTree, err = resolveTreeOID(ctx, repoRoot, currentParent)
		if err != nil {
			return sum, err
		}
	}
	if !publishedAny {
		sum.Skipped = true
		if evaluation.NeedsAttention {
			sum.SkippedReason = "intent_v2_needs_attention"
		} else {
			sum.SkippedReason = "intent_v2_candidate_wait"
		}
	}
	return sum, nil
}

func intentCandidatePlannerWindowPlan(
	request ai.IntentPlanRequest,
	evaluation IntentCandidateEvaluationResult,
) ai.IntentPlan {
	offered := make(map[int64]struct{}, len(request.OfferedCaptures))
	for _, capture := range request.OfferedCaptures {
		offered[capture.Seq] = struct{}{}
	}
	plan := ai.IntentPlan{Source: evaluation.ProtocolVersion}
	covered := make(map[int64]struct{}, len(offered))
	for _, decision := range evaluation.Decisions {
		seqs := make([]int64, 0, len(decision.Assignment.SelectedSeqs))
		for _, seq := range decision.Assignment.SelectedSeqs {
			if _, ok := offered[seq]; !ok {
				continue
			}
			seqs = append(seqs, seq)
			covered[seq] = struct{}{}
		}
		if len(seqs) == 0 {
			continue
		}
		if decision.Publishable {
			plan.SelectedSeqs = append(plan.SelectedSeqs, seqs...)
			plan.CommitGroups = append(plan.CommitGroups, ai.IntentCommitGroup{
				SelectedSeqs:   append([]int64(nil), seqs...),
				Subject:        decision.Assignment.Subject,
				Body:           decision.Assignment.Body,
				GroupingReason: decision.Assignment.GroupingReason,
			})
			continue
		}
		reason := intentCandidateDeferredReason(decision, evaluation)
		for _, seq := range seqs {
			plan.DeferredSeqs = append(plan.DeferredSeqs, seq)
			plan.DeferredReasons = append(plan.DeferredReasons, ai.DeferredReason{
				Seq: seq, Reason: reason,
			})
		}
	}
	for _, capture := range request.OfferedCaptures {
		if _, ok := covered[capture.Seq]; ok {
			continue
		}
		plan.DeferredSeqs = append(plan.DeferredSeqs, capture.Seq)
		plan.DeferredReasons = append(plan.DeferredReasons, ai.DeferredReason{
			Seq:    capture.Seq,
			Reason: "candidate evaluation retained this capture",
		})
	}
	sort.Slice(plan.SelectedSeqs, func(i, j int) bool {
		return plan.SelectedSeqs[i] < plan.SelectedSeqs[j]
	})
	sort.Slice(plan.DeferredSeqs, func(i, j int) bool {
		return plan.DeferredSeqs[i] < plan.DeferredSeqs[j]
	})
	sort.Slice(plan.DeferredReasons, func(i, j int) bool {
		return plan.DeferredReasons[i].Seq < plan.DeferredReasons[j].Seq
	})
	return plan
}

func intentCandidateDeferredReason(
	decision IntentCandidateDecision,
	evaluation IntentCandidateEvaluationResult,
) string {
	if len(decision.Assignment.MissingCompanions) > 0 {
		return strings.Join(decision.Assignment.MissingCompanions, "; ")
	}
	if evaluation.PlannerFailure != "" {
		return "planner failure retained this candidate"
	}
	if evaluation.NeedsAttention {
		return "candidate requires attention before publication"
	}
	return "candidate is waiting for an atomic publish boundary"
}

func intentCandidateScratchMaterializer(
	repoRoot, gitDir, parent string,
) IntentCandidateMaterializer {
	return func(ctx context.Context, captures []IntentCandidateCapture) error {
		_, err := materializeIntentCandidateTree(ctx, repoRoot, gitDir, parent, captures)
		return err
	}
}

func materializeIntentCandidateTree(
	ctx context.Context,
	repoRoot, gitDir, parent string,
	captures []IntentCandidateCapture,
) (string, error) {
	if gitDir == "" || parent == "" {
		return "", errors.New("daemon: intent v2 materialize: git dir and parent are required")
	}
	sort.Slice(captures, func(i, j int) bool {
		return captures[i].Event.Seq < captures[j].Event.Seq
	})
	seed := parent
	for _, capture := range captures {
		if capture.Event.State != state.EventStatePublished ||
			!capture.Event.CommitOID.Valid {
			continue
		}
		out, err := git.Run(ctx, git.RunOpts{Dir: repoRoot, Timeout: git.DefaultReadTimeout},
			"rev-parse", capture.Event.CommitOID.String+"^")
		if err != nil {
			return "", fmt.Errorf("daemon: intent v2 materialize: resolve soft-published base: %w", err)
		}
		seed = string(bytes.TrimSpace(out))
		break
	}
	root := filepath.Join(gitDir, "acd")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp(root, "intent-materialize-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", err
	}
	index := filepath.Join(dir, "idx")
	if err := git.ReadTree(ctx, repoRoot, index, seed); err != nil {
		return "", err
	}
	tree := ""
	for _, capture := range captures {
		if len(capture.Ops) == 0 {
			return "", fmt.Errorf("daemon: intent v2 materialize: capture %d has no ops",
				capture.Event.Seq)
		}
		if reason, err := detectConflict(ctx, repoRoot, index, capture.Ops); err != nil {
			return "", err
		} else if reason != "" {
			return "", errors.New(reason)
		}
		tree, err = applyOpsAndWriteTree(ctx, repoRoot, index, capture.Ops)
		if err != nil {
			return "", err
		}
	}
	return tree, nil
}

func repairIntentCandidateDecision(
	ctx context.Context,
	repoRoot, gitDir string,
	db *state.DB,
	activeCtx CaptureContext,
	opts ReplayOpts,
	decision IntentCandidateDecision,
	selected []intentReplayItem,
	currentParent string,
) (IntentRepairResult, int, error) {
	if !decision.Candidate.PublishedCommitOID.Valid ||
		decision.Candidate.PublishedCommitOID.String != currentParent {
		return IntentRepairResult{}, 0,
			errors.New("daemon: intent v2 repair: soft-published candidate is not the HEAD suffix")
	}
	captures := make([]IntentCandidateCapture, 0, len(selected))
	paths := make(map[string]struct{})
	pendingCount := 0
	for _, item := range selected {
		captures = append(captures, IntentCandidateCapture{
			Event: item.event, Ops: item.ops,
		})
		if item.event.State == state.EventStatePending {
			pendingCount += 1 + coverLen(item.coalesce)
		}
		for _, path := range touchedPaths(item.ops) {
			paths[path] = struct{}{}
		}
	}
	tree, err := materializeIntentCandidateTree(ctx, repoRoot, gitDir,
		currentParent, captures)
	if err != nil {
		return IntentRepairResult{}, 0, err
	}
	pathList := make([]string, 0, len(paths))
	for path := range paths {
		pathList = append(pathList, path)
	}
	sort.Strings(pathList)
	message := decision.Assignment.Subject
	if decision.Assignment.Body != "" {
		message += "\n\n" + decision.Assignment.Body
	}
	result, err := ApplyIntentRepairTransaction(ctx, repoRoot, gitDir, db,
		activeCtx, IntentRepairPlan{
			BranchRef: activeCtx.BranchRef, BranchGeneration: activeCtx.BranchGeneration,
			ExpectedHead: currentParent, Paths: pathList,
			MaxCommits: opts.IntentRepairMaxCommits,
			Candidates: []IntentRepairCandidatePlan{{
				CandidateID: decision.Candidate.ID,
				Replaces:    []string{currentParent},
				TreeOID:     tree, Message: message, AuthorOID: currentParent,
			}},
		})
	return result, pendingCount, err
}

func markIntentCandidatePublished(
	ctx context.Context,
	db *state.DB,
	candidateID, commitOID string,
	repairEnabled bool,
	horizon time.Duration,
) error {
	status := state.IntentCandidatePublished
	var deadline sql.NullFloat64
	if repairEnabled {
		status = state.IntentCandidateSoftPublished
		if horizon <= 0 {
			horizon = 10 * time.Minute
		}
		deadline = sql.NullFloat64{
			Float64: float64(time.Now().Add(horizon).UnixNano()) / 1e9,
			Valid:   true,
		}
	}
	res, err := db.SQL().ExecContext(ctx, `
UPDATE intent_candidates
SET status=?, published_commit_oid=?, soft_publication_deadline=?,
    updated_ts=?
WHERE id=? AND status='ready'`,
		status, commitOID, deadline,
		float64(time.Now().UnixNano())/1e9, candidateID)
	if err != nil {
		return fmt.Errorf("daemon: mark intent candidate published: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("daemon: mark intent candidate published: candidate %s changed",
			candidateID)
	}
	return nil
}

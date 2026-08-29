package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	metaIntentRepairFailedAttempt   = "intent.repair.failed_attempt"
	metaIntentRepairFailedOutput    = "intent.repair.failed_output"
	metaIntentRepairFailedStatus    = "intent.repair.failed_status"
	metaIntentRepairFailedCheckedTS = "intent.repair.failed_checked_ts"
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
	// A persisted verification failure can make the forced-aging baseline
	// invalid before candidate evaluation returns a decision. Give the strict
	// checkpoint recovery proof a chance to release that terminal candidate
	// before asking the planner to continue it.
	if forced && len(items) == 1 && opts.PublicationDrain == nil &&
		intentRecoveryItemQuiescent(items[0], cfg) {
		recovered, handled, recoveryErr := startFailedIntentCheckpointRecovery(
			ctx, repoRoot, db, activeCtx, opts,
			items[0].event.Seq, parent, sum)
		if recoveryErr != nil || handled {
			return recovered, recoveryErr
		}
	}
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
	presetID := telemetry.presetID
	if presetID == "" {
		presetID = "intent." + string(opts.IntentPreset)
	}
	presetVersion := telemetry.presetVersion
	if presetVersion <= 0 {
		presetVersion = config.PresetCatalogVersion
	}
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
		PresetID:      presetID,
		PresetVersion: presetVersion,
		LatestCommit:  legacyRequest.LatestCommit,
		PathContext:   legacyRequest.PathCommitContext,
		Hints:         runtimeIntentDependencyHints(captures),
		Materialize: intentCandidateScratchMaterializer(
			repoRoot, opts.GitDir, parent),
		PreflightMaterialize: intentCandidatePreflightMaterializer(
			repoRoot, opts.GitDir, parent),
		VerificationMode:    opts.IntentVerificationMode,
		Verify:              opts.IntentCandidateVerify,
		Now:                 time.Now().UTC(),
		TargetEventSeqs:     cfg.targetEventSeqs,
		RejectLocalFallback: cfg.semanticSalvage,
	})
	var semanticFallbackErr *IntentSemanticFallbackRequiredError
	var preflightErr *IntentPlanPreflightError
	if err != nil && !errors.As(err, &semanticFallbackErr) &&
		!errors.As(err, &preflightErr) {
		return sum, err
	}
	if cfg.atomicFallback &&
		evaluation.ResolutionMode != "waiting_message_rewrite" {
		evaluation.ResolutionMode = publicationFallbackLocalUnlock
	}
	if counted := attemptCounter.RetryCount(); counted > evaluation.RetryCount {
		evaluation.RetryCount = counted
	}
	windowCfg := cfg
	windowCfg.retryCount = evaluation.RetryCount
	windowCfg.fallbackUsed = evaluation.Fallback != ""
	windowCfg.planFingerprint = evaluation.PlanFingerprint
	windowCfg.planAttempt = evaluation.PlanAttempt
	windowCfg.planAttemptLimit = evaluation.PlanAttemptLimit
	windowCfg.unresolvedCaptureCount = evaluation.UnresolvedCaptureCount
	windowCfg.preservedGroupCount = evaluation.PreservedGroupCount
	windowCfg.resolutionMode = evaluation.ResolutionMode
	if preflightErr != nil {
		evaluation.PlannerFailure = preflightErr.Error()
	}
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
	if err := recordIntentDeferrals(
		ctx, db, windowPlan, items, activeCtx, evaluationStartedTS,
	); err != nil {
		return sum, err
	}
	if semanticFallbackErr != nil {
		return sum, semanticFallbackErr
	}
	if preflightErr != nil {
		return sum, preflightErr
	}
	if evaluation.PlannerFailure != "" && evaluation.NeedsAttention {
		sum.PlannerFailure = evaluation.PlannerFailure
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
	visibleCandidateIDs := make(map[string]struct{},
		len(evaluation.VisibleCandidateIDs))
	for _, candidateID := range evaluation.VisibleCandidateIDs {
		visibleCandidateIDs[candidateID] = struct{}{}
	}
	currentParent, currentTree := parent, parentTree
	publishedAny := false
	for _, decision := range evaluation.Decisions {
		if !decision.Publishable {
			continue
		}
		for _, prerequisite := range decision.Assignment.DependsOnCandidates {
			if _, ok := publishedCandidates[prerequisite]; ok {
				continue
			}
			candidate, ok, loadErr := state.IntentCandidateByID(
				ctx, db, prerequisite)
			if loadErr != nil {
				return sum, loadErr
			}
			if ok && (candidate.Status == state.IntentCandidateSoftPublished ||
				candidate.Status == state.IntentCandidatePublished) {
				continue
			}
			sum.Skipped = true
			sum.SkippedReason = "intent_v2_prerequisite_pending"
			return sum, nil
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
			item.candidateID = decision.Candidate.ID
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
				return recoverIntentCandidateForward(
					ctx, repoRoot, db, activeCtx, opts, decision.Candidate.ID,
					"repair_disabled", currentParent, sum)
			}
			repaired, publishedCount, repairErr := repairIntentCandidateDecision(
				ctx, repoRoot, opts.GitDir, db, activeCtx, opts, decision,
				selected, currentParent, visibleCandidateIDs)
			if repairErr != nil {
				return sum, repairErr
			}
			if repaired.Status != state.IntentRepairCompleted {
				if intentRepairSupportsForwardRecovery(repaired.Reason) {
					return recoverIntentCandidateForward(
						ctx, repoRoot, db, activeCtx, opts,
						decision.Candidate.ID, repaired.Reason, currentParent, sum)
				}
				sum.Skipped = true
				sum.SkippedReason = "intent_v2_repair_" + repaired.Status
				if repaired.Reason != "" {
					sum.SkippedReason += "_" + repaired.Reason
				}
				sum.Disposition, sum.DispositionReason =
					intentRepairSkipDisposition(repaired.Reason)
				return sum, nil
			}
			sum.Published += publishedCount
			sum.BaseHead = repaired.NewHead
			sum.InternalTransitionTargetOID = repaired.NewHead
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
		publishedCandidates[decision.Candidate.ID] = struct{}{}
		publishedAny = true
		currentParent = sum.BaseHead
		currentTree, err = resolveTreeOID(ctx, repoRoot, currentParent)
		if err != nil {
			return sum, err
		}
	}
	if !publishedAny {
		messageRewriteWait := evaluation.Fallback == "waiting_message_rewrite"
		if !messageRewriteWait && forced && len(items) == 1 &&
			opts.PublicationDrain == nil &&
			intentRecoveryItemQuiescent(items[0], cfg) {
			recovered, handled, recoveryErr :=
				startFailedIntentCheckpointRecovery(
					ctx, repoRoot, db, activeCtx, opts,
					items[0].event.Seq, currentParent, sum)
			if recoveryErr != nil || handled {
				return recovered, recoveryErr
			}
		}
		sum.Skipped = true
		if messageRewriteWait {
			sum.SkippedReason = "intent_v2_waiting_message_rewrite"
			sum.Disposition = ReplayDispositionTransientWait
			sum.DispositionReason = evaluation.PlannerFailure
			// Active drains and forward recovery own a frozen target and need an
			// immediate follow-up pass. Ordinary candidate windows can wait for
			// the normal poll/circuit cooldown instead of spinning while the
			// provider remains unavailable.
			sum.HasMore = publicationDrainMessageRewriteWait(
				opts, cfg, evaluation)
		} else if evaluation.NeedsAttention {
			sum.SkippedReason = "intent_v2_needs_attention"
			sum.Disposition = ReplayDispositionNeedsAttention
			sum.DispositionReason = evaluation.PlannerFailure
		} else {
			sum.SkippedReason = "intent_v2_candidate_wait"
		}
	}
	return sum, nil
}

func publicationDrainMessageRewriteWait(
	opts ReplayOpts,
	cfg intentReplayConfig,
	evaluation IntentCandidateEvaluationResult,
) bool {
	return evaluation.Fallback == "waiting_message_rewrite" &&
		(cfg.atomicFallback || opts.PublicationDrain != nil)
}

func intentRecoveryItemQuiescent(
	item intentReplayItem,
	cfg intentReplayConfig,
) bool {
	return cfg.pathQuiescence <= 0 || pathQuiescentForEvent(
		item.event, item.ops, cfg.pathQuiescence, pathQuiescenceNow())
}

func startFailedIntentCheckpointRecovery(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	activeCtx CaptureContext,
	opts ReplayOpts,
	heldEventSeq int64,
	currentParent string,
	sum ReplaySummary,
) (ReplaySummary, bool, error) {
	recovery, changed, err := state.StartFailedIntentCheckpointRecovery(
		ctx, db, activeCtx.BranchRef, activeCtx.BranchGeneration,
		heldEventSeq)
	if err != nil {
		return sum, true, err
	}
	if !changed {
		return sum, false, nil
	}
	slog.Default().Info("failed intent candidate entered checkpoint recovery",
		"candidate_id", recovery.CandidateID,
		"reason", recovery.Reason,
		"target_events", len(recovery.TargetEventSeqs),
		"branch_generation", recovery.BranchGeneration)
	sum.Skipped = true
	sum.SkippedReason = "intent_v2_forward_recovery_" + recovery.Reason
	sum.Disposition = ReplayDispositionRecoverableStall
	sum.DispositionReason = recovery.Reason
	sum.HasMore = true
	if opts.PublicationDrain != nil || opts.forwardRecoveryAttempted ||
		sum.Published > 0 {
		return sum, true, nil
	}
	opts.forwardRecoveryAttempted = true
	retryCtx := activeCtx
	retryCtx.BaseHead = currentParent
	retried, retryErr := Replay(ctx, repoRoot, db, retryCtx, opts)
	return retried, true, retryErr
}

func recoverIntentCandidateForward(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	activeCtx CaptureContext,
	opts ReplayOpts,
	candidateID string,
	reason string,
	currentParent string,
	sum ReplaySummary,
) (ReplaySummary, error) {
	pending, changed, err := state.ForwardRecoverIntentCandidate(
		ctx, db, state.IntentForwardRecovery{
			BranchRef:        activeCtx.BranchRef,
			BranchGeneration: activeCtx.BranchGeneration,
			CandidateID:      candidateID,
			Reason:           reason,
		})
	if err != nil {
		return sum, err
	}
	if changed {
		slog.Default().Info("intent candidate entered forward recovery",
			"candidate_id", candidateID,
			"reason", reason,
			"pending_events", pending,
			"branch_generation", activeCtx.BranchGeneration)
	}
	sum.Skipped = true
	sum.SkippedReason = "intent_v2_forward_recovery_" + reason
	sum.Disposition = ReplayDispositionRecoverableStall
	sum.DispositionReason = reason
	sum.HasMore = true
	if !changed || opts.PublicationDrain != nil ||
		opts.forwardRecoveryAttempted || sum.Published > 0 {
		return sum, nil
	}
	opts.forwardRecoveryAttempted = true
	retryCtx := activeCtx
	retryCtx.BaseHead = currentParent
	return Replay(ctx, repoRoot, db, retryCtx, opts)
}

func updateIntentForwardRecoveryAfterReplay(
	ctx context.Context,
	db *state.DB,
	recovery state.IntentForwardRecovery,
	sum ReplaySummary,
	replayErr error,
) (ReplaySummary, error) {
	if replayErr != nil {
		var exhausted *IntentSemanticFallbackRequiredError
		if recovery.Stage == publicationFallbackSemanticReplan &&
			errors.As(replayErr, &exhausted) {
			if _, err := state.AdvanceIntentForwardRecovery(
				ctx, db, recovery, publicationFallbackLocalUnlock, 0); err != nil {
				return sum, errors.Join(replayErr, err)
			}
			logIntentForwardRecoveryTransition(
				recovery, publicationFallbackLocalUnlock)
			sum.Skipped = true
			sum.SkippedReason = "intent_forward_recovery_local_unlock"
			sum.Disposition = ReplayDispositionRecoverableStall
			sum.DispositionReason = exhausted.Failure
			sum.HasMore = true
			return sum, nil
		}
		return sum, replayErr
	}
	if sum.Published > 0 {
		completed, err := state.CompleteResolvedIntentForwardRecovery(
			ctx, db, recovery)
		if err != nil {
			return sum, err
		}
		if completed {
			logIntentForwardRecoveryCompletion(recovery, sum.Published)
			return sum, nil
		}
		if recovery.Stage == publicationFallbackLocalUnlock {
			nextStage := publicationFallbackSemanticReplan
			if sum.PlannerCircuitOpen {
				nextStage = publicationFallbackLocalUnlock
			}
			if _, err := state.AdvanceIntentForwardRecovery(
				ctx, db, recovery, nextStage,
				sum.Published); err != nil {
				return sum, err
			}
			logIntentForwardRecoveryTransition(recovery, nextStage)
			sum.HasMore = true
			return sum, nil
		}
		// Semantic recovery may publish several independently verified groups.
		// Keep the frozen target until every member is durably resolved.
		sum.HasMore = true
		return sum, nil
	}
	if intentForwardRecoveryTransientWait(sum) {
		sum.HasMore = true
		return sum, nil
	}
	if recovery.Stage == publicationFallbackSemanticReplan {
		if _, err := state.AdvanceIntentForwardRecovery(
			ctx, db, recovery, publicationFallbackLocalUnlock, 0); err != nil {
			return sum, err
		}
		logIntentForwardRecoveryTransition(
			recovery, publicationFallbackLocalUnlock)
		sum.Skipped = true
		sum.SkippedReason = "intent_forward_recovery_local_unlock"
		sum.Disposition = ReplayDispositionRecoverableStall
		sum.DispositionReason = recovery.Reason
		sum.HasMore = true
	}
	return sum, nil
}

func intentForwardRecoveryTransientWait(sum ReplaySummary) bool {
	if sum.Disposition == ReplayDispositionTransientWait {
		return true
	}
	if !sum.Skipped {
		return false
	}
	switch sum.SkippedReason {
	case "skipped_due_path_quiescence",
		"skipped_due_intent_settle_window",
		"skipped_due_intent_batch_wait":
		return true
	default:
		return false
	}
}

func logIntentForwardRecoveryTransition(
	recovery state.IntentForwardRecovery,
	nextStage string,
) {
	if recovery.Stage == nextStage {
		return
	}
	slog.Default().Info("intent forward recovery transition",
		"candidate_id", recovery.CandidateID,
		"from_mode", recovery.Stage,
		"to_mode", nextStage,
		"unlock_count", recovery.UnlockCount,
		"reason", recovery.Reason)
}

func logIntentForwardRecoveryCompletion(
	recovery state.IntentForwardRecovery,
	published int,
) {
	slog.Default().Info("intent forward recovery completed",
		"candidate_id", recovery.CandidateID,
		"mode", recovery.Stage,
		"published_events", published,
		"unlock_count", recovery.UnlockCount,
		"reason", recovery.Reason)
}

func intentRepairSupportsForwardRecovery(reason string) bool {
	switch reason {
	case "repair_horizon_expired",
		"repair_commit_outside_suffix",
		"repair_final_tree_mismatch",
		"repair_suffix_not_acd_owned",
		"repair_repartition_not_proven",
		"repair_repartition_dependency",
		"repair_repartition_path_overlap",
		git.IntentRepairReasonNonLinearChain,
		git.IntentRepairReasonMergeCommit,
		git.IntentRepairReasonAlternateRef,
		git.IntentRepairReasonStagedOverlap,
		git.IntentRepairReasonOwnershipMissing:
		return true
	default:
		return false
	}
}

func intentRepairSkipDisposition(
	reason string,
) (ReplayDisposition, string) {
	switch reason {
	case git.IntentRepairReasonDetached,
		git.IntentRepairReasonBranchChanged,
		git.IntentRepairReasonHeadChanged,
		"manual or rewind pause is active":
		return ReplayDispositionTransientWait, reason
	case "repair_verification_unavailable",
		"repair_verification_needs_attention":
		return ReplayDispositionNeedsAttention, reason
	default:
		if strings.Contains(reason, "Git operation in progress") {
			return ReplayDispositionTransientWait, reason
		}
		if reason == "" {
			reason = "intent repair stopped without a safe forward recovery"
		}
		return ReplayDispositionNeedsAttention, reason
	}
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

func runtimeIntentDependencyHints(
	captures []IntentCandidateCapture,
) []IntentDependencyHint {
	type features struct {
		seq       int64
		path      string
		stem      string
		module    string
		diff      string
		symbols   map[string]struct{}
		changeIDs map[string]struct{}
		generated bool
	}
	ordered := append([]IntentCandidateCapture(nil), captures...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Event.Seq < ordered[j].Event.Seq
	})
	items := make([]features, 0, len(ordered))
	for _, capture := range ordered {
		diff := strings.ToLower(capture.CapturedDiff)
		item := features{
			seq: capture.Event.Seq, path: strings.ToLower(capture.Event.Path),
			stem:   intentSemanticStem(capture),
			module: intentCaptureModule(capture), diff: diff,
			symbols:   runtimeIntentSymbols(diff),
			changeIDs: runtimeIntentChangeIDs(diff),
			generated: strings.Contains(diff, "code generated") ||
				strings.Contains(diff, "generated from") ||
				runtimeIntentGeneratedPath(capture.Event.Path),
		}
		items = append(items, item)
	}
	var hints []IntentDependencyHint
	seen := map[string]struct{}{}
	add := func(from, to int64, strength ai.IntentDependencyStrength, kind, evidence string) {
		if from <= 0 || to <= 0 || from == to || evidence == "" ||
			len(hints) >= state.IntentDependencyMaxPerPair {
			return
		}
		key := fmt.Sprintf("%d:%d:%s:%s", from, to, strength, kind)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		hints = append(hints, IntentDependencyHint{
			PrerequisiteSeq: from, DependentSeq: to,
			Strength: strength, Kind: kind, Evidence: evidence,
		})
	}
	for i := range items {
		for j := i + 1; j < len(items); j++ {
			earlier, later := items[i], items[j]
			if shared := firstRuntimeIntentFeature(
				earlier.symbols, later.symbols); shared != "" {
				add(earlier.seq, later.seq, ai.IntentDependencySoft,
					"symbol_hash", shared)
			}
			if shared := firstRuntimeIntentFeature(
				earlier.changeIDs, later.changeIDs); shared != "" {
				add(earlier.seq, later.seq, ai.IntentDependencySoft,
					"hunk_hash", shared)
			}
			earlierBase := runtimeIntentStemBase(earlier.stem)
			laterBase := runtimeIntentStemBase(later.stem)
			switch {
			case earlierBase != "" && strings.Contains(later.diff, earlierBase):
				add(earlier.seq, later.seq, ai.IntentDependencySoft,
					"import_reference", earlierBase)
			case laterBase != "" && strings.Contains(earlier.diff, laterBase):
				add(earlier.seq, later.seq, ai.IntentDependencySoft,
					"import_reference", laterBase)
			}
			if later.generated && earlier.module == later.module &&
				(earlierBase != "" && strings.Contains(later.diff, earlierBase)) {
				add(earlier.seq, later.seq, ai.IntentDependencyHard,
					"generated_source", earlierBase)
			}
			generated, source := later, earlier
			if earlier.generated && !later.generated {
				generated, source = earlier, later
			}
			if generated.generated && !source.generated {
				artifactBase := strings.ToLower(path.Base(generated.path))
				if strings.Contains(source.diff, generated.path) ||
					(len(artifactBase) >= 4 && strings.Contains(source.diff, artifactBase)) {
					// The reference proves the semantic relationship without
					// assuming capture order. Publication order remains governed by
					// hard object/path dependencies.
					add(source.seq, generated.seq, ai.IntentDependencySoft,
						"generated_artifact_reference", generated.path)
				}
			}
		}
	}
	return hints
}

func runtimeIntentSymbols(diff string) map[string]struct{} {
	const maxSymbols = 128
	common := map[string]struct{}{
		"about": {}, "after": {}, "before": {}, "branch": {}, "candidate": {},
		"commit": {}, "config": {}, "context": {}, "error": {}, "false": {},
		"function": {}, "import": {}, "intent": {}, "package": {}, "return": {},
		"string": {}, "struct": {}, "testing": {}, "true": {}, "value": {},
	}
	out := make(map[string]struct{})
	for _, line := range strings.Split(diff, "\n") {
		if len(line) == 0 || (line[0] != '+' && line[0] != '-') ||
			strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "---") {
			continue
		}
		for _, token := range strings.FieldsFunc(line[1:], func(r rune) bool {
			return r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}) {
			token = strings.ToLower(token)
			// Short words such as "split", "content", or a directory name
			// are common across unrelated captures and are not reliable
			// symbol evidence. Keep this signal for identifier-like tokens.
			if len(token) < 8 {
				continue
			}
			if _, skip := common[token]; skip {
				continue
			}
			out[token] = struct{}{}
			if len(out) >= maxSymbols {
				return out
			}
		}
	}
	return out
}

func runtimeIntentChangeIDs(diff string) map[string]struct{} {
	const maxChanges = 64
	out := make(map[string]struct{})
	for _, line := range strings.Split(diff, "\n") {
		if len(line) < 8 || (line[0] != '+' && line[0] != '-') ||
			strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "---") {
			continue
		}
		normalized := strings.Join(strings.Fields(line[1:]), " ")
		if len(normalized) < 7 {
			continue
		}
		out[intentEvidenceHash(normalized)] = struct{}{}
		if len(out) >= maxChanges {
			break
		}
	}
	return out
}

func firstRuntimeIntentFeature(
	left, right map[string]struct{},
) string {
	for feature := range left {
		if _, ok := right[feature]; ok {
			return feature
		}
	}
	return ""
}

func runtimeIntentStemBase(stem string) string {
	base := strings.ToLower(path.Base(stem))
	if len(base) < 4 {
		return ""
	}
	return base
}

func runtimeIntentGeneratedPath(value string) bool {
	clean := strings.ToLower(path.Clean(value))
	base := path.Base(clean)
	if strings.Contains(base, ".generated.") ||
		strings.Contains(base, "_generated.") ||
		strings.Contains(base, ".gen.") ||
		strings.Contains(base, "_gen.") {
		return true
	}
	ext := strings.ToLower(path.Ext(base))
	archive := ext == ".zip" || ext == ".tgz" || ext == ".gz" ||
		ext == ".tar" || ext == ".bz2" || ext == ".xz"
	if !archive {
		return false
	}
	for _, dir := range strings.Split(path.Dir(clean), "/") {
		switch dir {
		case "output", "outputs", "dist", "build", "artifacts", "archives":
			return true
		}
	}
	return false
}

func intentCandidateScratchMaterializer(
	repoRoot, gitDir, parent string,
) IntentCandidateMaterializer {
	currentSeed := parent
	return func(ctx context.Context, captures []IntentCandidateCapture) error {
		tree, err := materializeIntentCandidateTree(
			ctx, repoRoot, gitDir, currentSeed, captures)
		if err == nil {
			currentSeed = tree
		}
		return err
	}
}

func intentCandidatePreflightMaterializer(
	repoRoot, gitDir, parent string,
) IntentCandidateMaterializer {
	// A correction can preflight a smaller request after the initial baseline.
	// Each request starts from the pass parent, not the prior synthetic result.
	return func(ctx context.Context, captures []IntentCandidateCapture) error {
		_, err := materializeIntentCandidateTree(
			ctx, repoRoot, gitDir, parent, captures)
		return err
	}
}

func materializeIntentCandidateTree(
	ctx context.Context,
	repoRoot, gitDir, parent string,
	captures []IntentCandidateCapture,
) (string, error) {
	return materializeIntentCandidateTreeFromSeed(
		ctx, repoRoot, gitDir, parent, captures, true)
}

func materializeIntentCandidateTreeFromSeed(
	ctx context.Context,
	repoRoot, gitDir, parent string,
	captures []IntentCandidateCapture,
	usePublishedBase bool,
) (string, error) {
	if gitDir == "" || parent == "" {
		return "", errors.New("daemon: intent v2 materialize: git dir and parent are required")
	}
	sort.Slice(captures, func(i, j int) bool {
		return captures[i].Event.Seq < captures[j].Event.Seq
	})
	seed := parent
	if usePublishedBase {
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
		if reason, err := detectConflictWithIdempotentUpdates(
			ctx, repoRoot, index, capture.Ops,
		); err != nil {
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
	visibleCandidateIDs map[string]struct{},
) (IntentRepairResult, int, error) {
	verificationRequired := opts.IntentVerificationMode == "fast" ||
		opts.IntentVerificationMode == "full"
	if verificationRequired && opts.IntentRepairCommitVerify == nil {
		return IntentRepairResult{
			Status: state.IntentRepairSkipped,
			Reason: "repair_verification_unavailable",
		}, 0, nil
	}
	sourceCommits, err := intentRepairCandidateSourceCommits(
		ctx,
		db,
		decision.Candidate,
	)
	if err != nil {
		return IntentRepairResult{}, 0, err
	}
	if !decision.Candidate.SoftPublicationDeadline.Valid ||
		decision.Candidate.SoftPublicationDeadline.Float64 <=
			float64(time.Now().UnixNano())/1e9 {
		return IntentRepairResult{
			Status: state.IntentRepairSkipped,
			Reason: "repair_horizon_expired",
		}, 0, nil
	}
	repairLimit := opts.IntentRepairMaxCommits
	if repairLimit <= 0 || repairLimit > git.MaxIntentRepairCommits {
		repairLimit = git.MaxIntentRepairCommits
	}
	out, err := git.Run(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, "rev-list", "--first-parent",
		fmt.Sprintf("--max-count=%d", repairLimit), currentParent)
	if err != nil {
		return IntentRepairResult{}, 0, err
	}
	newestFirst := strings.Fields(string(out))
	sourceSet := make(map[string]struct{}, len(sourceCommits))
	for _, oid := range sourceCommits {
		sourceSet[oid] = struct{}{}
	}
	oldestSourceIndex := -1
	for i, oid := range newestFirst {
		if _, ok := sourceSet[oid]; ok && i > oldestSourceIndex {
			oldestSourceIndex = i
		}
	}
	if oldestSourceIndex < 0 {
		return IntentRepairResult{
			Status: state.IntentRepairSkipped,
			Reason: "repair_commit_outside_suffix",
		}, 0, nil
	}
	for oid := range sourceSet {
		found := false
		for _, suffixOID := range newestFirst[:oldestSourceIndex+1] {
			if suffixOID == oid {
				found = true
				break
			}
		}
		if !found {
			return IntentRepairResult{
				Status: state.IntentRepairSkipped,
				Reason: "repair_commit_outside_suffix",
			}, 0, nil
		}
	}
	suffix := append([]string(nil), newestFirst[:oldestSourceIndex+1]...)
	for left, right := 0, len(suffix)-1; left < right; left, right = left+1, right-1 {
		suffix[left], suffix[right] = suffix[right], suffix[left]
	}
	baseOut, err := git.Run(
		ctx,
		git.RunOpts{Dir: repoRoot, Timeout: git.DefaultReadTimeout},
		"rev-parse",
		suffix[0]+"^",
	)
	if err != nil {
		return IntentRepairResult{}, 0,
			fmt.Errorf("daemon: intent v2 repair: resolve suffix base: %w", err)
	}
	baseParent := strings.TrimSpace(string(baseOut))
	captures := make([]IntentCandidateCapture, 0, len(selected))
	pendingCaptures := make([]IntentCandidateCapture, 0, len(selected))
	paths := make(map[string]struct{})
	pendingCount := 0
	for _, item := range selected {
		capture := IntentCandidateCapture{
			Event: item.event, Ops: item.ops,
		}
		captures = append(captures, capture)
		if item.event.State == state.EventStatePending {
			pendingCaptures = append(pendingCaptures, capture)
			pendingCount += 1 + coverLen(item.coalesce)
		}
		for _, path := range touchedPaths(item.ops) {
			paths[path] = struct{}{}
		}
	}
	tree, err := materializeIntentCandidateTreeFromSeed(
		ctx,
		repoRoot,
		gitDir,
		baseParent,
		captures,
		false,
	)
	if err != nil {
		return IntentRepairResult{}, 0, err
	}
	message := decision.Assignment.Subject
	if decision.Assignment.Body != "" {
		message += "\n\n" + decision.Assignment.Body
	}
	mergedReplaces := make([]string, 0, len(sourceSet))
	for _, oldOID := range suffix {
		if _, ok := sourceSet[oldOID]; ok {
			mergedReplaces = append(mergedReplaces, oldOID)
		}
	}
	plans := []IntentRepairCandidatePlan{{
		CandidateID: decision.Candidate.ID,
		Replaces:    mergedReplaces,
		EventSeqs:   intentRepairCandidateEventSeqs(decision.Candidate.Events),
		TreeOID:     tree, Message: message, AuthorOID: mergedReplaces[0],
	}}
	mergedPaths := make(map[string]struct{}, len(paths))
	for path := range paths {
		mergedPaths[path] = struct{}{}
	}
	seedTree := tree
	for _, oldOID := range suffix {
		if _, merged := sourceSet[oldOID]; merged {
			continue
		}
		candidate, ok, loadErr := state.IntentCandidateByPublishedCommit(
			ctx, db, activeCtx.BranchRef, activeCtx.BranchGeneration, oldOID)
		if loadErr != nil {
			return IntentRepairResult{}, 0, loadErr
		}
		if !ok {
			return IntentRepairResult{
				Status: state.IntentRepairSkipped,
				Reason: "repair_suffix_not_acd_owned",
			}, 0, nil
		}
		if _, visible := visibleCandidateIDs[candidate.ID]; !visible {
			return IntentRepairResult{
				Status: state.IntentRepairSkipped,
				Reason: "repair_repartition_not_proven",
			}, 0, nil
		}
		dependent, dependencyErr := intentRepairCapturesDependOnEachOther(
			ctx, db, activeCtx, selected, candidate.Events)
		if dependencyErr != nil {
			return IntentRepairResult{}, 0, dependencyErr
		}
		if dependent {
			return IntentRepairResult{
				Status: state.IntentRepairSkipped,
				Reason: "repair_repartition_dependency",
			}, 0, nil
		}
		laterCaptures := make([]IntentCandidateCapture, 0, len(candidate.Events))
		for _, member := range candidate.Events {
			event, loadErr := loadIntentCaptureEvent(ctx, db, member.EventSeq)
			if loadErr != nil {
				return IntentRepairResult{}, 0, loadErr
			}
			ops, loadErr := state.LoadCaptureOps(ctx, db, member.EventSeq)
			if loadErr != nil {
				return IntentRepairResult{}, 0, loadErr
			}
			laterCaptures = append(laterCaptures, IntentCandidateCapture{
				Event: event, Ops: ops,
			})
			for _, touched := range touchedPaths(ops) {
				if _, overlaps := mergedPaths[touched]; overlaps {
					return IntentRepairResult{
						Status: state.IntentRepairSkipped,
						Reason: "repair_repartition_path_overlap",
					}, 0, nil
				}
				paths[touched] = struct{}{}
			}
		}
		seedTree, err = materializeIntentCandidateTreeFromSeed(
			ctx, repoRoot, gitDir, seedTree, laterCaptures, false)
		if err != nil {
			return IntentRepairResult{}, 0, err
		}
		oldMessage, loadErr := git.Run(ctx, git.RunOpts{
			Dir: repoRoot, Timeout: git.DefaultReadTimeout,
		}, "show", "-s", "--format=%B", oldOID)
		if loadErr != nil {
			return IntentRepairResult{}, 0, loadErr
		}
		plans = append(plans, IntentRepairCandidatePlan{
			CandidateID: candidate.ID,
			Replaces:    []string{oldOID},
			EventSeqs:   intentRepairCandidateEventSeqs(candidate.Events),
			TreeOID:     seedTree,
			Message:     strings.TrimRight(string(oldMessage), "\n"),
			AuthorOID:   oldOID,
		})
	}
	expectedFinalTree, err := resolveTreeOID(ctx, repoRoot, currentParent)
	if err != nil {
		return IntentRepairResult{}, 0, err
	}
	if len(pendingCaptures) > 0 {
		expectedFinalTree, err = materializeIntentCandidateTreeFromSeed(
			ctx, repoRoot, gitDir, currentParent, pendingCaptures, false)
		if err != nil {
			return IntentRepairResult{}, 0, fmt.Errorf(
				"daemon: intent v2 repair: materialize expected final tree: %w", err)
		}
	}
	// Regrouping may change commit boundaries, but its final tree must equal
	// the current HEAD with only the still-pending captures applied. This
	// catches stale or misattributed commit ownership before repair can drop
	// unrelated content from the suffix.
	if seedTree != expectedFinalTree {
		return IntentRepairResult{
			Status: state.IntentRepairSkipped,
			Reason: "repair_final_tree_mismatch",
		}, 0, nil
	}
	pathList := make([]string, 0, len(paths))
	for path := range paths {
		pathList = append(pathList, path)
	}
	sort.Strings(pathList)
	repairPlan := IntentRepairPlan{
		BranchRef: activeCtx.BranchRef, BranchGeneration: activeCtx.BranchGeneration,
		ExpectedHead: currentParent, OldChain: suffix, Paths: pathList,
		MaxCommits:   repairLimit,
		Candidates:   plans,
		VerifyCommit: opts.IntentRepairCommitVerify,
	}
	repairPlan.PlanDigest, err = intentRepairPlanDigest(
		ctx, repoRoot, repairPlan)
	if err != nil {
		return IntentRepairResult{}, 0, err
	}
	attemptFingerprint := intentRepairAttemptFingerprint(
		repairPlan.PlanDigest,
		decision.Candidate.ConfigRevisionID,
		opts.IntentVerificationMode,
	)
	if previous, ok, metaErr := state.MetaGet(
		ctx, db, metaIntentRepairFailedAttempt); metaErr != nil {
		return IntentRepairResult{}, 0, metaErr
	} else if ok && previous == attemptFingerprint {
		if restoreErr := restoreIntentRepairVerificationFailure(
			ctx, db, decision.Candidate.ID); restoreErr != nil {
			return IntentRepairResult{}, 0, restoreErr
		}
		return IntentRepairResult{
			Status: state.IntentRepairSkipped,
			Reason: "repair_verification_needs_attention",
		}, 0, nil
	}
	result, err := ApplyIntentRepairTransaction(
		ctx, repoRoot, gitDir, db, activeCtx, repairPlan)
	if err != nil && errors.Is(err, git.ErrIntentRepairVerification) {
		var verificationFailure *intentRepairVerificationFailure
		if errors.As(err, &verificationFailure) {
			if saveErr := saveIntentRepairVerificationFailure(
				ctx, db, decision.Candidate.ID, verificationFailure); saveErr != nil {
				return IntentRepairResult{}, 0, errors.Join(err, saveErr)
			}
		}
		if metaErr := persistIntentRepairFailedAttempt(
			ctx, db, attemptFingerprint, verificationFailure); metaErr != nil {
			return IntentRepairResult{}, 0, errors.Join(err, metaErr)
		}
		return IntentRepairResult{
			ID: result.ID, Status: state.IntentRepairFailed,
			Reason: "repair_verification_needs_attention",
		}, 0, nil
	}
	if err == nil && result.Status == state.IntentRepairCompleted {
		_ = state.MetaSetMany(ctx, db, map[string]string{
			metaIntentRepairFailedAttempt:   "",
			metaIntentRepairFailedOutput:    "",
			metaIntentRepairFailedStatus:    "",
			metaIntentRepairFailedCheckedTS: "",
		})
		repairedCtx := activeCtx
		repairedCtx.BaseHead = result.NewHead
		for _, item := range selected {
			reconcileLiveIndexAfterPublish(
				ctx, repoRoot, opts.Trace, repairedCtx, item.event, item.ops)
		}
	}
	return result, pendingCount, err
}

func persistIntentRepairFailedAttempt(
	ctx context.Context,
	db *state.DB,
	fingerprint string,
	failure *intentRepairVerificationFailure,
) error {
	pairs := map[string]string{
		metaIntentRepairFailedAttempt: fingerprint,
	}
	if failure != nil {
		pairs[metaIntentRepairFailedOutput] = failure.Output
		pairs[metaIntentRepairFailedStatus] = failure.Status
		pairs[metaIntentRepairFailedCheckedTS] =
			strconv.FormatFloat(failure.CheckedTS, 'f', -1, 64)
	}
	return state.MetaSetMany(ctx, db, pairs)
}

func restoreIntentRepairVerificationFailure(
	ctx context.Context,
	db *state.DB,
	candidateID string,
) error {
	output, _, err := state.MetaGet(
		ctx, db, metaIntentRepairFailedOutput)
	if err != nil {
		return err
	}
	status, _, err := state.MetaGet(
		ctx, db, metaIntentRepairFailedStatus)
	if err != nil {
		return err
	}
	rawChecked, _, err := state.MetaGet(
		ctx, db, metaIntentRepairFailedCheckedTS)
	if err != nil {
		return err
	}
	checkedTS, _ := strconv.ParseFloat(rawChecked, 64)
	return saveIntentRepairVerificationFailure(
		ctx, db, candidateID, &intentRepairVerificationFailure{
			Status: status, Output: output, CheckedTS: checkedTS,
		})
}

func intentRepairAttemptFingerprint(
	planDigest string,
	revisionID sql.NullInt64,
	verificationMode string,
) string {
	var body strings.Builder
	body.WriteString(planDigest)
	body.WriteByte(0)
	if revisionID.Valid {
		body.WriteString(strconv.FormatInt(revisionID.Int64, 10))
	}
	body.WriteByte(0)
	body.WriteString(verificationMode)
	sum := sha256.Sum256([]byte(body.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func saveIntentRepairVerificationFailure(
	ctx context.Context,
	db *state.DB,
	candidateID string,
	failure *intentRepairVerificationFailure,
) error {
	if failure == nil {
		return nil
	}
	status := strings.TrimSpace(failure.Status)
	if status == "" {
		status = "needs_attention"
	}
	output := failure.Output
	if len(output) > state.IntentVerificationOutputMaxBytes {
		output = output[len(output)-state.IntentVerificationOutputMaxBytes:]
	}
	_, err := db.SQL().ExecContext(ctx, `
UPDATE intent_candidates
SET status='blocked', readiness='wait',
    verification_status=?, verification_output=?, verification_ts=?,
    updated_ts=?
WHERE id=? AND status IN ('open','waiting','ready','soft_published','blocked')`,
		status, output, failure.CheckedTS, failure.CheckedTS, candidateID)
	if err != nil {
		return fmt.Errorf(
			"daemon: persist repair verification attention: %w", err)
	}
	return nil
}

func intentRepairCapturesDependOnEachOther(
	ctx context.Context,
	db *state.DB,
	cctx CaptureContext,
	selected []intentReplayItem,
	otherEvents []state.IntentCandidateEvent,
) (bool, error) {
	selectedSeqs := make(map[int64]struct{}, len(selected))
	for _, item := range selected {
		selectedSeqs[item.event.Seq] = struct{}{}
	}
	otherSeqs := make(map[int64]struct{}, len(otherEvents))
	for _, event := range otherEvents {
		otherSeqs[event.EventSeq] = struct{}{}
	}
	dependencies, err := state.IntentCaptureDependenciesForPair(
		ctx, db, cctx.BranchRef, cctx.BranchGeneration)
	if err != nil {
		return false, err
	}
	for _, dependency := range dependencies {
		if dependency.Strength != state.IntentDependencyHard {
			continue
		}
		_, fromSelected := selectedSeqs[dependency.PrerequisiteSeq]
		_, toSelected := selectedSeqs[dependency.DependentSeq]
		_, fromOther := otherSeqs[dependency.PrerequisiteSeq]
		_, toOther := otherSeqs[dependency.DependentSeq]
		if (fromSelected && toOther) || (fromOther && toSelected) {
			return true, nil
		}
	}
	return false, nil
}

func intentRepairCandidateSourceCommits(
	ctx context.Context,
	db *state.DB,
	candidate state.IntentCandidate,
) ([]string, error) {
	seen := make(map[string]struct{})
	var commits []string
	add := func(oid string) {
		oid = strings.TrimSpace(oid)
		if oid == "" {
			return
		}
		if _, ok := seen[oid]; ok {
			return
		}
		seen[oid] = struct{}{}
		commits = append(commits, oid)
	}
	if candidate.PublishedCommitOID.Valid {
		add(candidate.PublishedCommitOID.String)
	}
	queue := []string{candidate.ID}
	visited := make(map[string]struct{})
	traversed := 0
	for len(queue) > 0 {
		targetID := queue[0]
		queue = queue[1:]
		if _, ok := visited[targetID]; ok {
			continue
		}
		visited[targetID] = struct{}{}
		lineage, err := state.IntentCandidateLineageForTarget(
			ctx,
			db,
			candidate.BranchRef,
			candidate.BranchGeneration,
			targetID,
			state.IntentCandidateLineageMaxPerPair,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"daemon: intent v2 repair: load candidate lineage: %w",
				err,
			)
		}
		for _, edge := range lineage {
			traversed++
			if traversed > state.IntentCandidateLineageMaxPerPair {
				return nil, errors.New(
					"daemon: intent v2 repair: candidate lineage cap exceeded",
				)
			}
			if edge.SourcePublishedCommitOID.Valid {
				add(edge.SourcePublishedCommitOID.String)
			}
			queue = append(queue, edge.SourceCandidateID)
		}
	}
	if len(commits) == 0 {
		return nil,
			errors.New("daemon: intent v2 repair: candidate has no published commit lineage")
	}
	return commits, nil
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

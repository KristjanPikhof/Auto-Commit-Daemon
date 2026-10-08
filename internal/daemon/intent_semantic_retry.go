package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const MetaKeyIntentSemanticRetry = "intent.semantic.retry"

// IntentSemanticRetrySnapshot is one bounded review cooldown for unchanged
// capture evidence. It contains no captured source or provider response.
type IntentSemanticRetrySnapshot struct {
	Version             int     `json:"version"`
	BranchRef           string  `json:"branch_ref"`
	BranchGeneration    int64   `json:"branch_generation"`
	EvidenceFingerprint string  `json:"evidence_fingerprint"`
	PlanFingerprint     string  `json:"plan_fingerprint"`
	RetryAtTS           float64 `json:"retry_at_ts"`
}

func DecodeIntentSemanticRetrySnapshot(raw string) (IntentSemanticRetrySnapshot, error) {
	var record IntentSemanticRetrySnapshot
	if json.Unmarshal([]byte(raw), &record) != nil || record.Version != 1 ||
		record.BranchRef == "" || len(record.BranchRef) > 1024 || record.BranchGeneration < 0 ||
		!validIntentPlannerHealthFingerprint(record.EvidenceFingerprint) ||
		!validIntentPlannerHealthFingerprint(record.PlanFingerprint) ||
		!validIntentPlannerHealthTimestamp(record.RetryAtTS) || record.RetryAtTS <= 0 {
		return IntentSemanticRetrySnapshot{}, errors.New("invalid Intent semantic retry record")
	}
	return record, nil
}

type IntentSemanticRetryWaitError struct{ RetryAt time.Time }

func (e *IntentSemanticRetryWaitError) Error() string {
	return "Intent goal review is waiting until " + e.RetryAt.UTC().Format(time.RFC3339)
}

func loadIntentSemanticRetry(ctx context.Context, db *state.DB) (IntentSemanticRetrySnapshot, bool, error) {
	raw, found, err := state.MetaGet(ctx, db, MetaKeyIntentSemanticRetry)
	if err != nil || !found || raw == "" {
		return IntentSemanticRetrySnapshot{}, false, err
	}
	record, err := DecodeIntentSemanticRetrySnapshot(raw)
	return record, err == nil, err
}

func intentSemanticRetryEvidence(req ai.IntentPlanRequestV2, input IntentCandidateEvaluation, limit int) (string, error) {
	// Candidate saves, their clocks, and scheduling pressure are observations,
	// not new capture evidence. Actual captures, relationships, HEAD context,
	// provider identity, and configuration still invalidate this cooldown.
	req.Candidates = nil
	req.BaselineCandidates = nil
	req.ActivityBoundaries = nil
	req.ForcedAging = false
	run, err := newIntentPlanRun(req, input, limit)
	return run.Fingerprint, err
}

func holdUnclearIntentMessages(req ai.IntentPlanRequestV2, plan ai.IntentPlanV2, localFallback bool) (ai.IntentPlanV2, bool) {
	plan = cloneIntentPlanV2(plan)
	needsReview := false
	legacy := ai.LegacyIntentPlanRequest(req)
	for i, candidate := range plan.Candidates {
		if candidate.Readiness != ai.IntentCandidateReady {
			needsReview = needsReview || localFallback
			continue
		}
		quality := ai.EvaluateIntentPlanMessageQuality(legacy, ai.IntentPlan{
			SelectedSeqs: candidate.SelectedSeqs, Subject: candidate.Subject, Body: candidate.Body,
		})
		if !quality.HasReason(ai.MessageQualityReasonGenericSubject) &&
			!quality.HasReason(ai.MessageQualityReasonFilenameOnly) &&
			!quality.HasReason(ai.MessageQualityReasonTokenOnly) &&
			!quality.HasReason(ai.MessageQualityReasonMalformedSubject) &&
			!quality.HasReason(ai.MessageQualityReasonTruncatedSubject) {
			continue
		}
		needsReview = true
		plan.Candidates[i].Readiness = ai.IntentCandidateWait
		plan.Candidates[i].MissingCompanions = []string{"captured evidence needs a meaningful goal message"}
		plan.Candidates[i].Subject, plan.Candidates[i].Body = "", ""
	}
	return plan, needsReview
}

func scheduleIntentSemanticRetry(ctx context.Context, db *state.DB, input IntentCandidateEvaluation, run state.IntentPlanRun, evidence string, retryAt time.Time) error {
	return state.MetaSetJSON(ctx, db, MetaKeyIntentSemanticRetry, IntentSemanticRetrySnapshot{
		Version: 1, BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration,
		EvidenceFingerprint: evidence, PlanFingerprint: run.Fingerprint,
		RetryAtTS: float64(retryAt.UnixNano()) / 1e9,
	})
}

func reopenIntentSemanticPlanRun(ctx context.Context, db *state.DB, req ai.IntentPlanRequestV2, run state.IntentPlanRun, plan ai.IntentPlanV2) (state.IntentPlanRun, error) {
	var ready []ai.IntentCandidateAssignment
	for _, candidate := range plan.Candidates {
		if candidate.Readiness == ai.IntentCandidateReady {
			ready = append(ready, candidate)
		}
	}
	preserved, partial, ok := preserveIntentPlanGroups(req, ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: ready,
	}, nil)
	run.Completed = false
	run.AttemptCount, run.ProviderDeadlineTS = 0, 0
	run.ProgressState = sql.NullString{String: "semantic_retry_running", Valid: true}
	run.ResolutionMode = sql.NullString{}
	run.NormalizedPartition = sql.NullString{}
	run.PreservedGroups = nil
	run.UnresolvedSeqs = offeredIntentSeqs(req)
	run.ResolvedPlanJSON = sql.NullString{}
	if ok {
		run.PreservedGroups = intentAssignmentMembership(preserved)
		run.UnresolvedSeqs = offeredIntentSeqs(partial)
		if err := storeResolvedIntentPlanRun(&run, ai.IntentPlanV2{
			ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: preserved,
		}, nil); err != nil {
			return run, err
		}
	}
	return run, state.UpdateIntentPlanRun(ctx, db, run)
}

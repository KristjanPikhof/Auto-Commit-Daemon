package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
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
	ReviewCount         int     `json:"review_count,omitempty"`
	ScheduledAtTS       float64 `json:"scheduled_at_ts,omitempty"`
}

func DecodeIntentSemanticRetrySnapshot(raw string) (IntentSemanticRetrySnapshot, error) {
	var record IntentSemanticRetrySnapshot
	if len(raw) > 4096 || json.Unmarshal([]byte(raw), &record) != nil || record.Version != 1 ||
		record.BranchRef == "" || len(record.BranchRef) > 1024 || record.BranchGeneration < 0 ||
		!validIntentPlannerHealthFingerprint(record.EvidenceFingerprint) ||
		!validIntentPlannerHealthFingerprint(record.PlanFingerprint) ||
		!validIntentPlannerHealthTimestamp(record.RetryAtTS) || record.RetryAtTS <= 0 ||
		record.ReviewCount < 0 || record.ReviewCount > 3 ||
		(record.ReviewCount == 0 && record.ScheduledAtTS != 0) ||
		(record.ReviewCount > 0 && (!validIntentPlannerHealthTimestamp(record.ScheduledAtTS) || record.ScheduledAtTS <= 0 || record.ScheduledAtTS > record.RetryAtTS)) {
		return IntentSemanticRetrySnapshot{}, errors.New("invalid Intent semantic retry record")
	}
	return record, nil
}

type IntentSemanticRetryWaitError struct{ RetryAt time.Time }

func (e *IntentSemanticRetryWaitError) Error() string {
	return "Intent goal review is waiting until " + e.RetryAt.UTC().Format(time.RFC3339)
}

func loadIntentSemanticRetry(ctx context.Context, db *state.DB) (IntentSemanticRetrySnapshot, bool, error) {
	return loadIntentSemanticRetryKey(ctx, db, MetaKeyIntentSemanticRetry)
}

func loadIntentSemanticRetryKey(ctx context.Context, db *state.DB, key string) (IntentSemanticRetrySnapshot, bool, error) {
	raw, found, err := state.MetaGet(ctx, db, key)
	if err != nil || !found || raw == "" {
		return IntentSemanticRetrySnapshot{}, false, err
	}
	record, err := DecodeIntentSemanticRetrySnapshot(raw)
	return record, err == nil, err
}

func intentSemanticRetryKey(evidence string) string {
	return MetaKeyIntentSemanticRetry + "." + strings.TrimPrefix(evidence, "sha256:")
}

func loadIntentSemanticRetryForEvidence(ctx context.Context, db *state.DB, evidence string) (IntentSemanticRetrySnapshot, bool, error) {
	record, found, err := loadIntentSemanticRetryKey(ctx, db, intentSemanticRetryKey(evidence))
	if err != nil || found {
		return record, found, err
	}
	// Accept the earlier single-record format without losing its deadline.
	record, found, err = loadIntentSemanticRetry(ctx, db)
	return record, found && record.EvidenceFingerprint == evidence, err
}

func saveIntentSemanticRetry(ctx context.Context, db *state.DB, record IntentSemanticRetrySnapshot) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return state.MetaSetMany(ctx, db, map[string]string{
		intentSemanticRetryKey(record.EvidenceFingerprint): string(raw),
		MetaKeyIntentSemanticRetry:                         string(raw),
	})
}

func projectIntentSemanticRetry(ctx context.Context, db *state.DB, record IntentSemanticRetrySnapshot) error {
	current, found, err := loadIntentSemanticRetry(ctx, db)
	if err != nil || found && current == record {
		return err
	}
	return state.MetaSetJSON(ctx, db, MetaKeyIntentSemanticRetry, record)
}

func clearIntentSemanticRetry(ctx context.Context, db *state.DB, evidence string) error {
	if _, err := state.MetaDelete(ctx, db, intentSemanticRetryKey(evidence)); err != nil {
		return err
	}
	current, found, err := loadIntentSemanticRetry(ctx, db)
	if err != nil {
		return err
	}
	if found && current.EvidenceFingerprint == evidence {
		_, err = state.MetaDelete(ctx, db, MetaKeyIntentSemanticRetry)
	}
	return err
}

// Prune only derived cooldowns at a real planning transition. Active deadlines
// survive; expired records lose authority once their captures are terminal or
// a newer waiting plan already covers all of their still-pending captures.
func pruneIntentSemanticRetries(ctx context.Context, db *state.DB, evidence string, now time.Time) error {
	_, err := db.SQL().ExecContext(ctx, `
DELETE FROM daemon_meta
WHERE key LIKE ? AND key<>? AND (
 value='' OR (json_valid(value) AND json_extract(value,'$.retry_at_ts')<=? AND (
  NOT EXISTS (
   SELECT 1 FROM intent_plan_runs old, json_each(old.unresolved_seqs) seq
   JOIN capture_events ev ON ev.seq=seq.value AND ev.state='pending'
   WHERE old.fingerprint=json_extract(daemon_meta.value,'$.plan_fingerprint')
  ) OR EXISTS (
   SELECT 1 FROM intent_plan_runs old JOIN intent_plan_runs newer
    ON newer.branch_ref=old.branch_ref AND newer.branch_generation=old.branch_generation
    AND newer.updated_ts>old.updated_ts AND newer.progress_state='waiting_semantic_retry'
   WHERE old.fingerprint=json_extract(daemon_meta.value,'$.plan_fingerprint')
   AND NOT EXISTS (
    SELECT 1 FROM json_each(old.unresolved_seqs) seq
    JOIN capture_events ev ON ev.seq=seq.value AND ev.state='pending'
    WHERE NOT EXISTS (SELECT 1 FROM json_each(newer.unresolved_seqs) covered WHERE covered.value=seq.value)
   )
  )
 )))`, MetaKeyIntentSemanticRetry+".%", intentSemanticRetryKey(evidence), intentPlannerHealthTimestamp(now))
	return err
}

// A due review gets one bounded session even when independent new work keeps
// arriving. Renewing its cooldown returns priority to the normal fresh window.
// Selection proves membership against the replay-safe pending queue and does
// not reserve attempts or change state.
func dueIntentSemanticReviewWindow(ctx context.Context, db *state.DB, pending []state.CaptureEvent, size int, now time.Time) ([]state.CaptureEvent, error) {
	if len(pending) == 0 || size <= 0 || pending[0].BranchRef == "" {
		return nil, nil
	}
	head := pending[0]
	// Older workers narrowed retry membership to rejected READY goals even
	// when their saved plan also retained WAIT goals. Derive that omitted
	// membership from the saved native plan and current ownership, read-only.
	owners := map[int64]string{}
	ownerRows, err := db.ReadSQL().QueryContext(ctx, `
SELECT member.event_seq, candidate.id
FROM intent_candidate_events member
JOIN intent_candidates candidate ON candidate.id=member.candidate_id
JOIN capture_events event ON event.seq=member.event_seq
WHERE member.membership_state='active' AND candidate.status='waiting'
 AND candidate.readiness='wait' AND candidate.published_commit_oid IS NULL
 AND candidate.soft_publication_deadline IS NULL
 AND candidate.branch_ref=? AND candidate.branch_generation=?
 AND event.state='pending' AND event.branch_ref=candidate.branch_ref
 AND event.branch_generation=candidate.branch_generation
ORDER BY member.event_seq LIMIT ?`, head.BranchRef, head.BranchGeneration,
		state.IntentCandidateMaxOpenPerPair*state.IntentCandidateMaxCaptures)
	if err != nil {
		return nil, err
	}
	for ownerRows.Next() {
		var seq int64
		var id string
		if err := ownerRows.Scan(&seq, &id); err != nil {
			ownerRows.Close()
			return nil, err
		}
		owners[seq] = id
	}
	if err := ownerRows.Close(); err != nil {
		return nil, err
	}
	if err := ownerRows.Err(); err != nil {
		return nil, err
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT retry.value, run.unresolved_seqs,
 CASE WHEN length(run.resolved_plan_json)<=? THEN coalesce(run.resolved_plan_json,'') ELSE '' END
FROM daemon_meta retry JOIN intent_plan_runs run
 ON run.fingerprint=json_extract(retry.value,'$.plan_fingerprint')
WHERE retry.key LIKE ? AND json_valid(retry.value)
 AND json_extract(retry.value,'$.branch_ref')=?
 AND json_extract(retry.value,'$.branch_generation')=?
 AND (json_extract(retry.value,'$.retry_at_ts')-
  CASE WHEN coalesce(json_extract(retry.value,'$.review_count'),0)=0 THEN 3300 ELSE 0 END)<=?
 AND run.branch_ref=? AND run.branch_generation=?
 AND run.progress_state IN ('waiting_semantic_retry','semantic_retry_running','waiting_for_ai')
 AND (EXISTS (
  SELECT 1 FROM json_each(run.unresolved_seqs) member
  JOIN capture_events event ON event.seq=member.value
  WHERE event.state='pending' AND event.branch_ref=run.branch_ref
   AND event.branch_generation=run.branch_generation
 ) OR EXISTS (
  SELECT 1 FROM json_each(CASE
   WHEN length(run.resolved_plan_json)<=? AND json_valid(run.resolved_plan_json)
   THEN CASE WHEN json_extract(run.resolved_plan_json,'$.plan.protocol_version')='v2'
    THEN json_extract(run.resolved_plan_json,'$.plan.candidates') ELSE '[]' END
   ELSE '[]' END) goal
  JOIN intent_candidates candidate ON candidate.id=json_extract(goal.value,'$.candidate_id')
  JOIN intent_candidate_events member ON member.candidate_id=candidate.id
  JOIN capture_events event ON event.seq=member.event_seq
  WHERE json_extract(goal.value,'$.readiness')='wait'
   AND candidate.status='waiting' AND candidate.readiness='wait'
   AND candidate.published_commit_oid IS NULL AND candidate.soft_publication_deadline IS NULL
   AND member.membership_state='active'
   AND candidate.branch_ref=run.branch_ref AND candidate.branch_generation=run.branch_generation
   AND event.state='pending' AND event.branch_ref=run.branch_ref
   AND event.branch_generation=run.branch_generation
   AND EXISTS (SELECT 1 FROM json_each(goal.value,'$.selected_seqs') selected WHERE selected.value=event.seq)
 ))
ORDER BY json_extract(retry.value,'$.retry_at_ts')-
 CASE WHEN coalesce(json_extract(retry.value,'$.review_count'),0)=0 THEN 3300 ELSE 0 END, retry.key
LIMIT ?`, state.IntentResolvedPlanJSONCap, MetaKeyIntentSemanticRetry+".%", head.BranchRef, head.BranchGeneration,
		intentPlannerHealthTimestamp(now), head.BranchRef, head.BranchGeneration,
		state.IntentResolvedPlanJSONCap, state.IntentCandidateMaxOpenPerPair)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw, members, savedPlan string
		if err := rows.Scan(&raw, &members, &savedPlan); err != nil {
			return nil, err
		}
		if _, err := DecodeIntentSemanticRetrySnapshot(raw); err != nil || len(members) > state.IntentCandidateMaxCaptures*24+2 {
			continue
		}
		var seqs []int64
		if json.Unmarshal([]byte(members), &seqs) != nil || len(seqs) > state.IntentCandidateMaxCaptures {
			continue
		}
		wanted := make(map[int64]bool, len(seqs))
		for _, seq := range seqs {
			wanted[seq] = true
		}
		var saved resolvedIntentPlanRun
		if savedPlan != "" && json.Unmarshal([]byte(savedPlan), &saved) == nil &&
			saved.Plan.ProtocolVersion == ai.IntentPlannerProtocolV2 && len(saved.Plan.Candidates) <= ai.IntentOpenCandidateCap {
			memberCount := 0
			for _, candidate := range saved.Plan.Candidates {
				memberCount += len(candidate.SelectedSeqs)
			}
			if memberCount <= state.IntentCandidateMaxCaptures {
				for _, candidate := range saved.Plan.Candidates {
					if candidate.Readiness != ai.IntentCandidateWait {
						continue
					}
					for _, seq := range candidate.SelectedSeqs {
						if owner, known := owners[seq]; known && owner == candidate.CandidateID {
							wanted[seq] = true
						}
					}
				}
			}
		}
		var window []state.CaptureEvent
		for _, event := range pending {
			if wanted[event.Seq] && event.BranchRef == head.BranchRef && event.BranchGeneration == head.BranchGeneration {
				window = append(window, event)
				if len(window) == size {
					break
				}
			}
		}
		if len(window) > 0 {
			return window, nil
		}
	}
	return nil, rows.Err()
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

func holdUnclearIntentMessages(req ai.IntentPlanRequestV2, plan ai.IntentPlanV2) (ai.IntentPlanV2, bool) {
	plan = cloneIntentPlanV2(plan)
	needsReview := false
	legacy := ai.LegacyIntentPlanRequest(req)
	for i, candidate := range plan.Candidates {
		if candidate.Readiness != ai.IntentCandidateReady {
			needsReview = true
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

func intentSemanticReviewDelay(count int) time.Duration {
	switch count {
	case 1:
		return 5 * time.Minute
	case 2:
		return 10 * time.Minute
	default:
		return time.Hour
	}
}

// Only worker planning shortens a legacy one-hour cooldown. Decoding and
// status remain read-only, and the stored original deadline anchors migration
// so a restart cannot schedule another five minutes from the current clock.
func normalizeLegacyIntentSemanticRetry(ctx context.Context, db *state.DB, record IntentSemanticRetrySnapshot) (IntentSemanticRetrySnapshot, error) {
	if record.ReviewCount != 0 {
		return record, nil
	}
	record.ReviewCount = 1
	record.ScheduledAtTS = record.RetryAtTS - time.Hour.Seconds()
	if record.ScheduledAtTS <= 0 {
		return IntentSemanticRetrySnapshot{}, errors.New("invalid legacy Intent semantic retry timestamp")
	}
	record.RetryAtTS = record.ScheduledAtTS + intentSemanticReviewDelay(1).Seconds()
	return record, saveIntentSemanticRetry(ctx, db, record)
}

func scheduleIntentSemanticRetry(ctx context.Context, db *state.DB, input IntentCandidateEvaluation, run state.IntentPlanRun, evidence string, scheduledAt time.Time) (IntentSemanticRetrySnapshot, error) {
	now := input.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if scheduledAt.IsZero() {
		scheduledAt = now
	}
	previous, found, err := loadIntentSemanticRetryForEvidence(ctx, db, evidence)
	if err != nil {
		return IntentSemanticRetrySnapshot{}, err
	}
	if found {
		previous, err = normalizeLegacyIntentSemanticRetry(ctx, db, previous)
		if err != nil {
			return IntentSemanticRetrySnapshot{}, err
		}
		if intentPlannerHealthTimestamp(now) < previous.RetryAtTS {
			return previous, projectIntentSemanticRetry(ctx, db, previous)
		}
	}
	count := 1
	if found {
		count = min(previous.ReviewCount+1, 3)
	}
	record := IntentSemanticRetrySnapshot{
		Version: 1, BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration,
		EvidenceFingerprint: evidence, PlanFingerprint: run.Fingerprint,
		ReviewCount: count, ScheduledAtTS: intentPlannerHealthTimestamp(scheduledAt),
		RetryAtTS: intentPlannerHealthTimestamp(scheduledAt.Add(intentSemanticReviewDelay(count))),
	}
	if err := pruneIntentSemanticRetries(ctx, db, evidence, now); err != nil {
		return IntentSemanticRetrySnapshot{}, err
	}
	return record, saveIntentSemanticRetry(ctx, db, record)
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

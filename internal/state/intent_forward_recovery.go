package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const intentForwardRecoveryMetaKey = "intent.v2.forward_recovery"

const failedIntentCheckpointRecoveryReason = "verification_failed_checkpoint_replan"

// IntentForwardRecovery is the durable forward-only salvage state for a
// candidate that must be replanned without changing captured work.
type IntentForwardRecovery struct {
	BranchRef        string  `json:"branch_ref"`
	BranchGeneration int64   `json:"branch_generation"`
	CandidateID      string  `json:"candidate_id"`
	Reason           string  `json:"reason"`
	Stage            string  `json:"stage,omitempty"`
	TargetEventSeqs  []int64 `json:"target_event_seqs,omitempty"`
	UnlockCount      int     `json:"unlock_count,omitempty"`
	LastProgressTS   float64 `json:"last_progress_ts,omitempty"`
	PlanFingerprint  string  `json:"plan_fingerprint,omitempty"`
	PrefixCursor     int     `json:"prefix_cursor,omitempty"`
	PrefixUnresolvedCount int `json:"prefix_unresolved_count,omitempty"`
	PrefixBaseHead   string  `json:"prefix_base_head,omitempty"`
	PrefixExhausted  bool    `json:"prefix_exhausted,omitempty"`
	NeedsAttention   bool    `json:"needs_attention,omitempty"`
	AttentionReason  string  `json:"attention_reason,omitempty"`
}

// OldestOverdueFailedIntentEventSeq returns the earliest pending capture held
// by a failed, unpublished candidate after that capture reaches the configured
// defer limit. Capture order wins over planner timestamps so unrelated newer
// work cannot starve checkpoint recovery.
func OldestOverdueFailedIntentEventSeq(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	deferLimit int,
) (int64, bool, error) {
	if d == nil || strings.TrimSpace(branchRef) == "" || branchGeneration < 0 {
		return 0, false, errors.New(
			"state: OldestOverdueFailedIntentEventSeq: invalid input")
	}
	if deferLimit < 0 {
		deferLimit = 0
	}
	var seq int64
	err := d.readSQL().QueryRowContext(ctx, `
WITH barrier AS (
    SELECT MIN(seq) AS first_seq
    FROM capture_events
    WHERE branch_ref=?
      AND branch_generation=?
      AND state IN (?,?)
)
SELECT event.seq
FROM intent_candidate_events membership
JOIN intent_candidates candidate ON candidate.id=membership.candidate_id
JOIN capture_events event ON event.seq=membership.event_seq
JOIN planner_state planner ON planner.event_seq=event.seq
JOIN checkpoint_events checkpoint_event ON checkpoint_event.event_seq=event.seq
JOIN checkpoints checkpoint ON checkpoint.id=checkpoint_event.checkpoint_id
CROSS JOIN barrier
WHERE membership.membership_state='active'
  AND candidate.branch_ref=?
  AND candidate.branch_generation=?
  AND candidate.status IN ('waiting','blocked')
  AND candidate.verification_status='failed'
  AND candidate.published_commit_oid IS NULL
  AND candidate.soft_publication_deadline IS NULL
  AND event.branch_ref=?
  AND event.branch_generation=?
  AND event.state='pending'
  AND planner.defer_count>=?
  AND checkpoint.phase='completed'
  AND (barrier.first_seq IS NULL OR event.seq<barrier.first_seq)
ORDER BY event.seq
LIMIT 1`, branchRef, branchGeneration, EventStateBlockedConflict,
		EventStateFailed, branchRef, branchGeneration, branchRef,
		branchGeneration, deferLimit).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf(
			"state: find overdue failed intent event: %w", err)
	}
	return seq, true, nil
}

// StartFailedIntentCheckpointRecovery atomically releases the bounded closure
// of failed candidates and completed checkpoints containing one held capture,
// then freezes that exact pending target for a semantic replan. It may release
// an incomplete waiting sibling because that sibling has no publishable
// boundary to preserve. It refuses any closure containing a ready or verified
// candidate, resolved capture, mixed branch pair, or active Git/state
// transition.
func StartFailedIntentCheckpointRecovery(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	heldEventSeq int64,
) (IntentForwardRecovery, bool, error) {
	var recovery IntentForwardRecovery
	if d == nil || strings.TrimSpace(branchRef) == "" ||
		branchGeneration < 0 || heldEventSeq <= 0 {
		return recovery, false, errors.New(
			"state: StartFailedIntentCheckpointRecovery: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return recovery, false, fmt.Errorf(
			"state: begin failed intent checkpoint recovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var markerRaw string
	err = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key=?`,
		intentForwardRecoveryMetaKey).Scan(&markerRaw)
	if err == nil {
		if err := json.Unmarshal([]byte(markerRaw), &recovery); err != nil {
			return IntentForwardRecovery{}, false, fmt.Errorf(
				"state: parse existing intent forward recovery: %w", err)
		}
		if recovery.Stage == "" {
			recovery.Stage = "semantic_replan"
		}
		if recovery.BranchRef == branchRef &&
			recovery.BranchGeneration == branchGeneration &&
			containsEventSeq(recovery.TargetEventSeqs, heldEventSeq) {
			return recovery, false, nil
		}
		return IntentForwardRecovery{}, false, errors.New(
			"state: conflicting intent forward recovery is active")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return recovery, false, fmt.Errorf(
			"state: inspect existing intent forward recovery: %w", err)
	}

	type candidateMember struct {
		seq  int64
		path string
	}
	type candidateRecord struct {
		id           string
		status       string
		readiness    string
		verification sql.NullString
		published    sql.NullString
		deadline     sql.NullFloat64
		revision     sql.NullInt64
		profile      sql.NullString
		members      []candidateMember
	}
	rootRows, err := tx.QueryContext(ctx, `
SELECT candidate.id
FROM intent_candidate_events membership
JOIN intent_candidates candidate ON candidate.id=membership.candidate_id
WHERE membership.event_seq=? AND membership.membership_state='active'
ORDER BY candidate.id`, heldEventSeq)
	if err != nil {
		return recovery, false, fmt.Errorf(
			"state: load held intent candidate: %w", err)
	}
	var rootCandidateIDs []string
	for rootRows.Next() {
		var candidateID string
		if err := rootRows.Scan(&candidateID); err != nil {
			_ = rootRows.Close()
			return recovery, false, fmt.Errorf(
				"state: scan held intent candidate: %w", err)
		}
		rootCandidateIDs = append(rootCandidateIDs, candidateID)
	}
	if err := rootRows.Err(); err != nil {
		_ = rootRows.Close()
		return recovery, false, fmt.Errorf(
			"state: iterate held intent candidates: %w", err)
	}
	if err := rootRows.Close(); err != nil {
		return recovery, false, fmt.Errorf(
			"state: close held intent candidates: %w", err)
	}
	if len(rootCandidateIDs) != 1 {
		return recovery, false, nil
	}

	candidates := make(map[string]candidateRecord)
	candidateQueued := map[string]struct{}{rootCandidateIDs[0]: {}}
	candidateQueue := []string{rootCandidateIDs[0]}
	checkpointSeen := make(map[string]struct{})
	checkpointQueued := make(map[string]struct{})
	var checkpointQueue []string
	targetEvents := make(map[int64]struct{})

	for len(candidateQueue) > 0 || len(checkpointQueue) > 0 {
		if len(candidateQueue) > 0 {
			candidateID := candidateQueue[0]
			candidateQueue = candidateQueue[1:]
			if _, loaded := candidates[candidateID]; loaded {
				continue
			}
			var candidate candidateRecord
			var candidateBranch string
			var candidateGeneration int64
			err := tx.QueryRowContext(ctx, `
SELECT id,branch_ref,branch_generation,status,readiness,verification_status,
       published_commit_oid,soft_publication_deadline,
       config_revision_id,config_profile
FROM intent_candidates WHERE id=?`, candidateID).Scan(
				&candidate.id, &candidateBranch, &candidateGeneration,
				&candidate.status, &candidate.readiness,
				&candidate.verification,
				&candidate.published, &candidate.deadline,
				&candidate.revision, &candidate.profile)
			if errors.Is(err, sql.ErrNoRows) {
				return IntentForwardRecovery{}, false, nil
			}
			if err != nil {
				return IntentForwardRecovery{}, false, fmt.Errorf(
					"state: load failed intent closure candidate: %w", err)
			}
			failed := (candidate.status == IntentCandidateWaiting ||
				candidate.status == IntentCandidateBlocked) &&
				candidate.verification.Valid &&
				candidate.verification.String == "failed"
			incomplete := candidate.status == IntentCandidateWaiting &&
				candidate.readiness == IntentReadinessWait &&
				(!candidate.verification.Valid ||
					candidate.verification.String == "pending")
			root := candidateID == rootCandidateIDs[0]
			if candidateBranch != branchRef ||
				candidateGeneration != branchGeneration ||
				(!failed && (root || !incomplete)) || candidate.published.Valid ||
				candidate.deadline.Valid {
				return IntentForwardRecovery{}, false, nil
			}

			memberRows, err := tx.QueryContext(ctx, `
SELECT membership.event_seq,event.path,event.state,event.branch_ref,
       event.branch_generation,checkpoint_event.checkpoint_id
FROM intent_candidate_events membership
JOIN capture_events event ON event.seq=membership.event_seq
LEFT JOIN checkpoint_events checkpoint_event
  ON checkpoint_event.event_seq=membership.event_seq
WHERE membership.candidate_id=? AND membership.membership_state='active'
ORDER BY membership.ord,membership.event_seq`, candidateID)
			if err != nil {
				return IntentForwardRecovery{}, false, fmt.Errorf(
					"state: load failed intent closure members: %w", err)
			}
			for memberRows.Next() {
				var member candidateMember
				var eventState, eventBranchRef string
				var eventGeneration int64
				var checkpointID sql.NullString
				if err := memberRows.Scan(
					&member.seq, &member.path, &eventState, &eventBranchRef,
					&eventGeneration, &checkpointID); err != nil {
					_ = memberRows.Close()
					return IntentForwardRecovery{}, false, fmt.Errorf(
						"state: scan failed intent closure member: %w", err)
				}
				if eventState != EventStatePending || eventBranchRef != branchRef ||
					eventGeneration != branchGeneration || !checkpointID.Valid ||
					strings.TrimSpace(checkpointID.String) == "" {
					_ = memberRows.Close()
					return IntentForwardRecovery{}, false, nil
				}
				candidate.members = append(candidate.members, member)
				if _, seen := checkpointSeen[checkpointID.String]; !seen {
					if _, queued := checkpointQueued[checkpointID.String]; !queued {
						checkpointQueued[checkpointID.String] = struct{}{}
						checkpointQueue = append(checkpointQueue, checkpointID.String)
					}
				}
			}
			if err := memberRows.Err(); err != nil {
				_ = memberRows.Close()
				return IntentForwardRecovery{}, false, fmt.Errorf(
					"state: iterate failed intent closure members: %w", err)
			}
			if err := memberRows.Close(); err != nil {
				return IntentForwardRecovery{}, false, fmt.Errorf(
					"state: close failed intent closure members: %w", err)
			}
			if len(candidate.members) == 0 {
				return IntentForwardRecovery{}, false, nil
			}
			candidates[candidateID] = candidate
			continue
		}

		checkpointID := checkpointQueue[0]
		checkpointQueue = checkpointQueue[1:]
		if _, seen := checkpointSeen[checkpointID]; seen {
			continue
		}
		checkpointSeen[checkpointID] = struct{}{}
		targetRows, err := tx.QueryContext(ctx, `
SELECT checkpoint_event.event_seq,event.state,event.branch_ref,
       event.branch_generation,checkpoint.phase
FROM checkpoint_events checkpoint_event
JOIN checkpoints checkpoint ON checkpoint.id=checkpoint_event.checkpoint_id
JOIN capture_events event ON event.seq=checkpoint_event.event_seq
WHERE checkpoint_event.checkpoint_id=?
ORDER BY checkpoint_event.ord`, checkpointID)
		if err != nil {
			return IntentForwardRecovery{}, false, fmt.Errorf(
				"state: load failed intent closure checkpoint: %w", err)
		}
		var checkpointEvents []int64
		for targetRows.Next() {
			var seq, eventGeneration int64
			var eventState, eventBranchRef, checkpointPhase string
			if err := targetRows.Scan(
				&seq, &eventState, &eventBranchRef, &eventGeneration,
				&checkpointPhase); err != nil {
				_ = targetRows.Close()
				return IntentForwardRecovery{}, false, fmt.Errorf(
					"state: scan failed intent closure checkpoint: %w", err)
			}
			if checkpointPhase != CheckpointCompleted ||
				eventBranchRef != branchRef ||
				eventGeneration != branchGeneration {
				_ = targetRows.Close()
				return IntentForwardRecovery{}, false, nil
			}
			switch eventState {
			case EventStatePublished, EventStateRecovered:
				// A prior bounded recovery may already have settled part of this
				// checkpoint. Keep that proof intact and replan only the durable
				// pending remainder.
				continue
			case EventStatePending:
			default:
				_ = targetRows.Close()
				return IntentForwardRecovery{}, false, nil
			}
			checkpointEvents = append(checkpointEvents, seq)
			targetEvents[seq] = struct{}{}
			if len(targetEvents) > IntentCandidateMaxCaptures {
				_ = targetRows.Close()
				return IntentForwardRecovery{}, false, nil
			}
		}
		if err := targetRows.Err(); err != nil {
			_ = targetRows.Close()
			return IntentForwardRecovery{}, false, fmt.Errorf(
				"state: iterate failed intent closure checkpoint: %w", err)
		}
		if err := targetRows.Close(); err != nil {
			return IntentForwardRecovery{}, false, fmt.Errorf(
				"state: close failed intent closure checkpoint: %w", err)
		}
		if len(checkpointEvents) == 0 {
			return IntentForwardRecovery{}, false, nil
		}
		for _, seq := range checkpointEvents {
			ownerRows, err := tx.QueryContext(ctx, `
SELECT candidate_id FROM intent_candidate_events
WHERE event_seq=? AND membership_state='active'
ORDER BY candidate_id`, seq)
			if err != nil {
				return IntentForwardRecovery{}, false, fmt.Errorf(
					"state: load failed intent closure owners: %w", err)
			}
			for ownerRows.Next() {
				var ownerID string
				if err := ownerRows.Scan(&ownerID); err != nil {
					_ = ownerRows.Close()
					return IntentForwardRecovery{}, false, fmt.Errorf(
						"state: scan failed intent closure owner: %w", err)
				}
				if _, queued := candidateQueued[ownerID]; queued {
					continue
				}
				candidateQueued[ownerID] = struct{}{}
				if len(candidateQueued) > IntentCandidateMaxOpenPerPair {
					_ = ownerRows.Close()
					return IntentForwardRecovery{}, false, nil
				}
				candidateQueue = append(candidateQueue, ownerID)
			}
			if err := ownerRows.Err(); err != nil {
				_ = ownerRows.Close()
				return IntentForwardRecovery{}, false, fmt.Errorf(
					"state: iterate failed intent closure owners: %w", err)
			}
			if err := ownerRows.Close(); err != nil {
				return IntentForwardRecovery{}, false, fmt.Errorf(
					"state: close failed intent closure owners: %w", err)
			}
		}
	}

	if len(candidates) == 0 || len(targetEvents) == 0 {
		return IntentForwardRecovery{}, false, nil
	}
	recovery.TargetEventSeqs = make([]int64, 0, len(targetEvents))
	for seq := range targetEvents {
		recovery.TargetEventSeqs = append(recovery.TargetEventSeqs, seq)
	}
	sort.Slice(recovery.TargetEventSeqs, func(i, j int) bool {
		return recovery.TargetEventSeqs[i] < recovery.TargetEventSeqs[j]
	})
	if !containsEventSeq(recovery.TargetEventSeqs, heldEventSeq) {
		return IntentForwardRecovery{}, false, nil
	}

	var activePublication, activeRepair, activeDrain int
	if err := tx.QueryRowContext(ctx, `
SELECT
  EXISTS(
    SELECT 1 FROM self_publications
    WHERE branch_ref=? AND branch_generation=?
      AND phase IN ('prepared','git_applied')
  ),
  EXISTS(
    SELECT 1 FROM intent_repairs
    WHERE branch_ref=? AND branch_generation=?
      AND status IN ('prepared','git_applied')
  ),
  EXISTS(
    SELECT 1 FROM publication_drains
    WHERE branch_ref=? AND branch_generation=?
      AND phase NOT IN ('completed','needs_action')
  )`,
		branchRef, branchGeneration,
		branchRef, branchGeneration,
		branchRef, branchGeneration).Scan(
		&activePublication, &activeRepair, &activeDrain); err != nil {
		return IntentForwardRecovery{}, false, fmt.Errorf(
			"state: inspect failed intent recovery ownership: %w", err)
	}
	switch {
	case activePublication != 0:
		return IntentForwardRecovery{}, false, errors.New(
			"state: failed intent checkpoint recovery requires self-publication recovery")
	case activeRepair != 0:
		return IntentForwardRecovery{}, false, errors.New(
			"state: failed intent checkpoint recovery requires intent repair recovery")
	case activeDrain != 0:
		return IntentForwardRecovery{}, false, errors.New(
			"state: failed intent checkpoint recovery requires publication drain recovery")
	}

	now := nowSeconds()
	candidateIDs := make([]string, 0, len(candidates))
	for candidateID := range candidates {
		candidateIDs = append(candidateIDs, candidateID)
	}
	sort.Strings(candidateIDs)
	for _, candidateID := range candidateIDs {
		candidate := candidates[candidateID]
		result, err := tx.ExecContext(ctx, `
UPDATE intent_candidates
SET status='superseded',readiness='wait',soft_publication_deadline=NULL,
    updated_ts=?
WHERE id=? AND branch_ref=? AND branch_generation=?
  AND (
    (status IN ('waiting','blocked') AND verification_status='failed')
    OR
    (status='waiting' AND readiness='wait'
      AND (verification_status IS NULL OR verification_status='pending'))
  )
  AND published_commit_oid IS NULL
  AND soft_publication_deadline IS NULL`,
			now, candidate.id, branchRef, branchGeneration)
		if err != nil {
			return IntentForwardRecovery{}, false, fmt.Errorf(
				"state: retire failed intent checkpoint candidate: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return IntentForwardRecovery{}, false, err
		}
		if changed != 1 {
			return IntentForwardRecovery{}, false, nil
		}
		result, err = tx.ExecContext(ctx, `
UPDATE intent_candidate_events
	SET membership_state='superseded'
	WHERE candidate_id=? AND membership_state='active'`, candidate.id)
		if err != nil {
			return IntentForwardRecovery{}, false, fmt.Errorf(
				"state: release failed intent checkpoint membership: %w", err)
		}
		released, err := result.RowsAffected()
		if err != nil {
			return IntentForwardRecovery{}, false, err
		}
		if released != int64(len(candidate.members)) {
			return IntentForwardRecovery{}, false, errors.New(
				"state: failed intent checkpoint membership changed")
		}
	}

	recovery.BranchRef = branchRef
	recovery.BranchGeneration = branchGeneration
	recovery.CandidateID = rootCandidateIDs[0]
	recovery.Reason = failedIntentCheckpointRecoveryReason
	recovery.Stage = "semantic_replan"
	recovery.LastProgressTS = now
	for _, candidateID := range candidateIDs {
		candidate := candidates[candidateID]
		for _, member := range candidate.members {
			if _, err := appendDecision(ctx, tx, DecisionRecord{
				DecisionTS:       now,
				Kind:             DecisionKindIntentForwardRecovery,
				Path:             sql.NullString{String: member.path, Valid: member.path != ""},
				Reason:           sql.NullString{String: recovery.Reason, Valid: true},
				EventSeq:         sql.NullInt64{Int64: member.seq, Valid: true},
				BranchRef:        sql.NullString{String: branchRef, Valid: true},
				BranchGeneration: sql.NullInt64{Int64: branchGeneration, Valid: true},
				ActionTaken: sql.NullString{
					String: "retired_checkpoint_candidate_for_recovery", Valid: true,
				},
				UserMessage: sql.NullString{
					String: "ACD released incomplete or failed commit groups and will replan their complete protected checkpoints.",
					Valid:  true,
				},
				ConfigRevisionID: candidate.revision,
				ConfigProfile:    candidate.profile,
			}); err != nil {
				return IntentForwardRecovery{}, false, err
			}
		}
	}
	markerRawBytes, err := json.Marshal(recovery)
	if err != nil {
		return IntentForwardRecovery{}, false, err
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO daemon_meta(key,value,updated_ts)
VALUES(?,?,?) ON CONFLICT(key) DO NOTHING`,
		intentForwardRecoveryMetaKey, string(markerRawBytes), now)
	if err != nil {
		return IntentForwardRecovery{}, false, fmt.Errorf(
			"state: persist failed intent checkpoint recovery: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return IntentForwardRecovery{}, false, err
	}
	if inserted != 1 {
		return IntentForwardRecovery{}, false, errors.New(
			"state: intent forward recovery marker changed")
	}
	if err := tx.Commit(); err != nil {
		return IntentForwardRecovery{}, false, fmt.Errorf(
			"state: commit failed intent checkpoint recovery: %w", err)
	}
	return recovery, true, nil
}

func containsEventSeq(seqs []int64, target int64) bool {
	for _, seq := range seqs {
		if seq == target {
			return true
		}
	}
	return false
}

// ForwardRecoverIntentCandidate retires one blocking candidate without
// changing capture state or Git history. Pending members become available for
// a new forward-only candidate, while published members remain published.
func ForwardRecoverIntentCandidate(
	ctx context.Context,
	d *DB,
	recovery IntentForwardRecovery,
) (int, bool, error) {
	if d == nil || strings.TrimSpace(recovery.BranchRef) == "" ||
		recovery.BranchGeneration < 0 || strings.TrimSpace(recovery.CandidateID) == "" ||
		strings.TrimSpace(recovery.Reason) == "" {
		return 0, false, errors.New(
			"state: ForwardRecoverIntentCandidate: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("state: begin intent forward recovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status string
	var revision sql.NullInt64
	var profile sql.NullString
	err = tx.QueryRowContext(ctx, `
SELECT status,config_revision_id,config_profile
FROM intent_candidates
WHERE id=? AND branch_ref=? AND branch_generation=?`,
		recovery.CandidateID, recovery.BranchRef,
		recovery.BranchGeneration).Scan(&status, &revision, &profile)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("state: load forward recovery candidate: %w", err)
	}
	switch status {
	case IntentCandidateOpen, IntentCandidateWaiting, IntentCandidateReady,
		IntentCandidateSoftPublished, IntentCandidateBlocked:
	default:
		return 0, false, nil
	}

	var recoverable int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM self_publications
    WHERE branch_ref=? AND branch_generation=?
      AND phase IN ('prepared','git_applied')
)`, recovery.BranchRef, recovery.BranchGeneration).Scan(&recoverable); err != nil {
		return 0, false, fmt.Errorf(
			"state: inspect forward recovery publications: %w", err)
	}
	if recoverable != 0 {
		return 0, false, errors.New(
			"state: intent forward recovery requires self-publication recovery")
	}

	type pendingMember struct {
		seq  int64
		path string
	}
	rows, err := tx.QueryContext(ctx, `
SELECT event.seq,event.path
FROM intent_candidate_events membership
JOIN capture_events event ON event.seq=membership.event_seq
WHERE membership.candidate_id=? AND membership.membership_state='active'
  AND event.state='pending'
ORDER BY membership.ord,event.seq`, recovery.CandidateID)
	if err != nil {
		return 0, false, fmt.Errorf("state: load forward recovery members: %w", err)
	}
	var pending []pendingMember
	for rows.Next() {
		var member pendingMember
		if err := rows.Scan(&member.seq, &member.path); err != nil {
			rows.Close()
			return 0, false, err
		}
		pending = append(pending, member)
	}
	if err := rows.Close(); err != nil {
		return 0, false, err
	}
	target := make(map[int64]struct{}, len(pending))
	queue := make([]int64, 0, len(pending))
	for _, member := range pending {
		target[member.seq] = struct{}{}
		queue = append(queue, member.seq)
	}
	edgeRows, err := tx.QueryContext(ctx, `
SELECT dependency.prerequisite_seq,dependency.dependent_seq
FROM intent_capture_dependencies dependency
JOIN capture_events prerequisite ON prerequisite.seq=dependency.prerequisite_seq
JOIN capture_events dependent ON dependent.seq=dependency.dependent_seq
WHERE dependency.branch_ref=? AND dependency.branch_generation=?
  AND dependency.strength='hard'
  AND prerequisite.state='pending' AND dependent.state='pending'`,
		recovery.BranchRef, recovery.BranchGeneration)
	if err != nil {
		return 0, false, fmt.Errorf("state: load forward recovery dependencies: %w", err)
	}
	adjacent := make(map[int64][]int64)
	for edgeRows.Next() {
		var left, right int64
		if err := edgeRows.Scan(&left, &right); err != nil {
			edgeRows.Close()
			return 0, false, err
		}
		adjacent[left] = append(adjacent[left], right)
		adjacent[right] = append(adjacent[right], left)
	}
	if err := edgeRows.Close(); err != nil {
		return 0, false, err
	}
	for len(queue) > 0 {
		seq := queue[0]
		queue = queue[1:]
		for _, neighbor := range adjacent[seq] {
			if _, seen := target[neighbor]; seen {
				continue
			}
			target[neighbor] = struct{}{}
			if len(target) > IntentCandidateMaxCaptures {
				return 0, false, fmt.Errorf(
					"state: forward recovery dependency component exceeds %d captures",
					IntentCandidateMaxCaptures)
			}
			queue = append(queue, neighbor)
		}
	}
	recovery.TargetEventSeqs = make([]int64, 0, len(target))
	for seq := range target {
		recovery.TargetEventSeqs = append(recovery.TargetEventSeqs, seq)
	}
	sort.Slice(recovery.TargetEventSeqs, func(i, j int) bool {
		return recovery.TargetEventSeqs[i] < recovery.TargetEventSeqs[j]
	})

	if _, err := tx.ExecContext(ctx, `
UPDATE intent_candidate_events
SET membership_state='superseded'
WHERE candidate_id=? AND membership_state='active'`,
		recovery.CandidateID); err != nil {
		return 0, false, fmt.Errorf(
			"state: release forward recovery membership: %w", err)
	}
	now := nowSeconds()
	res, err := tx.ExecContext(ctx, `
UPDATE intent_candidates
SET status='superseded',readiness='wait',soft_publication_deadline=NULL,
    updated_ts=?
WHERE id=? AND branch_ref=? AND branch_generation=?
  AND status IN ('open','waiting','ready','soft_published','blocked')`,
		now, recovery.CandidateID, recovery.BranchRef,
		recovery.BranchGeneration)
	if err != nil {
		return 0, false, fmt.Errorf("state: retire forward recovery candidate: %w", err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if changed != 1 {
		return 0, false, nil
	}

	for _, member := range pending {
		if _, err := appendDecision(ctx, tx, DecisionRecord{
			DecisionTS:       now,
			Kind:             DecisionKindIntentForwardRecovery,
			Path:             sql.NullString{String: member.path, Valid: member.path != ""},
			Reason:           sql.NullString{String: recovery.Reason, Valid: true},
			EventSeq:         sql.NullInt64{Int64: member.seq, Valid: true},
			BranchRef:        sql.NullString{String: recovery.BranchRef, Valid: true},
			BranchGeneration: sql.NullInt64{Int64: recovery.BranchGeneration, Valid: true},
			ActionTaken: sql.NullString{
				String: "retired_candidate_for_forward_recovery", Valid: true,
			},
			UserMessage: sql.NullString{
				String: "ACD kept the published commits and will commit the remaining changes forward.",
				Valid:  true,
			},
			ConfigRevisionID: revision,
			ConfigProfile:    profile,
		}); err != nil {
			return 0, false, err
		}
	}
	if recovery.Stage == "" {
		recovery.Stage = "semantic_replan"
	}
	raw, err := json.Marshal(recovery)
	if err != nil {
		return 0, false, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts`,
		intentForwardRecoveryMetaKey, string(raw), now); err != nil {
		return 0, false, fmt.Errorf("state: persist intent forward recovery: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("state: commit intent forward recovery: %w", err)
	}
	return len(pending), true, nil
}

// IntentForwardRecoveryForPair returns a matching durable recovery marker.
func IntentForwardRecoveryForPair(
	ctx context.Context,
	d *DB,
	branchRef string,
	generation int64,
) (IntentForwardRecovery, bool, error) {
	var recovery IntentForwardRecovery
	if d == nil || branchRef == "" || generation < 0 {
		return recovery, false, errors.New(
			"state: IntentForwardRecoveryForPair: invalid input")
	}
	var raw string
	err := d.readSQL().QueryRowContext(ctx,
		`SELECT value FROM daemon_meta WHERE key=?`,
		intentForwardRecoveryMetaKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return recovery, false, nil
	}
	if err != nil {
		return recovery, false, err
	}
	if err := json.Unmarshal([]byte(raw), &recovery); err != nil {
		return recovery, false, fmt.Errorf(
			"state: parse intent forward recovery: %w", err)
	}
	if recovery.BranchRef != branchRef || recovery.BranchGeneration != generation {
		return IntentForwardRecovery{}, false, nil
	}
	if recovery.Stage == "" {
		recovery.Stage = "semantic_replan"
	}
	return recovery, true, nil
}

// CompleteAnyResolvedIntentForwardRecovery retires the one durable forward
// recovery marker when its exact frozen target has already been settled. The
// marker is repository-wide, so a completed recovery from an older branch
// generation must be cleared before a newer generation can start its own
// bounded recovery.
func CompleteAnyResolvedIntentForwardRecovery(
	ctx context.Context,
	d *DB,
) (IntentForwardRecovery, bool, error) {
	var recovery IntentForwardRecovery
	if d == nil {
		return recovery, false, errors.New(
			"state: CompleteAnyResolvedIntentForwardRecovery: state db is required")
	}
	var raw string
	err := d.readSQL().QueryRowContext(ctx,
		`SELECT value FROM daemon_meta WHERE key=?`,
		intentForwardRecoveryMetaKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return recovery, false, nil
	}
	if err != nil {
		return recovery, false, fmt.Errorf(
			"state: load any resolved intent forward recovery: %w", err)
	}
	if err := json.Unmarshal([]byte(raw), &recovery); err != nil {
		return IntentForwardRecovery{}, false, fmt.Errorf(
			"state: parse any resolved intent forward recovery: %w", err)
	}
	if recovery.Stage == "" {
		recovery.Stage = "semantic_replan"
	}
	// Legacy forward-recovery markers predate the frozen target list. They
	// remain usable by the pair-specific recovery path, but cannot be proved
	// complete from durable event membership and therefore must not be guessed
	// away here.
	if len(recovery.TargetEventSeqs) == 0 {
		return recovery, false, nil
	}
	completed, err := CompleteResolvedIntentForwardRecovery(ctx, d, recovery)
	if err != nil {
		return recovery, false, err
	}
	return recovery, completed, nil
}

// CompleteResolvedIntentForwardRecovery clears a loaded recovery marker only
// after its frozen target is still exact and every member is durably resolved.
// It makes a crash after the last publication but before ordinary marker
// completion restart-safe without guessing from an empty pending queue.
func CompleteResolvedIntentForwardRecovery(
	ctx context.Context,
	d *DB,
	recovery IntentForwardRecovery,
) (bool, error) {
	if d == nil || strings.TrimSpace(recovery.BranchRef) == "" ||
		recovery.BranchGeneration < 0 ||
		strings.TrimSpace(recovery.CandidateID) == "" ||
		strings.TrimSpace(recovery.Reason) == "" ||
		len(recovery.TargetEventSeqs) == 0 ||
		len(recovery.TargetEventSeqs) > IntentCandidateMaxCaptures {
		return false, errors.New(
			"state: CompleteResolvedIntentForwardRecovery: invalid input")
	}
	seen := make(map[int64]struct{}, len(recovery.TargetEventSeqs))
	for _, seq := range recovery.TargetEventSeqs {
		if seq <= 0 {
			return false, errors.New(
				"state: CompleteResolvedIntentForwardRecovery: invalid target")
		}
		if _, duplicate := seen[seq]; duplicate {
			return false, errors.New(
				"state: CompleteResolvedIntentForwardRecovery: duplicate target")
		}
		seen[seq] = struct{}{}
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf(
			"state: begin resolved intent forward recovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var markerRaw string
	err = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key=?`,
		intentForwardRecoveryMetaKey).Scan(&markerRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf(
			"state: load resolved intent forward recovery: %w", err)
	}
	var current IntentForwardRecovery
	if err := json.Unmarshal([]byte(markerRaw), &current); err != nil {
		return false, fmt.Errorf(
			"state: parse resolved intent forward recovery: %w", err)
	}
	if current.Stage == "" {
		current.Stage = "semantic_replan"
	}
	if current.BranchRef != recovery.BranchRef ||
		current.BranchGeneration != recovery.BranchGeneration ||
		current.CandidateID != recovery.CandidateID ||
		current.Reason != recovery.Reason ||
		!sameEventSeqs(current.TargetEventSeqs, recovery.TargetEventSeqs) {
		return false, errors.New(
			"state: intent forward recovery marker changed")
	}

	placeholders := strings.TrimSuffix(
		strings.Repeat("?,", len(current.TargetEventSeqs)), ",")
	args := make([]any, 0, len(current.TargetEventSeqs)+2)
	for _, seq := range current.TargetEventSeqs {
		args = append(args, seq)
	}
	args = append(args, current.BranchRef, current.BranchGeneration)
	var existing, resolved int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN state IN ('published','recovered')
                         THEN 1 ELSE 0 END),0)
FROM capture_events
WHERE seq IN (`+placeholders+`)
  AND branch_ref=? AND branch_generation=?`, args...).Scan(
		&existing, &resolved); err != nil {
		return false, fmt.Errorf(
			"state: prove resolved intent forward recovery target: %w", err)
	}
	if existing != len(current.TargetEventSeqs) || resolved != existing {
		return false, nil
	}

	result, err := tx.ExecContext(ctx,
		`DELETE FROM daemon_meta WHERE key=?`, intentForwardRecoveryMetaKey)
	if err != nil {
		return false, fmt.Errorf(
			"state: clear resolved intent forward recovery: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if changed != 1 {
		return false, errors.New(
			"state: intent forward recovery marker changed")
	}
	now := nowSeconds()
	for key, value := range map[string]string{
		"intent.v2.last_fallback_mode":   current.Stage,
		"intent.v2.last_fallback_size":   strconv.Itoa(len(current.TargetEventSeqs)),
		"intent.v2.last_fallback_reason": current.Reason,
	} {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts`,
			key, value, now); err != nil {
			return false, fmt.Errorf(
				"state: record resolved intent forward recovery: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf(
			"state: commit resolved intent forward recovery: %w", err)
	}
	return true, nil
}

func sameEventSeqs(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func normalizedIntentForwardRecoveryStage(stage string) string {
	if stage == "" {
		return "semantic_replan"
	}
	return stage
}

func sameIntentForwardRecoveryMarker(
	left IntentForwardRecovery,
	right IntentForwardRecovery,
) bool {
	return left.BranchRef == right.BranchRef &&
		left.BranchGeneration == right.BranchGeneration &&
		left.CandidateID == right.CandidateID &&
		left.Reason == right.Reason &&
		normalizedIntentForwardRecoveryStage(left.Stage) ==
			normalizedIntentForwardRecoveryStage(right.Stage) &&
		sameEventSeqs(left.TargetEventSeqs, right.TargetEventSeqs) &&
		left.UnlockCount == right.UnlockCount &&
		left.LastProgressTS == right.LastProgressTS &&
		left.PlanFingerprint == right.PlanFingerprint &&
		left.PrefixCursor == right.PrefixCursor &&
		left.PrefixUnresolvedCount == right.PrefixUnresolvedCount &&
		left.PrefixBaseHead == right.PrefixBaseHead &&
		left.PrefixExhausted == right.PrefixExhausted &&
		left.NeedsAttention == right.NeedsAttention &&
		left.AttentionReason == right.AttentionReason
}

func loadIntentForwardRecoveryForUpdate(
	ctx context.Context,
	tx *sql.Tx,
) (IntentForwardRecovery, string, error) {
	var raw string
	if err := tx.QueryRowContext(ctx,
		`SELECT value FROM daemon_meta WHERE key=?`,
		intentForwardRecoveryMetaKey).Scan(&raw); err != nil {
		return IntentForwardRecovery{}, "", err
	}
	var current IntentForwardRecovery
	if err := json.Unmarshal([]byte(raw), &current); err != nil {
		return IntentForwardRecovery{}, "", err
	}
	current.Stage = normalizedIntentForwardRecoveryStage(current.Stage)
	return current, raw, nil
}

func storeIntentForwardRecoveryCAS(
	ctx context.Context,
	tx *sql.Tx,
	previousRaw string,
	recovery IntentForwardRecovery,
) error {
	updated, err := json.Marshal(recovery)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE daemon_meta SET value=?,updated_ts=? WHERE key=? AND value=?`,
		string(updated), nowSeconds(), intentForwardRecoveryMetaKey, previousRaw)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("state: intent forward recovery marker changed")
	}
	return nil
}

// AdvanceIntentForwardRecovery changes the restart-safe salvage subphase
// without changing its frozen target or branch identity.
func AdvanceIntentForwardRecovery(
	ctx context.Context,
	d *DB,
	recovery IntentForwardRecovery,
	stage string,
	published int,
) (IntentForwardRecovery, error) {
	if d == nil || recovery.BranchRef == "" || recovery.BranchGeneration < 0 ||
		(stage != "semantic_replan" && stage != "local_unlock") || published < 0 {
		return recovery, errors.New("state: AdvanceIntentForwardRecovery: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return recovery, err
	}
	defer func() { _ = tx.Rollback() }()
	current, raw, err := loadIntentForwardRecoveryForUpdate(ctx, tx)
	if err != nil {
		return recovery, err
	}
	if !sameIntentForwardRecoveryMarker(current, recovery) {
		return recovery, errors.New("state: intent forward recovery marker changed")
	}
	if published == 0 && (current.PrefixExhausted || current.NeedsAttention) &&
		stage != current.Stage {
		return recovery, errors.New(
			"state: exhausted intent forward recovery requires attention")
	}
	current.Stage = stage
	if published > 0 {
		current.UnlockCount++
		current.LastProgressTS = nowSeconds()
		current.PlanFingerprint = ""
		current.PrefixCursor = 0
		current.PrefixBaseHead = ""
		current.PrefixExhausted = false
		current.NeedsAttention = false
		current.AttentionReason = ""
	}
	if err := storeIntentForwardRecoveryCAS(ctx, tx, raw, current); err != nil {
		return recovery, err
	}
	if err := tx.Commit(); err != nil {
		return recovery, err
	}
	return current, nil
}

// AdvanceIntentForwardRecoveryPrefix starts or widens a semantic-prefix
// local-unlock attempt. PrefixCursor is the next number of ordered plan
// candidates to attempt. The plan and base HEAD stay immutable until durable
// publication progress resets them through AdvanceIntentForwardRecovery.
func AdvanceIntentForwardRecoveryPrefix(
	ctx context.Context,
	d *DB,
	recovery IntentForwardRecovery,
	planFingerprint string,
	baseHead string,
	prefixCursor int,
) (IntentForwardRecovery, error) {
	planLabelErr := boundedIntentLabel(
		"intent recovery plan fingerprint", planFingerprint, 128, true)
	baseLabelErr := boundedIntentLabel(
		"intent recovery prefix base HEAD", baseHead, 128, true)
	if d == nil || recovery.BranchRef == "" || recovery.BranchGeneration < 0 ||
		recovery.CandidateID == "" ||
		(normalizedIntentForwardRecoveryStage(recovery.Stage) != "semantic_replan" &&
			normalizedIntentForwardRecoveryStage(recovery.Stage) != "local_unlock") ||
		planLabelErr != nil || baseLabelErr != nil ||
		planFingerprint != strings.TrimSpace(planFingerprint) ||
		baseHead != strings.TrimSpace(baseHead) ||
		prefixCursor < 1 || prefixCursor > IntentCandidateMaxOpenPerPair ||
		len(recovery.TargetEventSeqs) == 0 {
		return recovery, errors.New(
			"state: AdvanceIntentForwardRecoveryPrefix: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return recovery, err
	}
	defer func() { _ = tx.Rollback() }()
	current, raw, err := loadIntentForwardRecoveryForUpdate(ctx, tx)
	if err != nil {
		return recovery, err
	}
	if !sameIntentForwardRecoveryMarker(current, recovery) {
		return recovery, errors.New(
			"state: intent forward recovery marker changed")
	}
	if current.PrefixExhausted || current.NeedsAttention {
		return recovery, errors.New(
			"state: intent forward recovery prefix is exhausted")
	}
	if current.PlanFingerprint == "" {
		if current.PrefixCursor != 0 || current.PrefixBaseHead != "" {
			return recovery, errors.New(
				"state: intent forward recovery prefix state is incomplete")
		}
		current.PlanFingerprint = planFingerprint
		current.PrefixBaseHead = baseHead
	} else {
		if current.PlanFingerprint != planFingerprint ||
			current.PrefixBaseHead != baseHead {
			return recovery, errors.New(
				"state: intent forward recovery plan changed before progress")
		}
		if prefixCursor < current.PrefixCursor {
			return recovery, errors.New(
				"state: intent forward recovery prefix cannot move backward")
		}
	}
	current.Stage = "local_unlock"
	current.PrefixCursor = prefixCursor
	if err := storeIntentForwardRecoveryCAS(ctx, tx, raw, current); err != nil {
		return recovery, err
	}
	if err := tx.Commit(); err != nil {
		return recovery, err
	}
	return current, nil
}

// MarkIntentForwardRecoveryNeedsAttention durably stops an exhausted semantic
// prefix at the required verification gate. It never changes the frozen target
// or permits an unverified publication.
func MarkIntentForwardRecoveryNeedsAttention(
	ctx context.Context,
	d *DB,
	recovery IntentForwardRecovery,
	reason string,
) (IntentForwardRecovery, error) {
	reason = strings.TrimSpace(reason)
	reasonErr := boundedIntentSummary(
		"intent recovery attention reason", reason,
		IntentCandidateSummaryMaxChars)
	if d == nil || recovery.BranchRef == "" || recovery.BranchGeneration < 0 ||
		recovery.CandidateID == "" || recovery.Stage != "local_unlock" ||
		recovery.PlanFingerprint == "" || recovery.PrefixCursor < 1 ||
		recovery.PrefixBaseHead == "" || reason == "" || reasonErr != nil {
		return recovery, errors.New(
			"state: MarkIntentForwardRecoveryNeedsAttention: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return recovery, err
	}
	defer func() { _ = tx.Rollback() }()
	current, raw, err := loadIntentForwardRecoveryForUpdate(ctx, tx)
	if err != nil {
		return recovery, err
	}
	if !sameIntentForwardRecoveryMarker(current, recovery) {
		return recovery, errors.New(
			"state: intent forward recovery marker changed")
	}
	if current.NeedsAttention {
		if current.PrefixExhausted && current.AttentionReason == reason {
			if err := tx.Commit(); err != nil {
				return recovery, err
			}
			return current, nil
		}
		return recovery, errors.New(
			"state: intent forward recovery attention reason changed")
	}
	current.PrefixExhausted = true
	current.NeedsAttention = true
	current.AttentionReason = reason
	if err := storeIntentForwardRecoveryCAS(ctx, tx, raw, current); err != nil {
		return recovery, err
	}
	if err := tx.Commit(); err != nil {
		return recovery, err
	}
	return current, nil
}

// CompleteIntentForwardRecovery clears the marker only after the frozen target
// is fully resolved. The published count remains an API guard for callers that
// observed progress; durable event state is the completion proof.
func CompleteIntentForwardRecovery(
	ctx context.Context,
	d *DB,
	recovery IntentForwardRecovery,
	published int,
) error {
	if d == nil || recovery.BranchRef == "" || recovery.BranchGeneration < 0 ||
		published <= 0 {
		return errors.New("state: CompleteIntentForwardRecovery: invalid input")
	}
	completed, err := CompleteResolvedIntentForwardRecovery(ctx, d, recovery)
	if err != nil {
		return err
	}
	if !completed {
		return errors.New(
			"state: intent forward recovery target remains unresolved")
	}
	return nil
}

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

// IntentForwardRecovery is the durable forward-only salvage state for a
// candidate whose published history can no longer be repaired safely.
type IntentForwardRecovery struct {
	BranchRef        string  `json:"branch_ref"`
	BranchGeneration int64   `json:"branch_generation"`
	CandidateID      string  `json:"candidate_id"`
	Reason           string  `json:"reason"`
	Stage            string  `json:"stage,omitempty"`
	TargetEventSeqs  []int64 `json:"target_event_seqs,omitempty"`
	UnlockCount      int     `json:"unlock_count,omitempty"`
	LastProgressTS   float64 `json:"last_progress_ts,omitempty"`
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
	var raw string
	if err := tx.QueryRowContext(ctx,
		`SELECT value FROM daemon_meta WHERE key=?`,
		intentForwardRecoveryMetaKey).Scan(&raw); err != nil {
		return recovery, err
	}
	var current IntentForwardRecovery
	if err := json.Unmarshal([]byte(raw), &current); err != nil {
		return recovery, err
	}
	if current.BranchRef != recovery.BranchRef ||
		current.BranchGeneration != recovery.BranchGeneration ||
		current.CandidateID != recovery.CandidateID {
		return recovery, errors.New("state: intent forward recovery marker changed")
	}
	current.Stage = stage
	if published > 0 {
		current.UnlockCount++
		current.LastProgressTS = nowSeconds()
	}
	updated, err := json.Marshal(current)
	if err != nil {
		return recovery, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE daemon_meta SET value=?,updated_ts=? WHERE key=?`,
		string(updated), nowSeconds(), intentForwardRecoveryMetaKey); err != nil {
		return recovery, err
	}
	if err := tx.Commit(); err != nil {
		return recovery, err
	}
	return current, nil
}

// CompleteIntentForwardRecovery clears the marker and records the successful
// recovery mode for read-only diagnostics.
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
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var raw string
	if err := tx.QueryRowContext(ctx,
		`SELECT value FROM daemon_meta WHERE key=?`,
		intentForwardRecoveryMetaKey).Scan(&raw); err != nil {
		return err
	}
	var current IntentForwardRecovery
	if err := json.Unmarshal([]byte(raw), &current); err != nil {
		return err
	}
	if current.BranchRef != recovery.BranchRef ||
		current.BranchGeneration != recovery.BranchGeneration ||
		current.CandidateID != recovery.CandidateID {
		return errors.New("state: intent forward recovery marker changed")
	}
	now := nowSeconds()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM daemon_meta WHERE key=?`, intentForwardRecoveryMetaKey); err != nil {
		return err
	}
	mode := recovery.Stage
	if mode == "" {
		mode = "semantic_replan"
	}
	for key, value := range map[string]string{
		"intent.v2.last_fallback_mode":   mode,
		"intent.v2.last_fallback_size":   strconv.Itoa(published),
		"intent.v2.last_fallback_reason": recovery.Reason,
	} {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts`,
			key, value, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

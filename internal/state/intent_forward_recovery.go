package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const intentForwardRecoveryMetaKey = "intent.v2.forward_recovery"

// IntentForwardRecovery is the durable one-shot local fallback requested by
// a candidate whose published history can no longer be repaired safely.
type IntentForwardRecovery struct {
	BranchRef        string `json:"branch_ref"`
	BranchGeneration int64  `json:"branch_generation"`
	CandidateID      string `json:"candidate_id"`
	Reason           string `json:"reason"`
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
	return recovery, true, nil
}

// CompleteIntentForwardRecovery clears the one-shot marker and records the
// last successful local fallback for read-only diagnostics.
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
	for key, value := range map[string]string{
		"intent.v2.last_fallback_mode":   "forward_atomic_components",
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

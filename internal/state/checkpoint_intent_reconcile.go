package state

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const CheckpointIntentReconciledMetaKey = "checkpoint.intent_membership_reconciled"

// CheckpointIntentReconcileResult describes the bounded v20 repair that
// disconnects legacy planning state from checkpoint-backed publication.
type CheckpointIntentReconcileResult struct {
	OperationID       string
	RetiredCandidates []string
}

// ReconcileCheckpointIntentMemberships retires non-terminal candidates whose
// active membership is no longer entirely pending and checkpoint-backed. It
// never changes capture events, terminal candidate history, or refs.
func ReconcileCheckpointIntentMemberships(
	ctx context.Context,
	db *DB,
	worktreeID string,
) (CheckpointIntentReconcileResult, error) {
	if db == nil || strings.TrimSpace(worktreeID) == "" {
		return CheckpointIntentReconcileResult{}, errors.New("state: checkpoint intent reconciliation requires a worktree")
	}

	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return CheckpointIntentReconcileResult{}, fmt.Errorf("state: begin checkpoint intent reconciliation: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
SELECT c.id
FROM intent_candidates c
WHERE c.status IN ('open','waiting','ready','blocked','failed')
  AND (
    NOT EXISTS (
      SELECT 1 FROM intent_candidate_events m
      WHERE m.candidate_id=c.id AND m.membership_state='active'
    )
    OR EXISTS (
      SELECT 1
      FROM intent_candidate_events m
      LEFT JOIN capture_events e ON e.seq=m.event_seq
      WHERE m.candidate_id=c.id
        AND m.membership_state='active'
        AND (e.seq IS NULL OR e.state<>'pending')
    )
    OR EXISTS (
      SELECT 1
      FROM intent_candidate_events m
      WHERE m.candidate_id=c.id
        AND m.membership_state='active'
        AND NOT EXISTS (
          SELECT 1
          FROM checkpoint_events ce
          JOIN checkpoints cp ON cp.id=ce.checkpoint_id
          WHERE ce.event_seq=m.event_seq AND cp.phase='completed'
        )
    )
  )
ORDER BY c.id`)
	if err != nil {
		return CheckpointIntentReconcileResult{}, fmt.Errorf("state: inspect checkpoint intent membership: %w", err)
	}
	var candidateIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return CheckpointIntentReconcileResult{}, fmt.Errorf("state: scan checkpoint intent candidate: %w", err)
		}
		candidateIDs = append(candidateIDs, id)
	}
	if err := rows.Close(); err != nil {
		return CheckpointIntentReconcileResult{}, err
	}
	if err := rows.Err(); err != nil {
		return CheckpointIntentReconcileResult{}, err
	}

	result := CheckpointIntentReconcileResult{RetiredCandidates: candidateIDs}
	if len(candidateIDs) > 0 {
		operationID, err := checkpointIntentOperationID()
		if err != nil {
			return CheckpointIntentReconcileResult{}, err
		}
		result.OperationID = operationID
		planDigest := checkpointIntentPlanDigest(worktreeID, candidateIDs)
		now := float64(time.Now().UnixNano()) / float64(time.Second)
		if _, err := tx.ExecContext(ctx, `
INSERT INTO operations(id,kind,worktree_id,phase,status,plan_digest,error,created_ts,updated_ts)
VALUES(?, 'intent_membership_repair', ?, 'applying', 'active', ?, '', ?, ?)`,
			operationID, worktreeID, planDigest, now, now); err != nil {
			return CheckpointIntentReconcileResult{}, fmt.Errorf("state: journal checkpoint intent repair: %w", err)
		}
		for index, id := range candidateIDs {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO operation_steps(operation_id,ord,kind,target,before_digest,after_digest,proof_id,phase,completed_ts)
VALUES(?, ?, 'retire_candidate', ?, 'active', 'superseded', ?, 'completed', ?)`,
				operationID, index+1, id, "checkpoint-membership-v1", now); err != nil {
				return CheckpointIntentReconcileResult{}, fmt.Errorf("state: journal candidate %s repair: %w", id, err)
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE intent_candidate_events
SET membership_state='superseded'
WHERE candidate_id=? AND membership_state='active'`, id); err != nil {
				return CheckpointIntentReconcileResult{}, fmt.Errorf("state: supersede candidate %s membership: %w", id, err)
			}
			res, err := tx.ExecContext(ctx, `
UPDATE intent_candidates
SET status='superseded', readiness='wait', updated_ts=?
WHERE id=? AND status IN ('open','waiting','ready','blocked','failed')`, now, id)
			if err != nil {
				return CheckpointIntentReconcileResult{}, fmt.Errorf("state: retire candidate %s: %w", id, err)
			}
			if affected, _ := res.RowsAffected(); affected != 1 {
				return CheckpointIntentReconcileResult{}, sql.ErrNoRows
			}
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE operations SET phase='completed',status='completed',updated_ts=?,completed_ts=? WHERE id=?`, now, now, operationID); err != nil {
			return CheckpointIntentReconcileResult{}, fmt.Errorf("state: complete checkpoint intent repair: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts`,
		CheckpointIntentReconciledMetaKey, "v1", float64(time.Now().UnixNano())/float64(time.Second)); err != nil {
		return CheckpointIntentReconcileResult{}, fmt.Errorf("state: mark checkpoint intent reconciliation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return CheckpointIntentReconcileResult{}, fmt.Errorf("state: commit checkpoint intent reconciliation: %w", err)
	}
	return result, nil
}

func checkpointIntentOperationID() (string, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("state: create checkpoint intent operation id: %w", err)
	}
	return fmt.Sprintf("intent-repair-%d-%s", time.Now().UnixMilli(), hex.EncodeToString(random[:])), nil
}

func checkpointIntentPlanDigest(worktreeID string, candidateIDs []string) string {
	ids := append([]string(nil), candidateIDs...)
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(worktreeID + "\x00" + strings.Join(ids, "\x00")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

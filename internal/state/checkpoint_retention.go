package state

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type RetentionCheckpoint struct {
	ID, WorktreeID, Reason, Ref, CommitOID string
	Sequence                               int64
	CreatedTS                              float64
	Retained, Published, Unresolved        bool
}

func RetentionCheckpoints(ctx context.Context, db *DB, worktreeID string) ([]RetentionCheckpoint, error) {
	if db == nil {
		return nil, errors.New("state: retention requires a database")
	}
	rows, err := db.readSQL().QueryContext(ctx, `
SELECT cp.id,cp.worktree_id,cp.reason,cp.checkpoint_ref,cp.commit_oid,
       cp.seq,cp.created_ts,cp.retained,
       EXISTS (
         SELECT 1 FROM checkpoint_events ce
         WHERE ce.checkpoint_id=cp.id
       ) AND NOT EXISTS (
         SELECT 1 FROM checkpoint_events ce JOIN capture_events e ON e.seq=ce.event_seq
         WHERE ce.checkpoint_id=cp.id AND e.state<>'published'
       ) AS published,
       EXISTS (
         SELECT 1 FROM operations o
         WHERE o.id=cp.operation_id AND o.status IN ('prepared','applying','active','needs_action')
       ) OR EXISTS (
         SELECT 1 FROM restore_operations r
         WHERE r.phase NOT IN ('completed','rolled_back')
           AND cp.id IN (r.target_checkpoint_id,
                         r.pre_restore_checkpoint_id,
                         r.post_restore_checkpoint_id)
       ) AS unresolved
FROM checkpoints cp
WHERE cp.worktree_id=? AND cp.phase='completed'
ORDER BY cp.seq DESC`, worktreeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var checkpoints []RetentionCheckpoint
	for rows.Next() {
		var item RetentionCheckpoint
		if err := rows.Scan(&item.ID, &item.WorktreeID, &item.Reason, &item.Ref,
			&item.CommitOID, &item.Sequence, &item.CreatedTS, &item.Retained,
			&item.Published, &item.Unresolved); err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, item)
	}
	return checkpoints, rows.Err()
}

func PrepareCheckpointPrune(ctx context.Context, db *DB, checkpoint RetentionCheckpoint, planDigest string) (string, error) {
	operationID := "gc-" + checkpoint.ID
	err := PrepareOperation(ctx, db, Operation{ID: operationID, Kind: "checkpoint_gc",
		WorktreeID: checkpoint.WorktreeID, PlanDigest: planDigest}, []OperationStep{{
		Kind: "delete_checkpoint_ref", Target: checkpoint.Ref,
		BeforeDigest: checkpoint.CommitOID, ProofID: checkpoint.ID,
	}})
	if err != nil {
		var count int
		queryErr := db.readSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM operations WHERE id=? AND status='prepared'`, operationID).Scan(&count)
		if queryErr == nil && count == 1 {
			return operationID, nil
		}
		return "", err
	}
	return operationID, nil
}

func CompleteCheckpointPrune(ctx context.Context, db *DB, operationID, checkpointID, ref, expectedCommit string) error {
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE checkpoints SET retained=0,pruned_ts=?
WHERE id=? AND checkpoint_ref=? AND commit_oid=? AND retained=1`, now, checkpointID, ref, expectedCommit)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		var retained bool
		if err := tx.QueryRowContext(ctx, `SELECT retained FROM checkpoints WHERE id=?`, checkpointID).Scan(&retained); err != nil || retained {
			return fmt.Errorf("state: checkpoint prune identity changed")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operation_steps SET phase='completed',completed_ts=? WHERE operation_id=? AND ord=1`, now, operationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET phase='completed',status='completed',updated_ts=?,completed_ts=? WHERE id=? AND status='prepared'`, now, now, operationID); err != nil {
		return err
	}
	return tx.Commit()
}

type PreparedCheckpointPrune struct {
	OperationID, CheckpointID, Ref, CommitOID string
}

func PreparedCheckpointPrunes(ctx context.Context, db *DB) ([]PreparedCheckpointPrune, error) {
	rows, err := db.readSQL().QueryContext(ctx, `
SELECT o.id,s.proof_id,s.target,s.before_digest
FROM operations o JOIN operation_steps s ON s.operation_id=o.id
WHERE o.kind='checkpoint_gc' AND o.status='prepared' AND s.ord=1
ORDER BY o.created_ts,o.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []PreparedCheckpointPrune
	for rows.Next() {
		var item PreparedCheckpointPrune
		if err := rows.Scan(&item.OperationID, &item.CheckpointID, &item.Ref, &item.CommitOID); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

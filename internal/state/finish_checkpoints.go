package state

import (
	"context"
	"database/sql"
)

const finishCheckpointBatchSize = 256

// WalkFinishCheckpointBatches reads only current protection snapshots in the same
// uninterrupted observed branch/HEAD window. Legacy captures and restore or
// migration snapshots never become a new finish's planning source.
func WalkFinishCheckpointBatches(ctx context.Context, db *DB, target Checkpoint, visit func([]Checkpoint, bool) error) error {
	tx, err := db.ReadSQL().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Use one read snapshot so retention cannot silently change which rows
	// belong to later batches while the caller verifies the earlier ones.
	var after int64
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM checkpoints boundary WHERE worktree_id=? AND seq<? AND
 (observed_ref<>? OR observed_head<>? OR reason NOT IN ('watch','poll','hint','manual_barrier')
 OR EXISTS(SELECT 1 FROM checkpoint_events ce WHERE ce.checkpoint_id=boundary.id))`,
		target.WorktreeID, target.Seq, target.ObservedRef, target.ObservedHead).Scan(&after)
	if err != nil {
		return err
	}
	for {
		batch, err := finishCheckpointBatch(ctx, tx, target, after)
		if err != nil {
			return err
		}
		last := len(batch) < finishCheckpointBatchSize
		if err := visit(batch, last); err != nil {
			return err
		}
		if last {
			return nil
		}
		after = batch[len(batch)-1].Seq
	}
}

func finishCheckpointBatch(ctx context.Context, tx *sql.Tx, target Checkpoint, after int64) ([]Checkpoint, error) {
	rows, err := tx.QueryContext(ctx, checkpointSelect+` WHERE worktree_id=? AND seq>? AND seq<?
 AND phase='completed' AND retained=1 AND coverage_complete=1 ORDER BY seq LIMIT ?`,
		target.WorktreeID, after, target.Seq, finishCheckpointBatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Checkpoint
	for rows.Next() {
		cp, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, cp)
	}
	return result, rows.Err()
}

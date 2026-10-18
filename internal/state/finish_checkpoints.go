package state

import (
	"context"
	"fmt"
)

// FinishCheckpoints reads only current protection snapshots in the same
// uninterrupted observed branch/HEAD window. Legacy captures and restore or
// migration snapshots never become a new finish's planning source.
func FinishCheckpoints(ctx context.Context, db *DB, target Checkpoint) ([]Checkpoint, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, checkpointSelect+` WHERE worktree_id=? AND seq<?
 AND seq>COALESCE((SELECT MAX(seq) FROM checkpoints boundary WHERE worktree_id=? AND seq<? AND
 (observed_ref<>? OR observed_head<>? OR reason NOT IN ('watch','poll','hint','manual_barrier')
 OR EXISTS(SELECT 1 FROM checkpoint_events ce WHERE ce.checkpoint_id=boundary.id))),0)
 AND observed_ref=? AND observed_head=? AND phase='completed' AND retained=1 AND coverage_complete=1
 AND reason IN ('watch','poll','hint','manual_barrier')
 AND NOT EXISTS(SELECT 1 FROM checkpoint_events ce WHERE ce.checkpoint_id=checkpoints.id)
 ORDER BY seq LIMIT ?`, target.WorktreeID, target.Seq, target.WorktreeID, target.Seq,
		target.ObservedRef, target.ObservedHead, target.ObservedRef, target.ObservedHead, FinishCheckpointLimit+1)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) > FinishCheckpointLimit {
		return nil, fmt.Errorf("finish: more than %d current checkpoints; recorded planning evidence is too large; choose Local for a complete offline batch", FinishCheckpointLimit)
	}
	return result, nil
}

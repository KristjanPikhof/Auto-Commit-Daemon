package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	OperationPrepared       = "prepared"
	OperationApplying       = "applying"
	OperationActive         = "active"
	OperationCompleted      = "completed"
	OperationRolledBack     = "rolled_back"
	OperationNeedsAttention = "needs_action"
)

type Operation struct {
	ID         string
	Kind       string
	WorktreeID string
	Phase      string
	Status     string
	PlanDigest string
	Error      string
	CreatedTS  float64
	UpdatedTS  float64
}

type OperationStep struct {
	OperationID  string
	Sequence     int
	Kind         string
	Target       string
	BeforeDigest string
	AfterDigest  string
	ProofID      string
	Phase        string
}

// BeginRestoreApply records the durable pre-restore checkpoint and makes the
// restore repairable before the first working-tree mutation.
func BeginRestoreApply(ctx context.Context, db *DB, operationID, preCheckpointID string) error {
	if db == nil || operationID == "" || preCheckpointID == "" {
		return errors.New("state: incomplete restore apply transition")
	}
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return fmt.Errorf("state: enable full restore durability: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "PRAGMA synchronous=NORMAL") }()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE restore_operations
SET pre_restore_checkpoint_id=?,phase=?,updated_ts=?
WHERE operation_id=? AND phase=?`, preCheckpointID, OperationApplying, now,
		operationID, OperationPrepared)
	if err != nil {
		return fmt.Errorf("state: begin restore apply: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	result, err = tx.ExecContext(ctx, `UPDATE operations SET phase=?,status=?,error=?,updated_ts=?
WHERE id=? AND status=?`, OperationApplying, OperationActive, "", now,
		operationID, OperationPrepared)
	if err != nil {
		return fmt.Errorf("state: activate restore operation: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return tx.Commit()
}

// FinishRestoreOperation moves the specialized and general journals together
// so recovery never observes a completed restore paired with an active parent.
func FinishRestoreOperation(
	ctx context.Context,
	db *DB,
	operationID, preCheckpointID, postCheckpointID, phase, status, sanitizedError string,
) error {
	if db == nil || operationID == "" || phase == "" || status == "" {
		return errors.New("state: incomplete restore finish transition")
	}
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE restore_operations
SET pre_restore_checkpoint_id=COALESCE(NULLIF(?,''),pre_restore_checkpoint_id),
    post_restore_checkpoint_id=COALESCE(NULLIF(?,''),post_restore_checkpoint_id),
    phase=?,updated_ts=? WHERE operation_id=?`, preCheckpointID, postCheckpointID,
		phase, now, operationID)
	if err != nil {
		return fmt.Errorf("state: finish restore operation: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	result, err = tx.ExecContext(ctx, `UPDATE operations
SET phase=?,status=?,error=?,updated_ts=?,completed_ts=CASE WHEN ? IN (?,?) THEN ? ELSE completed_ts END
WHERE id=?`, phase, status, sanitizedError, now, status,
		OperationCompleted, OperationRolledBack, now, operationID)
	if err != nil {
		return fmt.Errorf("state: finish parent operation: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return tx.Commit()
}

type RestoreOperation struct {
	OperationID        string
	WorktreeID         string
	TargetCheckpointID string
	PreCheckpointID    sql.NullString
	PostCheckpointID   sql.NullString
	PlanDigest         string
	Phase              string
	OperationStatus    string
	OperationCreatedTS float64
}

func PrepareOperation(ctx context.Context, db *DB, operation Operation, steps []OperationStep) error {
	if db == nil || operation.ID == "" || operation.Kind == "" || operation.WorktreeID == "" || operation.PlanDigest == "" {
		return errors.New("state: incomplete operation")
	}
	now := operation.CreatedTS
	if now == 0 {
		now = float64(time.Now().UnixNano()) / float64(time.Second)
	}
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations(id,kind,worktree_id,phase,status,plan_digest,error,created_ts,updated_ts)
VALUES(?,?,?,?,?,?,?,?,?)`, operation.ID, operation.Kind, operation.WorktreeID,
		OperationPrepared, OperationPrepared, operation.PlanDigest, "", now, now); err != nil {
		return fmt.Errorf("state: prepare operation: %w", err)
	}
	for i, step := range steps {
		sequence := step.Sequence
		if sequence == 0 {
			sequence = i + 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO operation_steps(operation_id,ord,kind,target,before_digest,after_digest,proof_id,phase,completed_ts)
VALUES(?,?,?,?,?,?,?,?,NULL)`, operation.ID, sequence, step.Kind, step.Target,
			step.BeforeDigest, step.AfterDigest, step.ProofID, OperationPrepared); err != nil {
			return fmt.Errorf("state: prepare operation step: %w", err)
		}
	}
	return tx.Commit()
}

func AdvanceOperation(ctx context.Context, db *DB, operationID, phase, status, sanitizedError string) error {
	if db == nil || operationID == "" || phase == "" || status == "" {
		return errors.New("state: incomplete operation transition")
	}
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	result, err := db.conn.ExecContext(ctx, `UPDATE operations SET phase=?,status=?,error=?,updated_ts=? WHERE id=?`,
		phase, status, sanitizedError, now, operationID)
	if err != nil {
		return fmt.Errorf("state: advance operation: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func CompleteOperationStep(ctx context.Context, db *DB, operationID string, sequence int, phase string) error {
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	result, err := db.conn.ExecContext(ctx, `UPDATE operation_steps SET phase=?,completed_ts=? WHERE operation_id=? AND ord=?`,
		phase, now, operationID, sequence)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func PrepareRestoreOperation(ctx context.Context, db *DB, operationID, targetCheckpointID, planDigest string) error {
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	_, err := db.conn.ExecContext(ctx, `INSERT INTO restore_operations(operation_id,target_checkpoint_id,plan_digest,phase,created_ts,updated_ts)
VALUES(?,?,?,?,?,?)`, operationID, targetCheckpointID, planDigest, OperationPrepared, now, now)
	return err
}

func CompleteRestoreOperation(ctx context.Context, db *DB, operationID, preCheckpointID, postCheckpointID, phase string) error {
	result, err := db.conn.ExecContext(ctx, `UPDATE restore_operations SET pre_restore_checkpoint_id=?,post_restore_checkpoint_id=?,phase=?,updated_ts=? WHERE operation_id=?`,
		preCheckpointID, postCheckpointID, phase, float64(time.Now().UnixNano())/float64(time.Second), operationID)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func AdvanceRestoreOperation(ctx context.Context, db *DB, operationID, preCheckpointID, postCheckpointID, phase string) error {
	result, err := db.conn.ExecContext(ctx, `UPDATE restore_operations
SET pre_restore_checkpoint_id=COALESCE(NULLIF(?,''),pre_restore_checkpoint_id),
    post_restore_checkpoint_id=COALESCE(NULLIF(?,''),post_restore_checkpoint_id),
    phase=?,updated_ts=? WHERE operation_id=?`, preCheckpointID, postCheckpointID,
		phase, float64(time.Now().UnixNano())/float64(time.Second), operationID)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func RepairableRestoreOperation(ctx context.Context, db *DB) (RestoreOperation, error) {
	var operation RestoreOperation
	err := db.conn.QueryRowContext(ctx, `
SELECT r.operation_id,o.worktree_id,r.target_checkpoint_id,r.pre_restore_checkpoint_id,
       r.post_restore_checkpoint_id,r.plan_digest,r.phase,o.status,o.created_ts
FROM restore_operations r JOIN operations o ON o.id=r.operation_id
WHERE o.status IN (?,?) AND r.phase IN ('applying','applied')
ORDER BY o.created_ts DESC LIMIT 1`, OperationActive, OperationNeedsAttention).Scan(
		&operation.OperationID, &operation.WorktreeID, &operation.TargetCheckpointID, &operation.PreCheckpointID,
		&operation.PostCheckpointID, &operation.PlanDigest, &operation.Phase,
		&operation.OperationStatus, &operation.OperationCreatedTS)
	return operation, err
}

// RestoreRepairPending reports whether this worktree has an interrupted
// restore that must hold capture and publication while the repair RPC remains
// available.
func RestoreRepairPending(ctx context.Context, db *DB) (bool, error) {
	if db == nil {
		return false, errors.New("state: restore repair check requires a database")
	}
	_, err := RepairableRestoreOperation(ctx, db)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// RestoreOperationSteps returns the immutable path proof ledger in execution
// order. Repair uses it to distinguish exact before/after states from edits
// that were not authored by the interrupted restore.
func RestoreOperationSteps(ctx context.Context, db *DB, operationID string) ([]OperationStep, error) {
	if db == nil || operationID == "" {
		return nil, errors.New("state: restore operation steps require an operation")
	}
	rows, err := db.readSQL().QueryContext(ctx, `
SELECT operation_id,ord,kind,target,before_digest,after_digest,proof_id,phase
FROM operation_steps WHERE operation_id=? ORDER BY ord`, operationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var steps []OperationStep
	for rows.Next() {
		var step OperationStep
		if err := rows.Scan(&step.OperationID, &step.Sequence, &step.Kind, &step.Target,
			&step.BeforeDigest, &step.AfterDigest, &step.ProofID, &step.Phase); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

func UncheckpointedEventSeqsSince(ctx context.Context, db *DB, capturedAfter float64) ([]int64, error) {
	rows, err := db.conn.QueryContext(ctx, `
SELECT e.seq FROM capture_events e
LEFT JOIN checkpoint_events ce ON ce.event_seq=e.seq
WHERE ce.event_seq IS NULL AND e.captured_ts>=?
ORDER BY e.seq`, capturedAfter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		result = append(result, seq)
	}
	return result, rows.Err()
}

package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CheckpointCapture is one capture event, its exact materialization, and the
// shadow transition it owns. The checkpoint must already be completed before
// this batch can become visible to publication.
type CheckpointCapture struct {
	Event         CaptureEvent
	Ops           []CaptureOp
	ShadowDeletes []string
	ShadowUpsert  *ShadowPath
}

// AttachCheckpointCaptures atomically appends capture events, attaches their
// checkpoint ownership, and advances shadow state. A failure leaves all three
// unchanged, so the next scan can classify the same live changes again.
func AttachCheckpointCaptures(ctx context.Context, d *DB, checkpointID string, captures []CheckpointCapture) ([]int64, error) {
	if d == nil || checkpointID == "" {
		return nil, errors.New("state: checkpoint capture requires state and checkpoint")
	}
	if len(captures) == 0 {
		return nil, nil
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("state: begin checkpoint capture: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var phase string
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM checkpoints WHERE id=?`, checkpointID).Scan(&phase); err != nil {
		return nil, fmt.Errorf("state: inspect capture checkpoint: %w", err)
	}
	if phase != CheckpointCompleted {
		return nil, fmt.Errorf("state: capture checkpoint %s is %s, want completed", checkpointID, phase)
	}
	var nextOrd int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(ord),-1)+1 FROM checkpoint_events WHERE checkpoint_id=?`, checkpointID).Scan(&nextOrd); err != nil {
		return nil, fmt.Errorf("state: inspect checkpoint membership: %w", err)
	}

	seqs := make([]int64, 0, len(captures))
	for index, capture := range captures {
		seq, err := insertCheckpointCapture(ctx, tx, checkpointID, nextOrd+index, capture)
		if err != nil {
			return nil, fmt.Errorf("state: attach checkpoint capture %d: %w", index, err)
		}
		seqs = append(seqs, seq)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("state: commit checkpoint captures: %w", err)
	}
	return seqs, nil
}

func insertCheckpointCapture(ctx context.Context, tx *sql.Tx, checkpointID string, ord int, capture CheckpointCapture) (int64, error) {
	ev := capture.Event
	if ev.BranchRef == "" || ev.BaseHead == "" || ev.Operation == "" || ev.Path == "" || ev.Fidelity == "" {
		return 0, errors.New("capture event required field missing")
	}
	if ev.CapturedTS == 0 {
		ev.CapturedTS = nowSeconds()
	}
	if ev.State == "" {
		ev.State = EventStatePending
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO capture_events(
    branch_ref, branch_generation, base_head, operation, path, old_path,
    fidelity, captured_ts, published_ts, state, commit_oid, error, message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.BranchRef, ev.BranchGeneration, ev.BaseHead, ev.Operation, ev.Path,
		ev.OldPath, ev.Fidelity, ev.CapturedTS, ev.PublishedTS, ev.State,
		ev.CommitOID, ev.Error, ev.Message)
	if err != nil {
		return 0, fmt.Errorf("insert capture event: %w", err)
	}
	seq, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("capture event sequence: %w", err)
	}
	for opOrd, op := range capture.Ops {
		if op.Op == "" || op.Path == "" || op.Fidelity == "" {
			return 0, fmt.Errorf("capture op %d required field missing", opOrd)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO capture_ops(
    event_seq, ord, op, path, old_path, before_oid, before_mode,
    after_oid, after_mode, fidelity
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, seq, opOrd, op.Op, op.Path,
			op.OldPath, op.BeforeOID, op.BeforeMode, op.AfterOID, op.AfterMode,
			op.Fidelity); err != nil {
			return 0, fmt.Errorf("insert capture op %d: %w", opOrd, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,?,?)`,
		checkpointID, ord, seq); err != nil {
		return 0, fmt.Errorf("insert checkpoint membership: %w", err)
	}
	for _, path := range capture.ShadowDeletes {
		if path == "" {
			return 0, errors.New("empty shadow delete path")
		}
		if _, err := tx.ExecContext(ctx, `
DELETE FROM shadow_paths WHERE branch_ref=? AND branch_generation=? AND path=?`,
			ev.BranchRef, ev.BranchGeneration, path); err != nil {
			return 0, fmt.Errorf("delete shadow path: %w", err)
		}
	}
	if capture.ShadowUpsert != nil {
		shadow := *capture.ShadowUpsert
		if shadow.BranchRef != ev.BranchRef || shadow.BranchGeneration != ev.BranchGeneration ||
			shadow.Path == "" || shadow.BaseHead == "" || shadow.Operation == "" || shadow.Fidelity == "" {
			return 0, errors.New("shadow upsert does not match capture event")
		}
		if shadow.UpdatedTS == 0 {
			shadow.UpdatedTS = nowSeconds()
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO shadow_paths(
    branch_ref,branch_generation,path,operation,mode,oid,old_path,
    base_head,fidelity,updated_ts
) VALUES(?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(branch_ref,branch_generation,path) DO UPDATE SET
    operation=excluded.operation,mode=excluded.mode,oid=excluded.oid,
    old_path=excluded.old_path,base_head=excluded.base_head,
    fidelity=excluded.fidelity,updated_ts=excluded.updated_ts`,
			shadow.BranchRef, shadow.BranchGeneration, shadow.Path,
			shadow.Operation, shadow.Mode, shadow.OID, shadow.OldPath,
			shadow.BaseHead, shadow.Fidelity, shadow.UpdatedTS); err != nil {
			return 0, fmt.Errorf("upsert shadow path: %w", err)
		}
	}
	return seq, nil
}

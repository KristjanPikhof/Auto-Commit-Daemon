package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

const finishHistoryDDL = `
CREATE TABLE IF NOT EXISTS finish_history_requests(
 plan_id TEXT NOT NULL,
 attempt INTEGER NOT NULL CHECK(attempt>0),
 worktree_id TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('running','prepared','completed','failed','needs_attention')),
 request_json TEXT NOT NULL,
 proof_json TEXT NOT NULL DEFAULT '',
 result_json TEXT NOT NULL DEFAULT '',
 error TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(plan_id,attempt)
);
CREATE UNIQUE INDEX IF NOT EXISTS finish_history_active ON finish_history_requests(worktree_id)
 WHERE status IN ('running','prepared','needs_attention');
CREATE TRIGGER IF NOT EXISTS finish_history_identity BEFORE UPDATE OF plan_id,attempt,worktree_id,request_json ON finish_history_requests
 BEGIN SELECT RAISE(ABORT,'immutable history request'); END;
CREATE TRIGGER IF NOT EXISTS finish_history_no_delete BEFORE DELETE ON finish_history_requests
 BEGIN SELECT RAISE(ABORT,'retain history evidence'); END;
CREATE TABLE IF NOT EXISTS finish_history_mappings(
 plan_id TEXT NOT NULL,
 attempt INTEGER NOT NULL,
 old_oid TEXT NOT NULL,
 new_oid TEXT NOT NULL,
 PRIMARY KEY(plan_id,attempt,old_oid,new_oid),
 FOREIGN KEY(plan_id,attempt) REFERENCES finish_history_requests(plan_id,attempt)
);
CREATE TRIGGER IF NOT EXISTS finish_history_mapping_update BEFORE UPDATE ON finish_history_mappings
 BEGIN SELECT RAISE(ABORT,'immutable history mapping'); END;
CREATE TRIGGER IF NOT EXISTS finish_history_mapping_delete BEFORE DELETE ON finish_history_mappings
 BEGIN SELECT RAISE(ABORT,'retain history mapping'); END;
`

type FinishHistoryCommit struct {
	OID, ParentOID, TreeOID, Message, AuthorOID string
}

type FinishHistoryMapping struct{ OldOID, NewOID string }

type FinishHistoryProof struct {
	Version                                                                                                                               int
	PlanID, WorktreeID, SourceBranchRef, TargetBranchRef, ExpectedHead, BaseParent, IndexDigest, SourceRef, ObjectRef, BackupRef, NewHead string
	Commits                                                                                                                               []FinishHistoryCommit
	Mappings                                                                                                                              []FinishHistoryMapping
}

type FinishHistoryRequest struct {
	PlanID       string          `json:"plan_id"`
	Attempt      int             `json:"attempt"`
	WorktreeID   string          `json:"worktree_id"`
	DeadlineTS   float64         `json:"deadline_ts"`
	ConfigDigest string          `json:"config_digest"`
	DryRun       bool            `json:"dry_run"`
	Status       string          `json:"status"`
	Proof        json.RawMessage `json:"proof,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	Error        string          `json:"error,omitempty"`
}

func ActiveFinishHistoryRequest(ctx context.Context, db *DB, worktree string) (FinishHistoryRequest, bool, error) {
	return readFinishHistoryRequest(db.readSQL().QueryRowContext(ctx, `SELECT request_json,status,proof_json,result_json,error FROM finish_history_requests WHERE worktree_id=? AND status IN ('running','prepared','needs_attention')`, worktree))
}

func LoadFinishHistoryRequest(ctx context.Context, db *DB, planID string) (FinishHistoryRequest, bool, error) {
	return readFinishHistoryRequest(db.readSQL().QueryRowContext(ctx, `SELECT request_json,status,proof_json,result_json,error FROM finish_history_requests WHERE plan_id=? ORDER BY attempt DESC LIMIT 1`, planID))
}

func readFinishHistoryRequest(row *sql.Row) (FinishHistoryRequest, bool, error) {
	var r FinishHistoryRequest
	var body, status, proof, result, failure string
	err := row.Scan(&body, &status, &proof, &result, &failure)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	if err := json.Unmarshal([]byte(body), &r); err != nil {
		return r, false, err
	}
	r.Status, r.Proof, r.Result, r.Error = status, json.RawMessage(proof), json.RawMessage(result), failure
	return r, true, nil
}

// StartFinishHistoryRequest is called only by the explicit canonical RPC. It
// reconnects to the same active plan and retains every prior attempt.
func StartFinishHistoryRequest(ctx context.Context, db *DB, r FinishHistoryRequest) (FinishHistoryRequest, bool, error) {
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return r, false, err
	}
	defer tx.Rollback()
	active, exists, err := readFinishHistoryRequest(tx.QueryRowContext(ctx, `SELECT request_json,status,proof_json,result_json,error FROM finish_history_requests WHERE worktree_id=? AND status IN ('running','prepared','needs_attention')`, r.WorktreeID))
	if err != nil {
		return r, false, err
	}
	if exists {
		if active.PlanID != r.PlanID || active.DryRun != r.DryRun {
			return active, false, ErrFinishBusy
		}
		return active, false, nil
	}
	old, exists, err := readFinishHistoryRequest(tx.QueryRowContext(ctx, `SELECT request_json,status,proof_json,result_json,error FROM finish_history_requests WHERE plan_id=? ORDER BY attempt DESC LIMIT 1`, r.PlanID))
	if err != nil {
		return r, false, err
	}
	if exists && old.Status == "completed" && !old.DryRun {
		return old, false, nil
	}
	r.Attempt, r.Status = old.Attempt+1, "running"
	body, err := json.Marshal(r)
	if err != nil {
		return r, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO finish_history_requests(plan_id,attempt,worktree_id,status,request_json) VALUES(?,?,?,'running',?)`, r.PlanID, r.Attempt, r.WorktreeID, string(body)); err != nil {
		return r, false, err
	}
	if err := tx.Commit(); err != nil {
		return r, false, err
	}
	return r, true, nil
}

func UpdateFinishHistoryRequest(ctx context.Context, db *DB, r FinishHistoryRequest, from string) error {
	if len(r.Proof) > IntentHistoryPlanByteCap || len(r.Result) > IntentHistoryPlanByteCap {
		return errors.New("state: history evidence exceeds limit")
	}
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE finish_history_requests SET status=?,proof_json=?,result_json=?,error=? WHERE plan_id=? AND attempt=? AND status=?`, r.Status, string(r.Proof), string(r.Result), r.Error, r.PlanID, r.Attempt, from)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("state: history request changed: %w", errors.Join(err, ErrFinishState))
	}
	if r.Status == "completed" && !r.DryRun && len(r.Proof) > 0 {
		var proof FinishHistoryProof
		if err := json.Unmarshal(r.Proof, &proof); err != nil {
			return err
		}
		for _, mapping := range proof.Mappings {
			if _, err := tx.ExecContext(ctx, `INSERT INTO finish_history_mappings(plan_id,attempt,old_oid,new_oid) VALUES(?,?,?,?) ON CONFLICT DO NOTHING`, r.PlanID, r.Attempt, mapping.OldOID, mapping.NewOID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func InterruptFinishHistoryRequests(ctx context.Context, db *DB, worktree string) error {
	_, err := db.conn.ExecContext(ctx, `UPDATE finish_history_requests SET status='failed',error='Worker restarted before preparation; explicitly apply the saved plan again' WHERE worktree_id=? AND status='running'`, worktree)
	return err
}

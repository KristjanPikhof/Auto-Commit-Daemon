package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"testing"
)

const testIntentRepairPlanDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func openLegacyTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	db, path := openTestDB(t)
	if _, err := db.SQL().ExecContext(t.Context(), LegacySchemaDDL); err != nil {
		t.Fatal(err)
	}
	return db, path
}

func fixtureAppendCaptureEvent(ctx context.Context, d *DB, e CaptureEvent, ops []CaptureOp) (int64, error) {
	tx, err := d.SQL().BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if e.State == "" {
		e.State = EventStatePending
	}
	if e.CapturedTS == 0 {
		e.CapturedTS = nowSeconds()
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO capture_events(branch_ref,branch_generation,base_head,operation,path,old_path,fidelity,captured_ts,published_ts,state,commit_oid,error,message) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, e.BranchRef, e.BranchGeneration, e.BaseHead, e.Operation, e.Path, e.OldPath, e.Fidelity, e.CapturedTS, e.PublishedTS, e.State, e.CommitOID, e.Error, e.Message)
	if err != nil {
		return 0, err
	}
	seq, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	for i, o := range ops {
		if _, err = tx.ExecContext(ctx, `INSERT INTO capture_ops VALUES(?,?,?,?,?,?,?,?,?,?)`, seq, i, o.Op, o.Path, o.OldPath, o.BeforeOID, o.BeforeMode, o.AfterOID, o.AfterMode, o.Fidelity); err != nil {
			return 0, err
		}
	}
	return seq, tx.Commit()
}

func fixtureSaveIntentCandidate(ctx context.Context, d *DB, c IntentCandidate) error {
	tx, err := d.SQL().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if c.Status == "" {
		c.Status = IntentCandidateOpen
	}
	if c.Readiness == "" {
		c.Readiness = IntentReadinessWait
	}
	if c.CreatedTS == 0 {
		c.CreatedTS = nowSeconds()
	}
	if c.UpdatedTS == 0 {
		c.UpdatedTS = c.CreatedTS
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO intent_candidates(id,branch_ref,branch_generation,status,purpose,created_ts,updated_ts,readiness,missing_companions,published_commit_oid,soft_publication_deadline,atomicity_status,atomicity_summary) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, c.ID, c.BranchRef, c.BranchGeneration, c.Status, c.Purpose, c.CreatedTS, c.UpdatedTS, c.Readiness, c.MissingCompanions, c.PublishedCommitOID, c.SoftPublicationDeadline, c.AtomicityStatus, c.AtomicitySummary)
	if err != nil {
		return err
	}
	for i, e := range c.Events {
		if e.MembershipState == "" {
			e.MembershipState = IntentMembershipActive
		}
		if e.EventRole == "" {
			e.EventRole = "implementation"
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO intent_candidate_events(candidate_id,ord,event_seq,event_role,membership_state) VALUES(?,?,?,?,?)`, c.ID, i, e.EventSeq, e.EventRole, e.MembershipState); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func fixtureSaveIntentRepair(ctx context.Context, d *DB, r IntentRepair) error {
	tx, err := d.SQL().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if r.Status == "" {
		r.Status = IntentRepairPrepared
	}
	if r.CreatedTS == 0 {
		r.CreatedTS = nowSeconds()
	}
	if r.MembershipMode == "" {
		r.MembershipMode = IntentRepairMembershipNone
		if len(r.Members) > 0 {
			r.MembershipMode = IntentRepairMembershipFrozen
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO intent_repairs(id,branch_ref,branch_generation,status,expected_head,plan_digest,backup_ref,old_head,new_head,created_ts,updated_ts,git_applied_ts,completed_ts,error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, r.ID, r.BranchRef, r.BranchGeneration, r.Status, r.ExpectedHead, r.PlanDigest, r.BackupRef, r.OldHead, r.NewHead, r.CreatedTS, r.CreatedTS, r.GitAppliedTS, r.CompletedTS, r.Error)
	if err != nil {
		return err
	}
	for i, c := range r.Commits {
		if _, err = tx.ExecContext(ctx, `INSERT INTO intent_repair_commits VALUES(?,?,?,?,?)`, r.ID, i, c.CandidateID, c.OldOID, c.NewOID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO intent_repair_commit_lineage VALUES(?,?,?,?,?)`, r.ID, i, c.CandidateID, c.OldOID, c.NewOID); err != nil {
			return err
		}
	}
	for i, m := range r.Members {
		if _, err = tx.ExecContext(ctx, `INSERT INTO intent_repair_members VALUES(?,?,?,?,?)`, r.ID, i, m.CandidateID, m.EventSeq, m.PriorState); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO intent_repair_member_seals VALUES(?,?,?)`, r.ID, r.MembershipMode, len(r.Members)); err != nil {
		return err
	}
	return tx.Commit()
}

func fixtureTransitionIntentRepair(ctx context.Context, d *DB, id string, tr IntentRepairTransition) (bool, error) {
	tx, err := d.SQL().BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE intent_repairs SET status=?,new_head=COALESCE(?,new_head),old_head=COALESCE(?,old_head),backup_ref=COALESCE(?,backup_ref),git_applied_ts=CASE WHEN ?='git_applied' THEN ? ELSE git_applied_ts END,completed_ts=CASE WHEN ?='completed' THEN ? ELSE completed_ts END WHERE id=? AND status=?`, tr.Status, tr.NewHead, tr.OldHead, tr.BackupRef, tr.Status, tr.TransitionTS, tr.Status, tr.TransitionTS, id, tr.ExpectedStatus)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	for i, c := range tr.Commits {
		for _, table := range []string{"intent_repair_commits", "intent_repair_commit_lineage"} {
			if _, err = tx.ExecContext(ctx, "UPDATE "+table+" SET new_oid=? WHERE repair_id=? AND ord=?", c.NewOID, id, i); err != nil {
				return false, err
			}
		}
	}
	return n == 1, tx.Commit()
}

func fixturePrepareSelfPublication(ctx context.Context, db *DB, p SelfPublication) (bool, error) {
	digest, err := SelfPublicationMembershipDigest(p.Members)
	if err != nil {
		return false, err
	}
	if !p.OperationID.Valid {
		id := "op_" + p.ID
		if len(id) > 128 {
			hash := sha256.Sum256([]byte(p.ID))
			id = "op_self_publication_" + hex.EncodeToString(hash[:])
		}
		p.OperationID = sql.NullString{String: id, Valid: true}
	}
	if p.CreatedTS == 0 {
		p.CreatedTS = nowSeconds()
	}
	if p.Completion.CandidateStatus == "" {
		p.Completion.CandidateStatus = IntentCandidatePublished
	}
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations(id,kind,phase,status,plan_digest,error,created_ts,updated_ts) VALUES(?,'self_publication','prepared','prepared',?,'',?,?)`, p.OperationID, digest, p.CreatedTS, p.CreatedTS); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO self_publications(id,operation_id,branch_ref,branch_generation,source_head,target_commit_oid,target_tree_oid,membership_digest,member_count,phase,created_ts,updated_ts,error,completion_published_ts,completion_candidate_status,completion_soft_deadline) VALUES(?,?,?,?,?,?,?,?,?,'prepared',?,?,'',?,?,?)`, p.ID, p.OperationID, p.BranchRef, p.BranchGeneration, p.SourceHead, p.TargetCommitOID, p.TargetTreeOID, digest, len(p.Members), p.CreatedTS, p.CreatedTS, p.Completion.PublishedTS, p.Completion.CandidateStatus, p.Completion.SoftPublicationDeadline); err != nil {
		return false, err
	}
	for ord, member := range p.Members {
		if _, err := tx.ExecContext(ctx, `INSERT INTO self_publication_members(publication_id,ord,event_seq,candidate_id) VALUES(?,?,?,?)`, p.ID, ord, member.EventSeq, member.CandidateID); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}

type Publish struct {
	EventSeq         sql.NullInt64
	BranchRef        sql.NullString
	BranchGeneration sql.NullInt64
	SourceHead       sql.NullString
	TargetCommitOID  sql.NullString
	Status           string
	Error            sql.NullString
	UpdatedTS        float64
}

func fixtureDeletePublishedEvents(ctx context.Context, d *DB) (int, error) {
	res, err := d.SQL().ExecContext(ctx, "DELETE FROM capture_events WHERE state='published'")
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
func fixtureLoadCaptureOps(ctx context.Context, d *DB, seq int64) ([]CaptureOp, error) {
	const q = `
SELECT event_seq, ord, op, path, old_path,
       before_oid, before_mode, after_oid, after_mode, fidelity
FROM capture_ops WHERE event_seq = ? ORDER BY ord ASC`
	rows, err := d.readSQL().QueryContext(ctx, q, seq)
	if err != nil {
		return nil, fmt.Errorf("state: query capture ops: %w", err)
	}
	defer rows.Close()
	var out []CaptureOp
	for rows.Next() {
		var op CaptureOp
		if err := rows.Scan(&op.EventSeq, &op.Ord, &op.Op, &op.Path, &op.OldPath,
			&op.BeforeOID, &op.BeforeMode, &op.AfterOID, &op.AfterMode, &op.Fidelity); err != nil {
			return nil, fmt.Errorf("state: scan capture op: %w", err)
		}
		out = append(out, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter capture ops: %w", err)
	}
	return out, nil
}
func fixtureLoadPublishState(ctx context.Context, d *DB) (Publish, bool, error) {
	const q = `
SELECT event_seq, branch_ref, branch_generation, source_head, target_commit_oid,
       status, error, updated_ts
FROM publish_state WHERE id = 1`
	var p Publish
	err := d.readSQL().QueryRowContext(ctx, q).Scan(
		&p.EventSeq, &p.BranchRef, &p.BranchGeneration, &p.SourceHead, &p.TargetCommitOID,
		&p.Status, &p.Error, &p.UpdatedTS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Publish{Status: "idle"}, false, nil
	}
	if err != nil {
		return Publish{}, false, fmt.Errorf("state: load publish_state: %w", err)
	}
	return p, true, nil
}
func fixtureUpsertShadowPath(ctx context.Context, d *DB, sp ShadowPath) error {
	if sp.BranchRef == "" || sp.Path == "" || sp.BaseHead == "" || sp.Operation == "" || sp.Fidelity == "" {
		return fmt.Errorf("state: fixtureUpsertShadowPath: required field missing")
	}
	if sp.UpdatedTS == 0 {
		sp.UpdatedTS = nowSeconds()
	}
	const q = `
INSERT INTO shadow_paths(
    branch_ref, branch_generation, path, operation, mode, oid,
    old_path, base_head, fidelity, updated_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(branch_ref, branch_generation, path) DO UPDATE SET
    operation  = excluded.operation,
    mode       = excluded.mode,
    oid        = excluded.oid,
    old_path   = excluded.old_path,
    base_head  = excluded.base_head,
    fidelity   = excluded.fidelity,
    updated_ts = excluded.updated_ts`
	if _, err := d.conn.ExecContext(ctx, q,
		sp.BranchRef, sp.BranchGeneration, sp.Path, sp.Operation, sp.Mode, sp.OID,
		sp.OldPath, sp.BaseHead, sp.Fidelity, sp.UpdatedTS,
	); err != nil {
		return fmt.Errorf("state: upsert shadow path: %w", err)
	}
	return nil
}
func fixtureGetShadowPath(ctx context.Context, d *DB, branchRef string, gen int64, path string) (ShadowPath, bool, error) {
	const q = `
SELECT branch_ref, branch_generation, path, operation, mode, oid,
       old_path, base_head, fidelity, updated_ts
FROM shadow_paths
WHERE branch_ref = ? AND branch_generation = ? AND path = ?`
	var sp ShadowPath
	err := d.readSQL().QueryRowContext(ctx, q, branchRef, gen, path).Scan(
		&sp.BranchRef, &sp.BranchGeneration, &sp.Path, &sp.Operation, &sp.Mode, &sp.OID,
		&sp.OldPath, &sp.BaseHead, &sp.Fidelity, &sp.UpdatedTS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ShadowPath{}, false, nil
	}
	if err != nil {
		return ShadowPath{}, false, fmt.Errorf("state: get shadow path: %w", err)
	}
	return sp, true, nil
}
func fixtureMarkEventBlocked(ctx context.Context, d *DB, seq int64, errMsg string, publishedTS float64,
	branchRef sql.NullString, branchGeneration sql.NullInt64, sourceHead sql.NullString,
) error {
	tx, err := d.SQL().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE capture_events SET state='blocked_conflict',error=?,published_ts=? WHERE seq=?", errMsg, publishedTS, seq); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO publish_state(id,event_seq,branch_ref,branch_generation,source_head,status,error,updated_ts) VALUES(1,?,?,?,?,'blocked_conflict',?,?)`, seq, branchRef, branchGeneration, sourceHead, errMsg, publishedTS); err != nil {
		return err
	}
	return tx.Commit()
}
func seedLegacyCheckpoint(
	t *testing.T,
	db *DB,
	paths []string,
) Checkpoint {
	t.Helper()
	ctx := context.Background()
	checkpoint := Checkpoint{
		ID:          "cp-1786486000000-0123456789abcdef",
		OperationID: "op-publication-drain",
		WorktreeID:  "0123456789abcdef", Reason: CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: "head",
		ObservedRef: "refs/heads/main", TreeOID: "tree", CommitOID: "commit",
		Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786486000000-0123456789abcdef",
		CreatedTS: 1,
	}
	for i, path := range paths {
		result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,'head','modify',?,'exact',?,'pending')`, path, i+1)
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		checkpoint.EventSeqs = append(checkpoint.EventSeqs, seq)
	}
	if created, err := PrepareCheckpoint(
		ctx, db, checkpoint, checkpointTestDigest); err != nil || !created {
		t.Fatalf("checkpoint prepare=(%t,%v)", created, err)
	}
	if err := CompleteCheckpoint(
		ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

// Package testutil builds isolated historical state for regression tests.
// Production code must not import it.
package testutil

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// InitLegacy installs the archived schema only in an explicit legacy fixture.
func InitLegacy(ctx context.Context, db *state.DB) error {
	exists, err := state.LegacyTableExists(ctx, db.ReadSQL(), "capture_events")
	if err != nil || exists {
		return err
	}
	_, err = db.SQL().ExecContext(ctx, state.LegacySchemaDDL)
	return err
}

func fixtureTime() float64 { return float64(time.Now().UnixNano()) / 1e9 }

type LegacyClient struct {
	SessionID, Harness string
	WatchPID           sql.NullInt64
	WatchFP            sql.NullString
	RegisteredTS       float64
	LastSeenTS         float64
}

func SeedLegacyClient(ctx context.Context, db *state.DB, c LegacyClient) error {
	if err := InitLegacy(ctx, db); err != nil {
		return err
	}
	if c.RegisteredTS == 0 {
		c.RegisteredTS = fixtureTime()
	}
	if c.LastSeenTS == 0 {
		c.LastSeenTS = c.RegisteredTS
	}
	_, err := db.SQL().ExecContext(ctx, `INSERT INTO daemon_clients(session_id,harness,watch_pid,watch_fp,registered_ts,last_seen_ts) VALUES(?,?,?,?,?,?)`, c.SessionID, c.Harness, c.WatchPID, c.WatchFP, c.RegisteredTS, c.LastSeenTS)
	return err
}

func SeedLegacyCaptureOutcome(ctx context.Context, db *state.DB, seq int64, outcome string, commit, failure, message sql.NullString, publishedTS float64) error {
	_, err := db.SQL().ExecContext(ctx, `UPDATE capture_events SET state=?,commit_oid=?,error=?,message=?,published_ts=? WHERE seq=?`, outcome, commit, failure, message, publishedTS, seq)
	return err
}

func SeedLegacyCapture(ctx context.Context, db *state.DB, e state.CaptureEvent, ops []state.CaptureOp) (int64, error) {
	if err := InitLegacy(ctx, db); err != nil {
		return 0, err
	}
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if e.State == "" {
		e.State = state.EventStatePending
	}
	if e.CapturedTS == 0 {
		e.CapturedTS = fixtureTime()
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO capture_events(branch_ref,branch_generation,base_head,operation,path,old_path,fidelity,captured_ts,published_ts,state,commit_oid,error,message) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, e.BranchRef, e.BranchGeneration, e.BaseHead, e.Operation, e.Path, e.OldPath, e.Fidelity, e.CapturedTS, e.PublishedTS, e.State, e.CommitOID, e.Error, e.Message)
	if err != nil {
		return 0, err
	}
	seq, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	for ord, op := range ops {
		if _, err := tx.ExecContext(ctx, `INSERT INTO capture_ops(event_seq,ord,op,path,old_path,before_oid,before_mode,after_oid,after_mode,fidelity) VALUES(?,?,?,?,?,?,?,?,?,?)`, seq, ord, op.Op, op.Path, op.OldPath, op.BeforeOID, op.BeforeMode, op.AfterOID, op.AfterMode, op.Fidelity); err != nil {
			return 0, err
		}
	}
	return seq, tx.Commit()
}

func SeedLegacyCandidate(ctx context.Context, db *state.DB, c state.IntentCandidate) error {
	if err := InitLegacy(ctx, db); err != nil {
		return err
	}
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if c.Status == "" {
		c.Status = state.IntentCandidateOpen
	}
	if c.Readiness == "" {
		c.Readiness = state.IntentReadinessWait
	}
	if c.CreatedTS == 0 {
		c.CreatedTS = fixtureTime()
	}
	if c.UpdatedTS == 0 {
		c.UpdatedTS = c.CreatedTS
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO intent_candidates(id,branch_ref,branch_generation,status,purpose,created_ts,updated_ts,readiness,missing_companions,published_commit_oid,soft_publication_deadline,atomicity_status,atomicity_summary) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, c.ID, c.BranchRef, c.BranchGeneration, c.Status, c.Purpose, c.CreatedTS, c.UpdatedTS, c.Readiness, c.MissingCompanions, c.PublishedCommitOID, c.SoftPublicationDeadline, c.AtomicityStatus, c.AtomicitySummary); err != nil {
		return err
	}
	for ord, member := range c.Events {
		if member.MembershipState == "" {
			member.MembershipState = state.IntentMembershipActive
		}
		if member.EventRole == "" {
			member.EventRole = "implementation"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO intent_candidate_events(candidate_id,ord,event_seq,event_role,membership_state) VALUES(?,?,?,?,?)`, c.ID, ord, member.EventSeq, member.EventRole, member.MembershipState); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func SeedLegacySelfPublication(ctx context.Context, db *state.DB, p state.SelfPublication) (bool, error) {
	if err := InitLegacy(ctx, db); err != nil {
		return false, err
	}
	digest, err := state.SelfPublicationMembershipDigest(p.Members)
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
		p.CreatedTS = fixtureTime()
	}
	if p.Completion.CandidateStatus == "" {
		p.Completion.CandidateStatus = state.IntentCandidatePublished
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

func SeedLegacyDrain(ctx context.Context, db *state.DB, d state.PublicationDrain) (bool, error) {
	if err := InitLegacy(ctx, db); err != nil {
		return false, err
	}
	if d.CreatedTS == 0 {
		d.CreatedTS = fixtureTime()
	}
	if d.UpdatedTS == 0 {
		d.UpdatedTS = d.CreatedTS
	}
	if d.LastProgressTS == 0 {
		d.LastProgressTS = d.CreatedTS
	}
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO publication_drains(id,checkpoint_id,worktree_id,branch_ref,branch_generation,commit_strategy,commit_format,config_revision_id,provider,provider_model,provider_fingerprint,phase,target_event_count,published_event_count,semantic_rebuild_attempts,event_fallback_count,commit_count,fallback_mode,last_error,staged_consent,staged_consumed,reason_code,reason_evidence,expected_index_digest,created_ts,updated_ts,last_progress_ts,completed_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, d.ID, d.CheckpointID, d.WorktreeID, d.BranchRef, d.BranchGeneration, d.CommitStrategy, d.CommitFormat, d.ConfigRevisionID, d.Provider, d.ProviderModel, d.ProviderFingerprint, d.Phase, d.TargetEventCount, d.PublishedEventCount, d.SemanticRebuildAttempts, d.EventFallbackCount, d.CommitCount, d.FallbackMode, d.LastError, d.StagedConsent, d.StagedConsumed, d.ReasonCode, d.ReasonEvidence, d.ExpectedIndexDigest, d.CreatedTS, d.UpdatedTS, d.LastProgressTS, d.CompletedTS); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO publication_drain_events(drain_id,ord,event_seq) SELECT ?,ord,event_seq FROM checkpoint_events WHERE checkpoint_id=?`, d.ID, d.CheckpointID); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

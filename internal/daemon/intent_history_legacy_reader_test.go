package daemon

import (
	"context"
	"database/sql"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishRemovalLegacyMappingReaderDoesNotMigrate(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	// This read-only compatibility scenario needs actual archived tables; fresh
	// protection databases deliberately contain no retired publication engine.
	if _, err := f.db.SQL().ExecContext(ctx, state.LegacySchemaDDL); err != nil {
		t.Fatal(err)
	}
	tree, err := git.RevParse(ctx, f.dir, "HEAD^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	target, err := git.CommitTree(ctx, f.dir, tree, "Recorded semantic goal", f.cctx.BaseHead)
	if err != nil {
		t.Fatal(err)
	}
	repair := state.IntentRepair{ID: "legacy-proof", BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, Status: state.IntentRepairCompleted, ExpectedHead: f.cctx.BaseHead, OldHead: sql.NullString{String: f.cctx.BaseHead, Valid: true}, NewHead: sql.NullString{String: target, Valid: true}, BackupRef: sql.NullString{String: "refs/acd/intent-repair/legacy-proof", Valid: true}, MembershipMode: state.IntentRepairMembershipLegacy, CompletedTS: sql.NullFloat64{Float64: 1, Valid: true}, Commits: []state.IntentRepairCommit{{RepairID: "legacy-proof", Ord: 0, CandidateID: sql.NullString{String: "recorded-goal", Valid: true}, OldOID: f.cctx.BaseHead, NewOID: sql.NullString{String: target, Valid: true}}}}
	repair.PlanDigest, err = recoveredIntentRepairPlanDigest(ctx, f.dir, repair, repair.Commits)
	if err != nil {
		t.Fatal(err)
	}
	// Seed immutable legacy evidence directly; new writers reject legacy membership.
	if _, err := f.db.SQL().ExecContext(ctx, `INSERT INTO intent_repairs
	(id,branch_ref,branch_generation,status,expected_head,plan_digest,backup_ref,old_head,new_head,created_ts,updated_ts,completed_ts,error)
	VALUES (?, ?, ?, 'completed', ?, ?, ?, ?, ?, 1, 1, 1, '')`, repair.ID, repair.BranchRef, repair.BranchGeneration, repair.ExpectedHead, repair.PlanDigest, repair.BackupRef.String, repair.OldHead.String, repair.NewHead.String); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `INSERT INTO intent_repair_commits (repair_id,ord,candidate_id,old_oid,new_oid) VALUES (?,0,?,?,?)`, repair.ID, "recorded-goal", f.cctx.BaseHead, target); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `INSERT INTO intent_repair_member_seals (repair_id,membership_mode,member_count) VALUES (?,'legacy',0)`, repair.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `UPDATE intent_repairs SET status='completed', completed_ts=1 WHERE id='legacy-proof'; DROP VIEW intent_repair_commit_mappings; PRAGMA user_version=29`); err != nil {
		t.Fatal(err)
	}
	reader, err := state.OpenReadOnly(ctx, f.db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	evidence := newExternalRepairEvidence(reader, f.dir, f.cctx.BranchRef, f.cctx.BranchGeneration)
	mappings, err := evidence.commitMappingsFrom(ctx, f.cctx.BaseHead)
	if err != nil || len(mappings) != 1 || mappings[0].newOID != target {
		t.Fatalf("legacy mappings=%+v err=%v", mappings, err)
	}
	if _, err := evidence.loadRepair(ctx, repair.ID); err != nil {
		t.Fatalf("legacy reader proof failed: %v", err)
	}
	version, err := state.ReadUserVersion(ctx, f.db.Path())
	if err != nil || version != 29 {
		t.Fatalf("read-only recovery migrated schema: version=%d err=%v", version, err)
	}
}

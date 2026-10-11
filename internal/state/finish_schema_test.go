package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestFinishSchemaFreshContainsOnlyCurrentState(t *testing.T) {
	ctx := context.Background()
	db, path := openTestDB(t)
	retired := []string{"daemon_clients", "capture_events", "capture_ops", "shadow_paths", "publish_state", "planner_state", "intent_planner_windows", "intent_planner_window_events", "intent_plan_runs", "intent_candidates", "intent_candidate_events", "intent_candidate_lineage", "intent_capture_dependencies", "intent_activity_boundaries", "intent_repairs", "intent_repair_commits", "intent_repair_commit_lineage", "intent_repair_members", "intent_repair_member_seals", "publication_drains", "publication_drain_events", "flush_requests", "config_validation_runs", "config_experiments", "rewrite_plans", "rewrite_plan_commits", "rewrite_plan_groups", "self_publications", "self_publication_members", "recovery_snapshots", "recovery_snapshot_events"}
	for _, table := range retired {
		exists, err := LegacyTableExists(ctx, db.ReadSQL(), table)
		if err != nil || exists {
			t.Fatalf("fresh legacy %s exists=%v err=%v", table, exists, err)
		}
	}
	first := finishFixture(t, db, 1)
	finishFixture(t, db, 2)
	if err := CheckRetentionLegacyProof(ctx, db); err != nil {
		t.Fatal(err)
	}
	checkpoints, err := RetentionCheckpoints(ctx, db, first.Target.WorktreeID)
	if err != nil || len(checkpoints) != 2 || !checkpoints[0].LatestComplete || checkpoints[1].Published {
		t.Fatalf("fresh retention=%+v err=%v", checkpoints, err)
	}
	if _, found, err := LoadRewritePlan(ctx, db, "old"); err != nil || found {
		t.Fatalf("fresh rewrite found=%v err=%v", found, err)
	}
	if groups, err := ScanGeneratedPendingDeletes(ctx, db.ReadSQL(), nil, 0); err != nil || len(groups) != 0 {
		t.Fatalf("fresh legacy cleanup=%+v err=%v", groups, err)
	}
	if inventory, err := InventoryFinishLegacy(ctx, db); err != nil || len(inventory.Objects) != 0 || len(inventory.Retirements) != 0 {
		t.Fatalf("ordinary checkpoints became legacy inventory=%+v err=%v", inventory, err)
	}
	if _, err := RetentionObjectPins(ctx, db, first.Target.WorktreeID); err != nil {
		t.Fatal(err)
	}
	if _, err := CompactPrunedCheckpoints(ctx, db, first.Target.WorktreeID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadLegacyWorkSummary(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range retired {
		exists, err := LegacyTableExists(ctx, db.ReadSQL(), table)
		if err != nil || exists {
			t.Fatalf("reopen recreated %s: %v", table, err)
		}
	}
	rows, err := db.ReadSQL().QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("fresh foreign key violation")
	}
}

func TestFinishSchemaUpgradePreservesOriginalEvidenceAndReadonlyBoundary(t *testing.T) {
	ctx := context.Background()
	db, path := openLegacyTestDB(t)
	if _, err := db.SQL().ExecContext(ctx, `INSERT INTO capture_events(seq,branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state,commit_oid) VALUES(7,'refs/heads/main',9,'original-base','modify','original.txt','exact',1,'failed','original-commit'); INSERT INTO capture_ops VALUES(7,0,'modify','original.txt',NULL,'old-blob','100644','new-blob','100644','exact'); PRAGMA user_version=32;`); err != nil {
		t.Fatal(err)
	}
	before, err := ReadLegacyEvidenceDigest(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRuntime(ctx, path); !errors.Is(err, ErrSetupRequired) {
		t.Fatalf("runtime silently upgraded v32: %v", err)
	}
	if view, err := ReadFinishLegacyCutover(ctx, path); err != nil || view.State != "needs_attention" {
		t.Fatalf("readonly unresolved=%+v err=%v", view, err)
	}
	if version, err := ReadUserVersion(ctx, path); err != nil || version != 32 {
		t.Fatalf("readonly upgraded=%d err=%v", version, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	after, err := ReadLegacyEvidenceDigest(ctx, path)
	if err != nil || before != after {
		t.Fatalf("upgrade rewrote provenance before=%s after=%s err=%v", before, after, err)
	}
	var state, commit string
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT state,commit_oid FROM capture_events WHERE seq=7").Scan(&state, &commit); err != nil || state != "failed" || commit != "original-commit" {
		t.Fatalf("invented publication %s %s %v", state, commit, err)
	}
	ref := "refs/acd/migration/finish-v1/0123456789abcdef/proof/snapshot"
	oid := strings.Repeat("a", 40)
	if _, err := PrepareFinishLegacyCutover(ctx, db, "0123456789abcdef", FinishLegacyManifest{Captures: []int64{7}}, ref, oid, nil); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"DELETE FROM capture_events WHERE seq=7", "UPDATE capture_ops SET after_oid='changed' WHERE event_seq=7", "DELETE FROM capture_ops WHERE event_seq=7"} {
		if _, err := db.SQL().ExecContext(ctx, query); err == nil {
			t.Fatalf("sealed original mutated: %s", query)
		}
	}
}

func TestFinishRemovalActiveLegacyExperimentDoesNotBlockExplicitApply(t *testing.T) {
	ctx := context.Background()
	db, _ := openLegacyTestDB(t)
	old := testConfigRevision(t, db, "old", 1)
	next := testConfigRevision(t, db, "new", 2)
	if _, err := db.SQL().ExecContext(ctx, `INSERT INTO config_experiments(baseline_revision_id,candidate_revision_id,window_budget,failure_policy,status,created_ts,updated_ts) VALUES(?,?,3,'continue','active',1,1)`, old.ID, next.ID); err != nil {
		t.Fatal(err)
	}
	req, ok, err := RequestConfigActivation(ctx, db, next.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("retired experiment blocked Apply: %+v %v", req, err)
	}
	if ok, err := AcknowledgeConfigActivation(ctx, db, req.ID, next.ID); err != nil || !ok {
		t.Fatal(fmt.Errorf("acknowledge %v: %w", ok, err))
	}
	if ok, err := ApplyConfigActivation(ctx, db, req.ID, next.ID); err != nil || !ok {
		t.Fatalf("apply=%v err=%v", ok, err)
	}
	var status string
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT status FROM config_experiments").Scan(&status); err != nil || status != "active" {
		t.Fatalf("legacy experiment was rewritten %s %v", status, err)
	}
}

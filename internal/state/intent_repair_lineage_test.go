package state

import (
	"context"
	"database/sql"
	"testing"
)

func TestIntentRepairV30MigrationPreservesProvenanceAndAllowsSplitMapping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, path := openTestDB(t)
	if err := SaveIntentRepair(ctx, d, IntentRepair{ID: "legacy-mapping", BranchRef: "refs/heads/main", BranchGeneration: 2, ExpectedHead: "mixed", PlanDigest: testIntentRepairPlanDigest, MembershipMode: IntentRepairMembershipNone, Commits: []IntentRepairCommit{{CandidateID: sql.NullString{String: "goal-a", Valid: true}, OldOID: "mixed"}}}); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open(driverName, buildDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `
DROP VIEW intent_repair_commit_mappings;
DROP TABLE intent_repair_commit_lineage;
PRAGMA user_version=29;
`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	// Inspecting an older runtime must not perform the table migration.
	reader, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var version int
	if err := reader.SQL().QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 29 {
		t.Fatalf("readonly version=%d err=%v", version, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := OpenRuntime(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	repair, ok, err := IntentRepairByID(ctx, migrated, "legacy-mapping")
	if err != nil || !ok || repair.ExpectedHead != "mixed" || repair.BranchGeneration != 2 || repair.Status != IntentRepairPrepared || repair.Commits[0].CandidateID.String != "goal-a" || repair.Commits[0].NewOID.Valid {
		t.Fatalf("migration changed provenance: %+v err=%v", repair, err)
	}
	if _, err := migrated.SQL().ExecContext(ctx, `INSERT INTO intent_repair_commit_lineage(repair_id,ord,candidate_id,old_oid,new_oid) VALUES('legacy-mapping',1,'goal-b','mixed',NULL)`); err != nil {
		t.Fatalf("split lineage refused: %v", err)
	}
	if _, err := migrated.SQL().ExecContext(ctx, `INSERT INTO intent_repair_commit_lineage(repair_id,ord,candidate_id,old_oid,new_oid) VALUES('legacy-mapping',2,'goal-b','mixed',NULL)`); err == nil {
		t.Fatal("same old/candidate relation can be duplicated")
	}
	var foreignKeyRows int
	rows, err := migrated.SQL().QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		foreignKeyRows++
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if foreignKeyRows != 0 {
		t.Fatalf("foreign key violations=%d", foreignKeyRows)
	}
}

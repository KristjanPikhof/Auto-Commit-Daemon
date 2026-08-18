package state

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
)

func TestIntentPlanRunAttemptBudgetSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	db, dbPath := openTestDB(t)
	run := IntentPlanRun{
		Fingerprint: "sha256:test", BranchRef: "refs/heads/main",
		BranchGeneration: 2, AttemptLimit: 3,
		Provider:       sql.NullString{String: "test", Valid: true},
		UnresolvedSeqs: []int64{1, 2},
	}
	for want := 1; want <= 2; want++ {
		got, allowed, err := ReserveIntentPlanAttempt(ctx, db, run)
		if err != nil || !allowed || got.AttemptCount != want {
			t.Fatalf("reserve %d=(%+v,%t,%v)", want, got, allowed, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	got, allowed, err := ReserveIntentPlanAttempt(ctx, db, run)
	if err != nil || !allowed || got.AttemptCount != 3 {
		t.Fatalf("reserve after reopen=(%+v,%t,%v)", got, allowed, err)
	}
	got.PreservedGroups = [][]int64{{1}}
	got.UnresolvedSeqs = []int64{2}
	got.FindingCodes = []string{"candidate_id_duplicate"}
	got.ResolutionMode = sql.NullString{String: "partial_replan", Valid: true}
	got.ResolvedPlanJSON = sql.NullString{
		String: `{"protocol_version":"v2","candidates":[]}`, Valid: true,
	}
	if err := UpdateIntentPlanRun(ctx, db, got); err != nil {
		t.Fatal(err)
	}
	exhausted, allowed, err := ReserveIntentPlanAttempt(ctx, db, run)
	if err != nil || allowed || exhausted.AttemptCount != 3 ||
		!reflect.DeepEqual(exhausted.PreservedGroups, [][]int64{{1}}) ||
		!reflect.DeepEqual(exhausted.UnresolvedSeqs, []int64{2}) ||
		exhausted.ResolvedPlanJSON != got.ResolvedPlanJSON {
		t.Fatalf("exhausted=(%+v,%t,%v)", exhausted, allowed, err)
	}
}

func TestIntentPlanRunRejectsOversizedResolvedPlan(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	run, err := EnsureIntentPlanRun(ctx, db, IntentPlanRun{
		Fingerprint: "sha256:oversized", BranchRef: "refs/heads/main",
		BranchGeneration: 1, AttemptLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	run.ResolvedPlanJSON = sql.NullString{
		String: string(make([]byte, IntentResolvedPlanJSONCap+1)), Valid: true,
	}
	if err := UpdateIntentPlanRun(ctx, db, run); err == nil {
		t.Fatal("oversized resolved plan was accepted")
	}
}

func TestEnsureIntentPlanRunRecordsZeroAttemptResolution(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	run, err := EnsureIntentPlanRun(ctx, db, IntentPlanRun{
		Fingerprint: "sha256:zero", BranchRef: "refs/heads/main",
		BranchGeneration: 1, AttemptLimit: 3,
	})
	if err != nil || run.AttemptCount != 0 {
		t.Fatalf("ensure=(%+v,%v)", run, err)
	}
	run.Completed = true
	run.ResolutionMode = sql.NullString{String: "evidence_partition", Valid: true}
	if err := UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	loaded, allowed, err := ReserveIntentPlanAttempt(ctx, db, run)
	if err != nil || allowed || !loaded.Completed || loaded.AttemptCount != 0 {
		t.Fatalf("completed zero-attempt run=(%+v,%t,%v)", loaded, allowed, err)
	}
}

func TestIntentPlanRunMigrationAddsResolvedPlan(t *testing.T) {
	ctx := context.Background()
	db, dbPath := openTestDB(t)
	if _, err := db.SQL().ExecContext(ctx,
		`ALTER TABLE intent_plan_runs DROP COLUMN resolved_plan_json`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `PRAGMA user_version = 22`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var count int
	if err := db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM pragma_table_info('intent_plan_runs')
WHERE name='resolved_plan_json'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("resolved_plan_json columns=%d want 1", count)
	}
}

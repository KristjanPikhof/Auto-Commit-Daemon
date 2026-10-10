package state

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"
)

func TestIntentProviderWaitSuspendsDeadlineWithoutResettingSemanticProgress(t *testing.T) {
	ctx := context.Background()
	db, path := openTestDB(t)
	run, allowed, err := ReserveIntentPlanAttempt(ctx, db, IntentPlanRun{
		Fingerprint: "retry-after-outage", BranchRef: "refs/heads/main",
		BranchGeneration: 1, AttemptLimit: 3,
	})
	if err != nil || !allowed {
		t.Fatalf("semantic attempt=%+v allowed=%t err=%v", run, allowed, err)
	}
	run, err = StartIntentProviderBudget(ctx, db, run, float64(time.Now().Add(time.Minute).Unix()))
	if err != nil {
		t.Fatal(err)
	}
	run.PreservedGroups = [][]int64{{1, 2}}
	run.UnresolvedSeqs = []int64{3}
	run.FindingCodes = []string{"candidate_dependency_invalid"}
	run.ResolvedPlanJSON = sql.NullString{String: `{"plan":{"protocol_version":"v2","candidates":[]}}`, Valid: true}
	run.ProviderDeadlineTS = 0
	run.ProgressState = sql.NullString{String: "waiting_for_ai", Valid: true}
	if err := UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	loaded, found, err := IntentPlanRunByFingerprint(ctx, db, run.Fingerprint)
	if err != nil || !found || loaded.ProviderDeadlineTS != 0 || loaded.AttemptCount != 1 || loaded.Completed ||
		!reflect.DeepEqual(loaded.PreservedGroups, run.PreservedGroups) ||
		!reflect.DeepEqual(loaded.UnresolvedSeqs, run.UnresolvedSeqs) ||
		!reflect.DeepEqual(loaded.FindingCodes, run.FindingCodes) || loaded.ResolvedPlanJSON != run.ResolvedPlanJSON {
		t.Fatalf("restart lost waiting progress: loaded=%+v want=%+v found=%t err=%v", loaded, run, found, err)
	}
	deadline := float64(time.Now().Add(5 * time.Minute).Unix())
	resumed, err := StartIntentProviderBudget(ctx, db, loaded, deadline)
	if err != nil || resumed.ProviderDeadlineTS != deadline || resumed.AttemptCount != 1 {
		t.Fatalf("reconnected cycle=%+v err=%v", resumed, err)
	}
	narrowed, err := StartIntentProviderBudget(ctx, db, resumed, deadline+3600)
	if err != nil || narrowed.ProviderDeadlineTS != deadline {
		t.Fatalf("semantic correction reset its active deadline: %+v err=%v", narrowed, err)
	}
	resumed, allowed, err = ReserveIntentPlanAttempt(ctx, db, resumed)
	if err != nil || !allowed || resumed.AttemptCount != 2 {
		t.Fatalf("outage reset semantic attempt cap: %+v allowed=%t err=%v", resumed, allowed, err)
	}
}

package state

import (
	"context"
	"strings"
	"testing"
)

func TestIntentHistoryPlanRemainsImmutableAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, path := openTestDB(t)
	plan, err := SaveIntentHistoryPlan(ctx, db, IntentHistoryPlan{SourceBranchRef: "refs/heads/main", TargetBranchRef: "refs/heads/goals", ExpectedHead: "frozen", Goals: []IntentHistoryGoal{{ID: "goal", Purpose: "complete outcome"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SaveIntentHistoryPlan(ctx, db, plan); err != nil {
		t.Fatal(err)
	}
	changed := plan
	changed.TargetBranchRef = "refs/heads/other"
	if _, err := SaveIntentHistoryPlan(ctx, db, changed); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("changed frozen plan: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	loaded, ok, err := LoadIntentHistoryPlan(ctx, readOnly, plan.ID)
	if err != nil || !ok || loaded.TargetBranchRef != plan.TargetBranchRef {
		t.Fatalf("read-only plan: %+v %v", loaded, err)
	}
}

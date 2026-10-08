package state

import (
	"context"
	"strings"
	"testing"
)

func TestIntentHistoryPlanRemainsImmutableAcrossQueueRestart(t *testing.T) {
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
	if err := EnqueueIntentHistoryRequest(ctx, db, plan.ID); err != nil {
		t.Fatal(err)
	}
	other, err := SaveIntentHistoryPlan(ctx, db, IntentHistoryPlan{TargetBranchRef: "refs/heads/other"})
	if err != nil {
		t.Fatal(err)
	}
	if err := EnqueueIntentHistoryRequest(ctx, db, other.ID); err == nil {
		t.Fatal("concurrent request overwrote pending request")
	}
	request, ok, err := LoadIntentHistoryRequest(ctx, db)
	if err != nil || !ok || request.PlanID != plan.ID || request.Status != "pending" {
		t.Fatalf("request=%+v ok=%v err=%v", request, ok, err)
	}
	request.Status = "running"
	if err := SaveIntentHistoryRequest(ctx, db, request); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := EnqueueIntentHistoryRequest(ctx, reopened, plan.ID); err != nil {
		t.Fatal(err)
	}
	request, _, err = LoadIntentHistoryRequest(ctx, reopened)
	if err != nil || request.Status != "running" {
		t.Fatalf("restart lost running request: %+v %v", request, err)
	}
	request.Status = "completed"
	if err := SaveIntentHistoryRequest(ctx, reopened, request); err != nil {
		t.Fatal(err)
	}
	if err := EnqueueIntentHistoryRequest(ctx, reopened, plan.ID); err != nil {
		t.Fatal(err)
	}
	request, _, _ = LoadIntentHistoryRequest(ctx, reopened)
	if request.Status != "completed" {
		t.Fatalf("completed request was requeued: %+v", request)
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

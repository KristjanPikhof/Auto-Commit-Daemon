package daemon

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentHistorySavedPlanRevalidatesGoalEvidenceAndVersions(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	a := mustCommitPath(t, f.dir, "a.txt", "alpha\n", "Add alpha")
	b := mustCommitPath(t, f.dir, "b.txt", "beta\n", "Add beta")
	correction := mustCommitPath(t, f.dir, "a.txt", "correct alpha\n", "Correct alpha")
	plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, []string{a, b, correction}, &historyAuditPlanner{}, ai.CommitFormatImperative, true)
	if err != nil {
		t.Fatal(err)
	}
	plan.TargetBranchRef = "refs/heads/goals"
	if _, err := ValidateIntentHistoryPlan(ctx, f.dir, plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Goals) != 2 || len(plan.Goals[0].Units) != 2 {
		t.Fatalf("lost interleaved correction: %+v", plan.Goals)
	}
	for _, scenario := range []struct {
		name   string
		change func(*state.IntentHistoryPlan)
	}{
		{"generic message", func(p *state.IntentHistoryPlan) { p.Goals[0].Message = "Update alpha code changes" }},
		{"invented version", func(p *state.IntentHistoryPlan) { p.Units[0].After.OID = p.Units[1].After.OID }},
		{"omitted capture", func(p *state.IntentHistoryPlan) { p.Goals[0].Units = p.Goals[0].Units[:1] }},
		{"changed goal tree", func(p *state.IntentHistoryPlan) { p.Goals[0].TreeOID = p.BaseTree }},
		{"changed base", func(p *state.IntentHistoryPlan) { p.BaseTree = p.Goals[1].TreeOID }},
		{"wrong frozen head", func(p *state.IntentHistoryPlan) { p.ExpectedHead = a }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			raw, _ := json.Marshal(plan)
			var changed state.IntentHistoryPlan
			_ = json.Unmarshal(raw, &changed)
			scenario.change(&changed)
			if _, err := ValidateIntentHistoryPlan(ctx, f.dir, changed); err == nil {
				t.Fatal("unsafe edited plan accepted")
			}
		})
	}
}

func TestIntentHistoryProviderOutageStopsAtOneCall(t *testing.T) {
	f := newCaptureFixture(t)
	a := mustCommitPath(t, f.dir, "a.txt", "alpha\n", "Add alpha")
	provider := &historyAuditPlanner{failure: &ai.ProviderHTTPError{StatusCode: 502, Detail: "temporary outage"}}
	if _, err := PlanIntentHistory(context.Background(), f.dir, f.cctx.BranchRef, []string{a}, provider, ai.CommitFormatImperative, true); err == nil {
		t.Fatal("provider outage accepted")
	}
	if provider.calls != 1 {
		t.Fatalf("transport failures consumed semantic retries: %d", provider.calls)
	}
}

func TestIntentHistoryDiffBudgetKeepsCompanionsComplete(t *testing.T) {
	raw := []string{strings.Repeat("large source evidence\n", 1000), strings.Repeat("caller correction\n", 20), strings.Repeat("required import\n", 15)}
	got := allocateIntentEvidenceDiffs(raw, 2048)
	if got[1] != raw[1] || got[2] != raw[2] {
		t.Fatal("small companion evidence was clipped equally with a large change")
	}
	total := 0
	for _, diff := range got {
		total += len(diff)
	}
	if total > 2048 {
		t.Fatalf("diff budget exceeded: %d", total)
	}
}

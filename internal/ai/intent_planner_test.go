package ai

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func sampleIntentPlanRequest(t *testing.T) IntentPlanRequest {
	t.Helper()
	now := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		LatestCommit: &CommitSummary{
			OID:       "abc123",
			Subject:   "Update checkout flow",
			Timestamp: now.Add(-time.Hour),
			Paths:     []string{"internal/checkout/service.go"},
		},
		PathCommitContext: []PathCommitContext{{
			Path: "internal/checkout/service.go",
			Commits: []CommitSummary{{
				OID:     "def456",
				Subject: "Refine checkout validation",
				Paths:   []string{"internal/checkout/service.go"},
			}},
		}},
		OfferedCaptures: []OfferedCapture{
			{
				Seq:          101,
				Path:         "internal/checkout/service.go",
				Op:           "modify",
				Timestamp:    now,
				Fidelity:     "full",
				DeferCount:   0,
				CapturedDiff: "+authorization: Bearer abcdefghij.klmnopqrst.uvwxyz123456\n",
			},
			{
				Seq:        102,
				Path:       "docs/checkout.md",
				Op:         "modify",
				Timestamp:  now.Add(time.Second),
				Fidelity:   "metadata",
				DeferCount: 2,
			},
		},
		IncludeCapturedDiffs: true,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}
	return req
}

func TestIntentPlanRequestIncludesCommitContextAndOfferedCaptures(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	var got struct {
		LatestCommit *CommitSummary `json:"latest_commit"`
		PathContext  []struct {
			Path    string          `json:"path"`
			Commits []CommitSummary `json:"commits"`
		} `json:"path_commit_context"`
		Offered []struct {
			Seq          int64  `json:"seq"`
			Path         string `json:"path"`
			Op           string `json:"op"`
			Timestamp    string `json:"timestamp"`
			Fidelity     string `json:"fidelity"`
			DeferCount   int    `json:"defer_count"`
			CapturedDiff string `json:"captured_diff"`
		} `json:"offered_captures"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if got.LatestCommit == nil || got.LatestCommit.Subject != "Update checkout flow" {
		t.Fatalf("latest commit missing: %+v", got.LatestCommit)
	}
	if len(got.PathContext) != 1 || got.PathContext[0].Path != "internal/checkout/service.go" {
		t.Fatalf("path context missing: %+v", got.PathContext)
	}
	if len(got.Offered) != 2 {
		t.Fatalf("offered captures=%d", len(got.Offered))
	}
	first := got.Offered[0]
	if first.Seq != 101 || first.Path == "" || first.Op == "" || first.Timestamp == "" || first.Fidelity == "" {
		t.Fatalf("first offered capture missing required fields: %+v", first)
	}
	if first.DeferCount != 0 {
		t.Fatalf("defer_count=%d", first.DeferCount)
	}
	if strings.Contains(first.CapturedDiff, "Bearer abcdefghij") {
		t.Fatalf("captured diff leaked secret: %q", first.CapturedDiff)
	}
	if !strings.Contains(first.CapturedDiff, redactedSecret) {
		t.Fatalf("captured diff missing redaction: %q", first.CapturedDiff)
	}
}

func TestIntentPlanRequestOmitsDiffsWhenEgressDisallowed(t *testing.T) {
	now := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		OfferedCaptures: []OfferedCapture{{
			Seq:          7,
			Path:         "secret.txt",
			Op:           "modify",
			Timestamp:    now,
			Fidelity:     "full",
			CapturedDiff: "+token=secret\n",
		}},
		IncludeCapturedDiffs: false,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}
	if req.OfferedCaptures[0].CapturedDiff != "" {
		t.Fatalf("captured diff should be omitted when egress disallowed: %q", req.OfferedCaptures[0].CapturedDiff)
	}
}

func TestIntentPlanRequestRejectsMultiCaptureForcedAgingWindow(t *testing.T) {
	now := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	_, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		ForcedAging: true,
		OfferedCaptures: []OfferedCapture{
			{Seq: 1, Path: "a.go", Op: "modify", Timestamp: now, Fidelity: "full"},
			{Seq: 2, Path: "b.go", Op: "modify", Timestamp: now, Fidelity: "full"},
		},
	})
	if err == nil {
		t.Fatalf("expected forced-aging multi-capture request to fail")
	}
}

func TestIntentPlannerPromptContainsPlannerRules(t *testing.T) {
	prompt := IntentPlannerSystemPrompt()
	required := []string{
		"select exactly one capture or any larger non-empty subset",
		"defer any offered capture",
		"return every offered seq as either selected or deferred",
		"Do not group unrelated captures",
		"Do not invent intent beyond the supplied evidence",
		"Forced-aging windows contain only the overdue capture",
	}
	for _, want := range required {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestValidateIntentPlanRejectsInvalidShapes(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	valid := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Update checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: "documentation change is separate",
		}},
	}
	if err := ValidateIntentPlan(req, valid); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}

	tests := []struct {
		name string
		mut  func(IntentPlan) IntentPlan
	}{
		{name: "empty selected", mut: func(p IntentPlan) IntentPlan {
			p.SelectedSeqs = nil
			return p
		}},
		{name: "unknown selected", mut: func(p IntentPlan) IntentPlan {
			p.SelectedSeqs = []int64{999}
			return p
		}},
		{name: "unknown deferred", mut: func(p IntentPlan) IntentPlan {
			p.DeferredSeqs = []int64{999}
			p.DeferredReasons = []DeferredReason{{Seq: 999, Reason: "unknown"}}
			return p
		}},
		{name: "overlap", mut: func(p IntentPlan) IntentPlan {
			p.DeferredSeqs = append(p.DeferredSeqs, 101)
			p.DeferredReasons = append(p.DeferredReasons, DeferredReason{Seq: 101, Reason: "overlap"})
			return p
		}},
		{name: "omission", mut: func(p IntentPlan) IntentPlan {
			p.DeferredSeqs = nil
			p.DeferredReasons = nil
			return p
		}},
		{name: "duplicate selected", mut: func(p IntentPlan) IntentPlan {
			p.SelectedSeqs = []int64{101, 101}
			return p
		}},
		{name: "duplicate deferred", mut: func(p IntentPlan) IntentPlan {
			p.DeferredSeqs = []int64{102, 102}
			p.DeferredReasons = []DeferredReason{{Seq: 102, Reason: "first"}, {Seq: 102, Reason: "second"}}
			return p
		}},
		{name: "missing deferred reason", mut: func(p IntentPlan) IntentPlan {
			p.DeferredReasons = nil
			return p
		}},
		{name: "empty subject", mut: func(p IntentPlan) IntentPlan {
			p.Subject = " "
			return p
		}},
		{name: "empty grouping reason", mut: func(p IntentPlan) IntentPlan {
			p.GroupingReason = " "
			return p
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateIntentPlan(req, tc.mut(valid)); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestDeterministicPlanIntentSelectsOneAndDefersRest(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	plan, err := (DeterministicProvider{}).PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if len(plan.SelectedSeqs) != 1 || plan.SelectedSeqs[0] != 101 {
		t.Fatalf("selected=%v", plan.SelectedSeqs)
	}
	if len(plan.DeferredSeqs) != 1 || plan.DeferredSeqs[0] != 102 {
		t.Fatalf("deferred=%v", plan.DeferredSeqs)
	}
	if len(plan.DeferredReasons) != 1 || plan.DeferredReasons[0].Reason == "" {
		t.Fatalf("deferred reasons=%+v", plan.DeferredReasons)
	}
	if plan.Source != "deterministic" {
		t.Fatalf("source=%q", plan.Source)
	}
}

type staticIntentPlannerProvider struct {
	name string
	plan IntentPlan
}

func (p staticIntentPlannerProvider) Name() string { return p.name }

func (p staticIntentPlannerProvider) Generate(context.Context, CommitContext) (Result, error) {
	return Result{}, nil
}

func (p staticIntentPlannerProvider) PlanIntent(context.Context, IntentPlanRequest) (IntentPlan, error) {
	plan := p.plan
	plan.Source = p.name
	return plan, nil
}

func TestComposedPlanIntentValidatesPrimaryBeforeAccepting(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	primary := staticIntentPlannerProvider{
		name: "bad-primary",
		plan: IntentPlan{
			SelectedSeqs:   []int64{101},
			Subject:        "Update checkout flow",
			GroupingReason: "omits the deferred seq",
		},
	}
	planner := Compose(primary, DeterministicProvider{})
	plan, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if plan.Source != "deterministic" {
		t.Fatalf("source=%q want deterministic fallback", plan.Source)
	}
	if len(plan.DeferredSeqs) != 1 || plan.DeferredSeqs[0] != 102 {
		t.Fatalf("deferred=%v", plan.DeferredSeqs)
	}
}

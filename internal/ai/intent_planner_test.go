package ai

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type badDeferredReasonFixture struct {
	OfferedCaptures []OfferedCapture `json:"offered_captures"`
	OpenAIResponse  json.RawMessage  `json:"openai_response"`
}

func loadBadDeferredReasonFixture(t *testing.T) badDeferredReasonFixture {
	t.Helper()
	path := filepath.Join("testdata", "intent_planner", "bad_deferred_reason.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fx badDeferredReasonFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(fx.OfferedCaptures) == 0 {
		t.Fatalf("fixture missing offered_captures")
	}
	if len(fx.OpenAIResponse) == 0 {
		t.Fatalf("fixture missing openai_response")
	}
	return fx
}

func badDeferredReasonRequest(t *testing.T, fx badDeferredReasonFixture) IntentPlanRequest {
	t.Helper()
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		OfferedCaptures: fx.OfferedCaptures,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}
	return req
}

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
		"Every deferred_reasons[i].seq must appear in deferred_seqs",
		"Worked example",
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

func TestNormalizeIntentPlanReasons(t *testing.T) {
	long := strings.Repeat("x", IntentReasonCap+20)
	plan := NormalizeIntentPlanReasons(IntentPlan{
		GroupingReason: " \tgroup\x00ing\nreason\x7f ",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: " \rdefer\x1breason\t ",
		}, {
			Seq:    103,
			Reason: long,
		}},
	})
	if plan.GroupingReason != "groupingreason" {
		t.Fatalf("grouping reason=%q", plan.GroupingReason)
	}
	if plan.DeferredReasons[0].Reason != "deferreason" {
		t.Fatalf("deferred reason=%q", plan.DeferredReasons[0].Reason)
	}
	if got := len([]rune(plan.DeferredReasons[1].Reason)); got != IntentReasonCap {
		t.Fatalf("bounded reason length=%d want %d", got, IntentReasonCap)
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
	err  error
}

func (p staticIntentPlannerProvider) Name() string { return p.name }

func (p staticIntentPlannerProvider) Generate(context.Context, CommitContext) (Result, error) {
	return Result{}, nil
}

func (p staticIntentPlannerProvider) PlanIntent(context.Context, IntentPlanRequest) (IntentPlan, error) {
	if p.err != nil {
		return IntentPlan{}, p.err
	}
	plan := p.plan
	plan.Source = p.name
	return plan, nil
}

func TestComposedPlanIntentReturnsPrimaryValidationError(t *testing.T) {
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
	_, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "omitted from selected/deferred") {
		t.Fatalf("error=%v", err)
	}
}

// TestBadDeferredReasonFixtureReproducesValidatorError pins the upstream
// planner bug captured in the Trekoon and Gitlab-Issues-Creator repo trace
// logs: the planner emits a deferred_reasons entry whose seq is a selected
// (not deferred) capture. parseIntentPlanToolCall accepts the JSON, but
// ValidateIntentPlan rejects it and the daemon falls back to deterministic
// one-item commits for the entire window. The fixture is the input to the
// provider-side normalization implemented later in this file.
func TestBadDeferredReasonFixtureReproducesValidatorError(t *testing.T) {
	fx := loadBadDeferredReasonFixture(t)
	req := badDeferredReasonRequest(t, fx)
	plan, err := parseIntentPlanToolCall(fx.OpenAIResponse)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if err := ValidateIntentPlan(req, plan); err == nil {
		t.Fatalf("expected validator to reject fixture plan, got nil")
	} else if !strings.Contains(err.Error(), "deferred reason references non-deferred seq") {
		t.Fatalf("unexpected validator error: %v", err)
	}
}

func TestComposedPlanIntentReturnsPrimaryProviderError(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	primaryErr := errors.New("primary unavailable")
	planner := Compose(staticIntentPlannerProvider{name: "bad-primary", err: primaryErr}, DeterministicProvider{})
	_, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if !errors.Is(err, primaryErr) {
		t.Fatalf("error=%v want %v", err, primaryErr)
	}
}

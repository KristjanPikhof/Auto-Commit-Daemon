package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// captureSlogDefault swaps the slog default logger for one that writes JSON
// records to a buffer and returns a function that returns those lines. Used
// by NormalizeIntentPlanDeferredReasons callers (provider.go, openai_compat.go)
// to assert the warn-line count.
func captureSlogDefault(t *testing.T) func() []string {
	t.Helper()
	prev := slog.Default()
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []string {
		raw := strings.TrimSpace(buf.String())
		if raw == "" {
			return nil
		}
		return strings.Split(raw, "\n")
	}
}

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
		"leave deferred_seqs and deferred_reasons empty",
		"Same-path causality",
		"never split a same-path chain",
		"Defer_count guidance",
		"defer_count >= 1",
		"selected_seqs and deferred_seqs MUST be disjoint",
		"union MUST equal offered_seqs",
		"Every deferred_reasons[i].seq must appear in deferred_seqs",
		"Worked example",
		"internal/checkout/service.go",
		"invalid plan",
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

func TestValidateIntentPlanReturnsTypedErrorForSelectedDeferredOverlap(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	plan := IntentPlan{
		SelectedSeqs:    []int64{101},
		DeferredSeqs:    []int64{101, 102},
		Subject:         "Update checkout flow",
		GroupingReason:  "single focused checkout change",
		DeferredReasons: []DeferredReason{{Seq: 101, Reason: "overlap"}, {Seq: 102, Reason: "docs"}},
	}
	err := ValidateIntentPlan(req, plan)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	var typed *IntentPlanValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("expected *IntentPlanValidationError, got %T: %v", err, err)
	}
	if typed.Code != IntentPlanValidationSelectedDeferredOverlap {
		t.Fatalf("code=%v want IntentPlanValidationSelectedDeferredOverlap", typed.Code)
	}
	if typed.Seq != 101 {
		t.Fatalf("seq=%d want 101", typed.Seq)
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

func TestNormalizeIntentPlanDeferredReasonsDropsNonDeferredEntries(t *testing.T) {
	plan := IntentPlan{
		SelectedSeqs: []int64{10, 11},
		DeferredSeqs: []int64{12},
		DeferredReasons: []DeferredReason{
			{Seq: 12, Reason: "docs change is independent"},
			{Seq: 11, Reason: "spurious entry for selected seq"},
			{Seq: 99, Reason: "spurious entry for unknown seq"},
		},
	}
	cleaned, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) != 2 || dropped[0] != 11 || dropped[1] != 99 {
		t.Fatalf("dropped=%v want [11 99]", dropped)
	}
	if len(synthesized) != 0 {
		t.Fatalf("synthesized=%v want empty", synthesized)
	}
	if len(overlapRemoved) != 0 {
		t.Fatalf("overlapRemoved=%v want empty", overlapRemoved)
	}
	if len(cleaned.DeferredReasons) != 1 || cleaned.DeferredReasons[0].Seq != 12 {
		t.Fatalf("cleaned reasons=%+v", cleaned.DeferredReasons)
	}
}

func TestNormalizeIntentPlanDeferredReasonsPreservesSelectedDeferredOverlap(t *testing.T) {
	plan := IntentPlan{
		SelectedSeqs:   []int64{10, 11},
		DeferredSeqs:   []int64{11, 12},
		Subject:        "Update checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{
			{Seq: 11, Reason: "overlap"},
			{Seq: 12, Reason: "valid defer"},
		},
	}
	cleaned, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(overlapRemoved) != 0 {
		t.Fatalf("overlapRemoved=%v want empty", overlapRemoved)
	}
	if len(dropped) != 0 {
		t.Fatalf("dropped=%v want empty", dropped)
	}
	if len(synthesized) != 0 {
		t.Fatalf("synthesized=%v want empty", synthesized)
	}
	if len(cleaned.DeferredSeqs) != 2 || cleaned.DeferredSeqs[0] != 11 || cleaned.DeferredSeqs[1] != 12 {
		t.Fatalf("deferred seqs=%v want [11 12]", cleaned.DeferredSeqs)
	}
	if len(cleaned.DeferredReasons) != 2 || cleaned.DeferredReasons[0].Seq != 11 || cleaned.DeferredReasons[1].Seq != 12 {
		t.Fatalf("deferred reasons=%+v want entries for 11 and 12", cleaned.DeferredReasons)
	}

	req := sampleIntentPlanRequest(t)
	req.OfferedCaptures[0].Seq = 10
	req.OfferedCaptures[1].Seq = 11
	req.OfferedCaptures = append(req.OfferedCaptures, OfferedCapture{
		Seq:       12,
		Path:      "docs/checkout.md",
		Op:        "modify",
		Timestamp: req.OfferedCaptures[1].Timestamp,
		Fidelity:  "full",
	})
	err := ValidateIntentPlan(req, cleaned)
	if err == nil {
		t.Fatalf("expected overlap validation error after normalization")
	}
	var typed *IntentPlanValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("error=%T %v want *IntentPlanValidationError", err, err)
	}
	if typed.Code != IntentPlanValidationSelectedDeferredOverlap || typed.Seq != 11 {
		t.Fatalf("typed error code=%v seq=%d want overlap seq 11", typed.Code, typed.Seq)
	}
}

func TestNormalizeIntentPlanDeferredReasonsNoOpWhenAllValid(t *testing.T) {
	plan := IntentPlan{
		DeferredSeqs:    []int64{12, 13},
		DeferredReasons: []DeferredReason{{Seq: 12, Reason: "a"}, {Seq: 13, Reason: "b"}},
	}
	cleaned, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) != 0 {
		t.Fatalf("dropped=%v want empty", dropped)
	}
	if len(synthesized) != 0 {
		t.Fatalf("synthesized=%v want empty", synthesized)
	}
	if len(overlapRemoved) != 0 {
		t.Fatalf("overlapRemoved=%v want empty", overlapRemoved)
	}
	if len(cleaned.DeferredReasons) != 2 {
		t.Fatalf("cleaned reasons=%+v", cleaned.DeferredReasons)
	}
}

func TestNormalizeIntentPlanDeferredReasonsEmptyPlanEarlyReturn(t *testing.T) {
	// No deferred seqs and no reasons -> nothing to do; early return keeps
	// aliasing on caller's nil slice.
	plan := IntentPlan{}
	cleaned, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if dropped != nil {
		t.Fatalf("dropped=%v want nil", dropped)
	}
	if synthesized != nil {
		t.Fatalf("synthesized=%v want nil", synthesized)
	}
	if overlapRemoved != nil {
		t.Fatalf("overlapRemoved=%v want nil", overlapRemoved)
	}
	if cleaned.DeferredReasons != nil {
		t.Fatalf("cleaned reasons=%+v want nil", cleaned.DeferredReasons)
	}
}

func TestNormalizeIntentPlanDeferredReasonsPreservesInputOrder(t *testing.T) {
	plan := IntentPlan{
		DeferredSeqs: []int64{12, 13},
		DeferredReasons: []DeferredReason{
			{Seq: 12, Reason: "valid"},
			{Seq: 99, Reason: "drop first"},
			{Seq: 13, Reason: "valid"},
			{Seq: 11, Reason: "drop second"},
		},
	}
	cleaned, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) != 2 || dropped[0] != 99 || dropped[1] != 11 {
		t.Fatalf("dropped=%v want [99 11]", dropped)
	}
	if len(synthesized) != 0 {
		t.Fatalf("synthesized=%v want empty", synthesized)
	}
	if len(overlapRemoved) != 0 {
		t.Fatalf("overlapRemoved=%v want empty", overlapRemoved)
	}
	if len(cleaned.DeferredReasons) != 2 || cleaned.DeferredReasons[0].Seq != 12 || cleaned.DeferredReasons[1].Seq != 13 {
		t.Fatalf("cleaned=%+v", cleaned.DeferredReasons)
	}
}

func TestValidateIntentPlanReturnsTypedErrorForDeferredReasonNotDeferred(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	plan := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Update checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{
			{Seq: 102, Reason: "documentation change is separate"},
			{Seq: 101, Reason: "spurious entry referencing selected seq"},
		},
	}
	err := ValidateIntentPlan(req, plan)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	var typed *IntentPlanValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("expected *IntentPlanValidationError, got %T: %v", err, err)
	}
	if typed.Code != IntentPlanValidationDeferredReasonNotDeferred {
		t.Fatalf("code=%v want IntentPlanValidationDeferredReasonNotDeferred", typed.Code)
	}
	if typed.Seq != 101 {
		t.Fatalf("seq=%d want 101", typed.Seq)
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

// scriptedIntentPlanner returns successive plans/errors from a script. Each
// PlanIntent call advances the script index. The captured RetryCorrection
// from the inbound request is recorded so tests can assert that the retry
// hand-off includes the verbatim validator message.
type scriptedIntentPlanner struct {
	name        string
	plans       []IntentPlan
	errs        []error
	calls       int
	corrections []string
	// requests captures every IntentPlanRequest the composed layer hands
	// the planner so tests can assert hint propagation across retries and
	// fallback paths. Retain insertion order; never mutated by PlanIntent.
	requests []IntentPlanRequest
}

func (p *scriptedIntentPlanner) Name() string { return p.name }

func (p *scriptedIntentPlanner) Generate(context.Context, CommitContext) (Result, error) {
	return Result{}, nil
}

func (p *scriptedIntentPlanner) PlanIntent(_ context.Context, req IntentPlanRequest) (IntentPlan, error) {
	idx := p.calls
	p.calls++
	p.corrections = append(p.corrections, req.RetryCorrection)
	p.requests = append(p.requests, req)
	if idx < len(p.errs) && p.errs[idx] != nil {
		return IntentPlan{}, p.errs[idx]
	}
	if idx >= len(p.plans) {
		return IntentPlan{}, errors.New("scriptedIntentPlanner: out of scripted plans")
	}
	plan := p.plans[idx]
	plan.Source = p.name
	return plan, nil
}

// TestComposedPlanIntentRetriesOnTypedValidationError covers the happy retry
// path: the primary returns an invalid plan that fails ValidateIntentPlan
// with a typed *IntentPlanValidationError, the composed retry quotes the
// error verbatim into RetryCorrection, and the second attempt returns a
// valid plan. The composed call succeeds with the corrected plan; no
// fallback to deterministic is triggered.
//
// Uses a planner-hallucination case (selected seq outside offered window)
// because the missing-deferred-reason case is now silently coerced by
// NormalizeIntentPlanDeferredReasons and never surfaces to the retry path.
func TestComposedPlanIntentRetriesOnTypedValidationError(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	invalidPlan := IntentPlan{
		// Selected seq 999 is not in the offered window -> typed
		// IntentPlanValidationOfferedWindow error that synth/drop
		// normalization cannot fix.
		SelectedSeqs:   []int64{999},
		DeferredSeqs:   []int64{102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: "documentation change is separate",
		}},
	}
	validPlan := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: "documentation change is separate",
		}},
	}
	primary := &scriptedIntentPlanner{
		name:  "scripted-primary",
		plans: []IntentPlan{invalidPlan, validPlan},
	}
	planner := Compose(primary, DeterministicProvider{})
	plan, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if primary.calls != 2 {
		t.Fatalf("primary calls=%d want 2", primary.calls)
	}
	if plan.Source != "scripted-primary" {
		t.Fatalf("source=%q want scripted-primary (no deterministic fallback expected)", plan.Source)
	}
	if len(plan.SelectedSeqs) != 1 || plan.SelectedSeqs[0] != 101 {
		t.Fatalf("selected=%v want [101]", plan.SelectedSeqs)
	}
	if len(plan.DeferredReasons) != 1 || plan.DeferredReasons[0].Seq != 102 {
		t.Fatalf("deferred reasons=%+v", plan.DeferredReasons)
	}
	// First attempt sees no correction; the second attempt receives the
	// verbatim validator message.
	if got := primary.corrections[0]; got != "" {
		t.Fatalf("first attempt RetryCorrection=%q want empty", got)
	}
	correction := primary.corrections[1]
	if correction == "" {
		t.Fatalf("second attempt missing RetryCorrection")
	}
	if !strings.Contains(correction, "selected seq 999 outside offered window") {
		t.Fatalf("RetryCorrection=%q does not quote validator", correction)
	}
}

func TestComposedPlanIntentRetriesOnSelectedDeferredOverlap(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	invalidPlan := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{101, 102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{
			{Seq: 101, Reason: "contradictory overlap"},
			{Seq: 102, Reason: "documentation change is separate"},
		},
	}
	validPlan := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: "documentation change is separate",
		}},
	}
	primary := &scriptedIntentPlanner{
		name:  "scripted-primary",
		plans: []IntentPlan{invalidPlan, validPlan},
	}
	planner := Compose(primary, DeterministicProvider{})
	plan, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if primary.calls != 2 {
		t.Fatalf("primary calls=%d want 2", primary.calls)
	}
	if len(plan.DeferredSeqs) != 1 || plan.DeferredSeqs[0] != 102 {
		t.Fatalf("deferred=%v want [102]", plan.DeferredSeqs)
	}
	correction := primary.corrections[1]
	if !strings.Contains(correction, "seq 101 appears in selected and deferred") {
		t.Fatalf("RetryCorrection=%q does not quote overlap validator", correction)
	}
}

// TestComposedPlanIntentRetryGivesUpAfterSecondInvalid pins the retry cap at
// one. If the second attempt is also invalid, the composed planner returns
// the validation error so replay records intent_planner_error and runs its
// deterministic fallback.
func TestComposedPlanIntentRetryGivesUpAfterSecondInvalid(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	invalidPlan := IntentPlan{
		// Selected seq 999 is not in the offered window; the planner
		// repeats the same hallucination on both attempts so retry
		// caps at 1 and returns the typed validation error.
		SelectedSeqs:   []int64{999},
		DeferredSeqs:   []int64{102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: "documentation change is separate",
		}},
	}
	primary := &scriptedIntentPlanner{
		name:  "scripted-primary",
		plans: []IntentPlan{invalidPlan, invalidPlan},
	}
	planner := Compose(primary, DeterministicProvider{})
	_, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err == nil {
		t.Fatalf("expected validation error after second attempt")
	}
	if primary.calls != 2 {
		t.Fatalf("primary calls=%d want exactly 2 (cap retries at 1)", primary.calls)
	}
	var typed *IntentPlanValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("error %v not *IntentPlanValidationError", err)
	}
	if typed.Code != IntentPlanValidationOfferedWindow {
		t.Fatalf("code=%v want OfferedWindow", typed.Code)
	}
}

// TestComposedPlanIntentSkipsRetryOnTransportError ensures transport errors
// (timeouts, network failures, HTTP errors) bypass the retry loop entirely
// — they fall through immediately so the daemon's deterministic fallback
// path runs without a wasted round-trip.
func TestComposedPlanIntentSkipsRetryOnTransportError(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	// Two consecutive timeout-shaped errors: if the retry loop fired we
	// would see two calls. The cap-at-1 + skip-transport rule means we
	// must see exactly one call.
	timeoutErr := context.DeadlineExceeded
	primary := &scriptedIntentPlanner{
		name: "scripted-primary",
		errs: []error{timeoutErr, timeoutErr},
	}
	planner := Compose(primary, DeterministicProvider{})
	_, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if !errors.Is(err, timeoutErr) {
		t.Fatalf("error=%v want context.DeadlineExceeded", err)
	}
	if primary.calls != 1 {
		t.Fatalf("primary calls=%d want 1 (transport errors skip retry)", primary.calls)
	}
}

// TestComposedRetry_SkipsForHealedCodes pins the contract that typed
// validation errors whose codes are normally healed by
// NormalizeIntentPlanDeferredReasons (DeferredReasonNotDeferred,
// DeferredReasonMissing) skip the retry round-trip when validation still
// fails after normalization. A second call would burn provider budget
// without changing the outcome because the planner is in a worse state
// than the error code suggests.
func TestComposedRetry_SkipsForHealedCodes(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	codes := []IntentPlanValidationCode{
		IntentPlanValidationDeferredReasonNotDeferred,
		IntentPlanValidationDeferredReasonMissing,
	}
	for _, code := range codes {
		t.Run(fmt.Sprintf("code-%d", code), func(t *testing.T) {
			primary := &scriptedIntentPlanner{
				name: "scripted-primary",
				errs: []error{
					&IntentPlanValidationError{Code: code, Seq: 102, Message: "synthetic"},
					nil,
				},
				plans: []IntentPlan{
					{},
					{SelectedSeqs: []int64{101}, DeferredSeqs: []int64{102},
						Subject: "x", GroupingReason: "y",
						DeferredReasons: []DeferredReason{{Seq: 102, Reason: "ok"}}},
				},
			}
			planner := Compose(primary, DeterministicProvider{})
			_, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
			if err == nil {
				t.Fatalf("expected typed validation error to surface")
			}
			if primary.calls != 1 {
				t.Fatalf("primary calls=%d want 1 (healed-code retry must be skipped)", primary.calls)
			}
		})
	}
}

// TestComposedRetry_DisabledByEnv verifies ACD_INTENT_RETRY_ON_INVALID=0
// disables the retry loop entirely; even a typically-retryable code
// (OfferedWindow hallucination) collapses to a single primary call.
func TestComposedRetry_DisabledByEnv(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	t.Setenv("ACD_INTENT_RETRY_ON_INVALID", "0")
	invalidPlan := IntentPlan{
		SelectedSeqs:   []int64{999},
		DeferredSeqs:   []int64{102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: "documentation change is separate",
		}},
	}
	primary := &scriptedIntentPlanner{
		name:  "scripted-primary",
		plans: []IntentPlan{invalidPlan, invalidPlan},
	}
	planner := Compose(primary, DeterministicProvider{})
	_, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err == nil {
		t.Fatalf("expected validation error after primary attempt")
	}
	if primary.calls != 1 {
		t.Fatalf("primary calls=%d want 1 (env opt-out must skip retry)", primary.calls)
	}
}

// TestComposedPlanIntentSkipsRetryOnUntypedError ensures plain (non-typed)
// validation errors do not trigger a retry. Only *IntentPlanValidationError
// qualifies — this protects against future planner errors that surface as
// fmt.Errorf strings.
func TestComposedPlanIntentSkipsRetryOnUntypedError(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	primary := &scriptedIntentPlanner{
		name: "scripted-primary",
		errs: []error{errors.New("openai-compat: http 500: upstream"), nil},
		plans: []IntentPlan{
			{}, // unused
			{SelectedSeqs: []int64{101}, DeferredSeqs: []int64{102}, Subject: "x", GroupingReason: "y", DeferredReasons: []DeferredReason{{Seq: 102, Reason: "ok"}}},
		},
	}
	planner := Compose(primary, DeterministicProvider{})
	_, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err == nil {
		t.Fatalf("expected primary error")
	}
	if primary.calls != 1 {
		t.Fatalf("primary calls=%d want 1 (untyped error skips retry)", primary.calls)
	}
}

// TestBuildIntentPlanUserPromptCarriesAboveDiffCapInOpsDiff verifies that
// per-capture diffs threaded through NewIntentPlanRequest can land in the
// user prompt at sizes above the per-event ai.DiffCap (4 KiB), bounded by
// IntentStageDiffCap. The previous implementation hard-wired the planner
// stage to ai.DiffCap so a single large captured diff was silently
// truncated to 4 KiB before the planner ever saw it.
func TestBuildIntentPlanUserPromptCarriesAboveDiffCapInOpsDiff(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	// 12 KiB synthetic captured diff — comfortably above DiffCap and below
	// IntentStageDiffCap so the threading is observable in the prompt.
	bigDiff := "@@ -1,1 +1,1200 @@\n" + strings.Repeat("+payload-line\n", 900)
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		OfferedCaptures: []OfferedCapture{{
			Seq:          7,
			Path:         "src/big.go",
			Op:           "modify",
			Timestamp:    now,
			Fidelity:     "full",
			CapturedDiff: bigDiff,
		}},
		IncludeCapturedDiffs: true,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}
	if got := len(req.OfferedCaptures[0].CapturedDiff); got <= DiffCap {
		t.Fatalf("CapturedDiff len=%d <= DiffCap=%d; intent-stage cap not applied", got, DiffCap)
	}
	if got := len(req.OfferedCaptures[0].CapturedDiff); got > IntentStageDiffCap {
		t.Fatalf("CapturedDiff len=%d > IntentStageDiffCap=%d; cap exceeded", got, IntentStageDiffCap)
	}
	prompt, err := BuildIntentPlanUserPrompt(req)
	if err != nil {
		t.Fatalf("BuildIntentPlanUserPrompt: %v", err)
	}
	if len(prompt) <= DiffCap {
		t.Fatalf("user prompt len=%d <= DiffCap=%d; large captured diff was truncated", len(prompt), DiffCap)
	}
	if !strings.Contains(prompt, "payload-line") {
		t.Fatalf("user prompt missing payload-line substring; threading dropped diff body")
	}
}

// TestBuildIntentPlanUserPromptIncludesRetryCorrection verifies the user
// prompt builder appends the correction block when RetryCorrection is set,
// and omits it on first-attempt requests.
func TestBuildIntentPlanUserPromptIncludesRetryCorrection(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	first, err := BuildIntentPlanUserPrompt(req)
	if err != nil {
		t.Fatalf("first BuildIntentPlanUserPrompt: %v", err)
	}
	if strings.Contains(first, "Your previous capture_intent_plan") {
		t.Fatalf("first attempt prompt unexpectedly carries correction block:\n%s", first)
	}

	req.RetryCorrection = "intent planner: deferred seq 102 missing reason"
	second, err := BuildIntentPlanUserPrompt(req)
	if err != nil {
		t.Fatalf("retry BuildIntentPlanUserPrompt: %v", err)
	}
	if !strings.Contains(second, "Your previous capture_intent_plan tool call failed validation") {
		t.Fatalf("retry prompt missing correction header:\n%s", second)
	}
	if !strings.Contains(second, "intent planner: deferred seq 102 missing reason") {
		t.Fatalf("retry prompt missing verbatim validator message:\n%s", second)
	}
	if !strings.Contains(second, "Return a corrected capture_intent_plan tool call") {
		t.Fatalf("retry prompt missing corrective instruction:\n%s", second)
	}
	if !strings.Contains(second, "selected_seqs and deferred_seqs are disjoint") {
		t.Fatalf("retry prompt missing disjoint seq instruction:\n%s", second)
	}
}

// TestNormalizeIntentPlanDeferredReasonsSynthesizesMissingEntries covers the
// two-missing-seqs synth path: deferred_seqs has two entries, deferred_reasons
// is empty, normalize synthesizes both with the marker text and reports them
// in DeferredSeqs order.
func TestNormalizeIntentPlanDeferredReasonsSynthesizesMissingEntries(t *testing.T) {
	plan := IntentPlan{
		SelectedSeqs: []int64{10},
		DeferredSeqs: []int64{11, 12},
		// DeferredReasons intentionally empty.
	}
	cleaned, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) != 0 {
		t.Fatalf("dropped=%v want empty", dropped)
	}
	if len(synthesized) != 2 || synthesized[0] != 11 || synthesized[1] != 12 {
		t.Fatalf("synthesized=%v want [11 12]", synthesized)
	}
	if len(overlapRemoved) != 0 {
		t.Fatalf("overlapRemoved=%v want empty", overlapRemoved)
	}
	if len(cleaned.DeferredReasons) != 2 {
		t.Fatalf("cleaned reasons len=%d want 2", len(cleaned.DeferredReasons))
	}
	for i, want := range []int64{11, 12} {
		if cleaned.DeferredReasons[i].Seq != want {
			t.Fatalf("cleaned[%d].Seq=%d want %d", i, cleaned.DeferredReasons[i].Seq, want)
		}
		if cleaned.DeferredReasons[i].Reason != IntentPlanReasonMarker {
			t.Fatalf("cleaned[%d].Reason=%q want %q", i, cleaned.DeferredReasons[i].Reason, IntentPlanReasonMarker)
		}
	}
}

// TestNormalizeIntentPlanDeferredReasonsMixedDropAndSynth covers the case
// where the planner emits one spurious entry (drop) and omits a real one
// (synth); both lists are populated and the cleaned plan reflects the fix.
func TestNormalizeIntentPlanDeferredReasonsMixedDropAndSynth(t *testing.T) {
	plan := IntentPlan{
		SelectedSeqs: []int64{10},
		DeferredSeqs: []int64{11, 12},
		DeferredReasons: []DeferredReason{
			{Seq: 11, Reason: "docs change is independent"},
			{Seq: 99, Reason: "spurious entry"},
		},
	}
	cleaned, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) != 1 || dropped[0] != 99 {
		t.Fatalf("dropped=%v want [99]", dropped)
	}
	if len(synthesized) != 1 || synthesized[0] != 12 {
		t.Fatalf("synthesized=%v want [12]", synthesized)
	}
	if len(overlapRemoved) != 0 {
		t.Fatalf("overlapRemoved=%v want empty", overlapRemoved)
	}
	// Cleaned should preserve the original valid entry then append the
	// synthesized marker entry.
	if len(cleaned.DeferredReasons) != 2 {
		t.Fatalf("cleaned reasons=%+v", cleaned.DeferredReasons)
	}
	if cleaned.DeferredReasons[0].Seq != 11 || cleaned.DeferredReasons[0].Reason != "docs change is independent" {
		t.Fatalf("cleaned[0]=%+v want preserved entry for 11", cleaned.DeferredReasons[0])
	}
	if cleaned.DeferredReasons[1].Seq != 12 || cleaned.DeferredReasons[1].Reason != IntentPlanReasonMarker {
		t.Fatalf("cleaned[1]=%+v want synthesized marker for 12", cleaned.DeferredReasons[1])
	}
}

// TestNormalizeIntentPlanDeferredReasonsIdempotentOnSecondCall verifies the
// double-invocation property required by the defense-in-depth pass in
// planIntentWithFallback: after one normalization, a second call must
// return empty dropped/synthesized so we never duplicate the slog.Warn.
func TestNormalizeIntentPlanDeferredReasonsIdempotentOnSecondCall(t *testing.T) {
	plan := IntentPlan{
		SelectedSeqs: []int64{10},
		DeferredSeqs: []int64{11, 12},
		DeferredReasons: []DeferredReason{
			{Seq: 99, Reason: "spurious"},
		},
	}
	cleaned1, dropped1, synthesized1, overlapRemoved1 := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped1) != 1 || len(synthesized1) != 2 {
		t.Fatalf("first pass dropped=%v synthesized=%v", dropped1, synthesized1)
	}
	if len(overlapRemoved1) != 0 {
		t.Fatalf("first pass overlapRemoved=%v want empty", overlapRemoved1)
	}
	cleaned2, dropped2, synthesized2, overlapRemoved2 := NormalizeIntentPlanDeferredReasons(cleaned1)
	if dropped2 != nil {
		t.Fatalf("second pass dropped=%v want nil", dropped2)
	}
	if synthesized2 != nil {
		t.Fatalf("second pass synthesized=%v want nil", synthesized2)
	}
	if overlapRemoved2 != nil {
		t.Fatalf("second pass overlapRemoved=%v want nil", overlapRemoved2)
	}
	if len(cleaned2.DeferredReasons) != len(cleaned1.DeferredReasons) {
		t.Fatalf("second pass altered reasons: %+v vs %+v", cleaned1.DeferredReasons, cleaned2.DeferredReasons)
	}
}

// TestComposedPlanIntentMixedNormalizeEmitsSingleWarn drives a mixed
// drop+synth response through Compose(scriptedPrimary, deterministic) and
// asserts the composed layer emits exactly one warn line naming both the
// dropped and synthesized seqs.
func TestComposedPlanIntentMixedNormalizeEmitsSingleWarn(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	plan := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{
			// Drop: seq 101 is selected, not deferred.
			{Seq: 101, Reason: "spurious"},
			// (No entry for 102 so synth fires too.)
		},
	}
	primary := &scriptedIntentPlanner{
		name:  "scripted-primary",
		plans: []IntentPlan{plan},
	}
	logs := captureSlogDefault(t)
	planner := Compose(primary, DeterministicProvider{})
	got, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if primary.calls != 1 {
		t.Fatalf("primary calls=%d want 1 (no retry needed after normalize)", primary.calls)
	}
	if len(got.DeferredReasons) != 1 || got.DeferredReasons[0].Seq != 102 || got.DeferredReasons[0].Reason != IntentPlanReasonMarker {
		t.Fatalf("normalized plan reasons=%+v want one synth marker for 102", got.DeferredReasons)
	}
	lines := logs()
	warns := 0
	for _, line := range lines {
		if strings.Contains(line, "intent planner: normalized deferred_reasons") {
			warns++
			if !strings.Contains(line, "dropped_seqs") {
				t.Fatalf("warn line missing dropped_seqs: %s", line)
			}
			if !strings.Contains(line, "synthesized_seqs") {
				t.Fatalf("warn line missing synthesized_seqs: %s", line)
			}
		}
	}
	if warns != 1 {
		t.Fatalf("normalize warns=%d want exactly 1 per response\nlogs:\n%s", warns, strings.Join(lines, "\n"))
	}
}

// TestComposedPlanIntentNormalizeStaysSilentOnCleanPlan asserts the no-op
// path: when the planner output already validates, neither the provider-side
// normalize nor the composed-layer normalize emits a warn.
func TestComposedPlanIntentNormalizeStaysSilentOnCleanPlan(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	clean := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		DeferredReasons: []DeferredReason{
			{Seq: 102, Reason: "documentation change is separate"},
		},
	}
	primary := &scriptedIntentPlanner{
		name:  "scripted-primary",
		plans: []IntentPlan{clean},
	}
	logs := captureSlogDefault(t)
	planner := Compose(primary, DeterministicProvider{})
	if _, err := planner.(IntentPlanner).PlanIntent(context.Background(), req); err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	for _, line := range logs() {
		if strings.Contains(line, "intent planner: normalized deferred_reasons") {
			t.Fatalf("unexpected normalize warn for clean plan: %s", line)
		}
	}
}

// TestNormalizeIntentPlanDeferredReasonsSynthEmptyReasons exercises the
// minimal synth path: deferred_seqs=[X], deferred_reasons empty, the planner
// supplied a perfectly valid plan minus the reason. After normalize, the
// plan validates without any drop.
func TestNormalizeIntentPlanDeferredReasonsSynthEmptyReasons(t *testing.T) {
	plan := IntentPlan{
		SelectedSeqs: []int64{10},
		DeferredSeqs: []int64{11},
	}
	cleaned, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) != 0 {
		t.Fatalf("dropped=%v want empty", dropped)
	}
	if len(synthesized) != 1 || synthesized[0] != 11 {
		t.Fatalf("synthesized=%v want [11]", synthesized)
	}
	if len(overlapRemoved) != 0 {
		t.Fatalf("overlapRemoved=%v want empty", overlapRemoved)
	}
	if len(cleaned.DeferredReasons) != 1 || cleaned.DeferredReasons[0].Reason != IntentPlanReasonMarker {
		t.Fatalf("cleaned=%+v want one synth marker entry", cleaned.DeferredReasons)
	}
}

// TestIntentPlanRequestPathRecentCommitsSerializesAsJSONHint asserts the
// daemon-supplied prior-commit affinity hint round-trips through the
// IntentPlanRequest user-message JSON the planner sees on the wire. The
// field is only present when the daemon decided the offered path matched a
// recent HEAD commit; absent values omit the JSON key entirely.
func TestIntentPlanRequestPathRecentCommitsSerializesAsJSONHint(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	hint := []PathRecentCommit{{
		Path:            "internal/checkout/service.go",
		OID:             "abcdef0123456789abcdef0123456789abcdef01",
		AgeSeconds:      45,
		SuggestedAction: PathRecentCommitSuggestionExtendOrWait,
	}}
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		OfferedCaptures: []OfferedCapture{{
			Seq:       7,
			Path:      "internal/checkout/service.go",
			Op:        "modify",
			Timestamp: now,
			Fidelity:  "full",
		}},
		PathRecentCommits: hint,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}
	if len(req.PathRecentCommits) != 1 {
		t.Fatalf("PathRecentCommits len=%d want 1", len(req.PathRecentCommits))
	}
	prompt, err := BuildIntentPlanUserPrompt(req)
	if err != nil {
		t.Fatalf("BuildIntentPlanUserPrompt: %v", err)
	}
	if !strings.Contains(prompt, `"path_recent_commits":`) {
		t.Fatalf("user prompt missing path_recent_commits key:\n%s", prompt)
	}
	if !strings.Contains(prompt, `"oid":"abcdef0123456789abcdef0123456789abcdef01"`) {
		t.Fatalf("user prompt missing OID:\n%s", prompt)
	}
	if !strings.Contains(prompt, `"age_seconds":45`) {
		t.Fatalf("user prompt missing age_seconds:\n%s", prompt)
	}
	if !strings.Contains(prompt, `"suggested_action":"extend or wait"`) {
		t.Fatalf("user prompt missing suggested_action:\n%s", prompt)
	}
}

// TestIntentPlanRequestPathRecentCommitsAbsentByDefault confirms the hint
// disappears from the user prompt when the daemon does not supply it. Used
// by the env=0 regression baseline so existing planner deployments do not
// suddenly pay the cost of the hint when the affinity window is disabled.
func TestIntentPlanRequestPathRecentCommitsAbsentByDefault(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		OfferedCaptures: []OfferedCapture{{
			Seq:       1,
			Path:      "a.go",
			Op:        "modify",
			Timestamp: now,
			Fidelity:  "full",
		}},
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}
	if req.PathRecentCommits != nil {
		t.Fatalf("PathRecentCommits=%+v want nil when not supplied", req.PathRecentCommits)
	}
	prompt, err := BuildIntentPlanUserPrompt(req)
	if err != nil {
		t.Fatalf("BuildIntentPlanUserPrompt: %v", err)
	}
	if strings.Contains(prompt, "path_recent_commits") {
		t.Fatalf("user prompt unexpectedly carries path_recent_commits:\n%s", prompt)
	}
}

// TestIntentPlanRequestPathRecentCommitsClonesInput protects callers that
// reuse the slice across NewIntentPlanRequest invocations. Mutating the
// input afterwards must not leak into the request.
func TestIntentPlanRequestPathRecentCommitsClonesInput(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	hint := []PathRecentCommit{{
		Path:            "a.go",
		OID:             "1111111111111111111111111111111111111111",
		AgeSeconds:      10,
		SuggestedAction: PathRecentCommitSuggestionExtendOrWait,
	}}
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		OfferedCaptures: []OfferedCapture{{
			Seq: 1, Path: "a.go", Op: "modify", Timestamp: now, Fidelity: "full",
		}},
		PathRecentCommits: hint,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}
	hint[0].Path = "MUTATED"
	if req.PathRecentCommits[0].Path != "a.go" {
		t.Fatalf("clone failed: req.PathRecentCommits[0].Path=%q want a.go", req.PathRecentCommits[0].Path)
	}
}

// TestComposedPlanIntentForwardsPathRecentCommitsToPrimaryAndFallback drives
// a request through Compose(primary, deterministic) with a
// PathRecentCommits hint, primary returns an invalid plan (so deterministic
// fallback runs), and asserts BOTH calls observed the hint. Wave 2's
// retry-on-typed-error path must keep the affinity hint attached so the
// planner sees consistent context across primary, retry, and fallback.
func TestComposedPlanIntentForwardsPathRecentCommitsToPrimaryAndFallback(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	hint := []PathRecentCommit{{
		Path:            "internal/checkout/service.go",
		OID:             "abcdef0123456789abcdef0123456789abcdef01",
		AgeSeconds:      30,
		SuggestedAction: PathRecentCommitSuggestionExtendOrWait,
	}}
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		OfferedCaptures: []OfferedCapture{
			{Seq: 101, Path: "internal/checkout/service.go", Op: "modify", Timestamp: now, Fidelity: "full"},
			{Seq: 102, Path: "docs/checkout.md", Op: "modify", Timestamp: now, Fidelity: "full"},
		},
		PathRecentCommits: hint,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}
	// Primary returns a plan that omits seq 102 entirely so ValidateIntentPlan
	// raises an untyped failure, the composed retry path is skipped, and
	// deterministic fallback fires. Both invocations must see the hint.
	primary := &scriptedIntentPlanner{
		name: "scripted-primary",
		plans: []IntentPlan{{
			// Empty selected_seqs collapses to a typed validation error
			// (IntentPlanValidationShape). The composed retry path appends
			// the validator message and re-invokes the primary; the second
			// attempt also returns the same invalid plan so deterministic
			// fallback runs. Both primary calls and the deterministic
			// fallback must observe the hint.
			SelectedSeqs:   nil,
			Subject:        "broken plan",
			GroupingReason: "intentionally empty selected_seqs",
		}, {
			SelectedSeqs:   nil,
			Subject:        "broken plan",
			GroupingReason: "intentionally empty selected_seqs (second attempt)",
		}},
	}
	planner := Compose(primary, DeterministicProvider{})
	got, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err == nil {
		// Composed surfaces the typed error; replay.go's planIntentWithFallback
		// is what swaps in deterministic. Here we only need to confirm the hint
		// survived as far as the primary saw it across both attempts. Drop
		// `got` defensively in case the composed path ever changes.
		_ = got
	}
	if primary.calls < 1 {
		t.Fatalf("primary.calls=%d want >= 1", primary.calls)
	}
	for i, seen := range primary.requests {
		if len(seen.PathRecentCommits) != 1 {
			t.Fatalf("primary.requests[%d].PathRecentCommits len=%d want 1", i, len(seen.PathRecentCommits))
		}
		if seen.PathRecentCommits[0].OID != "abcdef0123456789abcdef0123456789abcdef01" {
			t.Fatalf("primary.requests[%d] OID=%q lost across composed retry", i, seen.PathRecentCommits[0].OID)
		}
	}
	// Run the deterministic provider directly with the same request so the
	// fallback half of the assertion holds without depending on whether the
	// composed layer fell back inline. Deterministic must also see the hint.
	det := DeterministicProvider{}
	plan, derr := det.PlanIntent(context.Background(), req)
	if derr != nil {
		t.Fatalf("deterministic PlanIntent: %v", derr)
	}
	if len(plan.SelectedSeqs) != 1 || plan.SelectedSeqs[0] != 101 {
		t.Fatalf("deterministic plan selected=%v want [101]", plan.SelectedSeqs)
	}
	// Re-marshal the request (post-deterministic) to confirm the hint is
	// still present — deterministic must not strip the field as a side effect.
	if len(req.PathRecentCommits) != 1 || req.PathRecentCommits[0].OID == "" {
		t.Fatalf("PathRecentCommits stripped by deterministic: %+v", req.PathRecentCommits)
	}
}

// TestComposedPlanIntentSynthMarkerSurfacesInDeferredReasons end-to-ends the
// marker visibility contract: the marker survives Compose's normalize +
// validate pipeline and lands in the returned plan's DeferredReasons.Reason.
// This is the value daemon/replay.go writes into decision_records.reason via
// recordIntentDeferrals (line 1349) and AppendDecision (line 1380); a
// daemon-side test in replay_test.go can re-assert the column round-trip.
func TestComposedPlanIntentSynthMarkerSurfacesInDeferredReasons(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	plan := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Tighten checkout flow",
		GroupingReason: "single focused checkout change",
		// DeferredReasons missing the entry for 102.
	}
	primary := &scriptedIntentPlanner{
		name:  "scripted-primary",
		plans: []IntentPlan{plan},
	}
	planner := Compose(primary, DeterministicProvider{})
	got, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if len(got.DeferredReasons) != 1 || got.DeferredReasons[0].Seq != 102 {
		t.Fatalf("deferred reasons=%+v", got.DeferredReasons)
	}
	if got.DeferredReasons[0].Reason != IntentPlanReasonMarker {
		t.Fatalf("returned reason=%q want marker %q for round-trip into decision_records.reason",
			got.DeferredReasons[0].Reason, IntentPlanReasonMarker)
	}
}

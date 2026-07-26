package ai

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestValidateIntentPlanV2AllowsDependencySafeNonContiguousCandidates(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{
			{Seq: 1, Path: "internal/a.go", Op: "modify"},
			{Seq: 2, Path: "docs/b.md", Op: "modify"},
			{Seq: 3, Path: "internal/a_test.go", Op: "modify"},
		},
		[]IntentCaptureDependency{{
			FromSeq: 1, ToSeq: 3, Strength: IntentDependencySoft,
			Kind: "test_source", EvidenceHash: "sha256:abc",
		}},
	)
	plan := IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates: []IntentCandidateAssignment{
			readyCandidate("candidate-a", []int64{1, 3}),
			readyCandidate("candidate-b", []int64{2}),
		},
	}

	if err := ValidateIntentPlanV2(req, plan); err != nil {
		t.Fatalf("ValidateIntentPlanV2: %v", err)
	}
}

func TestValidateIntentPlanV2RejectsDisconnectedMegaGroup(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{
			{Seq: 1, Path: "internal/a.go", Op: "modify"},
			{Seq: 2, Path: "docs/b.md", Op: "modify"},
			{Seq: 3, Path: "internal/a_test.go", Op: "modify"},
		},
		[]IntentCaptureDependency{{
			FromSeq: 1, ToSeq: 3, Strength: IntentDependencySoft,
			Kind: "test_source",
		}},
	)
	plan := IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates:      []IntentCandidateAssignment{readyCandidate("mega", []int64{1, 2, 3})},
	}

	err := ValidateIntentPlanV2(req, plan)
	var validationErr *IntentPlanV2ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("error = %T %v, want *IntentPlanV2ValidationError", err, err)
	}
	if got := validationErr.Findings[0]; got.Gate != IntentAtomicitySeparation || got.Code != "candidate_disconnected" {
		t.Fatalf("finding = %#v", got)
	}
}

func TestValidateIntentPlanV2DoesNotTreatTimeAsCohesion(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{
			{Seq: 1, Path: "internal/a.go", Op: "modify"},
			{Seq: 2, Path: "docs/b.md", Op: "modify"},
		},
		[]IntentCaptureDependency{{
			FromSeq: 1, ToSeq: 2, Strength: IntentDependencySoft,
			Kind: "temporal_proximity",
		}},
	)
	err := ValidateIntentPlanV2(req, IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates:      []IntentCandidateAssignment{readyCandidate("mega", []int64{1, 2})},
	})
	var validationErr *IntentPlanV2ValidationError
	if !errors.As(err, &validationErr) || validationErr.Findings[0].Code != "candidate_disconnected" {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestValidateIntentPlanV2RequiresExactAssignment(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{
			{Seq: 1, Path: "a.go", Op: "modify"},
			{Seq: 2, Path: "b.go", Op: "modify"},
		},
		nil,
	)
	plan := IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates:      []IntentCandidateAssignment{readyCandidate("one", []int64{1})},
	}

	err := ValidateIntentPlanV2(req, plan)
	var validationErr *IntentPlanV2ValidationError
	if !errors.As(err, &validationErr) || validationErr.Findings[0].Code != "capture_unassigned" {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestValidateIntentPlanV2RejectsReadyCandidateWithMissingCompanion(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{{Seq: 1, Path: "a.go", Op: "modify"}},
		nil,
	)
	candidate := readyCandidate("incomplete", []int64{1})
	candidate.MissingCompanions = []string{"matching migration test"}

	err := ValidateIntentPlanV2(req, IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates:      []IntentCandidateAssignment{candidate},
	})
	var validationErr *IntentPlanV2ValidationError
	if !errors.As(err, &validationErr) || validationErr.Findings[0].Gate != IntentAtomicityCompleteness {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestValidateIntentPlanV2RequiresDeclaredHardDependency(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{
			{Seq: 1, Path: "migration.sql", Op: "create"},
			{Seq: 2, Path: "model.go", Op: "modify"},
		},
		[]IntentCaptureDependency{{
			FromSeq: 1, ToSeq: 2, Strength: IntentDependencyHard,
			Kind: "create_before_modify",
		}},
	)
	plan := IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates: []IntentCandidateAssignment{
			readyCandidate("migration", []int64{1}),
			readyCandidate("model", []int64{2}),
		},
	}

	err := ValidateIntentPlanV2(req, plan)
	var validationErr *IntentPlanV2ValidationError
	if !errors.As(err, &validationErr) || validationErr.Findings[0].Code != "hard_dependency_undeclared" {
		t.Fatalf("error = %T %v", err, err)
	}

	plan.Candidates[1].DependsOnCandidates = []string{"migration"}
	if err := ValidateIntentPlanV2(req, plan); err != nil {
		t.Fatalf("declared dependency rejected: %v", err)
	}
}

func TestValidateIntentPlanV2AcceptsReadyPersistedPrerequisite(t *testing.T) {
	req, err := NewIntentPlanRequestV2(IntentPlanRequestV2Options{
		OfferedCaptures: []OfferedCapture{{Seq: 2, Path: "model.go", Op: "modify"}},
		Candidates: []IntentCandidateSummary{{
			CandidateID:  "migration",
			Status:       "ready",
			SelectedSeqs: []int64{1},
			Ready:        true,
		}},
		Dependencies: []IntentCaptureDependency{{
			FromSeq: 1, ToSeq: 2, Strength: IntentDependencyHard,
			Kind: "migration_before_model",
		}},
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequestV2: %v", err)
	}
	model := readyCandidate("model", []int64{2})
	model.DependsOnCandidates = []string{"migration"}
	plan := IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates:      []IntentCandidateAssignment{model},
	}

	if err := ValidateIntentPlanV2(req, plan); err != nil {
		t.Fatalf("persisted prerequisite rejected: %v", err)
	}
}

func TestValidateIntentPlanV2RejectsLaterCandidateDependency(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{
			{Seq: 1, Path: "a.go", Op: "modify"},
			{Seq: 2, Path: "b.go", Op: "modify"},
		},
		nil,
	)
	first := readyCandidate("dependent", []int64{2})
	first.DependsOnCandidates = []string{"prerequisite"}
	plan := IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates: []IntentCandidateAssignment{
			first,
			readyCandidate("prerequisite", []int64{1}),
		},
	}

	err := ValidateIntentPlanV2(req, plan)
	var validationErr *IntentPlanV2ValidationError
	if !errors.As(err, &validationErr) || validationErr.Findings[0].Code != "publish_order_not_topological" {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestAdaptIntentPlanV1LabelsCompatibilityCandidates(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{
			{Seq: 1, Path: "a.go", Op: "modify"},
			{Seq: 2, Path: "b.go", Op: "modify"},
		},
		nil,
	)
	legacy := IntentPlan{
		SelectedSeqs:   []int64{1},
		DeferredSeqs:   []int64{2},
		Subject:        "Update candidate planner",
		GroupingReason: "first capture is independently ready",
		DeferredReasons: []DeferredReason{{
			Seq: 2, Reason: "waiting for a companion test",
		}},
	}

	got, err := AdaptIntentPlanV1(req, legacy)
	if err != nil {
		t.Fatalf("AdaptIntentPlanV1: %v", err)
	}
	if got.ProtocolVersion != IntentPlannerProtocolV1Compat {
		t.Fatalf("protocol = %q", got.ProtocolVersion)
	}
	if len(got.Candidates) != 2 {
		t.Fatalf("candidates = %#v", got.Candidates)
	}
	if got.Candidates[0].Readiness != IntentCandidateReady ||
		got.Candidates[1].Readiness != IntentCandidateWait ||
		got.Candidates[1].MissingCompanions[0] != "waiting for a companion test" {
		t.Fatalf("candidate translation = %#v", got.Candidates)
	}
}

func TestPlanIntentV2WithCompatibilityAdaptsLegacyPlanner(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{{Seq: 1, Path: "a.go", Op: "modify"}},
		nil,
	)
	planner := legacyIntentPlanner{plan: IntentPlan{
		SelectedSeqs:   []int64{1},
		Subject:        "Update candidate planner",
		GroupingReason: "capture is independently ready",
	}}

	got, err := PlanIntentV2WithCompatibility(context.Background(), planner, req)
	if err != nil {
		t.Fatalf("PlanIntentV2WithCompatibility: %v", err)
	}
	if got.ProtocolVersion != IntentPlannerProtocolV1Compat {
		t.Fatalf("protocol = %q", got.ProtocolVersion)
	}
}

func TestNewIntentPlanRequestV2RedactsAndCapsDiff(t *testing.T) {
	secret := "Authorization: Bearer sk-" + strings.Repeat("x", 80)
	longDiff := secret + "\n" + strings.Repeat("+sensitive-looking-source\n", IntentStageDiffCap)
	req, err := NewIntentPlanRequestV2(IntentPlanRequestV2Options{
		OfferedCaptures: []OfferedCapture{{
			Seq: 1, Path: "a.go", Op: "modify", CapturedDiff: longDiff,
		}},
		IncludeCapturedDiffs: true,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequestV2: %v", err)
	}
	got := req.OfferedCaptures[0].CapturedDiff
	if strings.Contains(got, secret) {
		t.Fatal("captured diff contains unredacted secret")
	}
	if len(got) > IntentStageDiffCap {
		t.Fatalf("captured diff length = %d, cap = %d", len(got), IntentStageDiffCap)
	}
	if !req.CapturedDiffTransform.RedactionApplied || !req.CapturedDiffTransform.Truncated {
		t.Fatalf("transform = %#v", req.CapturedDiffTransform)
	}
}

func TestValidateIntentPlanRequestV2EnforcesPrivacyBounds(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{{Seq: 1, Path: "a.go", Op: "modify"}},
		nil,
	)
	req.Candidates = []IntentCandidateSummary{{
		CandidateID: "candidate",
		Status:      "open",
		Purpose:     strings.Repeat("x", IntentCandidatePurposeCap+1),
	}}
	if err := ValidateIntentPlanRequestV2(req); err == nil || !strings.Contains(err.Error(), "purpose exceeds") {
		t.Fatalf("error = %v", err)
	}

	req.Candidates = nil
	req.Dependencies = make([]IntentCaptureDependency, IntentDependencyEdgeCap+1)
	if err := ValidateIntentPlanRequestV2(req); err == nil || !strings.Contains(err.Error(), "dependency edges exceed cap") {
		t.Fatalf("error = %v", err)
	}
}

func TestBuildIntentPlanV2UserPromptBoundsCorrection(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{{Seq: 1, Path: "a.go", Op: "modify"}},
		nil,
	)
	req.RetryCorrection = "\x1b[31m" + strings.Repeat("unsafe", IntentAtomicityCorrectionCap)

	got, err := BuildIntentPlanV2UserPrompt(req)
	if err != nil {
		t.Fatalf("BuildIntentPlanV2UserPrompt: %v", err)
	}
	if strings.ContainsRune(got, '\x1b') {
		t.Fatal("prompt retained control characters")
	}
	if len(got) > len(`Plan durable semantic commit candidates for these offered captures:
{"protocol_version":"v2","offered_captures":[{"seq":1,"path":"a.go","op":"modify","timestamp":"0001-01-01T00:00:00Z","fidelity":"","defer_count":0}],"commit_format":"imperative"}`)+IntentAtomicityCorrectionCap+400 {
		t.Fatalf("prompt unexpectedly large: %d", len(got))
	}
}

func TestDecodeIntentPlanV2RejectsUnknownAndMultipleValues(t *testing.T) {
	req := mustIntentPlanRequestV2(t,
		[]OfferedCapture{{Seq: 1, Path: "a.go", Op: "modify"}},
		nil,
	)
	valid := `{"protocol_version":"v2","candidates":[{"candidate_id":"one","selected_seqs":[1],"purpose":"one change","readiness":"ready","subject":"Update candidate planner","grouping_reason":"single capture"}]}`
	if _, err := DecodeIntentPlanV2([]byte(valid), req); err != nil {
		t.Fatalf("valid response rejected: %v", err)
	}
	if _, err := DecodeIntentPlanV2([]byte(strings.Replace(valid, `"candidates":`, `"unknown":true,"candidates":`, 1)), req); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := DecodeIntentPlanV2([]byte(valid+" {}"), req); err == nil {
		t.Fatal("multiple JSON values accepted")
	}
}

func TestNewIntentAtomicityReportRequiresAllSevenGates(t *testing.T) {
	report := NewIntentAtomicityReport("candidate",
		IntentAtomicityGateResult{Gate: IntentAtomicityCohesion, Status: IntentAtomicityPassed},
	)
	if report.Valid {
		t.Fatal("partial report is valid")
	}
	if len(report.Gates) != 7 {
		t.Fatalf("gate count = %d", len(report.Gates))
	}
	if err := ValidateIntentAtomicityReport(report); err != nil {
		t.Fatalf("pending report shape invalid: %v", err)
	}

	var passed []IntentAtomicityGateResult
	for _, gate := range intentAtomicityGates {
		passed = append(passed, IntentAtomicityGateResult{Gate: gate, Status: IntentAtomicityPassed})
	}
	report = NewIntentAtomicityReport("candidate", passed...)
	if !report.Valid {
		t.Fatal("fully passed report is not valid")
	}
	if err := ValidateIntentAtomicityReport(report); err != nil {
		t.Fatalf("passed report invalid: %v", err)
	}

	passed[5].Status = IntentAtomicityNotRequired
	report = NewIntentAtomicityReport("candidate", passed...)
	if !report.Valid {
		t.Fatal("verification not_required should be valid")
	}
	if err := ValidateIntentAtomicityReport(report); err != nil {
		t.Fatalf("verification not_required report invalid: %v", err)
	}

	passed[0].Status = IntentAtomicityNotRequired
	report = NewIntentAtomicityReport("candidate", passed...)
	if report.Valid {
		t.Fatal("cohesion not_required should not be valid")
	}
	if err := ValidateIntentAtomicityReport(report); err == nil {
		t.Fatal("cohesion not_required report accepted")
	}
}

func TestBuildIntentAtomicityCorrectionIsBounded(t *testing.T) {
	got := BuildIntentAtomicityCorrection([]IntentAtomicityFinding{{
		CandidateID: "candidate",
		Gate:        IntentAtomicitySeparation,
		Code:        "candidate_disconnected",
		Summary:     strings.Repeat("split this group ", IntentAtomicitySummaryCap),
	}})
	if len([]rune(got)) > IntentAtomicityCorrectionCap {
		t.Fatalf("correction runes = %d", len([]rune(got)))
	}
	if !strings.Contains(got, "candidate_disconnected") {
		t.Fatalf("correction = %q", got)
	}
}

func mustIntentPlanRequestV2(t *testing.T, captures []OfferedCapture, dependencies []IntentCaptureDependency) IntentPlanRequestV2 {
	t.Helper()
	req, err := NewIntentPlanRequestV2(IntentPlanRequestV2Options{
		OfferedCaptures: captures,
		Dependencies:    dependencies,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequestV2: %v", err)
	}
	return req
}

func readyCandidate(id string, seqs []int64) IntentCandidateAssignment {
	return IntentCandidateAssignment{
		CandidateID:    id,
		SelectedSeqs:   seqs,
		Purpose:        "one reviewable change",
		Readiness:      IntentCandidateReady,
		Subject:        "Update semantic candidate",
		GroupingReason: "dependency evidence connects this candidate",
	}
}

type legacyIntentPlanner struct {
	plan IntentPlan
	err  error
}

func (p legacyIntentPlanner) Name() string { return "legacy-test" }

func (p legacyIntentPlanner) PlanIntent(context.Context, IntentPlanRequest) (IntentPlan, error) {
	return p.plan, p.err
}

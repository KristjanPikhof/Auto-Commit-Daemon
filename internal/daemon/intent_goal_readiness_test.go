package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func assertIntentCandidateProtectedGoalWait(t *testing.T, decision IntentCandidateDecision) {
	t.Helper()
	if decision.Publishable || decision.Candidate.Status != state.IntentCandidateWaiting ||
		decision.Assignment.Readiness != ai.IntentCandidateWait || decision.Assignment.Subject != "" ||
		!strings.Contains(strings.Join(decision.Assignment.MissingCompanions, " "), "goal") {
		t.Fatalf("capture without semantic evidence was not retained for goal planning: %+v", decision)
	}
}

func TestIntentGoalReadinessRequiresGroundedRelationships(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		captures  []ai.OfferedCapture
		wantError bool
	}{
		{
			name: "late helper and registration complete a cross-module goal",
			captures: []ai.OfferedCapture{
				{Seq: 1, Path: "Assistant/Features/ExportView.swift", CapturedDiff: "+let document = ResolveSummaryDocument(recording)\n"},
				{Seq: 2, Path: "Assistant/App/DependencyContainer.swift", CapturedDiff: "+register(ResolveSummaryDocument.self)\n"},
				{Seq: 3, Path: "Assistant/Core/DocumentResolver.swift", CapturedDiff: "+func ResolveSummaryDocument(_ recording: Recording) -> Document {}\n"},
			},
		},
		{
			name: "confident rationale cannot join exports onboarding and reminders",
			captures: []ai.OfferedCapture{
				{Seq: 1, Path: "Assistant/Export.swift", CapturedDiff: "+func ExportTranscriptArchive() {}\n"},
				{Seq: 2, Path: "Assistant/Onboarding.swift", CapturedDiff: "+func PresentWelcomeWizard() {}\n"},
				{Seq: 3, Path: "Assistant/Reminders.swift", CapturedDiff: "+func ScheduleReminderAlert() {}\n"},
			},
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seqs := make([]int64, 0, len(test.captures))
			for _, capture := range test.captures {
				seqs = append(seqs, capture.Seq)
			}
			plan := ai.IntentPlanV2{Candidates: []ai.IntentCandidateAssignment{{
				CandidateID: "shared-goal", SelectedSeqs: seqs,
				Purpose:        "complete the assistant improvements",
				GroupingReason: "all edits implement a coherent complete user experience",
				Readiness:      ai.IntentCandidateReady,
			}}}
			err := validatePlannerSemanticRationale(ai.IntentPlanRequestV2{OfferedCaptures: test.captures}, plan)
			if (err != nil) != test.wantError {
				t.Fatalf("rationale error=%v wantError=%t", err, test.wantError)
			}
		})
	}
}

func TestIntentGoalReadinessKeepsAvailableTestsAndCorrectionsTogether(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{
		OfferedCaptures: []ai.OfferedCapture{{Seq: 1}, {Seq: 2}, {Seq: 3}},
		Dependencies: []ai.IntentCaptureDependency{
			{FromSeq: 1, ToSeq: 2, Kind: "test_source", Strength: ai.IntentDependencySoft},
			{FromSeq: 2, ToSeq: 3, Kind: "same_path", Strength: ai.IntentDependencyHard},
		},
	}
	plan := ai.IntentPlanV2{Candidates: []ai.IntentCandidateAssignment{
		{CandidateID: "implementation", SelectedSeqs: []int64{1}, Readiness: ai.IntentCandidateReady},
		{CandidateID: "assertion-correction", SelectedSeqs: []int64{2, 3}, Readiness: ai.IntentCandidateReady},
	}}
	if err := validatePlannerSemanticRationale(req, plan); err == nil || !strings.Contains(err.Error(), "available_companion_split") {
		t.Fatalf("split support was accepted: %v", err)
	}
	plan.Candidates = []ai.IntentCandidateAssignment{{
		CandidateID: "complete-goal", SelectedSeqs: []int64{1, 2, 3}, Readiness: ai.IntentCandidateReady,
	}}
	if err := validatePlannerSemanticRationale(req, plan); err != nil {
		t.Fatalf("implementation, tests and correction rejected: %v", err)
	}
}

func TestIntentGoalReadinessRecognizesSwiftTestsAcrossDirectories(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{OfferedCaptures: []ai.OfferedCapture{
		{Seq: 1, Path: "Assistant/Core/RecordingExporter.swift", CapturedDiff: "+func ExportRecordingArchive() {}\n"},
		{Seq: 2, Path: "AssistantTests/RecordingExporterTests.swift", CapturedDiff: "+func testEmptyArchive() {}\n"},
	}}
	plan := ai.IntentPlanV2{Candidates: []ai.IntentCandidateAssignment{
		{CandidateID: "export", SelectedSeqs: []int64{1}, Readiness: ai.IntentCandidateReady},
		{CandidateID: "test", SelectedSeqs: []int64{2}, Readiness: ai.IntentCandidateReady},
	}}
	if err := validatePlannerSemanticRationale(req, plan); err == nil || !strings.Contains(err.Error(), "available_companion_split") {
		t.Fatalf("Swift support split was accepted: %v", err)
	}
	plan.Candidates = []ai.IntentCandidateAssignment{{
		CandidateID: "export", SelectedSeqs: []int64{1, 2}, Readiness: ai.IntentCandidateReady,
	}}
	if err := validatePlannerSemanticRationale(req, plan); err != nil {
		t.Fatal(err)
	}
}

func TestIntentGoalReadinessContinuesPersistedSourceWithLaterTest(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{
		OfferedCaptures: []ai.OfferedCapture{{Seq: 2, Path: "exporter_test.go"}},
		Candidates:      []ai.IntentCandidateSummary{{CandidateID: "export", SelectedSeqs: []int64{1}}},
		Dependencies:    []ai.IntentCaptureDependency{{FromSeq: 1, ToSeq: 2, Kind: "test_source", Strength: ai.IntentDependencySoft}},
	}
	plan := ai.IntentPlanV2{Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "export", SelectedSeqs: []int64{2}, Readiness: ai.IntentCandidateReady,
	}}}
	if err := validatePlannerSemanticRationale(req, plan); err != nil {
		t.Fatalf("later test could not complete durable goal: %v", err)
	}
}

func TestIntentGoalReadinessSemanticPlanCannotBypassEvidenceGate(t *testing.T) {
	t.Parallel()
	db := openIntentCandidateTestDB(t)
	first := intentCandidateCaptureFixture(1, "Assistant/Export.swift", "modify", "", "export")
	second := intentCandidateCaptureFixture(2, "Assistant/Onboarding.swift", "modify", "", "onboarding")
	assignment := ai.IntentCandidateAssignment{
		CandidateID: "unsupported-goal", SelectedSeqs: []int64{1, 2},
		Purpose: "make assistant better", Readiness: ai.IntentCandidateReady,
	}
	decision, err := evaluateIntentCandidateAssignment(context.Background(), db,
		IntentCandidateEvaluation{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			Now: time.Unix(100, 0), VerificationMode: "none", allowSemanticPlan: true,
			Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
		}, ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}, assignment, nil,
		map[string]state.IntentCandidate{}, map[int64]IntentCandidateCapture{1: first, 2: second})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Publishable || decision.Atomicity.Valid || decision.Candidate.Status != state.IntentCandidateBlocked {
		t.Fatalf("unsupported semantic claim published: %+v", decision)
	}
	if status := intentAtomicityGateStatus(decision.Atomicity, ai.IntentAtomicityVerification); status != ai.IntentAtomicityNotRequired {
		t.Fatalf("verification status=%q", status)
	}
}

func TestIntentGoalFallbackRetainsUnknownMeaning(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{OfferedCaptures: []ai.OfferedCapture{{
		Seq: 1, Path: "Assistant/TranscriptionService.swift", Op: "modify",
		CapturedDiff: "+let interval = 5\n",
	}}}
	for _, plan := range []ai.IntentPlanV2{
		deterministicIntentCandidatePlan(req, true, false),
		func() ai.IntentPlanV2 { plan, _ := balancedIntentCandidatePlan(req); return plan }(),
	} {
		if len(plan.Candidates) != 1 {
			t.Fatalf("plan=%+v", plan)
		}
		candidate := plan.Candidates[0]
		if candidate.Readiness != ai.IntentCandidateWait || candidate.Subject != "" || len(candidate.MissingCompanions) == 0 {
			t.Fatalf("unknown goal turned into a generic commit: %+v", candidate)
		}
		if len(candidate.SelectedSeqs) != 1 || candidate.SelectedSeqs[0] != 1 {
			t.Fatalf("waiting lost capture ownership: %+v", candidate)
		}
	}
}

func TestIntentGoalFallbackUsesFinalCapturedEvidence(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{OfferedCaptures: []ai.OfferedCapture{
		{Seq: 1, Path: "service.go", Op: "create", CapturedDiff: "+func OriginalExportDraft() {}\n"},
		{Seq: 2, Path: "service.go", Op: "modify", CapturedDiff: "+func PublishRecordingArchive() {}\n"},
	}}
	subject, _ := deterministicIntentCandidateMessage(req, []int64{1, 2})
	if subject != "Add PublishRecordingArchive" {
		t.Fatalf("final captured subject=%q", subject)
	}
	req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{
		Seq: 3, Path: "onboarding.go", Op: "modify", CapturedDiff: "+func ShowWelcomeScreen() {}\n",
	})
	if subject, _ := deterministicIntentCandidateMessage(req, []int64{1, 2, 3}); subject != "" {
		t.Fatalf("different outcomes described by one source symbol: %q", subject)
	}
}

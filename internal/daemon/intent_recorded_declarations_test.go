package daemon

import (
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentRecordedDeclarationsProveExistingProviderHelper(t *testing.T) {
	t.Parallel()
	const source = "test/provider_test.go"
	const contents = "package example\nfunc WriteProviderPlan(value string) {}\nfunc UnusedHelper() {}\n"
	captures := []IntentCandidateCapture{
		{Event: state.CaptureEvent{Seq: 1, Path: source}, CapturedDiff: "+var fixtureSubject = \"Add recording archives\"\n"},
		{Event: state.CaptureEvent{Seq: 2, Path: "test/atomicity_test.go"}, CapturedDiff: " WriteProviderPlan(plan)\n+var fixturePurpose = \"construct recording archives\"\n"},
	}
	names := intentOtherCaptureReferenceNames(captures)
	references := intentRecordedDeclarationContext(source, contents, names)
	if !strings.Contains(references, " func WriteProviderPlan(") || strings.Contains(references, "UnusedHelper") {
		t.Fatalf("missing real helper or unrelated declaration exposed: %q", references)
	}
	assertRecordedOwnerCompletesGoal(t, captures, references)
}

func TestIntentRecordedDeclarationsKeepSwiftOwnerDocsWithCaller(t *testing.T) {
	t.Parallel()
	const source = "Assistant/AssistantIntentDispatcher.swift"
	const contents = "// class MisleadingOwner {}\nfinal class AssistantIntentDispatcher {}\n"
	captures := []IntentCandidateCapture{
		{Event: state.CaptureEvent{Seq: 1, Path: source}, CapturedDiff: "+// Keep dispatch ownership documented accurately.\n"},
		{Event: state.CaptureEvent{Seq: 2, Path: "Assistant/URLCommandRouter.swift"}, CapturedDiff: "+let dispatcher = AssistantIntentDispatcher()\n"},
	}
	references := intentRecordedDeclarationContext(source, contents, nil)
	if !strings.Contains(references, " final class AssistantIntentDispatcher") || strings.Contains(references, "MisleadingOwner") {
		t.Fatalf("comment became ownership or actual owner was missed: %q", references)
	}
	assertRecordedOwnerCompletesGoal(t, captures, references)
}

func TestIntentRecordedDeclarationsIgnoreNonCodeReferenceNames(t *testing.T) {
	t.Parallel()
	const source = "internal/provider.go"
	const contents = "package example\nfunc WriteProviderPlan(value string) {}\n"
	for _, capturePath := range []string{"docs/providers.md", "assistant.yaml"} {
		t.Run(capturePath, func(t *testing.T) {
			captures := []IntentCandidateCapture{{
				Event: state.CaptureEvent{Seq: 1, Path: capturePath}, CapturedDiff: "+WriteProviderPlan(plan)\n",
			}}
			names := intentOtherCaptureReferenceNames(captures)
			if names.outside("WriteProviderPlan", source) {
				t.Fatal("non-code evidence became a recorded code reference")
			}
			if references := intentRecordedDeclarationContext(source, contents, names); references != "" {
				t.Fatalf("non-code evidence exposed an unrelated declaration: %q", references)
			}
		})
	}
}

func assertRecordedOwnerCompletesGoal(t *testing.T, captures []IntentCandidateCapture, references string) {
	t.Helper()
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, capture := range captures {
		req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: capture.Event.Seq, Path: capture.Event.Path, CapturedDiff: capture.CapturedDiff})
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "owner", SelectedSeqs: []int64{1, 2}, Purpose: "Keep owner corrections complete with their callers",
		Readiness: ai.IntentCandidateReady, Subject: "Keep owner corrections with their callers",
		Body:           "- Review existing ownership and its available corrections together",
		GroupingReason: "The recorded declaration proves which changed file owns the called behavior",
	}}}
	if err := ValidateIntentGoalPlan(req, plan); err == nil {
		t.Fatal("fixture strings or comments alone proved ownership")
	}
	req.OfferedCaptures[0].CapturedDiff = prependIntentRecordedReferenceContext(req.OfferedCaptures[0].CapturedDiff, references)
	if err := ValidateIntentGoalPlan(req, plan); err != nil {
		t.Fatalf("immutable owner and actual caller failed: %v", err)
	}
}

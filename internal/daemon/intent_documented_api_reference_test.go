package daemon

import (
	"reflect"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentDocumentedAPIReferenceRequiresRecordedDeclaration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, source, doc string
		connected         bool
	}{
		{"changed_class", "+class VoiceKeyboardURLHandoffCoordinator {}\n", "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n", true},
		{"recorded_own_class", prependIntentRecordedReferenceContext("+// explain app handoff ownership\n", " class VoiceKeyboardURLHandoffCoordinator {}\n"), "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n", true},
		{"fenced_enum", "+enum VoiceKeyboardURLHandoffCoordinator {}\n", " ~~~swift\n+VoiceKeyboardURLHandoffCoordinator.start()\n ~~~\n", true},
		{"enclosing_function", "@@ -10,3 +10,3 @@ func VoiceKeyboardURLHandoffCoordinator() {\n-limit = 1\n+limit = 2\n", "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n", true},
		{"plain_prose", "+class VoiceKeyboardURLHandoffCoordinator {}\n", "+Use VoiceKeyboardURLHandoffCoordinator for app handoff.\n", false},
		{"quoted_code_label", "+let label = \"VoiceKeyboardURLHandoffCoordinator\"\n", "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n", false},
		{"local_variable", "+let VoiceKeyboardURLHandoffCoordinator = 2\n", "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n", false},
		{"constant_binding", "+const VoiceKeyboardURLHandoffCoordinator = 2\n", "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n", false},
		{"comment_declaration", "+// class VoiceKeyboardURLHandoffCoordinator {}\n", "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n", false},
		{"ordinary_context_declaration", " class VoiceKeyboardURLHandoffCoordinator {}\n+// explain app handoff\n", "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n", false},
		{"partial_api_name", "+class VoiceKeyboardURLHandoffCoordinator {}\n", "+Use `OtherVoiceKeyboardURLHandoffCoordinator` for app handoff.\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2, OfferedCaptures: []ai.OfferedCapture{
				{Seq: 1, Path: "VoiceKeyboardURLHandoffCoordinator.swift", CapturedDiff: tc.source},
				{Seq: 2, Path: "docs/voice-keyboard-app-handoff.md", CapturedDiff: tc.doc},
			}}
			goal := ai.IntentCandidateAssignment{CandidateID: "app-handoff", SelectedSeqs: []int64{1, 2},
				Purpose: "document app URL handoff ownership", Readiness: ai.IntentCandidateReady,
				Subject: "Document app URL handoff ownership", Body: "- Keep the coordinator contract with its handoff guide",
				GroupingReason: "the guide names the actual recorded coordinator API"}
			plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{goal}}
			if err := ValidateIntentGoalPlan(req, plan); (err == nil) != tc.connected {
				t.Fatalf("connected=%t err=%v", tc.connected, err)
			}
			if !tc.connected {
				for _, kind := range []string{"documented_api_reference", "published_documented_api_reference"} {
					req.Dependencies = []ai.IntentCaptureDependency{{FromSeq: 1, ToSeq: 2, Strength: ai.IntentDependencySoft, Kind: kind, EvidenceHash: "unproved-API"}}
					if err := ValidateIntentGoalPlan(req, plan); err == nil {
						t.Fatalf("retained %s bypassed actual declaration checks", kind)
					}
				}
				return
			}
			source, doc := goal, goal
			source.SelectedSeqs, doc.SelectedSeqs = []int64{1}, []int64{2}
			doc.CandidateID = "handoff-guide"
			plan.Candidates = []ai.IntentCandidateAssignment{source, doc}
			if err := ValidateIntentGoalPlan(req, plan); err == nil {
				t.Fatal("available API implementation and guide were split")
			}
			fallback, attention := balancedIntentCandidatePlan(req)
			if attention || len(fallback.Candidates) != 1 || !reflect.DeepEqual(fallback.Candidates[0].SelectedSeqs, []int64{1, 2}) {
				t.Fatalf("local recovery split the documented API: %+v attention=%t", fallback, attention)
			}
			req.OfferedCaptures = req.OfferedCaptures[1:]
			req.Candidates = []ai.IntentCandidateSummary{{CandidateID: "published-source", Status: state.IntentCandidatePublished,
				SelectedSeqs: []int64{1}, CapturedEvidence: []ai.OfferedCapture{{Seq: 1, Path: "VoiceKeyboardURLHandoffCoordinator.swift", CapturedDiff: tc.source}}}}
			plan.Candidates = []ai.IntentCandidateAssignment{doc}
			if err := ValidateIntentGoalPlan(req, plan); err != nil {
				t.Fatalf("published API context forced duplicate source publication: %v", err)
			}
		})
	}
}

func TestIntentDocumentedAPIWitnessSurvivesBoundedAllocation(t *testing.T) {
	t.Parallel()
	producer := intentCandidateCaptureFixture(1, "VoiceKeyboardURLHandoffCoordinator.swift", "modify", "before", "after")
	producer.CapturedDiff = prependIntentRecordedReferenceContext("+// explain app handoff ownership\n", " class VoiceKeyboardURLHandoffCoordinator {}\n")
	doc := intentCandidateCaptureFixture(2, "docs/voice-keyboard-app-handoff.md", "modify", "before-doc", "after-doc")
	doc.CapturedDiff = strings.Repeat("+Ordinary handoff prose\n", 1400) + "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n" + strings.Repeat("+Ordinary handoff prose\n", 1400)
	captures := []IntentCandidateCapture{producer, doc}
	diffs := allocateIntentEvidenceDiffs(prioritizeIntentRelationshipEvidence(captures), 512)
	for i := range captures {
		captures[i].CapturedDiff = diffs[i]
	}
	found := false
	for _, hint := range runtimeIntentDependencyHints(captures) {
		found = found || hint.Kind == "documented_api_reference"
	}
	if !found {
		t.Fatalf("immutable source and signed inline API witnesses lost: %q", diffs)
	}
	if !strings.Contains(diffs[1], "+Use `VoiceKeyboardURLHandoffCoordinator` for app handoff.\n") {
		t.Fatal("documentation witness altered the recorded signed line")
	}
}

package daemon

import (
	"fmt"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

func TestIntentLocalBindingCannotConnectIndependentGoals(t *testing.T) {
	t.Parallel()
	for _, keyword := range []string{"let", "var", "const"} {
		for _, indent := range []string{"    ", "\t"} {
			producer := intentCandidateCaptureFixture(1, "startup.swift", "modify", "before", "after")
			producer.CapturedDiff = fmt.Sprintf("+%s%s action = buildStartupAction()\n", indent, keyword)
			consumer := intentCandidateCaptureFixture(2, "editor.swift", "modify", "before-editor", "after-editor")
			consumer.CapturedDiff = "+configureEditorButton(action)\n"
			for _, hint := range runtimeIntentDependencyHints([]IntentCandidateCapture{producer, consumer}) {
				if hint.Kind == "symbol_hash" {
					t.Fatalf("indented %s supplied a cross-file declaration: %+v", keyword, hint)
				}
			}
		}
	}
	// A genuine package/file-level constant remains useful prerequisite evidence.
	producer := intentCandidateCaptureFixture(1, "defaults.go", "modify", "before", "after")
	producer.CapturedDiff = "+const StartupRetryLimit = 3\n"
	consumer := intentCandidateCaptureFixture(2, "startup.go", "modify", "before-startup", "after-startup")
	consumer.CapturedDiff = "+configureStartupRetries(StartupRetryLimit)\n"
	found := false
	for _, hint := range runtimeIntentDependencyHints([]IntentCandidateCapture{producer, consumer}) {
		found = found || hint.Kind == "symbol_hash"
	}
	if !found {
		t.Fatal("real global constant lost its cross-file consumer")
	}
}

func TestIntentBoundedWitnessesKeepStartupAndEditorGoalsIndependent(t *testing.T) {
	t.Parallel()
	captures := []IntentCandidateCapture{
		intentCandidateCaptureFixture(1, "StartupGuidanceViews.swift", "modify", "before-startup", "after-startup"),
		intentCandidateCaptureFixture(2, "StartupEntry.swift", "modify", "before-entry", "after-entry"),
		intentCandidateCaptureFixture(3, "EditorActionStyle.swift", "modify", "before-style", "after-style"),
		intentCandidateCaptureFixture(4, "EditorToolbar.swift", "modify", "before-toolbar", "after-toolbar"),
	}
	captures[0].CapturedDiff = "+struct StartupFailureView {\n+    let action: () -> Void\n+    let message = \"Unable to open shared storage\"\n+}\n"
	captures[1].CapturedDiff = "+func startupGuidance() -> StartupFailureView {\n+    return StartupFailureView(action: {})\n+}\n"
	captures[2].CapturedDiff = "+struct EditorActionStyle {\n+    let tint: String\n+}\n"
	captures[3].CapturedDiff = "+func editorButton(action: () -> Void) -> EditorActionStyle {\n+    let style = EditorActionStyle(tint: \"blue\")\n+    action()\n+    return style\n+}\n"
	diffs := allocateIntentEvidenceDiffs(prioritizeIntentRelationshipEvidence(captures), 512)
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for i := range captures {
		captures[i].CapturedDiff = diffs[i]
		req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: captures[i].Event.Seq, Path: captures[i].Event.Path, CapturedDiff: diffs[i]})
	}
	startupConnected, editorConnected := false, false
	for _, hint := range runtimeIntentDependencyHints(captures) {
		if hint.Kind != "symbol_hash" {
			continue
		}
		startupConnected = startupConnected || (hint.PrerequisiteSeq == 1 && hint.DependentSeq == 2)
		editorConnected = editorConnected || (hint.PrerequisiteSeq == 3 && hint.DependentSeq == 4)
		if hint.PrerequisiteSeq <= 2 && hint.DependentSeq >= 3 {
			t.Fatalf("local action linked independently revertible goals: %+v", hint)
		}
	}
	if !startupConnected || !editorConnected {
		t.Fatalf("actual type relationships lost after allocation: startup=%t editor=%t diffs=%q", startupConnected, editorConnected, diffs)
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{
		{CandidateID: "startup-guidance", SelectedSeqs: []int64{1, 2}, Readiness: ai.IntentCandidateReady,
			Purpose: "add shared storage startup guidance", Subject: "Add shared storage startup guidance",
			Body: "- Keep the storage failure message with its guidance factory", GroupingReason: "the startup entry constructs the actual guidance view"},
		{CandidateID: "editor-style", SelectedSeqs: []int64{3, 4}, Readiness: ai.IntentCandidateReady,
			Purpose: "add editor action styles", Subject: "Add editor action styles",
			Body: "- Keep the editor callback with its toolbar appearance", GroupingReason: "the editor factory constructs its own action style"},
	}}
	if err := ValidateIntentGoalPlan(req, plan); err != nil {
		t.Fatalf("native validation rejected two complete independent goals: %v", err)
	}
}

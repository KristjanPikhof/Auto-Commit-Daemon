package daemon

import (
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentRecordedConstantsConnectRecoveryImplementationAndTest(t *testing.T) {
	t.Parallel()
	const source = "internal/cli/fix.go"
	const contents = `package cli
const (
 fixActionClearPause = "clear_pause"
 fixActionReconcileUnpublishedChain = "reconcile_unpublished_chain"
)
func buildFixPlan(force bool) {
 if !force { return }
}
`
	captures := []IntentCandidateCapture{
		{Event: state.CaptureEvent{Seq: 1, Path: source}, CapturedDiff: "+if !pending && !force { continue }\n"},
		{Event: state.CaptureEvent{Seq: 2, Path: "internal/cli/fix_pending_capture_recovery_test.go"}, CapturedDiff: "+package cli\n+if action.Kind != fixActionReconcileUnpublishedChain { t.Fatal(action) }\n"},
	}
	references := intentRecordedDeclarationContext(source, contents, intentOtherCaptureReferenceNames(captures))
	if !strings.Contains(references, " const (\n") || !strings.Contains(references, " fixActionReconcileUnpublishedChain =") {
		t.Fatalf("actual grouped constant ownership omitted: %q", references)
	}
	assertRecordedOwnerCompletesGoal(t, captures, references)
	captures[0].CapturedDiff = prependIntentRecordedReferenceContext(captures[0].CapturedDiff, references)
	prioritized := prioritizeIntentRelationshipEvidence(captures)
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for i, capture := range captures {
		req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: capture.Event.Seq,
			Path: capture.Event.Path, CapturedDiff: prioritized[i]})
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "pending-recovery", SelectedSeqs: []int64{1, 2}, Readiness: ai.IntentCandidateReady,
		Purpose: "Preserve pending capture chains during explicit forced recovery", Subject: "Preserve pending capture chains",
		Body:           "- Keep recovery behavior and its pending-chain regression complete",
		GroupingReason: "The regression checks the exact recovery action owned by the changed implementation",
	}}}
	if err := ValidateIntentGoalPlan(req, plan); err != nil {
		t.Fatalf("prioritization lost the original grouped declaration: %v", err)
	}
}

func TestIntentRecordedConstantsRejectLocalAndUnrelatedOwners(t *testing.T) {
	t.Parallel()
	for name, contents := range map[string]string{
		"local":   "package cli\nfunc fixture() { const ( action = 1 ) }\n",
		"comment": "package cli\n// const action = 1\n",
		"quoted":  "package cli\nvar label = `const action = 1`\n",
	} {
		t.Run(name, func(t *testing.T) {
			captures := []IntentCandidateCapture{{Event: state.CaptureEvent{Path: "internal/cli/check_test.go"}, CapturedDiff: "+package cli\n+check(action)\n"}}
			if got := intentRecordedDeclarationContext("internal/cli/fix.go", contents, intentOtherCaptureReferenceNames(captures)); got != "" {
				t.Fatalf("non-global ownership supplied evidence: %q", got)
			}
		})
	}
	const contents = "package cli\nconst (\n action = 1\n)\n"
	for _, consumer := range []string{"internal/other/check_test.go", "docs/actions.md"} {
		captures := []IntentCandidateCapture{{Event: state.CaptureEvent{Path: consumer}, CapturedDiff: "+package cli\n+check(action)\n"}}
		if got := intentRecordedDeclarationContext("internal/cli/fix.go", contents, intentOtherCaptureReferenceNames(captures)); got != "" {
			t.Fatalf("unrelated package or prose supplied ownership: %s: %q", consumer, got)
		}
	}
	external := []IntentCandidateCapture{{Event: state.CaptureEvent{Path: "internal/cli/external_test.go"}, CapturedDiff: "+package cli_test\n+check(action)\n"}}
	if got := intentRecordedDeclarationContext("internal/cli/fix.go", contents, intentOtherCaptureReferenceNames(external)); got != "" {
		t.Fatalf("external test package acquired private constant ownership: %q", got)
	}
}

func TestIntentRecordedOwnerSurvivesRelationshipWrapper(t *testing.T) {
	t.Parallel()
	captures := []IntentCandidateCapture{
		{Event: state.CaptureEvent{Seq: 1, Path: "internal/worker.go"}, CapturedDiff: strings.Repeat("+// bounded unrelated padding\n", 1000) + "@@ -10,2 +10,2 @@ func KeepWorkerState() {\n-oldState()\n+newState()\n" + strings.Repeat("+// bounded unrelated padding\n", 1000)},
		{Event: state.CaptureEvent{Seq: 2, Path: "internal/caller.go"}, CapturedDiff: "+KeepWorkerState()\n"},
	}
	prioritized := prioritizeIntentRelationshipEvidence(captures)
	if !strings.HasPrefix(prioritized[0], intentRelationshipEvidencePrefix) {
		t.Fatal("scenario did not retain ownership in the relationship wrapper")
	}
	declared, _ := intentSourceSymbolsForPath(captures[0].Event.Path, prioritized[0])
	if _, ok := declared["KeepWorkerState"]; !ok {
		t.Fatalf("wrapped immutable function ownership lost: %q", prioritized[0])
	}
}

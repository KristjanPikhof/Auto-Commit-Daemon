package ai

import (
	"strings"
	"testing"
)

func TestIntentBaselineMessageWaitDoesNotPreventUrgentProviderRequest(t *testing.T) {
	baseline := IntentCandidateAssignment{
		CandidateID: "protected-offline-work", SelectedSeqs: []int64{1},
		Purpose:           "retain dependency component until its goal is known",
		Readiness:         IntentCandidateWait,
		MissingCompanions: []string{"captured evidence needs a meaningful goal message"},
	}
	req := IntentPlanRequestV2{
		ProtocolVersion: IntentPlannerProtocolV2, ForcedAging: true,
		OfferedCaptures:    []OfferedCapture{{Seq: 1, Path: "offline.md", Op: "create"}},
		BaselineCandidates: []IntentCandidateAssignment{baseline},
	}
	if err := ValidateIntentPlanRequestV2(req); err != nil {
		t.Fatalf("local message uncertainty prevented a semantic request: %v", err)
	}
	if err := ValidateIntentPlanV2(req, IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates:      []IntentCandidateAssignment{baseline},
	}); err == nil || !strings.Contains(err.Error(), "forced_capture_deferred") {
		t.Fatalf("baseline exemption leaked into final provider validation: %v", err)
	}
	resolved := baseline
	resolved.Purpose = "document continued capture while the provider is offline"
	resolved.Readiness = IntentCandidateReady
	resolved.MissingCompanions = nil
	resolved.Subject = "Document continued offline capture"
	resolved.GroupingReason = "the document describes one complete capture behavior"
	if err := ValidateIntentPlanV2(req, IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates:      []IntentCandidateAssignment{resolved},
	}); err != nil {
		t.Fatalf("purposeful provider resolution rejected: %v", err)
	}
}

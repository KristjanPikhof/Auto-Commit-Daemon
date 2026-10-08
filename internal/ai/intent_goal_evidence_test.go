package ai

import (
	"strings"
	"testing"
)

func TestIntentGoalEvidenceUsesPrivacyAndRecordedMembership(t *testing.T) {
	candidates := []IntentCandidateSummary{{CandidateID: "archive", Status: "waiting", SelectedSeqs: []int64{1}, Paths: []string{"export.go"}, CapturedEvidence: []OfferedCapture{{Seq: 1, Path: "export.go", Op: "create", CapturedDiff: "+api_key = abcdef1234567890\n"}}}}
	opts := IntentPlanRequestV2Options{OfferedCaptures: []OfferedCapture{{Seq: 2, Path: "export_test.go", Op: "create"}}, Candidates: candidates, IncludeCapturedDiffs: true}
	req, err := NewIntentPlanRequestV2(opts)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(req.Candidates[0].CapturedEvidence[0].CapturedDiff, "abcdef1234567890") {
		t.Fatal("goal evidence leaked secret")
	}
	if candidates[0].CapturedEvidence[0].CapturedDiff != "+api_key = abcdef1234567890\n" {
		t.Fatal("builder mutated original evidence")
	}
	opts.IncludeCapturedDiffs = false
	req, err = NewIntentPlanRequestV2(opts)
	if err != nil || req.Candidates[0].CapturedEvidence[0].CapturedDiff != "" {
		t.Fatalf("privacy opt-out failed: %+v %v", req, err)
	}
	opts.Candidates[0].CapturedEvidence[0].Seq = 3
	if _, err := NewIntentPlanRequestV2(opts); err == nil {
		t.Fatal("unowned context was accepted")
	}
}

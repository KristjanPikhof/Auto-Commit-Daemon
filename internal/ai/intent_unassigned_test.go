package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

type intentOmittingProvider struct{ plan IntentPlanV2 }

func (p intentOmittingProvider) Name() string { return "omitting-native" }
func (p intentOmittingProvider) PlanIntentV2(context.Context, IntentPlanRequestV2) (IntentPlanV2, error) {
	return p.plan, nil
}

func TestIntentUnassignedCompletionPreservesReadyGoal(t *testing.T) {
	t.Parallel()
	req := mustIntentPlanRequestV2(t, []OfferedCapture{
		{Seq: 1, Path: "retry.go", Op: "modify"},
		{Seq: 2, Path: "release.md", Op: "modify"},
	}, nil)
	ready := readyCandidate("speech-retry", []int64{1})
	ready.Subject = "Restore speech recognition after pauses"
	ready.Body = "- Resume recognition when the recording session becomes active"
	plan := IntentPlanV2{ProtocolVersion: IntentPlannerProtocolV2, Candidates: []IntentCandidateAssignment{ready}}
	if err := ValidateIntentPlanV2(req, plan); err == nil {
		t.Fatal("strict validator accepted an omitted capture")
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeIntentPlanV2(raw, req)
	if err != nil {
		t.Fatal(err)
	}
	native, err := PlanIntentV2WithCompatibility(context.Background(), intentOmittingProvider{plan}, req)
	if err != nil || !reflect.DeepEqual(native, decoded) {
		t.Fatalf("direct native and JSON completion diverged: plan=%+v err=%v", native, err)
	}
	if len(decoded.Candidates) != 2 || !reflect.DeepEqual(decoded.Candidates[0], ready) || len(plan.Candidates) != 1 {
		t.Fatalf("completion changed the ready goal or input: %+v", decoded)
	}
	wait := decoded.Candidates[1]
	if !reflect.DeepEqual(wait.SelectedSeqs, []int64{2}) || wait.Readiness != IntentCandidateWait ||
		wait.Subject != "" || wait.Body != "" || len(wait.MissingCompanions) == 0 {
		t.Fatalf("omission became a generic publishable goal: %+v", wait)
	}
	if err := ValidateIntentPlanV2(req, decoded); err != nil {
		t.Fatalf("completed ownership is not exact: %v", err)
	}
	if !wait.IsHostRetainedWait() || decoded.Candidates[0].IsHostRetainedWait() ||
		!cloneIntentPlanV2Value(decoded).Candidates[1].IsHostRetainedWait() {
		t.Fatal("host omission provenance was lost or applied to provider work")
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	var providerPlan IntentPlanV2
	if err := json.Unmarshal(encoded, &providerPlan); err != nil {
		t.Fatal(err)
	}
	if providerPlan.Candidates[1].IsHostRetainedWait() {
		t.Fatal("private host provenance crossed the provider JSON boundary")
	}
	explicit, err := DecodeIntentPlanV2(encoded, req)
	if err != nil || explicit.Candidates[1].IsHostRetainedWait() {
		t.Fatalf("explicit provider WAIT was inferred from purpose or ID: %+v err=%v", explicit, err)
	}
}

func TestIntentUnassignedCompletionKeepsUnsafePlansRejected(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"persisted_owner", "duplicate_capture", "outside_capture", "unknown_dependency", "invalid_id", "id_collision", "hard_prerequisite", "candidate_cap"} {
		t.Run(name, func(t *testing.T) {
			req := mustIntentPlanRequestV2(t, []OfferedCapture{{Seq: 1, Path: "retry.go", Op: "modify"}, {Seq: 2, Path: "release.md", Op: "modify"}}, nil)
			plan := IntentPlanV2{ProtocolVersion: IntentPlannerProtocolV2, Candidates: []IntentCandidateAssignment{readyCandidate("retry", []int64{1})}}
			switch name {
			case "persisted_owner":
				req.Candidates = []IntentCandidateSummary{{CandidateID: "approved-goal", Status: "waiting", Purpose: "an existing approved goal", SelectedSeqs: []int64{2}}}
			case "duplicate_capture":
				plan.Candidates = append(plan.Candidates, readyCandidate("duplicate", []int64{1}))
			case "outside_capture":
				plan.Candidates[0].SelectedSeqs = []int64{99}
			case "unknown_dependency":
				plan.Candidates[0].DependsOnCandidates = []string{"unknown"}
			case "invalid_id":
				plan.Candidates[0].CandidateID = "invalid\nid"
			case "id_collision":
				plan.Candidates[0].CandidateID = "host-wait-2"
			case "hard_prerequisite":
				req.Dependencies = []IntentCaptureDependency{{FromSeq: 2, ToSeq: 1, Strength: IntentDependencyHard, Kind: "object_reference", EvidenceHash: "sha256:proven"}}
			case "candidate_cap":
				req.OfferedCaptures, plan.Candidates = nil, nil
				for i := 1; i <= IntentOpenCandidateCap+1; i++ {
					seq := int64(i)
					req.OfferedCaptures = append(req.OfferedCaptures, OfferedCapture{Seq: seq, Path: fmt.Sprintf("file-%d.go", i), Op: "modify"})
					if i <= IntentOpenCandidateCap {
						plan.Candidates = append(plan.Candidates, readyCandidate(fmt.Sprintf("goal-%d", i), []int64{seq}))
					}
				}
			}
			raw, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeIntentPlanV2(raw, req)
			if err == nil || !reflect.DeepEqual(got, plan) {
				t.Fatalf("completion bypassed %s: plan=%+v err=%v", name, got, err)
			}
		})
	}
}

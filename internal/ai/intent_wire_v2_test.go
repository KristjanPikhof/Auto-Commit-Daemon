package ai

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestIntentPlanV2WireKeepsReadonlyProofWithoutAssignmentIDs(t *testing.T) {
	t.Parallel()
	consumer := readyCandidate("consumer", []int64{2})
	consumer.DependsOnCandidates = []string{"published-baseline"}
	mutable := readyCandidate("mutable-goal", []int64{4})
	mutable.Readiness, mutable.Subject = IntentCandidateWait, ""
	mutable.MissingCompanions = []string{"finish reconnect behavior"}
	req := IntentPlanRequestV2{ProtocolVersion: IntentPlannerProtocolV2,
		OfferedCaptures: []OfferedCapture{{Seq: 2, Path: "consumer.go", Op: "modify"}, {Seq: 4, Path: "reconnect_test.go", Op: "create"}},
		Candidates: []IntentCandidateSummary{
			{CandidateID: "published-baseline", Status: "published", Ready: true, Purpose: "retain verified provider reconnect behavior", SelectedSeqs: []int64{1}, Paths: []string{"baseline.go"},
				CapturedEvidence: []OfferedCapture{{Seq: 1, Path: "baseline.go", Op: "modify", Fidelity: "recorded", CapturedDiff: "+func TrustedBaseline() {}\n"}}},
			{CandidateID: "mutable-goal", Status: "waiting", Purpose: "complete provider reconnect coverage", SelectedSeqs: []int64{3, 4}, Paths: []string{"reconnect.go", "reconnect_test.go"},
				CapturedEvidence: []OfferedCapture{{Seq: 3, Path: "reconnect.go", Op: "modify", CapturedDiff: "+func PendingReconnect() {}\n"}, {Seq: 4, Path: "reconnect_test.go", Op: "create", CapturedDiff: "+PendingReconnect()\n"}}},
		},
		Dependencies: []IntentCaptureDependency{
			{FromSeq: 1, ToSeq: 2, Strength: IntentDependencyHard, Kind: "object_chain", EvidenceHash: "baseline-object-proof"},
			{FromSeq: 3, ToSeq: 4, Strength: IntentDependencySoft, Kind: "symbol_hash", EvidenceHash: "reconnect-symbol-proof"},
			{FromSeq: 2, ToSeq: 4, Strength: IntentDependencySoft, Kind: "symbol_hash", EvidenceHash: "consumer-test-proof"},
		},
		BaselineCandidates: []IntentCandidateAssignment{consumer, mutable},
	}
	if err := ValidateIntentPlanRequestV2(req); err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	for _, transport := range []string{"openai", "subprocess"} {
		t.Run(transport, func(t *testing.T) {
			var payload []byte
			if transport == "openai" {
				body, _, err := buildOpenAIIntentPlanV2RequestWithTrace("test-model", req)
				if err != nil {
					t.Fatal(err)
				}
				var sent struct {
					Messages []struct {
						Content string `json:"content"`
					} `json:"messages"`
				}
				if err := json.Unmarshal(body, &sent); err != nil {
					t.Fatal(err)
				}
				_, value, found := strings.Cut(sent.Messages[1].Content, "\n")
				if !found {
					t.Fatal("missing structured user request")
				}
				payload = []byte(value)
			} else {
				body, err := marshalSubprocessRequest(subprocessRequest{Version: 2, RequestType: "intent_plan_v2", PlannerRequestV2: &req})
				if err != nil {
					t.Fatal(err)
				}
				var sent struct {
					Request json.RawMessage `json:"planner_request_v2"`
				}
				if err := json.Unmarshal(body, &sent); err != nil {
					t.Fatal(err)
				}
				payload = sent.Request
			}
			var wire struct {
				Candidates []struct {
					CandidateID string           `json:"candidate_id"`
					Selected    []int64          `json:"selected_seqs"`
					Evidence    []OfferedCapture `json:"captured_evidence"`
					Readonly    []map[string]any `json:"readonly_evidence"`
				} `json:"candidates"`
				Baseline             []IntentCandidateAssignment `json:"baseline_candidates"`
				Dependencies         []IntentCaptureDependency   `json:"dependencies"`
				ReadonlyDependencies []intentReadonlyDependency  `json:"readonly_dependencies"`
			}
			if err := json.Unmarshal(payload, &wire); err != nil {
				t.Fatal(err)
			}
			if len(wire.Candidates) != 2 || len(wire.Candidates[0].Selected) != 0 || len(wire.Candidates[0].Evidence) != 0 ||
				!reflect.DeepEqual(wire.Candidates[1].Selected, []int64{4}) || len(wire.Candidates[1].Evidence) != 1 || wire.Candidates[1].Evidence[0].Seq != 4 {
				t.Fatalf("readonly membership became selectable: %+v", wire.Candidates)
			}
			for i, wanted := range []string{"+func TrustedBaseline() {}\n", "+func PendingReconnect() {}\n"} {
				if len(wire.Candidates[i].Readonly) != 1 || wire.Candidates[i].Readonly[0]["captured_diff"] != wanted {
					t.Fatalf("recorded baseline/goal evidence lost: %+v", wire.Candidates[i])
				}
				if _, exposed := wire.Candidates[i].Readonly[0]["seq"]; exposed {
					t.Fatal("readonly evidence exposed a numeric capture ID")
				}
			}
			if len(wire.Baseline) != 2 || !reflect.DeepEqual(wire.Baseline[1].SelectedSeqs, []int64{4}) ||
				len(wire.Dependencies) != 1 || wire.Dependencies[0].FromSeq != 2 || wire.Dependencies[0].ToSeq != 4 || len(wire.ReadonlyDependencies) != 2 ||
				wire.ReadonlyDependencies[0].FromCandidateID != "published-baseline" || wire.ReadonlyDependencies[0].FromSeq != 0 || wire.ReadonlyDependencies[0].ToSeq != 2 {
				t.Fatalf("baseline or candidate prerequisite projection changed: %+v", wire)
			}
		})
	}
	after, _ := json.Marshal(req)
	if string(before) != string(after) {
		t.Fatal("wire projection mutated the host's ownership/evidence")
	}
	plan := IntentPlanV2{ProtocolVersion: IntentPlannerProtocolV2, Candidates: []IntentCandidateAssignment{consumer, mutable}}
	if err := ValidateIntentPlanV2(req, plan); err != nil {
		t.Fatalf("full host continuation proof lost: %v", err)
	}
	bad := cloneIntentPlanV2Value(plan)
	bad.Candidates[0].SelectedSeqs = append(bad.Candidates[0].SelectedSeqs, 1)
	if err := ValidateIntentPlanV2(req, bad); err == nil {
		t.Fatal("wire projection relaxed published-capture reselection")
	}
}

func TestIntentPlanV2WireLeavesLegacySubprocessEnvelopeUnchanged(t *testing.T) {
	t.Parallel()
	req := subprocessRequest{Version: 1, RequestType: "intent_plan", PlannerRequest: &IntentPlanRequest{OfferedCaptures: []OfferedCapture{{Seq: 7, Path: "one.go"}}}}
	wanted, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := marshalSubprocessRequest(req)
	if err != nil || string(actual) != string(wanted) {
		t.Fatalf("legacy envelope changed: %s %v", actual, err)
	}
}

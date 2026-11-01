package ai

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishDecisionReasonMustReachGit(t *testing.T) {
	evidence := finishTestEvidence()
	good := finishTestGoal(evidence)
	good.Reason = "Defaults overwrote authored values. Prefer explicit settings so a saved choice survives loading."
	good.Message = "Preserve explicit settings\n\n- Defaults overwrote authored values. Prefer explicit settings so a\n  saved choice survives loading."
	for _, scenario := range []string{"missing", "metadata only", "title repeated", "wrapped"} {
		t.Run(scenario, func(t *testing.T) {
			goal := good
			switch scenario {
			case "missing":
				goal.Reason = ""
			case "metadata only":
				goal.Message = "Preserve explicit settings\n\n- Change settings precedence and update regression tests."
			case "title repeated":
				goal.Reason = "Preserve explicit settings."
				goal.Message = "Preserve explicit settings\n\n- Preserve explicit settings."
			}
			provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				if request.Correction != nil {
					if !strings.Contains(request.Correction.ValidationError, "reason") {
						t.Fatalf("correction lost decision feedback: %+v", request.Correction)
					}
					goal = good
				}
				// Do not synthesize missing reasons in the response fixture.
				return json.Marshal(finishPlanOutput{Goals: []finish.Goal{goal}})
			}}
			result, err := PlanFinish(t.Context(), provider, evidence, finishTestOptions(evidence))
			calls := 2
			if scenario == "wrapped" {
				calls = 1
			}
			if err != nil || result.Calls != calls || result.Plan.Steps[0].Message != good.Message || result.Plan.Steps[0].Reason != good.Reason {
				t.Fatalf("decision reason lost: %+v %v", result, err)
			}
		})
	}
}

func TestFinishUnsplittablePurposesFailWithoutBatchFallback(t *testing.T) {
	evidence := finishTestEvidence()
	const blocked = "The single settings.go version changes both key invalidation and unrelated layout. No intermediate version separates those decisions."
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Correction != nil && !strings.Contains(request.Correction.ValidationError, blocked) {
			t.Fatalf("correction lost the evidence limit: %+v", request.Correction)
		}
		return json.Marshal(finishPlanOutput{BlockedReason: blocked})
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, finishTestOptions(evidence))
	finishAssertError(t, err, "invalid_provider_plan")
	if result.Calls != 2 || len(result.Plan.Steps) != 0 || !strings.Contains(err.Error(), blocked) {
		t.Fatalf("mixed fallback or unexplained failure: %+v %v", result, err)
	}
}

func TestSavedHistoryReasonRemainsBackwardCompatible(t *testing.T) {
	evidence := finishTestEvidence()
	goal := finishTestGoal(evidence)
	if err := ValidateHistoryGoalQuality(goal, evidence); err != nil {
		t.Fatalf("old saved reasonless metadata became unreadable: %v", err)
	}
}

func TestFinishConfirmsReferencedUnchangedResources(t *testing.T) {
	evidence := finishTestEvidence()
	for _, path := range []string{"Resources/teleport.mp3", "private-unreferenced.txt", "removed.mp3"} {
		evidence.Base[path] = finishTestEntry("c")
		evidence.Target[path] = finishTestEntry("c")
	}
	delete(evidence.Target, "removed.mp3")
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
	options := finishTestOptions(evidence)
	options.Diffs[0].Text = "- old value\n+ resource = 'Resources/teleport.mp3'\n+ removed = 'removed.mp3'\n+ missing = 'missing.mp3'\n"
	request, _, err := prepareFinishRequest(evidence, options)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(request.ReferencedUnchangedPaths, []string{"Resources/teleport.mp3"}) {
		t.Fatalf("lost existing resource or disclosed unrelated paths: %+v", request.ReferencedUnchangedPaths)
	}
	wire, err := json.Marshal(finishWireRequest(request))
	if err != nil || strings.Contains(string(wire), "private-unreferenced.txt") {
		t.Fatalf("unreferenced metadata reached provider: %s %v", wire, err)
	}
}

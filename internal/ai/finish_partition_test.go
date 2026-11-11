package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishOversizedExplanationRequiresFocusedPartition(t *testing.T) {
	evidence := finishTestEvidence()
	for _, name := range []string{"settings_test.go", "retry.go", "retry_test.go"} {
		evidence.Target[name] = finishTestEntry("c")
	}
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
	options := finishTestOptions(evidence)
	options.ReviewLineTarget, options.PartitionOversized = 200, true
	options.MeasurePlan = func(_ context.Context, plan finish.Plan) ([]FinishStepSize, error) {
		sizes := make([]FinishStepSize, len(plan.Steps))
		for i, step := range plan.Steps {
			sizes[i] = FinishStepSize{ChangedLines: len(step.ChangeIDs) * 75, ChangedFiles: len(step.ChangeIDs)}
		}
		return sizes, nil
	}
	original := finishTestGoal(evidence)
	original.Reason = "Keep authored values ahead of defaults."
	original.Boundary = "Keep all reliability work in one workflow"
	original.LargeChangeReason = "The implementation and regression tests must change together to complete the shared workflow."
	original.Message += "\n- " + original.LargeChangeReason
	var focused int
	transport := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Partition == nil {
			return finishTestOutput(original), nil
		}
		focused++
		if request.Partition.ChangedLines != 300 || len(request.Changes) != 4 {
			t.Fatal("focused reviewer lost the measured candidate or exact membership")
		}
		goals := []finish.Goal{
			{Purpose: "Preserve explicit settings", Boundary: "Keep settings precedence independently reviewable", Reason: "Keep authored values ahead of defaults.", Message: "Preserve explicit settings\n\n- Keep authored values ahead of defaults."},
			{Purpose: "Retry failed requests", Boundary: "Keep request retry behavior independently reviewable", Reason: "Keep failed requests available for a later attempt.", Message: "Retry failed requests\n\n- Keep failed requests available for a later attempt."},
		}
		for _, change := range request.Changes {
			owner := 0
			if strings.HasPrefix(change.Path, "retry") {
				owner = 1
			}
			goals[owner].ChangeIDs = append(goals[owner].ChangeIDs, change.ID)
		}
		return json.Marshal(finishPlanOutput{Goals: goals})
	}}
	options.VerifyPlan = func(_ context.Context, plan finish.Plan, _ *finish.SourcePlan) error {
		if len(plan.Steps) != 2 {
			return fmt.Errorf("the oversized original reached verification")
		}
		return nil
	}
	result, err := PlanFinish(t.Context(), transport, evidence, options)
	if err != nil || focused != 1 || result.Calls != 1 || result.Disclosure.PartitionCalls != 1 ||
		len(result.Plan.Steps) != 2 {
		t.Fatalf("large explanation bypassed partitioning: calls=%d focused=%d steps=%d err=%v", result.Calls, focused, len(result.Plan.Steps), err)
	}
}

func TestFinishPartitionCorrectsMessagesWithinEvidenceBudget(t *testing.T) {
	for _, scenario := range []struct {
		name                         string
		globalInvalid, alwaysInvalid bool
	}{
		{name: "repairs after global correction", globalInvalid: true},
		{name: "stops after one focused correction", alwaysInvalid: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			evidence := finishTestEvidence()
			evidence.Target["retry.go"] = finishTestEntry("c")
			evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
			options := finishTestOptions(evidence)
			options.ReviewLineTarget, options.PartitionOversized = 200, true
			options.MeasurePlan = func(_ context.Context, plan finish.Plan) ([]FinishStepSize, error) {
				sizes := make([]FinishStepSize, len(plan.Steps))
				for i, step := range plan.Steps {
					sizes[i].ChangedLines = len(step.ChangeIDs) * 120
				}
				return sizes, nil
			}
			var global, focused, reserved int
			options.BeforePartitionCall = func(context.Context) error { reserved++; return nil }
			transport := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				if request.Partition == nil {
					global++
					goal := finishTestGoal(evidence)
					goal.Boundary = "Keep the supplied settings and request reliability work available for review"
					if global == 1 && scenario.globalInvalid {
						goal.Message = strings.Repeat("g", 51) + "\n\n- Keep authored values ahead of defaults."
					}
					return finishTestOutput(goal), nil
				}
				focused++
				prompt := finishPrompt(request)
				if !strings.Contains(prompt, "50 characters") || !strings.Contains(prompt, "72 characters") {
					t.Fatal("focused review omitted the commit message contract")
				}
				if focused == 1 && request.Correction != nil {
					t.Fatal("focused review inherited unrelated global correction output")
				}
				if focused == 2 && (request.Correction == nil || !strings.Contains(request.Correction.ValidationError, "50") || !strings.Contains(request.Correction.PreviousOutput, strings.Repeat("f", 51))) {
					t.Fatal("focused correction lost its own invalid proposal or exact error")
				}
				var goals []finish.Goal
				for _, change := range request.Changes {
					goal := finishTestGoal(evidence)
					goal.Boundary = "Review settings precedence independently from request retries"
					goal.ChangeIDs = []string{change.ID}
					if change.Path == "retry.go" {
						goal.Purpose, goal.Message = "Retry failed requests", "Retry failed requests\n\n- Keep failed requests available for another attempt."
					}
					goals = append(goals, goal)
				}
				if focused == 1 || scenario.alwaysInvalid {
					goals[0].Message = strings.Repeat("f", 51) + "\n\n- Keep failed requests available for another attempt."
				}
				return finishTestOutput(goals...), nil
			}}
			result, err := PlanFinish(t.Context(), transport, evidence, options)
			if focused != 2 || reserved != 2 || result.Disclosure.PartitionCalls != 2 {
				t.Fatalf("focused correction escaped its shared budget: focused=%d reserved=%d disclosure=%d err=%v", focused, reserved, result.Disclosure.PartitionCalls, err)
			}
			if scenario.alwaysInvalid {
				finishAssertError(t, err, "invalid_provider_plan")
				if global != 1 {
					t.Fatal("exhausted focused review started another global plan")
				}
			} else if err != nil || global != 2 || len(result.Plan.Steps) != 2 {
				t.Fatalf("formatting discarded a useful split: global=%d steps=%d err=%v", global, len(result.Plan.Steps), err)
			}
		})
	}
}

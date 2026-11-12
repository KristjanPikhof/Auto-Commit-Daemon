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

func TestFinishPartitionRechecksExactCodeAfterEvidenceReview(t *testing.T) {
	for _, largeParent := range []bool{false, true} {
		t.Run(fmt.Sprint("large_parent_", largeParent), func(t *testing.T) {
			evidence := finishTestEvidence()
			evidence.Target["retry.go"] = finishTestEntry("c")
			evidence.Target["other.go"] = finishTestEntry("d")
			evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
			options := finishTestOptions(evidence)
			options.ReviewLineTarget, options.PartitionOversized = 200, true
			repetitions, lines := 7000, 120
			if largeParent {
				repetitions, lines = 12000, 300
			}
			text := strings.Repeat("+ old test calls remain valid\n", repetitions) + "+ final compatibility evidence\n"
			for i := range options.Diffs {
				options.Diffs[i].Text = text
			}
			options.MeasurePlan = func(_ context.Context, plan finish.Plan) ([]FinishStepSize, error) {
				sizes := make([]FinishStepSize, len(plan.Steps))
				for i, step := range plan.Steps {
					sizes[i].ChangedLines = len(step.ChangeIDs) * lines
				}
				return sizes, nil
			}
			var exactCalls, summarizedCalls int
			transport := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				if request.Review != nil {
					var observations []map[string]string
					for _, part := range request.Review.Parts {
						observations = append(observations, map[string]string{"id": part.ID, "summary": "The test version adds checks for independently useful source interfaces."})
					}
					return json.Marshal(map[string]any{"observations": observations})
				}
				if request.Partition == nil {
					goal := finishTestGoal(evidence)
					goal.Boundary = "Keep the supplied settings and request interfaces available for review"
					goal.ChangeIDs = nil
					other := finishTestGoal(evidence)
					other.Boundary = "Keep the independent source interface separately reviewable"
					other.ChangeIDs = nil
					for _, change := range request.Changes {
						if change.Path == "other.go" {
							other.ChangeIDs = append(other.ChangeIDs, change.ID)
						} else {
							goal.ChangeIDs = append(goal.ChangeIDs, change.ID)
						}
					}
					return finishTestOutput(goal, other), nil
				}
				if len(request.Reviews) == 0 {
					t.Fatal("focused review lost the complete validated observations")
				}
				if largeParent && len(request.Changes) == 2 {
					summarizedCalls++
					for _, diff := range request.Diffs {
						if diff.Text != "" {
							t.Fatal("oversized raw parent exceeded the bounded request")
						}
					}
				} else {
					exactCalls++
					for _, diff := range request.Diffs {
						if diff.Text != text {
							t.Fatal("focused child lost exact compatibility evidence after the parent was summarized")
						}
					}
				}
				var goals []finish.Goal
				for _, change := range request.Changes {
					goal := finish.Goal{Purpose: "Preserve a compatible source interface", Boundary: "Keep the complete source interface independently reviewable", Reason: "Preserve existing test callers while adding the interface.", Message: "Preserve a compatible source interface\n\n- Preserve existing test callers while adding the interface.", ChangeIDs: []string{change.ID}}
					if len(request.Changes) == 1 {
						goal.LargeChangeReason = change.Path + " is the only complete supplied version."
						goal.Message += "\n- " + goal.LargeChangeReason
					}
					goals = append(goals, goal)
				}
				return finishTestOutput(goals...), nil
			}}
			result, err := PlanFinish(t.Context(), transport, evidence, options)
			wantExact, wantSummarized := 1, 0
			if largeParent {
				wantExact, wantSummarized = 3, 1
			}
			if err != nil || len(result.Plan.Steps) != 3 || result.Disclosure.EvidenceCalls == 0 || exactCalls != wantExact || summarizedCalls != wantSummarized {
				t.Fatalf("exact split evidence: steps=%d exact=%d summarized=%d err=%v", len(result.Plan.Steps), exactCalls, summarizedCalls, err)
			}
		})
	}
}

package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishIntentExactMessageDoesNotNeedGeneratedReason(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.Purpose = ""
	options.Message = "Preserve explicit settings"
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Message != options.Message || request.Purpose != "" {
			t.Fatalf("override or optional context changed: %+v", request)
		}
		goal := finishTestGoal(evidence)
		goal.Message = options.Message
		return finishTestOutput(goal), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 1 || result.Plan.Steps[0].Message != options.Message {
		t.Fatalf("exact override was rejected or rewritten: %+v %v", result, err)
	}
}

func TestFinishIntentOptionalContextSurvivesReviewAndCorrection(t *testing.T) {
	for _, contextText := range []string{"", "Problem: defaults overwrite saved choices.\n\nDecision: keep authored values ahead of defaults.\n\n" + strings.Repeat("Constraint: preserve the offline settings workflow and existing callers.\n", 30)} {
		for _, reviewed := range []bool{false, true} {
			t.Run(fmt.Sprintf("context=%t/reviewed=%t", contextText != "", reviewed), func(t *testing.T) {
				evidence := finishTestEvidence()
				options := finishTestOptions(evidence)
				options.Purpose = contextText
				if reviewed {
					options.Diffs[0].Text = strings.Repeat("+ keep authored settings\n", 30000)
				}
				provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
					if request.Purpose != contextText || request.Message != "" {
						t.Fatal("optional context was clipped or became an exact message")
					}
					if request.Review != nil {
						return finishTestReview(request), nil
					}
					goal := finishTestGoal(evidence)
					if request.Correction == nil {
						// An absent reason must still fail even when context is supplied.
						return json.Marshal(finishPlanOutput{Goals: []finish.Goal{goal}})
					}
					if !strings.Contains(request.Correction.ValidationError, "reason") {
						t.Fatal("reason correction was lost")
					}
					return finishTestOutput(goal), nil
				}}
				result, err := PlanFinish(t.Context(), provider, evidence, options)
				if err != nil || result.Calls != 2 || len(result.Plan.Steps) != 1 || result.Plan.Steps[0].Reason == "" || result.Plan.Steps[0].Purpose == contextText {
					t.Fatalf("context replaced inferred purpose or reason gate: %+v %v", result, err)
				}
				if reviewed != (result.Disclosure.EvidenceCalls > 0) {
					t.Fatal("review path was not exercised")
				}
			})
		}
	}
}

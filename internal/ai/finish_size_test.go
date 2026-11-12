package ai

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishReviewSizeSharesCorrectionWithChecks(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	options.ReviewLineTarget = FinishReviewLineTarget
	checks := 0
	options.VerifyPlan = func(ctx context.Context, plan finish.Plan, source *finish.SourcePlan) error {
		checks++
		deadline, _ := ctx.Deadline()
		if deadline != options.Deadline || source == nil || len(plan.Steps) != 2 {
			t.Fatal("candidate verification lost its source plan or frozen deadline")
		}
		if checks == 1 {
			return errors.Join(
				&CommitSizeFailure{Step: 1, ChangedLines: 240, ChangedFiles: 3, Limit: 200},
				&SourceCheckFailure{Step: 2, Output: "PRIVATE_OUTPUT: undefined: CameraID"},
			)
		}
		return nil
	}
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.ReviewLineTarget != 200 || !strings.Contains(finishPrompt(request), "smallest complete") {
			t.Fatal("new request lost its review-size contract")
		}
		if request.Correction != nil {
			feedback := request.Correction.ValidationError
			for _, detail := range []string{"240 lines", "3 files", "200-line", "step 2", "CameraID"} {
				if !strings.Contains(feedback, detail) {
					t.Fatalf("combined correction omitted %q: %s", detail, feedback)
				}
			}
			if strings.Contains(feedback, "PRIVATE_OUTPUT") {
				t.Fatal("raw project output escaped in size correction")
			}
		}
		return json.Marshal(finishPlanOutput{Goals: goals})
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 2 || checks != 2 {
		t.Fatalf("shared bounded correction: calls=%d checks=%d err=%v", result.Calls, checks, err)
	}
}

func TestFinishReviewOperationalFailureCannotHideBehindSize(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.ReviewLineTarget = 200
	options.VerifyPlan = func(context.Context, finish.Plan, *finish.SourcePlan) error {
		return errors.Join(&CommitSizeFailure{Step: 1, ChangedLines: 240, Limit: 200}, errors.New("PRIVATE_STATE_FAILURE"))
	}
	provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) {
		return finishTestOutput(finishTestGoal(evidence)), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	finishAssertError(t, err, "source_verification_failed")
	if result.Calls != 1 {
		t.Fatal("operational failure was sent back for a semantic correction")
	}
}

func TestFinishReviewRejectsPlaceholderReason(t *testing.T) {
	for _, target := range []int{0, FinishReviewLineTarget} {
		evidence := finishTestEvidence()
		options := finishTestOptions(evidence)
		options.ReviewLineTarget = target
		goal := finishTestGoal(evidence)
		goal.Reason = "Defaults overwrite authored choices; fix both."
		goal.Message = "Preserve explicit settings\n\n- " + goal.Reason
		provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
			if request.Correction != nil {
				if !strings.Contains(request.Correction.ValidationError, "mechanism") {
					t.Fatal("placeholder correction did not ask for the actual mechanism")
				}
				goal.Reason = "Defaults overwrite saved choices. Prefer authored values so reloads preserve those choices."
				goal.Message = "Preserve explicit settings\n\n- " + goal.Reason
			}
			return json.Marshal(finishPlanOutput{Goals: []finish.Goal{goal}})
		}}
		result, err := PlanFinish(t.Context(), provider, evidence, options)
		want := 1
		if target > 0 {
			want = 2
		}
		if err != nil || result.Calls != want {
			t.Fatalf("target=%d calls=%d err=%v", target, result.Calls, err)
		}
	}
}

func TestFinishLargeChangeReasonMustReachGit(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.ReviewLineTarget = FinishReviewLineTarget
	goal := finishTestGoal(evidence)
	goal.Reason = "Keep authored values ahead of defaults."
	goal.LargeChangeReason = "The recorded migration changes one shared format and all readers must switch together."
	checks := 0
	options.VerifyPlan = func(context.Context, finish.Plan, *finish.SourcePlan) error {
		checks++
		return nil
	}
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Correction != nil {
			if !strings.Contains(request.Correction.ValidationError, "large_change_reason verbatim") {
				t.Fatal("exception was accepted only in private metadata")
			}
			goal.Message += "\n- " + goal.LargeChangeReason
		}
		return json.Marshal(finishPlanOutput{Goals: []finish.Goal{goal}})
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 2 || checks != 1 || !finishMessageContainsExplanation(result.Plan.Steps[0].Message, goal.LargeChangeReason) {
		t.Fatalf("published exception: calls=%d checks=%d err=%v", result.Calls, checks, err)
	}
}

func TestFinishBlockedNewPolicyNeedsNewEvidence(t *testing.T) {
	for _, source := range []bool{false, true} {
		evidence := finishTestEvidence()
		options := finishTestOptions(evidence)
		if source {
			evidence, options, _ = finishSourceFixture()
		}
		options.ReviewLineTarget = FinishReviewLineTarget
		provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) {
			return json.Marshal(finishPlanOutput{BlockedReason: "The only recorded settings version contains two independent outcomes."})
		}}
		result, err := PlanFinish(t.Context(), provider, evidence, options)
		var failed *FinishPlanningError
		if !errors.As(err, &failed) || failed.Code != "provider_boundaries_unavailable" || failed.Retryable() || result.Calls != 2 {
			t.Fatalf("source=%v calls=%d err=%v", source, result.Calls, err)
		}
	}
}

func TestFinishWholeVersionPromptKeepsOnlyInseparableEditsTogether(t *testing.T) {
	strict := finishPrompt(FinishPlanRequest{RefineRecorded: true})
	grouped := finishPrompt(FinishPlanRequest{GroupIndivisibleVersions: true})
	if !strings.Contains(strict, finishStrictBoundaries) || !strings.Contains(strict, "exact change ID") {
		t.Fatal("checked refinement lost its boundary instructions")
	}
	if strings.Contains(grouped, finishStrictBoundaries) || !strings.Contains(grouped, finishWholeVersionBoundaries) {
		t.Fatal("whole-version planning received conflicting boundary instructions")
	}
}

func TestFinishReviewSchemaKeepsLegacyAndExactContracts(t *testing.T) {
	for _, scenario := range []string{"new", "source", "legacy", "exact", "history"} {
		request := FinishPlanRequest{Style: "imperative", ReviewLineTarget: FinishReviewLineTarget}
		switch scenario {
		case "source":
			request.SelectionOrder = []string{"edit-000001"}
			request.SourceEdits = []FinishSourceEdit{{ID: "edit-000001"}}
		case "legacy":
			request.ReviewLineTarget = 0
		case "exact":
			request.Message = "Preserve exact wording"
		case "history":
			request.HistoryCommits = []HistoryMessageCommit{{ID: "old-commit"}}
		}
		body, err := (&OpenAIProvider{}).EncodeFinishRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		var wire struct {
			Tools []struct {
				Function struct {
					Parameters map[string]any `json:"parameters"`
				} `json:"function"`
			} `json:"tools"`
		}
		if err := json.Unmarshal(body, &wire); err != nil {
			t.Fatal(err)
		}
		goal := wire.Tools[0].Function.Parameters["properties"].(map[string]any)["goals"].(map[string]any)["items"].(map[string]any)
		_, included := goal["properties"].(map[string]any)["large_change_reason"]
		want := scenario == "new" || scenario == "source"
		if included != want || strings.Contains(finishPrompt(request), "smallest complete") != want {
			t.Fatalf("%s changed its frozen schema or prompt contract", scenario)
		}
	}
}

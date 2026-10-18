package ai

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishIntentReplaysSelectedVersionsInOrder(t *testing.T) {
	evidence := finishTestEvidence()
	evidence.Changes[0].ID = "prepare"
	evidence.Changes = append(evidence.Changes, finish.Change{
		ID: "feature", Path: "settings.go", Before: finishTestEntry("b"),
		After: finishTestEntry("c"), Author: "author-a",
	})
	evidence.Target["settings.go"] = finishTestEntry("c")
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "one commit", true: "reversed groups"}[split], func(t *testing.T) {
			goal := finishTestGoal(evidence)
			goal.ChangeIDs = []string{"feature", "prepare"}
			goal.Boundary = "Keep the settings change and its unpublished correction together"
			goals := []finish.Goal{goal}
			if split {
				goals = append(goals, goal)
				goals[0].ChangeIDs = []string{"feature"}
				goals[1].ChangeIDs = []string{"prepare"}
			}
			provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				return finishTestOutput(goals...), nil
			}}
			result, err := PlanFinish(t.Context(), provider, evidence, finishTestOptions())
			expectedSteps := 1
			if split {
				expectedSteps = 2
			}
			if err != nil || result.Calls != 1 || len(result.Plan.Steps) != expectedSteps {
				t.Fatalf("selected versions failed: %+v %v", result, err)
			}
			var ids []string
			for _, step := range result.Plan.Steps {
				ids = append(ids, step.ChangeIDs...)
			}
			if !slices.Equal(ids, []string{"prepare", "feature"}) || !maps.Equal(result.Plan.Steps[expectedSteps-1].Tree, evidence.Target) {
				t.Fatalf("recorded order or final tree changed: %+v", result.Plan)
			}
		})
	}
}

func TestFinishIntentRejectsInterleavedGroups(t *testing.T) {
	evidence := finishTestEvidence()
	evidence.Changes[0].ID = "prepare"
	evidence.Changes = append(evidence.Changes,
		finish.Change{ID: "feature", Path: "settings.go", Before: finishTestEntry("b"), After: finishTestEntry("c"), Author: "author-a"},
		finish.Change{ID: "correction", Path: "settings.go", Before: finishTestEntry("c"), After: finishTestEntry("d"), Author: "author-a"},
	)
	evidence.Target["settings.go"] = finishTestEntry("d")
	first, second := finishTestGoal(evidence), finishTestGoal(evidence)
	first.ChangeIDs = []string{"prepare", "correction"}
	second.ChangeIDs = []string{"feature"}
	first.Boundary, second.Boundary = "Complete the settings preparation", "Add the settings consumer"
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Correction != nil && (!strings.Contains(request.Correction.ValidationError, "goal 1 needs goal 2") || !strings.Contains(request.Correction.ValidationError, "goal 2 needs goal 1")) {
			t.Fatalf("correction lost the conflicting memberships: %+v", request.Correction)
		}
		return finishTestOutput(first, second), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, finishTestOptions())
	finishAssertError(t, err, "invalid_provider_plan")
	if result.Calls != 2 || len(result.Plan.Steps) != 0 {
		t.Fatalf("conflicting groups changed or accepted: %+v", result)
	}
}

func TestFinishIntentCorrectionIncludesAllMembershipConflicts(t *testing.T) {
	evidence := finishTestEvidence()
	evidence.Target["settings_test.go"] = finishTestEntry("c")
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
	goal := finishTestGoal(evidence)
	goal.Boundary = "Keep the precedence fix and regression together"
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Correction == nil {
			return finishTestOutput(goal, goal), nil
		}
		for _, id := range goal.ChangeIDs {
			if !strings.Contains(request.Correction.ValidationError, `multiply assigned change "`+id+`" in goals 1 and 2`) {
				t.Fatalf("single correction lost a membership conflict: %+v", request.Correction)
			}
		}
		return finishTestOutput(goal), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, finishTestOptions())
	if err != nil || result.Calls != 2 || len(result.Plan.Steps) != 1 || !maps.Equal(result.Plan.Steps[0].Tree, evidence.Target) {
		t.Fatalf("combined membership correction failed: %+v %v", result, err)
	}
}

func TestFinishIntentRequiresGroundedContext(t *testing.T) {
	for _, source := range []string{"metadata", "context", "diffs", "exact message", "binary only"} {
		t.Run(source, func(t *testing.T) {
			options := finishTestOptions()
			options.Purpose = ""
			switch source {
			case "context":
				options.Purpose = "Keep authored settings ahead of defaults"
			case "diffs":
				options.DiffEgress = true
				options.Diffs = []FinishDiff{{ChangeID: "settings.go", Text: "- return defaults\n+ return authored\n"}}
			case "exact message":
				options.Message = "Preserve explicit settings"
			case "binary only":
				options.DiffEgress = true
				options.Diffs = []FinishDiff{{ChangeID: "settings.go", Binary: true}}
			}
			provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				goal := finishTestGoal(finishTestEvidence())
				if request.Message != "" {
					goal.Message = request.Message
				}
				return finishTestOutput(goal), nil
			}}
			result, err := PlanFinish(t.Context(), provider, finishTestEvidence(), options)
			if source == "metadata" || source == "binary only" {
				var failure *FinishPlanningError
				if !errors.As(err, &failure) || failure.Code != "provider_context_required" || failure.Retryable() {
					t.Fatalf("missing context needs a new request: %v", err)
				}
				if result.Calls != 0 || len(provider.requests) != 0 {
					t.Fatal("insufficient evidence spent a planning call")
				}
				return
			}
			if err != nil || result.Calls != 1 {
				t.Fatalf("grounded request failed: %+v %v", result, err)
			}
			if source == "exact message" && result.Plan.Steps[0].Message != options.Message {
				t.Fatal("changed the explicit batch message")
			}
		})
	}
}

func TestFinishIntentSingleBatchExplainsItsBoundary(t *testing.T) {
	evidence := finishTestEvidence()
	evidence.Target["settings_test.go"] = finishTestEntry("c")
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
	goal := finishTestGoal(evidence)
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Correction != nil {
			if !strings.Contains(request.Correction.ValidationError, "single batch") {
				t.Fatalf("missing batch explanation: %+v", request.Correction)
			}
			goal.Boundary = "The precedence fix and its direct regression test complete one change"
		}
		return finishTestOutput(goal), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, finishTestOptions())
	if err != nil || result.Calls != 2 || len(result.Plan.Steps) != 1 || result.Plan.Steps[0].Boundary == "" {
		t.Fatalf("unexplained batch accepted: %+v %v", result, err)
	}
}

func TestFinishIntentDoesNotLeaveReasonOnlyInMetadata(t *testing.T) {
	goal := finishTestGoal(finishTestEvidence())
	goal.Purpose = "Preserve authored values because defaults must not overwrite explicit settings"
	goal.Boundary = "This changes settings precedence without changing storage"
	goal.Message = "Preserve explicit settings"
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Correction != nil {
			if !strings.Contains(request.Correction.ValidationError, "short body") {
				t.Fatalf("missing actionable correction: %+v", request.Correction)
			}
			goal.Message += "\n\n- Prefer authored values so defaults cannot overwrite explicit choices."
		}
		return finishTestOutput(goal), nil
	}}
	result, err := PlanFinish(t.Context(), provider, finishTestEvidence(), finishTestOptions())
	if err != nil || result.Calls != 2 || !strings.Contains(result.Plan.Steps[0].Message, "cannot overwrite") {
		t.Fatalf("published reason lost: %+v %v", result, err)
	}
}

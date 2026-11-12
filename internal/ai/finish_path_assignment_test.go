package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishAssignsOmittedFilesInBoundedBatches(t *testing.T) {
	evidence := finishTestEvidence()
	const files = finishAssignmentBatchSize + 1
	for i := range files {
		evidence.Target[fmt.Sprintf("docs/example-%02d.md", i)] = finishTestEntry("c")
	}
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author")
	options := finishTestOptions(evidence)
	options.AllowPathOwnership = true
	for i := range options.Diffs {
		options.Diffs[i].Text = strings.Repeat("+ Explain the default preference.\n", 1100)
	}
	goal := finishTestGoal(evidence)
	goal.ChangeIDs = []string{"settings.go"}
	goal.Boundary = "Keep the settings precedence behavior separate"
	docs := finish.Goal{
		Purpose: "Document defaults in deployment examples", Boundary: "Keep example updates reviewable separately",
		Reason:  "Show operators how to configure the default in each deployment.",
		Message: "Document defaults in deployment examples\n\n- Show operators how to configure the default in each deployment.",
	}
	var batches int
	transport := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		switch {
		case request.Review != nil:
			observations := make([]map[string]string, 0, len(request.Review.Parts))
			for _, part := range request.Review.Parts {
				observations = append(observations, map[string]string{"id": part.ID, "summary": "Explain default settings in the deployment examples."})
			}
			return json.Marshal(map[string]any{"observations": observations})
		case request.AssignPaths != nil:
			batches++
			task := request.AssignPaths
			if len(task.Paths) > finishAssignmentBatchSize || len(task.Goals) != 2 ||
				len(request.Reviews) != len(task.Paths) || len(request.Changes) != len(task.Paths) {
				t.Fatalf("assignment lost its bounded evidence or goals: %+v", task)
			}
			assignments := make([]finishPathAssignment, 0, len(task.Paths))
			for _, path := range task.Paths {
				if !strings.HasPrefix(path, "docs/") {
					t.Fatalf("reassigned an existing goal member: %s", path)
				}
				assignments = append(assignments, finishPathAssignment{Path: path, Goal: 2})
			}
			return json.Marshal(map[string]any{"assignments": assignments})
		default:
			return finishTestOutput(goal, docs), nil
		}
	}}
	result, err := PlanFinish(t.Context(), transport, evidence, options)
	if err != nil || result.Calls != 1 || batches != 2 || result.Disclosure.AssignmentCalls != batches ||
		len(result.Plan.Steps) != 2 || len(result.Plan.Steps[1].ChangeIDs) != files {
		t.Fatalf("omitted files did not join the existing goal: calls=%d batches=%d steps=%d err=%v", result.Calls, batches, len(result.Plan.Steps), err)
	}
}

func TestFinishPathAssignmentsRejectUnknownAndDuplicateFiles(t *testing.T) {
	for _, response := range []string{
		`{"assignments":[{"path":"a","goal":1},{"path":"a","goal":1}]}`,
		`{"assignments":[{"path":"a","goal":1},{"path":"c","goal":1}]}`,
		`{"assignments":[{"path":"a","goal":1},{"path":"b","goal":2}]}`,
	} {
		if _, err := validateFinishPathAssignments([]byte(response), []string{"a", "b"}, 1); err == nil {
			t.Fatalf("accepted invalid assignment %s", response)
		}
	}
}

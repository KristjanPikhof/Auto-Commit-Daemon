package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

const finishAssignmentBatchSize = 16

type finishPathAssignmentTask struct {
	Paths []string                   `json:"paths"`
	Goals []finishPathAssignmentGoal `json:"goals"`
}

type finishPathAssignmentGoal struct {
	Purpose  string `json:"purpose"`
	Boundary string `json:"boundary"`
	Reason   string `json:"reason"`
}

type finishPathAssignment struct {
	Path string `json:"path"`
	Goal int    `json:"goal"`
}

// Repair omitted whole files using the same semantic goals and reviewed
// evidence. Existing memberships are immutable during these bounded calls.
func completeFinishPathOwnership(ctx context.Context, transport FinishTransport, request FinishPlanRequest, raw []byte, evidence finish.Evidence, options FinishOptions, disclosure *FinishDisclosure) ([]byte, error) {
	output, err := decodeFinishPlanOutput(raw)
	if err != nil || output.BlockedReason != "" || len(output.Goals) == 0 {
		return raw, nil // The normal validator handles other output failures.
	}
	goals := slices.Clone(output.Goals)
	catalogue := make([]finishPathAssignmentGoal, len(goals))
	for i, goal := range goals {
		goals[i].ChangeIDs = slices.Clone(goal.ChangeIDs)
		catalogue[i] = finishPathAssignmentGoal{
			Purpose: finishRedactText(goal.Purpose), Boundary: finishRedactText(goal.Boundary), Reason: finishRedactText(goal.Reason),
		}
	}
	if err := expandFinishPathOwnership(evidence.Changes, goals, output.PathOwnership); err != nil {
		return raw, nil
	}
	missing := unassignedFinishPaths(evidence, goals)
	if len(missing) == 0 {
		return raw, nil
	}
	if _, err := OrderFinishGoals(evidence, goals); err != nil {
		return raw, nil
	}
	inputLimit, err := FinishInputByteLimit(options.MaxInputBytes)
	if err != nil {
		return nil, err
	}
	limit := min(inputLimit, FinishContextByteCap)
	for len(missing) > 0 {
		size := min(finishAssignmentBatchSize, len(missing))
		var body []byte
		for {
			body, err = transport.EncodeFinishRequest(finishAssignmentRequest(request, missing[:size], catalogue))
			if err != nil {
				return nil, finishFailure("provider_input", err)
			}
			if len(body) <= limit {
				break
			}
			if size == 1 {
				// Leave this file for the global correction.
				body = nil
				break
			}
			size /= 2
		}
		paths := missing[:size]
		missing = missing[size:]
		if body == nil {
			continue
		}
		if disclosure.EvidenceCalls+disclosure.AssignmentCalls+disclosure.PartitionCalls >= finish.EvidenceReviewCallLimit {
			break // Keep the remaining coverage error for normal correction.
		}
		if err := ctx.Err(); err != nil {
			return nil, finishFailure("provider_deadline", err)
		}
		if options.BeforeAssignmentCall != nil {
			if err := options.BeforeAssignmentCall(ctx); err != nil {
				return nil, finishFailure("provider_budget", err)
			}
		}
		disclosure.AssignmentCalls++
		disclosure.EvidenceRequestBytes = append(disclosure.EvidenceRequestBytes, len(body))
		reply, err := transport.ExchangeFinish(ctx, body)
		if ctx.Err() != nil {
			return nil, finishDeadlineFailure(ctx.Err(), err)
		}
		var invalid *finishOutputError
		if errors.As(err, &invalid) {
			continue // Leave the omitted paths for the global correction.
		}
		if err != nil {
			return nil, finishFailure("provider_failure", err)
		}
		assignments, err := validateFinishPathAssignments(reply, paths, len(goals))
		if err != nil {
			continue
		}
		for _, assignment := range assignments {
			if assignment.Goal != 0 {
				output.PathOwnership = append(output.PathOwnership, finishPathOwnership{
					Goal: assignment.Goal, Paths: []string{assignment.Path},
				})
			}
		}
	}
	return json.Marshal(output)
}

func finishAssignmentRequest(request FinishPlanRequest, paths []string, catalogue []finishPathAssignmentGoal) FinishPlanRequest {
	selected := make(map[string]bool, len(paths))
	for _, path := range paths {
		selected[path] = true
	}
	ids := make(map[string]bool)
	for _, change := range request.Changes {
		if selected[change.Path] {
			ids[change.ID] = true
		}
	}
	subset := finishRequestForChanges(request, ids)
	subset.AssignPaths = &finishPathAssignmentTask{Paths: paths, Goals: catalogue}
	return subset
}

func finishRequestForChanges(request FinishPlanRequest, ids map[string]bool) FinishPlanRequest {
	subset := FinishPlanRequest{Purpose: request.Purpose, DiffIncluded: true}
	for _, change := range request.Changes {
		if ids[change.ID] {
			subset.Changes = append(subset.Changes, change)
		}
	}
	for _, diff := range request.Diffs {
		if ids[diff.ChangeID] {
			subset.Diffs = append(subset.Diffs, diff)
		}
	}
	for _, review := range request.Reviews {
		if ids[review.ChangeID] {
			subset.Reviews = append(subset.Reviews, review)
		}
	}
	return subset
}

func validateFinishPathAssignments(raw []byte, paths []string, goals int) ([]finishPathAssignment, error) {
	var output struct {
		Assignments []finishPathAssignment `json:"assignments"`
	}
	if len(raw) > subprocessResponseByteCap {
		return nil, errors.New("path assignment response exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&output); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF || len(output.Assignments) != len(paths) {
		return nil, errors.New("path assignment reply must cover the complete batch")
	}
	remaining := make(map[string]bool, len(paths))
	for _, path := range paths {
		remaining[path] = true
	}
	for _, assignment := range output.Assignments {
		if !remaining[assignment.Path] || assignment.Goal < 0 || assignment.Goal > goals {
			return nil, fmt.Errorf("invalid or repeated path assignment for %q", assignment.Path)
		}
		delete(remaining, assignment.Path)
	}
	return output.Assignments, nil
}

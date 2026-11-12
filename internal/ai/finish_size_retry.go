package ai

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

// FinishRetrySizeReview carries a complete earlier proposal that passed
// structural checks but lacked explanations for some measured large steps.
type FinishRetrySizeReview struct {
	Goals    []finish.Goal
	Failures []CommitSizeFailure
}

type finishSizeReviewStep struct {
	Step         int      `json:"step"`
	ChangedLines int      `json:"changed_lines"`
	ChangedFiles int      `json:"changed_files"`
	Purpose      string   `json:"purpose"`
	Boundary     string   `json:"boundary"`
	Reason       string   `json:"reason"`
	ChangeIDs    []string `json:"change_ids"`
}

func finishSizeReviewRequest(request FinishPlanRequest, retry *FinishRetrySizeReview) FinishPlanRequest {
	ids := map[string]bool{}
	var steps []finishSizeReviewStep
	for _, failure := range retry.Failures {
		goal := retry.Goals[failure.Step-1]
		steps = append(steps, finishSizeReviewStep{
			Step: failure.Step, ChangedLines: failure.ChangedLines, ChangedFiles: failure.ChangedFiles,
			Purpose: finishRedactText(goal.Purpose), Boundary: finishRedactText(goal.Boundary),
			Reason: finishRedactText(goal.Reason), ChangeIDs: goal.ChangeIDs,
		})
		for _, id := range goal.ChangeIDs {
			ids[id] = true
		}
	}
	subset := finishRequestForChanges(request, ids)
	subset.ReviewSizes = steps
	subset.ReviewLineTarget = request.ReviewLineTarget
	return subset
}

func applyFinishSizeReview(raw []byte, retry *FinishRetrySizeReview, exchangeErr error) ([]byte, error) {
	goals := slices.Clone(retry.Goals)
	expected := make(map[int]CommitSizeFailure, len(retry.Failures))
	for _, failure := range retry.Failures {
		expected[failure.Step] = failure
	}
	var reply struct {
		Reasons []struct {
			Step   int    `json:"step"`
			Reason string `json:"reason"`
		} `json:"reasons"`
	}
	err := exchangeErr
	if err == nil {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&reply); err == nil && decoder.Decode(new(any)) != io.EOF {
			err = errors.New("size review must return one JSON object")
		}
	}
	if err == nil {
		for _, item := range reply.Reasons {
			if _, ok := expected[item.Step]; !ok {
				err = fmt.Errorf("size review returned unknown or repeated step %d", item.Step)
				break
			}
			reason := strings.TrimSpace(item.Reason)
			if reason != "" {
				goal := &goals[item.Step-1]
				goal.LargeChangeReason = reason
				goal.Message += "\n- " + reason
			}
			delete(expected, item.Step)
		}
	}
	var failures []error
	if err != nil {
		failures = append(failures, err)
	}
	for _, failure := range retry.Failures {
		if goals[failure.Step-1].LargeChangeReason == "" {
			failures = append(failures, &failure)
		}
	}
	result, marshalErr := json.Marshal(finishPlanOutput{Goals: goals})
	return result, errors.Join(append(failures, marshalErr)...)
}

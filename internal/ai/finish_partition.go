package ai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

type FinishStepSize struct {
	ChangedLines int `json:"changed_lines"`
	ChangedFiles int `json:"changed_files"`
}

type FinishPartitionTask struct {
	Purpose      string `json:"purpose"`
	Boundary     string `json:"boundary"`
	ChangedLines int    `json:"changed_lines"`
	ChangedFiles int    `json:"changed_files"`
}

// This receipt is created after a focused partition request, never from the
// initial planner's large_change_reason.
type FinishPartitionReview struct {
	ChangeIDs    []string `json:"change_ids"`
	ChangedLines int      `json:"changed_lines"`
	Reason       string   `json:"reason"`
}

func HasFinishPartitionReview(goal finish.Goal, size FinishStepSize, reviews []FinishPartitionReview) bool {
	for _, review := range reviews {
		if size.ChangedLines == review.ChangedLines && slices.Equal(goal.ChangeIDs, review.ChangeIDs) &&
			goal.LargeChangeReason == review.Reason && review.Reason != "" {
			return true
		}
	}
	return false
}

// Every oversized whole-version group gets a focused attempt to partition its
// exact evidence. A successful split is reviewed again until its remaining
// larger steps have independent receipts.
func partitionFinishPlan(ctx context.Context, transport FinishTransport, request FinishPlanRequest, evidence finish.Evidence, plan finish.Plan, options FinishOptions, disclosure *FinishDisclosure) (finish.Plan, error) {
	if options.MeasurePlan == nil {
		return finish.Plan{}, errors.New("focused split review requires candidate measurements")
	}
	sizes, err := options.MeasurePlan(ctx, plan)
	if err != nil || len(sizes) != len(plan.Steps) {
		return finish.Plan{}, errors.Join(err, errors.New("candidate measurements do not match proposed commits"))
	}
	var goals []finish.Goal
	for i, step := range plan.Steps {
		size := sizes[i]
		if size.ChangedLines <= options.ReviewLineTarget || HasFinishPartitionReview(step.Goal, size, disclosure.PartitionReviews) {
			goals = append(goals, step.Goal)
			continue
		}
		base := plan.Base
		if i > 0 {
			base = plan.Steps[i-1].Tree
		}
		ids := make(map[string]bool, len(step.ChangeIDs))
		for _, id := range step.ChangeIDs {
			ids[id] = true
		}
		subset := finish.Evidence{Base: base, Target: step.Tree}
		for _, change := range evidence.Changes {
			if !ids[change.ID] {
				continue
			}
			dependencies := change.DependsOn
			change.DependsOn = nil
			for _, id := range dependencies {
				if ids[id] {
					change.DependsOn = append(change.DependsOn, id)
				}
			}
			subset.Changes = append(subset.Changes, change)
		}
		if err := finish.ValidateEvidence(subset); err != nil {
			return finish.Plan{}, err
		}
		focused := finishRequestForChanges(request, ids)
		focused.Changes = slices.Clone(subset.Changes)
		for j := range focused.Changes {
			focused.Changes[j].Provenance = ""
		}
		focused.Style, focused.ReviewLineTarget = options.Style, options.ReviewLineTarget
		focused.GroupIndivisibleVersions, focused.AllowPathOwnership = true, true
		focused.Partition = &FinishPartitionTask{
			Purpose: finishRedactText(step.Purpose), Boundary: finishRedactText(step.Boundary),
			ChangedLines: size.ChangedLines, ChangedFiles: size.ChangedFiles,
		}
		body, err := transport.EncodeFinishRequest(focused)
		if err != nil {
			return finish.Plan{}, finishFailure("provider_input", err)
		}
		inputLimit, err := FinishInputByteLimit(options.MaxInputBytes)
		if err != nil {
			return finish.Plan{}, err
		}
		if limit := min(inputLimit, FinishContextByteCap); len(body) > limit {
			return finish.Plan{}, finishContextLimit(len(body), limit)
		}
		if disclosure.EvidenceCalls+disclosure.AssignmentCalls+disclosure.PartitionCalls >= finish.EvidenceReviewCallLimit {
			return finish.Plan{}, finishFailure("provider_budget", errors.New("focused split review exhausted the evidence call budget"))
		}
		if err := ctx.Err(); err != nil {
			return finish.Plan{}, finishFailure("provider_deadline", err)
		}
		if options.BeforePartitionCall != nil {
			if err := options.BeforePartitionCall(ctx); err != nil {
				return finish.Plan{}, finishFailure("provider_budget", err)
			}
		}
		disclosure.PartitionCalls++
		disclosure.EvidenceRequestBytes = append(disclosure.EvidenceRequestBytes, len(body))
		raw, err := transport.ExchangeFinish(ctx, body)
		if ctx.Err() != nil {
			return finish.Plan{}, finishDeadlineFailure(ctx.Err(), err)
		}
		var invalid *finishOutputError
		if err != nil && !errors.As(err, &invalid) {
			return finish.Plan{}, finishFailure("provider_failure", err)
		}
		if err != nil {
			return finish.Plan{}, fmt.Errorf("split review for %q: %w", step.Purpose, err)
		}
		if len(focused.Reviews) > 0 {
			raw, err = completeFinishPathOwnership(ctx, transport, focused, raw, subset, options, disclosure)
			if err != nil {
				return finish.Plan{}, err
			}
		}
		refined, _, err := validateFinishReply(raw, subset, options)
		if err != nil {
			return finish.Plan{}, fmt.Errorf("split review for %q: %w", step.Purpose, err)
		}
		if len(refined.Steps) == 1 {
			goal := refined.Steps[0].Goal
			specific := false
			for _, change := range subset.Changes {
				if strings.Contains(goal.LargeChangeReason, path.Base(change.Path)) {
					specific = true
					break
				}
			}
			if !specific {
				return finish.Plan{}, fmt.Errorf("split review for %q kept the entire group without naming the exact file that prevents a useful split", step.Purpose)
			}
			disclosure.PartitionReviews = append(disclosure.PartitionReviews, FinishPartitionReview{
				ChangeIDs: slices.Clone(goal.ChangeIDs), ChangedLines: size.ChangedLines, Reason: goal.LargeChangeReason,
			})
			goals = append(goals, goal)
			continue
		}
		refined, err = partitionFinishPlan(ctx, transport, focused, subset, refined, options, disclosure)
		if err != nil {
			return finish.Plan{}, err
		}
		for _, child := range refined.Steps {
			goals = append(goals, child.Goal)
		}
	}
	return finish.Build(evidence, goals)
}

func encodeFinishGoals(plan finish.Plan) []byte {
	goals := make([]finish.Goal, len(plan.Steps))
	for i, step := range plan.Steps {
		goals[i] = step.Goal
	}
	raw, _ := json.Marshal(finishPlanOutput{Goals: goals})
	return raw
}

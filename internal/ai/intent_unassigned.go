package ai

import (
	"errors"
	"fmt"
)

// An omitted capture has no provider-approved goal. Keep it explicitly
// waiting without changing the provider's other assignments or messages.
// The validator remains strict, including every dependency and ownership
// constraint; this completion cannot turn a partial goal into a ready one.
func retainUnassignedIntentCaptures(req IntentPlanRequestV2, plan IntentPlanV2, validationErr error) (IntentPlanV2, error) {
	var validation *IntentPlanV2ValidationError
	if validationErr == nil || plan.ProtocolVersion != IntentPlannerProtocolV2 ||
		!errors.As(validationErr, &validation) || len(validation.Findings) != 1 ||
		validation.Findings[0].Code != "capture_unassigned" {
		return plan, validationErr
	}
	assigned := make(map[int64]bool)
	for _, candidate := range plan.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			assigned[seq] = true
		}
	}
	owned := make(map[int64]bool)
	for _, candidate := range req.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			owned[seq] = true
		}
	}
	missing := make([]int64, 0)
	for _, capture := range req.OfferedCaptures {
		if assigned[capture.Seq] {
			continue
		}
		// Only the daemon's protected provisional-group projection can release
		// existing ownership. Provider response repair cannot do so itself.
		if owned[capture.Seq] {
			return plan, validationErr
		}
		missing = append(missing, capture.Seq)
	}
	if len(plan.Candidates)+len(missing) > IntentOpenCandidateCap {
		return plan, validationErr
	}
	completed := cloneIntentPlanV2Value(plan)
	for _, seq := range missing {
		completed.Candidates = append(completed.Candidates, IntentCandidateAssignment{
			CandidateID:       fmt.Sprintf("host-wait-%d", seq),
			SelectedSeqs:      []int64{seq},
			Purpose:           "retain dependency component until its goal is known",
			Readiness:         IntentCandidateWait,
			MissingCompanions: []string{"captured evidence cannot yet explain a meaningful commit goal"},
			GroupingReason:    "bounded fallback requires planner review",
		})
	}
	if err := ValidateIntentPlanV2(req, completed); err != nil {
		return plan, err
	}
	return completed, nil
}

package ai

import (
	"errors"
	"strings"
	"testing"
)

func TestFinishIntentExactReasonFailurePrecedesProvider(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.Message = "Preserve explicit settings"
	provider := &finishFakeTransport{}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	var failed *FinishPlanningError
	if !errors.As(err, &failed) || failed.Retryable() || !strings.Contains(err.Error(), "body") || result.Calls != 0 || len(provider.requests) != 0 {
		t.Fatalf("reasonless exact message reached provider: %+v %v", result, err)
	}
}

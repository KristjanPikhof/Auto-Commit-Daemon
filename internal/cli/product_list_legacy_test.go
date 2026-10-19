package cli

import (
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestProductListLegacyWarningPreservesCurrentStatus(t *testing.T) {
	for _, scenario := range []struct {
		name, operational, finishState, status string
		state                                  productState
	}{
		{"healthy", "up_to_date", "", "healthy", productStateProtected},
		{"checkpointing", "checkpointing", "", "waiting", productStateWaiting},
		{"finishing", "finishing", state.FinishRunning, "working", productStatePublishing},
		{"finish-failed", "saved", state.FinishFailed, "finish failed", productStateNeedsAction},
		{"finish-attention", "saved", state.FinishNeedsAttention, "finish needs_attention", productStateNeedsAction},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			entry := productListEntry{State: scenario.state, OperationalState: scenario.operational}
			entry.PublicationOutcome.CheckpointAvailable = true
			entry.PublicationOutcome.Legacy.State = "needs_attention"
			if scenario.finishState != "" {
				entry.PublicationOutcome.Finish = &finishStatusReport{State: scenario.finishState}
			}
			entry.CheckpointMaintenance.State = "retrying"
			if got := productListStatus(entry); got != scenario.status {
				t.Fatalf("current status=%q, want %q", got, scenario.status)
			}
			want := scenario.status + " (maintenance retry) (legacy archive attention)"
			if got := productListDisplayStatus(entry); got != want {
				t.Fatalf("display status=%q, want %q", got, want)
			}
		})
	}
}

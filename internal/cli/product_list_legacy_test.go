package cli

import (
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestProductListLegacyWarningPreservesCurrentStatus(t *testing.T) {
	for _, scenario := range []struct {
		name, operational, finishState, status, display string
		state                                           productState
	}{
		{"healthy", "up_to_date", "", "healthy", "ready", productStateProtected},
		{"checkpointing", "checkpointing", "", "waiting", "saved, awaiting finish", productStateWaiting},
		{"finishing", "finishing", state.FinishRunning, "working", "Finish is in progress.", productStatePublishing},
		{"finish-failed", "saved", state.FinishFailed, "finish failed", "finish failed", productStateNeedsAction},
		{"finish-attention", "saved", state.FinishNeedsAttention, "finish needs review", "finish needs review", productStateNeedsAction},
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
			want := scenario.display + " (cleanup retry) (older saved data needs review)"
			if got := productListDisplayStatus(entry); got != want {
				t.Fatalf("display status=%q, want %q", got, want)
			}
		})
	}
}

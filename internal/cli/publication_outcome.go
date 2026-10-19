package cli

import (
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// publicationOutcome is a read-only projection of completed checkpoint
// membership. Recovery preserves work but does not put it on the branch.
// BranchCommitted is unknown when checkpoint state is unavailable.
type publicationOutcome struct {
	History               *finishHistoryStatusReport `json:"history"`
	Legacy                state.FinishLegacyCutover  `json:"legacy"`
	CheckpointAvailable   bool                       `json:"checkpoint_available"`
	Protected             bool                       `json:"protected"`
	PendingChanges        int                        `json:"pending_changes"`
	WorktreeChanges       int                        `json:"worktree_changes"`
	LaterChanges          int                        `json:"later_changes"`
	CountsKnown           bool                       `json:"counts_known"`
	Finish                *finishStatusReport        `json:"finish"`
	Resolved              bool                       `json:"-"`
	PendingClassification bool                       `json:"pending_classification"`
	BranchCommitted       *bool                      `json:"branch_committed"`
	BranchChanges         int                        `json:"branch_changes"`
	RecoveredChanges      int                        `json:"recovered_changes"`
	WaitingChanges        int                        `json:"waiting_changes"`
	ReasonCode            string                     `json:"reason_code,omitempty"`
	RetryAt               float64                    `json:"retry_at,omitempty"`
}

func checkpointOutcome(phase string, events, published, recovered int) string {
	if phase != "completed" {
		return phase
	}
	if events == 0 {
		return "saved"
	}
	if published == events {
		return "published"
	}
	if published+recovered == events {
		return "recovered"
	}
	return "waiting"
}

package cli

import (
	"context"
	"database/sql"
	"fmt"
)

// publicationOutcome is a read-only projection of completed checkpoint
// membership. Recovery preserves work but does not put it on the branch.
// BranchCommitted is unknown when checkpoint state is unavailable.
type publicationOutcome struct {
	BranchCommitted  *bool   `json:"branch_committed"`
	BranchChanges    int     `json:"branch_changes"`
	RecoveredChanges int     `json:"recovered_changes"`
	WaitingChanges   int     `json:"waiting_changes"`
	ReasonCode       string  `json:"reason_code,omitempty"`
	RetryAt          float64 `json:"retry_at,omitempty"`
}

func readPublicationOutcome(ctx context.Context, db *sql.DB, protected bool) (publicationOutcome, error) {
	var result publicationOutcome
	err := db.QueryRowContext(ctx, `
SELECT COALESCE(SUM(CASE WHEN state='published' THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN state='recovered' THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN state NOT IN ('published','recovered') THEN 1 ELSE 0 END),0)
FROM capture_events e
WHERE EXISTS (SELECT 1 FROM checkpoint_events ce JOIN checkpoints cp ON cp.id=ce.checkpoint_id
              WHERE ce.event_seq=e.seq AND cp.phase='completed')`).Scan(
		&result.BranchChanges, &result.RecoveredChanges, &result.WaitingChanges)
	if err != nil {
		return result, fmt.Errorf("publication outcome: %w", err)
	}
	committed := protected && result.WaitingChanges == 0 && result.RecoveredChanges == 0
	result.BranchCommitted = &committed
	return result, nil
}

func checkpointOutcome(phase string, events, published, recovered int) string {
	if phase != "completed" {
		return phase
	}
	if published == events {
		return "published"
	}
	if published+recovered == events {
		return "recovered"
	}
	return "waiting"
}

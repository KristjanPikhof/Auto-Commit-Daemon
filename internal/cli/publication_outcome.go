package cli

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

// publicationOutcome is a read-only projection of completed checkpoint
// membership. Recovery preserves work but does not put it on the branch.
// BranchCommitted is unknown when checkpoint state is unavailable.
type publicationOutcome struct {
	PendingClassification bool    `json:"pending_classification"`
	BranchCommitted       *bool   `json:"branch_committed"`
	BranchChanges         int     `json:"branch_changes"`
	RecoveredChanges      int     `json:"recovered_changes"`
	WaitingChanges        int     `json:"waiting_changes"`
	ReasonCode            string  `json:"reason_code,omitempty"`
	RetryAt               float64 `json:"retry_at,omitempty"`
}

func readPublicationOutcome(ctx context.Context, db *sql.DB, protected bool, repo string) (publicationOutcome, error) {
	branch, generation, known, err := currentWorktreeReplayPair(ctx, db, repo)
	if err != nil {
		return publicationOutcome{}, err
	}
	return readPublicationOutcomeForPair(ctx, db, protected, repo, branch, generation, known)
}

// Dashboard callers already resolved the live pair. Reuse that proof instead
// of starting the same Git processes again for every repository row.
func readPublicationOutcomeForPair(ctx context.Context, db *sql.DB, protected bool, repo, branch string, generation int64, known bool) (publicationOutcome, error) {
	var result publicationOutcome
	err := db.QueryRowContext(ctx, `
SELECT COALESCE(SUM(CASE WHEN state='published' AND branch_ref=? AND branch_generation=? THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN state='recovered' THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN state NOT IN ('published','recovered') AND branch_ref=? AND branch_generation=? THEN 1 ELSE 0 END),0)
FROM capture_events e
WHERE EXISTS (SELECT 1 FROM checkpoint_events ce JOIN checkpoints cp ON cp.id=ce.checkpoint_id
              WHERE ce.event_seq=e.seq AND cp.phase='completed')`, branch, generation, branch, generation).Scan(
		&result.BranchChanges, &result.RecoveredChanges, &result.WaitingChanges)
	if err != nil {
		return result, fmt.Errorf("publication outcome: %w", err)
	}
	pending, _, err := metaLookup(ctx, db, daemon.MetaKeyProtectionClassificationPending)
	if err != nil {
		return result, err
	}
	result.PendingClassification = pending == "true"
	if !known {
		return result, nil
	}
	committed := false
	if !protected || result.PendingClassification || result.WaitingChanges > 0 {
		result.BranchCommitted = &committed
		return result, nil
	}
	// Captured publication mappings and historical recovery rows are not
	// proof of today's branch after an external reset or recapture. Exact
	// tree equality is sufficient; exclusions or unavailable objects leave
	// the outcome unknown rather than inventing a publication result.
	var tree string
	if err := db.QueryRowContext(ctx, `SELECT tree_oid FROM checkpoints WHERE id=(SELECT value FROM daemon_meta WHERE key=?) AND phase='completed'`, daemon.MetaKeyProtectionCheckpointID).Scan(&tree); err != nil && err != sql.ErrNoRows {
		return result, err
	}
	if tree != "" {
		headTree, err := gitpkg.RevParse(ctx, repo, "HEAD^{tree}")
		if err == nil && headTree == tree {
			committed = true
			result.BranchCommitted = &committed
		}
	}

	return result, nil
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

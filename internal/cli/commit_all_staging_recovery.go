package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// A new consented request may preserve an old staging-blocked target and
// capture a replacement. It never changes that target's immutable approval.
// The caller holds the worker operation gate after checking the new preview.
func recoverCommitAllStagingReview(ctx context.Context, runtime *workerRuntime, branch string, generation int64) error {
	var id string
	err := runtime.db.ReadSQL().QueryRowContext(ctx, `
SELECT id FROM publication_drains
WHERE branch_ref=? AND branch_generation=? AND phase='needs_action'
  AND reason_code IN ('staging_changed','staging_approval_missing')
  AND staged_consent=1 AND staged_consumed=0
ORDER BY created_ts,id LIMIT 1`, branch, generation).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var first int64
	err = runtime.db.ReadSQL().QueryRowContext(ctx, `
SELECT seq FROM capture_events WHERE branch_ref=? AND branch_generation=?
  AND state IN ('pending','blocked_conflict','failed') ORDER BY seq LIMIT 1`, branch, generation).Scan(&first)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		result, err := daemon.ReconcileUnpublishedChain(ctx, runtime.worktree.Root, runtime.db, daemon.RecoveryReconcileOptions{
			GitDir: runtime.worktree.GitDir, BranchRef: branch, BranchGeneration: generation,
			FirstSeq: first, Trigger: "commit_all_staging_review", ArchiveOnly: true,
			InvalidateShadow: true, EvidenceLimit: state.CompletedBranchTransitionProofLimit,
		})
		if err != nil {
			return err
		}
		if !result.Handled {
			return errors.New("staging review could not prove preservation of the previous target")
		}
	}
	if _, err := state.ReconcileResolvedPublicationDrains(ctx, runtime.db, float64(time.Now().UnixNano())/1e9); err != nil {
		return err
	}
	drain, err := state.PublicationDrainByID(ctx, runtime.db, id)
	if err != nil {
		return err
	}
	if drain.Phase != state.PublicationDrainCompleted {
		return fmt.Errorf("staging review left previous target %s unresolved", id)
	}
	return nil
}

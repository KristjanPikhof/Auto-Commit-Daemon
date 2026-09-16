package state

import (
	"context"
	"fmt"
)

// CompleteReplacedStagingDrain retires a failed staging approval after its
// target has been preserved. A newly approved request owns the replacement;
// the old request must never claim that it consumed the user's index.
func CompleteReplacedStagingDrain(ctx context.Context, db *DB, id string, now float64) error {
	result, err := db.conn.ExecContext(ctx, `
UPDATE publication_drains AS d
SET phase='completed', published_event_count=target_event_count,
    reason_code='staging_review_replaced', last_error='',
    updated_ts=MAX(updated_ts,?), last_progress_ts=MAX(last_progress_ts,?), completed_ts=?
WHERE id=? AND phase='needs_action' AND staged_consent=1 AND staged_consumed=0
  AND reason_code IN ('staging_changed','staging_approval_missing')
  AND EXISTS (SELECT 1 FROM checkpoints c WHERE c.id=d.checkpoint_id AND c.phase='completed')
  AND target_event_count=(SELECT COUNT(*) FROM publication_drain_events de WHERE de.drain_id=d.id)
  AND target_event_count=(SELECT COUNT(*) FROM publication_drain_events de
      JOIN capture_events e ON e.seq=de.event_seq
      WHERE de.drain_id=d.id AND e.state IN ('published','recovered'))`, now, now, now, id)
	if err != nil {
		return fmt.Errorf("state: complete replaced staging review: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 1 {
		return nil
	}
	drain, err := PublicationDrainByID(ctx, db, id)
	if err != nil {
		return err
	}
	if drain.Phase == PublicationDrainCompleted && drain.ReasonCode == "staging_review_replaced" {
		return nil
	}
	return ErrPublicationDrainProgress
}

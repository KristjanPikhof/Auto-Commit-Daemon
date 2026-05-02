package state

import (
	"context"
	"fmt"
)

// Migrate brings the database forward from whatever PRAGMA user_version it
// currently reports up to SchemaVersion.
//
// v1 was the first acd release. v2 adds idempotent indexes through schemaDDL.
// v3 adds idx_capture_events_barrier (a covering index that keeps the
// PendingEvents barrier subquery off a full-table scan during long pauses).
// v4 adds idx_flush_requests_status_id so ClaimNextFlushRequest's
// pending-by-id scan stays index-backed after long uptime.
// Future migrations are append-only for daily_rollups (D9) — only ALTER TABLE
// ADD COLUMN. Schema-changing helpers belong here, not in db.go.
//
// Open's runBootstrap re-applies the idempotent schemaDDL whenever the
// stored user_version is below SchemaVersion, so simply bumping SchemaVersion
// and adding `CREATE INDEX IF NOT EXISTS` to schemaDDL is sufficient for
// pure-DDL migrations (such as v2→v3). Migrate is wired now so future phases
// requiring data backfill have a single entry point to extend.
func (d *DB) Migrate(ctx context.Context) error {
	cur, err := d.UserVersion(ctx)
	if err != nil {
		return err
	}
	if cur > SchemaVersion {
		return fmt.Errorf("state: db user_version=%d is newer than this binary's SchemaVersion=%d", cur, SchemaVersion)
	}
	if cur == SchemaVersion {
		return nil
	}
	// Open applies the idempotent schemaDDL for older databases before it stamps
	// SchemaVersion, so no explicit post-open migration step exists yet.
	return nil
}

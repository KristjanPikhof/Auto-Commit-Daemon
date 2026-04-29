package state

import (
	"context"
	"fmt"
)

// Migrate brings the database forward from whatever PRAGMA user_version it
// currently reports up to SchemaVersion.
//
// v1 was the first acd release. v2 adds idempotent indexes through schemaDDL.
// Future migrations are append-only for daily_rollups (D9) — only ALTER TABLE
// ADD COLUMN. Schema-changing helpers belong here, not in db.go.
//
// Open calls bootstrap which is itself idempotent for v1, so the daemon does
// not need to call Migrate explicitly today. Migrate is wired now so future
// phases have a single entry point to extend.
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

package state

import (
	"context"
	"fmt"
)

// Migrate brings the database forward from whatever PRAGMA user_version it
// currently reports up to SchemaVersion.
//
// v1 is the first acd release; there is no v0 to migrate from. Future
// migrations are append-only for daily_rollups (D9) — only ALTER TABLE ADD
// COLUMN. Schema-changing helpers belong here, not in db.go.
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
	// No < 1 path exists yet; bootstrap stamped the version in Open.
	return nil
}

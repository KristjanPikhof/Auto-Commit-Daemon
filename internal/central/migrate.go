package central

// Central stats DB migrations (§6.3, D9 anchor).
//
// MIGRATION POLICY — read before adding code below:
//
//   - The daily_rollups schema is the long-term backward-compat anchor for
//     the stats subsystem. Older binaries must remain able to read
//     databases written by newer ones for at least one major version.
//   - Therefore every future migration MUST be additive only:
//     `ALTER TABLE daily_rollups ADD COLUMN <name> <type> DEFAULT <const>`.
//   - You MAY NOT: drop columns, rename columns, reorder columns, change
//     a column's type, change a PRIMARY KEY, or remove a table.
//   - You MAY: add new tables, add new columns with NOT NULL defaults, add
//     new indexes (CREATE INDEX IF NOT EXISTS), add new rows to global_meta.
//
// global_meta is intentionally schemaless (key/value/updated_ts) — push new
// scalars there before adding columns elsewhere.
//
// Bumping StatsSchemaVersion requires:
//
//  1. Append the new ALTER TABLE statements to a new const, e.g. v2DDL.
//  2. Add a case in the version-step ladder below to apply them and stamp
//     the new user_version inside a single transaction.
//  3. Document the change in the changelog with the rationale.

import (
	"context"
	"fmt"
)

// Migrate brings the stats database forward from whatever PRAGMA user_version
// it currently reports to StatsSchemaVersion.
//
// v1 is the first acd release; there is no v0 to migrate from. Open() already
// runs the idempotent bootstrap, so callers do not strictly need to invoke
// Migrate today — but wiring the entry point now means future bumps land in
// one well-known location instead of being scattered across the package.
func (s *StatsDB) Migrate(ctx context.Context) error {
	cur, err := s.UserVersion(ctx)
	if err != nil {
		return err
	}
	if cur > StatsSchemaVersion {
		return fmt.Errorf("central: stats user_version=%d newer than this binary's StatsSchemaVersion=%d", cur, StatsSchemaVersion)
	}
	if cur == StatsSchemaVersion {
		return nil
	}
	// No < 1 path exists yet; bootstrap stamped the version in Open.
	return nil
}

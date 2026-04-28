package central

import (
	"context"
	"strings"
	"testing"
)

// openTestStats returns a freshly-opened StatsDB rooted at a per-test temp
// dir, so tests can run in isolation. Cannot use t.Parallel because
// rootsForTest calls t.Setenv (rootsForTest is shared with registry_test.go).
func openTestStats(t *testing.T) *StatsDB {
	t.Helper()
	roots := rootsForTest(t)
	s, err := Open(context.Background(), roots)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStatsDB_OpenCreatesSchema(t *testing.T) {
	s := openTestStats(t)
	ctx := context.Background()

	v, err := s.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion: %v", err)
	}
	if v != StatsSchemaVersion {
		t.Fatalf("user_version=%d, want %d", v, StatsSchemaVersion)
	}

	// journal_mode and busy_timeout should be live on every connection in
	// the pool because they are issued via _pragma= in the DSN.
	var jm string
	if err := s.conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&jm); err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if strings.ToLower(jm) != "wal" {
		t.Fatalf("journal_mode=%q, want wal", jm)
	}

	var bt int64
	if err := s.conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&bt); err != nil {
		t.Fatalf("busy_timeout: %v", err)
	}
	if bt != 5000 {
		t.Fatalf("busy_timeout=%d, want 5000", bt)
	}

	// Both anchor tables must exist.
	tables := map[string]bool{}
	rows, err := s.conn.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		tables[n] = true
	}
	for _, want := range []string{"daily_rollups", "global_meta"} {
		if !tables[want] {
			t.Fatalf("missing table %q (have %v)", want, tables)
		}
	}
}

func TestStatsDB_ReopenIdempotent(t *testing.T) {
	roots := rootsForTest(t)
	ctx := context.Background()

	s1, err := Open(ctx, roots)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// Re-opening must not fail with "table already exists" — bootstrap
	// uses CREATE TABLE IF NOT EXISTS.
	s2, err := Open(ctx, roots)
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer s2.Close()
	v, err := s2.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion #2: %v", err)
	}
	if v != StatsSchemaVersion {
		t.Fatalf("user_version=%d, want %d", v, StatsSchemaVersion)
	}
}

func TestStatsDB_InsertRollupOrIgnore(t *testing.T) {
	s := openTestStats(t)
	ctx := context.Background()

	row := DailyRollup{
		Day:                 "2026-04-28",
		RepoHash:            "abc123",
		RepoPath:            "/tmp/repo",
		EventsTotal:         100,
		CommitsTotal:        50,
		FilesChanged:        10,
		BytesChanged:        2048,
		ErrorsTotal:         1,
		SessionsSeen:        2,
		DaemonUptimeSeconds: 3600,
		AggregatedAt:        1714329600.5,
	}
	ok, err := s.InsertRollup(ctx, row)
	if err != nil {
		t.Fatalf("InsertRollup #1: %v", err)
	}
	if !ok {
		t.Fatal("first insert reported not inserted")
	}

	// Second insert with the same PK is a no-op (first-write-wins).
	row2 := row
	row2.EventsTotal = 9999 // different payload — must not overwrite
	ok, err = s.InsertRollup(ctx, row2)
	if err != nil {
		t.Fatalf("InsertRollup #2: %v", err)
	}
	if ok {
		t.Fatal("duplicate insert should return false")
	}

	// Verify the original row survived unchanged.
	got, err := s.ListRollupsSince(ctx, 0)
	if err != nil {
		t.Fatalf("ListRollupsSince: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("rows=%d, want 1", len(got))
	}
	if got[0].EventsTotal != 100 {
		t.Fatalf("events_total=%d, want 100 (or-ignore should not overwrite)", got[0].EventsTotal)
	}
}

func TestStatsDB_ListRollupsSinceOrdering(t *testing.T) {
	s := openTestStats(t)
	ctx := context.Background()

	rows := []DailyRollup{
		{Day: "2026-04-28", RepoHash: "b", RepoPath: "/b", AggregatedAt: 200},
		{Day: "2026-04-26", RepoHash: "a", RepoPath: "/a", AggregatedAt: 100},
		{Day: "2026-04-27", RepoHash: "a", RepoPath: "/a", AggregatedAt: 150},
		{Day: "2026-04-28", RepoHash: "a", RepoPath: "/a", AggregatedAt: 200},
	}
	for _, r := range rows {
		if _, err := s.InsertRollup(ctx, r); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	got, err := s.ListRollupsSince(ctx, 150)
	if err != nil {
		t.Fatalf("ListRollupsSince: %v", err)
	}
	// since=150 → drops the row with aggregated_at=100.
	if len(got) != 3 {
		t.Fatalf("rows=%d, want 3", len(got))
	}
	wantOrder := []struct {
		day, hash string
	}{
		{"2026-04-27", "a"},
		{"2026-04-28", "a"},
		{"2026-04-28", "b"},
	}
	for i, w := range wantOrder {
		if got[i].Day != w.day || got[i].RepoHash != w.hash {
			t.Fatalf("row %d = (%s, %s), want (%s, %s)", i, got[i].Day, got[i].RepoHash, w.day, w.hash)
		}
	}
}

func TestStatsDB_Meta(t *testing.T) {
	s := openTestStats(t)
	ctx := context.Background()

	// Missing key.
	_, ok, err := s.MetaGet(ctx, "missing")
	if err != nil {
		t.Fatalf("MetaGet missing: %v", err)
	}
	if ok {
		t.Fatal("missing key reported present")
	}

	// Set + get.
	if err := s.MetaSet(ctx, "last_aggregated_day", "2026-04-28", 1714329600); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	v, ok, err := s.MetaGet(ctx, "last_aggregated_day")
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	if !ok {
		t.Fatal("MetaGet reported absent after set")
	}
	if v != "2026-04-28" {
		t.Fatalf("value=%q, want 2026-04-28", v)
	}

	// Update existing key.
	if err := s.MetaSet(ctx, "last_aggregated_day", "2026-04-29", 1714416000); err != nil {
		t.Fatalf("MetaSet update: %v", err)
	}
	v, _, err = s.MetaGet(ctx, "last_aggregated_day")
	if err != nil {
		t.Fatalf("MetaGet updated: %v", err)
	}
	if v != "2026-04-29" {
		t.Fatalf("value=%q, want 2026-04-29 after update", v)
	}
}

// TestStatsDB_AlterTableAddColumn proves that an additive migration on the
// daily_rollups anchor table — the only kind we permit per the §6.3 policy —
// does not break existing reads. We open at v1, ALTER ADD COLUMN, reopen,
// and confirm both old and new shapes are queryable.
func TestStatsDB_AlterTableAddColumn(t *testing.T) {
	roots := rootsForTest(t)
	ctx := context.Background()

	s1, err := Open(ctx, roots)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	if _, err := s1.InsertRollup(ctx, DailyRollup{
		Day:          "2026-04-28",
		RepoHash:     "h1",
		RepoPath:     "/r",
		EventsTotal:  1,
		AggregatedAt: 1,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Simulate a future-version migration adding a forward-compatible column.
	if _, err := s1.conn.ExecContext(ctx,
		`ALTER TABLE daily_rollups ADD COLUMN test_added INTEGER NOT NULL DEFAULT 0`,
	); err != nil {
		t.Fatalf("ALTER ADD COLUMN: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// Reopen — the existing bootstrap CREATE TABLE IF NOT EXISTS must not
	// throw on the now-extended table.
	s2, err := Open(ctx, roots)
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer s2.Close()

	// Old-shape reads (the current API) still succeed.
	got, err := s2.ListRollupsSince(ctx, 0)
	if err != nil {
		t.Fatalf("ListRollupsSince: %v", err)
	}
	if len(got) != 1 || got[0].RepoHash != "h1" {
		t.Fatalf("got=%+v, want [{h1...}]", got)
	}

	// And the new column is reachable via raw SQL.
	var added int
	if err := s2.conn.QueryRowContext(ctx,
		`SELECT test_added FROM daily_rollups WHERE repo_hash = ?`, "h1",
	).Scan(&added); err != nil {
		t.Fatalf("SELECT new column: %v", err)
	}
	if added != 0 {
		t.Fatalf("test_added=%d, want 0 (default)", added)
	}

	// And new inserts (to a new PK) still work after the ALTER.
	if _, err := s2.InsertRollup(ctx, DailyRollup{
		Day:          "2026-04-29",
		RepoHash:     "h2",
		RepoPath:     "/r2",
		EventsTotal:  5,
		AggregatedAt: 2,
	}); err != nil {
		t.Fatalf("post-alter insert: %v", err)
	}
}

func TestStatsDB_MigrateNoOpAtCurrent(t *testing.T) {
	s := openTestStats(t)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
}

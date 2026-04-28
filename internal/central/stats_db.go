package central

// Central stats DB (~/.local/share/acd/stats.db, §6.3).
//
// stats.db mirrors per-repo daily_rollups but is keyed by repo_hash so a
// repo whose path has moved on disk still joins cleanly across days. The
// aggregator (§3.3) copies one row per (day, repo) into this DB nightly.
//
// Concurrency model: a single nightly aggregator process writes; CLI
// commands (`acd stats`) read. WAL + busy_timeout=5000 lets readers and
// the writer coexist without deadlock. We use modernc.org/sqlite (D16 —
// pure Go, no cgo) so the binary stays statically linkable across all
// supported platforms.
//
// Schema policy: append-only across versions (D9 / §6.3). Future migrations
// may only `ALTER TABLE ADD COLUMN` — never drop, rename, or reorder.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"

	// modernc.org/sqlite is the pure-Go driver mandated by D16 (zero cgo).
	// Keep this import in lockstep with internal/state/db.go.
	_ "modernc.org/sqlite"
)

// statsDriverName is the registered driver name for modernc.org/sqlite.
const statsDriverName = "sqlite"

// StatsSchemaVersion is the current PRAGMA user_version for stats.db. v1 is
// the first acd release; bumping requires a migration in migrate.go.
const StatsSchemaVersion = 1

// statsSchemaDDL is the canonical stats.db schema (§6.3). Every CREATE uses
// IF NOT EXISTS so the bootstrap is idempotent — re-opening an existing
// database is a no-op.
const statsSchemaDDL = `
CREATE TABLE IF NOT EXISTS daily_rollups(
    day                   TEXT NOT NULL,
    repo_hash             TEXT NOT NULL,
    repo_path             TEXT NOT NULL,
    events_total          INTEGER NOT NULL DEFAULT 0,
    commits_total         INTEGER NOT NULL DEFAULT 0,
    files_changed         INTEGER NOT NULL DEFAULT 0,
    bytes_changed         INTEGER NOT NULL DEFAULT 0,
    errors_total          INTEGER NOT NULL DEFAULT 0,
    sessions_seen         INTEGER NOT NULL DEFAULT 0,
    daemon_uptime_seconds INTEGER NOT NULL DEFAULT 0,
    aggregated_at         REAL NOT NULL,
    PRIMARY KEY (day, repo_hash)
);

CREATE TABLE IF NOT EXISTS global_meta(
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_ts  REAL NOT NULL
);
`

// StatsDB wraps the central stats *sql.DB along with the file path. The
// underlying *sql.DB is safe for concurrent use (modernc.org/sqlite serialises
// writers internally and WAL keeps readers non-blocking).
type StatsDB struct {
	conn *sql.DB
	path string
}

// Path returns the absolute path to the underlying stats.db file.
func (s *StatsDB) Path() string { return s.path }

// SQL returns the underlying *sql.DB so other central-package files (e.g.
// aggregate.go) can compose queries without re-piping every helper. External
// callers should prefer the typed methods.
func (s *StatsDB) SQL() *sql.DB { return s.conn }

// Close releases the underlying database handle. Safe to call multiple times.
func (s *StatsDB) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

// OpenAt opens (or creates) a central stats database at the given absolute
// path. Useful for callers (e.g. the daemon run loop) that already have a
// resolved StatsDBPath() and don't want to re-derive paths.Roots.
//
// Lifecycle and PRAGMA matrix are identical to Open — the only difference
// is the path source.
func OpenAt(ctx context.Context, dbPath string) (*StatsDB, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("central: OpenAt: empty path")
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, fmt.Errorf("central: mkdir stats parent: %w", err)
	}
	dsn := buildStatsDSN(dbPath)
	conn, err := sql.Open(statsDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("central: sql.Open stats: %w", err)
	}
	conn.SetMaxOpenConns(8)
	conn.SetMaxIdleConns(4)
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("central: ping stats: %w", err)
	}
	s := &StatsDB{conn: conn, path: dbPath}
	if err := s.bootstrap(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return s, nil
}

// Open opens (or creates) the central stats database under roots.Share. It
//
//  1. mkdir -p the share directory with 0o700,
//  2. opens the SQLite database with WAL + NORMAL sync + busy_timeout=5000,
//  3. applies statsSchemaDDL inside a transaction (idempotent),
//  4. stamps PRAGMA user_version = StatsSchemaVersion.
//
// Re-opening an existing database is a no-op for schema purposes.
func Open(ctx context.Context, roots paths.Roots) (*StatsDB, error) {
	dbPath := roots.StatsDBPath()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, fmt.Errorf("central: mkdir stats parent: %w", err)
	}

	dsn := buildStatsDSN(dbPath)
	conn, err := sql.Open(statsDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("central: sql.Open stats: %w", err)
	}
	// Stats DB sees one writer (aggregator) and a handful of readers (CLI).
	// Keep the pool conservative — same shape as the per-repo state DB.
	conn.SetMaxOpenConns(8)
	conn.SetMaxIdleConns(4)

	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("central: ping stats: %w", err)
	}

	s := &StatsDB{conn: conn, path: dbPath}
	if err := s.bootstrap(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return s, nil
}

// buildStatsDSN composes the DSN for stats.db. PRAGMAs match per-repo state
// (§6.1) so the operational characteristics (WAL, busy_timeout) are identical
// — anyone debugging a stuck DB sees the same pragma surface area.
func buildStatsDSN(dbPath string) string {
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "busy_timeout(5000)")
	return "file:" + dbPath + "?" + q.Encode()
}

// bootstrap applies DDL + stamps user_version. Idempotent.
func (s *StatsDB) bootstrap(ctx context.Context) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("central: begin stats bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, statsSchemaDDL); err != nil {
		return fmt.Errorf("central: apply stats schema: %w", err)
	}

	pragma := fmt.Sprintf("PRAGMA user_version = %d", StatsSchemaVersion)
	if _, err := tx.ExecContext(ctx, pragma); err != nil {
		return fmt.Errorf("central: stamp stats user_version: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("central: commit stats bootstrap: %w", err)
	}
	return nil
}

// UserVersion reads PRAGMA user_version.
func (s *StatsDB) UserVersion(ctx context.Context) (int, error) {
	var v int
	if err := s.conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return 0, fmt.Errorf("central: read stats user_version: %w", err)
	}
	return v, nil
}

// DailyRollup is one row of stats.db daily_rollups (§6.3). Schema is the
// long-term backward-compat anchor: this struct grows by adding fields
// matching new ALTER TABLE ADD COLUMN migrations — never reorder.
type DailyRollup struct {
	Day                 string // YYYY-MM-DD
	RepoHash            string // stable cross-repo identity
	RepoPath            string // last-known display path
	EventsTotal         int64
	CommitsTotal        int64
	FilesChanged        int64
	BytesChanged        int64
	ErrorsTotal         int64
	SessionsSeen        int64
	DaemonUptimeSeconds int64
	AggregatedAt        float64 // unix seconds with sub-second precision
}

// InsertRollup writes a daily_rollups row. First-write-wins: a second insert
// with the same (day, repo_hash) PK is a no-op. This matches the per-repo
// state-lane semantics (`InsertOrIgnore`) and keeps aggregator retries safe.
func (s *StatsDB) InsertRollup(ctx context.Context, r DailyRollup) (inserted bool, err error) {
	if r.Day == "" || r.RepoHash == "" {
		return false, fmt.Errorf("central: InsertRollup: day + repo_hash required")
	}
	const q = `
INSERT OR IGNORE INTO daily_rollups(
    day, repo_hash, repo_path, events_total, commits_total, files_changed,
    bytes_changed, errors_total, sessions_seen, daemon_uptime_seconds, aggregated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	res, err := s.conn.ExecContext(ctx, q,
		r.Day, r.RepoHash, r.RepoPath, r.EventsTotal, r.CommitsTotal, r.FilesChanged,
		r.BytesChanged, r.ErrorsTotal, r.SessionsSeen, r.DaemonUptimeSeconds, r.AggregatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("central: insert stats rollup: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("central: insert stats rollup rows: %w", err)
	}
	return n == 1, nil
}

// ListRollupsSince returns every row with aggregated_at >= sinceUnix, sorted
// by day asc, repo_hash asc. sinceUnix is unix seconds (matches aggregated_at).
//
// Used by `acd stats` to render multi-repo summaries; consumers can window
// further by day in-memory.
func (s *StatsDB) ListRollupsSince(ctx context.Context, sinceUnix int64) ([]DailyRollup, error) {
	const q = `
SELECT day, repo_hash, repo_path, events_total, commits_total, files_changed,
       bytes_changed, errors_total, sessions_seen, daemon_uptime_seconds, aggregated_at
FROM daily_rollups
WHERE aggregated_at >= ?
ORDER BY day ASC, repo_hash ASC`
	rows, err := s.conn.QueryContext(ctx, q, float64(sinceUnix))
	if err != nil {
		return nil, fmt.Errorf("central: list stats rollups: %w", err)
	}
	defer rows.Close()

	var out []DailyRollup
	for rows.Next() {
		var r DailyRollup
		if err := rows.Scan(&r.Day, &r.RepoHash, &r.RepoPath, &r.EventsTotal, &r.CommitsTotal,
			&r.FilesChanged, &r.BytesChanged, &r.ErrorsTotal, &r.SessionsSeen,
			&r.DaemonUptimeSeconds, &r.AggregatedAt); err != nil {
			return nil, fmt.Errorf("central: scan stats rollup: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("central: iter stats rollups: %w", err)
	}
	return out, nil
}

// MetaGet reads a single key from global_meta. Returns ("", false, nil) when
// the key is absent. Mirrors state.MetaGet.
func (s *StatsDB) MetaGet(ctx context.Context, key string) (string, bool, error) {
	if key == "" {
		return "", false, fmt.Errorf("central: MetaGet: empty key")
	}
	var v string
	err := s.conn.QueryRowContext(ctx, `SELECT value FROM global_meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("central: meta get %q: %w", key, err)
	}
	return v, true, nil
}

// MetaSet upserts a key/value pair. updated_ts uses unix seconds with
// sub-second precision (REAL column).
func (s *StatsDB) MetaSet(ctx context.Context, key, value string, nowUnix float64) error {
	if key == "" {
		return fmt.Errorf("central: MetaSet: empty key")
	}
	const q = `
INSERT INTO global_meta(key, value, updated_ts) VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`
	if _, err := s.conn.ExecContext(ctx, q, key, value, nowUnix); err != nil {
		return fmt.Errorf("central: meta set %q: %w", key, err)
	}
	return nil
}

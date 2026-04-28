package state

import (
	"context"
	"fmt"
)

// DailyRollup is one row of daily_rollups (§6.1). The schema is the long-term
// backward-compat anchor (D9): future migrations may only ALTER TABLE ADD
// COLUMN. To enforce that semantically, this package exposes only an
// idempotent insert + a read API. **No UPDATE or DELETE** is exposed for
// daily_rollups, deliberately.
//
// If a day's row needs to land twice (cron jitter, repeat aggregator run), the
// INSERT OR IGNORE guarantees the first row wins. That is the correct
// behaviour for a rollup anchor: numbers from the first run are sticky.
type DailyRollup struct {
	Day                 string // YYYY-MM-DD
	RepoRoot            string
	EventsTotal         int64
	CommitsTotal        int64
	FilesChanged        int64
	BytesChanged        int64
	ErrorsTotal         int64
	SessionsSeen        int64
	DaemonUptimeSeconds int64
}

// InsertDailyRollup writes a daily_rollups row. If a row already exists for
// (day, repo_root) it is left untouched and inserted=false is returned. There
// is intentionally no UpdateDailyRollup helper — see comment on DailyRollup.
func InsertDailyRollup(ctx context.Context, d *DB, r DailyRollup) (inserted bool, err error) {
	if r.Day == "" || r.RepoRoot == "" {
		return false, fmt.Errorf("state: InsertDailyRollup: day + repo_root required")
	}
	const q = `
INSERT OR IGNORE INTO daily_rollups(
    day, repo_root, events_total, commits_total, files_changed,
    bytes_changed, errors_total, sessions_seen, daemon_uptime_seconds
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	res, err := d.conn.ExecContext(ctx, q,
		r.Day, r.RepoRoot, r.EventsTotal, r.CommitsTotal, r.FilesChanged,
		r.BytesChanged, r.ErrorsTotal, r.SessionsSeen, r.DaemonUptimeSeconds,
	)
	if err != nil {
		return false, fmt.Errorf("state: insert daily rollup: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: insert daily rollup rows: %w", err)
	}
	return n == 1, nil
}

// ListDailyRollupsSince returns every row whose day >= sinceDay (lexicographic
// compare works because day is YYYY-MM-DD). Used by the central-stats
// aggregator (§6.3).
func ListDailyRollupsSince(ctx context.Context, d *DB, sinceDay string) ([]DailyRollup, error) {
	const q = `
SELECT day, repo_root, events_total, commits_total, files_changed,
       bytes_changed, errors_total, sessions_seen, daemon_uptime_seconds
FROM daily_rollups WHERE day >= ? ORDER BY day ASC, repo_root ASC`
	rows, err := d.conn.QueryContext(ctx, q, sinceDay)
	if err != nil {
		return nil, fmt.Errorf("state: list daily rollups: %w", err)
	}
	defer rows.Close()
	var out []DailyRollup
	for rows.Next() {
		var r DailyRollup
		if err := rows.Scan(&r.Day, &r.RepoRoot, &r.EventsTotal, &r.CommitsTotal, &r.FilesChanged,
			&r.BytesChanged, &r.ErrorsTotal, &r.SessionsSeen, &r.DaemonUptimeSeconds); err != nil {
			return nil, fmt.Errorf("state: scan daily rollup: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter daily rollups: %w", err)
	}
	return out, nil
}

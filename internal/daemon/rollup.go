// Package daemon — per-repo daily rollup aggregator (§8.10).
//
// Once per UTC day boundary, RunDailyRollup walks from the day after the
// last successfully-rolled day through yesterday (inclusive), and writes
// one daily_rollups row per day into the per-repo state.db.
//
// daily_rollups is the long-term backward-compat anchor (D9 / §6.1) and
// only supports INSERT OR IGNORE. Re-running RunDailyRollup is a no-op for
// any day already rolled. The aggregator advances the
// "rollup.last_day" daemon_meta key only after a day's row is committed,
// so a crash mid-day re-rolls that day next iteration without losing or
// duplicating numbers.
package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// rollup.last_day stores YYYY-MM-DD of the most recent UTC day that has a
// daily_rollups row. Empty / missing means "no day has been rolled yet" —
// the aggregator falls back to the oldest event day in capture_events.
const metaRollupLastDay = "rollup.last_day"

// rollup.last_error_at records the unix-seconds timestamp of the last
// failed RunDailyRollup attempt. It's diagnostic only — the run loop never
// reads it back.
const metaRollupLastErrorAt = "rollup.last_error_at"

// dayLayout is the canonical UTC day key (YYYY-MM-DD). Lex order ==
// chronological order, which is why daily_rollups uses it as the primary
// key segment.
const dayLayout = "2006-01-02"

// RunDailyRollupOpts tunes RunDailyRollup. Zero value = production
// defaults (real time.Now, repo path inferred from caller).
type RunDailyRollupOpts struct {
	// RepoPath is stamped into daily_rollups.repo_root for cross-repo
	// joins. Required.
	RepoPath string
	// Now lets tests inject a fake clock. Nil falls back to time.Now.
	Now func() time.Time
}

// RunDailyRollup aggregates capture_events into daily_rollups for every
// completed UTC day from "rollup.last_day"+1 through yesterday. Returns
// the number of new rows written.
//
// Idempotency: the per-repo daily_rollups table uses INSERT OR IGNORE on
// (day, repo_root); re-running on a day that already has a row is a
// no-op. rollup.last_day is advanced one day at a time, only after the
// row for that day commits successfully — so a crash mid-loop re-rolls
// the unfinished day next time.
func RunDailyRollup(ctx context.Context, db *state.DB, opts RunDailyRollupOpts) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("daemon: RunDailyRollup: nil DB")
	}
	if opts.RepoPath == "" {
		return 0, fmt.Errorf("daemon: RunDailyRollup: empty RepoPath")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	// Determine "first day to roll" = last_day + 1, or the oldest event
	// day if no rollup has happened yet. If there are no events at all,
	// nothing to do.
	startDay, ok, err := nextDayToRoll(ctx, db, now())
	if err != nil {
		return 0, fmt.Errorf("daemon: rollup: resolve start day: %w", err)
	}
	if !ok {
		return 0, nil
	}

	// "yesterday UTC" is the last completed day. Today is in-flight; we
	// never roll it up because more events may arrive before midnight.
	yesterday := now().UTC().Add(-24 * time.Hour).Format(dayLayout)
	if startDay > yesterday {
		return 0, nil
	}

	written := 0
	day := startDay
	for day <= yesterday {
		row, err := aggregateDay(ctx, db, opts.RepoPath, day)
		if err != nil {
			return written, fmt.Errorf("daemon: rollup %s: %w", day, err)
		}
		ins, err := state.InsertDailyRollup(ctx, db, row)
		if err != nil {
			return written, fmt.Errorf("daemon: rollup insert %s: %w", day, err)
		}
		if ins {
			written++
		}
		// Advance last_day one day at a time so a crash here re-rolls
		// the same day on the next iteration (the INSERT OR IGNORE
		// keeps the original row).
		if err := state.MetaSet(ctx, db, metaRollupLastDay, day); err != nil {
			return written, fmt.Errorf("daemon: rollup meta-set %s: %w", day, err)
		}
		day = nextDay(day)
	}
	return written, nil
}

// nextDayToRoll returns the first UTC day that needs rollup, or false if
// nothing is to be rolled (no events at all). When rollup.last_day is set,
// the result is last_day+1. Otherwise it's the day of the oldest event.
func nextDayToRoll(ctx context.Context, db *state.DB, now time.Time) (string, bool, error) {
	last, present, err := state.MetaGet(ctx, db, metaRollupLastDay)
	if err != nil {
		return "", false, err
	}
	if present && last != "" {
		return nextDay(last), true, nil
	}
	// No rollup yet — start from the oldest captured day.
	var oldestTS sql.NullFloat64
	err = db.SQL().QueryRowContext(ctx,
		`SELECT MIN(captured_ts) FROM capture_events`).Scan(&oldestTS)
	if err != nil {
		return "", false, err
	}
	if !oldestTS.Valid {
		return "", false, nil
	}
	d := time.Unix(int64(oldestTS.Float64), 0).UTC().Format(dayLayout)
	return d, true, nil
}

// nextDay returns the YYYY-MM-DD that follows day. day MUST parse with
// dayLayout — caller should never pass arbitrary input.
func nextDay(day string) string {
	t, err := time.Parse(dayLayout, day)
	if err != nil {
		// Defensive fallback — never silently corrupt the cursor.
		return day
	}
	return t.Add(24 * time.Hour).Format(dayLayout)
}

// dayBounds returns [startUnix, endUnix) for the given UTC day key. Both
// are unix seconds (matching capture_events.captured_ts / published_ts).
func dayBounds(day string) (float64, float64, error) {
	t, err := time.Parse(dayLayout, day)
	if err != nil {
		return 0, 0, fmt.Errorf("parse day %q: %w", day, err)
	}
	start := t.UTC()
	end := start.Add(24 * time.Hour)
	return float64(start.Unix()), float64(end.Unix()), nil
}

// aggregateDay computes a DailyRollup struct for the given UTC day by
// scanning capture_events. The published_ts boundary is preferred (the
// row's "completed" moment); pending/failed rows fall back to
// captured_ts so they still attribute to the day they arrived.
func aggregateDay(ctx context.Context, db *state.DB, repoPath, day string) (state.DailyRollup, error) {
	startTS, endTS, err := dayBounds(day)
	if err != nil {
		return state.DailyRollup{}, err
	}

	const q = `
SELECT
    COUNT(*) AS events_total,
    COALESCE(SUM(CASE WHEN state = 'published' AND commit_oid IS NOT NULL AND commit_oid != ''
                      THEN 1 ELSE 0 END), 0) AS commits_total,
    COUNT(DISTINCT path) AS files_changed,
    COALESCE(SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END), 0) AS errors_total
FROM capture_events
WHERE COALESCE(published_ts, captured_ts) >= ?
  AND COALESCE(published_ts, captured_ts) <  ?`

	var (
		events, commits, files, errs int64
	)
	if err := db.SQL().QueryRowContext(ctx, q, startTS, endTS).
		Scan(&events, &commits, &files, &errs); err != nil {
		return state.DailyRollup{}, fmt.Errorf("aggregate scan: %w", err)
	}

	// bytes_changed and daemon_uptime_seconds aren't tracked by the
	// capture pipeline yet — they stay 0 until the schema grows the
	// supporting columns (D9 mandates ALTER TABLE ADD COLUMN only).
	// sessions_seen reflects the count of distinct harnesses currently
	// registered, which is the closest proxy available without a
	// per-day client log.
	var sessions int64
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT harness) FROM daemon_clients`).Scan(&sessions); err != nil {
		return state.DailyRollup{}, fmt.Errorf("sessions scan: %w", err)
	}

	return state.DailyRollup{
		Day:                 day,
		RepoRoot:            repoPath,
		EventsTotal:         events,
		CommitsTotal:        commits,
		FilesChanged:        files,
		BytesChanged:        0,
		ErrorsTotal:         errs,
		SessionsSeen:        sessions,
		DaemonUptimeSeconds: 0,
	}, nil
}

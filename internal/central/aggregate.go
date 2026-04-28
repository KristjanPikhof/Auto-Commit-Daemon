// Package central — push per-repo daily_rollups into central stats.db (§8.10).
//
// PushRollupsToCentral reads every per-repo daily_rollups row newer than
// "rollup.central_pushed_through" and INSERT-OR-IGNOREs it into the
// central stats.db keyed by repo_hash. Re-running is a no-op thanks to
// the (day, repo_hash) PK on stats.db daily_rollups.
package central

import (
	"context"
	"fmt"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// metaRollupCentralPushedThrough stores the YYYY-MM-DD of the most
// recent day successfully pushed to the central stats.db. Empty / missing
// means "nothing pushed yet" — the aggregator pushes everything in
// daily_rollups.
const metaRollupCentralPushedThrough = "rollup.central_pushed_through"

// dayLayout is the canonical UTC day key. Mirrors daemon/rollup.go but is
// duplicated here so this package has zero dependency on internal/daemon.
const dayLayout = "2006-01-02"

// PushRollupsToCentral copies new per-repo daily_rollups rows into the
// central stats.db keyed by repoHash + repoPath. Returns the number of
// rows inserted (rows already present are no-ops thanks to INSERT OR
// IGNORE on (day, repo_hash) in stats.db).
//
// Idempotency: a second call with no new per-repo rows is a no-op (returns
// 0). The "rollup.central_pushed_through" cursor advances only after the
// stats.db inserts succeed.
func PushRollupsToCentral(
	ctx context.Context,
	perRepoDB *state.DB,
	stats *StatsDB,
	repoHash, repoPath string,
) (int, error) {
	if perRepoDB == nil {
		return 0, fmt.Errorf("central: PushRollupsToCentral: nil per-repo DB")
	}
	if stats == nil {
		return 0, fmt.Errorf("central: PushRollupsToCentral: nil stats DB")
	}
	if repoHash == "" {
		return 0, fmt.Errorf("central: PushRollupsToCentral: empty repo_hash")
	}

	// Determine "since day" = central_pushed_through + 1, else "" which
	// ListDailyRollupsSince treats as "everything".
	since, present, err := state.MetaGet(ctx, perRepoDB, metaRollupCentralPushedThrough)
	if err != nil {
		return 0, fmt.Errorf("central: read pushed-through: %w", err)
	}
	startDay := ""
	if present && since != "" {
		startDay = nextDayCentral(since)
	}

	rows, err := state.ListDailyRollupsSince(ctx, perRepoDB, startDay)
	if err != nil {
		return 0, fmt.Errorf("central: list per-repo rollups: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	now := float64(time.Now().Unix())
	pushed := 0
	maxDay := since
	for _, r := range rows {
		ins, err := stats.InsertRollup(ctx, DailyRollup{
			Day:                 r.Day,
			RepoHash:            repoHash,
			RepoPath:            repoPath,
			EventsTotal:         r.EventsTotal,
			CommitsTotal:        r.CommitsTotal,
			FilesChanged:        r.FilesChanged,
			BytesChanged:        r.BytesChanged,
			ErrorsTotal:         r.ErrorsTotal,
			SessionsSeen:        r.SessionsSeen,
			DaemonUptimeSeconds: r.DaemonUptimeSeconds,
			AggregatedAt:        now,
		})
		if err != nil {
			return pushed, fmt.Errorf("central: insert stats day %s: %w", r.Day, err)
		}
		if ins {
			pushed++
		}
		if r.Day > maxDay {
			maxDay = r.Day
		}
	}

	if maxDay != "" && maxDay != since {
		if err := state.MetaSet(ctx, perRepoDB, metaRollupCentralPushedThrough, maxDay); err != nil {
			return pushed, fmt.Errorf("central: meta-set pushed-through: %w", err)
		}
	}
	return pushed, nil
}

// nextDayCentral returns the day after `day`. Mirrors the helper in
// daemon/rollup.go; duplicated to keep packages independent.
func nextDayCentral(day string) string {
	t, err := time.Parse(dayLayout, day)
	if err != nil {
		return day
	}
	return t.Add(24 * time.Hour).Format(dayLayout)
}

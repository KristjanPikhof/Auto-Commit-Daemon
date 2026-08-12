package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
)

// LegacyWorkSummary is the source-only v19 safety projection used when a
// disabled worktree is missing during the global cutover.
type LegacyWorkSummary struct {
	Unpublished     int
	Terminal        int
	OpenPublication int
}

func ReadLegacyWorkSummary(ctx context.Context, dbPath string) (LegacyWorkSummary, error) {
	var summary LegacyWorkSummary
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		return summary, nil
	} else if err != nil {
		return summary, err
	}
	q := url.Values{}
	q.Add("mode", "ro")
	q.Add("_pragma", "busy_timeout(5000)")
	db, err := sql.Open(driverName, "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return summary, err
	}
	defer db.Close()
	if err := db.QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN state IN ('pending','blocked_conflict','failed') THEN 1 ELSE 0 END),0),
  COALESCE(SUM(CASE WHEN state IN ('blocked_conflict','failed') THEN 1 ELSE 0 END),0)
FROM capture_events`).Scan(&summary.Unpublished, &summary.Terminal); err != nil {
		return summary, fmt.Errorf("state: legacy work summary: %w", err)
	}
	var hasJournal int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='self_publications'`).Scan(&hasJournal); err != nil {
		return summary, err
	}
	if hasJournal > 0 {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM self_publications WHERE phase NOT IN ('completed','abandoned')`).Scan(&summary.OpenPublication); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

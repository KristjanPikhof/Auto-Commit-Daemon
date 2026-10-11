package state

import (
	"context"
	"database/sql"
	"strings"
)

type legacyQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// LegacyTableExists inspects optional upgraded evidence without creating it.
func LegacyTableExists(ctx context.Context, conn *sql.DB, table string) (bool, error) {
	return legacyTableExists(ctx, conn, table)
}

func legacyTableExists(ctx context.Context, conn legacyQueryer, table string) (bool, error) {
	rows, err := conn.QueryContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)", table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var exists bool
	if rows.Next() {
		err = rows.Scan(&exists)
	}
	if err == nil {
		err = rows.Err()
	}
	return exists, err
}

type legacySelect struct{ table, query string }

func legacyUnion(ctx context.Context, conn legacyQueryer, selections []legacySelect) (string, error) {
	var parts []string
	for _, s := range selections {
		exists, err := legacyTableExists(ctx, conn, s.table)
		if err != nil {
			return "", err
		}
		if exists {
			parts = append(parts, s.query)
		}
	}
	return strings.Join(parts, " UNION ALL "), nil
}

func legacyCaptureCount(ctx context.Context, conn legacyQueryer, where string) (int64, error) {
	exists, err := legacyTableExists(ctx, conn, "capture_events")
	if err != nil || !exists {
		return 0, err
	}
	rows, err := conn.QueryContext(ctx, "SELECT COUNT(*) FROM capture_events "+where)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count int64
	if rows.Next() {
		err = rows.Scan(&count)
	}
	if err == nil {
		err = rows.Err()
	}
	return count, err
}

package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// MetaGet reads a single key from daemon_meta. Returns ("", false, nil) when
// the key is absent. The daemon uses this for the branch-generation token,
// the daemon fingerprint, and other small scalars that do not deserve their
// own table.
func MetaGet(ctx context.Context, d *DB, key string) (string, bool, error) {
	if key == "" {
		return "", false, fmt.Errorf("state: MetaGet: empty key")
	}
	var v string
	err := d.conn.QueryRowContext(ctx, `SELECT value FROM daemon_meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("state: meta get %q: %w", key, err)
	}
	return v, true, nil
}

// MetaSet upserts a key/value pair in daemon_meta with updated_ts=now.
func MetaSet(ctx context.Context, d *DB, key, value string) error {
	if key == "" {
		return fmt.Errorf("state: MetaSet: empty key")
	}
	const q = `
INSERT INTO daemon_meta(key, value, updated_ts) VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`
	if _, err := d.conn.ExecContext(ctx, q, key, value, nowSeconds()); err != nil {
		return fmt.Errorf("state: meta set %q: %w", key, err)
	}
	return nil
}

// MetaDelete removes a key. Returns whether the key was present.
func MetaDelete(ctx context.Context, d *DB, key string) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("state: MetaDelete: empty key")
	}
	res, err := d.conn.ExecContext(ctx, `DELETE FROM daemon_meta WHERE key = ?`, key)
	if err != nil {
		return false, fmt.Errorf("state: meta delete %q: %w", key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: meta delete rows: %w", err)
	}
	return n > 0, nil
}

package state

import (
	"context"
	"database/sql"
	"encoding/json"
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
	err := d.readSQL().QueryRowContext(ctx, `SELECT value FROM daemon_meta WHERE key = ?`, key).Scan(&v)
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

// MetaSetMany upserts multiple daemon_meta key/value pairs in a single
// transaction. All rows share the same updated_ts timestamp (now) so they are
// observed atomically by readers — either every key in the batch is present
// at the new value, or none of them are.
//
// The single-transaction shape matters at runtime: the daemon's per-tick run
// loop frequently stamps 2-4 small meta keys back-to-back (e.g. fsnotify
// diagnostics, operation-in-progress transitions, branch-token transitions).
// Under SQLite's busy_timeout=5s, each individual MetaSet call may block on
// a contending writer; stacking N back-to-back calls per tick can amplify a
// single contention episode into N×5s = up to 30s tick latency. Folding the
// stamps into one transaction collapses N busy retries into 1.
//
// Empty pairs is a no-op. Empty keys are rejected (matches MetaSet
// validation). The transaction is rolled back on any per-key error so the
// daemon never observes a half-applied batch.
func MetaSetMany(ctx context.Context, d *DB, pairs map[string]string) error {
	if len(pairs) == 0 {
		return nil
	}
	for k := range pairs {
		if k == "" {
			return fmt.Errorf("state: MetaSetMany: empty key")
		}
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: meta set many: begin tx: %w", err)
	}
	const q = `
INSERT INTO daemon_meta(key, value, updated_ts) VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`
	ts := nowSeconds()
	for k, v := range pairs {
		if _, err := tx.ExecContext(ctx, q, k, v, ts); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("state: meta set many %q: %w", k, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: meta set many: commit: %w", err)
	}
	return nil
}

// MetaSetJSON marshals value as JSON and stores it under key in daemon_meta.
func MetaSetJSON(ctx context.Context, d *DB, key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("state: meta json marshal %q: %w", key, err)
	}
	return MetaSet(ctx, d, key, string(b))
}

// MetaGetJSON reads key from daemon_meta and unmarshals it into dst. It
// returns false when the key is absent.
func MetaGetJSON(ctx context.Context, d *DB, key string, dst any) (bool, error) {
	v, ok, err := MetaGet(ctx, d, key)
	if err != nil || !ok {
		return ok, err
	}
	if err := json.Unmarshal([]byte(v), dst); err != nil {
		return true, fmt.Errorf("state: meta json unmarshal %q: %w", key, err)
	}
	return true, nil
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

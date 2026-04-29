package state

import (
	"context"
	"database/sql"
	"fmt"
)

// Client is one row of the daemon_clients refcount table (§6.1).
//
// session_id is the universal key. harness identifies the source (claude-code,
// codex, opencode, pi, shell, ...). watch_pid + watch_fp form the fast-path
// liveness probe (D20): when the OS confirms watch_pid is dead, the client is
// expired immediately rather than waiting for ACD_CLIENT_TTL_SECONDS (D21).
type Client struct {
	SessionID    string
	Harness      string
	WatchPID     sql.NullInt64
	WatchFP      sql.NullString
	RegisteredTS float64
	LastSeenTS   float64
}

// RegisterClient inserts (or refreshes) a client row. registered_ts is set on
// insert only; last_seen_ts is updated on every call so this doubles as a
// "register if missing, otherwise touch" helper for `acd start`.
func RegisterClient(ctx context.Context, d *DB, c Client) error {
	if c.SessionID == "" {
		return fmt.Errorf("state: RegisterClient: empty session_id")
	}
	if c.Harness == "" {
		return fmt.Errorf("state: RegisterClient: empty harness")
	}
	now := nowSeconds()
	if c.RegisteredTS == 0 {
		c.RegisteredTS = now
	}
	if c.LastSeenTS == 0 {
		c.LastSeenTS = now
	}
	const q = `
INSERT INTO daemon_clients(session_id, harness, watch_pid, watch_fp, registered_ts, last_seen_ts)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
    harness      = excluded.harness,
    watch_pid    = excluded.watch_pid,
    watch_fp     = excluded.watch_fp,
    last_seen_ts = excluded.last_seen_ts`
	if _, err := d.conn.ExecContext(ctx, q,
		c.SessionID, c.Harness, c.WatchPID, c.WatchFP,
		c.RegisteredTS, c.LastSeenTS,
	); err != nil {
		return fmt.Errorf("state: register client: %w", err)
	}
	return nil
}

// TouchClient bumps last_seen_ts only. Used by `acd touch`/`acd wake` so a
// long-lived session can keep the daemon alive without re-asserting harness +
// watch metadata on every heartbeat.
func TouchClient(ctx context.Context, d *DB, sessionID string, ts float64) (bool, error) {
	if sessionID == "" {
		return false, fmt.Errorf("state: TouchClient: empty session_id")
	}
	const q = `UPDATE daemon_clients SET last_seen_ts = ? WHERE session_id = ?`
	res, err := d.conn.ExecContext(ctx, q, ts, sessionID)
	if err != nil {
		return false, fmt.Errorf("state: touch client: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: touch client rows: %w", err)
	}
	return n > 0, nil
}

// DeregisterClient removes a single client by session_id. Returns whether the
// row existed (so `acd stop` can report "unknown session" on a no-op).
func DeregisterClient(ctx context.Context, d *DB, sessionID string) (bool, error) {
	if sessionID == "" {
		return false, fmt.Errorf("state: DeregisterClient: empty session_id")
	}
	res, err := d.conn.ExecContext(ctx, `DELETE FROM daemon_clients WHERE session_id = ?`, sessionID)
	if err != nil {
		return false, fmt.Errorf("state: deregister client: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: deregister client rows: %w", err)
	}
	return n > 0, nil
}

// ListClients returns every active client row, ordered by last_seen_ts ASC so
// the oldest entries (most likely to be expired) come first. The daemon's
// refcount-GC pass (§8.4) iterates in that order.
func ListClients(ctx context.Context, d *DB) ([]Client, error) {
	const q = `
SELECT session_id, harness, watch_pid, watch_fp, registered_ts, last_seen_ts
FROM daemon_clients ORDER BY last_seen_ts ASC`
	rows, err := d.readSQL().QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("state: list clients: %w", err)
	}
	defer rows.Close()
	var out []Client
	for rows.Next() {
		var c Client
		if err := rows.Scan(&c.SessionID, &c.Harness, &c.WatchPID, &c.WatchFP,
			&c.RegisteredTS, &c.LastSeenTS); err != nil {
			return nil, fmt.Errorf("state: scan client: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter clients: %w", err)
	}
	return out, nil
}

// CountClients returns the number of registered clients. Used by the run-loop
// shutdown gate ("exit when refcount hits zero").
func CountClients(ctx context.Context, d *DB) (int, error) {
	var n int
	err := d.readSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM daemon_clients`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("state: count clients: %w", err)
	}
	return n, nil
}

// ExpireClientsBefore removes every client whose last_seen_ts is strictly
// older than cutoff. Returns the number of expired rows. This is the universal
// liveness fallback when the fast-path PID probe is unavailable (D20).
func ExpireClientsBefore(ctx context.Context, d *DB, cutoff float64) (int, error) {
	res, err := d.conn.ExecContext(ctx,
		`DELETE FROM daemon_clients WHERE last_seen_ts < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("state: expire clients: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: expire clients rows: %w", err)
	}
	return int(n), nil
}

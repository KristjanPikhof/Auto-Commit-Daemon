package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// DaemonState is the singleton row in the daemon_state table (§6.1). There is
// always exactly one row, identified by id=1. Mode values are one of:
// "stopped", "starting", "running", "draining". The exact set is enforced by
// the caller (run loop in internal/daemon), not at the SQL level.
type DaemonState struct {
	PID               int
	Mode              string
	HeartbeatTS       float64
	BranchRef         sql.NullString
	BranchGeneration  sql.NullInt64
	Note              sql.NullString
	DaemonToken       sql.NullString
	DaemonFingerprint sql.NullString
	UpdatedTS         float64
}

// LoadDaemonState reads the singleton daemon_state row. If the row does not
// exist yet it returns a zero-value DaemonState with mode="stopped" — the same
// shape an UPSERT would produce — and ok=false so callers can distinguish
// "never written" from "explicitly stopped".
func LoadDaemonState(ctx context.Context, d *DB) (DaemonState, bool, error) {
	const q = `
SELECT pid, mode, heartbeat_ts, branch_ref, branch_generation,
       note, daemon_token, daemon_fingerprint, updated_ts
FROM daemon_state WHERE id = 1`

	var s DaemonState
	row := d.readSQL().QueryRowContext(ctx, q)
	err := row.Scan(&s.PID, &s.Mode, &s.HeartbeatTS, &s.BranchRef, &s.BranchGeneration,
		&s.Note, &s.DaemonToken, &s.DaemonFingerprint, &s.UpdatedTS)
	if errors.Is(err, sql.ErrNoRows) {
		return DaemonState{Mode: "stopped"}, false, nil
	}
	if err != nil {
		return DaemonState{}, false, fmt.Errorf("state: load daemon_state: %w", err)
	}
	return s, true, nil
}

// SaveDaemonState upserts the singleton row. updated_ts is set to time.Now()
// in unix-seconds-as-float (REAL) form to match the schema.
func SaveDaemonState(ctx context.Context, d *DB, s DaemonState) error {
	const q = `
INSERT INTO daemon_state(
    id, pid, mode, heartbeat_ts, branch_ref, branch_generation,
    note, daemon_token, daemon_fingerprint, updated_ts
) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    pid                = excluded.pid,
    mode               = excluded.mode,
    heartbeat_ts       = excluded.heartbeat_ts,
    branch_ref         = excluded.branch_ref,
    branch_generation  = excluded.branch_generation,
    note               = excluded.note,
    daemon_token       = excluded.daemon_token,
    daemon_fingerprint = excluded.daemon_fingerprint,
    updated_ts         = excluded.updated_ts`

	now := nowSeconds()
	if s.UpdatedTS == 0 {
		s.UpdatedTS = now
	}
	_, err := d.conn.ExecContext(ctx, q,
		s.PID, s.Mode, s.HeartbeatTS, s.BranchRef, s.BranchGeneration,
		s.Note, s.DaemonToken, s.DaemonFingerprint, s.UpdatedTS,
	)
	if err != nil {
		return fmt.Errorf("state: save daemon_state: %w", err)
	}
	return nil
}

// TouchHeartbeat updates only heartbeat_ts (and updated_ts) on the singleton
// row, creating the row first if needed. The daemon's run loop calls this on
// every tick to refresh the liveness signal (§3.4). It is the hot path.
func TouchHeartbeat(ctx context.Context, d *DB, ts float64) error {
	const q = `
INSERT INTO daemon_state(id, heartbeat_ts, updated_ts) VALUES (1, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    heartbeat_ts = excluded.heartbeat_ts,
    updated_ts   = excluded.updated_ts`
	if _, err := d.conn.ExecContext(ctx, q, ts, ts); err != nil {
		return fmt.Errorf("state: touch heartbeat: %w", err)
	}
	return nil
}

// nowSeconds returns the current time as unix seconds with sub-second
// precision, matching the REAL columns the schema uses for timestamps.
func nowSeconds() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

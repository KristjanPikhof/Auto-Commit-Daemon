package state

import (
	"context"
	"strconv"
	"time"
)

const (
	ActivityMetaKey = "activity.last_event_ts"
	RewritePIDMetaKey = "activity.rewrite_pid"
)

// RecordActivity preserves user/hook activity after client rows expire. This
// optional projection must not turn a successful hook into a failed one.
// Repeated events in the same second do not cause another database write.
func RecordActivity(ctx context.Context, db *DB, at time.Time) {
	if db == nil { return }
	ts := at.Unix()
	_, _ = db.conn.ExecContext(ctx, `INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts
WHERE CAST(daemon_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)`,
		ActivityMetaKey, strconv.FormatInt(ts, 10), ts)
}

// RewriteActivity identifies the live process applying a rewrite, not a saved proposal.
type RewriteActivity struct {
 PID int `json:"pid"`
 Fingerprint string `json:"fingerprint"`
}

package state

import (
	"context"
	"database/sql"

	"fmt"

	"net/url"
)

// Capture event lifecycle state values stored in capture_events.state.
//
// The replay queue is a strict FIFO. A non-pending row is terminal — replay
// must NOT re-queue it. The set is intentionally small:
//
//   - EventStatePending        : awaiting replay (the only state PendingEvents returns).
//   - EventStatePublished      : commit-tree succeeded and the branch ref was advanced.
//   - EventStateFailed         : malformed event (validation, missing ops, commit-build
//     error). Operator inspection only — never retried automatically.
//   - EventStateBlockedConflict: replay refused to commit because the scratch index
//     disagreed with the event's before-state (e.g. live worktree raced ahead of
//     the queue, an `update-ref` CAS lost). Distinct from "failed" so operators
//     can spot index/branch divergence vs malformed input. Like "failed" it is
//     terminal — a stuck event would otherwise re-run on every poll tick and
//     prevent later events from making progress (they would replay on top of a
//     broken predecessor).
//   - EventStateRecovered: an unpublished chain was preserved by a durable
//     recovery snapshot/ref. It is terminal, non-pending, and never a replay
//     barrier; recovery_snapshot_events retains exact chain membership.
const (
	EventStatePending         = "pending"
	EventStatePublished       = "published"
	EventStateFailed          = "failed"
	EventStateBlockedConflict = "blocked_conflict"
	EventStateRecovered       = "recovered"
)

// CaptureEvent is one row of capture_events (§6.1). seq is autoincrement and
// monotonic per repo — readers can rely on seq ordering as the canonical
// "happened before" relation for replay.
type CaptureEvent struct {
	Seq              int64
	BranchRef        string
	BranchGeneration int64
	BaseHead         string
	Operation        string
	Path             string
	OldPath          sql.NullString
	Fidelity         string
	CapturedTS       float64
	PublishedTS      sql.NullFloat64
	State            string // EventState* lifecycle constant.
	CommitOID        sql.NullString
	Error            sql.NullString
	Message          sql.NullString
}

// CaptureOp is one row of capture_ops, the per-event detail records that the
// replay step consumes to construct the actual git tree mutation.
type CaptureOp struct {
	EventSeq   int64
	Ord        int
	Op         string
	Path       string
	OldPath    sql.NullString
	BeforeOID  sql.NullString
	BeforeMode sql.NullString
	AfterOID   sql.NullString
	AfterMode  sql.NullString
	Fidelity   string
}

// PendingCaptureQueueSummary reports every pending capture_event row,
// independent of replay barrier visibility.
type PendingCaptureQueueSummary struct {
	Count     int
	OldestSeq int64
}

type UnresolvedCapturePair struct {
	BranchRef  string
	Generation int64
	FirstSeq   int64
	LastSeq    int64
}

// ReadUnresolvedCapturePairs inspects an existing repository database without
// opening a writer or applying migrations.
func ReadUnresolvedCapturePairs(ctx context.Context, dbPath string) ([]UnresolvedCapturePair, error) {
	q := url.Values{}
	q.Set("mode", "ro")
	q.Add("_pragma", "query_only(ON)")
	conn, err := sql.Open(driverName, "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	exists, err := legacyTableExists(ctx, conn, "capture_events")
	if err != nil || !exists {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, `
SELECT branch_ref, branch_generation, MIN(seq), MAX(seq)
FROM capture_events
WHERE state IN ('pending','blocked_conflict','failed')
GROUP BY branch_ref, branch_generation
ORDER BY branch_ref, branch_generation`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pairs []UnresolvedCapturePair
	for rows.Next() {
		var pair UnresolvedCapturePair
		if err := rows.Scan(&pair.BranchRef, &pair.Generation, &pair.FirstSeq, &pair.LastSeq); err != nil {
			return nil, err
		}
		pairs = append(pairs, pair)
	}
	return pairs, rows.Err()
}

func PendingEvents(ctx context.Context, d *DB, limit int) ([]CaptureEvent, error) {
	return pendingEvents(ctx, d, limit, false, 0)
}

func pendingEvents(ctx context.Context, d *DB, limit int, checkpointOnly bool, afterSeq int64) ([]CaptureEvent, error) {
	checkpointJoin := ""
	checkpointWhere := ""
	if checkpointOnly {
		checkpointJoin = `
JOIN checkpoint_events ce ON ce.event_seq = e.seq
JOIN checkpoints cp ON cp.id = ce.checkpoint_id`
		checkpointWhere = "\n  AND cp.phase = 'completed'"
	}
	q := `
WITH barriers AS (
    SELECT branch_ref, branch_generation, MIN(seq) AS first_seq
    FROM capture_events
    WHERE state IN ('blocked_conflict', 'failed')
    GROUP BY branch_ref, branch_generation
)
SELECT e.seq, e.branch_ref, e.branch_generation, e.base_head, e.operation, e.path, e.old_path,
       e.fidelity, e.captured_ts, e.published_ts, e.state, e.commit_oid, e.error, e.message
FROM capture_events e
` + checkpointJoin + `
LEFT JOIN barriers b
       ON b.branch_ref = e.branch_ref
      AND b.branch_generation = e.branch_generation
WHERE e.state = 'pending'` + checkpointWhere + `
  AND e.seq > ?
  AND (b.first_seq IS NULL OR e.seq < b.first_seq)
ORDER BY e.seq ASC`
	args := []any{afterSeq}
	exists, err := legacyTableExists(ctx, d.ReadSQL(), "capture_events")
	if err != nil || !exists {
		return nil, err
	}

	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := d.readSQL().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("state: query pending events: %w", err)
	}
	defer rows.Close()
	var out []CaptureEvent
	for rows.Next() {
		var ev CaptureEvent
		if err := rows.Scan(&ev.Seq, &ev.BranchRef, &ev.BranchGeneration, &ev.BaseHead,
			&ev.Operation, &ev.Path, &ev.OldPath, &ev.Fidelity,
			&ev.CapturedTS, &ev.PublishedTS, &ev.State, &ev.CommitOID, &ev.Error, &ev.Message); err != nil {
			return nil, fmt.Errorf("state: scan event: %w", err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter events: %w", err)
	}
	return out, nil
}

// LoadCaptureOpsBounded returns ordered operations without reading beyond one
// shared proof budget. The limit sentinel is classified as invalid completed
// transition evidence so recovery callers fail closed.
func LoadCaptureOpsBounded(
	ctx context.Context,
	d *DB,
	seq int64,
	limit int,
) ([]CaptureOp, error) {
	if d == nil || seq < 1 || limit < 0 ||
		limit > CompletedBranchTransitionProofLimit {
		return nil, fmt.Errorf("state: LoadCaptureOpsBounded: invalid input")
	}
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT event_seq, ord, op, path, old_path,
       before_oid, before_mode, after_oid, after_mode, fidelity
FROM capture_ops WHERE event_seq = ? ORDER BY ord ASC
LIMIT ?`, seq, limit+1)
	if err != nil {
		return nil, fmt.Errorf("state: query bounded capture ops: %w", err)
	}
	defer rows.Close()
	var out []CaptureOp
	for rows.Next() {
		var op CaptureOp
		if err := rows.Scan(
			&op.EventSeq, &op.Ord, &op.Op, &op.Path, &op.OldPath,
			&op.BeforeOID, &op.BeforeMode, &op.AfterOID, &op.AfterMode,
			&op.Fidelity); err != nil {
			return nil, fmt.Errorf("state: scan bounded capture op: %w", err)
		}
		out = append(out, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate bounded capture ops: %w", err)
	}
	if len(out) > limit {
		return nil, completedBranchTransitionProofError(
			"capture %d exceeds remaining operation evidence budget %d",
			seq, limit)
	}
	return out, nil
}

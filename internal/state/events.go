package state

import (
	"context"
	"database/sql"
	"fmt"
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
	State            string // "pending" | "published" | "failed"
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

// AppendCaptureEvent inserts a capture event plus its ordered ops in a single
// transaction. The returned seq is the autoincrement primary key, which the
// caller can use to correlate downstream commit_oid back to the event.
//
// Caller invariants:
//   - sensitive paths must be filtered upstream (sensitive.go).
//   - ops must be ordered; ord is reassigned monotonically starting at 0.
func AppendCaptureEvent(ctx context.Context, d *DB, ev CaptureEvent, ops []CaptureOp) (int64, error) {
	if ev.BranchRef == "" || ev.BaseHead == "" || ev.Operation == "" || ev.Path == "" || ev.Fidelity == "" {
		return 0, fmt.Errorf("state: AppendCaptureEvent: required field missing")
	}
	if ev.CapturedTS == 0 {
		ev.CapturedTS = nowSeconds()
	}
	if ev.State == "" {
		ev.State = "pending"
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("state: begin capture tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insEvent = `
INSERT INTO capture_events(
    branch_ref, branch_generation, base_head, operation, path, old_path,
    fidelity, captured_ts, published_ts, state, commit_oid, error, message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	res, err := tx.ExecContext(ctx, insEvent,
		ev.BranchRef, ev.BranchGeneration, ev.BaseHead, ev.Operation, ev.Path, ev.OldPath,
		ev.Fidelity, ev.CapturedTS, ev.PublishedTS, ev.State, ev.CommitOID, ev.Error, ev.Message,
	)
	if err != nil {
		return 0, fmt.Errorf("state: insert capture event: %w", err)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("state: capture event seq: %w", err)
	}

	if len(ops) > 0 {
		const insOp = `
INSERT INTO capture_ops(
    event_seq, ord, op, path, old_path,
    before_oid, before_mode, after_oid, after_mode, fidelity
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		stmt, err := tx.PrepareContext(ctx, insOp)
		if err != nil {
			return 0, fmt.Errorf("state: prepare capture_ops insert: %w", err)
		}
		defer stmt.Close()
		for i, op := range ops {
			if op.Op == "" || op.Path == "" || op.Fidelity == "" {
				return 0, fmt.Errorf("state: capture op %d: required field missing", i)
			}
			if _, err := stmt.ExecContext(ctx,
				seq, i, op.Op, op.Path, op.OldPath,
				op.BeforeOID, op.BeforeMode, op.AfterOID, op.AfterMode, op.Fidelity,
			); err != nil {
				return 0, fmt.Errorf("state: insert capture op %d: %w", i, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("state: commit capture tx: %w", err)
	}
	return seq, nil
}

// MarkEventPublished updates an event row when the replay step has produced
// (or failed to produce) a commit. State is one of "published" or "failed".
func MarkEventPublished(ctx context.Context, d *DB, seq int64, state string, commitOID sql.NullString, errMsg sql.NullString, message sql.NullString, publishedTS float64) error {
	const q = `
UPDATE capture_events SET
    state = ?,
    commit_oid = ?,
    error = ?,
    message = ?,
    published_ts = ?
WHERE seq = ?`
	if _, err := d.conn.ExecContext(ctx, q, state, commitOID, errMsg, message, publishedTS, seq); err != nil {
		return fmt.Errorf("state: mark event published: %w", err)
	}
	return nil
}

// PendingEvents returns up to limit pending events ordered by seq ascending
// (FIFO replay). limit <= 0 means "no limit".
func PendingEvents(ctx context.Context, d *DB, limit int) ([]CaptureEvent, error) {
	q := `
SELECT seq, branch_ref, branch_generation, base_head, operation, path, old_path,
       fidelity, captured_ts, published_ts, state, commit_oid, error, message
FROM capture_events
WHERE state = 'pending'
ORDER BY seq ASC`
	args := []any{}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := d.conn.QueryContext(ctx, q, args...)
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

// LoadCaptureOps returns ordered ops for an event seq.
func LoadCaptureOps(ctx context.Context, d *DB, seq int64) ([]CaptureOp, error) {
	const q = `
SELECT event_seq, ord, op, path, old_path,
       before_oid, before_mode, after_oid, after_mode, fidelity
FROM capture_ops WHERE event_seq = ? ORDER BY ord ASC`
	rows, err := d.conn.QueryContext(ctx, q, seq)
	if err != nil {
		return nil, fmt.Errorf("state: query capture ops: %w", err)
	}
	defer rows.Close()
	var out []CaptureOp
	for rows.Next() {
		var op CaptureOp
		if err := rows.Scan(&op.EventSeq, &op.Ord, &op.Op, &op.Path, &op.OldPath,
			&op.BeforeOID, &op.BeforeMode, &op.AfterOID, &op.AfterMode, &op.Fidelity); err != nil {
			return nil, fmt.Errorf("state: scan capture op: %w", err)
		}
		out = append(out, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter capture ops: %w", err)
	}
	return out, nil
}

// PrunePublishedEventsBefore deletes capture_events rows whose state is
// 'published' (terminal success) AND whose captured_ts is strictly older
// than cutoff. Returns the number of rows removed.
//
// 'failed' rows are intentionally retained so operators can inspect why a
// replay failed. 'pending' rows are retained so an unrecoverable backlog
// is not silently swept under the rug.
//
// CASCADE on the capture_ops foreign key drops the matching ops rows in
// the same transaction.
func PrunePublishedEventsBefore(ctx context.Context, d *DB, cutoff float64) (int, error) {
	res, err := d.conn.ExecContext(ctx,
		`DELETE FROM capture_events WHERE state = 'published' AND captured_ts < ?`,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("state: prune published events: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: prune events rows: %w", err)
	}
	return int(n), nil
}

// LatestEventSeq returns the highest seq value present, or 0 if the table is
// empty. Useful as a smoke-test for monotonic ordering and for the daily
// rollup window query.
func LatestEventSeq(ctx context.Context, d *DB) (int64, error) {
	var seq sql.NullInt64
	err := d.conn.QueryRowContext(ctx, `SELECT MAX(seq) FROM capture_events`).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("state: latest event seq: %w", err)
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
}

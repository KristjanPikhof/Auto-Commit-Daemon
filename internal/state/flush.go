package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// FlushRequest is one row of flush_requests (§6.1). The CLI's `acd wake`
// (and similar) inserts a row; the daemon's run loop polls, acknowledges, and
// completes it. Status transitions: pending -> acknowledged -> completed (or
// failed). The schema does not enforce the transitions — the helpers do.
type FlushRequest struct {
	ID             int64
	Command        string
	NonBlocking    bool
	RequestedTS    float64
	AcknowledgedTS sql.NullFloat64
	CompletedTS    sql.NullFloat64
	Status         string
	Note           sql.NullString
}

// EnqueueFlushRequest inserts a new pending flush request and returns its ID.
func EnqueueFlushRequest(ctx context.Context, d *DB, command string, nonBlocking bool, note sql.NullString) (int64, error) {
	if command == "" {
		return 0, fmt.Errorf("state: EnqueueFlushRequest: empty command")
	}
	const q = `
INSERT INTO flush_requests(command, non_blocking, requested_ts, status, note)
VALUES (?, ?, ?, 'pending', ?)`
	nb := 0
	if nonBlocking {
		nb = 1
	}
	res, err := d.conn.ExecContext(ctx, q, command, nb, nowSeconds(), note)
	if err != nil {
		return 0, fmt.Errorf("state: enqueue flush request: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("state: flush request id: %w", err)
	}
	return id, nil
}

// ClaimNextFlushRequest atomically picks the oldest pending request and marks
// it acknowledged. Returns (zero, false, nil) when the queue is empty.
//
// SQLite serialises writers, so the SELECT/UPDATE pair is safe inside a
// transaction even with multiple goroutines competing.
func ClaimNextFlushRequest(ctx context.Context, d *DB) (FlushRequest, bool, error) {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return FlushRequest{}, false, fmt.Errorf("state: begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const sel = `
SELECT id, command, non_blocking, requested_ts, acknowledged_ts, completed_ts, status, note
FROM flush_requests WHERE status = 'pending' ORDER BY id ASC LIMIT 1`
	var fr FlushRequest
	var nb int
	err = tx.QueryRowContext(ctx, sel).Scan(
		&fr.ID, &fr.Command, &nb, &fr.RequestedTS, &fr.AcknowledgedTS, &fr.CompletedTS, &fr.Status, &fr.Note,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return FlushRequest{}, false, nil
	}
	if err != nil {
		return FlushRequest{}, false, fmt.Errorf("state: select pending flush: %w", err)
	}
	fr.NonBlocking = nb != 0

	now := nowSeconds()
	if _, err := tx.ExecContext(ctx,
		`UPDATE flush_requests SET status = 'acknowledged', acknowledged_ts = ? WHERE id = ?`,
		now, fr.ID,
	); err != nil {
		return FlushRequest{}, false, fmt.Errorf("state: ack flush: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return FlushRequest{}, false, fmt.Errorf("state: commit claim tx: %w", err)
	}
	fr.Status = "acknowledged"
	fr.AcknowledgedTS = sql.NullFloat64{Float64: now, Valid: true}
	return fr, true, nil
}

// CompleteFlushRequest moves an acknowledged request to "completed" or
// "failed" depending on success.
func CompleteFlushRequest(ctx context.Context, d *DB, id int64, success bool, note sql.NullString) error {
	status := "completed"
	if !success {
		status = "failed"
	}
	const q = `
UPDATE flush_requests SET status = ?, completed_ts = ?, note = COALESCE(?, note)
WHERE id = ?`
	if _, err := d.conn.ExecContext(ctx, q, status, nowSeconds(), note, id); err != nil {
		return fmt.Errorf("state: complete flush request: %w", err)
	}
	return nil
}

// DrainFlushRequests atomically claims and completes up to limit oldest
// pending requests. The single UPDATE owns each selected row by requiring its
// status to still be pending, so concurrent callers cannot return the same
// request. A request transitions through acknowledged to completed as one
// atomic observation: acknowledged_ts and completed_ts are persisted together
// and no crash window can leave an orphaned acknowledged row.
//
// Returned requests are sorted by ascending ID so callers preserve FIFO
// command semantics even though SQLite does not guarantee RETURNING order.
func DrainFlushRequests(
	ctx context.Context,
	d *DB,
	limit int,
	note sql.NullString,
) ([]FlushRequest, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("state: DrainFlushRequests: limit must be positive")
	}
	now := nowSeconds()
	rows, err := d.conn.QueryContext(ctx, `
UPDATE flush_requests
SET status = 'completed',
    acknowledged_ts = ?,
    completed_ts = ?,
    note = COALESCE(?, note)
WHERE id IN (
    SELECT id
    FROM flush_requests
    WHERE status = 'pending'
    ORDER BY id ASC
    LIMIT ?
)
RETURNING id, command, non_blocking, requested_ts,
          acknowledged_ts, completed_ts, status, note`,
		now, now, note, limit)
	if err != nil {
		return nil, fmt.Errorf("state: drain flush requests: %w", err)
	}
	defer rows.Close()

	requests := make([]FlushRequest, 0, limit)
	for rows.Next() {
		var (
			request FlushRequest
			nb      int
		)
		if err := rows.Scan(
			&request.ID,
			&request.Command,
			&nb,
			&request.RequestedTS,
			&request.AcknowledgedTS,
			&request.CompletedTS,
			&request.Status,
			&request.Note,
		); err != nil {
			return nil, fmt.Errorf("state: scan drained flush request: %w", err)
		}
		request.NonBlocking = nb != 0
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate drained flush requests: %w", err)
	}
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].ID < requests[j].ID
	})
	return requests, nil
}

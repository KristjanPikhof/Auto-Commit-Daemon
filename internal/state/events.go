package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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

// PlannerState is durable, per-event bookkeeping for intent planning. It is
// keyed by capture_events.seq so planner retries and deferrals survive daemon
// restarts without changing capture_events lifecycle state.
type PlannerState struct {
	EventSeq        int64
	DeferCount      int
	LastPlannedTS   float64
	LastDeferReason sql.NullString
	LastPlanError   sql.NullString
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
// (or failed to produce) a commit. State is one of EventStatePublished,
// EventStateFailed, or EventStateBlockedConflict — all three are terminal
// and remove the row from PendingEvents output.
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
//
// Only rows with state = EventStatePending are returned. A terminal failed or
// blocked_conflict predecessor for the same branch generation forms a replay
// barrier: later pending rows stay out of the queue until the operator removes
// the terminal predecessor. Published predecessors do not block because they
// already advanced the branch history.
//
// Implementation: a CTE collapses every (branch_ref, branch_generation) into
// its lowest barrier seq, then a left-join filters pending rows whose seq is
// at or beyond that barrier. This is an order-of-magnitude faster than the
// previous correlated NOT EXISTS subquery once the queue grows past a few
// thousand pending rows (the case during a long pause). The leading-state
// covering index idx_capture_events_barrier (schema v3) keeps both the CTE
// aggregation and the outer pending-row scan off the unindexed full-table
// path.
// PendingCaptureQueueSummary reports every pending capture_event row,
// independent of replay barrier visibility.
type PendingCaptureQueueSummary struct {
	Count     int
	OldestSeq int64
}

// CountAllPendingCaptureEvents counts all pending capture_events rows without
// applying PendingEvents' failed/blocked replay barrier filter. Safety gates
// that must refuse any queued work before history rewrites should use this
// helper rather than PendingEvents.
func CountAllPendingCaptureEvents(ctx context.Context, d *DB) (PendingCaptureQueueSummary, error) {
	var summary PendingCaptureQueueSummary
	if err := d.readSQL().QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(MIN(seq), 0)
FROM capture_events
WHERE state = ?`, EventStatePending).Scan(&summary.Count, &summary.OldestSeq); err != nil {
		return PendingCaptureQueueSummary{}, fmt.Errorf("state: count all pending capture events: %w", err)
	}
	return summary, nil
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

// ListUnresolvedCapturePairs returns every immutable pair that setup must
// prove or preserve before the one-shot v20 cutover can commit.
func ListUnresolvedCapturePairs(ctx context.Context, d *DB) ([]UnresolvedCapturePair, error) {
	rows, err := d.readSQL().QueryContext(ctx, `
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
	return pendingEvents(ctx, d, limit, false)
}

// PublishableEvents returns only pending capture rows owned by a completed
// checkpoint. Protection may append rows before private-ref completion, but
// publication must never observe that cross-store prepared window.
func PublishableEvents(ctx context.Context, d *DB, limit int) ([]CaptureEvent, error) {
	return pendingEvents(ctx, d, limit, true)
}

func pendingEvents(ctx context.Context, d *DB, limit int, checkpointOnly bool) ([]CaptureEvent, error) {
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
  AND (b.first_seq IS NULL OR e.seq < b.first_seq)
ORDER BY e.seq ASC`
	args := []any{}
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

// PlannerStateForEvent returns persisted planner bookkeeping for one event.
func PlannerStateForEvent(ctx context.Context, d *DB, eventSeq int64) (PlannerState, bool, error) {
	if eventSeq <= 0 {
		return PlannerState{}, false, fmt.Errorf("state: PlannerStateForEvent: event_seq must be positive")
	}
	var ps PlannerState
	err := d.readSQL().QueryRowContext(ctx, `
SELECT event_seq, defer_count, last_planned_ts, last_defer_reason, last_plan_error
FROM planner_state WHERE event_seq = ?`, eventSeq).Scan(
		&ps.EventSeq, &ps.DeferCount, &ps.LastPlannedTS, &ps.LastDeferReason, &ps.LastPlanError)
	if err == sql.ErrNoRows {
		return PlannerState{}, false, nil
	}
	if err != nil {
		return PlannerState{}, false, fmt.Errorf("state: load planner state: %w", err)
	}
	return ps, true, nil
}

// RecordPlannerOffer marks an event as considered by the intent planner. It
// does not mutate capture_events.state, so replay barriers and pending FIFO
// semantics remain owned by the replay layer.
func RecordPlannerOffer(ctx context.Context, d *DB, eventSeq int64, plannedTS float64) error {
	if eventSeq <= 0 {
		return fmt.Errorf("state: RecordPlannerOffer: event_seq must be positive")
	}
	if plannedTS <= 0 {
		plannedTS = nowSeconds()
	}
	const q = `
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts, last_defer_reason, last_plan_error)
VALUES (?, 0, ?, NULL, NULL)
ON CONFLICT(event_seq) DO UPDATE SET
    last_planned_ts = excluded.last_planned_ts,
    last_plan_error = NULL`
	if _, err := d.conn.ExecContext(ctx, q, eventSeq, plannedTS); err != nil {
		return fmt.Errorf("state: record planner offer: %w", err)
	}
	return nil
}

// RecordPlannerDefer increments an event's deferral count and stores the most
// recent reason. The increment happens inside SQLite, so concurrent callers do
// not lose updates.
func RecordPlannerDefer(ctx context.Context, d *DB, eventSeq int64, plannedTS float64, reason string) error {
	if eventSeq <= 0 {
		return fmt.Errorf("state: RecordPlannerDefer: event_seq must be positive")
	}
	if plannedTS <= 0 {
		plannedTS = nowSeconds()
	}
	reasonValue := sql.NullString{String: reason, Valid: reason != ""}
	const q = `
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts, last_defer_reason, last_plan_error)
VALUES (?, 1, ?, ?, NULL)
ON CONFLICT(event_seq) DO UPDATE SET
    defer_count = planner_state.defer_count + 1,
    last_planned_ts = excluded.last_planned_ts,
    last_defer_reason = excluded.last_defer_reason,
    last_plan_error = NULL`
	if _, err := d.conn.ExecContext(ctx, q, eventSeq, plannedTS, reasonValue); err != nil {
		return fmt.Errorf("state: record planner defer: %w", err)
	}
	return nil
}

// RecordPlannerError stores the last planner failure for an event without
// moving the capture event out of the pending queue.
func RecordPlannerError(ctx context.Context, d *DB, eventSeq int64, plannedTS float64, errMsg string) error {
	if eventSeq <= 0 {
		return fmt.Errorf("state: RecordPlannerError: event_seq must be positive")
	}
	if plannedTS <= 0 {
		plannedTS = nowSeconds()
	}
	errValue := sql.NullString{String: errMsg, Valid: errMsg != ""}
	const q = `
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts, last_defer_reason, last_plan_error)
VALUES (?, 0, ?, NULL, ?)
ON CONFLICT(event_seq) DO UPDATE SET
    last_planned_ts = excluded.last_planned_ts,
    last_plan_error = excluded.last_plan_error`
	if _, err := d.conn.ExecContext(ctx, q, eventSeq, plannedTS, errValue); err != nil {
		return fmt.Errorf("state: record planner error: %w", err)
	}
	return nil
}

// ClearPlannerState removes durable planning bookkeeping for an event once the
// event leaves the pending queue. Missing rows are harmless.
func ClearPlannerState(ctx context.Context, d *DB, eventSeq int64) error {
	if eventSeq <= 0 {
		return fmt.Errorf("state: ClearPlannerState: event_seq must be positive")
	}
	if _, err := d.conn.ExecContext(ctx,
		`DELETE FROM planner_state WHERE event_seq = ?`, eventSeq); err != nil {
		return fmt.Errorf("state: clear planner state: %w", err)
	}
	return nil
}

// OldestOverduePlannerEvent returns the oldest pending event whose defer_count
// has reached deferLimit. Ties are resolved by event_seq for deterministic
// planner behavior.
func OldestOverduePlannerEvent(ctx context.Context, d *DB, branchRef string, branchGeneration int64, deferLimit int) (CaptureEvent, PlannerState, bool, error) {
	if branchRef == "" {
		return CaptureEvent{}, PlannerState{}, false, fmt.Errorf("state: OldestOverduePlannerEvent: empty branch_ref")
	}
	if deferLimit < 0 {
		deferLimit = 0
	}
	const q = `
WITH barrier AS (
    SELECT MIN(seq) AS first_seq
    FROM capture_events
    WHERE branch_ref = ?
      AND branch_generation = ?
      AND state IN (?, ?)
)
SELECT e.seq, e.branch_ref, e.branch_generation, e.base_head, e.operation, e.path, e.old_path,
       e.fidelity, e.captured_ts, e.published_ts, e.state, e.commit_oid, e.error, e.message,
       ps.event_seq, ps.defer_count, ps.last_planned_ts, ps.last_defer_reason, ps.last_plan_error
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
CROSS JOIN barrier b
WHERE e.branch_ref = ?
  AND e.branch_generation = ?
  AND e.state = ?
  AND ps.defer_count >= ?
  AND (b.first_seq IS NULL OR e.seq < b.first_seq)
ORDER BY ps.last_planned_ts ASC, ps.event_seq ASC
LIMIT 1`
	var ev CaptureEvent
	var ps PlannerState
	err := d.readSQL().QueryRowContext(ctx, q,
		branchRef, branchGeneration, EventStateBlockedConflict, EventStateFailed,
		branchRef, branchGeneration, EventStatePending, deferLimit,
	).Scan(
		&ev.Seq, &ev.BranchRef, &ev.BranchGeneration, &ev.BaseHead, &ev.Operation, &ev.Path, &ev.OldPath,
		&ev.Fidelity, &ev.CapturedTS, &ev.PublishedTS, &ev.State, &ev.CommitOID, &ev.Error, &ev.Message,
		&ps.EventSeq, &ps.DeferCount, &ps.LastPlannedTS, &ps.LastDeferReason, &ps.LastPlanError,
	)
	if err == sql.ErrNoRows {
		return CaptureEvent{}, PlannerState{}, false, nil
	}
	if err != nil {
		return CaptureEvent{}, PlannerState{}, false, fmt.Errorf("state: oldest overdue planner event: %w", err)
	}
	return ev, ps, true, nil
}

// CountEventsByState returns the number of capture_events rows matching the
// given state (e.g. EventStateBlockedConflict, EventStateFailed). Useful for
// `acd status` to surface terminal-failure counts distinct from the FIFO
// pending depth.
func CountEventsByState(ctx context.Context, d *DB, state string) (int, error) {
	var n int
	if err := d.readSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`, state).Scan(&n); err != nil {
		return 0, fmt.Errorf("state: count events by state: %w", err)
	}
	return n, nil
}

// CountPendingEventsForGeneration returns how many capture_events rows are
// currently in EventStatePending for the (branch_ref, branch_generation)
// pair. This is the daemon's depth gauge for the per-generation FIFO and
// drives the soft-cap eviction decision in capture.AppendCaptureEvent
// callers. The query is index-backed by idx_capture_events_barrier
// (state-leading covering index from schema v3).
func CountPendingEventsForGeneration(ctx context.Context, d *DB, branchRef string, branchGeneration int64) (int, error) {
	if branchRef == "" {
		return 0, fmt.Errorf("state: CountPendingEventsForGeneration: empty branch_ref")
	}
	var n int
	if err := d.readSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events
		  WHERE state = ? AND branch_ref = ? AND branch_generation = ?`,
		EventStatePending, branchRef, branchGeneration).Scan(&n); err != nil {
		return 0, fmt.Errorf("state: count pending for generation: %w", err)
	}
	return n, nil
}

// CountPendingEventsAll returns the total number of capture_events rows in
// EventStatePending across every (branch_ref, branch_generation). Used by
// `acd diagnose --json` to surface the global pending depth.
func CountPendingEventsAll(ctx context.Context, d *DB) (int, error) {
	var n int
	if err := d.readSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		EventStatePending).Scan(&n); err != nil {
		return 0, fmt.Errorf("state: count pending events: %w", err)
	}
	return n, nil
}

// MarkEventBlocked atomically settles a capture_events row in
// EventStateBlockedConflict and upserts the singleton publish_state row to
// status="blocked_conflict" within a single transaction. This pairs the two
// surfaces so a status reader never sees a "blocked" event with a stale
// publish_state, or vice versa.
//
// errMsg is recorded on both rows. publishedTS is stamped on capture_events
// (terminal timestamp); publish_state.updated_ts is stamped now.
func MarkEventBlocked(ctx context.Context, d *DB, seq int64, errMsg string, publishedTS float64,
	branchRef sql.NullString, branchGeneration sql.NullInt64, sourceHead sql.NullString,
) error {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin block tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const updEvent = `
UPDATE capture_events SET
    state        = ?,
    error        = ?,
    published_ts = ?
WHERE seq = ?`
	if _, err := tx.ExecContext(ctx, updEvent,
		EventStateBlockedConflict,
		sql.NullString{String: errMsg, Valid: true},
		publishedTS, seq); err != nil {
		return fmt.Errorf("state: mark event blocked: %w", err)
	}

	const upsertPub = `
INSERT INTO publish_state(
    id, event_seq, branch_ref, branch_generation, source_head, target_commit_oid,
    status, error, updated_ts
) VALUES (1, ?, ?, ?, ?, NULL, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    event_seq         = excluded.event_seq,
    branch_ref        = excluded.branch_ref,
    branch_generation = excluded.branch_generation,
    source_head       = excluded.source_head,
    target_commit_oid = excluded.target_commit_oid,
    status            = excluded.status,
    error             = excluded.error,
    updated_ts        = excluded.updated_ts`
	if _, err := tx.ExecContext(ctx, upsertPub,
		sql.NullInt64{Int64: seq, Valid: true},
		branchRef, branchGeneration, sourceHead,
		"blocked_conflict",
		sql.NullString{String: errMsg, Valid: true},
		publishedTS); err != nil {
		return fmt.Errorf("state: upsert blocked publish_state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit block tx: %w", err)
	}
	return nil
}

// BlockedEventsForGeneration returns capture_events rows in
// EventStateBlockedConflict for (branch_ref, branch_generation), ordered by
// seq ascending. This is the input for the replay self-heal pass — the
// daemon enumerates blocked rows on the active generation and probes each
// against HEAD to see whether an external committer already landed the
// captured change.
//
// Empty branchRef is rejected to avoid a bag-of-blocked sweep across stale
// generations; callers must pass a live anchor. limit <= 0 means "no limit".
func BlockedEventsForGeneration(ctx context.Context, d *DB, branchRef string, branchGeneration int64, limit int) ([]CaptureEvent, error) {
	if branchRef == "" {
		return nil, fmt.Errorf("state: BlockedEventsForGeneration: empty branch_ref")
	}
	q := `
SELECT seq, branch_ref, branch_generation, base_head, operation, path, old_path,
       fidelity, captured_ts, published_ts, state, commit_oid, error, message
FROM capture_events
WHERE state = ? AND branch_ref = ? AND branch_generation = ?
ORDER BY seq ASC`
	args := []any{EventStateBlockedConflict, branchRef, branchGeneration}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := d.readSQL().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("state: query blocked events: %w", err)
	}
	defer rows.Close()
	var out []CaptureEvent
	for rows.Next() {
		var ev CaptureEvent
		if err := rows.Scan(&ev.Seq, &ev.BranchRef, &ev.BranchGeneration, &ev.BaseHead,
			&ev.Operation, &ev.Path, &ev.OldPath, &ev.Fidelity,
			&ev.CapturedTS, &ev.PublishedTS, &ev.State, &ev.CommitOID, &ev.Error, &ev.Message); err != nil {
			return nil, fmt.Errorf("state: scan blocked event: %w", err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter blocked events: %w", err)
	}
	return out, nil
}

// ErrBlockedRowNotEligible is returned by TransitionBlockedToPublished when
// the target capture_events row is no longer in EventStateBlockedConflict. It
// distinguishes a benign race (a concurrent recovery action or replay pass
// already moved the row) from a real persistence failure so the caller can
// skip self-heal of this row and continue with the next candidate.
var ErrBlockedRowNotEligible = errors.New("state: row not in blocked_conflict state")

// TransitionBlockedToPublished settles a previously-blocked capture_events row
// as published in a single transaction. It is the persistence half of the
// daemon's self-heal probe: when alreadyPublishedAtHEAD confirms an external
// committer already landed the captured change, this helper records the
// promotion atomically (capture_events + publish_state singleton) so a
// status reader never sees a half-update.
//
// The row's current state MUST be EventStateBlockedConflict — that guard is
// race-safe (the UPDATE includes the state predicate so a concurrent
// transition into 'failed' or back into 'pending' cannot be silently
// overwritten). If the predicate fails the helper returns
// ErrBlockedRowNotEligible and writes nothing.
//
// publish_state is upserted in 'published' status carrying commitOID,
// branchRef, branchGeneration, sourceHead, error=NULL, event_seq=seq. This
// mirrors the shape settlePublishedEvent uses for the normal path so
// downstream readers do not need a separate code path for self-healed rows.
func TransitionBlockedToPublished(ctx context.Context, d *DB, seq int64, commitOID string, publishedTS float64, branchRef string, branchGeneration int64, sourceHead string) error {
	if d == nil {
		return fmt.Errorf("state: TransitionBlockedToPublished: nil db")
	}
	if seq <= 0 {
		return fmt.Errorf("state: TransitionBlockedToPublished: invalid seq %d", seq)
	}
	if commitOID == "" {
		return fmt.Errorf("state: TransitionBlockedToPublished: empty commit oid")
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin transition tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const updEvent = `
UPDATE capture_events SET
    state        = ?,
    commit_oid   = ?,
    error        = NULL,
    published_ts = ?
WHERE seq = ? AND state = ?`
	res, err := tx.ExecContext(ctx, updEvent,
		EventStatePublished,
		sql.NullString{String: commitOID, Valid: true},
		publishedTS, seq, EventStateBlockedConflict,
	)
	if err != nil {
		return fmt.Errorf("state: transition blocked->published seq=%d: %w", seq, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("state: transition blocked->published rows seq=%d: %w", seq, err)
	}
	if n == 0 {
		return ErrBlockedRowNotEligible
	}

	const upsertPub = `
INSERT INTO publish_state(
    id, event_seq, branch_ref, branch_generation, source_head, target_commit_oid,
    status, error, updated_ts
) VALUES (1, ?, ?, ?, ?, ?, ?, NULL, ?)
ON CONFLICT(id) DO UPDATE SET
    event_seq         = excluded.event_seq,
    branch_ref        = excluded.branch_ref,
    branch_generation = excluded.branch_generation,
    source_head       = excluded.source_head,
    target_commit_oid = excluded.target_commit_oid,
    status            = excluded.status,
    error             = excluded.error,
    updated_ts        = excluded.updated_ts`
	if _, err := tx.ExecContext(ctx, upsertPub,
		sql.NullInt64{Int64: seq, Valid: true},
		sql.NullString{String: branchRef, Valid: branchRef != ""},
		sql.NullInt64{Int64: branchGeneration, Valid: true},
		sql.NullString{String: sourceHead, Valid: sourceHead != ""},
		sql.NullString{String: commitOID, Valid: true},
		"published",
		publishedTS); err != nil {
		return fmt.Errorf("state: upsert published publish_state seq=%d: %w", seq, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit transition tx seq=%d: %w", seq, err)
	}
	return nil
}

// LoadCaptureOps returns ordered ops for an event seq.
func LoadCaptureOps(ctx context.Context, d *DB, seq int64) ([]CaptureOp, error) {
	const q = `
SELECT event_seq, ord, op, path, old_path,
       before_oid, before_mode, after_oid, after_mode, fidelity
FROM capture_ops WHERE event_seq = ? ORDER BY ord ASC`
	rows, err := d.readSQL().QueryContext(ctx, q, seq)
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

// PrunePublishedEventsBefore deletes old published rows that do not belong to
// a recovery snapshot and are not materialization context for an unresolved
// chain. Context includes same-base prefixes, earlier rows that touch a later
// unresolved path, and published events interleaved between unresolved rows.
// Snapshot members require repo-aware Git-ref locking and are pruned separately
// by the daemon, regardless of whether reconciliation classified them as
// published or recovered.
func PrunePublishedEventsBefore(ctx context.Context, d *DB, cutoff float64) (int, error) {
	if d == nil {
		return 0, fmt.Errorf("state: PrunePublishedEventsBefore: nil db")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("state: begin published prune: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
CREATE TEMP TABLE IF NOT EXISTS acd_prune_recovery_context(
    event_seq INTEGER PRIMARY KEY
) WITHOUT ROWID`); err != nil {
		return 0, fmt.Errorf("state: create published prune context: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acd_prune_recovery_context`); err != nil {
		return 0, fmt.Errorf("state: clear published prune context: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
CREATE TEMP TABLE IF NOT EXISTS acd_prune_intent_events(
    event_seq INTEGER PRIMARY KEY
) WITHOUT ROWID`); err != nil {
		return 0, fmt.Errorf("state: create intent prune set: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acd_prune_intent_events`); err != nil {
		return 0, fmt.Errorf("state: clear intent prune set: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM checkpoint_events
WHERE checkpoint_id IN (
    SELECT id FROM checkpoints
    WHERE retained=0 AND pruned_ts IS NOT NULL
)`); err != nil {
		return 0, fmt.Errorf("state: clear pruned checkpoint event membership: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
SELECT branch_ref, branch_generation, MIN(seq), MAX(seq)
FROM capture_events
WHERE state IN (?, ?, ?)
GROUP BY branch_ref, branch_generation`,
		EventStatePending, EventStateBlockedConflict, EventStateFailed)
	if err != nil {
		return 0, fmt.Errorf("state: list unresolved recovery pairs for prune: %w", err)
	}
	type unresolvedPair struct {
		branchRef  string
		generation int64
		firstSeq   int64
		lastSeq    int64
	}
	var pairs []unresolvedPair
	for rows.Next() {
		var pair unresolvedPair
		if err := rows.Scan(&pair.branchRef, &pair.generation, &pair.firstSeq, &pair.lastSeq); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("state: scan unresolved recovery pair for prune: %w", err)
		}
		pairs = append(pairs, pair)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("state: iterate unresolved recovery pairs for prune: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("state: close unresolved recovery pairs for prune: %w", err)
	}

	insertContext, err := tx.PrepareContext(ctx,
		`INSERT OR IGNORE INTO acd_prune_recovery_context(event_seq) VALUES (?)`)
	if err != nil {
		return 0, fmt.Errorf("state: prepare published prune context insert: %w", err)
	}
	for _, pair := range pairs {
		selected, err := loadPublishedRecoveryContextSeqs(
			ctx, tx, pair.branchRef, pair.generation, pair.firstSeq, pair.lastSeq,
		)
		if errors.Is(err, errPublishedRecoveryContextLimit) {
			if _, retainErr := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO acd_prune_recovery_context(event_seq)
SELECT seq
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND state = ?`,
				pair.branchRef, pair.generation, EventStatePublished); retainErr != nil {
				_ = insertContext.Close()
				return 0, fmt.Errorf("state: retain oversized published prune context for %s generation %d: %w",
					pair.branchRef, pair.generation, retainErr)
			}
			slog.Default().Warn("published recovery context exceeds prune bound; retaining exact pair",
				"branch_ref", pair.branchRef,
				"branch_generation", pair.generation,
				"max_events", maxPublishedRecoveryContextEvents)
			continue
		}
		if err != nil {
			_ = insertContext.Close()
			return 0, fmt.Errorf("state: load published prune context for %s generation %d: %w",
				pair.branchRef, pair.generation, err)
		}
		for seq := range selected {
			if _, err := insertContext.ExecContext(ctx, seq); err != nil {
				_ = insertContext.Close()
				return 0, fmt.Errorf("state: insert published prune context seq=%d: %w", seq, err)
			}
		}
	}
	if err := insertContext.Close(); err != nil {
		return 0, fmt.Errorf("state: close published prune context insert: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO acd_prune_recovery_context(event_seq)
SELECT published.seq
FROM capture_events published
WHERE published.state = 'published'
  AND NOT EXISTS (
      SELECT 1
      FROM acd_prune_recovery_context context
      WHERE context.event_seq = published.seq
  )
  AND EXISTS (
      SELECT 1
      FROM capture_events unpublished
      WHERE unpublished.branch_ref = published.branch_ref
        AND unpublished.branch_generation = published.branch_generation
        AND unpublished.base_head = published.base_head
        AND unpublished.seq > published.seq
        AND unpublished.state IN (?, ?, ?)
  )`,
		EventStatePending, EventStateBlockedConflict, EventStateFailed); err != nil {
		return 0, fmt.Errorf("state: retain same-base published prune context: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO acd_prune_recovery_context(event_seq)
SELECT published.seq
FROM capture_events published
WHERE published.state = 'published'
  AND NOT EXISTS (
      SELECT 1
      FROM acd_prune_recovery_context context
      WHERE context.event_seq = published.seq
  )
  AND EXISTS (
      SELECT 1
      FROM capture_events earlier
      WHERE earlier.branch_ref = published.branch_ref
        AND earlier.branch_generation = published.branch_generation
        AND earlier.seq < published.seq
        AND earlier.state IN (?, ?, ?)
  )
  AND EXISTS (
      SELECT 1
      FROM capture_events later
      WHERE later.branch_ref = published.branch_ref
        AND later.branch_generation = published.branch_generation
        AND later.seq > published.seq
        AND later.state IN (?, ?, ?)
  )`,
		EventStatePending, EventStateBlockedConflict, EventStateFailed,
		EventStatePending, EventStateBlockedConflict, EventStateFailed); err != nil {
		return 0, fmt.Errorf("state: retain interleaved published prune context: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO acd_prune_intent_events(event_seq)
SELECT seq FROM capture_events
WHERE state = 'published'
  AND captured_ts < ?
  AND NOT EXISTS (
      SELECT 1
      FROM recovery_snapshot_events member
      WHERE member.event_seq = capture_events.seq
  )
  AND NOT EXISTS (
      SELECT 1
      FROM checkpoint_events member
      WHERE member.event_seq = capture_events.seq
  )
  AND NOT EXISTS (
      SELECT 1
      FROM acd_prune_recovery_context context
      WHERE context.event_seq = capture_events.seq
  )
  AND NOT EXISTS (
      SELECT 1
      FROM intent_candidate_events member
      JOIN intent_candidates candidate ON candidate.id=member.candidate_id
      WHERE member.event_seq=capture_events.seq
        AND member.membership_state='active'
        AND candidate.status IN
            ('open','waiting','ready','soft_published','blocked')
  )`, cutoff); err != nil {
		return 0, fmt.Errorf("state: select published intent prune set: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM intent_capture_dependencies
WHERE prerequisite_seq IN (SELECT event_seq FROM acd_prune_intent_events)
   OR dependent_seq IN (SELECT event_seq FROM acd_prune_intent_events)`); err != nil {
		return 0, fmt.Errorf("state: prune intent dependency edges: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM intent_candidate_events
WHERE event_seq IN (SELECT event_seq FROM acd_prune_intent_events)`); err != nil {
		return 0, fmt.Errorf("state: prune terminal intent membership: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
DELETE FROM capture_events
WHERE seq IN (SELECT event_seq FROM acd_prune_intent_events)`)
	if err != nil {
		return 0, fmt.Errorf("state: prune published events: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: prune events rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acd_prune_recovery_context`); err != nil {
		return 0, fmt.Errorf("state: clear completed published prune context: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acd_prune_intent_events`); err != nil {
		return 0, fmt.Errorf("state: clear completed intent prune set: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("state: commit published prune: %w", err)
	}
	return int(n), nil
}

// PruneRecoverySnapshotEventsBefore removes old published or recovered rows
// belonging to one recovery snapshot. The caller must hold a Git transaction
// lock that verifies recovery_snapshots.recovery_ref still resolves exactly to
// recovery_snapshots.commit_oid for the duration of this statement.
func PruneRecoverySnapshotEventsBefore(ctx context.Context, d *DB, snapshotID int64, cutoff float64) (int, error) {
	if d == nil || snapshotID < 1 {
		return 0, fmt.Errorf("state: PruneRecoverySnapshotEventsBefore: invalid selector")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("state: begin protected recovery snapshot prune: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
CREATE TEMP TABLE IF NOT EXISTS acd_prune_snapshot_events(
    event_seq INTEGER PRIMARY KEY
) WITHOUT ROWID`); err != nil {
		return 0, fmt.Errorf("state: create protected recovery snapshot prune set: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acd_prune_snapshot_events`); err != nil {
		return 0, fmt.Errorf("state: clear protected recovery snapshot prune set: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO acd_prune_snapshot_events(event_seq)
SELECT capture_events.seq
FROM capture_events
WHERE state IN ('published', 'recovered')
  AND captured_ts < ?
  AND EXISTS (
      SELECT 1
      FROM recovery_snapshot_events member
      WHERE member.snapshot_id = ?
        AND member.event_seq = capture_events.seq
  )
  AND NOT EXISTS (
      SELECT 1
      FROM checkpoint_events member
      WHERE member.event_seq = capture_events.seq
  )
  AND NOT EXISTS (
      SELECT 1
      FROM publication_drain_events member
      WHERE member.event_seq = capture_events.seq
  )
  AND NOT EXISTS (
      SELECT 1
      FROM intent_candidate_events member
      JOIN intent_candidates candidate ON candidate.id=member.candidate_id
      WHERE member.event_seq=capture_events.seq
        AND member.membership_state='active'
        AND candidate.status IN
            ('open','waiting','ready','soft_published','blocked')
  )`, cutoff, snapshotID); err != nil {
		return 0, fmt.Errorf("state: select protected recovery snapshot prune set: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM intent_capture_dependencies
WHERE prerequisite_seq IN (SELECT event_seq FROM acd_prune_snapshot_events)
   OR dependent_seq IN (SELECT event_seq FROM acd_prune_snapshot_events)`); err != nil {
		return 0, fmt.Errorf("state: prune protected recovery snapshot intent dependencies: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM intent_candidate_events
WHERE event_seq IN (SELECT event_seq FROM acd_prune_snapshot_events)`); err != nil {
		return 0, fmt.Errorf("state: prune protected recovery snapshot intent membership: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
DELETE FROM capture_events
WHERE seq IN (SELECT event_seq FROM acd_prune_snapshot_events)`)
	if err != nil {
		return 0, fmt.Errorf("state: prune protected recovery snapshot events: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: prune protected recovery snapshot event rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acd_prune_snapshot_events`); err != nil {
		return 0, fmt.Errorf("state: clear completed recovery snapshot prune set: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("state: commit protected recovery snapshot prune: %w", err)
	}
	return int(n), nil
}

// PruneTerminalEventsBefore deliberately preserves every blocked_conflict and
// failed row. A valid recovery snapshot transition changes those rows to
// recovered/published atomically, so a terminal failure can never be both
// snapshot-protected and still terminal. Keep this compatibility entry point
// as an explicit no-op; protected-row retention belongs to the daemon's
// Git-locked recovery-snapshot pruning path.
func PruneTerminalEventsBefore(ctx context.Context, d *DB, cutoff float64) (int, error) {
	_ = ctx
	_ = d
	_ = cutoff
	return 0, nil
}

// LatestEventSeq returns the highest seq value present, or 0 if the table is
// empty. Useful as a smoke-test for monotonic ordering and for the daily
// rollup window query.
func LatestEventSeq(ctx context.Context, d *DB) (int64, error) {
	var seq sql.NullInt64
	err := d.readSQL().QueryRowContext(ctx, `SELECT MAX(seq) FROM capture_events`).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("state: latest event seq: %w", err)
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
}

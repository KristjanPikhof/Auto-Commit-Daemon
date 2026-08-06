package state

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

const (
	CheckpointPrepared                = "prepared"
	CheckpointCompleted               = "completed"
	CheckpointNeedsAction             = "needs_action"
	CheckpointReasonWatch             = "watch"
	CheckpointReasonPoll              = "poll"
	CheckpointReasonHint              = "hint"
	CheckpointReasonMigration         = "migration"
	CheckpointReasonMigrationRecovery = "migration_recovery"
	CheckpointReasonPreRestore        = "pre_restore"
	CheckpointReasonRestore           = "restore"
	CheckpointReasonManualBarrier     = "manual_barrier"
)

var (
	ErrCheckpointIdentityMismatch = errors.New("state: checkpoint identity mismatch")
	ErrCheckpointPhaseConflict    = errors.New("state: illegal checkpoint phase transition")
)

// CheckpointExclusion records privacy-safe scan outcomes. Category is a
// bounded classifier such as ignored, sensitive, oversized, unreadable, or
// unstable; paths never enter this table.
type CheckpointExclusion struct {
	Category string
	Count    int64
}

// Checkpoint is the durable SQLite identity for one full protected worktree
// snapshot. Git objects and the private ref are written by the checkpoint
// service; state owns the before-ref and after-ref phases.
type Checkpoint struct {
	ID               string
	Seq              int64
	OperationID      string
	WorktreeID       string
	Reason           string
	ObservationEpoch int64
	CoverageEpoch    int64
	ObservedHead     string
	ObservedRef      string
	TreeOID          string
	CommitOID        string
	Ref              string
	Phase            string
	CreatedTS        float64
	CompletedTS      sql.NullFloat64
	Error            string
	EventSeqs        []int64
	Exclusions       []CheckpointExclusion
}

// CheckpointReadOnlyProjection is safe for status/history callers. A pre-v20
// database reports Available=false without creating or migrating anything.
type CheckpointReadOnlyProjection struct {
	Available     bool
	SchemaVersion int
	Prepared      int
	Completed     int
	NeedsAction   int
	Latest        *Checkpoint
	Recoverable   []Checkpoint
}

// PrepareCheckpoint persists the immutable checkpoint identity with FULL
// SQLite synchronization before the private Git ref may be created. Reusing
// an ID with the exact same identity is idempotent; any mismatch fails closed.
func PrepareCheckpoint(ctx context.Context, d *DB, checkpoint Checkpoint, planDigest string) (bool, error) {
	if d == nil {
		return false, errors.New("state: PrepareCheckpoint: nil db")
	}
	if err := validateCheckpoint(checkpoint, planDigest); err != nil {
		return false, err
	}
	conn, err := d.conn.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("state: acquire checkpoint writer: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return false, fmt.Errorf("state: enable full checkpoint durability: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "PRAGMA synchronous=NORMAL") }()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin checkpoint prepare: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	existing, ok, err := checkpointByIDQuery(ctx, tx, checkpoint.ID, true)
	if err != nil {
		return false, err
	}
	if ok {
		if !sameCheckpointIdentity(existing, checkpoint) {
			return false, ErrCheckpointIdentityMismatch
		}
		return false, nil
	}

	ts := checkpoint.CreatedTS
	if ts <= 0 {
		ts = nowSeconds()
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO operations(
    id, kind, worktree_id, phase, status, plan_digest, created_ts, updated_ts
) VALUES (?, 'checkpoint', ?, 'prepared', 'prepared', ?, ?, ?)`,
		checkpoint.OperationID, checkpoint.WorktreeID, planDigest, ts, ts); err != nil {
		return false, fmt.Errorf("state: insert checkpoint operation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO checkpoints(
    id, seq, operation_id, worktree_id, reason, observation_epoch,
    coverage_epoch, observed_head, observed_ref, tree_oid, commit_oid,
    checkpoint_ref, phase, created_ts
) VALUES (?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM checkpoints),
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?)`,
		checkpoint.ID, checkpoint.OperationID, checkpoint.WorktreeID,
		checkpoint.Reason, checkpoint.ObservationEpoch, checkpoint.CoverageEpoch,
		checkpoint.ObservedHead, checkpoint.ObservedRef, checkpoint.TreeOID,
		checkpoint.CommitOID, checkpoint.Ref, ts); err != nil {
		return false, fmt.Errorf("state: insert checkpoint: %w", err)
	}
	seenEvents := make(map[int64]struct{}, len(checkpoint.EventSeqs))
	for ord, seq := range checkpoint.EventSeqs {
		if seq <= 0 {
			return false, fmt.Errorf("state: checkpoint event %d has invalid sequence", ord)
		}
		if _, duplicate := seenEvents[seq]; duplicate {
			return false, fmt.Errorf("state: checkpoint event sequence %d is duplicated", seq)
		}
		seenEvents[seq] = struct{}{}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO checkpoint_events(checkpoint_id, ord, event_seq) VALUES (?, ?, ?)`,
			checkpoint.ID, ord, seq); err != nil {
			return false, fmt.Errorf("state: insert checkpoint event %d: %w", ord, err)
		}
	}
	seenCategories := make(map[string]struct{}, len(checkpoint.Exclusions))
	for _, exclusion := range checkpoint.Exclusions {
		category := strings.TrimSpace(exclusion.Category)
		if category == "" || len(category) > 64 || exclusion.Count < 0 {
			return false, errors.New("state: invalid checkpoint exclusion")
		}
		if _, duplicate := seenCategories[category]; duplicate {
			return false, fmt.Errorf("state: checkpoint exclusion %q is duplicated", category)
		}
		seenCategories[category] = struct{}{}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO checkpoint_exclusions(checkpoint_id, category, count) VALUES (?, ?, ?)`,
			checkpoint.ID, category, exclusion.Count); err != nil {
			return false, fmt.Errorf("state: insert checkpoint exclusion: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("state: commit checkpoint prepare: %w", err)
	}
	return true, nil
}

// CompleteCheckpoint records the exact-ref observation after Git has made the
// checkpoint commit reachable. Both the operation and checkpoint settle in
// one FULL-synchronous transaction.
func CompleteCheckpoint(ctx context.Context, d *DB, id, expectedRef, expectedCommit string, completedTS float64) error {
	if d == nil {
		return errors.New("state: CompleteCheckpoint: nil db")
	}
	if completedTS <= 0 {
		completedTS = nowSeconds()
	}
	conn, err := d.conn.Conn(ctx)
	if err != nil {
		return fmt.Errorf("state: acquire checkpoint writer: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return fmt.Errorf("state: enable full checkpoint durability: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "PRAGMA synchronous=NORMAL") }()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin checkpoint completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	checkpoint, ok, err := checkpointByIDQuery(ctx, tx, id, false)
	if err != nil {
		return err
	}
	if !ok || checkpoint.Ref != expectedRef || checkpoint.CommitOID != expectedCommit {
		return ErrCheckpointIdentityMismatch
	}
	if checkpoint.Phase == CheckpointCompleted {
		return nil
	}
	if checkpoint.Phase != CheckpointPrepared {
		return ErrCheckpointPhaseConflict
	}
	result, err := tx.ExecContext(ctx, `
UPDATE checkpoints
SET phase='completed', completed_ts=?, error=''
WHERE id=? AND phase='prepared'`, completedTS, id)
	if err != nil {
		return fmt.Errorf("state: complete checkpoint: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return ErrCheckpointPhaseConflict
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE operations
SET phase='completed', status='completed', updated_ts=?, completed_ts=?, error=''
WHERE id=? AND status='prepared'`, completedTS, completedTS, checkpoint.OperationID); err != nil {
		return fmt.Errorf("state: complete checkpoint operation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit checkpoint completion: %w", err)
	}
	return nil
}

// MarkCheckpointNeedsAction durably blocks ambiguous recovery without
// deleting or rewriting the private ref or checkpoint identity.
func MarkCheckpointNeedsAction(ctx context.Context, d *DB, id, message string) error {
	if d == nil {
		return errors.New("state: MarkCheckpointNeedsAction: nil db")
	}
	message = strings.TrimSpace(message)
	if message == "" || len(message) > 2048 {
		return errors.New("state: checkpoint needs-action message is invalid")
	}
	ts := nowSeconds()
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin checkpoint needs-action: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	checkpoint, ok, err := checkpointByIDQuery(ctx, tx, id, false)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrNoRows
	}
	if checkpoint.Phase == CheckpointCompleted {
		return ErrCheckpointPhaseConflict
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE checkpoints SET phase='needs_action', error=? WHERE id=?`, message, id); err != nil {
		return fmt.Errorf("state: mark checkpoint needs-action: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE operations
SET phase='needs_action', status='needs_action', error=?, updated_ts=?
WHERE id=?`, message, ts, checkpoint.OperationID); err != nil {
		return fmt.Errorf("state: mark checkpoint operation needs-action: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit checkpoint needs-action: %w", err)
	}
	return nil
}

// ReadCheckpointProjection opens an existing database read-only and never
// runs migrations. It is suitable for status fallback when no worker answers.
func ReadCheckpointProjection(ctx context.Context, dbPath string, recoverableLimit int) (CheckpointReadOnlyProjection, error) {
	projection := CheckpointReadOnlyProjection{}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	conn, err := sql.Open(driverName, "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return projection, fmt.Errorf("state: open checkpoint projection: %w", err)
	}
	defer conn.Close()
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&projection.SchemaVersion); err != nil {
		return projection, fmt.Errorf("state: read checkpoint schema version: %w", err)
	}
	if projection.SchemaVersion < 20 {
		return projection, nil
	}
	projection.Available = true
	if err := conn.QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN phase='prepared' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN phase='completed' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN phase='needs_action' THEN 1 ELSE 0 END), 0)
FROM checkpoints`).Scan(&projection.Prepared, &projection.Completed, &projection.NeedsAction); err != nil {
		return projection, fmt.Errorf("state: count checkpoints: %w", err)
	}
	latest, ok, err := checkpointByIDOrLatestQuery(ctx, conn, "")
	if err != nil {
		return projection, err
	}
	if ok {
		projection.Latest = &latest
	}
	if recoverableLimit <= 0 {
		recoverableLimit = 100
	}
	rows, err := conn.QueryContext(ctx, checkpointSelect+`
 WHERE phase IN ('prepared','needs_action') ORDER BY seq ASC LIMIT ?`, recoverableLimit)
	if err != nil {
		return projection, fmt.Errorf("state: query recoverable checkpoints: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		checkpoint, err := scanCheckpoint(rows)
		if err != nil {
			return projection, err
		}
		projection.Recoverable = append(projection.Recoverable, checkpoint)
	}
	return projection, rows.Err()
}

func validateCheckpoint(checkpoint Checkpoint, planDigest string) error {
	if !validCheckpointID(checkpoint.ID) {
		return errors.New("state: invalid checkpoint id")
	}
	if checkpoint.OperationID == "" || len(checkpoint.OperationID) > 128 {
		return errors.New("state: invalid checkpoint operation id")
	}
	if !validHexID(checkpoint.WorktreeID, 16) {
		return errors.New("state: invalid checkpoint worktree id")
	}
	if !validCheckpointReason(checkpoint.Reason) {
		return errors.New("state: invalid checkpoint reason")
	}
	if checkpoint.ObservationEpoch < 0 || checkpoint.CoverageEpoch < 0 || checkpoint.CoverageEpoch > checkpoint.ObservationEpoch {
		return errors.New("state: invalid checkpoint coverage epoch")
	}
	if checkpoint.TreeOID == "" || checkpoint.CommitOID == "" {
		return errors.New("state: checkpoint object ids are required")
	}
	wantRef := "refs/acd/checkpoints/v1/" + checkpoint.WorktreeID + "/" + checkpoint.ID
	if checkpoint.Ref != wantRef {
		return errors.New("state: checkpoint ref does not match its identity")
	}
	if !validDigest(planDigest) {
		return errors.New("state: invalid checkpoint plan digest")
	}
	return nil
}

func validCheckpointID(id string) bool {
	parts := strings.Split(id, "-")
	if len(parts) != 3 || parts[0] != "cp" || len(parts[1]) < 1 || len(parts[2]) != 16 {
		return false
	}
	for _, r := range parts[1] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return validHexID(parts[2], 16)
}

func validHexID(value string, length int) bool {
	if len(value) != length || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validDigest(value string) bool {
	return len(value) == 71 && strings.HasPrefix(value, "sha256:") && validHexID(strings.TrimPrefix(value, "sha256:"), 64)
}

func validCheckpointReason(reason string) bool {
	switch reason {
	case CheckpointReasonWatch, CheckpointReasonPoll, CheckpointReasonHint,
		CheckpointReasonMigration, CheckpointReasonMigrationRecovery,
		CheckpointReasonPreRestore, CheckpointReasonRestore,
		CheckpointReasonManualBarrier:
		return true
	default:
		return false
	}
}

const checkpointSelect = `
SELECT id, seq, operation_id, worktree_id, reason, observation_epoch,
       coverage_epoch, observed_head, observed_ref, tree_oid, commit_oid,
       checkpoint_ref, phase, created_ts, completed_ts, error
FROM checkpoints`

type checkpointQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type checkpointRows interface {
	Scan(...any) error
}

func checkpointByIDQuery(ctx context.Context, query checkpointQuery, id string, loadChildren bool) (Checkpoint, bool, error) {
	checkpoint, ok, err := checkpointByIDOrLatestQuery(ctx, query, id)
	if err != nil || !ok || !loadChildren {
		return checkpoint, ok, err
	}
	childQuery, ok := query.(interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	})
	if !ok {
		return Checkpoint{}, false, errors.New("state: checkpoint query cannot load membership")
	}
	rows, err := childQuery.QueryContext(ctx, `
SELECT event_seq FROM checkpoint_events WHERE checkpoint_id=? ORDER BY ord`, id)
	if err != nil {
		return Checkpoint{}, false, fmt.Errorf("state: load checkpoint events: %w", err)
	}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			rows.Close()
			return Checkpoint{}, false, fmt.Errorf("state: scan checkpoint event: %w", err)
		}
		checkpoint.EventSeqs = append(checkpoint.EventSeqs, seq)
	}
	if err := rows.Close(); err != nil {
		return Checkpoint{}, false, err
	}
	exclusionRows, err := childQuery.QueryContext(ctx, `
SELECT category, count FROM checkpoint_exclusions WHERE checkpoint_id=? ORDER BY category`, id)
	if err != nil {
		return Checkpoint{}, false, fmt.Errorf("state: load checkpoint exclusions: %w", err)
	}
	defer exclusionRows.Close()
	for exclusionRows.Next() {
		var exclusion CheckpointExclusion
		if err := exclusionRows.Scan(&exclusion.Category, &exclusion.Count); err != nil {
			return Checkpoint{}, false, fmt.Errorf("state: scan checkpoint exclusion: %w", err)
		}
		checkpoint.Exclusions = append(checkpoint.Exclusions, exclusion)
	}
	return checkpoint, true, exclusionRows.Err()
}

func checkpointByIDOrLatestQuery(ctx context.Context, query checkpointQuery, id string) (Checkpoint, bool, error) {
	statement := checkpointSelect + " WHERE id=?"
	args := []any{id}
	if id == "" {
		statement = checkpointSelect + " ORDER BY seq DESC LIMIT 1"
		args = nil
	}
	checkpoint, err := scanCheckpoint(query.QueryRowContext(ctx, statement, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, err
	}
	return checkpoint, true, nil
}

func scanCheckpoint(row checkpointRows) (Checkpoint, error) {
	var checkpoint Checkpoint
	if err := row.Scan(
		&checkpoint.ID, &checkpoint.Seq, &checkpoint.OperationID,
		&checkpoint.WorktreeID, &checkpoint.Reason,
		&checkpoint.ObservationEpoch, &checkpoint.CoverageEpoch,
		&checkpoint.ObservedHead, &checkpoint.ObservedRef,
		&checkpoint.TreeOID, &checkpoint.CommitOID, &checkpoint.Ref,
		&checkpoint.Phase, &checkpoint.CreatedTS, &checkpoint.CompletedTS,
		&checkpoint.Error,
	); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

func sameCheckpointIdentity(left, right Checkpoint) bool {
	if left.ID != right.ID || left.OperationID != right.OperationID ||
		left.WorktreeID != right.WorktreeID || left.Reason != right.Reason ||
		left.ObservationEpoch != right.ObservationEpoch || left.CoverageEpoch != right.CoverageEpoch ||
		left.ObservedHead != right.ObservedHead || left.ObservedRef != right.ObservedRef ||
		left.TreeOID != right.TreeOID || left.CommitOID != right.CommitOID || left.Ref != right.Ref ||
		len(left.EventSeqs) != len(right.EventSeqs) {
		return false
	}
	for i := range left.EventSeqs {
		if left.EventSeqs[i] != right.EventSeqs[i] {
			return false
		}
	}
	return true
}

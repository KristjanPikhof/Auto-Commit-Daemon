package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ShadowPath is one row of shadow_paths (§6.1). The (branch_ref,
// branch_generation, path) tuple is the primary key — bumping
// branch_generation effectively invalidates the previous shadow set without
// having to delete it (history retained for forensic queries).
type ShadowPath struct {
	BranchRef        string
	BranchGeneration int64
	Path             string
	Operation        string
	Mode             sql.NullString
	OID              sql.NullString
	OldPath          sql.NullString
	BaseHead         string
	Fidelity         string
	UpdatedTS        float64
}

// UpsertShadowPath writes (or replaces) a shadow row for a given branch
// generation + path. Sensitive paths must be filtered by the caller before
// reaching here (see sensitive.go). This helper does not re-check, so the
// daemon must call IsSensitivePath upstream — kept that way to avoid hashing
// or persisting bytes for paths the caller has already decided to skip.
func UpsertShadowPath(ctx context.Context, d *DB, sp ShadowPath) error {
	if sp.BranchRef == "" || sp.Path == "" || sp.BaseHead == "" || sp.Operation == "" || sp.Fidelity == "" {
		return fmt.Errorf("state: UpsertShadowPath: required field missing")
	}
	if sp.UpdatedTS == 0 {
		sp.UpdatedTS = nowSeconds()
	}
	const q = `
INSERT INTO shadow_paths(
    branch_ref, branch_generation, path, operation, mode, oid,
    old_path, base_head, fidelity, updated_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(branch_ref, branch_generation, path) DO UPDATE SET
    operation  = excluded.operation,
    mode       = excluded.mode,
    oid        = excluded.oid,
    old_path   = excluded.old_path,
    base_head  = excluded.base_head,
    fidelity   = excluded.fidelity,
    updated_ts = excluded.updated_ts`
	if _, err := d.conn.ExecContext(ctx, q,
		sp.BranchRef, sp.BranchGeneration, sp.Path, sp.Operation, sp.Mode, sp.OID,
		sp.OldPath, sp.BaseHead, sp.Fidelity, sp.UpdatedTS,
	); err != nil {
		return fmt.Errorf("state: upsert shadow path: %w", err)
	}
	return nil
}

// AppendShadowBatch upserts a batch of ShadowPath rows in a single
// transaction with a reused prepared statement, so bootstrap reseeds avoid
// the per-row begin/commit fsync overhead of UpsertShadowPath. The whole
// batch commits atomically — a context cancel mid-batch leaves shadow_paths
// untouched (deferred Rollback runs because Commit was never reached).
//
// Sensitive paths must already have been filtered by the caller; this helper
// does not consult sensitive.go on each row. Empty batches are a no-op.
//
// Required fields per row: BranchRef, Path, BaseHead, Operation, Fidelity.
// UpdatedTS defaults to nowSeconds() when zero. ON CONFLICT semantics match
// UpsertShadowPath (replace on (branch_ref, branch_generation, path)).
func AppendShadowBatch(ctx context.Context, d *DB, rows []ShadowPath) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin shadow batch tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const q = `
INSERT INTO shadow_paths(
    branch_ref, branch_generation, path, operation, mode, oid,
    old_path, base_head, fidelity, updated_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(branch_ref, branch_generation, path) DO UPDATE SET
    operation  = excluded.operation,
    mode       = excluded.mode,
    oid        = excluded.oid,
    old_path   = excluded.old_path,
    base_head  = excluded.base_head,
    fidelity   = excluded.fidelity,
    updated_ts = excluded.updated_ts`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("state: prepare shadow batch insert: %w", err)
	}
	defer stmt.Close()

	for i := range rows {
		sp := rows[i]
		if sp.BranchRef == "" || sp.Path == "" || sp.BaseHead == "" || sp.Operation == "" || sp.Fidelity == "" {
			return fmt.Errorf("state: AppendShadowBatch row %d: required field missing", i)
		}
		if sp.UpdatedTS == 0 {
			sp.UpdatedTS = nowSeconds()
		}
		if _, err := stmt.ExecContext(ctx,
			sp.BranchRef, sp.BranchGeneration, sp.Path, sp.Operation, sp.Mode, sp.OID,
			sp.OldPath, sp.BaseHead, sp.Fidelity, sp.UpdatedTS,
		); err != nil {
			return fmt.Errorf("state: insert shadow batch row %d: %w", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit shadow batch tx: %w", err)
	}
	return nil
}

// GetShadowPath fetches one shadow row. Returns (zero, false, nil) if not
// found. Useful for the capture diff path that asks "did we already record
// this OID for this generation?".
func GetShadowPath(ctx context.Context, d *DB, branchRef string, gen int64, path string) (ShadowPath, bool, error) {
	const q = `
SELECT branch_ref, branch_generation, path, operation, mode, oid,
       old_path, base_head, fidelity, updated_ts
FROM shadow_paths
WHERE branch_ref = ? AND branch_generation = ? AND path = ?`
	var sp ShadowPath
	err := d.readSQL().QueryRowContext(ctx, q, branchRef, gen, path).Scan(
		&sp.BranchRef, &sp.BranchGeneration, &sp.Path, &sp.Operation, &sp.Mode, &sp.OID,
		&sp.OldPath, &sp.BaseHead, &sp.Fidelity, &sp.UpdatedTS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ShadowPath{}, false, nil
	}
	if err != nil {
		return ShadowPath{}, false, fmt.Errorf("state: get shadow path: %w", err)
	}
	return sp, true, nil
}

// DeleteShadowGeneration removes every shadow row for a (branch_ref, gen)
// pair. Called by the GC pass when an old generation is no longer referenced
// by any pending capture_event.
func DeleteShadowGeneration(ctx context.Context, d *DB, branchRef string, gen int64) (int, error) {
	res, err := d.conn.ExecContext(ctx,
		`DELETE FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
		branchRef, gen)
	if err != nil {
		return 0, fmt.Errorf("state: delete shadow generation: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: delete shadow generation rows: %w", err)
	}
	return int(n), nil
}

// InvalidateShadowGeneration atomically removes one exact shadow pair and its
// bootstrap marker. Branch-transition rollback uses this when a token changes
// after a prospective reseed: leaving either rows or marker behind could make
// the next tick accept a shadow built from the wrong HEAD.
func InvalidateShadowGeneration(ctx context.Context, d *DB, branchRef string, gen int64, markerKey string) error {
	if d == nil || branchRef == "" || gen < 1 || markerKey == "" {
		return fmt.Errorf("state: InvalidateShadowGeneration: invalid selector")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: invalidate shadow generation: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
		branchRef, gen); err != nil {
		return fmt.Errorf("state: invalidate shadow generation rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM daemon_meta WHERE key = ?`, markerKey); err != nil {
		return fmt.Errorf("state: invalidate shadow generation marker: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: invalidate shadow generation: commit: %w", err)
	}
	return nil
}

// RebaseShadowGeneration advances the recorded HEAD for an existing shadow
// without changing its live path snapshot. ACD-owned history repair preserves
// the worktree and index, so rebuilding this shadow from the repaired HEAD
// would forget already-captured dirty content and capture it again.
func RebaseShadowGeneration(
	ctx context.Context,
	d *DB,
	branchRef string,
	gen int64,
	baseHead string,
) (int, error) {
	if d == nil || branchRef == "" || gen < 1 || baseHead == "" {
		return 0, fmt.Errorf("state: RebaseShadowGeneration: invalid selector")
	}
	res, err := d.conn.ExecContext(ctx, `
UPDATE shadow_paths
SET base_head = ?
WHERE branch_ref = ? AND branch_generation = ?`,
		baseHead, branchRef, gen)
	if err != nil {
		return 0, fmt.Errorf("state: rebase shadow generation: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: rebase shadow generation rows: %w", err)
	}
	return int(n), nil
}

// ReplaceShadowGeneration atomically swaps one generation's complete shadow
// snapshot and keeps its bootstrap marker in the same transaction. Branch
// reconciliation uses this when a fresh HEAD baseline must retain a small set
// of already-captured dirty paths: a crash must expose either the old snapshot
// or the complete merged snapshot, never an in-between state.
func ReplaceShadowGeneration(
	ctx context.Context,
	d *DB,
	branchRef string,
	gen int64,
	markerKey string,
	rows []ShadowPath,
) (int, error) {
	if d == nil || branchRef == "" || gen < 1 || markerKey == "" {
		return 0, fmt.Errorf("state: ReplaceShadowGeneration: invalid selector")
	}
	if err := validateShadowReplacement(
		branchRef, gen, markerKey, rows); err != nil {
		return 0, err
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("state: replace shadow generation: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := replaceShadowGenerationTx(
		ctx, tx, branchRef, gen, markerKey, rows); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("state: replace shadow generation: commit: %w", err)
	}
	return len(rows), nil
}

func validateShadowReplacement(
	branchRef string,
	gen int64,
	markerKey string,
	rows []ShadowPath,
) error {
	if branchRef == "" || gen < 1 || markerKey == "" {
		return fmt.Errorf("state: ReplaceShadowGeneration: invalid selector")
	}
	for i := range rows {
		row := rows[i]
		if row.BranchRef != branchRef || row.BranchGeneration != gen ||
			row.Path == "" || row.BaseHead == "" || row.Operation == "" ||
			row.Fidelity == "" {
			return fmt.Errorf(
				"state: ReplaceShadowGeneration row %d: invalid identity", i)
		}
	}
	return nil
}

func replaceShadowGenerationTx(
	ctx context.Context,
	tx *sql.Tx,
	branchRef string,
	gen int64,
	markerKey string,
	rows []ShadowPath,
) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
		branchRef, gen); err != nil {
		return fmt.Errorf("state: replace shadow generation rows: %w", err)
	}
	const insert = `
INSERT INTO shadow_paths(
    branch_ref, branch_generation, path, operation, mode, oid,
    old_path, base_head, fidelity, updated_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	stmt, err := tx.PrepareContext(ctx, insert)
	if err != nil {
		return fmt.Errorf("state: prepare replacement shadow rows: %w", err)
	}
	defer stmt.Close()
	for i := range rows {
		row := rows[i]
		if row.UpdatedTS == 0 {
			row.UpdatedTS = nowSeconds()
		}
		if _, err := stmt.ExecContext(ctx,
			row.BranchRef, row.BranchGeneration, row.Path, row.Operation,
			row.Mode, row.OID, row.OldPath, row.BaseHead, row.Fidelity,
			row.UpdatedTS); err != nil {
			return fmt.Errorf(
				"state: insert replacement shadow row %d: %w", i, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO daemon_meta(key, value, updated_ts) VALUES (?, '1', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value,
                               updated_ts = excluded.updated_ts`,
		markerKey, nowSeconds()); err != nil {
		return fmt.Errorf("state: replace shadow bootstrap marker: %w", err)
	}
	return nil
}

// PruneShadowGenerations deletes shadow rows for branch generations older than
// the configured retention window behind currentGeneration. retainBehind is the
// number of prior generations to keep in addition to the current generation.
func PruneShadowGenerations(ctx context.Context, d *DB, branchRef string, currentGeneration, retainBehind int64) (int, error) {
	if branchRef == "" {
		return 0, fmt.Errorf("state: PruneShadowGenerations: empty branch_ref")
	}
	if currentGeneration <= 0 {
		return 0, fmt.Errorf("state: PruneShadowGenerations: invalid current generation %d", currentGeneration)
	}
	if retainBehind < 0 {
		retainBehind = 0
	}
	minGeneration := currentGeneration - retainBehind
	res, err := d.conn.ExecContext(ctx,
		`DELETE FROM shadow_paths WHERE branch_ref = ? AND branch_generation < ?`,
		branchRef, minGeneration)
	if err != nil {
		return 0, fmt.Errorf("state: prune shadow generations: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: prune shadow generation rows: %w", err)
	}
	return int(n), nil
}

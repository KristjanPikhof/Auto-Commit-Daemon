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

package state

import (
	"context"
	"database/sql"
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

// validateShadowReplacement guards explicit archival recovery of a known chain.
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

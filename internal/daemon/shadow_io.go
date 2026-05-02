// shadow_io.go bridges the daemon-internal classify shapes to the
// state.ShadowPath / sql.NullString types persisted in shadow_paths.
package daemon

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// loadShadow returns the persisted shadow map keyed by path for the active
// (branch, generation). Sensitive paths must already have been excluded
// from the table on insert so we do not re-filter here.
func loadShadow(ctx context.Context, db *state.DB, cctx CaptureContext) (map[string]ShadowEntry, error) {
	// Read via the multi-connection read pool so a long-running replay
	// drain holding the serialized writer connection does not block the
	// per-pass shadow load (which capture relies on for diff classification).
	rows, err := db.ReadSQL().QueryContext(ctx,
		`SELECT path, mode, oid FROM shadow_paths
		   WHERE branch_ref = ? AND branch_generation = ?`,
		cctx.BranchRef, cctx.BranchGeneration)
	if err != nil {
		return nil, fmt.Errorf("daemon: query shadow: %w", err)
	}
	defer rows.Close()
	out := map[string]ShadowEntry{}
	for rows.Next() {
		var path string
		var mode, oid sql.NullString
		if err := rows.Scan(&path, &mode, &oid); err != nil {
			return nil, fmt.Errorf("daemon: scan shadow: %w", err)
		}
		out[path] = ShadowEntry{Path: path, Mode: mode.String, OID: oid.String}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("daemon: iter shadow: %w", err)
	}
	return out, nil
}

// updateShadow reflects one classified op back into shadow_paths so the next
// capture pass diffs against the new live state.
func updateShadow(ctx context.Context, db *state.DB, cctx CaptureContext, op ClassifiedOp) error {
	switch op.Op {
	case "delete":
		_, err := db.SQL().ExecContext(ctx,
			`DELETE FROM shadow_paths WHERE branch_ref=? AND branch_generation=? AND path=?`,
			cctx.BranchRef, cctx.BranchGeneration, op.Path)
		return err
	case "rename":
		// Drop the old path, then upsert the new one.
		if _, err := db.SQL().ExecContext(ctx,
			`DELETE FROM shadow_paths WHERE branch_ref=? AND branch_generation=? AND path=?`,
			cctx.BranchRef, cctx.BranchGeneration, op.OldPath); err != nil {
			return err
		}
		fallthrough
	case "create", "modify", "mode":
		return state.UpsertShadowPath(ctx, db, state.ShadowPath{
			BranchRef:        cctx.BranchRef,
			BranchGeneration: cctx.BranchGeneration,
			Path:             op.Path,
			Operation:        op.Op,
			Mode:             sql.NullString{String: op.AfterMode, Valid: op.AfterMode != ""},
			OID:              sql.NullString{String: op.AfterOID, Valid: op.AfterOID != ""},
			OldPath:          sql.NullString{String: op.OldPath, Valid: op.OldPath != ""},
			BaseHead:         cctx.BaseHead,
			Fidelity:         op.Fidelity,
		})
	}
	return fmt.Errorf("daemon: updateShadow: unknown op %q", op.Op)
}

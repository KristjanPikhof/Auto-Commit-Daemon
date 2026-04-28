package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Publish is the singleton publish_state row (§6.1) that tracks the daemon's
// in-flight replay attempt. Status values are caller-defined; canonical set:
// "idle", "publishing", "succeeded", "failed".
type Publish struct {
	EventSeq         sql.NullInt64
	BranchRef        sql.NullString
	BranchGeneration sql.NullInt64
	SourceHead       sql.NullString
	TargetCommitOID  sql.NullString
	Status           string
	Error            sql.NullString
	UpdatedTS        float64
}

// LoadPublishState reads the singleton publish_state row. Returns
// (zero{Status:"idle"}, false, nil) if not yet written.
func LoadPublishState(ctx context.Context, d *DB) (Publish, bool, error) {
	const q = `
SELECT event_seq, branch_ref, branch_generation, source_head, target_commit_oid,
       status, error, updated_ts
FROM publish_state WHERE id = 1`
	var p Publish
	err := d.conn.QueryRowContext(ctx, q).Scan(
		&p.EventSeq, &p.BranchRef, &p.BranchGeneration, &p.SourceHead, &p.TargetCommitOID,
		&p.Status, &p.Error, &p.UpdatedTS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Publish{Status: "idle"}, false, nil
	}
	if err != nil {
		return Publish{}, false, fmt.Errorf("state: load publish_state: %w", err)
	}
	return p, true, nil
}

// SavePublishState upserts the singleton publish_state row.
func SavePublishState(ctx context.Context, d *DB, p Publish) error {
	if p.Status == "" {
		p.Status = "idle"
	}
	if p.UpdatedTS == 0 {
		p.UpdatedTS = nowSeconds()
	}
	const q = `
INSERT INTO publish_state(
    id, event_seq, branch_ref, branch_generation, source_head, target_commit_oid,
    status, error, updated_ts
) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    event_seq         = excluded.event_seq,
    branch_ref        = excluded.branch_ref,
    branch_generation = excluded.branch_generation,
    source_head       = excluded.source_head,
    target_commit_oid = excluded.target_commit_oid,
    status            = excluded.status,
    error             = excluded.error,
    updated_ts        = excluded.updated_ts`
	if _, err := d.conn.ExecContext(ctx, q,
		p.EventSeq, p.BranchRef, p.BranchGeneration, p.SourceHead, p.TargetCommitOID,
		p.Status, p.Error, p.UpdatedTS,
	); err != nil {
		return fmt.Errorf("state: save publish_state: %w", err)
	}
	return nil
}

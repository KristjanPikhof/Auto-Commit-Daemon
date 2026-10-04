package state

import (
	"context"
	"database/sql"
	"errors"
)

var ErrCheckpointPartial = errors.New("checkpoint has incomplete coverage; choose a complete checkpoint for full-tree restore")

// CheckpointCaptureIssue names only eligible paths, never privacy exclusions.
type CheckpointCaptureIssue struct {
	Path    string `json:"path"`
	Subtree bool   `json:"subtree,omitempty"`
	Reason  string `json:"reason"`
}

type coverageQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func CurrentCaptureIssues(ctx context.Context, db *DB) ([]CheckpointCaptureIssue, error) {
	id, ok, err := MetaGet(ctx, db, "protection.checkpoint_id")
	if err != nil || !ok || id == "" {
		return nil, err
	}
	checkpoint := Checkpoint{ID: id}
	err = loadCheckpointCoverage(ctx, db.ReadSQL(), &checkpoint)
	if err != nil {
		return nil, err
	}
	if checkpoint.Partial && len(checkpoint.CaptureIssues) == 0 {
		return nil, errors.New("partial checkpoint capture scope is unavailable")
	}
	return checkpoint.CaptureIssues, nil
}

func loadCheckpointCoverage(ctx context.Context, query coverageQuery, checkpoint *Checkpoint) error {
	if err := query.QueryRowContext(ctx, `SELECT coverage_complete=0 FROM checkpoints WHERE id=?`, checkpoint.ID).Scan(&checkpoint.Partial); err != nil {
		return err
	}
	if !checkpoint.Partial {
		return nil
	}
	rows, err := query.QueryContext(ctx, `SELECT path,subtree,reason FROM checkpoint_capture_issues WHERE checkpoint_id=? ORDER BY path`, checkpoint.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var issue CheckpointCaptureIssue
		if err := rows.Scan(&issue.Path, &issue.Subtree, &issue.Reason); err != nil {
			return err
		}
		checkpoint.CaptureIssues = append(checkpoint.CaptureIssues, issue)
	}
	return rows.Err()
}

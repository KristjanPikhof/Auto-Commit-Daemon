package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// CaptureHealth is shared by read-only CLI projections. Liveness is separate.
type CaptureHealth struct {
	NextRetryTS    float64                  `json:"next_retry_ts,omitempty"`
	LastProgressTS float64                  `json:"last_progress_ts,omitempty"`
	CheckpointID   string                   `json:"checkpoint_id,omitempty"`
	State          string                   `json:"state"`
	Error          string                   `json:"error,omitempty"`
	SinceTS        float64                  `json:"since_ts,omitempty"`
	LastSuccessTS  float64                  `json:"last_success_ts,omitempty"`
	IssueCount     int                      `json:"issue_count,omitempty"`
	Issues         []CheckpointCaptureIssue `json:"issues,omitempty"`
}

func ReadCaptureHealth(ctx context.Context, query checkpointQuery) (CaptureHealth, error) {
	health := CaptureHealth{State: "healthy"}
	var raw string
	var errorUpdatedTS float64
	err := query.QueryRowContext(ctx, `SELECT value FROM daemon_meta WHERE key='capture.health'`).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return health, err
	}
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &health); err != nil {
			return health, err
		}
	}
	err = query.QueryRowContext(ctx, `SELECT value,updated_ts FROM daemon_meta WHERE key='last_capture_error'`).Scan(&raw, &errorUpdatedTS)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return health, err
	}
	if err == nil && raw != "" {
		if health.Error != raw || health.State == "healthy" {
			health.State = "blocked"
		}
		health.Error = raw
		if health.SinceTS == 0 {
			health.SinceTS = errorUpdatedTS
		}
	} else if err == nil && raw == "" {
		health = CaptureHealth{State: "healthy", LastSuccessTS: health.LastSuccessTS, LastProgressTS: health.LastProgressTS, CheckpointID: health.CheckpointID}
	}
	return health, nil
}

func RecordCaptureHealth(ctx context.Context, db *DB, failure, checkpointID string, now time.Time, nextRetry ...time.Time) error {
	health, err := ReadCaptureHealth(ctx, db.ReadSQL())
	if err != nil {
		return err
	}
	ts := float64(now.UnixNano()) / float64(time.Second)
	if failure != "" {
		if health.Error == "" || health.SinceTS == 0 {
			health.SinceTS = ts
		}
		health.State = "blocked"
		health.Error = failure
		if len(nextRetry) > 0 {
			health.NextRetryTS = float64(nextRetry[0].UnixNano()) / 1e9
		}
		if checkpointID != "" {
			checkpoint := Checkpoint{ID: checkpointID}
			err := loadCheckpointCoverage(ctx, db.ReadSQL(), &checkpoint)
			if err != nil {
				return err
			}
			{
				if checkpoint.ID != health.CheckpointID {
					health.LastProgressTS = ts
					health.CheckpointID = checkpoint.ID
				}
				retryable := len(checkpoint.CaptureIssues) > 0
				for _, issue := range checkpoint.CaptureIssues {
					if issue.Reason != "unstable" && issue.Reason != "lstat_error" {
						retryable = false
					}
				}
				if retryable && ts-health.SinceTS < 120 {
					health.State = "retrying"
				}
				health.IssueCount = len(checkpoint.CaptureIssues)
				health.Issues = checkpoint.CaptureIssues[:min(10, len(checkpoint.CaptureIssues))]
			}
		}
	} else {
		progress := health.LastProgressTS
		if checkpointID != "" && checkpointID != health.CheckpointID {
			progress = ts
		}
		health = CaptureHealth{State: "healthy", LastSuccessTS: ts, LastProgressTS: progress, CheckpointID: checkpointID}
	}
	encoded, err := json.Marshal(health)
	if err != nil {
		return err
	}
	old, _, err := MetaGet(ctx, db, "capture.health")
	if err != nil {
		return err
	}
	if old == string(encoded) {
		return nil
	}
	return MetaSetMany(ctx, db, map[string]string{"capture.health": string(encoded), "last_capture_error": failure})
}

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
	State string `json:"state"`
	Error string `json:"error,omitempty"`
	SinceTS float64 `json:"since_ts,omitempty"`
	LastSuccessTS float64 `json:"last_success_ts,omitempty"`
	IssueCount int `json:"issue_count,omitempty"`
	Issues []CheckpointCaptureIssue `json:"issues,omitempty"`
}

func ReadCaptureHealth(ctx context.Context, query checkpointQuery) (CaptureHealth, error) {
	health := CaptureHealth{State:"healthy"}
	var raw string
	err := query.QueryRowContext(ctx, `SELECT value FROM daemon_meta WHERE key='capture.health'`).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) { return health, err }
	if raw != "" { if err := json.Unmarshal([]byte(raw), &health); err != nil { return health, err } }
	err = query.QueryRowContext(ctx, `SELECT value FROM daemon_meta WHERE key='last_capture_error'`).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) { return health, err }
	if err == nil && raw != "" { health.State="blocked"; health.Error=raw }
	return health, nil
}

func RecordCaptureHealth(ctx context.Context, db *DB, failure, checkpointID string, now time.Time) error {
	health, err := ReadCaptureHealth(ctx, db.ReadSQL())
	if err != nil { return err }
	ts := float64(now.UnixNano())/float64(time.Second)
	if failure != "" {
		if health.State != "blocked" || health.SinceTS == 0 { health.SinceTS=ts }
		health.State="blocked"; health.Error=failure
		if checkpointID != "" {
			checkpoint, ok, err := checkpointByIDQuery(ctx, db.ReadSQL(), checkpointID, true)
			if err != nil { return err }
			if ok { health.IssueCount=len(checkpoint.CaptureIssues); health.Issues=checkpoint.CaptureIssues[:min(10,len(checkpoint.CaptureIssues))] }
		}
	} else {
		health=CaptureHealth{State:"healthy",LastSuccessTS:ts}
	}
	encoded, err := json.Marshal(health)
	if err != nil { return err }
	old, _, err := MetaGet(ctx, db, "capture.health")
	if err != nil { return err }
	if old == string(encoded) { return nil }
	return MetaSetMany(ctx, db, map[string]string{"capture.health":string(encoded),"last_capture_error":failure})
}

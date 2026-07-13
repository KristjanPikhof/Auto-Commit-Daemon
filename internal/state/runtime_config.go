package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

const (
	ActivationPending      = "pending"
	ActivationAcknowledged = "acknowledged"
	ActivationApplied      = "applied"
	ActivationRejected     = "rejected"
	ActivationCancelled    = "cancelled"

	ExperimentActive    = "active"
	ExperimentCompleted = "completed"
	ExperimentExpired   = "expired"
	ExperimentFailed    = "failed"
	ExperimentCancelled = "cancelled"
)

// ConfigRevision is an immutable, non-secret resolved runtime snapshot.
type ConfigRevision struct {
	ID               int64
	CreatedTS        float64
	Profile          string
	Scope            string
	SnapshotJSON     string
	SnapshotHash     string
	SourceGeneration int64
	Reason           sql.NullString
}

type ConfigRevisionInput struct {
	Snapshot         []byte
	Profile          string
	Scope            string
	SourceGeneration int64
	Reason           string
	CreatedTS        float64
}

// RuntimeConfigState is the singleton desired/applied activation projection.
type RuntimeConfigState struct {
	DesiredRevisionID       sql.NullInt64
	AppliedRevisionID       sql.NullInt64
	LastKnownGoodRevisionID sql.NullInt64
	DesiredRequestID        sql.NullInt64
	DesiredTS               sql.NullFloat64
	AppliedTS               sql.NullFloat64
	LastError               sql.NullString
	UpdatedTS               float64
}

type ConfigActivationRequest struct {
	ID                     int64
	RevisionID             int64
	PriorDesiredRevisionID sql.NullInt64
	Status                 string
	RequestedTS            float64
	AcknowledgedTS         sql.NullFloat64
	CompletedTS            sql.NullFloat64
	Error                  sql.NullString
}

// ConfigExperiment describes a bounded candidate-revision run.
type ConfigExperiment struct {
	ID                  int64
	BaselineRevisionID  int64
	CandidateRevisionID int64
	WindowBudget        int
	CompletedWindows    int
	ExpiresTS           sql.NullFloat64
	FailurePolicy       string
	Status              string
	CreatedTS           float64
	UpdatedTS           float64
	CompletedTS         sql.NullFloat64
	TerminalReason      sql.NullString
}

type ConfigExperimentInput struct {
	BaselineRevisionID  int64
	CandidateRevisionID int64
	WindowBudget        int
	ExpiresTS           sql.NullFloat64
	FailurePolicy       string
}

// InsertConfigRevision canonicalizes and validates a resolved JSON snapshot
// before appending it. Secret-looking keys are rejected recursively.
func InsertConfigRevision(ctx context.Context, d *DB, in ConfigRevisionInput) (ConfigRevision, error) {
	if d == nil {
		return ConfigRevision{}, fmt.Errorf("state: InsertConfigRevision: nil db")
	}
	if strings.TrimSpace(in.Profile) == "" || strings.TrimSpace(in.Scope) == "" {
		return ConfigRevision{}, fmt.Errorf("state: config revision profile and scope are required")
	}
	if in.SourceGeneration < 0 {
		return ConfigRevision{}, fmt.Errorf("state: config revision source generation must be non-negative")
	}
	canonical, err := canonicalNonSecretJSON(in.Snapshot)
	if err != nil {
		return ConfigRevision{}, err
	}
	sum := sha256.Sum256(canonical)
	created := in.CreatedTS
	if created <= 0 {
		created = nowSeconds()
	}
	reason := nullableSanitizedText(in.Reason)
	res, err := d.conn.ExecContext(ctx, `
INSERT INTO config_revisions(
    created_ts, profile, scope, snapshot_json, snapshot_hash,
    source_generation, reason
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		created, strings.TrimSpace(in.Profile), strings.TrimSpace(in.Scope),
		string(canonical), hex.EncodeToString(sum[:]), in.SourceGeneration, reason)
	if err != nil {
		return ConfigRevision{}, fmt.Errorf("state: insert config revision: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return ConfigRevision{}, fmt.Errorf("state: config revision id: %w", err)
	}
	return ConfigRevisionByID(ctx, d, id)
}

func ConfigRevisionByID(ctx context.Context, d *DB, id int64) (ConfigRevision, error) {
	if d == nil || id <= 0 {
		return ConfigRevision{}, fmt.Errorf("state: ConfigRevisionByID: invalid input")
	}
	var rev ConfigRevision
	err := d.readSQL().QueryRowContext(ctx, `
SELECT id, created_ts, profile, scope, snapshot_json, snapshot_hash,
       source_generation, reason
FROM config_revisions WHERE id = ?`, id).Scan(
		&rev.ID, &rev.CreatedTS, &rev.Profile, &rev.Scope, &rev.SnapshotJSON,
		&rev.SnapshotHash, &rev.SourceGeneration, &rev.Reason)
	if err != nil {
		return ConfigRevision{}, fmt.Errorf("state: get config revision: %w", err)
	}
	return rev, nil
}

func RuntimeConfigActivationState(ctx context.Context, d *DB) (RuntimeConfigState, error) {
	if d == nil {
		return RuntimeConfigState{}, fmt.Errorf("state: RuntimeConfigActivationState: nil db")
	}
	var out RuntimeConfigState
	err := d.readSQL().QueryRowContext(ctx, `
SELECT desired_revision_id, applied_revision_id, last_known_good_revision_id,
       desired_request_id, desired_ts, applied_ts, last_error, updated_ts
FROM runtime_config_state WHERE id = 1`).Scan(
		&out.DesiredRevisionID, &out.AppliedRevisionID,
		&out.LastKnownGoodRevisionID, &out.DesiredRequestID, &out.DesiredTS,
		&out.AppliedTS, &out.LastError, &out.UpdatedTS)
	if errors.Is(err, sql.ErrNoRows) {
		return RuntimeConfigState{}, nil
	}
	if err != nil {
		return RuntimeConfigState{}, fmt.Errorf("state: get runtime config state: %w", err)
	}
	return out, nil
}

// RequestConfigActivation atomically replaces desired only when its current
// revision matches expectedDesired. A null expectation means no desired
// revision. The bool is false for a stale writer.
func RequestConfigActivation(ctx context.Context, d *DB, revisionID int64, expectedDesired sql.NullInt64) (ConfigActivationRequest, bool, error) {
	if d == nil || revisionID <= 0 {
		return ConfigActivationRequest{}, false, fmt.Errorf("state: RequestConfigActivation: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return ConfigActivationRequest{}, false, fmt.Errorf("state: begin activation request: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureRuntimeConfigState(ctx, tx); err != nil {
		return ConfigActivationRequest{}, false, err
	}
	var current sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT desired_revision_id FROM runtime_config_state WHERE id=1`).Scan(&current); err != nil {
		return ConfigActivationRequest{}, false, fmt.Errorf("state: read desired revision: %w", err)
	}
	if !equalNullInt64(current, expectedDesired) {
		return ConfigActivationRequest{}, false, nil
	}
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM config_revisions WHERE id=?`, revisionID).Scan(&exists); err != nil || exists != 1 {
		if err == nil {
			err = sql.ErrNoRows
		}
		return ConfigActivationRequest{}, false, fmt.Errorf("state: desired config revision: %w", err)
	}
	now := nowSeconds()
	res, err := tx.ExecContext(ctx, `
INSERT INTO config_activation_requests(
    revision_id, prior_desired_revision_id, status, requested_ts
) VALUES (?, ?, 'pending', ?)`, revisionID, current, now)
	if err != nil {
		return ConfigActivationRequest{}, false, fmt.Errorf("state: insert activation request: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return ConfigActivationRequest{}, false, fmt.Errorf("state: activation request id: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_config_state
SET desired_revision_id=?, desired_request_id=?, desired_ts=?, last_error=NULL, updated_ts=?
WHERE id=1`, revisionID, id, now, now); err != nil {
		return ConfigActivationRequest{}, false, fmt.Errorf("state: set desired revision: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ConfigActivationRequest{}, false, fmt.Errorf("state: commit activation request: %w", err)
	}
	return ConfigActivationRequest{ID: id, RevisionID: revisionID,
		PriorDesiredRevisionID: current, Status: ActivationPending, RequestedTS: now}, true, nil
}

func AcknowledgeConfigActivation(ctx context.Context, d *DB, requestID, revisionID int64) (bool, error) {
	return transitionActivation(ctx, d, requestID, revisionID, ActivationPending, ActivationAcknowledged, "")
}

func ApplyConfigActivation(ctx context.Context, d *DB, requestID, revisionID int64) (bool, error) {
	if d == nil || requestID <= 0 || revisionID <= 0 {
		return false, fmt.Errorf("state: ApplyConfigActivation: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin apply activation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := nowSeconds()
	res, err := tx.ExecContext(ctx, `
UPDATE config_activation_requests
SET status='applied', completed_ts=?, error=NULL
WHERE id=? AND revision_id=? AND status='acknowledged'
  AND EXISTS (SELECT 1 FROM runtime_config_state
              WHERE id=1 AND desired_request_id=? AND desired_revision_id=?)`,
		now, requestID, revisionID, requestID, revisionID)
	if err != nil {
		return false, fmt.Errorf("state: apply activation request: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_config_state
SET applied_revision_id=?, last_known_good_revision_id=?, applied_ts=?,
    last_error=NULL, updated_ts=?
WHERE id=1 AND desired_request_id=? AND desired_revision_id=?`,
		revisionID, revisionID, now, now, requestID, revisionID); err != nil {
		return false, fmt.Errorf("state: update applied revision: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("state: commit applied revision: %w", err)
	}
	return true, nil
}

func RejectConfigActivation(ctx context.Context, d *DB, requestID, revisionID int64, reason string) (bool, error) {
	return transitionActivation(ctx, d, requestID, revisionID, "pending|acknowledged", ActivationRejected, reason)
}

func CancelConfigActivation(ctx context.Context, d *DB, requestID, revisionID int64, reason string) (bool, error) {
	return transitionActivation(ctx, d, requestID, revisionID, "pending|acknowledged", ActivationCancelled, reason)
}

// RevertConfigActivation copies a known-good snapshot into a new immutable
// revision and requests it with the same desired-revision CAS contract.
func RevertConfigActivation(ctx context.Context, d *DB, sourceRevisionID int64, expectedDesired sql.NullInt64, sourceGeneration int64) (ConfigRevision, ConfigActivationRequest, bool, error) {
	source, err := ConfigRevisionByID(ctx, d, sourceRevisionID)
	if err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, false, err
	}
	rev, err := InsertConfigRevision(ctx, d, ConfigRevisionInput{
		Snapshot: []byte(source.SnapshotJSON), Profile: source.Profile,
		Scope: source.Scope, SourceGeneration: sourceGeneration,
		Reason: "revert from revision", // Deliberately excludes user/provider text.
	})
	if err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, false, err
	}
	req, ok, err := RequestConfigActivation(ctx, d, rev.ID, expectedDesired)
	return rev, req, ok, err
}

func ActivationRequestByID(ctx context.Context, d *DB, id int64) (ConfigActivationRequest, error) {
	var out ConfigActivationRequest
	err := d.readSQL().QueryRowContext(ctx, `
SELECT id, revision_id, prior_desired_revision_id, status, requested_ts,
       acknowledged_ts, completed_ts, error
FROM config_activation_requests WHERE id=?`, id).Scan(
		&out.ID, &out.RevisionID, &out.PriorDesiredRevisionID, &out.Status,
		&out.RequestedTS, &out.AcknowledgedTS, &out.CompletedTS, &out.Error)
	if err != nil {
		return ConfigActivationRequest{}, fmt.Errorf("state: get activation request: %w", err)
	}
	return out, nil
}

func CreateConfigExperiment(ctx context.Context, d *DB, in ConfigExperimentInput) (ConfigExperiment, error) {
	if d == nil || in.BaselineRevisionID <= 0 || in.CandidateRevisionID <= 0 ||
		in.BaselineRevisionID == in.CandidateRevisionID || in.WindowBudget <= 0 {
		return ConfigExperiment{}, fmt.Errorf("state: CreateConfigExperiment: invalid input")
	}
	policy := strings.TrimSpace(in.FailurePolicy)
	if policy == "" {
		policy = "continue"
	}
	now := nowSeconds()
	res, err := d.conn.ExecContext(ctx, `
INSERT INTO config_experiments(
    baseline_revision_id, candidate_revision_id, window_budget,
    expires_ts, failure_policy, status, created_ts, updated_ts
) SELECT ?, ?, ?, ?, ?, 'active', ?, ?
WHERE EXISTS (SELECT 1 FROM config_revisions WHERE id=?)
  AND EXISTS (SELECT 1 FROM config_revisions WHERE id=?)`,
		in.BaselineRevisionID, in.CandidateRevisionID, in.WindowBudget,
		in.ExpiresTS, policy, now, now, in.BaselineRevisionID, in.CandidateRevisionID)
	if err != nil {
		return ConfigExperiment{}, fmt.Errorf("state: create config experiment: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil || id == 0 {
		if err == nil {
			err = sql.ErrNoRows
		}
		return ConfigExperiment{}, fmt.Errorf("state: config experiment revisions: %w", err)
	}
	return ConfigExperimentByID(ctx, d, id)
}

func ConfigExperimentByID(ctx context.Context, d *DB, id int64) (ConfigExperiment, error) {
	var out ConfigExperiment
	err := d.readSQL().QueryRowContext(ctx, `
SELECT id, baseline_revision_id, candidate_revision_id, window_budget,
       completed_windows, expires_ts, failure_policy, status, created_ts,
       updated_ts, completed_ts, terminal_reason
FROM config_experiments WHERE id=?`, id).Scan(
		&out.ID, &out.BaselineRevisionID, &out.CandidateRevisionID,
		&out.WindowBudget, &out.CompletedWindows, &out.ExpiresTS,
		&out.FailurePolicy, &out.Status, &out.CreatedTS, &out.UpdatedTS,
		&out.CompletedTS, &out.TerminalReason)
	if err != nil {
		return ConfigExperiment{}, fmt.Errorf("state: get config experiment: %w", err)
	}
	return out, nil
}

func FinishConfigExperiment(ctx context.Context, d *DB, id int64, status, reason string) (bool, error) {
	if status != ExperimentExpired && status != ExperimentFailed && status != ExperimentCancelled {
		return false, fmt.Errorf("state: invalid terminal experiment status %q", status)
	}
	now := nowSeconds()
	res, err := d.conn.ExecContext(ctx, `
UPDATE config_experiments
SET status=?, updated_ts=?, completed_ts=?, terminal_reason=?
WHERE id=? AND status='active'`, status, now, now, nullableSanitizedText(reason), id)
	if err != nil {
		return false, fmt.Errorf("state: finish config experiment: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func ensureRuntimeConfigState(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO runtime_config_state(id, updated_ts) VALUES (1, ?)`, nowSeconds())
	if err != nil {
		return fmt.Errorf("state: initialize runtime config state: %w", err)
	}
	return nil
}

func transitionActivation(ctx context.Context, d *DB, requestID, revisionID int64, from, to, reason string) (bool, error) {
	if d == nil || requestID <= 0 || revisionID <= 0 {
		return false, fmt.Errorf("state: transition activation: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin activation transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := nowSeconds()
	fromClause := "status=?"
	args := []any{to}
	if to == ActivationAcknowledged {
		args = append(args, now)
	} else {
		args = append(args, now, nullableSanitizedText(reason))
	}
	if from == "pending|acknowledged" {
		fromClause = "status IN ('pending','acknowledged')"
	} else {
		args = append(args, from)
	}
	setClause := "status=?, acknowledged_ts=?"
	if to != ActivationAcknowledged {
		setClause = "status=?, completed_ts=?, error=?"
	}
	args = append(args, requestID, revisionID, requestID, revisionID)
	q := `UPDATE config_activation_requests SET ` + setClause + `
WHERE id=? AND revision_id=? AND ` + fromClause + `
  AND EXISTS (SELECT 1 FROM runtime_config_state
              WHERE id=1 AND desired_request_id=? AND desired_revision_id=?)`
	res, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		return false, fmt.Errorf("state: transition activation request: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if to == ActivationRejected || to == ActivationCancelled {
		clearDesired := to == ActivationCancelled
		if clearDesired {
			_, err = tx.ExecContext(ctx, `
UPDATE runtime_config_state SET desired_revision_id=NULL, desired_request_id=NULL,
    desired_ts=NULL, last_error=?, updated_ts=?
WHERE id=1 AND desired_request_id=? AND desired_revision_id=?`,
				nullableSanitizedText(reason), now, requestID, revisionID)
		} else {
			_, err = tx.ExecContext(ctx, `
UPDATE runtime_config_state SET last_error=?, updated_ts=?
WHERE id=1 AND desired_request_id=? AND desired_revision_id=?`,
				nullableSanitizedText(reason), now, requestID, revisionID)
		}
		if err != nil {
			return false, fmt.Errorf("state: update rejected desired state: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("state: commit activation transition: %w", err)
	}
	return true, nil
}

func canonicalNonSecretJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, fmt.Errorf("state: invalid config snapshot JSON: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("state: invalid config snapshot JSON: trailing value")
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, fmt.Errorf("state: config snapshot must be a JSON object")
	}
	if key, ok := secretBearingKey(value); ok {
		return nil, fmt.Errorf("state: config snapshot contains forbidden secret key %q", key)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("state: canonicalize config snapshot: %w", err)
	}
	return canonical, nil
}

func secretBearingKey(value any) (string, bool) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.ToLower(key))
			for _, marker := range []string{"apikey", "secret", "password", "credential", "accesstoken", "authtoken"} {
				if strings.Contains(normalized, marker) {
					return key, true
				}
			}
			if key, ok := secretBearingKey(child); ok {
				return key, true
			}
		}
	case []any:
		for _, child := range typed {
			if key, ok := secretBearingKey(child); ok {
				return key, true
			}
		}
	}
	return "", false
}

func nullableSanitizedText(value string) sql.NullString {
	value = strings.TrimSpace(strings.Map(func(r rune) rune {
		if r == '\x1b' || (!unicode.IsPrint(r) && !unicode.IsSpace(r)) {
			return -1
		}
		return r
	}, value))
	if len(value) > 1024 {
		value = value[:1024]
	}
	return sql.NullString{String: value, Valid: value != ""}
}

func equalNullInt64(a, b sql.NullInt64) bool {
	return a.Valid == b.Valid && (!a.Valid || a.Int64 == b.Int64)
}

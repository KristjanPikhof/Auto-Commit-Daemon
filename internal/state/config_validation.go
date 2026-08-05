package state

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

const (
	ConfigValidationQueued    = "queued"
	ConfigValidationRunning   = "running"
	ConfigValidationPassed    = "passed"
	ConfigValidationFailed    = "failed"
	ConfigValidationTimedOut  = "timed_out"
	ConfigValidationCancelled = "cancelled"

	configValidationOutputLimit = 64 * 1024
	configValidationErrorLimit  = 2048
)

// ConfigValidationSpec is the non-secret, fingerprint-bound setup gate stored
// with a new activation request. The raw command remains in the immutable
// runtime revision and is represented here only by its digest.
type ConfigValidationSpec struct {
	BranchRef        string
	BranchGeneration int64
	ExpectedHead     string
	Mode             string
	CommandSource    string
	CommandDigest    string
	ApprovalID       string
	RequestedTS      float64
}

type ConfigValidationRun struct {
	ID                  int64
	ActivationRequestID int64
	RevisionID          int64
	Attempt             int
	BranchRef           string
	BranchGeneration    int64
	ExpectedHead        string
	Mode                string
	CommandSource       string
	CommandDigest       string
	ApprovalID          string
	Status              string
	OwnerPID            sql.NullInt64
	RequestedTS         float64
	StartedTS           sql.NullFloat64
	CompletedTS         sql.NullFloat64
	ExitCode            sql.NullInt64
	SanitizedOutput     string
	BoundedError        sql.NullString
}

// CreateConfigActivationWithValidation appends one immutable revision,
// requests it, and queues its first validation attempt in one transaction.
func CreateConfigActivationWithValidation(
	ctx context.Context,
	d *DB,
	revisionInput ConfigRevisionInput,
	expectedDesired sql.NullInt64,
	spec ConfigValidationSpec,
) (ConfigRevision, ConfigActivationRequest, ConfigValidationRun, bool, error) {
	if d == nil {
		return ConfigRevision{}, ConfigActivationRequest{}, ConfigValidationRun{}, false,
			errors.New("state: CreateConfigActivationWithValidation: nil db")
	}
	prepared, err := prepareConfigRevision(revisionInput)
	if err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, ConfigValidationRun{}, false, err
	}
	spec, err = validateConfigValidationSpec(spec)
	if err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, ConfigValidationRun{}, false, err
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, ConfigValidationRun{}, false,
			fmt.Errorf("state: begin validated activation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	revisionID, err := insertConfigRevisionTx(ctx, tx, prepared)
	if err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, ConfigValidationRun{}, false, err
	}
	request, ok, err := requestConfigActivationTx(ctx, tx, revisionID, expectedDesired)
	if err != nil || !ok {
		return ConfigRevision{}, request, ConfigValidationRun{}, ok, err
	}
	run, err := insertConfigValidationTx(ctx, tx, request.ID, revisionID, 1, spec)
	if err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, ConfigValidationRun{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, ConfigValidationRun{}, false,
			fmt.Errorf("state: commit validated activation: %w", err)
	}
	revision, err := ConfigRevisionByID(ctx, d, revisionID)
	if err != nil {
		return ConfigRevision{}, ConfigActivationRequest{}, ConfigValidationRun{}, false, err
	}
	return revision, request, run, true, nil
}

func ConfigValidationByID(
	ctx context.Context,
	d *DB,
	id int64,
) (ConfigValidationRun, error) {
	if d == nil || id <= 0 {
		return ConfigValidationRun{}, errors.New("state: ConfigValidationByID: invalid input")
	}
	run, err := scanConfigValidation(d.readSQL().QueryRowContext(ctx, `
SELECT id, activation_request_id, revision_id, attempt, branch_ref,
       branch_generation, expected_head, mode, command_source, command_digest,
       approval_id, status, owner_pid, requested_ts, started_ts, completed_ts,
       exit_code, sanitized_output, bounded_error
FROM config_validation_runs WHERE id=?`, id))
	if err != nil {
		return ConfigValidationRun{}, fmt.Errorf("state: get config validation: %w", err)
	}
	return run, nil
}

func LatestConfigValidationForRequest(
	ctx context.Context,
	d *DB,
	requestID int64,
) (ConfigValidationRun, bool, error) {
	if d == nil || requestID <= 0 {
		return ConfigValidationRun{}, false,
			errors.New("state: LatestConfigValidationForRequest: invalid input")
	}
	run, err := scanConfigValidation(d.readSQL().QueryRowContext(ctx, `
SELECT id, activation_request_id, revision_id, attempt, branch_ref,
       branch_generation, expected_head, mode, command_source, command_digest,
       approval_id, status, owner_pid, requested_ts, started_ts, completed_ts,
       exit_code, sanitized_output, bounded_error
FROM config_validation_runs
WHERE activation_request_id=?
ORDER BY attempt DESC LIMIT 1`, requestID))
	if errors.Is(err, sql.ErrNoRows) {
		return ConfigValidationRun{}, false, nil
	}
	if err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: get latest config validation: %w", err)
	}
	return run, true, nil
}

func DesiredConfigValidation(
	ctx context.Context,
	d *DB,
) (ConfigValidationRun, bool, error) {
	if d == nil {
		return ConfigValidationRun{}, false,
			errors.New("state: DesiredConfigValidation: nil db")
	}
	projection, err := RuntimeConfigActivationState(ctx, d)
	if err != nil {
		return ConfigValidationRun{}, false, err
	}
	if !projection.DesiredRequestID.Valid {
		return ConfigValidationRun{}, false, nil
	}
	return LatestConfigValidationForRequest(
		ctx, d, projection.DesiredRequestID.Int64,
	)
}

// ClaimNextConfigValidation atomically claims the oldest queued attempt only
// when it still belongs to the current desired activation request.
func ClaimNextConfigValidation(
	ctx context.Context,
	d *DB,
	ownerPID int,
) (ConfigValidationRun, bool, error) {
	if d == nil || ownerPID <= 0 {
		return ConfigValidationRun{}, false,
			errors.New("state: ClaimNextConfigValidation: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: begin config validation claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var id int64
	err = tx.QueryRowContext(ctx, `
SELECT v.id
FROM config_validation_runs v
JOIN runtime_config_state r
  ON r.id=1 AND r.desired_request_id=v.activation_request_id
JOIN config_activation_requests a
  ON a.id=v.activation_request_id
 AND a.revision_id=v.revision_id
 AND a.status='pending'
WHERE v.status='queued'
ORDER BY v.id
LIMIT 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return ConfigValidationRun{}, false, nil
	}
	if err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: select config validation claim: %w", err)
	}
	now := nowSeconds()
	res, err := tx.ExecContext(ctx, `
UPDATE config_validation_runs
SET status='running', owner_pid=?, started_ts=?, completed_ts=NULL,
    exit_code=NULL, sanitized_output='', bounded_error=NULL
WHERE id=? AND status='queued'`, ownerPID, now, id)
	if err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: claim config validation: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return ConfigValidationRun{}, false, err
	}
	run, err := scanConfigValidation(tx.QueryRowContext(ctx, `
SELECT id, activation_request_id, revision_id, attempt, branch_ref,
       branch_generation, expected_head, mode, command_source, command_digest,
       approval_id, status, owner_pid, requested_ts, started_ts, completed_ts,
       exit_code, sanitized_output, bounded_error
FROM config_validation_runs WHERE id=?`, id))
	if err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: load claimed config validation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: commit config validation claim: %w", err)
	}
	return run, true, nil
}

func CompleteConfigValidation(
	ctx context.Context,
	d *DB,
	runID int64,
	ownerPID int,
	status string,
	exitCode sql.NullInt64,
	output string,
	boundedError string,
) (bool, error) {
	if d == nil || runID <= 0 || ownerPID <= 0 ||
		(status != ConfigValidationPassed &&
			status != ConfigValidationFailed &&
			status != ConfigValidationTimedOut &&
			status != ConfigValidationCancelled) {
		return false, errors.New("state: CompleteConfigValidation: invalid input")
	}
	output = boundedConfigValidationText(output, configValidationOutputLimit)
	boundedError = boundedConfigValidationText(boundedError, configValidationErrorLimit)
	res, err := d.conn.ExecContext(ctx, `
UPDATE config_validation_runs
SET status=?, completed_ts=?, exit_code=?, sanitized_output=?,
    bounded_error=?, owner_pid=NULL
WHERE id=? AND status='running' AND owner_pid=?`,
		status, nowSeconds(), exitCode, output,
		nullableSanitizedText(boundedError), runID, ownerPID)
	if err != nil {
		return false, fmt.Errorf("state: complete config validation: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func RetryConfigValidation(
	ctx context.Context,
	d *DB,
	requestID int64,
) (ConfigValidationRun, bool, error) {
	if d == nil || requestID <= 0 {
		return ConfigValidationRun{}, false,
			errors.New("state: RetryConfigValidation: invalid input")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: begin config validation retry: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	latest, err := scanConfigValidation(tx.QueryRowContext(ctx, `
SELECT id, activation_request_id, revision_id, attempt, branch_ref,
       branch_generation, expected_head, mode, command_source, command_digest,
       approval_id, status, owner_pid, requested_ts, started_ts, completed_ts,
       exit_code, sanitized_output, bounded_error
FROM config_validation_runs
WHERE activation_request_id=?
ORDER BY attempt DESC LIMIT 1`, requestID))
	if errors.Is(err, sql.ErrNoRows) {
		return ConfigValidationRun{}, false, nil
	}
	if err != nil {
		return ConfigValidationRun{}, false, fmt.Errorf("state: load config validation retry: %w", err)
	}
	if latest.Status != ConfigValidationFailed &&
		latest.Status != ConfigValidationTimedOut &&
		latest.Status != ConfigValidationCancelled {
		return ConfigValidationRun{}, false, nil
	}
	var desiredRequest sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT desired_request_id FROM runtime_config_state WHERE id=1`,
	).Scan(&desiredRequest); err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: read desired request for retry: %w", err)
	}
	if !desiredRequest.Valid || desiredRequest.Int64 != requestID {
		return ConfigValidationRun{}, false, nil
	}
	spec := ConfigValidationSpec{
		BranchRef:        latest.BranchRef,
		BranchGeneration: latest.BranchGeneration,
		ExpectedHead:     latest.ExpectedHead,
		Mode:             latest.Mode,
		CommandSource:    latest.CommandSource,
		CommandDigest:    latest.CommandDigest,
		ApprovalID:       latest.ApprovalID,
		RequestedTS:      nowSeconds(),
	}
	run, err := insertConfigValidationTx(
		ctx, tx, requestID, latest.RevisionID, latest.Attempt+1, spec,
	)
	if err != nil {
		return ConfigValidationRun{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return ConfigValidationRun{}, false,
			fmt.Errorf("state: commit config validation retry: %w", err)
	}
	return run, true, nil
}

func CancelSupersededConfigValidations(
	ctx context.Context,
	d *DB,
	currentRequestID int64,
	reason string,
) (int64, error) {
	if d == nil || currentRequestID <= 0 {
		return 0, errors.New("state: CancelSupersededConfigValidations: invalid input")
	}
	res, err := d.conn.ExecContext(ctx, `
UPDATE config_validation_runs
SET status='cancelled', completed_ts=?, owner_pid=NULL, bounded_error=?
WHERE activation_request_id<>? AND status IN ('queued','running')`,
		nowSeconds(),
		nullableSanitizedText(boundedConfigValidationText(reason, configValidationErrorLimit)),
		currentRequestID)
	if err != nil {
		return 0, fmt.Errorf("state: cancel superseded config validations: %w", err)
	}
	return res.RowsAffected()
}

func RequeueConfigValidation(
	ctx context.Context,
	d *DB,
	runID int64,
	ownerPID int,
) (bool, error) {
	if d == nil || runID <= 0 || ownerPID <= 0 {
		return false, errors.New("state: RequeueConfigValidation: invalid input")
	}
	res, err := d.conn.ExecContext(ctx, `
UPDATE config_validation_runs
SET status='queued', owner_pid=NULL, started_ts=NULL, completed_ts=NULL,
    exit_code=NULL, sanitized_output='', bounded_error=NULL
WHERE id=? AND status='running' AND owner_pid=?`, runID, ownerPID)
	if err != nil {
		return false, fmt.Errorf("state: requeue config validation: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func RunningConfigValidations(
	ctx context.Context,
	d *DB,
) ([]ConfigValidationRun, error) {
	if d == nil {
		return nil, errors.New("state: RunningConfigValidations: nil db")
	}
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT id, activation_request_id, revision_id, attempt, branch_ref,
       branch_generation, expected_head, mode, command_source, command_digest,
       approval_id, status, owner_pid, requested_ts, started_ts, completed_ts,
       exit_code, sanitized_output, bounded_error
FROM config_validation_runs
WHERE status='running'
ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("state: list running config validations: %w", err)
	}
	defer rows.Close()
	var runs []ConfigValidationRun
	for rows.Next() {
		run, err := scanConfigValidation(rows)
		if err != nil {
			return nil, fmt.Errorf("state: scan running config validation: %w", err)
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

type configValidationScanner interface {
	Scan(dest ...any) error
}

func scanConfigValidation(row configValidationScanner) (ConfigValidationRun, error) {
	var run ConfigValidationRun
	err := row.Scan(
		&run.ID, &run.ActivationRequestID, &run.RevisionID, &run.Attempt,
		&run.BranchRef, &run.BranchGeneration, &run.ExpectedHead, &run.Mode,
		&run.CommandSource, &run.CommandDigest, &run.ApprovalID, &run.Status,
		&run.OwnerPID, &run.RequestedTS, &run.StartedTS, &run.CompletedTS,
		&run.ExitCode, &run.SanitizedOutput, &run.BoundedError,
	)
	return run, err
}

func insertConfigValidationTx(
	ctx context.Context,
	tx *sql.Tx,
	requestID int64,
	revisionID int64,
	attempt int,
	spec ConfigValidationSpec,
) (ConfigValidationRun, error) {
	var err error
	spec, err = validateConfigValidationSpec(spec)
	if err != nil {
		return ConfigValidationRun{}, err
	}
	if attempt <= 0 {
		return ConfigValidationRun{}, errors.New("state: config validation attempt must be positive")
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO config_validation_runs(
    activation_request_id, revision_id, attempt, branch_ref,
    branch_generation, expected_head, mode, command_source, command_digest,
    approval_id, status, requested_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)`,
		requestID, revisionID, attempt, spec.BranchRef, spec.BranchGeneration,
		spec.ExpectedHead, spec.Mode, spec.CommandSource, spec.CommandDigest,
		spec.ApprovalID, spec.RequestedTS)
	if err != nil {
		return ConfigValidationRun{}, fmt.Errorf("state: insert config validation: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return ConfigValidationRun{}, fmt.Errorf("state: config validation id: %w", err)
	}
	return ConfigValidationRun{
		ID: id, ActivationRequestID: requestID, RevisionID: revisionID,
		Attempt: attempt, BranchRef: spec.BranchRef,
		BranchGeneration: spec.BranchGeneration, ExpectedHead: spec.ExpectedHead,
		Mode: spec.Mode, CommandSource: spec.CommandSource,
		CommandDigest: spec.CommandDigest, ApprovalID: spec.ApprovalID,
		Status: ConfigValidationQueued, RequestedTS: spec.RequestedTS,
	}, nil
}

func validateConfigValidationSpec(
	spec ConfigValidationSpec,
) (ConfigValidationSpec, error) {
	var err error
	spec.BranchRef, err = safeConfigLabel("validation branch", spec.BranchRef)
	if err != nil || spec.BranchRef == "" {
		return ConfigValidationSpec{}, errors.New("state: config validation branch is required")
	}
	if spec.BranchGeneration < 0 {
		return ConfigValidationSpec{}, errors.New("state: config validation generation must be non-negative")
	}
	if !boundedHex(spec.ExpectedHead, 40, 64) {
		return ConfigValidationSpec{}, errors.New("state: config validation expected HEAD is invalid")
	}
	if spec.Mode != "structural" && spec.Mode != "fast" && spec.Mode != "full" {
		return ConfigValidationSpec{}, errors.New("state: config validation mode is invalid")
	}
	spec.CommandSource, err = safeConfigLabel("validation command source", spec.CommandSource)
	if err != nil || spec.CommandSource == "" {
		return ConfigValidationSpec{}, errors.New("state: config validation command source is required")
	}
	if !boundedHex(spec.CommandDigest, 64, 64) {
		return ConfigValidationSpec{}, errors.New("state: config validation command digest is invalid")
	}
	spec.ApprovalID, err = safeConfigLabel("validation approval", spec.ApprovalID)
	if err != nil || spec.ApprovalID == "" {
		return ConfigValidationSpec{}, errors.New("state: config validation approval is required")
	}
	if spec.RequestedTS <= 0 {
		spec.RequestedTS = nowSeconds()
	}
	return spec, nil
}

func boundedHex(value string, minBytes, maxBytes int) bool {
	if len(value) < minBytes || len(value) > maxBytes {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func boundedConfigValidationText(value string, limit int) string {
	value = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || unicode.IsPrint(r) {
			return r
		}
		return -1
	}, value)
	if len(value) > limit {
		value = value[len(value)-limit:]
	}
	return value
}

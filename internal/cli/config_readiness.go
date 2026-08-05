package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type configReadinessReport struct {
	Available            bool   `json:"available"`
	Configuration        string `json:"configuration"`
	Experience           string `json:"experience,omitempty"`
	Validation           string `json:"validation"`
	Command              string `json:"command,omitempty"`
	Source               string `json:"source,omitempty"`
	ExpectedDuration     string `json:"expected_duration,omitempty"`
	ElapsedDuration      string `json:"elapsed_duration,omitempty"`
	Attempt              int    `json:"attempt,omitempty"`
	ExitCode             *int64 `json:"exit_code,omitempty"`
	SanitizedFailureTail string `json:"sanitized_failure_tail,omitempty"`
	Error                string `json:"error,omitempty"`
	ActivationRequestID  int64  `json:"activation_request_id,omitempty"`
	RevisionID           int64  `json:"revision_id,omitempty"`
	ExpectedHead         string `json:"expected_head,omitempty"`
}

func loadConfigReadinessReport(
	ctx context.Context,
	conn *sql.DB,
	now time.Time,
) (configReadinessReport, error) {
	report := configReadinessReport{
		Configuration: "ready",
		Validation:    "not_required",
	}
	hasRuns, err := sqliteTableExists(
		ctx, conn, "config_validation_runs",
	)
	if err != nil {
		return report, err
	}
	hasRuntimeState, err := sqliteTableExists(
		ctx, conn, "runtime_config_state",
	)
	if err != nil {
		return report, err
	}
	report.Available = hasRuns && hasRuntimeState
	if !report.Available {
		return report, nil
	}
	var requestID sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		`SELECT desired_request_id FROM runtime_config_state WHERE id=1`,
	).Scan(&requestID); errors.Is(err, sql.ErrNoRows) {
		return report, nil
	} else if err != nil {
		return report, err
	}
	if !requestID.Valid {
		return report, nil
	}
	var (
		run       state.ConfigValidationRun
		exitCode  sql.NullInt64
		started   sql.NullFloat64
		completed sql.NullFloat64
		bounded   sql.NullString
	)
	err = conn.QueryRowContext(ctx, `
SELECT id, activation_request_id, revision_id, attempt, branch_ref,
       branch_generation, expected_head, mode, command_source, command_digest,
       approval_id, status, owner_pid, requested_ts, started_ts, completed_ts,
       exit_code, sanitized_output, bounded_error
FROM config_validation_runs
WHERE activation_request_id=?
ORDER BY attempt DESC LIMIT 1`, requestID.Int64).Scan(
		&run.ID, &run.ActivationRequestID, &run.RevisionID, &run.Attempt,
		&run.BranchRef, &run.BranchGeneration, &run.ExpectedHead, &run.Mode,
		&run.CommandSource, &run.CommandDigest, &run.ApprovalID, &run.Status,
		&run.OwnerPID, &run.RequestedTS, &started, &completed,
		&exitCode, &run.SanitizedOutput, &bounded,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return report, nil
	}
	if err != nil {
		return report, err
	}
	report.Validation = run.Status
	report.Source = run.CommandSource
	report.Attempt = run.Attempt
	report.ActivationRequestID = run.ActivationRequestID
	report.RevisionID = run.RevisionID
	report.ExpectedHead = run.ExpectedHead
	report.SanitizedFailureTail = run.SanitizedOutput
	report.Error = bounded.String
	if exitCode.Valid {
		value := exitCode.Int64
		report.ExitCode = &value
	}
	switch run.Status {
	case state.ConfigValidationQueued, state.ConfigValidationRunning:
		report.Configuration = "validating"
	case state.ConfigValidationPassed:
		report.Configuration = "ready"
	default:
		report.Configuration = "needs_attention"
	}
	if run.Mode == "full" {
		report.ExpectedDuration = "potentially several minutes"
	} else if run.Mode == "fast" {
		report.ExpectedDuration = "usually under 2 minutes"
	} else {
		report.ExpectedDuration = "usually a few seconds"
	}
	if started.Valid {
		end := now
		if completed.Valid {
			end = time.Unix(0, int64(completed.Float64*1e9))
		}
		begin := time.Unix(0, int64(started.Float64*1e9))
		if end.After(begin) {
			report.ElapsedDuration = formatDurationCompact(end.Sub(begin))
		}
	}
	var snapshot string
	if err := conn.QueryRowContext(ctx,
		`SELECT snapshot_json FROM config_revisions WHERE id=?`,
		run.RevisionID,
	).Scan(&snapshot); err != nil {
		return report, err
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal([]byte(snapshot), &values); err != nil {
		return report, err
	}
	var strategy, preset string
	_ = json.Unmarshal(values[config.FieldCommitStrategy], &strategy)
	_ = json.Unmarshal(values[config.FieldCommitPreset], &preset)
	report.Experience = configureExperienceName(strategy, preset)
	commandField := config.FieldVerificationFastCommand
	if run.Mode == "full" {
		commandField = config.FieldVerificationFullCommand
	}
	if run.Mode != "structural" {
		_ = json.Unmarshal(values[commandField], &report.Command)
		report.Command = safeCommandPreview(report.Command)
	}
	return report, nil
}

func renderConfigReadinessHuman(
	out io.Writer,
	report configReadinessReport,
) {
	if !report.Available && report.Validation == "not_required" {
		return
	}
	fmt.Fprintf(out, "Configuration: %s", report.Configuration)
	if report.Experience != "" {
		fmt.Fprintf(out, " (%s)", report.Experience)
	}
	fmt.Fprintln(out)
	fmt.Fprintf(out, "Validation: %s", report.Validation)
	if report.Attempt > 0 {
		fmt.Fprintf(out, " attempt=%d", report.Attempt)
	}
	if report.ElapsedDuration != "" {
		fmt.Fprintf(out, " elapsed=%s", report.ElapsedDuration)
	}
	fmt.Fprintln(out)
	if report.Command != "" {
		fmt.Fprintf(out, "Validation command: %s\n", report.Command)
	} else if report.Validation != "not_required" {
		fmt.Fprintln(out,
			"Validation check: built-in structural and materialization gates")
	}
	if report.Source != "" {
		fmt.Fprintf(out, "Validation source: %s\n", report.Source)
	}
	if report.SanitizedFailureTail != "" {
		fmt.Fprintln(out, "Validation failure tail:")
		fmt.Fprintln(out, safeConfigureVerificationOutput(
			report.SanitizedFailureTail, 8*1024))
	}
	if report.Error != "" {
		fmt.Fprintf(out, "Validation error: %s\n",
			safePreviewText(report.Error, 512))
	}
}

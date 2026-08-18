package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

const IntentResolvedPlanJSONCap = 512 * 1024

// IntentPlanRun is the privacy-safe durable identity and progress record for
// one unchanged adaptive planning window. It never stores diffs or responses.
type IntentPlanRun struct {
	Fingerprint         string
	BranchRef           string
	BranchGeneration    int64
	Provider            sql.NullString
	Model               sql.NullString
	ConfigRevisionID    sql.NullInt64
	AttemptCount        int
	AttemptLimit        int
	PreservedGroups     [][]int64
	UnresolvedSeqs      []int64
	FindingCodes        []string
	NormalizedPartition sql.NullString
	ProgressState       sql.NullString
	ResolutionMode      sql.NullString
	ResolvedPlanJSON    sql.NullString
	Completed           bool
	CreatedTS           float64
	UpdatedTS           float64
}

// EnsureIntentPlanRun returns the durable row for a fingerprint without
// consuming a remote-attempt slot. Circuit-open evidence fallback uses this
// path so a zero-call resolution is still observable after restart.
func EnsureIntentPlanRun(ctx context.Context, d *DB, run IntentPlanRun) (IntentPlanRun, error) {
	if d == nil || run.Fingerprint == "" || run.BranchRef == "" || run.AttemptLimit < 1 {
		return IntentPlanRun{}, fmt.Errorf("state: EnsureIntentPlanRun: invalid input")
	}
	now := nowSeconds()
	_, err := d.conn.ExecContext(ctx, `
INSERT OR IGNORE INTO intent_plan_runs(
    fingerprint, branch_ref, branch_generation, provider, model,
    config_revision_id, attempt_limit, created_ts, updated_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, run.Fingerprint, run.BranchRef,
		run.BranchGeneration, run.Provider, run.Model, run.ConfigRevisionID,
		run.AttemptLimit, now, now)
	if err != nil {
		return IntentPlanRun{}, fmt.Errorf("state: ensure intent plan run: %w", err)
	}
	current, err := scanIntentPlanRun(d.conn.QueryRowContext(ctx,
		intentPlanRunSelect+` WHERE fingerprint=?`, run.Fingerprint))
	if err != nil {
		return IntentPlanRun{}, err
	}
	return current, nil
}

// ReserveIntentPlanAttempt atomically consumes one remote-attempt slot. A
// restart, flush, or off/on cycle sees the same row and cannot reset the cap.
func ReserveIntentPlanAttempt(ctx context.Context, d *DB, run IntentPlanRun) (IntentPlanRun, bool, error) {
	if d == nil || run.Fingerprint == "" || run.BranchRef == "" || run.AttemptLimit < 1 {
		return IntentPlanRun{}, false, fmt.Errorf("state: ReserveIntentPlanAttempt: invalid input")
	}
	now := nowSeconds()
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return IntentPlanRun{}, false, fmt.Errorf("state: begin intent plan attempt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `
INSERT OR IGNORE INTO intent_plan_runs(
    fingerprint, branch_ref, branch_generation, provider, model,
    config_revision_id, attempt_limit, created_ts, updated_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, run.Fingerprint, run.BranchRef,
		run.BranchGeneration, run.Provider, run.Model, run.ConfigRevisionID,
		run.AttemptLimit, now, now)
	if err != nil {
		return IntentPlanRun{}, false, fmt.Errorf("state: create intent plan run: %w", err)
	}
	current, err := scanIntentPlanRun(tx.QueryRowContext(ctx, intentPlanRunSelect+` WHERE fingerprint=?`, run.Fingerprint))
	if err != nil {
		return IntentPlanRun{}, false, err
	}
	if current.Completed || current.AttemptCount >= current.AttemptLimit {
		if err := tx.Commit(); err != nil {
			return IntentPlanRun{}, false, err
		}
		return current, false, nil
	}
	current.AttemptCount++
	current.UpdatedTS = now
	if _, err := tx.ExecContext(ctx, `
UPDATE intent_plan_runs SET attempt_count=?, updated_ts=? WHERE fingerprint=?`,
		current.AttemptCount, now, current.Fingerprint); err != nil {
		return IntentPlanRun{}, false, fmt.Errorf("state: reserve intent plan attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return IntentPlanRun{}, false, fmt.Errorf("state: commit intent plan attempt: %w", err)
	}
	return current, true, nil
}

// UpdateIntentPlanRun records normalized summaries after an attempt or final
// evidence resolution. All collections are bounded by the Intent window caps.
func UpdateIntentPlanRun(ctx context.Context, d *DB, run IntentPlanRun) error {
	if run.ResolvedPlanJSON.Valid &&
		len(run.ResolvedPlanJSON.String) > IntentResolvedPlanJSONCap {
		return fmt.Errorf("state: resolved intent plan exceeds %d bytes",
			IntentResolvedPlanJSONCap)
	}
	preserved, err := json.Marshal(run.PreservedGroups)
	if err != nil {
		return err
	}
	unresolved, err := json.Marshal(run.UnresolvedSeqs)
	if err != nil {
		return err
	}
	findings, err := json.Marshal(run.FindingCodes)
	if err != nil {
		return err
	}
	res, err := d.conn.ExecContext(ctx, `
UPDATE intent_plan_runs
SET preserved_groups=?, unresolved_seqs=?, finding_codes=?,
    normalized_partition=?, progress_state=?, resolution_mode=?,
    resolved_plan_json=?, completed=?,
    updated_ts=?
WHERE fingerprint=?`, string(preserved), string(unresolved), string(findings),
		run.NormalizedPartition, run.ProgressState, run.ResolutionMode,
		run.ResolvedPlanJSON, boolInt(run.Completed), nowSeconds(), run.Fingerprint)
	if err != nil {
		return fmt.Errorf("state: update intent plan run: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("state: intent plan run %s not found", run.Fingerprint)
	}
	return nil
}

const intentPlanRunSelect = `SELECT fingerprint, branch_ref, branch_generation,
       provider, model, config_revision_id, attempt_count, attempt_limit,
       preserved_groups, unresolved_seqs, finding_codes, normalized_partition,
       progress_state, resolution_mode, resolved_plan_json, completed,
       created_ts, updated_ts
FROM intent_plan_runs`

func scanIntentPlanRun(row *sql.Row) (IntentPlanRun, error) {
	var run IntentPlanRun
	var preserved, unresolved, findings string
	var completed int
	err := row.Scan(&run.Fingerprint, &run.BranchRef, &run.BranchGeneration,
		&run.Provider, &run.Model, &run.ConfigRevisionID, &run.AttemptCount,
		&run.AttemptLimit, &preserved, &unresolved, &findings,
		&run.NormalizedPartition, &run.ProgressState, &run.ResolutionMode,
		&run.ResolvedPlanJSON, &completed, &run.CreatedTS, &run.UpdatedTS)
	if err != nil {
		return IntentPlanRun{}, fmt.Errorf("state: scan intent plan run: %w", err)
	}
	run.Completed = completed != 0
	if err := json.Unmarshal([]byte(preserved), &run.PreservedGroups); err != nil {
		return IntentPlanRun{}, err
	}
	if err := json.Unmarshal([]byte(unresolved), &run.UnresolvedSeqs); err != nil {
		return IntentPlanRun{}, err
	}
	if err := json.Unmarshal([]byte(findings), &run.FindingCodes); err != nil {
		return IntentPlanRun{}, err
	}
	return run, nil
}

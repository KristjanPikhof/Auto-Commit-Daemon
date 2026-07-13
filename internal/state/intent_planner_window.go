package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// IntentPlannerWindowGroup is a privacy-safe summary of one commit group
// selected from a planner-visible window. It stores seqs and rationale, never
// raw diffs.
type IntentPlannerWindowGroup struct {
	SelectedSeqs   []int64 `json:"selected_seqs"`
	OriginalSeqs   []int64 `json:"original_seqs,omitempty"`
	Subject        string  `json:"subject,omitempty"`
	GroupingReason string  `json:"grouping_reason,omitempty"`
}

// IntentPlannerWindowDeferredReason records the planner's explanation for a
// deferred capture seq.
type IntentPlannerWindowDeferredReason struct {
	Seq    int64  `json:"seq"`
	Reason string `json:"reason"`
}

// IntentPlannerWindowEvent indexes how one original capture seq participated
// in a planner window. Offered is true only for seqs that appeared directly in
// the planner request; Hidden marks seqs covered by legacy same-path coalesce.
type IntentPlannerWindowEvent struct {
	EventSeq int64
	Offered  bool
	Hidden   bool
	Selected bool
	Deferred bool
	GroupOrd sql.NullInt64
}

// IntentPlannerWindow is the durable, privacy-safe planner observability
// record. JSON fields are arrays of seqs/groups only; prompt traces remain the
// opt-in source for exact prompts and diffs.
type IntentPlannerWindow struct {
	ID                  int64
	PlannedTS           float64
	Provider            sql.NullString
	Model               sql.NullString
	BranchRef           string
	BranchGeneration    int64
	Source              sql.NullString
	CommitFormat        sql.NullString
	Forced              bool
	ForcedReason        sql.NullString
	ValidationFailure   sql.NullString
	OfferedSeqs         []int64
	VisibleOriginalSeqs []int64
	HiddenSeqs          []int64
	SelectedGroups      []IntentPlannerWindowGroup
	DeferredSeqs        []int64
	DeferredReasons     []IntentPlannerWindowDeferredReason
	Events              []IntentPlannerWindowEvent
	ConfigRevisionID    sql.NullInt64
	ConfigProfile       sql.NullString
	DurationMS          sql.NullInt64
	RetryCount          int
	FallbackUsed        bool
	Outcome             sql.NullString
	ExperimentID        sql.NullInt64
	ExperimentConsumed  bool
}

// AppendIntentPlannerWindow records one planner-visible window plus a compact
// per-event lookup index in a single transaction.
func AppendIntentPlannerWindow(ctx context.Context, d *DB, win IntentPlannerWindow) (int64, error) {
	if win.BranchRef == "" {
		return 0, fmt.Errorf("state: AppendIntentPlannerWindow: empty branch_ref")
	}
	if win.PlannedTS <= 0 {
		win.PlannedTS = nowSeconds()
	}
	offered, err := marshalArray(win.OfferedSeqs)
	if err != nil {
		return 0, fmt.Errorf("state: marshal offered seqs: %w", err)
	}
	visible, err := marshalArray(win.VisibleOriginalSeqs)
	if err != nil {
		return 0, fmt.Errorf("state: marshal visible original seqs: %w", err)
	}
	hidden, err := marshalArray(win.HiddenSeqs)
	if err != nil {
		return 0, fmt.Errorf("state: marshal hidden seqs: %w", err)
	}
	groups, err := marshalArray(win.SelectedGroups)
	if err != nil {
		return 0, fmt.Errorf("state: marshal selected groups: %w", err)
	}
	deferred, err := marshalArray(win.DeferredSeqs)
	if err != nil {
		return 0, fmt.Errorf("state: marshal deferred seqs: %w", err)
	}
	deferredReasons, err := marshalArray(win.DeferredReasons)
	if err != nil {
		return 0, fmt.Errorf("state: marshal deferred reasons: %w", err)
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("state: begin intent planner window tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insWindow = `
INSERT INTO intent_planner_windows(
    planned_ts, provider, model, branch_ref, branch_generation, source,
    commit_format, forced, forced_reason, validation_failure, offered_seqs,
    visible_original_seqs, hidden_seqs, selected_groups, deferred_seqs,
    deferred_reasons, config_revision_id, config_profile, duration_ms,
    retry_count, fallback_used, outcome, experiment_id, experiment_consumed
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`
	res, err := tx.ExecContext(ctx, insWindow,
		win.PlannedTS, win.Provider, win.Model, win.BranchRef, win.BranchGeneration,
		win.Source, win.CommitFormat, boolInt(win.Forced), win.ForcedReason,
		win.ValidationFailure, offered, visible, hidden, groups, deferred,
		deferredReasons, win.ConfigRevisionID, win.ConfigProfile, win.DurationMS,
		win.RetryCount, boolInt(win.FallbackUsed), win.Outcome, win.ExperimentID,
	)
	if err != nil {
		return 0, fmt.Errorf("state: insert intent planner window: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("state: intent planner window id: %w", err)
	}
	if len(win.Events) > 0 {
		const insEvent = `
INSERT INTO intent_planner_window_events(
    window_id, event_seq, offered, hidden, selected, deferred, group_ord
) VALUES (?, ?, ?, ?, ?, ?, ?)`
		stmt, err := tx.PrepareContext(ctx, insEvent)
		if err != nil {
			return 0, fmt.Errorf("state: prepare intent planner window events: %w", err)
		}
		defer stmt.Close()
		for _, ev := range win.Events {
			if ev.EventSeq <= 0 {
				return 0, fmt.Errorf("state: intent planner window event seq must be positive")
			}
			if _, err := stmt.ExecContext(ctx,
				id, ev.EventSeq, boolInt(ev.Offered), boolInt(ev.Hidden),
				boolInt(ev.Selected), boolInt(ev.Deferred), ev.GroupOrd,
			); err != nil {
				return 0, fmt.Errorf("state: insert intent planner window event %d: %w", ev.EventSeq, err)
			}
		}
	}
	consumed, err := consumeExperimentWindow(ctx, tx, id, win)
	if err != nil {
		return 0, err
	}
	if consumed {
		if _, err := tx.ExecContext(ctx, `
UPDATE intent_planner_windows SET experiment_consumed=1 WHERE id=?`, id); err != nil {
			return 0, fmt.Errorf("state: mark planner window experiment consumption: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("state: commit intent planner window tx: %w", err)
	}
	return id, nil
}

// RecentIntentPlannerWindows returns recent planner-window summaries newest
// first.
func RecentIntentPlannerWindows(ctx context.Context, d *DB, limit int) ([]IntentPlannerWindow, error) {
	if limit <= 0 {
		limit = 1
	}
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT id, planned_ts, provider, model, branch_ref, branch_generation, source,
       commit_format, forced, forced_reason, validation_failure, offered_seqs,
       visible_original_seqs, hidden_seqs, selected_groups, deferred_seqs,
       deferred_reasons, config_revision_id, config_profile, duration_ms,
       retry_count, fallback_used, outcome, experiment_id, experiment_consumed
FROM intent_planner_windows
ORDER BY id DESC
LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("state: query intent planner windows: %w", err)
	}
	defer rows.Close()
	windows, err := scanIntentPlannerWindows(rows)
	if err != nil {
		return nil, err
	}
	for i := range windows {
		events, err := intentPlannerWindowEvents(ctx, d, windows[i].ID)
		if err != nil {
			return nil, err
		}
		windows[i].Events = events
	}
	return windows, nil
}

// IntentPlannerWindowForEvent returns the latest planner window that mentions
// eventSeq, if any.
func IntentPlannerWindowForEvent(ctx context.Context, d *DB, eventSeq int64) (IntentPlannerWindow, bool, error) {
	if eventSeq <= 0 {
		return IntentPlannerWindow{}, false, fmt.Errorf("state: IntentPlannerWindowForEvent: invalid seq %d", eventSeq)
	}
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT w.id, w.planned_ts, w.provider, w.model, w.branch_ref, w.branch_generation,
       w.source, w.commit_format, w.forced, w.forced_reason, w.validation_failure,
       w.offered_seqs, w.visible_original_seqs, w.hidden_seqs, w.selected_groups,
       w.deferred_seqs, w.deferred_reasons, w.config_revision_id,
       w.config_profile, w.duration_ms, w.retry_count, w.fallback_used,
       w.outcome, w.experiment_id, w.experiment_consumed
FROM intent_planner_windows w
JOIN intent_planner_window_events e ON e.window_id = w.id
WHERE e.event_seq = ?
ORDER BY w.id DESC
LIMIT 1`, eventSeq)
	if err != nil {
		return IntentPlannerWindow{}, false, fmt.Errorf("state: query intent planner window for event: %w", err)
	}
	defer rows.Close()
	windows, err := scanIntentPlannerWindows(rows)
	if err != nil {
		return IntentPlannerWindow{}, false, err
	}
	if len(windows) == 0 {
		return IntentPlannerWindow{}, false, nil
	}
	events, err := intentPlannerWindowEvents(ctx, d, windows[0].ID)
	if err != nil {
		return IntentPlannerWindow{}, false, err
	}
	windows[0].Events = events
	return windows[0], true, nil
}

func intentPlannerWindowEvents(ctx context.Context, d *DB, windowID int64) ([]IntentPlannerWindowEvent, error) {
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT event_seq, offered, hidden, selected, deferred, group_ord
FROM intent_planner_window_events
WHERE window_id = ?
ORDER BY event_seq ASC`, windowID)
	if err != nil {
		return nil, fmt.Errorf("state: query intent planner window events: %w", err)
	}
	defer rows.Close()
	var out []IntentPlannerWindowEvent
	for rows.Next() {
		var ev IntentPlannerWindowEvent
		var offered, hidden, selected, deferred int
		if err := rows.Scan(&ev.EventSeq, &offered, &hidden, &selected, &deferred, &ev.GroupOrd); err != nil {
			return nil, fmt.Errorf("state: scan intent planner window event: %w", err)
		}
		ev.Offered = offered != 0
		ev.Hidden = hidden != 0
		ev.Selected = selected != 0
		ev.Deferred = deferred != 0
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter intent planner window events: %w", err)
	}
	return out, nil
}

func scanIntentPlannerWindows(rows *sql.Rows) ([]IntentPlannerWindow, error) {
	var out []IntentPlannerWindow
	for rows.Next() {
		var win IntentPlannerWindow
		var forced, fallback, consumed int
		var offered, visible, hidden, groups, deferred, deferredReasons string
		if err := rows.Scan(
			&win.ID, &win.PlannedTS, &win.Provider, &win.Model,
			&win.BranchRef, &win.BranchGeneration, &win.Source,
			&win.CommitFormat, &forced, &win.ForcedReason,
			&win.ValidationFailure, &offered, &visible, &hidden, &groups,
			&deferred, &deferredReasons, &win.ConfigRevisionID,
			&win.ConfigProfile, &win.DurationMS, &win.RetryCount, &fallback,
			&win.Outcome, &win.ExperimentID, &consumed,
		); err != nil {
			return nil, fmt.Errorf("state: scan intent planner window: %w", err)
		}
		win.Forced = forced != 0
		win.FallbackUsed = fallback != 0
		win.ExperimentConsumed = consumed != 0
		if err := unmarshalArray(offered, &win.OfferedSeqs); err != nil {
			return nil, fmt.Errorf("state: unmarshal offered seqs: %w", err)
		}
		if err := unmarshalArray(visible, &win.VisibleOriginalSeqs); err != nil {
			return nil, fmt.Errorf("state: unmarshal visible original seqs: %w", err)
		}
		if err := unmarshalArray(hidden, &win.HiddenSeqs); err != nil {
			return nil, fmt.Errorf("state: unmarshal hidden seqs: %w", err)
		}
		if err := unmarshalArray(groups, &win.SelectedGroups); err != nil {
			return nil, fmt.Errorf("state: unmarshal selected groups: %w", err)
		}
		if err := unmarshalArray(deferred, &win.DeferredSeqs); err != nil {
			return nil, fmt.Errorf("state: unmarshal deferred seqs: %w", err)
		}
		if err := unmarshalArray(deferredReasons, &win.DeferredReasons); err != nil {
			return nil, fmt.Errorf("state: unmarshal deferred reasons: %w", err)
		}
		out = append(out, win)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter intent planner windows: %w", err)
	}
	return out, nil
}

// consumeExperimentWindow increments at most one active experiment in the
// same transaction that appends the completed planner outcome. This makes
// retries idempotent at the durable window boundary: a committed window and
// its progress can never be observed separately.
func consumeExperimentWindow(ctx context.Context, tx *sql.Tx, windowID int64, win IntentPlannerWindow) (bool, error) {
	if !win.ExperimentID.Valid {
		return false, nil
	}
	if !win.ConfigRevisionID.Valid {
		return false, fmt.Errorf("state: experiment planner window requires config revision")
	}
	var exp ConfigExperiment
	err := tx.QueryRowContext(ctx, `
SELECT id, baseline_revision_id, candidate_revision_id, window_budget,
       completed_windows, expires_ts, failure_policy, status, created_ts,
       updated_ts, completed_ts, terminal_reason
FROM config_experiments WHERE id=?`, win.ExperimentID.Int64).Scan(
		&exp.ID, &exp.BaselineRevisionID, &exp.CandidateRevisionID,
		&exp.WindowBudget, &exp.CompletedWindows, &exp.ExpiresTS,
		&exp.FailurePolicy, &exp.Status, &exp.CreatedTS, &exp.UpdatedTS,
		&exp.CompletedTS, &exp.TerminalReason)
	if err != nil {
		return false, fmt.Errorf("state: load planner-window experiment: %w", err)
	}
	if exp.Status != ExperimentActive {
		return false, nil
	}
	if exp.CandidateRevisionID != win.ConfigRevisionID.Int64 {
		return false, fmt.Errorf("state: planner window revision does not match experiment candidate")
	}
	when := win.PlannedTS
	if exp.ExpiresTS.Valid && when >= exp.ExpiresTS.Float64 {
		if _, err := tx.ExecContext(ctx, `
UPDATE config_experiments SET status='expired', updated_ts=?, completed_ts=?,
    terminal_reason='expiry reached before completed window'
WHERE id=? AND status='active'`, when, when, exp.ID); err != nil {
			return false, fmt.Errorf("state: expire config experiment: %w", err)
		}
		return false, nil
	}
	next := exp.CompletedWindows + 1
	status := ExperimentActive
	reason := sql.NullString{}
	outcome := strings.ToLower(strings.TrimSpace(win.Outcome.String))
	if (outcome == "failed" || outcome == "error") && exp.FailurePolicy != "continue" {
		status = ExperimentFailed
		reason = sql.NullString{String: "planner window failure policy", Valid: true}
	} else if next >= exp.WindowBudget {
		status = ExperimentCompleted
		reason = sql.NullString{String: "window budget consumed", Valid: true}
	}
	var completed any
	if status != ExperimentActive {
		completed = when
	}
	res, err := tx.ExecContext(ctx, `
UPDATE config_experiments
SET completed_windows=?, status=?, updated_ts=?, completed_ts=?, terminal_reason=?
WHERE id=? AND status='active' AND completed_windows=?`,
		next, status, when, completed, reason, exp.ID, exp.CompletedWindows)
	if err != nil {
		return false, fmt.Errorf("state: consume experiment planner window %d: %w", windowID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: count consumed experiment window: %w", err)
	}
	return n == 1, nil
}

func marshalArray(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	if string(b) == "null" {
		return "[]", nil
	}
	return string(b), nil
}

func unmarshalArray(raw string, v any) error {
	if raw == "" {
		raw = "[]"
	}
	return json.Unmarshal([]byte(raw), v)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

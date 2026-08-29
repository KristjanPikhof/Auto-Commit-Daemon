package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type intentPlannerWindowSummary struct {
	ID                     int64                                         `json:"id"`
	PlannedTS              int64                                         `json:"planned_ts"`
	Time                   string                                        `json:"time"`
	Provider               string                                        `json:"provider,omitempty"`
	Model                  string                                        `json:"model,omitempty"`
	BranchRef              string                                        `json:"branch_ref,omitempty"`
	BranchGeneration       int64                                         `json:"branch_generation,omitempty"`
	Source                 string                                        `json:"source,omitempty"`
	CommitFormat           string                                        `json:"commit_format,omitempty"`
	Forced                 bool                                          `json:"forced,omitempty"`
	ForcedReason           string                                        `json:"forced_reason,omitempty"`
	ValidationFailure      string                                        `json:"validation_failure,omitempty"`
	OfferedSeqs            []int64                                       `json:"offered_seqs,omitempty"`
	VisibleOriginalSeqs    []int64                                       `json:"visible_original_seqs,omitempty"`
	HiddenSeqs             []int64                                       `json:"hidden_seqs,omitempty"`
	SelectedGroups         []state.IntentPlannerWindowGroup              `json:"selected_groups,omitempty"`
	DeferredSeqs           []int64                                       `json:"deferred_seqs,omitempty"`
	DeferredReasons        []state.IntentPlannerWindowDeferredReason     `json:"deferred_reasons,omitempty"`
	PlanAttempt            int                                           `json:"plan_attempt,omitempty"`
	PlanAttemptLimit       int                                           `json:"plan_attempt_limit,omitempty"`
	UnresolvedCaptureCount int                                           `json:"unresolved_capture_count,omitempty"`
	PreservedGroupCount    int                                           `json:"preserved_group_count,omitempty"`
	ResolutionMode         string                                        `json:"resolution_mode,omitempty"`
	PreflightState         string                                        `json:"preflight_state,omitempty"`
	FindingCodes           []string                                      `json:"finding_codes,omitempty"`
	ProviderCallSkipped    string                                        `json:"provider_call_skipped_reason,omitempty"`
	Event                  *intentPlannerWindowEventParticipationSummary `json:"event,omitempty"`
}

type intentPlannerWindowEventParticipationSummary struct {
	Seq      int64 `json:"seq"`
	Offered  bool  `json:"offered,omitempty"`
	Hidden   bool  `json:"hidden,omitempty"`
	Selected bool  `json:"selected,omitempty"`
	Deferred bool  `json:"deferred,omitempty"`
	GroupOrd int64 `json:"group_ord,omitempty"`
}

func loadLastIntentPlannerWindowSQL(ctx context.Context, conn *sql.DB) (*intentPlannerWindowSummary, error) {
	return loadLastIntentPlannerWindowSQLWithFilter(ctx, conn, "", nil)
}

func loadLastIntentPlannerWindowForPairSQL(
	ctx context.Context,
	conn *sql.DB,
	branchRef string,
	branchGeneration int64,
) (*intentPlannerWindowSummary, error) {
	if branchRef == "" {
		return nil, nil
	}
	return loadLastIntentPlannerWindowSQLWithFilter(
		ctx,
		conn,
		"WHERE w.branch_ref=? AND w.branch_generation=?",
		[]any{branchRef, branchGeneration},
	)
}

func loadLastIntentPlannerWindowSQLWithFilter(
	ctx context.Context,
	conn *sql.DB,
	filter string,
	args []any,
) (*intentPlannerWindowSummary, error) {
	ok, err := sqliteTableExists(ctx, conn, "intent_planner_windows")
	if err != nil {
		return nil, fmt.Errorf("intent planner windows table check: %w", err)
	}
	if !ok {
		return nil, nil
	}
	rows, err := conn.QueryContext(ctx, intentPlannerWindowSelectSQL+`
	FROM intent_planner_windows w
	`+filter+`
	ORDER BY w.id DESC
	LIMIT 1`, args...)
	if err != nil {
		return nil, fmt.Errorf("query last intent planner window: %w", err)
	}
	defer rows.Close()
	windows, err := scanIntentPlannerWindowSummaries(rows)
	if err != nil {
		return nil, err
	}
	if len(windows) == 0 {
		return nil, nil
	}
	if err := enrichIntentPlannerWindowRun(ctx, conn, &windows[0]); err != nil {
		return nil, err
	}
	return &windows[0], nil
}

func enrichIntentPlannerWindowRun(ctx context.Context, conn *sql.DB, win *intentPlannerWindowSummary) error {
	ok, err := sqliteTableExists(ctx, conn, "intent_plan_runs")
	if err != nil || !ok {
		return err
	}
	var resolution, progress sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT attempt_count, attempt_limit, unresolved_seqs, preserved_groups,
       resolution_mode, progress_state, finding_codes
FROM intent_plan_runs
WHERE branch_ref=? AND branch_generation=?
ORDER BY updated_ts DESC LIMIT 1`, win.BranchRef, win.BranchGeneration).Scan(
		&win.PlanAttempt, &win.PlanAttemptLimit, newJSONArrayCount(&win.UnresolvedCaptureCount),
		newJSONArrayCount(&win.PreservedGroupCount), &resolution, &progress,
		newJSONArrayStrings(&win.FindingCodes))
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("query latest intent plan run: %w", err)
	}
	win.ResolutionMode = resolution.String
	win.PreflightState = progress.String
	if progress.String == "preflight_blocked" {
		win.ProviderCallSkipped = "invalid_local_baseline"
	}
	return nil
}

type jsonArrayCount struct{ target *int }

type jsonArrayStrings struct{ target *[]string }

func newJSONArrayCount(target *int) *jsonArrayCount { return &jsonArrayCount{target: target} }

func newJSONArrayStrings(target *[]string) *jsonArrayStrings {
	return &jsonArrayStrings{target: target}
}

func (s *jsonArrayStrings) Scan(src any) error {
	var raw []byte
	switch value := src.(type) {
	case string:
		raw = []byte(value)
	case []byte:
		raw = value
	case nil:
		raw = []byte("[]")
	default:
		return fmt.Errorf("scan JSON array strings from %T", src)
	}
	return json.Unmarshal(raw, s.target)
}

func (c *jsonArrayCount) Scan(src any) error {
	var raw []byte
	switch value := src.(type) {
	case string:
		raw = []byte(value)
	case []byte:
		raw = value
	case nil:
		raw = []byte("[]")
	default:
		return fmt.Errorf("unsupported JSON array value %T", src)
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return err
	}
	*c.target = len(values)
	return nil
}

func loadIntentPlannerWindowForEventSQL(ctx context.Context, conn *sql.DB, eventSeq int64) (*intentPlannerWindowSummary, error) {
	if eventSeq <= 0 {
		return nil, nil
	}
	ok, err := sqliteTableExists(ctx, conn, "intent_planner_window_events")
	if err != nil {
		return nil, fmt.Errorf("intent planner window events table check: %w", err)
	}
	if !ok {
		return nil, nil
	}
	rows, err := conn.QueryContext(ctx, intentPlannerWindowSelectSQL+`,
       e.offered, e.hidden, e.selected, e.deferred, e.group_ord
FROM intent_planner_windows w
JOIN intent_planner_window_events e ON e.window_id = w.id
WHERE e.event_seq = ?
ORDER BY w.id DESC
LIMIT 1`, eventSeq)
	if err != nil {
		return nil, fmt.Errorf("query intent planner window for event: %w", err)
	}
	defer rows.Close()
	windows, err := scanIntentPlannerWindowSummariesWithEvent(rows, eventSeq)
	if err != nil {
		return nil, err
	}
	if len(windows) == 0 {
		return nil, nil
	}
	if err := enrichIntentPlannerWindowRun(ctx, conn, &windows[0]); err != nil {
		return nil, err
	}
	return &windows[0], nil
}

const intentPlannerWindowSelectSQL = `
SELECT w.id, w.planned_ts, w.provider, w.model, w.branch_ref, w.branch_generation,
       w.source, w.commit_format, w.forced, w.forced_reason, w.validation_failure,
       w.offered_seqs, w.visible_original_seqs, w.hidden_seqs, w.selected_groups,
       w.deferred_seqs, w.deferred_reasons`

func scanIntentPlannerWindowSummaries(rows *sql.Rows) ([]intentPlannerWindowSummary, error) {
	var out []intentPlannerWindowSummary
	for rows.Next() {
		win, err := scanIntentPlannerWindowSummary(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, win)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter intent planner windows: %w", err)
	}
	return out, nil
}

func scanIntentPlannerWindowSummariesWithEvent(rows *sql.Rows, eventSeq int64) ([]intentPlannerWindowSummary, error) {
	var out []intentPlannerWindowSummary
	for rows.Next() {
		var offered, hidden, selected, deferred int
		var groupOrd sql.NullInt64
		win, err := scanIntentPlannerWindowSummary(func(dest ...any) error {
			return rows.Scan(append(dest, &offered, &hidden, &selected, &deferred, &groupOrd)...)
		})
		if err != nil {
			return nil, err
		}
		win.Event = &intentPlannerWindowEventParticipationSummary{
			Seq:      eventSeq,
			Offered:  offered != 0,
			Hidden:   hidden != 0,
			Selected: selected != 0,
			Deferred: deferred != 0,
		}
		if groupOrd.Valid {
			win.Event.GroupOrd = groupOrd.Int64
		}
		out = append(out, win)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter intent planner window events: %w", err)
	}
	return out, nil
}

type scannerFunc func(dest ...any) error

func scanIntentPlannerWindowSummary(scan scannerFunc) (intentPlannerWindowSummary, error) {
	var win intentPlannerWindowSummary
	var provider, model, source, commitFormat, forcedReason, validationFailure sql.NullString
	var plannedTS float64
	var forced int
	var offered, visible, hidden, groups, deferred, deferredReasons string
	if err := scan(
		&win.ID, &plannedTS, &provider, &model, &win.BranchRef, &win.BranchGeneration,
		&source, &commitFormat, &forced, &forcedReason, &validationFailure,
		&offered, &visible, &hidden, &groups, &deferred, &deferredReasons,
	); err != nil {
		return intentPlannerWindowSummary{}, fmt.Errorf("scan intent planner window: %w", err)
	}
	win.PlannedTS = int64(plannedTS)
	win.Time = time.Unix(int64(plannedTS), 0).Format(time.RFC3339)
	win.Provider = provider.String
	win.Model = model.String
	win.Source = source.String
	win.CommitFormat = commitFormat.String
	win.Forced = forced != 0
	win.ForcedReason = forcedReason.String
	win.ValidationFailure = ai.SanitizePlannerError(validationFailure.String)
	if err := json.Unmarshal([]byte(emptyJSONArray(offered)), &win.OfferedSeqs); err != nil {
		return intentPlannerWindowSummary{}, fmt.Errorf("unmarshal offered seqs: %w", err)
	}
	if err := json.Unmarshal([]byte(emptyJSONArray(visible)), &win.VisibleOriginalSeqs); err != nil {
		return intentPlannerWindowSummary{}, fmt.Errorf("unmarshal visible original seqs: %w", err)
	}
	if err := json.Unmarshal([]byte(emptyJSONArray(hidden)), &win.HiddenSeqs); err != nil {
		return intentPlannerWindowSummary{}, fmt.Errorf("unmarshal hidden seqs: %w", err)
	}
	if err := json.Unmarshal([]byte(emptyJSONArray(groups)), &win.SelectedGroups); err != nil {
		return intentPlannerWindowSummary{}, fmt.Errorf("unmarshal selected groups: %w", err)
	}
	if err := json.Unmarshal([]byte(emptyJSONArray(deferred)), &win.DeferredSeqs); err != nil {
		return intentPlannerWindowSummary{}, fmt.Errorf("unmarshal deferred seqs: %w", err)
	}
	if err := json.Unmarshal([]byte(emptyJSONArray(deferredReasons)), &win.DeferredReasons); err != nil {
		return intentPlannerWindowSummary{}, fmt.Errorf("unmarshal deferred reasons: %w", err)
	}
	return win, nil
}

func emptyJSONArray(raw string) string {
	if raw == "" || raw == "null" {
		return "[]"
	}
	return raw
}

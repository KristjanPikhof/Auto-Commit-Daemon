package cli

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type intentStrategyReport struct {
	Strategy                 string `json:"strategy"`
	Active                   bool   `json:"active"`
	Window                   int    `json:"window,omitempty"`
	RecentCommits            int    `json:"recent_commits,omitempty"`
	DeferLimit               int    `json:"defer_limit,omitempty"`
	DeferredEvents           int    `json:"deferred_events,omitempty"`
	MaxDeferCount            int    `json:"max_defer_count,omitempty"`
	ForcedAgingReady         int    `json:"forced_aging_ready,omitempty"`
	LastDeferredEventSeq     int64  `json:"last_deferred_event_seq,omitempty"`
	LastDeferredPath         string `json:"last_deferred_path,omitempty"`
	LastDeferredReason       string `json:"last_deferred_reason,omitempty"`
	LastPlannerErrorEventSeq int64  `json:"last_planner_error_event_seq,omitempty"`
	LastPlannerErrorPath     string `json:"last_planner_error_path,omitempty"`
	LastPlannerError         string `json:"last_planner_error,omitempty"`
}

func renderIntentStrategyHuman(out io.Writer, r intentStrategyReport) {
	status := "event"
	if r.Strategy != "" {
		status = r.Strategy
	}
	if r.Active {
		fmt.Fprintf(out, "Commit strategy: %s (window %d, recent commits %d, defer limit %d)\n",
			status, r.Window, r.RecentCommits, r.DeferLimit)
	} else {
		fmt.Fprintf(out, "Commit strategy: %s\n", status)
	}
	if r.DeferredEvents > 0 || r.ForcedAgingReady > 0 || r.LastPlannerError != "" {
		fmt.Fprintf(out, "Intent planner: deferred=%d max_defer=%d forced_ready=%d\n",
			r.DeferredEvents, r.MaxDeferCount, r.ForcedAgingReady)
		if r.LastDeferredReason != "" {
			fmt.Fprintf(out, "  Last defer: seq %d %s (%s)\n",
				r.LastDeferredEventSeq, valueOrUnset(r.LastDeferredPath), r.LastDeferredReason)
		}
		if r.LastPlannerError != "" {
			fmt.Fprintf(out, "  Last planner error: seq %d %s (%s)\n",
				r.LastPlannerErrorEventSeq, valueOrUnset(r.LastPlannerErrorPath), r.LastPlannerError)
		}
	}
}

func intentStrategyFromEnv() intentStrategyReport {
	cfg := ai.LoadProviderConfigFromEnv()
	return intentStrategyReport{
		Strategy:      string(cfg.CommitStrategy),
		Active:        cfg.CommitStrategy == ai.CommitStrategyIntent,
		Window:        cfg.IntentWindow,
		RecentCommits: cfg.IntentRecentCommits,
		DeferLimit:    cfg.IntentDeferLimit,
	}
}

func loadIntentStrategyReport(ctx context.Context, conn *sql.DB) (intentStrategyReport, error) {
	report := intentStrategyFromEnv()
	if conn == nil {
		return report, nil
	}
	ok, err := sqliteTableExists(ctx, conn, "planner_state")
	if err != nil {
		return report, fmt.Errorf("planner_state table check: %w", err)
	}
	if !ok {
		return report, nil
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(MAX(ps.defer_count), 0)
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.defer_count > 0`, state.EventStatePending).Scan(&report.DeferredEvents, &report.MaxDeferCount); err != nil {
		return report, fmt.Errorf("planner deferred summary: %w", err)
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.defer_count >= ?`, state.EventStatePending, report.DeferLimit).Scan(&report.ForcedAgingReady); err != nil {
		return report, fmt.Errorf("planner forced-aging summary: %w", err)
	}
	var lastDeferredSeq sql.NullInt64
	var lastDeferredPath, lastDeferredReason sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT ps.event_seq, e.path, ps.last_defer_reason
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.last_defer_reason IS NOT NULL AND ps.last_defer_reason != ''
ORDER BY ps.last_planned_ts DESC, ps.event_seq DESC
LIMIT 1`, state.EventStatePending).Scan(&lastDeferredSeq, &lastDeferredPath, &lastDeferredReason)
	if err != nil && err != sql.ErrNoRows {
		return report, fmt.Errorf("planner last defer: %w", err)
	}
	if lastDeferredSeq.Valid {
		report.LastDeferredEventSeq = lastDeferredSeq.Int64
	}
	if lastDeferredPath.Valid {
		report.LastDeferredPath = lastDeferredPath.String
	}
	if lastDeferredReason.Valid {
		report.LastDeferredReason = lastDeferredReason.String
	}

	var lastErrorSeq sql.NullInt64
	var lastErrorPath, lastError sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT ps.event_seq, e.path, ps.last_plan_error
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.last_plan_error IS NOT NULL AND ps.last_plan_error != ''
ORDER BY ps.last_planned_ts DESC, ps.event_seq DESC
LIMIT 1`, state.EventStatePending).Scan(&lastErrorSeq, &lastErrorPath, &lastError)
	if err != nil && err != sql.ErrNoRows {
		return report, fmt.Errorf("planner last error: %w", err)
	}
	if lastErrorSeq.Valid {
		report.LastPlannerErrorEventSeq = lastErrorSeq.Int64
	}
	if lastErrorPath.Valid {
		report.LastPlannerErrorPath = lastErrorPath.String
	}
	if lastError.Valid {
		report.LastPlannerError = lastError.String
	}
	return report, nil
}

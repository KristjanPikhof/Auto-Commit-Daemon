package cli

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type intentStrategyReport struct {
	Strategy                 string `json:"strategy"`
	Active                   bool   `json:"active"`
	Window                   int    `json:"window,omitempty"`
	RecentCommits            int    `json:"recent_commits,omitempty"`
	DeferLimit               int    `json:"defer_limit,omitempty"`
	MinPending               int    `json:"min_pending,omitempty"`
	MaxPendingAgeSeconds     int64  `json:"max_pending_age_seconds,omitempty"`
	VisiblePendingEvents     int    `json:"visible_pending_events,omitempty"`
	OldestPendingEventSeq    int64  `json:"oldest_pending_event_seq,omitempty"`
	OldestPendingPath        string `json:"oldest_pending_path,omitempty"`
	OldestPendingAgeSeconds  int64  `json:"oldest_pending_age_seconds,omitempty"`
	AgeTriggerTS             int64  `json:"age_trigger_ts,omitempty"`
	AgeTriggerInSeconds      int64  `json:"age_trigger_in_seconds,omitempty"`
	BatchWaitActive          bool   `json:"batch_wait_active,omitempty"`
	BatchWaitReason          string `json:"batch_wait_reason,omitempty"`
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
		fmt.Fprintf(out, "Commit strategy: %s (window %d, min pending %d, max age %s, recent commits %d, defer limit %d)\n",
			status, r.Window, r.MinPending, formatDurationCompact(time.Duration(r.MaxPendingAgeSeconds)*time.Second), r.RecentCommits, r.DeferLimit)
	} else {
		fmt.Fprintf(out, "Commit strategy: %s\n", status)
	}
	if r.BatchWaitActive {
		fmt.Fprintf(out, "Intent batch wait: pending=%d min_pending=%d oldest_age=%s max_age=%s trigger_in=%s\n",
			r.VisiblePendingEvents,
			r.MinPending,
			formatDurationCompact(time.Duration(r.OldestPendingAgeSeconds)*time.Second),
			formatDurationCompact(time.Duration(r.MaxPendingAgeSeconds)*time.Second),
			formatDurationCompact(time.Duration(r.AgeTriggerInSeconds)*time.Second))
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

// ResolveEffectiveCommitStrategy returns the commit strategy currently in
// effect for a repo. When conn is nil, the result reflects only env
// (ACD_COMMIT_STRATEGY) and the canonical default. When conn is non-nil and
// daemon_meta carries a daemon-stamped commit.strategy, that overlay wins so
// CLI tooling reports the value the running daemon actually uses.
func ResolveEffectiveCommitStrategy(ctx context.Context, conn *sql.DB) (ai.CommitStrategy, error) {
	cfg := ai.LoadProviderConfigFromEnv()
	strategy := cfg.CommitStrategy
	if conn == nil {
		return strategy, nil
	}
	raw, ok, err := metaLookup(ctx, conn, "commit.strategy")
	if err != nil {
		return strategy, fmt.Errorf("commit.strategy: %w", err)
	}
	if !ok {
		return strategy, nil
	}
	trimmed := strings.TrimSpace(strings.ToLower(raw))
	switch trimmed {
	case "":
		return strategy, nil
	case string(ai.CommitStrategyEvent):
		return ai.CommitStrategyEvent, nil
	case string(ai.CommitStrategyIntent):
		return ai.CommitStrategyIntent, nil
	default:
		return strategy, nil
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
		MinPending:    cfg.IntentMinPending,
		MaxPendingAgeSeconds: int64(
			cfg.IntentMaxPendingAge / time.Second,
		),
	}
}

func loadIntentStrategyReport(ctx context.Context, conn *sql.DB) (intentStrategyReport, error) {
	report := intentStrategyFromEnv()
	if conn == nil {
		return report, nil
	}
	strategy, err := ResolveEffectiveCommitStrategy(ctx, conn)
	if err != nil {
		return report, err
	}
	report.Strategy = string(strategy)
	report.Active = strategy == ai.CommitStrategyIntent
	if v, ok, err := metaLookup(ctx, conn, "intent.window"); err != nil {
		return report, fmt.Errorf("intent.window: %w", err)
	} else if ok {
		report.Window = parseIntentMetaInt(v, report.Window)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.recent_commits"); err != nil {
		return report, fmt.Errorf("intent.recent_commits: %w", err)
	} else if ok {
		report.RecentCommits = parseIntentMetaInt(v, report.RecentCommits)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.defer_limit"); err != nil {
		return report, fmt.Errorf("intent.defer_limit: %w", err)
	} else if ok {
		report.DeferLimit = parseIntentMetaInt(v, report.DeferLimit)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.min_pending"); err != nil {
		return report, fmt.Errorf("intent.min_pending: %w", err)
	} else if ok {
		report.MinPending = parseIntentMetaInt(v, report.MinPending)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.max_pending_age"); err != nil {
		return report, fmt.Errorf("intent.max_pending_age: %w", err)
	} else if ok {
		report.MaxPendingAgeSeconds = parseIntentMetaDurationSeconds(v, report.MaxPendingAgeSeconds)
	}
	if err := loadLastIntentPlannerError(ctx, conn, &report); err != nil {
		return report, err
	}
	ok, err := sqliteTableExists(ctx, conn, "planner_state")
	if err != nil {
		return report, fmt.Errorf("planner_state table check: %w", err)
	}
	if !ok {
		if err := loadIntentBatchWait(ctx, conn, &report); err != nil {
			return report, err
		}
		return report, nil
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(MAX(ps.defer_count), 0)
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.defer_count > 0
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events barrier
      WHERE barrier.branch_ref = e.branch_ref
        AND barrier.branch_generation = e.branch_generation
        AND barrier.seq < e.seq
        AND barrier.state IN (?, ?)
  )`, state.EventStatePending, state.EventStateFailed, state.EventStateBlockedConflict).Scan(&report.DeferredEvents, &report.MaxDeferCount); err != nil {
		return report, fmt.Errorf("planner deferred summary: %w", err)
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.defer_count >= ?
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events barrier
      WHERE barrier.branch_ref = e.branch_ref
        AND barrier.branch_generation = e.branch_generation
        AND barrier.seq < e.seq
        AND barrier.state IN (?, ?)
  )`, state.EventStatePending, report.DeferLimit, state.EventStateFailed, state.EventStateBlockedConflict).Scan(&report.ForcedAgingReady); err != nil {
		return report, fmt.Errorf("planner forced-aging summary: %w", err)
	}
	var lastDeferredSeq sql.NullInt64
	var lastDeferredPath, lastDeferredReason sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT ps.event_seq, e.path, ps.last_defer_reason
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.last_defer_reason IS NOT NULL AND ps.last_defer_reason != ''
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events barrier
      WHERE barrier.branch_ref = e.branch_ref
        AND barrier.branch_generation = e.branch_generation
        AND barrier.seq < e.seq
        AND barrier.state IN (?, ?)
  )
ORDER BY ps.last_planned_ts DESC, ps.event_seq DESC
LIMIT 1`, state.EventStatePending, state.EventStateFailed, state.EventStateBlockedConflict).Scan(&lastDeferredSeq, &lastDeferredPath, &lastDeferredReason)
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
	if err := loadIntentBatchWait(ctx, conn, &report); err != nil {
		return report, err
	}

	return report, nil
}

func loadLastIntentPlannerError(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	ok, err := sqliteTableExists(ctx, conn, "decision_records")
	if err != nil {
		return fmt.Errorf("decision table check: %w", err)
	}
	if !ok {
		return nil
	}
	var lastErrorSeq sql.NullInt64
	var lastErrorPath, lastError sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT event_seq, path, COALESCE(NULLIF(reason, ''), NULLIF(user_message, ''), NULLIF(action_taken, ''))
FROM decision_records
WHERE kind = ?
ORDER BY id DESC
LIMIT 1`, state.DecisionKindIntentPlannerError).Scan(&lastErrorSeq, &lastErrorPath, &lastError)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("planner last error: %w", err)
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
	return nil
}

func loadIntentBatchWait(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	ok, err := sqliteTableExists(ctx, conn, "capture_events")
	if err != nil {
		return fmt.Errorf("capture_events table check: %w", err)
	}
	if !ok {
		return nil
	}
	var oldestSeq sql.NullInt64
	var oldestPath sql.NullString
	var oldestCaptured sql.NullFloat64
	if err := conn.QueryRowContext(ctx, `
WITH barriers AS (
    SELECT branch_ref, branch_generation, MIN(seq) AS first_seq
    FROM capture_events
    WHERE state IN (?, ?)
    GROUP BY branch_ref, branch_generation
), visible_pending AS (
    SELECT e.seq, e.path, e.captured_ts
    FROM capture_events e
    LEFT JOIN barriers b
           ON b.branch_ref = e.branch_ref
          AND b.branch_generation = e.branch_generation
    WHERE e.state = ?
      AND (b.first_seq IS NULL OR e.seq < b.first_seq)
)
SELECT COUNT(*), MIN(seq), (
    SELECT path FROM visible_pending ORDER BY seq ASC LIMIT 1
), (
    SELECT captured_ts FROM visible_pending ORDER BY seq ASC LIMIT 1
)
FROM visible_pending`, state.EventStateBlockedConflict, state.EventStateFailed, state.EventStatePending).Scan(
		&report.VisiblePendingEvents,
		&oldestSeq,
		&oldestPath,
		&oldestCaptured,
	); err != nil {
		return fmt.Errorf("intent batch wait summary: %w", err)
	}
	if oldestSeq.Valid {
		report.OldestPendingEventSeq = oldestSeq.Int64
	}
	if oldestPath.Valid {
		report.OldestPendingPath = oldestPath.String
	}
	if !oldestCaptured.Valid || report.VisiblePendingEvents == 0 || report.MaxPendingAgeSeconds <= 0 {
		return nil
	}
	nowSec := float64(time.Now().UnixNano()) / 1e9
	ageSeconds := int64(nowSec - oldestCaptured.Float64)
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	report.OldestPendingAgeSeconds = ageSeconds
	report.AgeTriggerTS = int64(oldestCaptured.Float64) + report.MaxPendingAgeSeconds
	if remaining := report.AgeTriggerTS - int64(nowSec); remaining > 0 {
		report.AgeTriggerInSeconds = remaining
	}
	if report.Active &&
		report.ForcedAgingReady == 0 &&
		report.VisiblePendingEvents < report.MinPending &&
		report.OldestPendingAgeSeconds < report.MaxPendingAgeSeconds {
		report.BatchWaitActive = true
		report.BatchWaitReason = "skipped_due_intent_batch_wait"
	}
	return nil
}

func parseIntentMetaInt(raw string, fallback int) int {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func parseIntentMetaDurationSeconds(raw string, fallback int64) int64 {
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return int64(d / time.Second)
}

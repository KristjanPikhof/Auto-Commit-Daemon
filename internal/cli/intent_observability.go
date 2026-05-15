package cli

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
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
	IntentStageDiffCap       int    `json:"intent_stage_diff_cap,omitempty"`
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
	// PlannerErrorRateRecent is the share of intent_planner_error rows in
	// the most recent IntentRecentDecisionWindow decisions. The denominator
	// is always IntentRecentDecisionWindow (default 100) regardless of how
	// many decisions have actually been recorded — the rate moves smoothly
	// as the ledger fills rather than oscillating wildly during the first
	// few decisions.
	//
	// JSON encoding: the field uses `,omitempty` so a zero rate is
	// absent from the payload. Operators consuming this metric should
	// treat absent and 0.0 identically — both mean "no planner errors
	// observed in the most recent window". The decision_records table
	// existence check upstream (see loadIntentRecentRates) already gates
	// the field off when no decisions have ever been recorded, so the
	// "never observed" vs "observed, exactly zero" distinction is not
	// meaningful from the JSON shape and operators should not try to
	// infer it.
	PlannerErrorRateRecent float64 `json:"planner_error_rate_recent,omitempty"`
	// SingletonCommitRateRecent is the share of one-event commits in the
	// most recent IntentRecentCommitWindow distinct commit OIDs. The
	// denominator follows the same "fixed 100 even when not yet filled"
	// policy as PlannerErrorRateRecent.
	SingletonCommitRateRecent float64 `json:"singleton_commit_rate_recent,omitempty"`
	// PlannerErrorRateRecentWarn surfaces the intent_strategy threshold
	// breach to operators in the human renderer. Set to true whenever
	// PlannerErrorRateRecent exceeds IntentPlannerErrorRateWarnThreshold
	// (default 0.05) so the diagnose remediation hint and the status human
	// output stay in sync without re-deriving the threshold separately.
	PlannerErrorRateRecentWarn bool `json:"planner_error_rate_recent_warn,omitempty"`
	// PathQuiescenceGatedEvents records the most recent count of pending
	// capture events held back by the per-path quiescence gate (see
	// ACD_PATH_QUIESCENCE_SECONDS in CLAUDE.md). The daemon stamps this
	// value once per replay pass to daemon_meta; status reads it
	// best-effort and adjusts VisiblePendingEvents downward so the
	// reported count reflects the planner-visible window. Absent when
	// the daemon has never recorded a snapshot.
	PathQuiescenceGatedEvents int `json:"path_quiescence_gated_events,omitempty"`
}

// IntentRecentDecisionWindow is the fixed denominator for
// PlannerErrorRateRecent — the number of most-recent decision_records rows
// considered when computing the planner-error share. The value is fixed at
// 100 so the metric is comparable across repos and over time; raising it
// would smooth the rate further at the cost of taking longer to react to
// new planner regressions.
const IntentRecentDecisionWindow = 100

// IntentRecentCommitWindow is the fixed denominator for
// SingletonCommitRateRecent — the number of most-recent unique commit OIDs
// considered when computing the singleton (one-event) commit share. Mirrors
// IntentRecentDecisionWindow.
const IntentRecentCommitWindow = 100

// IntentPlannerErrorRateWarnThreshold is the planner-error rate above which
// the diagnose remediation surfaces a warning. 0.05 (5%) reflects the
// observed noise floor of healthy planner deployments under the Wave 2
// retry+normalize stack; sustained rates above this are an operator signal
// to inspect <gitDir>/acd/planner-rejects.jsonl.
//
// Warn gating: PlannerErrorRateRecentWarn is only set when the
// decision_records table holds at least IntentRecentDecisionWindow
// rows. Below the window a fresh ledger can trip the threshold simply
// because the dilution denominator and the row count match (5 errors
// out of 5 decisions = 0.05 = threshold), which is a noise signal, not
// an operator-actionable regression.
const IntentPlannerErrorRateWarnThreshold = 0.05

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
	if r.PlannerErrorRateRecent > 0 || r.SingletonCommitRateRecent > 0 {
		warn := ""
		if r.PlannerErrorRateRecentWarn {
			warn = " WARN above " + formatRate(IntentPlannerErrorRateWarnThreshold)
		}
		fmt.Fprintf(out, "Intent rates (last %d): planner_error=%s singleton_commit=%s%s\n",
			IntentRecentDecisionWindow,
			formatRate(r.PlannerErrorRateRecent),
			formatRate(r.SingletonCommitRateRecent),
			warn,
		)
	}
}

// formatRate renders a rate in a stable two-decimal form. Avoids the
// language-dependent default formatting of fmt.Sprintf("%v", float64) so
// the human renderer is locale-stable.
func formatRate(r float64) string {
	return strconv.FormatFloat(r, 'f', 3, 64)
}

// ResolveEffectiveCommitStrategy returns the commit strategy currently in
// effect for a repo. When conn is nil, the result reflects only env
// (ACD_COMMIT_STRATEGY) and the canonical default. When daemon_meta
// carries a *recognized* commit.strategy value, that overlay wins;
// unrecognized values are loud (slog.Warn) and the env-derived value is
// used so corrupt meta cannot silently override the operator's intent.
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
		// Daemon meta carries a value but it is not one of the known
		// commit strategies. Silently demoting to the env-derived
		// value would hide daemon misconfiguration; log a warning and
		// preserve the existing fallback so callers don't crash.
		slog.Default().Warn(
			"daemon meta commit.strategy has unrecognized value; falling back to env-derived strategy",
			slog.String("commit.strategy", raw),
			slog.String("fallback", string(strategy)),
		)
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
		IntentStageDiffCap: ai.IntentStageDiffCap,
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
	if err := loadIntentRecentRates(ctx, conn, &report); err != nil {
		return report, err
	}

	return report, nil
}

// loadIntentRecentRates populates PlannerErrorRateRecent and
// SingletonCommitRateRecent on report. Reads are best-effort: a missing
// decision_records table (fresh repo, never committed) leaves both fields at
// their zero value rather than aborting the report.
//
// Denominator policy: both rates use a fixed denominator
// (IntentRecentDecisionWindow / IntentRecentCommitWindow). When the ledger
// holds fewer rows than the window, the rate dilutes toward zero — this
// keeps the metric stable and comparable across repos at the cost of
// understating short-term spikes during the first 100 decisions.
func loadIntentRecentRates(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	ok, err := sqliteTableExists(ctx, conn, "decision_records")
	if err != nil {
		return fmt.Errorf("intent rate decision table check: %w", err)
	}
	if !ok {
		return nil
	}
	if err := loadIntentPlannerErrorRate(ctx, conn, report); err != nil {
		return err
	}
	if err := loadIntentSingletonCommitRate(ctx, conn, report); err != nil {
		return err
	}
	if report.PlannerErrorRateRecent > IntentPlannerErrorRateWarnThreshold {
		report.PlannerErrorRateRecentWarn = true
	}
	return nil
}

// loadIntentPlannerErrorRate counts intent_planner_error rows in the most
// recent IntentRecentDecisionWindow decisions. Uses a window-bounded
// subquery so the planner-error count never re-scans the full ledger.
func loadIntentPlannerErrorRate(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	const q = `
SELECT COUNT(*)
FROM (
    SELECT id, kind
    FROM decision_records
    ORDER BY id DESC
    LIMIT ?
) recent
WHERE recent.kind = ?`
	var errs int
	if err := conn.QueryRowContext(ctx, q, IntentRecentDecisionWindow, state.DecisionKindIntentPlannerError).Scan(&errs); err != nil {
		return fmt.Errorf("planner error rate: %w", err)
	}
	report.PlannerErrorRateRecent = float64(errs) / float64(IntentRecentDecisionWindow)
	return nil
}

// loadIntentSingletonCommitRate counts singleton commits among the most
// recent IntentRecentCommitWindow distinct commit OIDs. A singleton commit
// is defined as a committed-decision commit_oid that maps to exactly one
// committed decision row — i.e. exactly one capture event landed in that
// commit.
//
// The query first windows the commit OID list to the recent IntentRecentCommitWindow,
// then GROUP BYs to count rows per OID and counts how many groups have
// exactly one row. The denominator is the fixed IntentRecentCommitWindow
// (not the actual count of recent commits) so the rate dilutes toward zero
// while the ledger fills, mirroring the planner-error rate policy.
func loadIntentSingletonCommitRate(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	const q = `
SELECT COUNT(*)
FROM (
    SELECT commit_oid
    FROM decision_records
    WHERE commit_oid IS NOT NULL
      AND commit_oid != ''
      AND kind = ?
      AND commit_oid IN (
          SELECT commit_oid
          FROM decision_records
          WHERE commit_oid IS NOT NULL
            AND commit_oid != ''
            AND kind = ?
          GROUP BY commit_oid
          ORDER BY MAX(id) DESC
          LIMIT ?
      )
    GROUP BY commit_oid
    HAVING COUNT(*) = 1
)`
	var singletons int
	if err := conn.QueryRowContext(ctx, q,
		state.DecisionKindCommitted,
		state.DecisionKindCommitted,
		IntentRecentCommitWindow,
	).Scan(&singletons); err != nil {
		return fmt.Errorf("singleton commit rate: %w", err)
	}
	report.SingletonCommitRateRecent = float64(singletons) / float64(IntentRecentCommitWindow)
	return nil
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
	// Path-quiescence aware reporting: when the daemon stamped a recent
	// gated-count snapshot we subtract it from VisiblePendingEvents so the
	// number reflects the planner-visible window, not the durable FIFO
	// depth. OldestPendingAgeSeconds is intentionally NOT adjusted —
	// quiescence does not change the persistence timestamp on the oldest
	// row, only when the planner is offered the captures behind it.
	if v, ok, err := metaLookup(ctx, conn, "path_quiescence.gated_count"); err == nil && ok {
		if gated, perr := strconv.Atoi(strings.TrimSpace(v)); perr == nil && gated > 0 {
			report.PathQuiescenceGatedEvents = gated
			adjusted := report.VisiblePendingEvents - gated
			if adjusted < 0 {
				adjusted = 0
			}
			report.VisiblePendingEvents = adjusted
		}
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

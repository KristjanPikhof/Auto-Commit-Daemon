package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"

	_ "modernc.org/sqlite"
)

// statusClient is one row of the §7.6 client list, JSON-friendly.
type statusClient struct {
	SessionID    string `json:"session_id"`
	Harness      string `json:"harness"`
	WatchPID     int64  `json:"watch_pid,omitempty"`
	LastSeenTS   int64  `json:"last_seen_ts"`
	LastSeenAgeS int64  `json:"last_seen_age_seconds"`
	TTLRemaining int64  `json:"ttl_remaining_seconds"`
}

// statusReport is the JSON shape for `acd status --json`. Mirrors the
// human-readable layout 1:1 so users can flip flags without losing fields.
type statusReport struct {
	Repo                          string                    `json:"repo"`
	RepoHash                      string                    `json:"repo_hash"`
	Daemon                        string                    `json:"daemon"`
	Stale                         bool                      `json:"stale"`
	PID                           int                       `json:"pid"`
	StartedTS                     int64                     `json:"started_ts,omitempty"`
	UptimeSeconds                 int64                     `json:"uptime_seconds,omitempty"`
	HeartbeatTS                   int64                     `json:"heartbeat_ts,omitempty"`
	HeartbeatAgeSeconds           int64                     `json:"heartbeat_age_seconds,omitempty"`
	BranchRef                     string                    `json:"branch_ref,omitempty"`
	BranchGenToken                string                    `json:"branch_generation_token,omitempty"`
	Clients                       []statusClient            `json:"clients"`
	PendingEvents                 int                       `json:"pending_events"`
	BlockedConflicts              int                       `json:"blocked_conflicts"`
	ActiveBarriers                int                       `json:"active_barriers,omitempty"`
	ActiveTerminalEvents          int                       `json:"active_terminal_events,omitempty"`
	FailedEvents                  int                       `json:"failed_events"`
	FailedBlockingPending         int                       `json:"failed_blocking_pending"`
	LastCommitOID                 string                    `json:"last_commit_oid,omitempty"`
	LastCommitTS                  int64                     `json:"last_commit_ts,omitempty"`
	LastCommitMessage             string                    `json:"last_commit_message,omitempty"`
	CaptureErrors                 int                       `json:"capture_errors"`
	Paused                        bool                      `json:"paused,omitempty"`
	Pause                         *pauseInfo                `json:"pause,omitempty"`
	BackpressurePaused            bool                      `json:"backpressure_paused,omitempty"`
	BackpressurePausedAt          string                    `json:"backpressure_paused_at,omitempty"`
	EventsDroppedTotal            int64                     `json:"events_dropped_total,omitempty"`
	DecisionCounts                map[string]int            `json:"decision_counts,omitempty"`
	RecentDecisions               []eventEntry              `json:"recent_decisions,omitempty"`
	DecisionCursor                int64                     `json:"decision_cursor,omitempty"`
	IntentStrategy                intentStrategyReport      `json:"intent_strategy"`
	RuntimeConfig                 runtimeConfigReport       `json:"runtime_config"`
	Configuration                 configReadinessReport     `json:"configuration"`
	Replay                        replayObservabilityReport `json:"replay"`
	IntentV2                      intentV2Report            `json:"intent_v2"`
	SelfPublication               selfPublicationReport     `json:"self_publication"`
	CheckpointProtectionAvailable bool                      `json:"checkpoint_protection_available"`
	Protected                     bool                      `json:"protected"`
	ObservationEpoch              int64                     `json:"observation_epoch,omitempty"`
	CoveredEpoch                  int64                     `json:"covered_epoch,omitempty"`
	LatestCheckpointID            string                    `json:"latest_checkpoint_id,omitempty"`
	UnpublishedCheckpoints        int                       `json:"unpublished_checkpoints,omitempty"`
	CheckpointRetentionOverBudget bool                      `json:"checkpoint_retention_over_budget,omitempty"`
	FullPollTS                    float64                   `json:"full_poll_ts,omitempty"`
	WatcherQueueDepth             int                       `json:"watcher_queue_depth,omitempty"`
}

func newStatusCmd() *cobra.Command {
	var watch bool
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print current daemon + clients for one repo (default: cwd)",
		Long: `Print daemon, client, queue, blocked-vs-waiting recovery state, pause, branch, and recent decision state for one registered repo.

The default repo is the current working directory. Use --watch to refresh the
same repo until interrupted. Use --json for automation. For all registered
repos, use acd list; for why/how questions, use acd explain and acd events.`,
		Example: `  acd status
  acd status --watch
  acd status --repo /path/to/repo
  acd status --json
  acd explain --path internal/state/schema.go
  acd diagnose --repo . --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			if watch {
				if jsonOut {
					return fmt.Errorf("acd status: --watch does not support --json")
				}
				ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
				defer stop()
				return runStatusWatch(ctx, cmd.OutOrStdout(), repo, interval)
			}
			return runStatus(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "Refresh status output until interrupted")
	cmd.Flags().DurationVar(&interval, "interval", defaultListWatchInterval, "Refresh interval for --watch (Go duration)")
	return cmd
}

func runStatus(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rec, _, _, err := lookupRegisteredRepo("status", repo)
	if err != nil {
		return err
	}

	report, err := buildStatusReport(ctx, rec, time.Now())
	if err != nil {
		return fmt.Errorf("acd status: %w", err)
	}

	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	return renderStatusHuman(out, report)
}

// findRepo returns the registry record whose Path matches abs. If expected
// state DB paths are supplied, legacy rows whose Path is a subdirectory but
// StateDB points at the canonical repo DB are treated as the same repo without
// mutating the registry.
func findRepo(reg *central.Registry, abs string, expectedStateDBs ...string) (central.RepoRecord, bool) {
	for _, r := range reg.Repos {
		if central.SameRepoPath(r.Path, abs) {
			return r, true
		}
		if matchesStateDB(r.StateDB, expectedStateDBs) {
			r.Path = abs
			return r, true
		}
	}
	return central.RepoRecord{}, false
}

func matchesStateDB(actual string, expected []string) bool {
	if actual == "" {
		return false
	}
	for _, want := range expected {
		if want != "" && central.SameRepoPath(actual, want) {
			return true
		}
	}
	return false
}

// buildStatusReport opens the per-repo state.db read-only and projects the
// daemon_state + daemon_clients + last commit + meta rows into a flat
// report struct. Never mutates state.
func buildStatusReport(ctx context.Context, rec central.RepoRecord, now time.Time) (statusReport, error) {
	report := statusReport{
		Repo:     rec.Path,
		RepoHash: rec.RepoHash,
		Daemon:   "stopped",
		Clients:  []statusClient{},
	}
	if !fileExists(rec.StateDB) {
		return report, fmt.Errorf("state.db missing for repo %s", rec.Path)
	}
	ttl := clientTTLForRepo(rec.Path)
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	dsn := "file:" + rec.StateDB + "?" + q.Encode()
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return report, fmt.Errorf("open state.db: %w", err)
	}
	defer conn.Close()
	if err := conn.PingContext(ctx); err != nil {
		return report, fmt.Errorf("ping state.db: %w", err)
	}
	var schemaVersion int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schemaVersion); err != nil {
		return report, fmt.Errorf("read schema version: %w", err)
	}
	if schemaVersion >= 20 {
		report.CheckpointProtectionAvailable = true
		if value, ok, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionObservationEpoch); ok {
			report.ObservationEpoch, _ = strconv.ParseInt(value, 10, 64)
		}
		if value, ok, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionCoveredEpoch); ok {
			report.CoveredEpoch, _ = strconv.ParseInt(value, 10, 64)
		}
		if value, ok, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionCheckpointID); ok {
			report.LatestCheckpointID = value
		}
		complete := false
		if value, ok, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionComplete); ok {
			complete = strings.EqualFold(value, "true")
		}
		if value, ok, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionRetentionOverBudget); ok {
			report.CheckpointRetentionOverBudget = value == "true" || value == "needs_action"
		}
		if value, ok, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionFullPollTS); ok {
			report.FullPollTS, _ = strconv.ParseFloat(value, 64)
		}
		if value, ok, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionWatcherQueueDepth); ok {
			report.WatcherQueueDepth, _ = strconv.Atoi(value)
		}
		var prepared, needsAction int
		if err := conn.QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN phase='prepared' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN phase='needs_action' THEN 1 ELSE 0 END), 0)
FROM checkpoints`).Scan(&prepared, &needsAction); err != nil {
			return report, fmt.Errorf("checkpoint phases: %w", err)
		}
		if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM checkpoints cp
WHERE cp.phase='completed'
  AND EXISTS (
      SELECT 1
      FROM checkpoint_events ce
      JOIN capture_events e ON e.seq=ce.event_seq
      WHERE ce.checkpoint_id=cp.id AND e.state<>'published'
  )`).Scan(&report.UnpublishedCheckpoints); err != nil {
			return report, fmt.Errorf("unpublished checkpoints: %w", err)
		}
		report.Protected = complete && report.LatestCheckpointID != "" &&
			report.ObservationEpoch == report.CoveredEpoch &&
			prepared == 0 && needsAction == 0
	}

	// daemon_state singleton.
	var pid int
	var mode string
	var heartbeatTS, updatedTS float64
	var branchRef sql.NullString
	var branchGeneration sql.NullInt64
	row := conn.QueryRowContext(ctx,
		`SELECT pid, mode, heartbeat_ts, branch_ref, branch_generation, updated_ts FROM daemon_state WHERE id = 1`)
	if err := row.Scan(&pid, &mode, &heartbeatTS, &branchRef, &branchGeneration, &updatedTS); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, fmt.Errorf("daemon_state: %w", err)
	} else if err == nil {
		report.PID = pid
		report.Daemon = mode
		report.HeartbeatTS = int64(heartbeatTS)
		if heartbeatTS > 0 {
			age := now.Sub(time.Unix(int64(heartbeatTS), 0))
			report.HeartbeatAgeSeconds = int64(age.Seconds())
			if age > ttl {
				report.Stale = true
			}
		}
		if branchRef.Valid {
			report.BranchRef = branchRef.String
		}
	}

	// started_ts is stored in daemon_meta (set by start.go in this lane).
	if v, ok, _ := metaLookup(ctx, conn, "daemon.started_ts"); ok {
		if ts, err := parseFloatStr(v); err == nil {
			report.StartedTS = int64(ts)
			if ts > 0 {
				report.UptimeSeconds = int64(now.Sub(time.Unix(int64(ts), 0)).Seconds())
			}
		}
	}
	if v, ok, _ := metaLookup(ctx, conn, "branch.generation_token"); ok {
		report.BranchGenToken = v
	}

	// Clients.
	rows, err := conn.QueryContext(ctx,
		`SELECT session_id, harness, watch_pid, last_seen_ts
		 FROM daemon_clients ORDER BY last_seen_ts DESC`)
	if err != nil {
		return report, fmt.Errorf("clients: %w", err)
	}
	ttlSecs := int64(ttl.Seconds())
	for rows.Next() {
		var sc statusClient
		var watchPID sql.NullInt64
		var lastSeen float64
		if err := rows.Scan(&sc.SessionID, &sc.Harness, &watchPID, &lastSeen); err != nil {
			rows.Close()
			return report, fmt.Errorf("scan client: %w", err)
		}
		if watchPID.Valid {
			sc.WatchPID = watchPID.Int64
		}
		sc.LastSeenTS = int64(lastSeen)
		ageSecs := int64(now.Sub(time.Unix(int64(lastSeen), 0)).Seconds())
		sc.LastSeenAgeS = ageSecs
		sc.TTLRemaining = ttlSecs - ageSecs
		if sc.TTLRemaining < 0 {
			sc.TTLRemaining = 0
		}
		report.Clients = append(report.Clients, sc)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return report, fmt.Errorf("iter clients: %w", err)
	}
	rows.Close()

	// Pending events (FIFO queue depth) and blocker counts. Shared recovery
	// predicates keep status/list/diagnose aligned while preserving the total
	// blocked_conflicts field as a global terminal stuck-row count.
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStatePending).Scan(&report.PendingEvents); err != nil {
		return report, fmt.Errorf("pending events: %w", err)
	}
	activeGen := int64(0)
	if branchGeneration.Valid {
		activeGen = branchGeneration.Int64
	}
	blockers, err := loadRecoveryBlockerCounts(ctx, conn, report.BranchRef, activeGen)
	if err != nil {
		return report, fmt.Errorf("recovery blocker counts: %w", err)
	}
	report.BlockedConflicts = blockers.TotalBlockedConflicts
	report.ActiveBarriers = blockers.ActiveBlockedBarriersWithSuccessors
	report.ActiveTerminalEvents = blockers.ActiveTerminalEvents
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStateFailed).Scan(&report.FailedEvents); err != nil {
		return report, fmt.Errorf("failed events: %w", err)
	}
	report.FailedBlockingPending = blockers.FailedBarriersWithSuccessors

	// Last commit (latest seq with commit_oid).
	var lastOID sql.NullString
	var lastTS sql.NullFloat64
	var lastMsg sql.NullString
	row = conn.QueryRowContext(ctx,
		`SELECT commit_oid, published_ts, message FROM capture_events
		 WHERE commit_oid IS NOT NULL
		 ORDER BY seq DESC LIMIT 1`)
	if err := row.Scan(&lastOID, &lastTS, &lastMsg); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, fmt.Errorf("last commit: %w", err)
	}
	if lastOID.Valid {
		report.LastCommitOID = lastOID.String
	}
	if lastTS.Valid {
		report.LastCommitTS = int64(lastTS.Float64)
	}
	if lastMsg.Valid {
		report.LastCommitMessage = lastMsg.String
	}

	// Capture errors: count of meta rows under the capture_error.* prefix.
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM daemon_meta WHERE key LIKE 'capture_error.%'`).Scan(&report.CaptureErrors); err != nil {
		return report, fmt.Errorf("capture errors: %w", err)
	}

	// Durable capture-backpressure state. Presence of the meta key signals
	// "saturated"; readers should not block on the timestamp shape.
	if v, ok, err := metaLookup(ctx, conn, "capture.backpressure_paused_at"); err != nil {
		return report, fmt.Errorf("backpressure state: %w", err)
	} else if ok {
		report.BackpressurePaused = true
		report.BackpressurePausedAt = v
	}
	if v, ok, err := metaLookup(ctx, conn, "capture.events_dropped_total"); err != nil {
		return report, fmt.Errorf("events dropped total: %w", err)
	} else if ok && v != "" {
		if total, perr := strconv.ParseInt(v, 10, 64); perr == nil {
			report.EventsDroppedTotal = total
		}
	}
	if info, err := pauseInfoForRepo(ctx, conn, rec.StateDB, now); err != nil {
		return report, fmt.Errorf("pause state: %w", err)
	} else if info != nil {
		report.Paused = true
		report.Pause = info
	}
	if intentStrategy, err := loadIntentStrategyReport(ctx, conn); err != nil {
		return report, fmt.Errorf("intent strategy: %w", err)
	} else {
		report.IntentStrategy = intentStrategy
	}
	if err := statusDecisionSummary(ctx, conn, &report); err != nil {
		return report, err
	}
	if runtimeConfig, err := loadRuntimeConfigReport(ctx, conn, rec.RepoHash, now); err != nil {
		return report, fmt.Errorf("runtime settings: %w", err)
	} else {
		report.RuntimeConfig = runtimeConfig
	}
	if readiness, err := loadConfigReadinessReport(ctx, conn, now); err != nil {
		return report, fmt.Errorf("configuration readiness: %w", err)
	} else {
		report.Configuration = readiness
	}
	if replay, err := loadReplayObservabilityReport(ctx, conn); err != nil {
		return report, fmt.Errorf("replay observability: %w", err)
	} else {
		report.Replay = replay
	}
	if intentV2, err := loadIntentV2Report(ctx, conn); err != nil {
		return report, err
	} else {
		report.IntentV2 = intentV2
	}
	if publication, err := loadSelfPublicationReport(
		ctx, conn, rec.StateDB, now, len(findDaemonProcesses(ctx, rec.Path)),
	); err != nil {
		return report, fmt.Errorf("self-publication observability: %w", err)
	} else {
		report.SelfPublication = publication
	}

	return report, nil
}

func runStatusWatch(ctx context.Context, out io.Writer, repo string, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("acd status: --interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fmt.Fprint(out, "\033[2J\033[H")
		fmt.Fprintf(out, "Updated: %s\n\n", time.Now().Format(time.RFC3339))
		if err := runStatus(ctx, out, repo, false); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func statusDecisionSummary(ctx context.Context, conn *sql.DB, report *statusReport) error {
	ok, err := sqliteTableExists(ctx, conn, "decision_records")
	if err != nil {
		return fmt.Errorf("decision table check: %w", err)
	}
	if !ok {
		return nil
	}
	rows, err := conn.QueryContext(ctx, `SELECT kind, COUNT(*) FROM decision_records GROUP BY kind`)
	if err != nil {
		return fmt.Errorf("decision counts: %w", err)
	}
	counts := map[string]int{}
	for rows.Next() {
		var kind string
		var n int
		if err := rows.Scan(&kind, &n); err != nil {
			rows.Close()
			return fmt.Errorf("scan decision counts: %w", err)
		}
		counts[kind] = n
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iter decision counts: %w", err)
	}
	rows.Close()
	if len(counts) > 0 {
		report.DecisionCounts = counts
	}

	recentRows, err := conn.QueryContext(ctx, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
ORDER BY id DESC
LIMIT 3`)
	if err != nil {
		return fmt.Errorf("recent decisions: %w", err)
	}
	defer recentRows.Close()
	for recentRows.Next() {
		var row state.DecisionRecord
		if err := recentRows.Scan(&row.ID, &row.DecisionTS, &row.Kind, &row.Path, &row.Reason,
			&row.EventSeq, &row.HeadSHA, &row.CommitOID, &row.BranchRef,
			&row.BranchGeneration, &row.ActionTaken, &row.UserMessage); err != nil {
			return fmt.Errorf("scan recent decision: %w", err)
		}
		report.RecentDecisions = append(report.RecentDecisions, decisionEntry(row))
		if row.ID > report.DecisionCursor {
			report.DecisionCursor = row.ID
		}
	}
	if err := recentRows.Err(); err != nil {
		return fmt.Errorf("iter recent decisions: %w", err)
	}
	if err := enrichEventEntries(ctx, conn, report.RecentDecisions); err != nil {
		return fmt.Errorf("enrich recent decisions: %w", err)
	}
	return nil
}

func sqliteTableExists(ctx context.Context, conn *sql.DB, name string) (bool, error) {
	var n int
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name,
	).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

func countBlockingTerminalEvents(ctx context.Context, conn *sql.DB, terminalState string) (int, error) {
	return countTerminalBarriersWithSuccessors(ctx, conn, terminalState, "", 0)
}

// metaLookup is the read-only equivalent of state.MetaGet against a raw
// *sql.DB connection (we don't want to spin up the migration path on a
// read-only DSN).
func metaLookup(ctx context.Context, conn *sql.DB, key string) (string, bool, error) {
	var v string
	err := conn.QueryRowContext(ctx, `SELECT value FROM daemon_meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

func parseFloatStr(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err
}

func renderStatusHuman(out io.Writer, r statusReport) error {
	fmt.Fprintf(out, "Repo: %s\n", r.Repo)

	daemon := r.Daemon
	if r.Stale {
		daemon = "stale"
	}
	parts := []string{daemon}
	if r.PID > 0 {
		parts = append(parts, fmt.Sprintf("pid %d", r.PID))
	}
	if r.HeartbeatTS > 0 {
		parts = append(parts, fmt.Sprintf("heartbeat %s ago",
			formatDurationCompact(time.Duration(r.HeartbeatAgeSeconds)*time.Second)))
	}
	if r.UptimeSeconds > 0 {
		parts = append(parts, fmt.Sprintf("started %s ago",
			formatDurationCompact(time.Duration(r.UptimeSeconds)*time.Second)))
	}
	fmt.Fprintf(out, "Daemon: %s\n", joinParens(parts))
	renderRuntimeConfigHuman(out, r.RuntimeConfig)
	renderConfigReadinessHuman(out, r.Configuration)
	renderReplayObservabilityHuman(out, r.Replay)
	renderIntentV2Human(out, r.IntentV2)
	renderSelfPublicationHuman(out, r.SelfPublication, "")

	fmt.Fprintf(out, "Clients (%d):\n", len(r.Clients))
	for _, c := range r.Clients {
		ageStr := formatDurationCompact(time.Duration(c.LastSeenAgeS) * time.Second)
		sid := c.SessionID
		if len(sid) > 8 {
			sid = sid[:4] + "..."
		}
		fmt.Fprintf(out, "  - %-12s session %s last_seen %s ago\n", c.Harness, sid, ageStr)
	}

	fmt.Fprintf(out, "Pending events: %d\n", r.PendingEvents)
	if r.BlockedConflicts > 0 {
		fmt.Fprintf(out, "Blocked conflicts: %d (inspect with `acd diagnose`; preview safe cleanup with `acd fix --dry-run`)\n", r.BlockedConflicts)
		if r.ActiveBarriers > 0 {
			fmt.Fprintf(out, "Blocked barriers with pending replay: %d (archive-only recovery preview: `acd fix --force --dry-run`)\n", r.ActiveBarriers)
		}
	}
	if r.FailedEvents > 0 {
		fmt.Fprintf(out, "Failed terminal events: %d\n", r.FailedEvents)
		if r.FailedBlockingPending > 0 {
			fmt.Fprintf(out, "Failed barriers blocking pending replay: %d (inspect with `acd diagnose`; preview cleanup with `acd fix --dry-run`)\n",
				r.FailedBlockingPending)
		}
	}
	if r.BackpressurePaused {
		stamp := r.BackpressurePausedAt
		if stamp == "" {
			stamp = "unset"
		}
		fmt.Fprintf(out, "Backpressure: paused since %s (events dropped lifetime: %d)\n",
			stamp, r.EventsDroppedTotal)
	} else if r.EventsDroppedTotal > 0 {
		fmt.Fprintf(out, "Capture dropped lifetime: %d\n", r.EventsDroppedTotal)
	}

	if r.LastCommitOID != "" {
		oid := r.LastCommitOID
		if len(oid) > 7 {
			oid = oid[:7]
		}
		bits := []string{oid}
		if r.LastCommitTS > 0 {
			age := time.Since(time.Unix(r.LastCommitTS, 0))
			bits = append(bits, formatDurationCompact(age)+" ago")
		}
		if r.LastCommitMessage != "" {
			bits = append(bits, fmt.Sprintf("%q", r.LastCommitMessage))
		}
		fmt.Fprintf(out, "Last commit: %s\n", joinParens2(bits))
	} else {
		fmt.Fprintln(out, "Last commit: none")
	}

	if r.CaptureErrors == 0 {
		fmt.Fprintln(out, "Capture errors: none")
	} else {
		fmt.Fprintf(out, "Capture errors: %d\n", r.CaptureErrors)
	}

	if r.Pause != nil {
		fmt.Fprintln(out, "Pause:")
		fmt.Fprintf(out, "  Source: %s\n", strings.ReplaceAll(r.Pause.Source, "_", " "))
		if r.Pause.Reason != "" {
			fmt.Fprintf(out, "  Reason: %s\n", r.Pause.Reason)
		}
		if r.Pause.SetAt != "" {
			fmt.Fprintf(out, "  Set at: %s\n", r.Pause.SetAt)
		}
		if r.Pause.ExpiresAt != "" {
			fmt.Fprintf(out, "  Expires at: %s (%s remaining)\n",
				r.Pause.ExpiresAt,
				formatDurationCompact(time.Duration(r.Pause.RemainingSeconds)*time.Second))
		}
	}

	renderIntentStrategyHuman(out, r.IntentStrategy)

	if len(r.DecisionCounts) > 0 {
		fmt.Fprintf(out, "Decisions: %s\n", formatDecisionCounts(r.DecisionCounts))
		if len(r.RecentDecisions) > 0 {
			fmt.Fprintln(out, "Recent decisions:")
			for _, ev := range r.RecentDecisions {
				fmt.Fprintf(out, "  - #%d %s", ev.ID, ev.Kind)
				if ev.Path != "" {
					fmt.Fprintf(out, " %s", ev.Path)
				}
				if ev.ActionTaken != "" {
					fmt.Fprintf(out, " (%s)", ev.ActionTaken)
				} else if ev.Reason != "" {
					fmt.Fprintf(out, " (%s)", ev.Reason)
				}
				if len(ev.GroupedSeqs) > 1 {
					fmt.Fprintf(out, " seqs=%s", formatSeqs(ev.GroupedSeqs))
				}
				fmt.Fprintln(out)
			}
		}
		fmt.Fprintln(out, "Explain: acd explain --path FILE; stream: acd events --watch")
	}

	if r.BranchGenToken != "" {
		fmt.Fprintf(out, "Branch generation: %s\n", r.BranchGenToken)
	}
	return nil
}

func formatDecisionCounts(counts map[string]int) string {
	order := []string{
		state.DecisionKindProtected,
		state.DecisionKindHandledExternal,
		state.DecisionKindSupersededExternal,
		state.DecisionKindBlocked,
		state.DecisionKindCommitted,
		state.DecisionKindCaptured,
		state.DecisionKindSkipped,
		state.DecisionKindPaused,
		state.DecisionKindResumed,
		state.DecisionKindIntentDeferred,
		state.DecisionKindIntentForced,
		state.DecisionKindIntentPlannerError,
		state.DecisionKindMessageQualityRewrite,
		state.DecisionKindMessageQualityFallback,
	}
	seen := make(map[string]bool, len(counts))
	var parts []string
	for _, kind := range order {
		if n, ok := counts[kind]; ok {
			parts = append(parts, fmt.Sprintf("%s=%d", kind, n))
			seen[kind] = true
		}
	}
	for kind, n := range counts {
		if !seen[kind] {
			parts = append(parts, fmt.Sprintf("%s=%d", kind, n))
		}
	}
	return strings.Join(parts, " ")
}

// joinParens renders ["running", "pid 123", "heartbeat 2s ago"] as
// "running (pid 123, heartbeat 2s ago)".
func joinParens(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return parts[0] + " (" + strings.Join(parts[1:], ", ") + ")"
}

// joinParens2 renders ["a1b2c3d", "47s ago", "\"Update auth.py\""] as
// `a1b2c3d (47s ago, "Update auth.py")`.
func joinParens2(parts []string) string { return joinParens(parts) }

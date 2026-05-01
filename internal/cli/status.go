package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
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
	Repo                 string         `json:"repo"`
	RepoHash             string         `json:"repo_hash"`
	Daemon               string         `json:"daemon"`
	Stale                bool           `json:"stale"`
	PID                  int            `json:"pid"`
	StartedTS            int64          `json:"started_ts,omitempty"`
	UptimeSeconds        int64          `json:"uptime_seconds,omitempty"`
	HeartbeatTS          int64          `json:"heartbeat_ts,omitempty"`
	HeartbeatAgeSeconds  int64          `json:"heartbeat_age_seconds,omitempty"`
	BranchRef            string         `json:"branch_ref,omitempty"`
	BranchGenToken       string         `json:"branch_generation_token,omitempty"`
	Clients              []statusClient `json:"clients"`
	PendingEvents        int            `json:"pending_events"`
	BlockedConflicts     int            `json:"blocked_conflicts"`
	LastCommitOID        string         `json:"last_commit_oid,omitempty"`
	LastCommitTS         int64          `json:"last_commit_ts,omitempty"`
	LastCommitMessage    string         `json:"last_commit_message,omitempty"`
	CaptureErrors        int            `json:"capture_errors"`
	Paused               bool           `json:"paused,omitempty"`
	Pause                *pauseInfo     `json:"pause,omitempty"`
	BackpressurePaused   bool           `json:"backpressure_paused,omitempty"`
	BackpressurePausedAt string         `json:"backpressure_paused_at,omitempty"`
	EventsDroppedTotal   int64          `json:"events_dropped_total,omitempty"`
}

func newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print current daemon + clients for one repo (default: cwd)",
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runStatus(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
	return cmd
}

func runStatus(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	abs, err := resolveRepo(repo)
	if err != nil {
		return err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd status: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return fmt.Errorf("acd status: load registry: %w", err)
	}
	rec, ok := findRepo(reg, abs)
	if !ok {
		return fmt.Errorf("acd status: repo %s is not registered (try `acd start --repo %s`)", abs, abs)
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

// findRepo returns the registry record whose Path matches abs.
func findRepo(reg *central.Registry, abs string) (central.RepoRecord, bool) {
	for _, r := range reg.Repos {
		if central.SameRepoPath(r.Path, abs) {
			return r, true
		}
	}
	return central.RepoRecord{}, false
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

	// daemon_state singleton.
	var pid int
	var mode string
	var heartbeatTS, updatedTS float64
	var branchRef sql.NullString
	row := conn.QueryRowContext(ctx,
		`SELECT pid, mode, heartbeat_ts, branch_ref, updated_ts FROM daemon_state WHERE id = 1`)
	if err := row.Scan(&pid, &mode, &heartbeatTS, &branchRef, &updatedTS); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, fmt.Errorf("daemon_state: %w", err)
	} else if err == nil {
		report.PID = pid
		report.Daemon = mode
		report.HeartbeatTS = int64(heartbeatTS)
		if heartbeatTS > 0 {
			age := now.Sub(time.Unix(int64(heartbeatTS), 0))
			report.HeartbeatAgeSeconds = int64(age.Seconds())
			if age > clientTTL() {
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
	ttlSecs := int64(clientTTL().Seconds())
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

	// Pending events (FIFO queue depth) and blocked-conflict count
	// (terminal replay blockers — distinct from pending so the operator
	// can spot a stuck row that the daemon will not retry on its own).
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStatePending).Scan(&report.PendingEvents); err != nil {
		return report, fmt.Errorf("pending events: %w", err)
	}
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStateBlockedConflict).Scan(&report.BlockedConflicts); err != nil {
		return report, fmt.Errorf("blocked conflicts: %w", err)
	}

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

	return report, nil
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
		fmt.Fprintf(out, "Blocked conflicts: %d\n", r.BlockedConflicts)
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

	if r.BranchGenToken != "" {
		fmt.Fprintf(out, "Branch generation: %s\n", r.BranchGenToken)
	}
	return nil
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

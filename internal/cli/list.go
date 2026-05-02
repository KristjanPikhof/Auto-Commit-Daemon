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
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"

	_ "modernc.org/sqlite"
)

const defaultListWatchInterval = 2 * time.Second

// listEntry is one row in the `acd list` output. JSON marshal tags match
// the §7.7 example shape.
type listEntry struct {
	Path             string     `json:"path"`
	RepoHash         string     `json:"repo_hash"`
	Daemon           string     `json:"daemon"`
	PID              int        `json:"pid,omitempty"`
	Clients          int        `json:"clients"`
	LastSeq          int64      `json:"last_seq"`
	LastCommitOID    string     `json:"last_commit_oid,omitempty"`
	HeartbeatAgeSecs float64    `json:"heartbeat_age_seconds,omitempty"`
	PendingEvents    int        `json:"pending_events"`
	BlockedConflicts int        `json:"blocked_conflicts"`
	Status           string     `json:"status"`
	StatusNote       string     `json:"status_note,omitempty"`
	Paused           bool       `json:"paused,omitempty"`
	StaleHeartbeat   bool       `json:"stale_heartbeat,omitempty"`
	Pause            *pauseInfo `json:"pause,omitempty"`
}

func newListCmd() *cobra.Command {
	var watch bool
	var interval time.Duration

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all known daemons across repos",
		RunE: func(cmd *cobra.Command, args []string) error {
			jsonOut, _ := cmd.Flags().GetBool("json")
			if watch {
				if jsonOut {
					return fmt.Errorf("acd list: --watch does not support --json")
				}
				return runListWatch(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), interval)
			}
			return runList(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), jsonOut)
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "Refresh list output until interrupted")
	cmd.Flags().DurationVar(&interval, "interval", defaultListWatchInterval, "Refresh interval for --watch (Go duration)")
	return cmd
}

type listSnapshot struct {
	UpdatedAt time.Time
	Entries   []listEntry
}

func runList(ctx context.Context, out, errOut io.Writer, jsonOut bool) error {
	snapshot, err := collectListSnapshot(ctx, errOut)
	if err != nil {
		return err
	}
	if jsonOut {
		return renderListJSON(out, snapshot.Entries)
	}
	return renderListTable(out, snapshot.Entries)
}

func runListWatch(ctx context.Context, out, errOut io.Writer, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("acd list: --interval must be positive")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		snapshot, err := collectListSnapshot(ctx, errOut)
		if err != nil {
			return err
		}
		if err := renderListWatchFrame(out, snapshot); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func collectListSnapshot(ctx context.Context, errOut io.Writer) (listSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	roots, err := paths.Resolve()
	if err != nil {
		return listSnapshot{}, fmt.Errorf("acd list: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return listSnapshot{}, fmt.Errorf("acd list: load registry: %w", err)
	}

	now := time.Now()
	ttl := clientTTL()
	entries := make([]listEntry, 0, len(reg.Repos))

	for _, rec := range reg.Repos {
		e := listEntry{
			Path:     rec.Path,
			RepoHash: rec.RepoHash,
			Daemon:   "-",
			Status:   "OK",
		}

		// Repo dir missing — we still emit a row so the user sees what gc
		// would prune.
		if !fileExists(rec.Path) {
			e.Status = "missing"
			e.StatusNote = "repo missing"
			entries = append(entries, e)
			continue
		}

		// State DB missing or unreadable — log the skip and emit a row
		// flagged so gc can pick it up.
		if !fileExists(rec.StateDB) {
			fmt.Fprintf(errOut, "acd list: state.db missing for %s\n", rec.Path)
			e.Status = "missing"
			e.StatusNote = "state.db missing"
			entries = append(entries, e)
			continue
		}

		summary, err := summarizeRepo(ctx, rec.StateDB, now, ttl)
		if err != nil {
			fmt.Fprintf(errOut, "acd list: skip %s: %v\n", rec.Path, err)
			e.Status = "unreadable"
			e.StatusNote = err.Error()
			entries = append(entries, e)
			continue
		}
		e.Daemon = summary.daemon
		e.PID = summary.pid
		e.Clients = summary.clients
		e.LastSeq = summary.lastSeq
		e.LastCommitOID = summary.lastCommitOID
		e.HeartbeatAgeSecs = summary.heartbeatAge.Seconds()
		e.PendingEvents = summary.pendingEvents
		e.BlockedConflicts = summary.blockedConflicts
		if summary.pause != nil {
			e.Status = "paused"
			e.StatusNote = pauseStatusNote(summary.pause)
			e.Paused = true
			e.Pause = summary.pause
		}
		if summary.daemon == "stale" {
			e.StaleHeartbeat = true
			staleNote := "daemon stale " + formatDurationCompact(summary.heartbeatAge)
			if e.Status == "paused" {
				// Combined paused+stale presentation: keep Status="paused"
				// (operator intent wins) but append the stale-heartbeat fact
				// so a paused-but-dead daemon never silently disappears.
				if e.StatusNote == "" {
					e.StatusNote = staleNote
				} else {
					e.StatusNote = e.StatusNote + "; " + staleNote
				}
			} else {
				if summary.clients == 0 {
					// Inactive stale daemon with zero live clients is hidden
					// from the default list (gc is responsible for it). When
					// a pause marker is present we already kept the row above.
					continue
				}
				e.Status = "stale"
				e.StatusNote = "stale heartbeat (" + formatDurationCompact(summary.heartbeatAge) + ")"
			}
		}
		entries = append(entries, e)
	}

	return listSnapshot{
		UpdatedAt: now,
		Entries:   entries,
	}, nil
}

func renderListJSON(out io.Writer, entries []listEntry) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		Repos []listEntry `json:"repos"`
	}{Repos: entries})
}

func renderListTable(out io.Writer, entries []listEntry) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "REPO\tDAEMON\tCLIENTS\tPENDING\tBLOCKED\tLAST_COMMIT\tSTATUS")
	for _, e := range entries {
		clients := dashIfMissing(e.Status, fmt.Sprintf("%d", e.Clients))
		pending := dashIfMissing(e.Status, fmt.Sprintf("%d", e.PendingEvents))
		blocked := dashIfMissing(e.Status, fmt.Sprintf("%d", e.BlockedConflicts))
		lastOID := "-"
		if e.LastCommitOID != "" {
			if len(e.LastCommitOID) > 7 {
				lastOID = e.LastCommitOID[:7]
			} else {
				lastOID = e.LastCommitOID
			}
		}
		statusCol := e.Status
		if e.StatusNote != "" {
			statusCol = e.Status + " (" + e.StatusNote + ")"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			homeShort(e.Path), e.Daemon, clients, pending, blocked, lastOID, statusCol)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("acd list: flush: %w", err)
	}
	return nil
}

func renderListWatchFrame(out io.Writer, snapshot listSnapshot) error {
	fmt.Fprint(out, "\033[2J\033[H")
	fmt.Fprintf(out, "Updated: %s\n\n", snapshot.UpdatedAt.Format(time.RFC3339))
	return renderListTable(out, snapshot.Entries)
}

// dashIfMissing returns "-" when the row represents a missing/unreadable
// repo so the table reads "no data yet" without lying about zero rows.
func dashIfMissing(status, val string) string {
	if status == "missing" || status == "unreadable" {
		return "-"
	}
	return val
}

// repoSummary is the subset of state.db fields the CLI needs.
type repoSummary struct {
	daemon           string
	pid              int
	clients          int
	lastSeq          int64
	lastCommitOID    string
	heartbeatAge     time.Duration
	startedTS        float64
	heartbeatTS      float64
	pendingEvents    int
	blockedConflicts int
	pause            *pauseInfo
}

// summarizeRepo opens the per-repo state.db read-only and pulls a small
// summary used by both list and status. Read-only DSN avoids accidentally
// touching the file when a daemon is not running.
func summarizeRepo(ctx context.Context, dbPath string, now time.Time, ttl time.Duration) (repoSummary, error) {
	if !fileExists(dbPath) {
		return repoSummary{}, errors.New("state.db missing")
	}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	dsn := "file:" + dbPath + "?" + q.Encode()
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return repoSummary{}, fmt.Errorf("open: %w", err)
	}
	defer conn.Close()
	if err := conn.PingContext(ctx); err != nil {
		return repoSummary{}, fmt.Errorf("ping: %w", err)
	}

	var s repoSummary

	// daemon_state row (singleton).
	var pid int
	var mode string
	var heartbeat float64
	var note sql.NullString
	row := conn.QueryRowContext(ctx,
		`SELECT pid, mode, heartbeat_ts, note FROM daemon_state WHERE id = 1`)
	if err := row.Scan(&pid, &mode, &heartbeat, &note); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.daemon = "stopped"
		} else {
			return repoSummary{}, fmt.Errorf("daemon_state: %w", err)
		}
	} else {
		s.pid = pid
		s.daemon = mode
		s.heartbeatTS = heartbeat
		if heartbeat > 0 {
			age := now.Sub(time.Unix(int64(heartbeat), 0))
			s.heartbeatAge = age
			if age > ttl {
				s.daemon = "stale"
			}
		}
		// PID dead overlay: even if heartbeat is fresh-looking, a dead PID
		// implies the daemon crashed without updating state.
		if pid > 0 && !identity.Alive(pid) && mode == "running" {
			s.daemon = "stale"
		}
	}

	// Client count. Count the clients that would survive the daemon's
	// refcount sweep; otherwise stale rows linger forever once a daemon dies.
	clients, err := countLiveClients(ctx, conn, now, ttl)
	if err != nil {
		return repoSummary{}, err
	}
	s.clients = clients

	// Last commit (latest seq with non-null commit_oid).
	var lastSeq sql.NullInt64
	var lastOID sql.NullString
	row = conn.QueryRowContext(ctx,
		`SELECT seq, commit_oid FROM capture_events
		 WHERE commit_oid IS NOT NULL
		 ORDER BY seq DESC LIMIT 1`)
	if err := row.Scan(&lastSeq, &lastOID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return repoSummary{}, fmt.Errorf("last commit: %w", err)
	}
	if lastSeq.Valid {
		s.lastSeq = lastSeq.Int64
	}
	if lastOID.Valid {
		s.lastCommitOID = lastOID.String
	}

	// Pending FIFO depth + terminal blocked-conflict count. Same RO conn
	// already in hand — read both directly so list/status/doctor agree.
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStatePending).Scan(&s.pendingEvents); err != nil {
		return repoSummary{}, fmt.Errorf("pending events: %w", err)
	}
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStateBlockedConflict).Scan(&s.blockedConflicts); err != nil {
		return repoSummary{}, fmt.Errorf("blocked conflicts: %w", err)
	}
	if info, err := pauseInfoForRepo(ctx, conn, dbPath, now); err != nil {
		return repoSummary{}, fmt.Errorf("pause state: %w", err)
	} else {
		s.pause = info
	}

	return s, nil
}

func countLiveClients(ctx context.Context, conn *sql.DB, now time.Time, ttl time.Duration) (int, error) {
	rows, err := conn.QueryContext(ctx,
		`SELECT watch_pid, last_seen_ts FROM daemon_clients`)
	if err != nil {
		return 0, fmt.Errorf("count clients: %w", err)
	}
	defer rows.Close()

	cutoff := float64(now.UnixNano())/1e9 - ttl.Seconds()
	live := 0
	for rows.Next() {
		var watchPID sql.NullInt64
		var lastSeen float64
		if err := rows.Scan(&watchPID, &lastSeen); err != nil {
			return 0, fmt.Errorf("scan clients: %w", err)
		}
		if lastSeen < cutoff {
			continue
		}
		if watchPID.Valid && watchPID.Int64 > 0 && !identity.Alive(int(watchPID.Int64)) {
			continue
		}
		live++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iter clients: %w", err)
	}
	return live, nil
}

// silence unused-import warning when paths package is not referenced
// directly inside this file (kept for symmetry with paths usage in tests).
var _ = os.Stdin

package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/mattn/go-isatty"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"

	_ "modernc.org/sqlite"
)

const defaultListWatchInterval = 2 * time.Second

// listUseWatchMode reports whether acd list should run the live dashboard.
// Explicit --once disables watch; explicit --watch enables it; otherwise TTY
// stdout defaults to watch.
func listUseWatchMode(stdout *os.File, once, watchExplicit bool) bool {
	if once {
		return false
	}
	if watchExplicit {
		return true
	}
	if stdout == nil {
		return false
	}
	return isatty.IsTerminal(stdout.Fd()) || isatty.IsCygwinTerminal(stdout.Fd())
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
	activeBarriers   int
	pause            *pauseInfo
	intentWait       *listIntentWaitSummary
	intentV2         intentV2Report
}

type listIntentWaitSummary struct {
	waitSeconds    int64
	visiblePending int
	minPending     int
	reason         string
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
	var branchRef sql.NullString
	var branchGeneration sql.NullInt64
	row := conn.QueryRowContext(ctx,
		`SELECT pid, mode, heartbeat_ts, note, branch_ref, branch_generation FROM daemon_state WHERE id = 1`)
	if err := row.Scan(&pid, &mode, &heartbeat, &note, &branchRef, &branchGeneration); err != nil {
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
	// already in hand — read through the shared recovery predicates so list,
	// status, diagnose, and fix-facing counts stay aligned.
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStatePending).Scan(&s.pendingEvents); err != nil {
		return repoSummary{}, fmt.Errorf("pending events: %w", err)
	}
	activeBranchRef := ""
	if branchRef.Valid {
		activeBranchRef = branchRef.String
	}
	activeGen := int64(0)
	if branchGeneration.Valid {
		activeGen = branchGeneration.Int64
	}
	blockers, err := loadRecoveryBlockerCounts(ctx, conn, activeBranchRef, activeGen)
	if err != nil {
		return repoSummary{}, fmt.Errorf("recovery blocker counts: %w", err)
	}
	s.blockedConflicts = blockers.TotalBlockedConflicts
	s.activeBarriers = blockers.ActiveBlockedBarriersWithSuccessors
	if s.intentV2, err = loadIntentV2Report(ctx, conn); err != nil {
		return repoSummary{}, fmt.Errorf("Intent v2 summary: %w", err)
	}
	if s.pendingEvents > 0 && s.blockedConflicts == 0 && s.activeBarriers == 0 {
		if intentWait, err := loadListIntentWaitSummary(ctx, conn); err != nil {
			return repoSummary{}, fmt.Errorf("intent wait summary: %w", err)
		} else {
			s.intentWait = intentWait
		}
	}
	if info, err := pauseInfoForRepo(ctx, conn, dbPath, now); err != nil {
		return repoSummary{}, fmt.Errorf("pause state: %w", err)
	} else {
		s.pause = info
	}

	return s, nil
}

func loadListIntentWaitSummary(ctx context.Context, conn *sql.DB) (*listIntentWaitSummary, error) {
	report := intentStrategyFromEnv()
	strategy, err := ResolveEffectiveCommitStrategy(ctx, conn)
	if err != nil {
		return nil, err
	}
	report.Strategy = string(strategy)
	report.Active = strategy == "intent"
	if !report.Active {
		return nil, nil
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.min_pending"); err != nil {
		return nil, fmt.Errorf("intent.min_pending: %w", err)
	} else if ok {
		report.MinPending = parseIntentMetaInt(v, report.MinPending)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.max_pending_age"); err != nil {
		return nil, fmt.Errorf("intent.max_pending_age: %w", err)
	} else if ok {
		report.MaxPendingAgeSeconds = parseIntentMetaDurationSeconds(v, report.MaxPendingAgeSeconds)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.defer_limit"); err != nil {
		return nil, fmt.Errorf("intent.defer_limit: %w", err)
	} else if ok {
		report.DeferLimit = parseIntentMetaInt(v, report.DeferLimit)
	}
	if ok, err := sqliteTableExists(ctx, conn, "planner_state"); err != nil {
		return nil, fmt.Errorf("planner_state table check: %w", err)
	} else if ok {
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
			return nil, fmt.Errorf("planner forced-aging summary: %w", err)
		}
	}
	if err := loadIntentBatchWait(ctx, conn, &report); err != nil {
		return nil, err
	}
	if !report.BatchWaitActive {
		return nil, nil
	}
	waitSeconds := report.AgeTriggerInSeconds
	if report.BatchWaitReason == "skipped_due_intent_settle_window" {
		waitSeconds = report.SettleTriggerInSeconds
	}
	if waitSeconds <= 0 {
		return nil, nil
	}
	return &listIntentWaitSummary{
		waitSeconds:    waitSeconds,
		visiblePending: report.VisiblePendingEvents,
		minPending:     report.MinPending,
		reason:         report.BatchWaitReason,
	}, nil
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

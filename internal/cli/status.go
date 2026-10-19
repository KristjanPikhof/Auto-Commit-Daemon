package cli

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "modernc.org/sqlite"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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

// publicationProgressReport is the small, shared product projection used by
// status and list. WorkerResponsive is deliberately separate from
// LastProgressTS: a fresh heartbeat proves that the worker is alive, not that
// the publication frontier is moving.
type publicationProgressReport struct {
	Strategy               string  `json:"strategy"`
	Phase                  string  `json:"phase"`
	LastProgressTS         float64 `json:"last_progress_ts,omitempty"`
	LastProgressAgeSeconds int64   `json:"last_progress_age_seconds,omitempty"`
	WorkerResponsive       bool    `json:"worker_responsive"`
	HeartbeatAgeSeconds    int64   `json:"heartbeat_age_seconds,omitempty"`
}

// statusReport is the JSON shape for `acd status --json`. Mirrors the
// human-readable layout 1:1 so users can flip flags without losing fields.
type statusReport struct {
	PublicationOutcome            publicationOutcome           `json:"publication_outcome"`
	Repo                          string                       `json:"repo"`
	RepoHash                      string                       `json:"repo_hash"`
	Daemon                        string                       `json:"daemon"`
	Stale                         bool                         `json:"stale"`
	PID                           int                          `json:"pid"`
	StartedTS                     int64                        `json:"started_ts,omitempty"`
	UptimeSeconds                 int64                        `json:"uptime_seconds,omitempty"`
	HeartbeatTS                   int64                        `json:"heartbeat_ts,omitempty"`
	HeartbeatAgeSeconds           int64                        `json:"heartbeat_age_seconds,omitempty"`
	BranchRef                     string                       `json:"branch_ref,omitempty"`
	BranchGeneration              int64                        `json:"branch_generation,omitempty"`
	BranchGenToken                string                       `json:"branch_generation_token,omitempty"`
	Clients                       []statusClient               `json:"clients"`
	PendingEvents                 int                          `json:"pending_events"`
	BlockedConflicts              int                          `json:"blocked_conflicts"`
	ActiveBarriers                int                          `json:"active_barriers,omitempty"`
	ActiveTerminalEvents          int                          `json:"active_terminal_events,omitempty"`
	FailedEvents                  int                          `json:"failed_events"`
	FailedBlockingPending         int                          `json:"failed_blocking_pending"`
	LastCommitOID                 string                       `json:"last_commit_oid,omitempty"`
	LastCommitTS                  int64                        `json:"last_commit_ts,omitempty"`
	LastCommitMessage             string                       `json:"last_commit_message,omitempty"`
	CaptureHealth                 state.CaptureHealth          `json:"capture_health"`
	CaptureErrors                 int                          `json:"capture_errors"`
	Paused                        bool                         `json:"paused,omitempty"`
	Pause                         *pauseInfo                   `json:"pause,omitempty"`
	BackpressurePaused            bool                         `json:"backpressure_paused,omitempty"`
	BackpressurePausedAt          string                       `json:"backpressure_paused_at,omitempty"`
	EventsDroppedTotal            int64                        `json:"events_dropped_total,omitempty"`
	DecisionCounts                map[string]int               `json:"decision_counts,omitempty"`
	RecentDecisions               []eventEntry                 `json:"recent_decisions,omitempty"`
	DecisionCursor                int64                        `json:"decision_cursor,omitempty"`
	CheckpointProtectionAvailable bool                         `json:"checkpoint_protection_available"`
	Protected                     bool                         `json:"protected"`
	ObservationEpoch              int64                        `json:"observation_epoch,omitempty"`
	CoveredEpoch                  int64                        `json:"covered_epoch,omitempty"`
	LatestCheckpointID            string                       `json:"latest_checkpoint_id,omitempty"`
	UnpublishedCheckpoints        int                          `json:"unpublished_checkpoints,omitempty"`
	CheckpointMaintenance         checkpoint.MaintenanceStatus `json:"checkpoint_maintenance"`
	CheckpointRetentionOverBudget bool                         `json:"checkpoint_retention_over_budget,omitempty"`
	FullPollTS                    float64                      `json:"full_poll_ts,omitempty"`
	WatcherQueueDepth             int                          `json:"watcher_queue_depth,omitempty"`
	Busy                          bool                         `json:"busy"`
	OperationalState              string                       `json:"operational_state"`
	WorktreeClean                 bool                         `json:"worktree_clean"`
	AllChangesCommittedInGit      bool                         `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD      bool                         `json:"checkpoint_published_by_acd"`

	PublicationProgress   publicationProgressReport `json:"publication_progress"`
	checkpointPrepared    bool
	checkpointNeedsAction bool
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

func checkpointPublishedByACD(report statusReport) bool {
	if report.PublicationOutcome.CheckpointAvailable {
		return report.Protected && report.PublicationOutcome.Resolved
	}
	return report.Protected && report.UnpublishedCheckpoints == 0 &&
		report.PendingEvents == 0 && report.ActiveTerminalEvents == 0
}

// buildStatusReport opens the per-repo state.db read-only and projects the
// daemon_state + daemon_clients + last commit + meta rows into a flat
// report struct. Never mutates state.
func buildStatusReport(ctx context.Context, rec central.RepoRecord, now time.Time) (statusReport, error) {
	return readFinishStatusReport(ctx, rec, now)
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

package cli

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"

	_ "modernc.org/sqlite"
)

const (
	productListReadLimit   = 8
	productListReadTimeout = 500 * time.Millisecond
)

var (
	productListReadSupervisor = readSupervisorStatus
	productListReadRepo       = readProductListRepo
	productListCurrentPair    = currentWorktreeReplayPair
	productListReadOutcome    = readPublicationOutcomeForPair
)

type productListRepoOverview struct {
	report       statusReport
	clients      int
	lastActivity time.Time
	unfinished   bool
	rewriting    bool
}

func collectProductListOverview(ctx context.Context) (productListData, productState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	roots, err := paths.Resolve()
	if err != nil {
		return productListData{}, productStateNeedsAction, err
	}
	registry, err := central.Load(roots)
	if err != nil {
		return productListData{}, productStateNeedsAction, err
	}
	workers := map[string]supervisor.WorkerStatus{}
	if status, ok := productListReadSupervisor(ctx, roots); ok {
		for _, worker := range status.Workers {
			workers[worker.RepositoryID] = worker
		}
	}

	now := time.Now()
	records := make([]central.RepoRecord, 0, len(registry.Repos))
	for _, record := range registry.Repos {
		if !record.LifecycleDisabled() {
			records = append(records, record)
		}
	}
	entries := make([]productListEntry, len(records))
	semaphore := make(chan struct{}, productListReadLimit)
	var wg sync.WaitGroup
	for index, record := range records {
		index, record := index, record
		wg.Add(1)
		go func() {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			readCtx, cancel := context.WithTimeout(ctx, productListReadTimeout)
			defer cancel()
			overview, readErr := productListReadRepo(readCtx, record, now)
			entries[index] = productListEntryFromOverview(record, workers[record.RepositoryID], overview, readErr)
		}()
	}
	wg.Wait()

	overall := productStateProtected
	for _, entry := range entries {
		overall = higherProductState(overall, entry.State)
	}
	sortProductListEntries(entries)
	return productListData{
		UpdatedAt: now.UTC().Format(time.RFC3339),
		Repos:     entries,
	}, overall, nil
}

func productListEntryFromOverview(
	record central.RepoRecord,
	worker supervisor.WorkerStatus,
	overview productListRepoOverview,
	readErr error,
) productListEntry {
	if record.RepositoryID == "" || record.WorktreeID == "" {
		activity := overview.lastActivity
		return productListEntry{
			Repo: record.Path, RepoHash: record.RepoHash, ActionRequired: true, State: productStateNeedsAction,
			OperationalState: "needs_attention", LastActivityAt: formatProductListActivity(activity),
			Summary:      "This repository needs the current ACD protection format.",
			NextAction:   productListTargetAction("Run `acd setup` to upgrade and enable protection.", record.Path),
			lastActivity: activity, UnfinishedWork: overview.unfinished,
		}
	}
	if readErr != nil {
		if productListReadTransient(readErr) && worker.State != "needs_action" {
			activity := overview.lastActivity
			stateName, operational := productStateWaiting, "refreshing"
			summary := "ACD is refreshing this repository's protection state."
			if worker.State == "starting" || worker.State == "backoff" {
				stateName, operational = productStatePublishing, "retrying"
				summary = "ACD is starting or retrying the background worker."
			}
			return productListEntry{
				Repo: record.Path, RepoHash: record.RepoHash, Enabled: true,
				State: stateName, WorkerState: worker.State,
				OperationalState: operational, ProtectionUnknown: true,
				PublicationProgress: publicationProgressReport{Phase: "protection_refresh"},
				PublicationOutcome:  publicationOutcome{PendingClassification: true, ReasonCode: "protection_read_pending"},
				LastActivityAt:      formatProductListActivity(activity),
				Summary:             summary, NextAction: "No action needed.",
				lastActivity: activity, UnfinishedWork: overview.unfinished,
			}
		}
		activity := overview.lastActivity
		return productListEntry{
			Repo: record.Path, RepoHash: record.RepoHash, Enabled: true, ActionRequired: true,
			State: productStateNeedsAction, WorkerState: worker.State,
			OperationalState: "needs_attention", LastActivityAt: formatProductListActivity(activity),
			Summary:      "ACD could not read this repository's protection state.",
			NextAction:   productListTargetAction("Run `acd doctor` for details.", record.Path),
			lastActivity: activity, UnfinishedWork: overview.unfinished,
		}
	}
	report := overview.report
	control := controlResult{
		OK: true, Command: "status", Repo: record.Path, Registered: true,
		Enabled: true, Actions: []string{}, NextAction: "No action needed.",
	}
	daemonAlive := report.Daemon == "running" && report.PID > 0 && !report.Stale
	applyControlStatusWithDaemonAlive(&control, report, daemonAlive)
	checkpointing := report.CaptureHealth.Error == "" && daemonAlive && report.CheckpointProtectionAvailable && !report.Protected &&
		report.PublicationProgress.Origin == "" &&
		(report.PublicationProgress.Phase == "" || report.PublicationProgress.Phase == "checkpointing") &&
		!productListHasIndependentAttention(report)
	if checkpointing {
		control.OK = true
		control.Health = controlHealthPublishing
		control.Summary = "ACD is checkpointing the latest observed changes."
		control.NextAction = "No action needed."
	}
	if worker.RepositoryID != "" {
		applySupervisorWorkerFailure(&control, worker)
		if report.CaptureHealth.Error == "" && (worker.State == "starting" || worker.State == "backoff") &&
			!productListHasIndependentAttention(report) {
			control.OK = true
			control.Health = controlHealthPublishing
			control.Summary = "ACD is starting or retrying the background worker."
			control.NextAction = "No action needed."
		}
	}
	envelope := envelopeFromControl(control)
	entryState := envelope.State
	actionRequired := entryState == productStateNeedsAction || entryState == productStateOff
	if actionRequired {
		entryState = productStateNeedsAction
	}
	operational := report.OperationalState
	if checkpointing {
		operational = "busy"
	} else if report.Paused && report.Pause != nil && report.Pause.Source != "rewind_grace" {
		operational = "paused"
	} else if report.Paused && report.Pause != nil && report.Pause.Source == "rewind_grace" {
		operational = "waiting"
	} else if report.Configuration.Configuration == "validating" {
		operational = "validating"
	}
	entry := productListEntry{
		Repo: record.Path, RepoHash: record.RepoHash, Enabled: control.Enabled, Protected: control.Protected,
		PublicationOutcome: control.PublicationOutcome,
		Published:          control.Published, ActionRequired: actionRequired, State: entryState,
		PendingEvents: control.PendingEvents, BlockedEvents: control.BlockedEvents,
		CheckpointID: control.CheckpointID, WorkerState: worker.State,
		OperationalState: operational, LastActivityAt: formatProductListActivity(overview.lastActivity),
		PublicationDrain:      report.PublicationDrain,
		CheckpointMaintenance: report.CheckpointMaintenance,
		UnfinishedWork:        overview.unfinished,
		CaptureHealth:         report.CaptureHealth,
		PublicationProgress:   report.PublicationProgress, Summary: control.Summary,
		Clients: overview.clients, LastCommitOID: report.LastCommitOID,
		lastActivity: overview.lastActivity,
	}
	if overview.rewriting {
		entry.OperationalState = "rewriting"
		entry.Summary = "ACD is applying a history rewrite."
	}
	if envelope.NextAction != nil {
		entry.NextAction = productListTargetAction(*envelope.NextAction, record.Path)
	}
	return entry
}

func productListReadTransient(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database table is locked") ||
		strings.Contains(message, "sqlite_busy") ||
		strings.Contains(message, "sqlite_locked")
}

func productListHasIndependentAttention(report statusReport) bool {
	manualPause := report.Paused && (report.Pause == nil || report.Pause.Source != "rewind_grace")
	return report.CheckpointRetentionOverBudget || report.CheckpointMaintenance.NeedsAction() ||
		report.checkpointNeedsAction ||
		(report.IntentV2.SchemaVersion > 0 && !report.CheckpointProtectionAvailable) ||
		manualPause || report.BackpressurePaused ||
		report.Configuration.Configuration == "needs_attention" ||
		report.PublicationDrain.Phase == state.PublicationDrainNeedsAction ||
		(report.PublicationProgress.Origin == "intent_recovery" &&
			report.PublicationProgress.Phase == "needs_action") ||
		report.Replay.State == "needs_attention" ||
		report.ActiveTerminalEvents > 0 || report.ActiveBarriers > 0
}

func readProductListRepo(ctx context.Context, record central.RepoRecord, now time.Time) (overview productListRepoOverview, err error) {
	requested := record
	if requested.StateDB == "" {
		return overview, errors.New("state.db missing")
	}
	overview.report, err = buildStatusReport(ctx, requested, now)
	if err != nil {
		return overview, err
	}
	r := overview.report
	overview.clients = len(r.Clients)
	overview.lastActivity = laterProductListActivity(overview.lastActivity, r.CaptureHealth.LastProgressTS)
	for _, client := range r.Clients {
		overview.lastActivity = laterProductListActivity(overview.lastActivity, float64(client.LastSeenTS))
	}
	if f := r.PublicationOutcome.Finish; f != nil {
		overview.lastActivity = laterProductListActivity(overview.lastActivity, f.UpdatedTS)
	}
	overview.unfinished = r.PublicationOutcome.PendingChanges > 0 || r.PublicationOutcome.WorktreeChanges > 0 || r.Busy || r.CaptureHealth.Error != ""
	return overview, nil
}

func readProductListProtection(ctx context.Context, conn *sql.DB, report *statusReport) error {
	report.CheckpointProtectionAvailable = true
	lookupInt := func(key string) int64 {
		value, ok, _ := metaLookup(ctx, conn, key)
		if !ok {
			return 0
		}
		parsed, _ := strconv.ParseInt(value, 10, 64)
		return parsed
	}
	report.ObservationEpoch = lookupInt(daemon.MetaKeyProtectionObservationEpoch)
	report.CoveredEpoch = lookupInt(daemon.MetaKeyProtectionCoveredEpoch)
	report.LatestCheckpointID, _, _ = metaLookup(ctx, conn, daemon.MetaKeyProtectionCheckpointID)
	fullPoll, _, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionFullPollTS)
	report.FullPollTS, _ = strconv.ParseFloat(fullPoll, 64)
	completeValue, _, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionComplete)
	var err error
	report.CheckpointMaintenance, err = readCheckpointMaintenance(ctx, conn)
	if err != nil {
		return err
	}
	report.CheckpointRetentionOverBudget = report.CheckpointMaintenance.State == "over_budget"

	var prepared, needsAction int
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(phase='prepared'),0),COALESCE(SUM(phase='needs_action'),0) FROM checkpoints`).Scan(&prepared, &needsAction); err != nil {
		return err
	}
	report.checkpointNeedsAction = needsAction > 0
	report.checkpointPrepared = prepared > 0
	unresolved, err := countUnresolvedCompletedCheckpoints(ctx, conn)
	if err != nil {
		return err
	}
	report.UnpublishedCheckpoints = unresolved
	report.Protected = strings.EqualFold(completeValue, "true") && report.LatestCheckpointID != "" &&
		report.ObservationEpoch == report.CoveredEpoch && prepared == 0 && needsAction == 0
	return nil
}

func readProductListClients(ctx context.Context, conn *sql.DB, now time.Time, ttl time.Duration) (int, float64, error) {
	rows, err := conn.QueryContext(ctx, `SELECT watch_pid,last_seen_ts FROM daemon_clients`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	cutoff := float64(now.UnixNano())/1e9 - ttl.Seconds()
	count, newest := 0, float64(0)
	for rows.Next() {
		var watchPID sql.NullInt64
		var seen float64
		if err := rows.Scan(&watchPID, &seen); err != nil {
			return 0, 0, err
		}
		if seen > newest {
			newest = seen
		}
		if seen >= cutoff && (!watchPID.Valid || watchPID.Int64 <= 0 || identity.Alive(int(watchPID.Int64))) {
			count++
		}
	}
	return count, newest, rows.Err()
}

func laterProductListActivity(current time.Time, unixSeconds float64) time.Time {
	if unixSeconds <= 0 {
		return current
	}
	candidate := time.Unix(0, int64(unixSeconds*float64(time.Second)))
	if candidate.After(current) {
		return candidate
	}
	return current
}

func formatProductListActivity(value time.Time) string {
	if value.IsZero() || value.Unix() <= 0 {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func sortProductListEntries(entries []productListEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].lastActivity.Equal(entries[j].lastActivity) {
			return entries[i].lastActivity.After(entries[j].lastActivity)
		}
		return entries[i].Repo < entries[j].Repo
	})
}

func productListPriority(entry productListEntry) int {
	if entry.ActionRequired {
		return 0
	}
	switch productListStatus(entry) {
	case "needs action":
		return 0
	case "working", "waiting", "stalled":
		return 1
	case "paused":
		return 3
	default:
		return 2
	}
}

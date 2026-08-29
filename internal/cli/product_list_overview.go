package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
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
)

type productListRepoOverview struct {
	report       statusReport
	clients      int
	lastActivity time.Time
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
		activity := time.Unix(record.LastSeenTS, 0)
		return productListEntry{
			Repo: record.Path, RepoHash: record.RepoHash, ActionRequired: true, State: productStateNeedsAction,
			OperationalState: "needs_attention", LastActivityAt: formatProductListActivity(activity),
			Summary:      "This repository needs the current ACD protection format.",
			NextAction:   productListTargetAction("Run `acd setup` to upgrade and enable protection.", record.Path),
			lastActivity: activity,
		}
	}
	if readErr != nil {
		if productListReadTransient(readErr) && worker.State != "needs_action" {
			activity := time.Unix(record.LastSeenTS, 0)
			stateName, operational := productStateProtected, "healthy_idle"
			summary := "ACD is refreshing this repository's protection state."
			if worker.State == "starting" || worker.State == "backoff" {
				stateName, operational = productStatePublishing, "retrying"
				summary = "ACD is starting or retrying the background worker."
			}
			return productListEntry{
				Repo: record.Path, RepoHash: record.RepoHash, Enabled: true,
				State: stateName, WorkerState: worker.State,
				OperationalState: operational, ProtectionUnknown: true,
				LastActivityAt: formatProductListActivity(activity),
				Summary:        summary, NextAction: "No action needed.",
				lastActivity: activity,
			}
		}
		activity := time.Unix(record.LastSeenTS, 0)
		return productListEntry{
			Repo: record.Path, RepoHash: record.RepoHash, Enabled: true, ActionRequired: true,
			State: productStateNeedsAction, WorkerState: worker.State,
			OperationalState: "needs_attention", LastActivityAt: formatProductListActivity(activity),
			Summary:      "ACD could not read this repository's protection state.",
			NextAction:   productListTargetAction("Run `acd doctor` for details.", record.Path),
			lastActivity: activity,
		}
	}
	report := overview.report
	control := controlResult{
		OK: true, Command: "status", Repo: record.Path, Registered: true,
		Enabled: true, Actions: []string{}, NextAction: "No action needed.",
	}
	daemonAlive := report.Daemon == "running" && report.PID > 0 && !report.Stale
	applyControlStatusWithDaemonAlive(&control, report, daemonAlive)
	checkpointing := daemonAlive && report.CheckpointProtectionAvailable && !report.Protected &&
		!productListHasIndependentAttention(report)
	if checkpointing {
		control.OK = true
		control.Health = controlHealthPublishing
		control.Summary = "ACD is checkpointing the latest observed changes."
		control.NextAction = "No action needed."
	}
	if worker.RepositoryID != "" {
		applySupervisorWorkerFailure(&control, worker)
		if (worker.State == "starting" || worker.State == "backoff") &&
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
		Published: control.Published, ActionRequired: actionRequired, State: entryState,
		PendingEvents: control.PendingEvents, BlockedEvents: control.BlockedEvents,
		CheckpointID: control.CheckpointID, WorkerState: worker.State,
		OperationalState: operational, LastActivityAt: formatProductListActivity(overview.lastActivity),
		PublicationDrain:    report.PublicationDrain,
		PublicationProgress: report.PublicationProgress, Summary: control.Summary,
		Clients: overview.clients, LastCommitOID: report.LastCommitOID,
		lastActivity: overview.lastActivity,
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
	return report.CheckpointRetentionOverBudget ||
		report.checkpointNeedsAction ||
		(report.IntentV2.SchemaVersion > 0 && !report.CheckpointProtectionAvailable) ||
		manualPause || report.BackpressurePaused ||
		report.Configuration.Configuration == "needs_attention" ||
		report.PublicationDrain.Phase == state.PublicationDrainNeedsAction ||
		report.Replay.State == "needs_attention" ||
		report.ActiveTerminalEvents > 0 || report.ActiveBarriers > 0
}

func readProductListRepo(ctx context.Context, record central.RepoRecord, now time.Time) (productListRepoOverview, error) {
	overview := productListRepoOverview{
		report:       statusReport{Repo: record.Path, RepoHash: record.RepoHash, Daemon: "stopped", Clients: []statusClient{}},
		lastActivity: time.Unix(record.LastSeenTS, 0),
	}
	if !fileExists(record.StateDB) {
		return overview, errors.New("state.db missing")
	}
	query := url.Values{}
	query.Add("mode", "ro")
	query.Add("_pragma", "query_only(ON)")
	query.Add("_pragma", "busy_timeout(250)")
	conn, err := sql.Open("sqlite", "file:"+record.StateDB+"?"+query.Encode())
	if err != nil {
		return overview, err
	}
	conn.SetMaxOpenConns(1)
	defer conn.Close()
	if err := conn.PingContext(ctx); err != nil {
		return overview, err
	}
	report := &overview.report
	var schemaVersion int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schemaVersion); err != nil {
		return overview, err
	}
	if schemaVersion > state.SchemaVersion {
		return overview, fmt.Errorf("state schema v%d is newer than supported v%d", schemaVersion, state.SchemaVersion)
	}
	if schemaVersion >= 20 {
		if err := readProductListProtection(ctx, conn, report); err != nil {
			return overview, err
		}
		var checkpointActivity float64
		if err := conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(COALESCE(completed_ts,created_ts)),0) FROM checkpoints`).Scan(&checkpointActivity); err != nil {
			return overview, err
		}
		overview.lastActivity = laterProductListActivity(overview.lastActivity, checkpointActivity)
	}
	var heartbeat float64
	var branchRef sql.NullString
	var generation sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT pid,mode,heartbeat_ts,branch_ref,branch_generation FROM daemon_state WHERE id=1`).Scan(
		&report.PID, &report.Daemon, &heartbeat, &branchRef, &generation)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return overview, err
	}
	if err == nil {
		report.HeartbeatTS = int64(heartbeat)
		if heartbeat > 0 {
			age := now.Sub(time.Unix(int64(heartbeat), 0))
			report.HeartbeatAgeSeconds = int64(age.Seconds())
			report.Stale = age > clientTTLForRepo(record.Path)
		}
	}
	currentBranchRef, currentBranchGeneration, hasCurrentPair, pairErr :=
		currentWorktreeReplayPair(ctx, conn, record.Path)
	if pairErr != nil {
		return overview, pairErr
	}
	if hasCurrentPair {
		report.BranchRef = currentBranchRef
		report.BranchGeneration = currentBranchGeneration
	}
	if schemaVersion >= 21 {
		report.PublicationDrain, err = readStatusPublicationDrain(
			ctx, conn, report.BranchRef, report.BranchGeneration)
		if err != nil {
			return overview, err
		}
		overview.lastActivity = laterProductListActivity(
			overview.lastActivity, report.PublicationDrain.LastProgressTS)
	}
	clients, _, err := readProductListClients(ctx, conn, now, clientTTLForRepo(record.Path))
	if err != nil {
		return overview, err
	}
	overview.clients = clients
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events WHERE state=?`, state.EventStatePending).Scan(&report.PendingEvents); err != nil {
		return overview, err
	}
	activeGeneration := report.BranchGeneration
	blockers, err := loadRecoveryBlockerCounts(ctx, conn, report.BranchRef, activeGeneration)
	if err != nil {
		return overview, err
	}
	report.BlockedConflicts = blockers.TotalBlockedConflicts
	report.ActiveBarriers = blockers.ActiveBlockedBarriersWithSuccessors
	report.ActiveTerminalEvents = blockers.ActiveTerminalEvents

	var lastCommitTS sql.NullFloat64
	var lastCommitOID sql.NullString
	if err := conn.QueryRowContext(ctx, `SELECT commit_oid,published_ts FROM capture_events WHERE commit_oid IS NOT NULL ORDER BY seq DESC LIMIT 1`).Scan(&lastCommitOID, &lastCommitTS); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return overview, err
	}
	if lastCommitOID.Valid {
		report.LastCommitOID = lastCommitOID.String
	}
	if lastCommitTS.Valid {
		report.LastCommitTS = int64(lastCommitTS.Float64)
		overview.lastActivity = laterProductListActivity(overview.lastActivity, lastCommitTS.Float64)
	}
	var newestCapture sql.NullFloat64
	if err := conn.QueryRowContext(ctx, `SELECT captured_ts FROM capture_events ORDER BY seq DESC LIMIT 1`).Scan(&newestCapture); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return overview, err
	}
	if newestCapture.Valid {
		overview.lastActivity = laterProductListActivity(overview.lastActivity, newestCapture.Float64)
	}
	if info, err := pauseInfoForRepo(ctx, conn, record.StateDB, now); err != nil {
		return overview, err
	} else if info != nil {
		report.Paused = true
		report.Pause = info
	}
	if value, ok, err := metaLookup(ctx, conn, "capture.backpressure_paused_at"); err != nil {
		return overview, err
	} else if ok {
		report.BackpressurePaused = true
		report.BackpressurePausedAt = value
	}
	if report.Configuration, err = loadConfigReadinessReport(ctx, conn, now); err != nil {
		return overview, err
	}
	if report.Replay, err = loadReplayObservabilityReport(ctx, conn); err != nil {
		return overview, err
	}
	if schemaVersion >= 15 {
		report.IntentV2.SchemaVersion = schemaVersion
	}
	strategy, strategyErr := ResolveEffectiveCommitStrategy(ctx, conn)
	if strategyErr != nil {
		return overview, strategyErr
	}
	report.IntentStrategy.Strategy = string(strategy)
	report.IntentStrategy.Active = strategy == "intent"
	envIntent := intentStrategyFromEnv()
	report.IntentStrategy.EffectiveProvider = envIntent.EffectiveProvider
	report.IntentStrategy.EffectiveModel = envIntent.EffectiveModel
	if value, ok, providerErr := metaLookup(ctx, conn, "ai.provider"); providerErr != nil {
		return overview, providerErr
	} else if ok && strings.TrimSpace(value) != "" {
		report.IntentStrategy.EffectiveProvider = strings.TrimSpace(value)
	}
	if value, ok, modelErr := metaLookup(ctx, conn, "ai.model"); modelErr != nil {
		return overview, modelErr
	} else if ok {
		report.IntentStrategy.EffectiveModel = strings.TrimSpace(value)
	}
	if report.IntentStrategy.Active && report.PendingEvents > 0 {
		plannerHealth, warning, healthErr := loadIntentPlannerHealth(ctx, conn)
		if healthErr != nil {
			return overview, healthErr
		}
		report.IntentStrategy.PlannerHealth = plannerHealth
		report.IntentStrategy.PlannerHealthWarning = warning
		lastWindow, windowErr := loadLastIntentPlannerWindowForPairSQL(
			ctx, conn, report.BranchRef, report.BranchGeneration)
		if windowErr != nil {
			return overview, windowErr
		}
		report.IntentStrategy.LastPlannerWindow = lastWindow
		if lastWindow != nil {
			report.IntentStrategy.ResolutionMode = lastWindow.ResolutionMode
		}
	}
	if wait, waitErr := loadListIntentWaitSummary(ctx, conn); waitErr != nil {
		return overview, waitErr
	} else if wait != nil {
		report.IntentStrategy.BatchWaitActive = true
		report.IntentStrategy.BatchWaitReason = wait.reason
		report.IntentStrategy.VisiblePendingEvents = wait.visiblePending
		report.IntentStrategy.MinPending = wait.minPending
		if wait.reason == "skipped_due_intent_settle_window" {
			report.IntentStrategy.SettleTriggerInSeconds = wait.waitSeconds
		} else {
			report.IntentStrategy.AgeTriggerInSeconds = wait.waitSeconds
		}
	}
	if schemaVersion >= 18 {
		report.SelfPublication.Available = true
		report.SelfPublication.Phase = "idle"
		if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(phase='prepared'),0),COALESCE(SUM(phase='git_applied'),0) FROM self_publications`).Scan(
			&report.SelfPublication.PreparedCount, &report.SelfPublication.GitAppliedCount); err != nil {
			return overview, err
		}
		if report.SelfPublication.PreparedCount > 0 || report.SelfPublication.GitAppliedCount > 0 {
			report.SelfPublication.Phase = "active"
		}
	}
	report.Busy = report.Daemon == "running" && !report.Stale &&
		(report.PendingEvents > 0 || report.SelfPublication.Phase == "active" ||
			(report.PublicationDrain.ID != "" && report.PublicationDrain.Phase != state.PublicationDrainCompleted && report.PublicationDrain.Phase != state.PublicationDrainNeedsAction) ||
			report.Configuration.Configuration == "validating")
	report.OperationalState = statusOperationalStateWithDaemonAlive(*report,
		report.Daemon == "running" && report.PID > 0 && !report.Stale)
	report.PublicationProgress, err = buildPublicationProgressReport(
		ctx, conn, *report, now)
	if err != nil {
		return overview, err
	}
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
	completeValue, _, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionComplete)
	retentionValue, _, _ := metaLookup(ctx, conn, daemon.MetaKeyProtectionRetentionOverBudget)
	report.CheckpointRetentionOverBudget = retentionValue == "true" || retentionValue == "needs_action"
	var prepared, needsAction int
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(phase='prepared'),0),COALESCE(SUM(phase='needs_action'),0) FROM checkpoints`).Scan(&prepared, &needsAction); err != nil {
		return err
	}
	report.checkpointNeedsAction = needsAction > 0
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoints cp WHERE cp.phase='completed' AND EXISTS (SELECT 1 FROM checkpoint_events ce JOIN capture_events e ON e.seq=ce.event_seq WHERE ce.checkpoint_id=cp.id AND e.state<>'published')`).Scan(&report.UnpublishedCheckpoints); err != nil {
		return err
	}
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
		left, right := productListPriority(entries[i]), productListPriority(entries[j])
		if left != right {
			return left < right
		}
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

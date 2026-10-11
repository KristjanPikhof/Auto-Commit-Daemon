package cli

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
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
			Summary:      "Upgrade ACD before protecting this repository.",
			NextAction:   productListTargetAction("Run `acd setup` to upgrade and enable protection.", record.Path),
			lastActivity: activity, UnfinishedWork: overview.unfinished,
		}
	}
	if readErr != nil {
		if productListReadTransient(readErr) && worker.State != "needs_action" {
			activity := overview.lastActivity
			stateName, operational := productStateWaiting, "refreshing"
			summary := "ACD is checking protection for this repository."
			if worker.State == "starting" || worker.State == "backoff" {
				stateName, operational = productStatePublishing, "retrying"
				summary = "ACD is starting background protection."
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
			Summary:      "ACD could not check protection for this repository.",
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
		(report.PublicationProgress.Phase == "" || report.PublicationProgress.Phase == "checkpointing") &&
		!productListHasIndependentAttention(report)
	if checkpointing {
		control.OK = true
		control.Health = controlHealthPublishing
		control.Summary = "ACD is saving the latest changes it checked."
		control.NextAction = "No action needed."
	}
	if worker.RepositoryID != "" {
		applySupervisorWorkerFailure(&control, worker)
		if report.CaptureHealth.Error == "" && (worker.State == "starting" || worker.State == "backoff") &&
			!productListHasIndependentAttention(report) {
			control.OK = true
			control.Health = controlHealthPublishing
			control.Summary = "ACD is starting background protection."
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

	}
	entry := productListEntry{
		Repo: record.Path, RepoHash: record.RepoHash, Enabled: control.Enabled, Protected: control.Protected,
		PublicationOutcome: control.PublicationOutcome,
		Published:          control.Published, ActionRequired: actionRequired, State: entryState,
		PendingEvents: control.PendingEvents, BlockedEvents: control.BlockedEvents,
		CheckpointID: control.CheckpointID, WorkerState: worker.State,
		OperationalState: operational, LastActivityAt: formatProductListActivity(overview.lastActivity),
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
	f := report.PublicationOutcome.Finish
	return report.CheckpointRetentionOverBudget || report.CheckpointMaintenance.NeedsAction() || report.checkpointNeedsAction || report.CaptureHealth.State == "blocked" || (f != nil && (f.State == state.FinishFailed || f.State == state.FinishNeedsAttention))
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
	overview.unfinished = r.PublicationOutcome.PendingChanges > 0 || r.PublicationOutcome.WorktreeChanges > 0 || r.Busy || r.CaptureHealth.Error != "" || r.PublicationOutcome.Legacy.State == "needs_attention"
	return overview, nil
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

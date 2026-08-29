package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

const (
	controlHealthHealthy        = "healthy"
	controlHealthWaiting        = "waiting"
	controlHealthPublishing     = "publishing"
	controlHealthDegraded       = "degraded"
	controlHealthNeedsAttention = "needs_attention"
	controlHealthOff            = "off"
	controlHealthNotRepo        = "not_a_repo"
)

// controlResult is the stable response shared by bare acd, acd on, and
// acd off. Keep fields non-optional so --json consumers receive the same
// shape for every outcome; Actions is initialized to an empty slice rather
// than null for the same reason.
type controlResult struct {
	OK                       bool                      `json:"ok"`
	Command                  string                    `json:"command"`
	Repo                     string                    `json:"repo"`
	Health                   string                    `json:"health"`
	Summary                  string                    `json:"summary"`
	NextAction               string                    `json:"next_action"`
	Registered               bool                      `json:"registered"`
	Enabled                  bool                      `json:"enabled"`
	Daemon                   string                    `json:"daemon"`
	DaemonPID                int                       `json:"daemon_pid"`
	PendingEvents            int                       `json:"pending_events"`
	BlockedEvents            int                       `json:"blocked_events"`
	Changed                  bool                      `json:"changed"`
	Actions                  []string                  `json:"actions"`
	StatePreserved           bool                      `json:"state_preserved"`
	Protected                bool                      `json:"protected"`
	Published                bool                      `json:"published"`
	Busy                     bool                      `json:"busy"`
	OperationalState         string                    `json:"operational_state"`
	WorktreeClean            bool                      `json:"worktree_clean"`
	AllChangesCommittedInGit bool                      `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD bool                      `json:"checkpoint_published_by_acd"`
	CheckpointID             string                    `json:"checkpoint_id,omitempty"`
	PublicationDrain         publicationDrainReport    `json:"publication_drain"`
	PublicationProgress      publicationProgressReport `json:"publication_progress"`
	RecoveryRequired         bool                      `json:"-"`
	CLIVersion               string                    `json:"cli_version,omitempty"`
	SupervisorVersion        string                    `json:"supervisor_version,omitempty"`
	SupervisorWorkerState    string                    `json:"supervisor_worker_state,omitempty"`
	SupervisorWorkerRestarts int                       `json:"supervisor_worker_restarts,omitempty"`
	SupervisorWorkerError    string                    `json:"supervisor_worker_error,omitempty"`
}

type controlRepoLookup struct {
	Worktree   git.Worktree
	Roots      paths.Roots
	Record     central.RepoRecord
	Registered bool
}

func newOnCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "on",
		Short: "Start protecting this repository",
		Long: `Start ACD protection for this repository.

ACD replaces any managed background worker for this repository, waits for the
new worker to be ready, and confirms that current changes have a durable
checkpoint. Running the command again is safe. Existing checkpoints are kept.

If ACD has not been installed or upgraded for checkpoint protection, run
acd setup first. Recovery that needs your approval is never applied silently.`,
		Example: `  acd on
  acd on --repo /path/to/repo
  acd on --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runControlOn(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
}

func newOffCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "off",
		Short: "Save a final checkpoint and stop protection",
		Long: `Save a final durable checkpoint, then stop ACD protection for this
repository.

Existing checkpoints and ACD state are kept, so you can turn protection on
again later. Running the command again is safe.

If ACD cannot confirm the final checkpoint, protection stays on and the output
explains what to do next. Use --force only when you accept stopping without a
confirmed current checkpoint.`,
		Example: `  acd off
  acd off --repo /path/to/repo
  acd off --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runControlOffWithForce(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut, force)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Stop even if ACD cannot confirm the final checkpoint")
	return cmd
}

// runControlStatus is the read-only default action for bare `acd`. It only
// reads Git, the central registry, and an existing state DB through the same
// read-only report used by `acd status`.
func runControlStatus(ctx context.Context, out io.Writer, repoFlag string, jsonOut bool) error {
	res, err := inspectControl(ctx, repoFlag)
	if err != nil {
		return err
	}
	if err := renderControl(out, res, jsonOut); err != nil {
		return err
	}
	if res.Health == controlHealthNeedsAttention || res.Health == controlHealthNotRepo {
		return actionRequiredError("needs_action", res.Summary)
	}
	return nil
}

func runControlOn(ctx context.Context, out io.Writer, repoFlag string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	lookup, err := loadControlRepo(ctx, repoFlag)
	if err != nil {
		return controlWorktreeError("on", repoFlag, err)
	}

	if err := ensureMutationSupervisor(ctx, lookup.Roots); err != nil {
		if strings.Contains(err.Error(), "run `acd setup`") {
			return actionRequiredError("setup_required", "acd on: "+err.Error())
		}
		return unavailableError("acd on: " + err.Error())
	}
	actions := make([]string, 0, 6)
	lookup, preparationActions, err := prepareControlRepository(ctx, lookup)
	if err != nil {
		if errors.Is(err, state.ErrSetupRequired) || strings.Contains(err.Error(), "run `acd setup`") {
			return actionRequiredError("setup_required", "acd on: "+err.Error())
		}
		return err
	}
	actions = append(actions, preparationActions...)
	if _, err := callSupervisor(ctx, lookup, "enable_repository", nil, 30*time.Second); err != nil {
		return fmt.Errorf("acd on: %w", err)
	}
	if _, err := callSupervisor(ctx, lookup, "restart_repository", nil, 30*time.Second); err != nil {
		return fmt.Errorf("acd on: restart worker: %w", err)
	}
	actions = append(actions, "restarted")
	if err := waitControlWorkerReady(ctx, lookup, 10*time.Second); err != nil {
		return err
	}
	if _, err := callSupervisor(ctx, lookup, "checkpoint_barrier", nil, supervisor.CheckpointBarrierTimeout); err != nil {
		return actionRequiredError("checkpoint_barrier_failed", fmt.Sprintf("acd on: initial checkpoint barrier failed: %v; run `acd doctor`", err))
	}
	actions = append(actions, "checkpointed")

	res, err := inspectControl(ctx, lookup.Worktree.Root)
	if err != nil {
		return err
	}
	if res.RecoveryRequired {
		recoveryErr := runFix(ctx, io.Discard, lookup.Worktree.Root, false, true, false, false, true)
		res, err = inspectControl(ctx, lookup.Worktree.Root)
		if err != nil {
			return err
		}
		if recoveryErr != nil {
			res.Command = "on"
			res.Changed = len(actions) > 0
			res.Actions = append(actions, "recovery_failed")
			res.StatePreserved = true
			if renderErr := renderControl(out, res, jsonOut); renderErr != nil {
				return renderErr
			}
			return actionRequiredError("recovery_failed", fmt.Sprintf("acd on: automatic exact-chain recovery failed: %v", recoveryErr))
		}
		actions = append(actions, "recovered")
	}
	res.Command = "on"
	res.Changed = len(actions) > 0
	res.Actions = actions
	res.StatePreserved = true
	if err := renderControl(out, res, jsonOut); err != nil {
		return err
	}
	if !res.OK {
		return actionRequiredError("needs_action", fmt.Sprintf("acd on: repository remains unhealthy: %s", res.Summary))
	}
	return nil
}

func waitControlWorkerReady(ctx context.Context, lookup controlRepoLookup, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := supervisor.DoWorker(ctx,
			supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID),
			supervisor.Request{
				Version: supervisor.ProtocolVersion,
				ID:      fmt.Sprintf("control-worker-ready-%d", time.Now().UnixNano()),
				Method:  "status", RepositoryID: lookup.Record.RepositoryID,
				WorktreeID: lookup.Record.WorktreeID,
			}, time.Second)
		if err == nil && response.OK && response.Version == supervisor.ProtocolVersion {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			message := "acd on: worker did not become ready"
			if worker, ok := readSupervisorWorkerStatus(ctx, lookup.Roots,
				lookup.Record.RepositoryID); ok && worker.LastError != "" {
				message += ": " + worker.LastError
			}
			return unavailableError(message + "; run `acd doctor`")
		case <-ticker.C:
		}
	}
}

func runControlOff(ctx context.Context, out io.Writer, repoFlag string, jsonOut bool) error {
	return runControlOffWithForce(ctx, out, repoFlag, jsonOut, false)
}

func runControlOffWithForce(ctx context.Context, out io.Writer, repoFlag string, jsonOut, force bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	lookup, err := loadControlRepo(ctx, repoFlag)
	if err != nil {
		return controlWorktreeError("off", repoFlag, err)
	}

	actions := make([]string, 0, 2)
	if !lookup.Registered {
		res, inspectErr := inspectControl(ctx, lookup.Worktree.Root)
		if inspectErr != nil {
			return inspectErr
		}
		res.Command = "off"
		return renderControl(out, res, jsonOut)
	}
	if lookup.Record.LifecycleDisabled() {
		if _, err := callSupervisor(ctx, lookup, "stop_repository", nil, 30*time.Second); err != nil {
			return fmt.Errorf("acd off: stop worker: %w", err)
		}
		res, inspectErr := inspectControl(ctx, lookup.Worktree.Root)
		if inspectErr != nil {
			return inspectErr
		}
		res.Command = "off"
		return renderControl(out, res, jsonOut)
	}
	if _, err := callSupervisor(ctx, lookup, "checkpoint_barrier", nil, supervisor.CheckpointBarrierTimeout); err != nil {
		if !force {
			return actionRequiredError("checkpoint_barrier_failed", fmt.Sprintf("acd off: final checkpoint barrier failed: %v; rerun with --force only if you accept unconfirmed protection", err))
		}
		actions = append(actions, "checkpoint_unconfirmed")
	} else {
		actions = append(actions, "checkpointed")
	}
	if _, err := callSupervisor(ctx, lookup, "disable_repository", nil, 30*time.Second); err != nil {
		return fmt.Errorf("acd off: %w", err)
	}
	actions = append(actions, "disabled")
	if _, err := callSupervisor(ctx, lookup, "stop_repository", nil, 30*time.Second); err != nil {
		return fmt.Errorf("acd off: stop worker: %w", err)
	}
	actions = append(actions, "stopped")

	res, err := inspectControl(ctx, lookup.Worktree.Root)
	if err != nil {
		return err
	}
	res.Command = "off"
	res.Changed = len(actions) > 0
	res.Actions = actions
	res.StatePreserved = true
	return renderControl(out, res, jsonOut)
}

func callSupervisor(ctx context.Context, lookup controlRepoLookup, method string, params json.RawMessage, timeout time.Duration) (supervisor.Response, error) {
	if err := ensureMutationSupervisor(ctx, lookup.Roots); err != nil {
		return supervisor.Response{}, unavailableError(err.Error())
	}
	request := supervisor.Request{Version: supervisor.ProtocolVersion,
		ID: fmt.Sprintf("control-%s-%d", method, time.Now().UnixNano()), Method: method,
		RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID,
		DeadlineMS: time.Now().Add(timeout).UnixMilli(), Params: params}
	response, err := (supervisor.Client{SocketPath: lookup.Roots.SupervisorSocketPath(), Timeout: timeout}).Do(ctx, request)
	if err != nil {
		return response, unavailableError(err.Error())
	}
	if response.Error != nil {
		if response.Error.Retryable {
			return response, unavailableError(response.Error.Message)
		}
		return response, &CommandError{Code: response.Error.Code, Message: response.Error.Message, Exit: ExitInternal}
	}
	return response, nil
}

// registerControlOptOut records lifecycle intent without opening state.db or
// requiring an attached branch. This lets users turn ACD off safely during a
// detached-HEAD inspection; acd on will create state when the repo is attached
// again and ready to start.
func registerControlOptOut(lookup controlRepoLookup) error {
	return central.WithLock(lookup.Roots, func(reg *central.Registry) error {
		_, err := reg.RegisterResolvedRepo(lookup.Worktree, "", time.Now().Unix())
		return err
	})
}

func inspectControl(ctx context.Context, repoFlag string) (controlResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	base := controlResult{
		OK:         true,
		Command:    "status",
		Daemon:     "unknown",
		Actions:    []string{},
		NextAction: "No action needed.",
	}
	lookup, err := loadControlRepo(ctx, repoFlag)
	if err != nil {
		if errors.Is(err, git.ErrNotWorktree) {
			base.OK = false
			base.Repo = controlRequestedPath(repoFlag)
			base.Health = controlHealthNotRepo
			base.Summary = "The current directory is not inside a Git worktree."
			base.NextAction = "Run `acd` from inside a Git worktree."
			return base, nil
		}
		return controlResult{}, err
	}
	base.Repo = lookup.Worktree.Root
	if !lookup.Registered {
		base.Health = controlHealthOff
		base.Daemon = "stopped"
		base.Summary = "ACD is not enabled for this repository."
		base.NextAction = "Run `acd on` to enable it."
		return base, nil
	}

	base.Registered = true
	base.StatePreserved = true
	if lookup.Record.RepositoryID == "" || lookup.Record.WorktreeID == "" {
		base.Health = controlHealthOff
		base.Daemon = "stopped"
		base.Summary = "This repository needs the current ACD protection format."
		base.NextAction = "Run `acd setup` to upgrade and enable protection."
		return base, nil
	}
	if lookup.Record.LifecycleDisabled() {
		applyDisabledControlTruth(ctx, &base, lookup)
		base.Health = controlHealthOff
		base.Daemon = "stopped"
		base.Summary = "ACD protection is turned off for this repository."
		base.NextAction = "Run `acd on` to enable it."
		return base, nil
	}
	base.Enabled = true
	if !fileExists(lookup.Record.StateDB) {
		base.OK = false
		base.Health = controlHealthNeedsAttention
		base.Daemon = "stopped"
		base.Summary = "ACD cannot find this repository's protection database."
		base.NextAction = "Run `acd on` to recreate state and start ACD."
		return base, nil
	}

	status, err := buildStatusReport(ctx, lookup.Record, time.Now())
	if err != nil {
		base.OK = false
		base.Health = controlHealthNeedsAttention
		base.Summary = "ACD could not read the current repository health."
		base.NextAction = "Run `acd doctor` for details."
		return base, nil
	}
	applyControlStatus(&base, status)
	if lookup.Record.RepositoryID != "" {
		if supervisorStatus, ok := readSupervisorStatus(ctx, lookup.Roots); ok {
			base.CLIVersion = version.String()
			base.SupervisorVersion = supervisorStatus.Version
			if supervisorStatus.Version != "" && supervisorStatus.Version != base.CLIVersion {
				base.Health = controlHealthNeedsAttention
				base.Summary = fmt.Sprintf("The CLI version %s does not match supervisor version %s.", base.CLIVersion, supervisorStatus.Version)
				base.NextAction = "Run `acd setup` to install the matching managed binary."
			}
			for _, worker := range supervisorStatus.Workers {
				if worker.RepositoryID != lookup.Record.RepositoryID {
					continue
				}
				base.SupervisorWorkerState = worker.State
				base.SupervisorWorkerRestarts = worker.Restarts
				base.SupervisorWorkerError = worker.LastError
				applySupervisorWorkerFailure(&base, worker)
				break
			}
		}
	}
	return base, nil
}

func applyDisabledControlTruth(
	ctx context.Context,
	result *controlResult,
	lookup controlRepoLookup,
) {
	if fileExists(lookup.Record.StateDB) {
		if status, err := buildStatusReport(ctx, lookup.Record, time.Now()); err == nil {
			result.CheckpointPublishedByACD = status.CheckpointPublishedByACD
			result.CheckpointID = status.LatestCheckpointID
		}
	}
	body, err := git.Run(ctx, git.RunOpts{Dir: lookup.Worktree.Root},
		"status", "--porcelain=v1", "--untracked-files=all")
	if err == nil {
		result.WorktreeClean = len(body) == 0
		result.AllChangesCommittedInGit = result.WorktreeClean
		result.Published = result.WorktreeClean
	}
}

func readSupervisorStatus(ctx context.Context, roots paths.Roots) (supervisor.Status, bool) {
	response, err := (supervisor.Client{
		SocketPath: roots.SupervisorSocketPath(), Timeout: 500 * time.Millisecond,
	}).Do(ctx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("status-%d", time.Now().UnixNano()),
		Method: "status", DeadlineMS: time.Now().Add(500 * time.Millisecond).UnixMilli(),
	})
	if err != nil || response.Error != nil {
		return supervisor.Status{}, false
	}
	status, err := decodeProductData[supervisor.Status](response.Data)
	return status, err == nil
}

func readSupervisorWorkerStatus(
	ctx context.Context,
	roots paths.Roots,
	repositoryID string,
) (supervisor.WorkerStatus, bool) {
	status, ok := readSupervisorStatus(ctx, roots)
	if !ok {
		return supervisor.WorkerStatus{}, false
	}
	for _, worker := range status.Workers {
		if worker.RepositoryID == repositoryID {
			return worker, true
		}
	}
	return supervisor.WorkerStatus{}, false
}

func applySupervisorWorkerFailure(result *controlResult, worker supervisor.WorkerStatus) {
	if worker.State != "needs_action" || worker.LastError == "" {
		return
	}
	result.OK = false
	result.Health = controlHealthNeedsAttention
	switch {
	case strings.Contains(worker.LastError, "checkpoint-first setup required"):
		result.Summary = "ACD could not start because its state database needs a safe schema upgrade."
		result.NextAction = "Run `acd on`; it will back up and upgrade compatible state, then start a new worker."
	case strings.Contains(worker.LastError, "acquire repository ownership"):
		result.Summary = "ACD could not start because an older worker still owns this repository."
		result.NextAction = "Run `acd on` to stop the managed worker and start a fresh one."
	default:
		result.Summary = "ACD tried to restart the worker, but the startup error still needs attention."
		result.NextAction = "Run `acd on` once. If it still fails, run `acd support logs --lines 100`."
	}
}

func loadControlRepo(ctx context.Context, repoFlag string) (controlRepoLookup, error) {
	wt, err := git.ResolveWorktree(ctx, repoFlag)
	if err != nil {
		return controlRepoLookup{}, err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return controlRepoLookup{}, fmt.Errorf("resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return controlRepoLookup{}, fmt.Errorf("load registry: %w", err)
	}
	rec, ok := findRepo(reg, wt.Root, state.DBPathFromGitDir(wt.GitDir))
	return controlRepoLookup{
		Worktree:   wt,
		Roots:      roots,
		Record:     rec,
		Registered: ok,
	}, nil
}

func controlDaemonRunning(ctx context.Context, rec central.RepoRecord) bool {
	if rec.StateDB == "" || !fileExists(rec.StateDB) {
		return false
	}
	report, err := buildStatusReport(ctx, rec, time.Now())
	return err == nil && report.Daemon == "running" && !report.Stale &&
		report.PID > 0 && identity.AliveContext(ctx, report.PID)
}

func applyControlStatus(res *controlResult, status statusReport) {
	applyControlStatusWithDaemonAlive(res, status, status.PID > 0 && identity.Alive(status.PID))
}

func applyControlStatusWithDaemonAlive(res *controlResult, status statusReport, daemonAlive bool) {
	manualPause := status.Paused && (status.Pause == nil || status.Pause.Source != "rewind_grace")
	res.Daemon = status.Daemon
	res.DaemonPID = status.PID
	res.PendingEvents = status.PendingEvents
	res.BlockedEvents = status.BlockedConflicts
	res.Protected = status.Protected
	res.Published = status.Protected && status.UnpublishedCheckpoints == 0 && status.PendingEvents == 0
	res.Busy = status.Busy
	res.OperationalState = status.OperationalState
	res.WorktreeClean = status.WorktreeClean
	res.AllChangesCommittedInGit = status.AllChangesCommittedInGit
	res.CheckpointPublishedByACD = status.CheckpointPublishedByACD
	res.CheckpointID = status.LatestCheckpointID
	res.PublicationDrain = status.PublicationDrain
	res.PublicationProgress = status.PublicationProgress
	res.RecoveryRequired = status.Replay.State == "needs_attention" ||
		status.ActiveTerminalEvents > 0 || status.ActiveBarriers > 0 ||
		(status.PublicationProgress.Origin == "intent_recovery" &&
			status.PublicationProgress.Phase == "needs_action")

	switch {
	case status.CheckpointRetentionOverBudget:
		res.Health = controlHealthNeedsAttention
		res.Summary = "Protected checkpoints use more storage than the configured limit. No protected data was deleted."
		res.NextAction = "Run `acd repo gc` to review storage use and safe cleanup."
	case status.IntentV2.SchemaVersion > 0 && !status.CheckpointProtectionAvailable:
		res.Health = controlHealthNeedsAttention
		res.Summary = "This repository uses an older ACD protection format."
		res.NextAction = "Run `acd setup` to upgrade it safely."
	case status.Stale:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD background protection has stopped responding."
		res.NextAction = "Run `acd on` to restart it."
	case status.Daemon != "running" || !daemonAlive:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD is enabled, but background protection is not running."
		res.NextAction = "Run `acd on` to start it."
	case status.PublicationProgress.Origin == "intent_recovery" &&
		status.PublicationProgress.Phase == "needs_action":
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = intentRecoveryVerificationAttentionSummary
		res.NextAction = intentRecoveryVerificationAttentionNext
	case manualPause:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD protection and publication are paused."
		res.NextAction = "Run `acd status` to review the pause reason."
	case status.BackpressurePaused:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD protection is paused because its protected storage is full."
		res.NextAction = "Run `acd doctor` before clearing backpressure."
	case status.Configuration.Configuration == "needs_attention":
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "The saved configuration did not pass validation. ACD is still protecting changes."
		res.NextAction = "Run `acd config edit` to retry validation or select another experience."
	case status.PublicationDrain.Phase == state.PublicationDrainNeedsAction &&
		(status.PublicationDrain.LastError == "publication_drain_runtime_contract_unavailable" ||
			status.PublicationDrain.LastError == "publication_drain_environment_runtime_changed"):
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD can no longer reconstruct the exact runtime needed to resume this commit-all run. Your captured work remains protected."
		res.NextAction = "Run `acd fix --force --dry-run`, review the archive-only recovery plan, then run `acd fix --force --yes`."
	case status.PublicationDrain.Phase == state.PublicationDrainNeedsAction:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "The current commit-all run stopped at a safety check. Your work remains protected."
		res.NextAction = "Run `acd doctor` to see what blocked publication."
	case status.Replay.State == "needs_attention":
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "A safety block stopped Git publication, but checkpoint protection is still active."
		res.NextAction = "Run `acd support recover --dry-run`, review the plan, then run `acd support recover --yes`."
	case status.PublicationProgress.Phase == "verifying":
		res.Health = controlHealthPublishing
		if status.PublicationProgress.Origin == "intent_recovery" {
			res.Summary = "ACD is verifying the semantic group for automatic recovery. The recovery target and your work remain protected."
		} else {
			res.Summary = "ACD is verifying the semantic group. Your work remains protected."
		}
		res.NextAction = "No action needed. ACD will continue when verification finishes."
	case status.PublicationProgress.Phase == "provider_call":
		res.Health = controlHealthPublishing
		if status.PublicationProgress.Origin == "intent_recovery" {
			res.Summary = "ACD is waiting for the current Intent provider response before continuing automatic recovery. The recovery target and your work remain protected."
		} else {
			res.Summary = "ACD is waiting for the current Intent provider response. Your work remains protected."
		}
		res.NextAction = "No action needed. ACD will continue when the provider responds."
	case status.PendingEvents > 0 &&
		status.IntentStrategy.ResolutionMode == "waiting_message_rewrite":
		res.Health = controlHealthWaiting
		res.Summary = "ACD is waiting for the Intent provider to write a semantic commit message. Your work remains protected."
		res.NextAction = "No action needed. ACD will retry the provider automatically."
	case status.PublicationProgress.Origin == "intent_recovery" &&
		status.PublicationProgress.Phase == "provider_wait":
		res.Health = controlHealthWaiting
		res.Summary = "ACD is waiting for the Intent provider before continuing automatic recovery. The recovery target and your work remain protected."
		if status.PublicationProgress.WaitRemainingSeconds > 0 {
			res.NextAction = fmt.Sprintf(
				"No action needed. ACD will retry in %s.",
				formatDurationCompact(time.Duration(
					status.PublicationProgress.WaitRemainingSeconds)*time.Second))
		} else {
			res.NextAction = "No action needed. ACD will retry the provider automatically."
		}
	case status.PublicationProgress.Phase == "stalled":
		res.Health = controlHealthDegraded
		if status.PublicationProgress.Origin == "intent_recovery" {
			recoveryActivity := "Automatic Intent recovery is active"
			if status.PublicationProgress.TemporaryLocalFallback {
				recoveryActivity = "ACD is widening a verified Intent group"
			}
			res.Summary = fmt.Sprintf(
				"%s, but its target has not moved for %s. The worker is responsive and your work remains protected.",
				recoveryActivity, formatDurationCompact(time.Duration(
					status.PublicationProgress.LastProgressAgeSeconds)*time.Second))
			res.NextAction = "No action needed yet. ACD will keep replanning the exact recovery target; run `acd doctor` if this persists."
		} else {
			res.Summary = fmt.Sprintf(
				"The worker is responsive, but the publication queue has not moved for %s. Your work remains protected.",
				formatDurationCompact(time.Duration(
					status.PublicationProgress.LastProgressAgeSeconds)*time.Second))
			res.NextAction = "No action needed yet. ACD will start bounded recovery automatically; run `acd doctor` if this persists."
		}
	case status.PublicationProgress.Origin == "intent_recovery":
		res.Health = controlHealthPublishing
		if status.PublicationProgress.Phase == "local_fallback" {
			res.Summary = "ACD is widening a verified Intent group to recover the protected target."
		} else {
			res.Summary = "ACD is automatically rebuilding semantic commit groups for the protected changes."
		}
		res.NextAction = "No action needed. ACD will keep the exact recovery target protected and continue automatically."
	case status.PublicationDrain.Phase == state.PublicationDrainEventFallback &&
		status.PublicationDrain.FallbackMode == "semantic_replan":
		res.Health = controlHealthPublishing
		res.Summary = "ACD is replanning the remaining protected changes by intent."
		res.NextAction = "No action needed. If planning stalls, ACD will publish one local group and try intent again."
	case status.PublicationDrain.Phase == state.PublicationDrainEventFallback:
		res.Health = controlHealthPublishing
		res.Summary = "ACD is publishing one safe local group to unblock intent planning."
		res.NextAction = "No action needed. ACD will return to intent planning after this group."
	case status.PublicationDrain.Phase == state.PublicationDrainNormalizing ||
		status.PublicationDrain.Phase == state.PublicationDrainCheckpointing:
		res.Health = controlHealthPublishing
		res.Summary = "ACD is rebuilding the publication plan after a recoverable problem."
		res.NextAction = "No action needed. Recovery continues automatically."
	case status.PublicationDrain.Phase == state.PublicationDrainSemantic:
		res.Health = controlHealthPublishing
		res.Summary = "ACD is planning commits for the protected checkpoint."
		res.NextAction = "No action needed. If planning stalls, ACD switches to safe local groups automatically."
	case status.ActiveTerminalEvents > 0 || status.ActiveBarriers > 0:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "A blocked publication needs recovery on the active branch."
		res.NextAction = "Run `acd support recover --dry-run`, review the plan, then run `acd support recover --yes`."
	case status.CheckpointProtectionAvailable && !status.Protected && status.Busy:
		res.Health = controlHealthWaiting
		res.Summary = "ACD is scanning recent changes and completing their protection checkpoint."
		res.NextAction = "No action needed; checkpoint protection completes automatically."
	case status.CheckpointProtectionAvailable && !status.Protected:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "The latest observed changes are not yet covered by a completed checkpoint."
		res.NextAction = "Run `acd doctor` if protection does not complete after the current scan."
	case status.SelfPublication.GitAppliedCount > 0:
		res.Health = controlHealthPublishing
		res.Summary = "Current changes are checkpointed while ACD publishes Git commits."
		res.NextAction = "No action needed."
	case status.Paused && status.Pause != nil && status.Pause.Source == "rewind_grace":
		res.Health = controlHealthWaiting
		res.Summary = "ACD is briefly paused because Git history changed."
		if status.Pause.RemainingSeconds > 0 {
			res.Summary = fmt.Sprintf("ACD is briefly paused because Git history changed (%s remaining).",
				formatDurationCompact(time.Duration(status.Pause.RemainingSeconds)*time.Second))
		}
		res.NextAction = "No action needed; capture and replay resume automatically."
	case status.Configuration.Configuration == "validating":
		res.Health = controlHealthWaiting
		res.Summary = "ACD capture is active while configuration validation runs."
		res.NextAction = "No action needed; commit publishing activates after validation passes."
	case status.PendingEvents == 0 && status.IntentStrategy.LastPlannerWindow != nil &&
		status.IntentStrategy.LastPlannerWindow.ResolutionMode == "evidence_partition":
		res.Health = controlHealthHealthy
		res.Summary = "ACD safely grouped and published the last batch using local dependency evidence."
		res.NextAction = "No action needed."
	case status.IntentStrategy.PlannerHealth != nil &&
		(status.IntentStrategy.PlannerHealth.State == daemon.IntentPlannerCircuitOpen ||
			status.IntentStrategy.PlannerHealth.State == daemon.IntentPlannerCircuitHalfOpen):
		res.Health = controlHealthDegraded
		res.Summary = "The Intent provider is temporarily unavailable, so semantic commit messages are waiting. Your work remains protected."
		if status.IntentStrategy.PlannerHealth.State == daemon.IntentPlannerCircuitHalfOpen {
			res.NextAction = "No immediate action needed; ACD is running the automatic provider probe. Run `acd doctor` for details."
		} else {
			res.NextAction = "No immediate action needed; ACD will probe the provider automatically after cooldown. Run `acd doctor` for details."
		}
	case status.IntentStrategy.PlannerHealthWarning != "":
		res.Health = controlHealthDegraded
		res.Summary = "ACD is running, but it could not read saved AI provider health information."
		res.NextAction = "Run `acd doctor` for the safe metadata warning."
	case status.Replay.State == "degraded":
		res.Health = controlHealthDegraded
		res.Summary = fmt.Sprintf("Git publication is retrying after %d consecutive error(s). Checkpoint protection remains active.", status.Replay.ErrorRepeatCount)
		res.NextAction = "No action needed; ACD will retry automatically. Run `acd doctor` if the retrying state persists."
	case status.CaptureErrors > 0 || status.IntentStrategy.PlannerErrorRateRecentWarn:
		res.Health = controlHealthDegraded
		res.Summary = "ACD is running with recoverable capture or planning errors."
		res.NextAction = "No action needed; ACD will retry automatically. Run `acd doctor` if the degraded state persists."
	case status.PendingEvents > 0 && status.IntentStrategy.Active && status.IntentStrategy.BatchWaitActive:
		res.Health = controlHealthWaiting
		res.Summary = fmt.Sprintf("ACD is waiting to group %d protected change(s) into a useful commit.", status.PendingEvents)
		res.NextAction = "No action needed. ACD will publish when the batch is ready."
	case status.PendingEvents > 0:
		res.Health = controlHealthHealthy
		res.Summary = fmt.Sprintf("ACD is publishing %d protected change(s).", status.PendingEvents)
		res.NextAction = "No action needed."
	default:
		res.Health = controlHealthHealthy
		res.Summary = "ACD is enabled and running normally."
		res.NextAction = "No action needed."
	}
}

func controlWorktreeError(command, repoFlag string, err error) error {
	if errors.Is(err, git.ErrNotWorktree) {
		return fmt.Errorf("acd %s: repo %q is not inside a Git worktree: %w", command, repoFlag, err)
	}
	return fmt.Errorf("acd %s: %w", command, err)
}

func controlRequestedPath(repoFlag string) string {
	if repoFlag == "" {
		if cwd, err := os.Getwd(); err == nil {
			return cwd
		}
		return "."
	}
	if abs, err := filepath.Abs(repoFlag); err == nil {
		return abs
	}
	return repoFlag
}

func renderControl(out io.Writer, res controlResult, jsonOut bool) error {
	return renderProductEnvelope(out, envelopeFromControl(res), jsonOut)
}

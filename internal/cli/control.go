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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
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
	CaptureHealth            state.CaptureHealth `json:"capture_health"`
	PublicationOutcome       publicationOutcome  `json:"publication_outcome"`
	OK                       bool                `json:"ok"`
	Command                  string              `json:"command"`
	Repo                     string              `json:"repo"`
	Health                   string              `json:"health"`
	Summary                  string              `json:"summary"`
	NextAction               string              `json:"next_action"`
	Registered               bool                `json:"registered"`
	Enabled                  bool                `json:"enabled"`
	Daemon                   string              `json:"daemon"`
	DaemonPID                int                 `json:"daemon_pid"`
	PendingEvents            int                 `json:"pending_events"`
	BlockedEvents            int                 `json:"blocked_events"`
	Changed                  bool                `json:"changed"`
	Actions                  []string            `json:"actions"`
	StatePreserved           bool                `json:"state_preserved"`
	Protected                bool                `json:"protected"`
	Published                bool                `json:"published"`
	Busy                     bool                `json:"busy"`
	OperationalState         string              `json:"operational_state"`
	WorktreeClean            bool                `json:"worktree_clean"`
	AllChangesCommittedInGit bool                `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD bool                `json:"checkpoint_published_by_acd"`
	CheckpointID             string              `json:"checkpoint_id,omitempty"`

	CheckpointMaintenance    checkpoint.MaintenanceStatus `json:"checkpoint_maintenance"`
	PublicationProgress      publicationProgressReport    `json:"publication_progress"`
	RecoveryRequired         bool                         `json:"-"`
	CLIVersion               string                       `json:"cli_version,omitempty"`
	SupervisorVersion        string                       `json:"supervisor_version,omitempty"`
	SupervisorWorkerState    string                       `json:"supervisor_worker_state,omitempty"`
	SupervisorWorkerRestarts int                          `json:"supervisor_worker_restarts,omitempty"`
	SupervisorWorkerError    string                       `json:"supervisor_worker_error,omitempty"`
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
		Long: `Start checkpoint protection for this repository.

First use initializes the local runtime with offline defaults. ACD starts the
worker and confirms a durable checkpoint of current eligible changes.
Running the command again is safe. Branch commits are created by acd finish.

Existing installations that need an upgrade direct you to acd setup.`,
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
	if err := bootstrapControlSetup(ctx, lookup); err != nil {
		return err
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
	if controlWorkerNeedsRestart(ctx, lookup) {
		if _, err := callSupervisor(ctx, lookup, "restart_repository", nil, 30*time.Second); err != nil {
			return fmt.Errorf("acd on: restart worker: %w", err)
		}
		actions = append(actions, "restarted")
	}
	progress := out
	if jsonOut {
		progress = io.Discard
	}
	if err := waitControlWorkerReady(ctx, lookup, supervisor.CheckpointBarrierTimeout, progress); err != nil {
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

// Reuse a responsive worker unless it must load a missing linked worktree.
func controlWorkerNeedsRestart(ctx context.Context, lookup controlRepoLookup) bool {
	worker, ok := readSupervisorWorkerStatus(ctx, lookup.Roots, lookup.Record.RepositoryID)
	if !ok {
		return false
	}
	if worker.State == "needs_action" {
		return true
	}
	if !controlWorkerReady(ctx, lookup, worker) {
		return false
	}
	response, err := supervisor.DoWorker(ctx, supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID), supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "control-worktree-status", Method: "status",
		RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID,
	}, time.Second)
	return err == nil && response.Error != nil && response.Error.Code == "worktree_not_found"
}

func controlWorkerReady(ctx context.Context, lookup controlRepoLookup, worker supervisor.WorkerStatus) bool {
	if worker.PID <= 0 {
		return false
	}
	response, err := supervisor.DoWorker(ctx,
		supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID),
		supervisor.Request{Version: supervisor.ProtocolVersion, ID: "control-worker-ready",
			Method: "status", RepositoryID: lookup.Record.RepositoryID}, time.Second)
	if err != nil || !response.OK || response.Error != nil || response.Version != supervisor.ProtocolVersion {
		return false
	}
	ready, err := decodeProductData[supervisor.WorkerReadiness](response.Data)
	return err == nil && ready.Ready && ready.PID == worker.PID && ready.RepositoryID == worker.RepositoryID
}

func waitControlWorkerReady(ctx context.Context, lookup controlRepoLookup, timeout time.Duration, progress io.Writer) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	announced := false
	for {
		worker, ok := readSupervisorWorkerStatus(waitCtx, lookup.Roots, lookup.Record.RepositoryID)
		if ok && controlWorkerReady(waitCtx, lookup, worker) {
			return nil
		}
		if !announced {
			fmt.Fprintln(progress, "Waiting for the repository worker to finish startup and protect this worktree…")
			announced = true
		}
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			message := "acd on: worker did not become ready"
			if ok && worker.LastError != "" {
				message += ": " + worker.LastError
			}
			return unavailableError(message + "; run `acd doctor`")
		case <-ticker.C:
		}
	}
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
		if fileExists(lookup.Record.StateDB) {
			if status, err := buildStatusReport(ctx, lookup.Record, time.Now()); err == nil {
				applyFinishControlStatus(&base, status, false)
			}
		}
		if count, err := productWorktreeChangeCount(ctx, lookup.Worktree.Root); err == nil {
			base.WorktreeClean = count == 0
			base.AllChangesCommittedInGit = base.WorktreeClean
			base.PublicationOutcome.WorktreeChanges = count
		}
		base.Health = controlHealthOff
		base.Daemon = "stopped"
		base.OK = true
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
		result.NextAction = "Run `acd setup` to back up and upgrade state before starting protection. Preserved work remains saved."
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

func applyControlStatus(res *controlResult, status statusReport) {
	applyControlStatusWithDaemonAlive(res, status, status.PID > 0 && identity.Alive(status.PID))
}

func applyControlStatusWithDaemonAlive(res *controlResult, status statusReport, daemonAlive bool) {
	applyFinishControlStatus(res, status, daemonAlive)
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

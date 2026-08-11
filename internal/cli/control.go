package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	OK                       bool     `json:"ok"`
	Command                  string   `json:"command"`
	Repo                     string   `json:"repo"`
	Health                   string   `json:"health"`
	Summary                  string   `json:"summary"`
	NextAction               string   `json:"next_action"`
	Registered               bool     `json:"registered"`
	Enabled                  bool     `json:"enabled"`
	Daemon                   string   `json:"daemon"`
	DaemonPID                int      `json:"daemon_pid"`
	PendingEvents            int      `json:"pending_events"`
	BlockedEvents            int      `json:"blocked_events"`
	Changed                  bool     `json:"changed"`
	Actions                  []string `json:"actions"`
	StatePreserved           bool     `json:"state_preserved"`
	Protected                bool     `json:"protected"`
	Published                bool     `json:"published"`
	Busy                     bool     `json:"busy"`
	OperationalState         string   `json:"operational_state"`
	WorktreeClean            bool     `json:"worktree_clean"`
	AllChangesCommittedInGit bool     `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD bool     `json:"checkpoint_published_by_acd"`
	CheckpointID             string   `json:"checkpoint_id,omitempty"`
	RecoveryRequired         bool     `json:"-"`
	CLIVersion               string   `json:"cli_version,omitempty"`
	SupervisorVersion        string   `json:"supervisor_version,omitempty"`
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
		Short: "Enable checkpoint protection for this repository",
		Long: `Enable checkpoint protection for the current repository.

The command is idempotent and asks the user supervisor to reconcile the
repository's worker. It applies safe exact-chain recovery automatically after
the initial checkpoint. Archive-only recovery still requires explicit consent.
Run acd setup first when the checkpoint-first cutover has not been completed.
Existing checkpoint history is preserved.`,
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
		Short: "Take a final checkpoint and disable protection",
		Long: `Complete a final durable checkpoint barrier, then disable protection.

The command is idempotent and preserves checkpoint history and repository
state. If the final checkpoint cannot be confirmed, ACD remains enabled and
returns needs_action unless --force is supplied.`,
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
	cmd.Flags().BoolVar(&force, "force", false, "Disable even when the final checkpoint cannot be confirmed")
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

	actions := make([]string, 0, 3)
	if !lookup.Registered {
		return actionRequiredError("setup_required", "acd on: repository is not configured; run `acd setup`")
	}
	if lookup.Record.RepositoryID == "" || lookup.Record.WorktreeID == "" {
		return actionRequiredError("setup_required", "acd on: repository requires `acd setup` checkpoint cutover")
	}
	changed := lookup.Record.LifecycleDisabled()
	if _, err := callSupervisor(ctx, lookup, "enable_repository", nil, 30*time.Second); err != nil {
		return fmt.Errorf("acd on: %w", err)
	}
	if changed {
		actions = append(actions, "enabled")
	} else {
		actions = append(actions, "verified")
	}
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
			return unavailableError("acd on: worker did not become ready; run `acd doctor`")
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
		base.Summary = "This repository has not completed the checkpoint-first cutover."
		base.NextAction = "Run `acd setup` to upgrade and enable protection."
		return base, nil
	}
	if lookup.Record.LifecycleDisabled() {
		base.Health = controlHealthOff
		base.Daemon = "stopped"
		base.Summary = "ACD is durably disabled for this repository."
		base.NextAction = "Run `acd on` to enable it."
		return base, nil
	}
	base.Enabled = true
	if !fileExists(lookup.Record.StateDB) {
		base.OK = false
		base.Health = controlHealthNeedsAttention
		base.Daemon = "stopped"
		base.Summary = "The registered ACD state database is missing."
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
		response, supervisorErr := (supervisor.Client{SocketPath: lookup.Roots.SupervisorSocketPath(), Timeout: 500 * time.Millisecond}).Do(ctx,
			supervisor.Request{Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("status-%d", time.Now().UnixNano()), Method: "status", DeadlineMS: time.Now().Add(500 * time.Millisecond).UnixMilli()})
		if supervisorErr == nil && response.Error == nil {
			supervisorStatus, decodeErr := decodeProductData[supervisor.Status](response.Data)
			if decodeErr == nil {
				base.CLIVersion = version.String()
				base.SupervisorVersion = supervisorStatus.Version
				if supervisorStatus.Version != "" && supervisorStatus.Version != base.CLIVersion {
					base.Health = controlHealthNeedsAttention
					base.Summary = fmt.Sprintf("The CLI version %s does not match supervisor version %s.", base.CLIVersion, supervisorStatus.Version)
					base.NextAction = "Run `acd setup` to install the matching managed binary."
				}
			}
		}
	}
	return base, nil
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

	switch {
	case status.CheckpointRetentionOverBudget:
		res.Health = controlHealthNeedsAttention
		res.Summary = "Protected checkpoint content exceeds the configured soft budget; no protected data was dropped."
		res.NextAction = "Run `acd repo gc` to review retention and storage usage."
	case status.IntentV2.SchemaVersion > 0 && !status.CheckpointProtectionAvailable:
		res.Health = controlHealthNeedsAttention
		res.Summary = "This repository still uses the v19 protection ledger."
		res.NextAction = "Run `acd setup` to perform the checkpoint-first cutover."
	case status.Stale:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD background protection has stopped responding."
		res.NextAction = "Run `acd on` to restart it."
	case status.Daemon != "running" || status.PID <= 0 || !identity.Alive(status.PID):
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD is enabled, but background protection is not running."
		res.NextAction = "Run `acd on` to start it."
	case manualPause:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD protection and publication are paused."
		res.NextAction = "Run `acd status` to review the pause reason."
	case status.BackpressurePaused:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD protection is paused because durable storage is full."
		res.NextAction = "Run `acd doctor` before clearing backpressure."
	case status.Configuration.Configuration == "needs_attention":
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "Configuration validation needs attention; capture remains active."
		res.NextAction = "Run `acd config edit` to retry validation or select another experience."
	case status.Replay.State == "needs_attention":
		res.OK = false
		res.RecoveryRequired = true
		res.Health = controlHealthNeedsAttention
		res.Summary = "A durable block has stopped Git publication; this blocked publication needs recovery while checkpoint protection remains active."
		res.NextAction = "Run `acd support recover --dry-run`, then `acd support recover --yes` to apply the displayed exact-chain recovery."
	case status.ActiveTerminalEvents > 0 || status.ActiveBarriers > 0:
		res.OK = false
		res.RecoveryRequired = true
		res.Health = controlHealthNeedsAttention
		res.Summary = "A blocked publication needs recovery on the active branch."
		res.NextAction = "Run `acd support recover --dry-run`, then `acd support recover --yes` to apply the displayed exact-chain recovery."
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
		res.Summary = "ACD rewind safety grace is active."
		if status.Pause.RemainingSeconds > 0 {
			res.Summary = fmt.Sprintf("ACD rewind safety grace is active (%s remaining).",
				formatDurationCompact(time.Duration(status.Pause.RemainingSeconds)*time.Second))
		}
		res.NextAction = "No action needed; capture and replay resume automatically."
	case status.Configuration.Configuration == "validating":
		res.Health = controlHealthWaiting
		res.Summary = "ACD capture is active while configuration validation runs."
		res.NextAction = "No action needed; commit publishing activates after validation passes."
	case status.IntentStrategy.PlannerHealth != nil &&
		(status.IntentStrategy.PlannerHealth.State == daemon.IntentPlannerCircuitOpen ||
			status.IntentStrategy.PlannerHealth.State == daemon.IntentPlannerCircuitHalfOpen):
		res.Health = controlHealthDegraded
		res.Summary = "The intent planner is degraded; deterministic fallback is keeping replay moving."
		if status.IntentStrategy.PlannerHealth.State == daemon.IntentPlannerCircuitHalfOpen {
			res.NextAction = "No immediate action needed; ACD is running the automatic provider probe. Run `acd doctor` for details."
		} else {
			res.NextAction = "No immediate action needed; ACD will probe the provider automatically after cooldown. Run `acd doctor` for details."
		}
	case status.IntentStrategy.PlannerHealthWarning != "":
		res.Health = controlHealthDegraded
		res.Summary = "ACD is running, but persisted intent planner health metadata could not be read safely."
		res.NextAction = "Run `acd doctor` for the safe metadata warning."
	case status.Replay.State == "degraded":
		res.Health = controlHealthDegraded
		res.Summary = fmt.Sprintf("Replay is retrying after %d consecutive error(s); checkpoint protection remains active.", status.Replay.ErrorRepeatCount)
		res.NextAction = "No action needed; ACD will retry automatically. Run `acd doctor` if the retrying state persists."
	case status.CaptureErrors > 0 || status.IntentStrategy.PlannerErrorRateRecentWarn:
		res.Health = controlHealthDegraded
		res.Summary = "ACD is running with recoverable errors or deterministic fallback."
		res.NextAction = "No action needed; ACD will retry automatically. Run `acd doctor` if the degraded state persists."
	case status.PendingEvents > 0 && status.IntentStrategy.Active && status.IntentStrategy.BatchWaitActive:
		res.Health = controlHealthWaiting
		res.Summary = fmt.Sprintf("Intent mode is waiting normally with %d pending event(s).", status.PendingEvents)
		res.NextAction = "No action needed; ACD will publish at the configured batch boundary."
	case status.PendingEvents > 0:
		res.Health = controlHealthHealthy
		res.Summary = fmt.Sprintf("ACD is running and draining %d pending event(s).", status.PendingEvents)
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

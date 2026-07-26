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
)

const (
	controlHealthHealthy        = "healthy"
	controlHealthWaiting        = "waiting"
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
	OK             bool     `json:"ok"`
	Command        string   `json:"command"`
	Repo           string   `json:"repo"`
	Health         string   `json:"health"`
	Summary        string   `json:"summary"`
	NextAction     string   `json:"next_action"`
	Registered     bool     `json:"registered"`
	Enabled        bool     `json:"enabled"`
	Daemon         string   `json:"daemon"`
	DaemonPID      int      `json:"daemon_pid"`
	PendingEvents  int      `json:"pending_events"`
	BlockedEvents  int      `json:"blocked_events"`
	Changed        bool     `json:"changed"`
	Actions        []string `json:"actions"`
	StatePreserved bool     `json:"state_preserved"`
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
		Short: "Enable ACD and ensure its daemon is running for this repo",
		Long: `Put the current repository into ACD's enabled desired state.

The command is idempotent: it registers an unknown repo, enables a disabled
repo, and starts or refreshes its daemon. Existing hook commands and sessions
keep their current behavior. State is preserved throughout.`,
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
	return &cobra.Command{
		Use:   "off",
		Short: "Durably disable ACD for this repo while preserving state",
		Long: `Put the current repository into ACD's disabled desired state.

The command is idempotent: it records an opt-out even for a previously unknown
repo, stops a live daemon, clears start caches, and preserves .git/acd state.
Harness hooks then skip this repo until acd on is run.`,
		Example: `  acd off
  acd off --repo /path/to/repo
  acd off --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runControlOff(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
}

// runControlStatus is the read-only default action for bare `acd`. It only
// reads Git, the central registry, and an existing state DB through the same
// read-only report used by `acd status`.
func runControlStatus(ctx context.Context, out io.Writer, repoFlag string, jsonOut bool) error {
	res, err := inspectControl(ctx, repoFlag)
	if err != nil {
		return err
	}
	return renderControl(out, res, jsonOut)
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
		if err := runRepoInit(ctx, io.Discard, lookup.Worktree.Root, true); err != nil {
			return fmt.Errorf("acd on: register repo: %w", err)
		}
		actions = append(actions, "registered")
		lookup, err = loadControlRepo(ctx, lookup.Worktree.Root)
		if err != nil || !lookup.Registered {
			if err == nil {
				err = errors.New("registry row missing after registration")
			}
			return fmt.Errorf("acd on: reload registered repo: %w", err)
		}
	}

	target := central.RepoRemovalTarget{
		Path:    lookup.Worktree.Root,
		StateDB: state.DBPathFromGitDir(lookup.Worktree.GitDir),
	}
	if lookup.Record.LifecycleDisabled() {
		life, err := applyRepoEnable(ctx, lookup.Roots, target)
		if err != nil {
			return fmt.Errorf("acd on: enable repo: %w", err)
		}
		if life.Updated {
			actions = append(actions, "enabled")
		}
		lookup.Record = life.Record
	}

	wasRunning := controlDaemonRunning(ctx, lookup.Record)
	if err := runStart(ctx, io.Discard, lookup.Worktree.Root, "", "", 0, false); err != nil {
		return fmt.Errorf("acd on: ensure daemon: %w", err)
	}
	if !wasRunning {
		actions = append(actions, "started")
	}

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
		return fmt.Errorf("acd on: repository remains unhealthy: %s", res.Summary)
	}
	return nil
}

func runControlOff(ctx context.Context, out io.Writer, repoFlag string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	lookup, err := loadControlRepo(ctx, repoFlag)
	if err != nil {
		return controlWorktreeError("off", repoFlag, err)
	}

	actions := make([]string, 0, 3)
	if !lookup.Registered {
		// A durable opt-out needs a registry row; otherwise the next harness
		// start would rediscover and enable the repo again.
		if err := registerControlOptOut(lookup); err != nil {
			return fmt.Errorf("acd off: register opt-out: %w", err)
		}
		actions = append(actions, "registered")
		lookup, err = loadControlRepo(ctx, lookup.Worktree.Root)
		if err != nil || !lookup.Registered {
			if err == nil {
				err = errors.New("registry row missing after registration")
			}
			return fmt.Errorf("acd off: reload registered repo: %w", err)
		}
	}

	target := central.RepoRemovalTarget{
		Path:    lookup.Worktree.Root,
		StateDB: state.DBPathFromGitDir(lookup.Worktree.GitDir),
	}
	life, err := applyRepoDisable(ctx, lookup.Roots, target)
	if err != nil {
		return fmt.Errorf("acd off: disable repo: %w", err)
	}
	if life.Updated {
		actions = append(actions, "disabled")
	}
	if life.Stopped != nil && life.Stopped.Stopped {
		actions = append(actions, "stopped")
	}

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
		base.NextAction = "Run `acd diagnose` for details."
		return base, nil
	}
	applyControlStatus(&base, status)
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
	res.Daemon = status.Daemon
	res.DaemonPID = status.PID
	res.PendingEvents = status.PendingEvents
	res.BlockedEvents = status.BlockedConflicts

	switch {
	case status.Stale:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "The ACD daemon heartbeat is stale."
		res.NextAction = "Run `acd on` to restart it."
	case status.Daemon != "running" || status.PID <= 0 || !identity.Alive(status.PID):
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD is enabled, but its daemon process is not running."
		res.NextAction = "Run `acd on` to start it."
	case status.Paused && (status.Pause == nil || status.Pause.Source != "rewind_grace"):
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD capture and replay are paused."
		res.NextAction = "Run `acd status` to review the pause reason."
	case status.BackpressurePaused:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "ACD capture is paused by durable backpressure."
		res.NextAction = "Run `acd diagnose` before clearing backpressure."
	case status.Configuration.Configuration == "needs_attention":
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "Configuration validation needs attention; capture remains active."
		res.NextAction = "Run `acd configure` to retry validation or select another experience."
	case status.ActiveTerminalEvents > 0 || status.ActiveBarriers > 0:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "A terminal replay event needs recovery on the active branch."
		res.NextAction = "Run `acd diagnose` to inspect the blocker."
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
			res.NextAction = "No immediate action needed; ACD is running the automatic provider probe. Run `acd diagnose` for details."
		} else {
			res.NextAction = "No immediate action needed; ACD will probe the provider automatically after cooldown. Run `acd diagnose` for details."
		}
	case status.IntentStrategy.PlannerHealthWarning != "":
		res.Health = controlHealthDegraded
		res.Summary = "ACD is running, but persisted intent planner health metadata could not be read safely."
		res.NextAction = "Run `acd diagnose` for the safe metadata warning."
	case status.CaptureErrors > 0 || status.IntentStrategy.PlannerErrorRateRecentWarn:
		res.Health = controlHealthDegraded
		res.Summary = "ACD is running with recoverable errors or deterministic fallback."
		res.NextAction = "Run `acd diagnose` if the degraded state persists."
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
	if res.Actions == nil {
		res.Actions = []string{}
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if res.Command != "status" {
		change := "no changes"
		if len(res.Actions) > 0 {
			change = strings.Join(res.Actions, ", ")
		}
		fmt.Fprintf(out, "ACD %s: %s\n", res.Command, change)
	}
	fmt.Fprintf(out, "Health: %s\n", strings.ReplaceAll(res.Health, "_", " "))
	if res.Repo != "" {
		fmt.Fprintf(out, "Repo: %s\n", res.Repo)
	}
	fmt.Fprintf(out, "Summary: %s\n", res.Summary)
	fmt.Fprintf(out, "Next: %s\n", res.NextAction)
	return nil
}

package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type repoInitResult struct {
	Repo       string `json:"repo"`
	RepoHash   string `json:"repo_hash"`
	StateDB    string `json:"state_db"`
	Inserted   bool   `json:"inserted"`
	Refreshed  bool   `json:"refreshed"`
	BranchRef  string `json:"branch_ref"`
	ConfigPath string `json:"config_path,omitempty"`
}

type repoListEntry struct {
	central.RepoRecord
	Safety           central.RepoRemovalSafety `json:"safety"`
	Status           string                    `json:"status"`
	Daemon           string                    `json:"daemon"`
	PID              int                       `json:"pid,omitempty"`
	Clients          int                       `json:"clients"`
	PendingEvents    int                       `json:"pending_events"`
	BlockedConflicts int                       `json:"blocked_conflicts"`
}

type repoRemoveResult struct {
	Target             central.RepoRemovalTarget `json:"target"`
	DryRun             bool                      `json:"dry_run"`
	Removed            bool                      `json:"removed"`
	NotFound           bool                      `json:"not_found"`
	PurgeState         bool                      `json:"purge_state"`
	StatePreserved     bool                      `json:"state_preserved"`
	StatePurged        bool                      `json:"state_purged"`
	StartCachesCleared bool                      `json:"start_caches_cleared"`
	RemovedRecord      *central.RepoRecord       `json:"removed_record,omitempty"`
	Safety             central.RepoRemovalSafety `json:"safety"`
	Stopped            *stopRepoResult           `json:"stopped,omitempty"`
}

type repoLifecycleCommandResult struct {
	Target             central.RepoRemovalTarget `json:"target"`
	Repo               string                    `json:"repo,omitempty"`
	StateDB            string                    `json:"state_db,omitempty"`
	Action             string                    `json:"action"`
	Updated            bool                      `json:"updated"`
	NotFound           bool                      `json:"not_found"`
	StatePreserved     bool                      `json:"state_preserved"`
	StartCachesCleared bool                      `json:"start_caches_cleared,omitempty"`
	Stopped            *stopRepoResult           `json:"stopped,omitempty"`
	Record             central.RepoRecord        `json:"record,omitempty"`
}

type repoManageMode int

const (
	repoManageCompact repoManageMode = iota
	repoManageVerbose
)

var repoRemoveStopOneRepo = stopOneRepo
var repoDisableStopOneRepo = stopOneRepo

func newRepoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "repo",
		Short: "Manage explicit repo registration lifecycle",
		Long: `Manage explicit acd repository registration without starting normal capture and replay workflows.

Autodiscovery is enabled by default, so normal harness hooks still create repo state automatically. Use repo init to prepare the current Git worktree when autodiscovery is disabled, repo disable to stop and disable a registered repo without deleting state, repo enable to re-enable a disabled repo, repo list to inspect every registry row, and repo remove to preview or remove a repo registration.

Disable implicit repo registration with repo_lifecycle.autodiscovery in ~/.config/acd/config.json or override one shell with ACD_REPO_AUTODISCOVERY=disabled.`,
		Example: `  acd repo init
  acd repo init --json
  acd repo disable --repo /path/to/repo
  acd repo enable --repo /path/to/repo --json
  acd repo manage
  acd repo list --json
  acd repo remove --dry-run
  acd repo remove --yes
  acd repo remove --yes --purge-state`,
	}
	cmd.AddCommand(
		newRepoInitCmd(),
		newRepoDisableCmd(),
		newRepoEnableCmd(),
		newRepoManageCmd(),
		newRepoListCmd(),
		newRepoRemoveCmd(),
	)
	return cmd
}

func newRepoInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize acd state and registry for the current repo",
		Long: `Initialize acd state and central registry metadata for the resolved Git worktree.

This command refuses detached HEAD, opens or creates .git/acd/state.db, and records the repo in the central registry. It does not start the daemon.`,
		Example: `  acd repo init
  acd repo init --repo /path/to/repo
  acd repo init --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repoFlag, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runRepoInit(cmd.Context(), cmd.OutOrStdout(), repoFlag, jsonOut)
		},
	}
}

func newRepoDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Disable a registered repo without deleting state",
		Long: `Disable a registered repo in the central registry without deleting .git/acd/state.db.

The command stops a live repo daemon, clears start caches, and preserves state so the repo can be enabled again later. It does not create a registry row for unknown repos.`,
		Example: `  acd repo disable --repo /path/to/repo
  acd repo disable --repo /path/to/repo --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repoFlag, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runRepoDisable(cmd.Context(), cmd.OutOrStdout(), repoFlag, jsonOut)
		},
	}
}

func newRepoEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Enable a registered repo without starting its daemon",
		Long: `Enable a registered repo in the central registry without starting its daemon.

The command clears the disabled lifecycle state and preserves .git/acd/state.db. It does not create a registry row for unknown repos.`,
		Example: `  acd repo enable --repo /path/to/repo
  acd repo enable --repo /path/to/repo --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repoFlag, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runRepoEnable(cmd.Context(), cmd.OutOrStdout(), repoFlag, jsonOut)
		},
	}
}

func newRepoManageCmd() *cobra.Command {
	var verbose bool
	cmd := &cobra.Command{
		Use:   "manage",
		Short: "Open the interactive repo lifecycle manager",
		Long: `Open a line-oriented repo lifecycle manager for registered repos.

The manager starts in compact mode by default. Use t N to toggle a repo,
e N to enable, d N to disable, r to refresh, v to switch compact and
verbose views, and q to exit.`,
		Example: `  acd repo manage
  acd repo manage --verbose
  acd list --interactive`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRepoManageWithInput(cmd.Context(), cmd.OutOrStdout(), cmd.InOrStdin(), verbose)
		},
	}
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Start in verbose audit mode")
	return cmd
}

func newRepoListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List every repo registry row for management",
		Long:  `List every repo in the central registry, including stopped, missing, and state-db-missing rows that daemon-focused acd list may hide.`,
		Example: `  acd repo list
  acd repo list --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runRepoList(cmd.Context(), cmd.OutOrStdout(), jsonOut)
		},
	}
}

func runRepoManageWithInput(ctx context.Context, out io.Writer, in io.Reader, verbose bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd repo manage: resolve paths: %w", err)
	}
	mode := repoManageCompact
	if verbose {
		mode = repoManageVerbose
	}
	reader := bufio.NewReader(in)
	for {
		entries, err := loadRepoManagementEntries(ctx, roots)
		if err != nil {
			return err
		}
		if err := renderRepoManage(out, entries, mode); err != nil {
			return err
		}
		fmt.Fprint(out, "repo manage> ")
		line, readErr := reader.ReadString('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("acd repo manage: read command: %w", readErr)
		}
		cmd := strings.TrimSpace(line)
		if cmd == "" {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			continue
		}
		quit, err := handleRepoManageCommand(ctx, roots, out, cmd, entries, &mode)
		if err != nil {
			fmt.Fprintf(out, "Invalid command: %v\n", err)
		}
		if quit || errors.Is(readErr, io.EOF) {
			return nil
		}
	}
}

func loadRepoManagementEntries(ctx context.Context, roots paths.Roots) ([]repoListEntry, error) {
	reg, err := central.Load(roots)
	if err != nil {
		return nil, fmt.Errorf("acd repo manage: load registry: %w", err)
	}
	return collectRepoManagementEntries(ctx, reg.Repos), nil
}

func handleRepoManageCommand(ctx context.Context, roots paths.Roots, out io.Writer, command string, entries []repoListEntry, mode *repoManageMode) (bool, error) {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return false, nil
	}
	switch strings.ToLower(fields[0]) {
	case "q", "quit":
		fmt.Fprintln(out, "acd repo manage: done")
		return true, nil
	case "r", "refresh":
		fmt.Fprintln(out, "acd repo manage: refreshed")
		return false, nil
	case "v", "verbose":
		if *mode == repoManageCompact {
			*mode = repoManageVerbose
			fmt.Fprintln(out, "acd repo manage: verbose")
		} else {
			*mode = repoManageCompact
			fmt.Fprintln(out, "acd repo manage: compact")
		}
		return false, nil
	case "t", "toggle", "e", "enable", "d", "disable":
		if len(fields) != 2 {
			return false, fmt.Errorf("usage: %s N", fields[0])
		}
		idx, err := parseRepoManageIndex(fields[1], len(entries))
		if err != nil {
			return false, err
		}
		entry := entries[idx]
		disable := fields[0] == "d" || fields[0] == "disable" ||
			(fields[0] == "t" || fields[0] == "toggle") && !entry.LifecycleDisabled()
		if err := applyRepoManageLifecycle(ctx, roots, out, entry, disable); err != nil {
			return false, err
		}
		return false, nil
	default:
		return false, fmt.Errorf("commands: t N, e N, d N, r, v, q")
	}
}

func parseRepoManageIndex(input string, count int) (int, error) {
	n, err := strconv.Atoi(input)
	if err != nil || n < 1 || n > count {
		return 0, fmt.Errorf("choose a number from 1 to %d", count)
	}
	return n - 1, nil
}

func applyRepoManageLifecycle(ctx context.Context, roots paths.Roots, out io.Writer, entry repoListEntry, disable bool) error {
	target := central.RepoRemovalTarget{Path: entry.Path, StateDB: entry.StateDB}
	if disable {
		res, err := applyRepoDisable(ctx, roots, target)
		if err != nil {
			return err
		}
		return renderRepoLifecycleCommand(out, res, false)
	}
	res, err := applyRepoEnable(ctx, roots, target)
	if err != nil {
		return err
	}
	return renderRepoLifecycleCommand(out, res, false)
}

func newRepoRemoveCmd() *cobra.Command {
	var dryRun bool
	var yes bool
	var purgeState bool

	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Preview or remove the current repo registration",
		Long: `Preview or remove the current repo from the central registry.

Without --yes, bare acd repo remove starts an interactive registry manager. Use --dry-run or --json for a non-mutating preview. Pass --yes to remove the target repo registration and clear start caches. State is preserved unless --purge-state is also confirmed interactively or supplied with --yes.`,
		Example: `  acd repo remove
  acd repo remove --repo /path/to/repo
  acd repo remove --dry-run
  acd repo remove --yes
  acd repo remove --yes --purge-state
  acd repo remove --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repoFlag, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runRepoRemoveWithInput(cmd.Context(), cmd.OutOrStdout(), cmd.InOrStdin(), repoFlag, dryRun, yes, purgeState, jsonOut)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview removal without mutating registry or state")
	cmd.Flags().BoolVar(&yes, "yes", false, "Apply the removal")
	cmd.Flags().BoolVar(&purgeState, "purge-state", false, "Delete .git/acd state after removing the registry row; requires --yes or interactive confirmation")
	return cmd
}

func runRepoInit(ctx context.Context, out io.Writer, repoFlag string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	wt, err := resolveRepoWorktree(ctx, repoFlag, "repo init")
	if err != nil {
		return err
	}
	branchRef, err := repoBranchRef(ctx, wt.Root, "repo init")
	if err != nil {
		return err
	}
	dbPath := state.DBPathFromGitDir(wt.GitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd repo init: open state.db: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("acd repo init: close state.db: %w", err)
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd repo init: resolve paths: %w", err)
	}
	var regResult central.RepoRegistrationResult
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		var err error
		regResult, err = reg.RegisterResolvedRepo(wt, "", time.Now().Unix())
		return err
	}); err != nil {
		return fmt.Errorf("acd repo init: update registry: %w", err)
	}
	res := repoInitResult{
		Repo:       regResult.Record.Path,
		RepoHash:   regResult.Record.RepoHash,
		StateDB:    regResult.Record.StateDB,
		Inserted:   regResult.Inserted,
		Refreshed:  regResult.Refreshed,
		BranchRef:  branchRef,
		ConfigPath: roots.ConfigPath(),
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if res.Inserted {
		fmt.Fprintf(out, "acd repo init: registered %s\n", res.Repo)
	} else {
		fmt.Fprintf(out, "acd repo init: already registered %s\n", res.Repo)
	}
	fmt.Fprintf(out, "state: %s\n", res.StateDB)
	fmt.Fprintf(out, "config: %s\n", res.ConfigPath)
	return nil
}

func runRepoList(ctx context.Context, out io.Writer, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd repo list: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return fmt.Errorf("acd repo list: load registry: %w", err)
	}
	entries := collectRepoManagementEntries(ctx, reg.Repos)
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(struct {
			Repos []repoListEntry `json:"repos"`
		}{Repos: entries})
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "REPO\tDAEMON\tCLIENTS\tPENDING\tBLOCKED\tSTATUS")
	for _, entry := range entries {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%s\n",
			homeShort(entry.Path),
			repoEntryDaemon(entry),
			entry.Clients,
			entry.PendingEvents,
			entry.BlockedConflicts,
			entry.Status)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("acd repo list: flush: %w", err)
	}
	return nil
}

func renderRepoManage(out io.Writer, entries []repoListEntry, mode repoManageMode) error {
	if mode == repoManageVerbose {
		return renderRepoManageVerbose(out, entries)
	}
	return renderRepoManageCompact(out, entries)
}

func renderRepoManageCompact(out io.Writer, entries []repoListEntry) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "N\tSTATE\tREPO\tDAEMON\tPEND\tBLK\tSTATUS")
	for i, entry := range entries {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%d\t%s\n",
			i+1,
			entry.LifecycleStateName(),
			homeShort(entry.Path),
			repoEntryDaemon(entry),
			entry.PendingEvents,
			entry.BlockedConflicts,
			entry.Status)
	}
	return tw.Flush()
}

func renderRepoManageVerbose(out io.Writer, entries []repoListEntry) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "N\tSTATE\tREPO\tDAEMON\tCLIENTS\tPENDING\tBLOCKED\tSTATE_DB\tLAST_SEEN\tHARNESSES\tSTATUS")
	for i, entry := range entries {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%d\t%s\t%s\n",
			i+1,
			entry.LifecycleStateName(),
			homeShort(entry.Path),
			repoEntryDaemon(entry),
			entry.Clients,
			entry.PendingEvents,
			entry.BlockedConflicts,
			entry.StateDB,
			entry.LastSeenTS,
			strings.Join(entry.Harnesses, ","),
			entry.Status)
	}
	return tw.Flush()
}

func runRepoDisable(ctx context.Context, out io.Writer, repoFlag string, jsonOut bool) error {
	return runRepoLifecycleCommand(ctx, out, repoFlag, true, jsonOut)
}

func runRepoEnable(ctx context.Context, out io.Writer, repoFlag string, jsonOut bool) error {
	return runRepoLifecycleCommand(ctx, out, repoFlag, false, jsonOut)
}

func runRepoLifecycleCommand(ctx context.Context, out io.Writer, repoFlag string, disable bool, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	action := "enable"
	if disable {
		action = "disable"
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd repo %s: resolve paths: %w", action, err)
	}
	target, err := repoRemovalTargetForCommand(ctx, repoFlag, "repo "+action)
	if err != nil {
		return err
	}
	reg, err := central.Load(roots)
	if err != nil {
		return fmt.Errorf("acd repo %s: load registry: %w", action, err)
	}
	rec, ok := reg.FindRepo(target.Path, target.StateDB)
	if !ok {
		return repoLifecycleUnknownTargetError(action, target)
	}
	_ = rec
	var res repoLifecycleCommandResult
	if disable {
		res, err = applyRepoDisable(ctx, roots, target)
	} else {
		res, err = applyRepoEnable(ctx, roots, target)
	}
	if err != nil {
		return err
	}
	return renderRepoLifecycleCommand(out, res, jsonOut)
}

func applyRepoDisable(ctx context.Context, roots paths.Roots, target central.RepoRemovalTarget) (repoLifecycleCommandResult, error) {
	return applyRepoLifecycle(ctx, roots, target, true)
}

func applyRepoEnable(ctx context.Context, roots paths.Roots, target central.RepoRemovalTarget) (repoLifecycleCommandResult, error) {
	return applyRepoLifecycle(ctx, roots, target, false)
}

func applyRepoLifecycle(ctx context.Context, roots paths.Roots, target central.RepoRemovalTarget, disable bool) (repoLifecycleCommandResult, error) {
	action := "enable"
	if disable {
		action = "disable"
	}
	reg, err := central.Load(roots)
	if err != nil {
		return repoLifecycleCommandResult{}, fmt.Errorf("acd repo %s: load registry: %w", action, err)
	}
	rec, ok := reg.FindRepo(target.Path, target.StateDB)
	if !ok {
		return repoLifecycleCommandResult{}, repoLifecycleUnknownTargetError(action, target)
	}
	safety := central.ProbeRepoRemovalSafety(ctx, rec)
	res := repoLifecycleCommandResult{
		Target:         target,
		Repo:           rec.Path,
		StateDB:        rec.StateDB,
		Action:         action,
		StatePreserved: true,
		Record:         rec,
	}
	var changed central.RepoLifecycleResult
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		if disable {
			changed = reg.DisableRepo(target, time.Now().Unix())
		} else {
			changed = reg.EnableRepo(target, time.Now().Unix())
		}
		return nil
	}); err != nil {
		return repoLifecycleCommandResult{}, fmt.Errorf("acd repo %s: update registry: %w", action, err)
	}
	if changed.NotFound {
		return repoLifecycleCommandResult{}, repoLifecycleUnknownTargetError(action, target)
	}
	res.Updated = changed.Updated
	res.Record = changed.Record
	res.Repo = changed.Record.Path
	res.StateDB = changed.Record.StateDB
	if disable && safety.DaemonAlive {
		stopRes, err := repoDisableStopOneRepo(ctx, rec.Path, "", true)
		res.Stopped = &stopRes
		if err != nil {
			return repoLifecycleCommandResult{}, fmt.Errorf("acd repo disable: stop daemon: %w", err)
		}
		if !stopRes.Stopped {
			return repoLifecycleCommandResult{}, fmt.Errorf("acd repo disable: daemon did not stop: %s", stopRes.Reason)
		}
	}
	if disable && safety.GitDir != "" {
		removeAllStartCaches(safety.GitDir)
		res.StartCachesCleared = true
	}
	return res, nil
}

func repoLifecycleUnknownTargetError(action string, target central.RepoRemovalTarget) error {
	name := target.Path
	if name == "" {
		name = target.StateDB
	}
	if name == "" {
		name = "."
	}
	return fmt.Errorf("acd repo %s: repo %s is not registered; run `acd repo init --repo %s` to register it or `acd repo list` to inspect registered repos", action, name, name)
}

func runRepoRemove(ctx context.Context, out io.Writer, repoFlag string, dryRun, yes, purgeState, jsonOut bool) error {
	return runRepoRemoveWithInput(ctx, out, os.Stdin, repoFlag, dryRun, yes, purgeState, jsonOut)
}

func runRepoRemoveWithInput(ctx context.Context, out io.Writer, in io.Reader, repoFlag string, dryRun, yes, purgeState, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if dryRun && yes {
		return fmt.Errorf("acd repo remove: --dry-run and --yes cannot be combined")
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd repo remove: resolve paths: %w", err)
	}
	if !yes && !dryRun && !jsonOut && repoFlag == "" {
		return runRepoRemoveInteractive(ctx, roots, out, in, purgeState)
	}
	target, err := repoRemovalTargetForCommand(ctx, repoFlag, "repo remove")
	if err != nil {
		return err
	}
	res, err := buildRepoRemovePreview(ctx, roots, target, purgeState)
	if err != nil {
		return err
	}
	if !yes {
		return renderRepoRemove(out, res, jsonOut)
	}
	res, err = applyRepoRemove(ctx, roots, target, purgeState)
	if err != nil {
		return err
	}
	return renderRepoRemove(out, res, jsonOut)
}

func collectRepoManagementEntries(ctx context.Context, repos []central.RepoRecord) []repoListEntry {
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	entries := make([]repoListEntry, 0, len(repos))
	for _, rec := range repos {
		safety := central.ProbeRepoRemovalSafety(ctx, rec)
		entry := repoListEntry{
			RepoRecord: rec,
			Safety:     safety,
			Status:     repoLifecycleStatus(rec, safety),
			Daemon:     "-",
		}
		if safety.DaemonStateKnown {
			entry.Daemon = safety.DaemonMode
			entry.PID = safety.DaemonPID
		}
		if safety.StateDBExists && safety.DaemonStateError == "" {
			if summary, err := summarizeRepo(ctx, rec.StateDB, now, clientTTLForRepo(rec.Path)); err == nil {
				entry.Daemon = summary.daemon
				entry.PID = summary.pid
				entry.Clients = summary.clients
				entry.PendingEvents = summary.pendingEvents
				entry.BlockedConflicts = summary.blockedConflicts
				if summary.daemon == "stale" {
					entry.Status = "stale"
				}
			}
		}
		entries = append(entries, entry)
	}
	return entries
}

func buildRepoRemovePreview(ctx context.Context, roots paths.Roots, target central.RepoRemovalTarget, purgeState bool) (repoRemoveResult, error) {
	preview, err := previewRepoRemoval(ctx, roots, target)
	if err != nil {
		return repoRemoveResult{}, err
	}
	return repoRemoveResult{
		Target:         target,
		DryRun:         true,
		Removed:        preview.Removed,
		NotFound:       preview.NotFound,
		PurgeState:     purgeState,
		StatePreserved: !purgeState,
		RemovedRecord:  preview.RemovedRecord,
		Safety:         preview.Safety,
	}, nil
}

func applyRepoRemove(ctx context.Context, roots paths.Roots, target central.RepoRemovalTarget, purgeState bool) (repoRemoveResult, error) {
	res, err := buildRepoRemovePreview(ctx, roots, target, purgeState)
	if err != nil {
		return repoRemoveResult{}, err
	}
	if res.Removed && res.Safety.DaemonAlive {
		stopRes, err := repoRemoveStopOneRepo(ctx, res.RemovedRecord.Path, "", true)
		res.Stopped = &stopRes
		if err != nil {
			return repoRemoveResult{}, fmt.Errorf("acd repo remove: stop daemon: %w", err)
		}
		if !stopRes.Stopped {
			return repoRemoveResult{}, fmt.Errorf("acd repo remove: daemon did not stop: %s", stopRes.Reason)
		}
	}
	var removed central.RepoRemovalResult
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		removed = reg.RemoveRepo(ctx, target)
		return nil
	}); err != nil {
		return repoRemoveResult{}, fmt.Errorf("acd repo remove: update registry: %w", err)
	}
	res.Removed = removed.Removed
	res.NotFound = removed.NotFound
	res.RemovedRecord = removed.RemovedRecord
	res.Safety = removed.Safety
	res.DryRun = false
	if res.Removed {
		if res.Safety.GitDir != "" {
			removeAllStartCaches(res.Safety.GitDir)
			res.StartCachesCleared = true
		}
		if purgeState {
			if res.Safety.StateDir == "" {
				return repoRemoveResult{}, fmt.Errorf("acd repo remove: cannot purge state; state dir is unknown")
			}
			if !isSafeRepoStatePurge(res.Safety) {
				return repoRemoveResult{}, fmt.Errorf("acd repo remove: refusing to purge non-acd state path %s", res.Safety.StateDir)
			}
			if err := os.RemoveAll(res.Safety.StateDir); err != nil {
				return repoRemoveResult{}, fmt.Errorf("acd repo remove: purge state: %w", err)
			}
			res.StatePurged = true
			res.StatePreserved = false
		}
	}
	return res, nil
}

func runRepoRemoveInteractive(ctx context.Context, roots paths.Roots, out io.Writer, in io.Reader, purgeState bool) error {
	reg, err := central.Load(roots)
	if err != nil {
		return fmt.Errorf("acd repo remove: load registry: %w", err)
	}
	entries := collectRepoManagementEntries(ctx, reg.Repos)
	if len(entries) == 0 {
		fmt.Fprintln(out, "acd repo remove: no registered repos")
		return nil
	}
	reader := bufio.NewReader(in)
	if err := renderRepoRemoveChoices(out, entries); err != nil {
		return err
	}
	selected, canceled, err := readRepoRemoveSelection(out, reader, len(entries))
	if err != nil {
		return err
	}
	if canceled {
		fmt.Fprintln(out, "acd repo remove: canceled")
		return nil
	}
	previews := make([]repoRemoveResult, 0, len(selected))
	for _, idx := range selected {
		entry := entries[idx]
		target := central.RepoRemovalTarget{Path: entry.Path, StateDB: entry.StateDB}
		preview, err := buildRepoRemovePreview(ctx, roots, target, purgeState)
		if err != nil {
			return err
		}
		previews = append(previews, preview)
	}
	renderRepoRemovePreview(out, previews, purgeState)
	if !readExactConfirmation(out, reader, "Type remove to remove selected repo(s): ", "remove") {
		fmt.Fprintln(out, "acd repo remove: canceled")
		return nil
	}
	if purgeState && !readExactConfirmation(out, reader, "Type purge to delete selected .git/acd state: ", "purge") {
		fmt.Fprintln(out, "acd repo remove: canceled; purge requires explicit confirmation")
		return nil
	}
	for _, preview := range previews {
		applied, err := applyRepoRemove(ctx, roots, preview.Target, purgeState)
		if err != nil {
			return err
		}
		if err := renderRepoRemove(out, applied, false); err != nil {
			return err
		}
	}
	return nil
}

func renderRepoRemoveChoices(out io.Writer, entries []repoListEntry) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "N\tREPO\tDAEMON\tCLIENTS\tPENDING\tBLOCKED\tSTATE_DB\tSTATUS")
	for i, entry := range entries {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%d\t%d\t%s\t%s\n",
			i+1,
			homeShort(entry.Path),
			repoEntryDaemon(entry),
			entry.Clients,
			entry.PendingEvents,
			entry.BlockedConflicts,
			entry.StateDB,
			entry.Status)
	}
	return tw.Flush()
}

func readRepoRemoveSelection(out io.Writer, reader *bufio.Reader, count int) ([]int, bool, error) {
	for {
		fmt.Fprint(out, "Select repo number(s) to remove, or c to cancel: ")
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, false, fmt.Errorf("acd repo remove: read selection: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" || isCancelInput(line) {
			return nil, true, nil
		}
		selected, parseErr := parseRepoSelection(line, count)
		if parseErr == nil {
			return selected, false, nil
		}
		fmt.Fprintf(out, "Invalid selection: %v\n", parseErr)
		if errors.Is(err, io.EOF) {
			return nil, true, nil
		}
	}
}

func parseRepoSelection(input string, count int) ([]int, error) {
	parts := strings.FieldsFunc(input, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
	seen := make(map[int]bool, len(parts))
	selected := make([]int, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 1 || n > count {
			return nil, fmt.Errorf("choose a number from 1 to %d", count)
		}
		idx := n - 1
		if !seen[idx] {
			seen[idx] = true
			selected = append(selected, idx)
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("choose at least one repo")
	}
	return selected, nil
}

func renderRepoRemovePreview(out io.Writer, previews []repoRemoveResult, purgeState bool) {
	fmt.Fprintln(out, "Preview:")
	for _, res := range previews {
		if res.RemovedRecord == nil {
			fmt.Fprintf(out, "  - %s: not registered\n", res.Target.Path)
			continue
		}
		fmt.Fprintf(out, "  - %s\n", homeShort(res.RemovedRecord.Path))
		if res.Safety.DaemonAlive {
			fmt.Fprintf(out, "      stop daemon : %s pid=%d\n", res.Safety.DaemonMode, res.Safety.DaemonPID)
		}
		fmt.Fprintln(out, "      registry    : remove row")
		fmt.Fprintln(out, "      start cache : clear")
		if purgeState {
			fmt.Fprintf(out, "      state       : purge %s\n", res.Safety.StateDir)
		} else {
			fmt.Fprintf(out, "      state       : preserve %s\n", res.Safety.StateDB)
		}
	}
}

func readExactConfirmation(out io.Writer, reader *bufio.Reader, prompt, want string) bool {
	fmt.Fprint(out, prompt)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false
	}
	return strings.TrimSpace(line) == want
}

func isCancelInput(input string) bool {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "c", "cancel", "q", "quit":
		return true
	default:
		return false
	}
}

func resolveRepoWorktree(ctx context.Context, repoFlag, command string) (git.Worktree, error) {
	wt, err := git.ResolveWorktree(ctx, repoFlag)
	if err != nil {
		if errors.Is(err, git.ErrNotWorktree) {
			return git.Worktree{}, fmt.Errorf("acd %s: repo %q is not inside a Git worktree: %w", command, repoFlag, err)
		}
		return git.Worktree{}, err
	}
	return wt, nil
}

func repoBranchRef(ctx context.Context, repo, command string) (string, error) {
	branchRef, err := git.RunBranchRef(ctx, repo)
	if err != nil {
		return "", fmt.Errorf("acd %s: resolve HEAD branch: %w", command, err)
	}
	if branchRef == "" {
		return "", fmt.Errorf("acd %s: detached HEAD is not supported; checkout a branch before running repo lifecycle commands", command)
	}
	return branchRef, nil
}

func repoRemovalTarget(ctx context.Context, repoFlag string) (central.RepoRemovalTarget, error) {
	return repoRemovalTargetForCommand(ctx, repoFlag, "repo remove")
}

func repoRemovalTargetForCommand(ctx context.Context, repoFlag, command string) (central.RepoRemovalTarget, error) {
	if wt, err := git.ResolveWorktree(ctx, repoFlag); err == nil {
		return central.RepoRemovalTarget{
			Path:    wt.Root,
			StateDB: state.DBPathFromGitDir(wt.GitDir),
		}, nil
	} else if !errors.Is(err, git.ErrNotWorktree) && !errors.Is(err, os.ErrNotExist) {
		return central.RepoRemovalTarget{}, err
	}
	path := repoFlag
	if path == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return central.RepoRemovalTarget{}, fmt.Errorf("acd %s: get cwd: %w", command, err)
		}
		path = cwd
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return central.RepoRemovalTarget{}, fmt.Errorf("acd %s: abs %q: %w", command, path, err)
	}
	return central.RepoRemovalTarget{Path: filepath.Clean(abs)}, nil
}

func isSafeRepoStatePurge(s central.RepoRemovalSafety) bool {
	if s.StateDB == "" || s.StateDir == "" || s.GitDir == "" {
		return false
	}
	return filepath.Base(s.StateDB) == state.DBFileName &&
		filepath.Base(s.StateDir) == "acd" &&
		filepath.Clean(filepath.Dir(s.StateDir)) == filepath.Clean(s.GitDir)
}

func previewRepoRemoval(ctx context.Context, roots paths.Roots, target central.RepoRemovalTarget) (central.RepoRemovalResult, error) {
	reg, err := central.Load(roots)
	if err != nil {
		return central.RepoRemovalResult{}, fmt.Errorf("acd repo remove: load registry: %w", err)
	}
	if rec, ok := reg.FindRepo(target.Path, target.StateDB); ok {
		recCopy := rec
		return central.RepoRemovalResult{
			Removed:       true,
			RemovedRecord: &recCopy,
			Safety:        central.ProbeRepoRemovalSafety(ctx, rec),
		}, nil
	}
	return central.RepoRemovalResult{
		NotFound: true,
		Safety:   central.ProbeRepoRemovalSafety(ctx, central.RepoRecord{Path: target.Path, StateDB: target.StateDB}),
	}, nil
}

func repoLifecycleStatus(rec central.RepoRecord, safety central.RepoRemovalSafety) string {
	switch {
	case rec.LifecycleDisabled():
		return "disabled"
	case rec.Path != "" && !fileExists(rec.Path):
		return "repo-missing"
	case !safety.StateDBExists:
		return "state-db-missing"
	case safety.DaemonStateError != "":
		return "state-unreadable"
	case safety.DaemonAlive:
		return "running"
	case safety.DaemonStateKnown:
		return "stopped"
	default:
		return "registered"
	}
}

func repoEntryDaemon(entry repoListEntry) string {
	if entry.Daemon == "" {
		return "-"
	}
	if entry.PID > 0 {
		return fmt.Sprintf("%s pid=%d", entry.Daemon, entry.PID)
	}
	return entry.Daemon
}

func renderRepoLifecycleCommand(out io.Writer, res repoLifecycleCommandResult, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	past := "enabled"
	if res.Action == "disable" {
		past = "disabled"
	}
	if res.Updated {
		fmt.Fprintf(out, "acd repo %s: %s %s\n", res.Action, past, res.Repo)
	} else {
		fmt.Fprintf(out, "acd repo %s: already %s %s\n", res.Action, past, res.Repo)
	}
	if res.Action == "disable" {
		if res.Stopped != nil && res.Stopped.Stopped {
			fmt.Fprintf(out, "daemon: stopped pid=%d\n", res.Stopped.DaemonPID)
		}
		if res.StartCachesCleared {
			fmt.Fprintln(out, "start cache: cleared")
		}
	}
	fmt.Fprintf(out, "state: preserved %s\n", res.StateDB)
	return nil
}

func renderRepoRemove(out io.Writer, res repoRemoveResult, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if res.DryRun {
		action := "would remove"
		if res.NotFound {
			action = "not registered"
		}
		fmt.Fprintf(out, "acd repo remove: dry-run: %s %s\n", action, res.Target.Path)
		if res.RemovedRecord != nil {
			fmt.Fprintf(out, "state: %s (preserved by default)\n", res.RemovedRecord.StateDB)
		}
		if res.PurgeState {
			fmt.Fprintln(out, "state: would purge with --yes --purge-state")
		}
		return nil
	}
	if res.NotFound {
		fmt.Fprintf(out, "acd repo remove: not registered %s\n", res.Target.Path)
		return nil
	}
	fmt.Fprintf(out, "acd repo remove: removed %s\n", res.Target.Path)
	if res.StatePurged {
		fmt.Fprintf(out, "state: purged %s\n", res.Safety.StateDir)
	} else {
		fmt.Fprintf(out, "state: preserved %s\n", res.Safety.StateDB)
	}
	return nil
}

package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

const productListDefaultRows = 5

var productListCollect = collectProductListOverview

type productListEntry struct {
	Repo                string                    `json:"repo"`
	Enabled             bool                      `json:"enabled"`
	Protected           bool                      `json:"protected"`
	Published           bool                      `json:"published"`
	ActionRequired      bool                      `json:"action_required"`
	State               productState              `json:"state"`
	PendingEvents       int                       `json:"pending_events"`
	BlockedEvents       int                       `json:"blocked_events"`
	CheckpointID        string                    `json:"checkpoint_id,omitempty"`
	WorkerState         string                    `json:"worker_state"`
	OperationalState    string                    `json:"operational_state"`
	LastActivityAt      string                    `json:"last_activity_at"`
	PublicationDrain    publicationDrainReport    `json:"publication_drain"`
	PublicationProgress publicationProgressReport `json:"publication_progress"`
	Summary             string                    `json:"summary"`
	NextAction          string                    `json:"next_action,omitempty"`
	RepoHash            string                    `json:"-"`
	Clients             int                       `json:"-"`
	LastCommitOID       string                    `json:"-"`
	ProtectionUnknown   bool                      `json:"-"`
	lastActivity        time.Time
}

type productListData struct {
	UpdatedAt string             `json:"updated_at"`
	Repos     []productListEntry `json:"repos"`
}

func newProductListCmd() *cobra.Command {
	var watch, once, verbose, interactive, showAll bool
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "list",
		Short: "Show live protection and commit progress",
		Long: `Show repositories that need action or are processing commits, followed
by the repositories where ACD most recently handled changes. Paused repositories
appear only when the compact view still has room.

In a terminal, the dashboard refreshes until you stop it with Ctrl-C. Use
--once for one snapshot, --all for every enabled repository, or --verbose for
operational details. Use acd repo list for the static registration inventory.`,
		Example: `  acd list
  acd list --once
  acd list --all
  acd list --watch --interval 5s
  acd list --once --verbose
  acd list --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			jsonOut, _ := cmd.Flags().GetBool("json")
			repo, _ := cmd.Flags().GetString("repo")
			if repo != "" {
				return invalidCommandError("acd list: --repo is not supported; list already shows every enabled repository")
			}
			if watch && once {
				return invalidCommandError("acd list: --watch and --once cannot be combined")
			}
			if interval <= 0 {
				return invalidCommandError("acd list: --interval must be positive")
			}
			if interactive {
				if jsonOut || watch || once || showAll {
					return invalidCommandError("acd list: --interactive cannot be combined with --json, --watch, --once, or --all")
				}
				return runRepoManageWithInput(cmd.Context(), cmd.OutOrStdout(), cmd.InOrStdin(), verbose)
			}
			if watch && jsonOut {
				return invalidCommandError("acd list: --watch does not support --json")
			}
			stdout, _ := cmd.OutOrStdout().(*os.File)
			if listUseWatchMode(stdout, once, watch) && !jsonOut {
				watchCtx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
				defer stop()
				return runProductListWatchDisplay(watchCtx, cmd.OutOrStdout(), interval,
					verbose, showAll, listUseWatchMode(stdout, false, false))
			}
			return runProductListOnceView(cmd.Context(), cmd.OutOrStdout(), jsonOut, verbose, showAll)
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "Refresh until interrupted (default on a TTY)")
	cmd.Flags().BoolVar(&once, "once", false, "Print one snapshot and exit")
	cmd.Flags().BoolVar(&showAll, "all", false, "Include every enabled repository")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Include operational details and next actions")
	cmd.Flags().BoolVar(&interactive, "interactive", false, "Open the advanced repository manager")
	cmd.Flags().DurationVar(&interval, "interval", defaultListWatchInterval, "Refresh interval")
	return cmd
}

func collectProductList(ctx context.Context) (productListData, productState, error) {
	return productListCollect(ctx)
}

func higherProductState(current, candidate productState) productState {
	priority := map[productState]int{
		productStateProtected: 0, productStateOff: 1, productStateWaiting: 2,
		productStatePublishing: 3, productStateNeedsAction: 4,
	}
	if priority[candidate] > priority[current] {
		return candidate
	}
	return current
}

func productListTargetAction(action, repo string) string {
	repoArgument := " --repo " + productListShellQuote(repo)
	var targeted strings.Builder
	for action != "" {
		start := strings.Index(action, "`acd ")
		if start < 0 {
			targeted.WriteString(action)
			break
		}
		targeted.WriteString(action[:start])
		action = action[start:]
		end := strings.Index(action[1:], "`")
		if end < 0 {
			targeted.WriteString(action)
			break
		}
		end++
		command := action[:end]
		if !strings.Contains(command, " --repo ") && !strings.Contains(command, " --repo=") {
			command += repoArgument
		}
		targeted.WriteString(command)
		targeted.WriteByte('`')
		action = action[end+1:]
	}
	return targeted.String()
}

func productListShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func runProductListOnce(ctx context.Context, out io.Writer, jsonOut, verbose bool) error {
	return runProductListOnceView(ctx, out, jsonOut, verbose, false)
}

func runProductListOnceView(ctx context.Context, out io.Writer, jsonOut, verbose, showAll bool) error {
	data, stateName, err := collectProductList(ctx)
	if err != nil {
		return fmt.Errorf("acd list: %w", err)
	}
	if jsonOut {
		if err := renderAnyProductEnvelope(out, productEnvelope{
			OK: true, State: stateName, Actions: []productAction{}, Data: data,
		}, true); err != nil {
			return err
		}
	} else if err := renderProductListDashboard(out, data.Repos, verbose, showAll); err != nil {
		return err
	}
	if productListRequiresAction(data.Repos) {
		return &CommandError{
			Code:     "needs_action",
			Message:  "acd list: one or more repositories require action",
			Exit:     ExitActionRequired,
			rendered: true,
		}
	}
	return nil
}

func productListRequiresAction(entries []productListEntry) bool {
	for _, entry := range entries {
		if entry.ActionRequired {
			return true
		}
	}
	return false
}

func runProductListWatch(ctx context.Context, out io.Writer, interval time.Duration, verbose bool) error {
	return runProductListWatchView(ctx, out, interval, verbose, false)
}

func runProductListWatchView(ctx context.Context, out io.Writer, interval time.Duration, verbose, showAll bool) error {
	return runProductListWatchDisplay(ctx, out, interval, verbose, showAll, false)
}

func runProductListWatchDisplay(
	ctx context.Context,
	out io.Writer,
	interval time.Duration,
	verbose, showAll, terminalScreen bool,
) error {
	terminalStarted := false
	lastKnown := make(map[string]productListEntry)
	defer func() {
		if terminalStarted {
			fmt.Fprint(out, "\033[?25h\033[?1049l")
		}
	}()
	for {
		data, _, err := collectProductList(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("acd list: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		stabilizeProductListFrame(data.Repos, lastKnown)
		if terminalScreen && !terminalStarted {
			fmt.Fprint(out, "\033[?1049h\033[?25l")
			terminalStarted = true
		}
		fmt.Fprint(out, "\033[2J\033[H")
		fmt.Fprintf(out, "Updated: %s\n\n", data.UpdatedAt)
		if err := renderProductListDashboard(out, data.Repos, verbose, showAll); err != nil {
			return err
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func stabilizeProductListFrame(entries []productListEntry, lastKnown map[string]productListEntry) {
	for index, entry := range entries {
		if !entry.ProtectionUnknown {
			lastKnown[entry.Repo] = entry
			continue
		}
		previous, ok := lastKnown[entry.Repo]
		if ok && productListPriority(previous) <= productListPriority(entry) {
			entries[index] = previous
		}
	}
}

func renderProductListTable(out io.Writer, entries []productListEntry, verbose bool) error {
	return renderProductListDashboard(out, entries, verbose, true)
}

func renderProductListDashboard(out io.Writer, entries []productListEntry, verbose, showAll bool) error {
	visible, hidden := selectProductListEntries(entries, showAll)
	labels := productListLabels(visible)
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if verbose {
		fmt.Fprintln(tw, "REPOSITORY\tWORKER\tLIVE TOOLS\tSAFE\tMODE\tQUEUE\tTARGET\tLAST MOVE\tPHASE\tBLOCKED\tLAST COMMIT\tSTATUS\tDETAILS")
	} else {
		fmt.Fprintln(tw, "REPO\tSAFE\tMODE\tQUEUE\tTARGET\tLAST MOVE\tPHASE\tSTATUS")
	}
	for index, entry := range visible {
		if verbose {
			details := entry.Summary
			if entry.NextAction != "" && entry.NextAction != "No action needed." {
				details = strings.TrimSpace(details + " " + entry.NextAction)
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%d\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
				homeShort(entry.Repo), valueOrDash(entry.WorkerState), entry.Clients,
				productListSafety(entry), productListMode(entry), entry.PendingEvents,
				productListTarget(entry), productListProgressAge(entry), productListPhase(entry),
				entry.BlockedEvents, productListLastCommit(entry.LastCommitOID),
				productListStatus(entry), details)
			continue
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n", labels[index],
			productListSafety(entry), productListMode(entry), entry.PendingEvents,
			productListTarget(entry), productListProgressAge(entry), productListPhase(entry),
			productListStatus(entry))
	}
	if len(visible) == 0 {
		if verbose {
			fmt.Fprintln(tw, "No enabled repositories.\t\t\t\t\t\t\t\t\t\t\t\t")
		} else {
			fmt.Fprintln(tw, "No enabled repositories.\t\t\t\t\t\t\t")
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if hidden > 0 {
		_, err := fmt.Fprintf(out, "%d repositories hidden; use acd list --all\n", hidden)
		return err
	}
	return nil
}

func productListMode(entry productListEntry) string {
	mode := entry.PublicationProgress.Strategy
	if mode == "" {
		return "-"
	}
	return mode
}

func productListTarget(entry productListEntry) string {
	progress := entry.PublicationProgress
	if progress.TargetTotal <= 0 {
		return "-"
	}
	switch progress.Origin {
	case "commit_all":
		return fmt.Sprintf("commit-all:%d/%d", progress.TargetRemaining,
			progress.TargetTotal)
	case "intent_recovery":
		return fmt.Sprintf("recover:%d/%d", progress.TargetRemaining,
			progress.TargetTotal)
	default:
		return "-"
	}
}

func productListProgressAge(entry productListEntry) string {
	progress := entry.PublicationProgress
	if progress.LastProgressTS <= 0 {
		return "-"
	}
	return strings.ReplaceAll(formatDurationCompact(
		time.Duration(progress.LastProgressAgeSeconds)*time.Second), " ", "")
}

func productListPhase(entry productListEntry) string {
	progress := entry.PublicationProgress
	switch progress.Phase {
	case "intent_wait":
		if progress.WaitRemainingSeconds > 0 {
			return "wait:" + strings.ReplaceAll(formatDurationCompact(
				time.Duration(progress.WaitRemainingSeconds)*time.Second), " ", "")
		}
		return "waiting"
	case "intent_planning":
		return "intent-plan"
	case "intent_replanning":
		return "intent-replan"
	case "intent_processing":
		return "intent"
	case "rewind_wait":
		if progress.WaitRemainingSeconds > 0 {
			return "rewind:" + strings.ReplaceAll(formatDurationCompact(
				time.Duration(progress.WaitRemainingSeconds)*time.Second), " ", "")
		}
		return "rewind-wait"
	case "config_wait":
		return "config-wait"
	case "local_fallback":
		if progress.Origin == "intent_recovery" {
			return "intent-widen"
		}
		return "local-recovery"
	case "provider_wait":
		return "provider-wait"
	case "event_publishing":
		return "event"
	case "needs_action":
		return "blocked"
	case "stalled":
		return "stalled"
	default:
		return strings.ReplaceAll(valueOrDash(progress.Phase), "_", "-")
	}
}

func productListSafety(entry productListEntry) string {
	if entry.ProtectionUnknown {
		return "-"
	}
	return yesNo(entry.Protected)
}

func selectProductListEntries(entries []productListEntry, showAll bool) ([]productListEntry, int) {
	if showAll {
		return entries, 0
	}
	mandatory := make([]productListEntry, 0, len(entries))
	recent := make([]productListEntry, 0, len(entries))
	paused := make([]productListEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.ActionRequired {
			mandatory = append(mandatory, entry)
			continue
		}
		switch productListStatus(entry) {
		case "needs action", "working", "waiting", "stalled":
			mandatory = append(mandatory, entry)
		case "paused":
			paused = append(paused, entry)
		default:
			recent = append(recent, entry)
		}
	}
	visible := append([]productListEntry(nil), mandatory...)
	remaining := productListDefaultRows - len(visible)
	if remaining < 0 {
		remaining = 0
	}
	if count := min(remaining, len(recent)); count > 0 {
		visible = append(visible, recent[:count]...)
		remaining -= count
	}
	if count := min(remaining, len(paused)); count > 0 {
		visible = append(visible, paused[:count]...)
	}
	return visible, len(entries) - len(visible)
}

func productListStatus(entry productListEntry) string {
	switch {
	case entry.OperationalState == "paused":
		return "paused"
	case entry.ActionRequired || entry.State == productStateNeedsAction || entry.OperationalState == "needs_attention":
		return "needs action"
	case entry.PublicationProgress.Phase == "needs_action":
		return "needs action"
	case entry.PublicationProgress.Phase == "stalled":
		return "stalled"
	case entry.PublicationProgress.Phase == "provider_wait" ||
		entry.PublicationProgress.Phase == "intent_wait" ||
		entry.PublicationProgress.Phase == "rewind_wait" ||
		entry.PublicationProgress.Phase == "config_wait":
		return "waiting"
	case entry.OperationalState == "waiting":
		return "waiting"
	case productListWorkingState(entry.OperationalState):
		return "working"
	case entry.State == productStateWaiting:
		return "waiting"
	case entry.State == productStatePublishing || entry.PendingEvents > 0:
		return "working"
	default:
		return "healthy"
	}
}

func productListWorkingState(operational string) bool {
	switch operational {
	case "busy", "planning", "event_fallback", "self_healing", "fallback", "retrying", "validating":
		return true
	default:
		return false
	}
}

func productListLastCommit(oid string) string {
	if oid == "" {
		return "-"
	}
	if len(oid) > 10 {
		return oid[:10]
	}
	return oid
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func productListLabels(entries []productListEntry) []string {
	labels := make([]string, len(entries))
	counts := make(map[string]int, len(entries))
	for index, entry := range entries {
		label := filepath.Base(entry.Repo)
		if label == "." || label == string(filepath.Separator) || label == "" {
			label = homeShort(entry.Repo)
		}
		labels[index] = label
		counts[label]++
	}
	for index, label := range labels {
		if counts[label] < 2 {
			continue
		}
		hash := entries[index].RepoHash
		if len(hash) > 6 {
			hash = hash[:6]
		}
		if hash == "" {
			hash = filepath.Base(filepath.Dir(entries[index].Repo))
		}
		labels[index] = fmt.Sprintf("%s [%s]", label, hash)
	}
	return labels
}

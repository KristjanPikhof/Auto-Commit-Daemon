package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const productListActiveWindow = time.Hour

var productListCollect = collectProductListOverview

type productListEntry struct {
	PublicationOutcome    publicationOutcome           `json:"publication_outcome"`
	Repo                  string                       `json:"repo"`
	Enabled               bool                         `json:"enabled"`
	Protected             bool                         `json:"protected"`
	Published             bool                         `json:"published"`
	ActionRequired        bool                         `json:"action_required"`
	State                 productState                 `json:"state"`
	PendingEvents         int                          `json:"pending_events"`
	BlockedEvents         int                          `json:"blocked_events"`
	CheckpointID          string                       `json:"checkpoint_id,omitempty"`
	WorkerState           string                       `json:"worker_state"`
	OperationalState      string                       `json:"operational_state"`
	LastActivityAt        string                       `json:"last_activity_at"`
	CheckpointMaintenance checkpoint.MaintenanceStatus `json:"checkpoint_maintenance"`
	CaptureHealth         state.CaptureHealth          `json:"capture_health"`
	PublicationProgress   publicationProgressReport    `json:"publication_progress"`
	Summary               string                       `json:"summary"`
	NextAction            string                       `json:"next_action,omitempty"`
	RepoHash              string                       `json:"-"`
	Clients               int                          `json:"-"`
	LastCommitOID         string                       `json:"-"`
	ProtectionUnknown     bool                         `json:"protection_unknown,omitempty"`
	UnfinishedWork        bool                         `json:"unfinished_work"`
	lastActivity          time.Time
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
		Long: `Show repositories active in the last hour, including those that need action,
and repositories with unfinished work. Repositories that need action stay
visible. Other idle repositories are hidden. Repository blocks stay in place
while the view refreshes.

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
				return invalidCommandError("acd list: --repo is not supported. Use acd status --repo /path/to/repo for one repository.")
			}
			if watch && once {
				return invalidCommandError("acd list: --watch and --once cannot be combined")
			}
			if interval <= 0 {
				return invalidCommandError("acd list: --interval must be longer than 0s, for example --interval 5s")
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
	cmd.Flags().BoolVar(&watch, "watch", false, "Refresh until Ctrl-C (default in a terminal)")
	cmd.Flags().BoolVar(&once, "once", false, "Print one snapshot and exit")
	cmd.Flags().BoolVar(&showAll, "all", false, "Include every enabled repository")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Include background activity and diagnostic details")
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

func runProductListOnceView(ctx context.Context, out io.Writer, jsonOut, verbose, showAll bool) error {
	data, stateName, err := collectProductList(ctx)
	if err != nil {
		return fmt.Errorf("ACD could not check protection.\nNext: run acd doctor.\nDetails: acd list: %w", err)
	}
	entries := data.Repos
	if jsonOut {
		if err := renderAnyProductEnvelope(out, productEnvelope{
			OK: true, State: stateName, Actions: []productAction{}, Data: data,
		}, true); err != nil {
			return err
		}
	} else {
		var hidden int
		entries, hidden = selectProductListEntries(data.Repos, showAll)
		if err := renderProductListSelection(out, entries, hidden, verbose, showAll); err != nil {
			return err
		}
	}
	if productListRequiresAction(entries) {
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

func runProductListWatchDisplay(
	ctx context.Context,
	out io.Writer,
	interval time.Duration,
	verbose, showAll, terminalScreen bool,
) error {
	terminalStarted := false
	interrupted := false
	lastFrame := ""
	lastKnown := make(map[string]productListEntry)
	var rowOrder []string
	defer func() {
		if terminalStarted {
			fmt.Fprint(out, "\033[?25h\033[?1049l")
			if interrupted {
				fmt.Fprint(out, lastFrame)
			}
		}
	}()
	for {
		data, _, err := collectProductList(ctx)
		if err != nil {
			if ctx.Err() != nil {
				interrupted = true
				return nil
			}
			return fmt.Errorf("ACD could not check protection.\nNext: run acd doctor.\nDetails: acd list: %w", err)
		}
		if ctx.Err() != nil {
			interrupted = true
			return nil
		}
		stabilizeProductListFrame(data.Repos, lastKnown)
		visible, hidden := selectProductListEntries(data.Repos, showAll)
		visible, rowOrder = orderProductListFrame(visible, rowOrder)
		var frame strings.Builder
		fmt.Fprintf(&frame, "Updated: %s\n\n", data.UpdatedAt)
		if err := renderProductListSelection(&frame, visible, hidden, verbose, showAll); err != nil {
			return err
		}
		if terminalScreen && !terminalStarted {
			fmt.Fprint(out, "\033[?1049h\033[?25l")
			terminalStarted = true
		}
		if terminalScreen {
			fmt.Fprint(out, "\033[2J\033[H")
		}
		completedFrame := frame.String()
		if _, err := io.WriteString(out, completedFrame); err != nil {
			return err
		}
		lastFrame = completedFrame

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			interrupted = true
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func stabilizeProductListFrame(entries []productListEntry, lastKnown map[string]productListEntry) {
	present := make(map[string]bool, len(entries))
	for index, entry := range entries {
		present[entry.Repo] = true
		if !entry.ProtectionUnknown {
			lastKnown[entry.Repo] = entry
			continue
		}
		previous, ok := lastKnown[entry.Repo]
		if ok && productListPriority(previous) <= productListPriority(entry) {
			if entry.OperationalState == "refreshing" && !previous.ActionRequired &&
				previous.State != productStateNeedsAction && previous.OperationalState != "paused" &&
				previous.OperationalState != "needs_attention" && previous.PublicationProgress.Phase != "needs_action" {
				entry.UnfinishedWork = entry.UnfinishedWork || previous.UnfinishedWork
				if previous.lastActivity.After(entry.lastActivity) {
					entry.lastActivity, entry.LastActivityAt = previous.lastActivity, previous.LastActivityAt
				}
				entries[index] = entry
				continue
			}
			previous.ProtectionUnknown = true
			entries[index] = previous
		}
	}
	for repo := range lastKnown {
		if !present[repo] {
			delete(lastKnown, repo)
		}
	}

}

func renderProductListSelection(out io.Writer, visible []productListEntry, hidden int, verbose, showAll bool) error {
	labels := productListLabels(visible)
	for index, entry := range visible {
		fmt.Fprintf(out, "%s\n  Saved: %s | Changes: %s\n  Status: %s\n",
			safePreviewText(labels[index], 0), productListSafety(entry), productListSavedWork(entry), safePreviewText(productListDisplayStatus(entry), 0))
		if verbose {
			fmt.Fprintf(out, "  Repository: %s\n  Mode: %s | Last progress: %s\n",
				safePreviewText(homeShort(entry.Repo), 0), productListMode(entry), productListProgressAge(entry))
			fmt.Fprintf(out, "  Background protection: %s\n", safePreviewText(valueOrDash(entry.WorkerState), 0))
			fmt.Fprintf(out, "  Older tool sessions: %d | Issues: %d\n  Last commit: %s\n", entry.Clients, entry.BlockedEvents, safePreviewText(productListLastCommit(entry.LastCommitOID), 0))
			fmt.Fprintln(out, consoleProse("  Details: "+entry.Summary))
			if summary := entry.CheckpointMaintenance.Summary(); summary != "" && !strings.Contains(entry.Summary, summary) {
				fmt.Fprintln(out, consoleProse("  Cleanup: "+maintenanceDetails(entry.CheckpointMaintenance)))
			}
		}
		if entry.ActionRequired || entry.State == productStateNeedsAction {
			if entry.NextAction != "" {
				fmt.Fprintln(out, "  Next:", safePreviewText(entry.NextAction, 0))
				fmt.Fprintf(out, "  Repository: %s\n", productListShellQuote(entry.Repo))
			}
		}
		fmt.Fprintln(out)
	}
	if len(visible) == 0 {
		if showAll {
			fmt.Fprintln(out, "No enabled repositories.")
		} else {
			fmt.Fprintln(out, "No recently active repositories. Use acd list --all to see every enabled repository.")
		}
	}
	if hidden > 0 {
		_, err := fmt.Fprintf(out, "%d idle repositories hidden. Use acd list --all to include them.\n", hidden)
		return err
	}
	return nil
}

func productListSavedWork(entry productListEntry) string {
	if entry.ProtectionUnknown {
		return "?"
	}
	if entry.PublicationOutcome.CheckpointAvailable {
		if !entry.PublicationOutcome.CountsKnown {
			return "?"
		}
		return fmt.Sprint(entry.PublicationOutcome.PendingChanges)
	}
	return fmt.Sprint(entry.PendingEvents)
}

func productListMode(entry productListEntry) string {
	mode := entry.PublicationProgress.Strategy
	if mode == "" {
		return "-"
	}
	return mode
}

func productListTarget(entry productListEntry) string {
	if h := entry.PublicationOutcome.History; h != nil {
		return "history:" + h.PlanID[:min(8, len(h.PlanID))]
	}
	if f := entry.PublicationOutcome.Finish; f != nil {
		return "finish:" + f.RequestID[:min(8, len(f.RequestID))]
	}
	return "-"
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
	if entry.PublicationOutcome.CheckpointAvailable {
		if h := entry.PublicationOutcome.History; h != nil {
			return "history-" + h.State
		}
		if f := entry.PublicationOutcome.Finish; f != nil {
			if f.Stage != "" {
				return f.Stage
			}
			return "finish-" + f.State
		}
		return entry.OperationalState
	}
	if entry.PublicationProgress.Phase == "protection_refresh" {
		return "checking protection"
	}
	if entry.OperationalState == "rewriting" {
		return "history-rewrite"
	}
	return strings.ReplaceAll(entry.PublicationProgress.Phase, "_", "-")
}

func productListSafety(entry productListEntry) string {
	if entry.ProtectionUnknown {
		return "-"
	}
	return yesNo(entry.Protected)
}

func selectProductListEntries(entries []productListEntry, showAll bool) ([]productListEntry, int) {
	return selectProductListEntriesAt(entries, showAll, time.Now())
}

func selectProductListEntriesAt(entries []productListEntry, showAll bool, now time.Time) ([]productListEntry, int) {
	if showAll {
		return entries, 0
	}
	visible := make([]productListEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.ActionRequired || entry.State == productStateNeedsAction || entry.UnfinishedWork || entry.PendingEvents > 0 || entry.BlockedEvents > 0 ||
			(!entry.lastActivity.IsZero() && entry.lastActivity.After(now.Add(-productListActiveWindow))) {
			visible = append(visible, entry)
		}
	}
	return visible, len(entries) - len(visible)
}

func orderProductListFrame(entries []productListEntry, previous []string) ([]productListEntry, []string) {
	remaining := make(map[string]productListEntry, len(entries))
	for _, entry := range entries {
		remaining[entry.Repo] = entry
	}
	ordered := make([]productListEntry, 0, len(entries))
	for _, repo := range previous {
		if entry, ok := remaining[repo]; ok {
			ordered = append(ordered, entry)
			delete(remaining, repo)
		}
	}
	for _, entry := range entries {
		if _, ok := remaining[entry.Repo]; ok {
			ordered = append(ordered, entry)
			delete(remaining, entry.Repo)
		}
	}
	order := make([]string, len(ordered))
	for i, entry := range ordered {
		order[i] = entry.Repo
	}
	return ordered, order
}

func productListDisplayStatus(entry productListEntry) string {
	status := productListStatus(entry)
	switch status {
	case "waiting":
		status = "saved, awaiting finish"
	case "working":
		status = "saving changes"
	case "healthy":
		status = "ready"
	case "refreshing":
		status = "checking protection"
	}
	if f := entry.PublicationOutcome.Finish; f != nil && (f.State == state.FinishRunning || f.State == state.FinishPrepared) {
		status = finishStageLabel(f.Stage)
	}
	if h := entry.PublicationOutcome.History; h != nil {
		status = "history rewrite: " + strings.ReplaceAll(h.State, "_", " ")
	}
	switch entry.CheckpointMaintenance.State {
	case "retrying":
		status += " (cleanup retry)"
	case "prerequisite":
		status += " (Xcode license)"
	case "needs_action":
		status += " (cleanup needs review)"
	case "over_budget":
		status += " (storage limit reached)"
	}
	if entry.PublicationOutcome.Legacy.State == "needs_attention" {
		status += " (older saved data needs review)"
	}
	return status
}

func productListStatus(entry productListEntry) string {
	if f := entry.PublicationOutcome.Finish; entry.PublicationOutcome.CheckpointAvailable && f != nil && (f.State == state.FinishFailed || f.State == state.FinishNeedsAttention) {
		if f.State == state.FinishNeedsAttention {
			return "finish needs review"
		}
		return "finish failed"
	}

	switch {
	case entry.PublicationProgress.Phase == "needs_action":
		return "needs action"
	case entry.OperationalState == "paused":
		return "paused"
	case entry.ActionRequired || entry.State == productStateNeedsAction || entry.OperationalState == "needs_attention":
		return "needs action"
	case entry.OperationalState == "refreshing":
		return "refreshing"
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
	case "rewriting", "busy", "planning", "event_fallback", "self_healing", "fallback", "retrying", "validating":
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

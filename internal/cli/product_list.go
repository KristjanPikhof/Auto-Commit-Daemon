package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

type productListEntry struct {
	Repo             string                 `json:"repo"`
	Enabled          bool                   `json:"enabled"`
	Protected        bool                   `json:"protected"`
	Published        bool                   `json:"published"`
	ActionRequired   bool                   `json:"action_required"`
	State            productState           `json:"state"`
	PendingEvents    int                    `json:"pending_events"`
	BlockedEvents    int                    `json:"blocked_events"`
	CheckpointID     string                 `json:"checkpoint_id,omitempty"`
	WorkerState      string                 `json:"worker_state,omitempty"`
	OperationalState string                 `json:"operational_state,omitempty"`
	LastActivityAt   string                 `json:"last_activity_at,omitempty"`
	PublicationDrain publicationDrainReport `json:"publication_drain"`
	Summary          string                 `json:"summary"`
	NextAction       string                 `json:"next_action,omitempty"`
	Clients          int                    `json:"-"`
	LastCommitOID    string                 `json:"-"`
	lastActivity     time.Time
}

type productListData struct {
	UpdatedAt string             `json:"updated_at"`
	Repos     []productListEntry `json:"repos"`
}

func newProductListCmd() *cobra.Command {
	var watch, once, verbose, interactive bool
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "list",
		Short: "Show protection across all enabled repositories",
		Long: `Show whether each enabled repository is protected, published to local
Git, or needs action.

In a terminal, the dashboard refreshes until you stop it with Ctrl-C. Use
--once for one snapshot. Use acd repo list to include disabled or missing
registrations.`,
		Example: `  acd list
  acd list --once
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
				if jsonOut || watch || once {
					return invalidCommandError("acd list: --interactive cannot be combined with --json, --watch, or --once")
				}
				return runRepoManageWithInput(cmd.Context(), cmd.OutOrStdout(), cmd.InOrStdin(), verbose)
			}
			if watch && jsonOut {
				return invalidCommandError("acd list: --watch does not support --json")
			}
			stdout, _ := cmd.OutOrStdout().(*os.File)
			if listUseWatchMode(stdout, once, watch) && !jsonOut {
				return runProductListWatch(cmd.Context(), cmd.OutOrStdout(), interval, verbose)
			}
			return runProductListOnce(cmd.Context(), cmd.OutOrStdout(), jsonOut, verbose)
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "Refresh until interrupted (default on a TTY)")
	cmd.Flags().BoolVar(&once, "once", false, "Print one snapshot and exit")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Include summaries and next actions")
	cmd.Flags().BoolVar(&interactive, "interactive", false, "Open the advanced repository manager")
	cmd.Flags().DurationVar(&interval, "interval", defaultListWatchInterval, "Refresh interval")
	return cmd
}

func collectProductList(ctx context.Context) (productListData, productState, error) {
	return collectProductListOverview(ctx)
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
	} else if err := renderProductListTable(out, data.Repos, verbose); err != nil {
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
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		data, _, err := collectProductList(ctx)
		if err != nil {
			return fmt.Errorf("acd list: %w", err)
		}
		fmt.Fprint(out, "\033[2J\033[H")
		fmt.Fprintf(out, "Updated: %s\n\n", data.UpdatedAt)
		if err := renderProductListTable(out, data.Repos, verbose); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func renderProductListTable(out io.Writer, entries []productListEntry, verbose bool) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "REPOSITORY\tENABLED\tPROTECTED\tGIT\tACTION\tSTATE")
	for _, entry := range entries {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", homeShort(entry.Repo),
			yesNo(entry.Enabled), yesNo(entry.Protected), productListGitState(entry),
			yesNo(entry.ActionRequired), entry.State)
		if verbose {
			fmt.Fprintf(tw, "  %s\t\t\t\t\t%s\n", entry.Summary, entry.NextAction)
		}
	}
	if len(entries) == 0 {
		fmt.Fprintln(tw, "No enabled repositories.\t\t\t\t\t")
	}
	return tw.Flush()
}

func productListGitState(entry productListEntry) string {
	switch {
	case entry.Published:
		return "published"
	case !entry.Enabled:
		return "off"
	case entry.ActionRequired || entry.State == productStateNeedsAction || !entry.Protected:
		return "blocked"
	default:
		return "not-published"
	}
}

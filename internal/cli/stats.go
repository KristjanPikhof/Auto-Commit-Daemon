package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

// statsByDay is a single day in the §7.8 by_day series.
type statsByDay struct {
	Day          string `json:"day"`
	Events       int64  `json:"events"`
	Commits      int64  `json:"commits"`
	FilesChanged int64  `json:"files_changed"`
	BytesChanged int64  `json:"bytes_changed"`
	ErrorsTotal  int64  `json:"errors_total"`
}

// statsByRepo is a single repo aggregate over the window.
type statsByRepo struct {
	RepoHash     string `json:"repo_hash"`
	RepoPath     string `json:"repo_path"`
	Events       int64  `json:"events"`
	Commits      int64  `json:"commits"`
	FilesChanged int64  `json:"files_changed"`
	BytesChanged int64  `json:"bytes_changed"`
	ErrorsTotal  int64  `json:"errors_total"`
}

// statsTotals are window-wide sums for the §7.8 "Across all repos" block.
type statsTotals struct {
	Events       int64 `json:"events"`
	Commits      int64 `json:"commits"`
	FilesChanged int64 `json:"files_changed"`
	BytesChanged int64 `json:"bytes_changed"`
	ErrorsTotal  int64 `json:"errors_total"`
}

// statsReport is the JSON shape per §7.8.
type statsReport struct {
	Since  string        `json:"since"`
	Until  string        `json:"until"`
	Totals statsTotals   `json:"totals"`
	ByDay  []statsByDay  `json:"by_day"`
	ByRepo []statsByRepo `json:"by_repo"`
}

func newStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Aggregated commits/events/bytes across all repos",
		Long: `Print aggregate commit, event, file, byte, and error counts from the central stats database.

Stats are across all registered repos and default to the last 7 days. Use --since with a duration such as 24h, 30d, or 1y, and --json for automation.`,
		Example: `  acd stats
  acd stats --since 30d
  acd stats --since 1y --json
  acd list`,
		RunE: func(cmd *cobra.Command, args []string) error {
			since, _ := cmd.Flags().GetString("since")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runStats(cmd.Context(), cmd.OutOrStdout(), since, jsonOut)
		},
	}
	cmd.Flags().String("since", "7d", "Lookback window (e.g. 7d, 30d, 1y)")
	return cmd
}

func runStats(ctx context.Context, out io.Writer, sinceStr string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	d, err := parseSince(sinceStr)
	if err != nil {
		return fmt.Errorf("acd stats: %w", err)
	}
	now := time.Now()
	until := now
	since := now.Add(-d)

	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd stats: resolve paths: %w", err)
	}
	db, err := central.Open(ctx, roots)
	if err != nil {
		return fmt.Errorf("acd stats: open stats.db: %w", err)
	}
	defer db.Close()

	rows, err := db.ListRollupsSince(ctx, since.Unix())
	if err != nil {
		return fmt.Errorf("acd stats: list rollups: %w", err)
	}

	report := buildStatsReport(rows, since, until)
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	return renderStatsHuman(out, report, sinceStr)
}

// buildStatsReport rolls up DailyRollup rows into per-day, per-repo, and
// total slices. Days/repos are sorted deterministically for stable output.
func buildStatsReport(rows []central.DailyRollup, since, until time.Time) statsReport {
	report := statsReport{
		Since:  since.Format("2006-01-02"),
		Until:  until.Format("2006-01-02"),
		ByDay:  []statsByDay{},
		ByRepo: []statsByRepo{},
	}
	dayIdx := map[string]*statsByDay{}
	repoIdx := map[string]*statsByRepo{}
	for _, r := range rows {
		report.Totals.Events += r.EventsTotal
		report.Totals.Commits += r.CommitsTotal
		report.Totals.FilesChanged += r.FilesChanged
		report.Totals.BytesChanged += r.BytesChanged
		report.Totals.ErrorsTotal += r.ErrorsTotal

		d := dayIdx[r.Day]
		if d == nil {
			d = &statsByDay{Day: r.Day}
			dayIdx[r.Day] = d
		}
		d.Events += r.EventsTotal
		d.Commits += r.CommitsTotal
		d.FilesChanged += r.FilesChanged
		d.BytesChanged += r.BytesChanged
		d.ErrorsTotal += r.ErrorsTotal

		repo := repoIdx[r.RepoHash]
		if repo == nil {
			repo = &statsByRepo{RepoHash: r.RepoHash, RepoPath: r.RepoPath}
			repoIdx[r.RepoHash] = repo
		}
		// Keep the most recent path label (stats.db rows are ordered ASC
		// by day so a later iteration wins).
		repo.RepoPath = r.RepoPath
		repo.Events += r.EventsTotal
		repo.Commits += r.CommitsTotal
		repo.FilesChanged += r.FilesChanged
		repo.BytesChanged += r.BytesChanged
		repo.ErrorsTotal += r.ErrorsTotal
	}
	for _, d := range dayIdx {
		report.ByDay = append(report.ByDay, *d)
	}
	sort.Slice(report.ByDay, func(i, j int) bool {
		return report.ByDay[i].Day < report.ByDay[j].Day
	})
	for _, r := range repoIdx {
		report.ByRepo = append(report.ByRepo, *r)
	}
	sort.Slice(report.ByRepo, func(i, j int) bool {
		if report.ByRepo[i].Commits != report.ByRepo[j].Commits {
			return report.ByRepo[i].Commits > report.ByRepo[j].Commits
		}
		return report.ByRepo[i].RepoPath < report.ByRepo[j].RepoPath
	})
	return report
}

func renderStatsHuman(out io.Writer, r statsReport, sinceLabel string) error {
	fmt.Fprintf(out, "Period: last %s (%s -> %s)\n\n", sinceLabel, r.Since, r.Until)
	fmt.Fprintln(out, "Across all repos:")
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  Commits\t%s\n", formatThousands(r.Totals.Commits))
	fmt.Fprintf(tw, "  Events\t%s\n", formatThousands(r.Totals.Events))
	fmt.Fprintf(tw, "  Files\t%s unique\n", formatThousands(r.Totals.FilesChanged))
	fmt.Fprintf(tw, "  Bytes\t%s\n", formatBytesSigned(r.Totals.BytesChanged))
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush totals: %w", err)
	}

	fmt.Fprintln(out)
	if len(r.ByRepo) == 0 {
		fmt.Fprintln(out, "Per repo: (no data)")
	} else {
		fmt.Fprintln(out, "Per repo:")
		// Width for the repo path column.
		maxLabel := 0
		for _, repo := range r.ByRepo {
			label := homeShort(repo.RepoPath)
			if len(label) > maxLabel {
				maxLabel = len(label)
			}
		}
		// Bar chart total.
		var totalCommits int64
		for _, repo := range r.ByRepo {
			totalCommits += repo.Commits
		}
		for _, repo := range r.ByRepo {
			label := homeShort(repo.RepoPath)
			pct := 0
			if totalCommits > 0 {
				pct = int((repo.Commits * 100) / totalCommits)
			}
			bar := renderBar(pct, 18)
			fmt.Fprintf(out, "  %-*s  %5s commits  %s  %3d%%\n",
				maxLabel, label,
				formatThousands(repo.Commits), bar, pct)
		}
	}
	return nil
}

// renderBar produces an N-cell ASCII bar where filled cells use "#" and
// empty cells use ".". Width is the number of cells; pct is 0-100.
//
// We do not use Unicode block glyphs even though §7.8 example shows them —
// the example is illustrative; ASCII keeps tests + non-UTF-8 terminals
// trivially readable.
func renderBar(pct, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := (pct * width) / 100
	if filled > width {
		filled = width
	}
	return strings.Repeat("#", filled) + strings.Repeat(".", width-filled)
}

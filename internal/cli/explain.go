package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const defaultExplainLimit = 10

type explainReport struct {
	Repo           string       `json:"repo"`
	Mode           string       `json:"mode"`
	Path           string       `json:"path,omitempty"`
	Commit         string       `json:"commit,omitempty"`
	CurrentState   string       `json:"current_state,omitempty"`
	PendingEvents  int          `json:"pending_events,omitempty"`
	Explanation    string       `json:"explanation"`
	Recommended    string       `json:"recommended_next_step"`
	DecisionCursor int64        `json:"decision_cursor,omitempty"`
	Decisions      []eventEntry `json:"decisions"`
}

func newExplainCmd() *cobra.Command {
	var (
		path   string
		commit string
		last   bool
		since  int64
		limit  int
	)

	cmd := &cobra.Command{
		Use:   "explain",
		Short: "Explain why ACD did or did not commit something",
		Long: `Explain recent ACD decisions in product-facing terms.

With no flags, explain summarizes recent repo decisions and likely next steps.
Use --path for one file, --commit for an ACD or externally-handled commit, or
--last to explain HEAD.`,
		Example: `  acd explain
  acd explain --path internal/state/schema.go
  acd explain --commit HEAD
  acd explain --last
  acd explain --since 42 --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runExplain(cmd.Context(), cmd.OutOrStdout(), repo, path, commit, last, since, limit, jsonOut)
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "Explain ACD decisions for this repo-relative path")
	cmd.Flags().StringVar(&commit, "commit", "", "Explain decisions linked to this commit or revision")
	cmd.Flags().BoolVar(&last, "last", false, "Explain HEAD")
	cmd.Flags().Int64Var(&since, "since", 0, "Only consider decisions after this decision cursor ID")
	cmd.Flags().IntVar(&limit, "limit", defaultExplainLimit, "Maximum decisions to inspect")
	return cmd
}

func runExplain(ctx context.Context, out io.Writer, repo, path, commit string, last bool, since int64, limit int, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if since < 0 {
		return fmt.Errorf("acd explain: --since must be non-negative")
	}
	if limit <= 0 {
		return fmt.Errorf("acd explain: --limit must be positive")
	}
	if path != "" && (commit != "" || last) {
		return fmt.Errorf("acd explain: choose only one of --path, --commit, or --last")
	}
	if commit != "" && last {
		return fmt.Errorf("acd explain: choose only one of --commit or --last")
	}

	rec, err := eventsRepoRecord(repo)
	if err != nil {
		return err
	}
	db, err := state.Open(ctx, rec.StateDB)
	if err != nil {
		return fmt.Errorf("acd explain: open state.db for repo %s: %w", rec.Path, err)
	}
	defer db.Close()

	report, err := buildExplainReport(ctx, rec.Path, db, path, commit, last, since, limit)
	if err != nil {
		return err
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	return renderExplainHuman(out, report)
}

func buildExplainReport(ctx context.Context, repo string, db *state.DB, path, commit string, last bool, since int64, limit int) (explainReport, error) {
	report := explainReport{Repo: repo}
	var rows []state.DecisionRecord
	var err error
	switch {
	case path != "":
		report.Mode = "path"
		report.Path = path
		report.CurrentState = explainPathState(ctx, repo, path)
		report.PendingEvents = countPendingEventsForPath(ctx, db, path)
		if since > 0 {
			rows, err = state.DecisionsForPathSince(ctx, db, path, since, limit)
		} else {
			rows, err = state.DecisionsForPath(ctx, db, path, limit)
		}
	case commit != "" || last:
		report.Mode = "commit"
		rev := commit
		if last {
			rev = "HEAD"
		}
		resolved, rerr := git.RevParse(ctx, repo, rev)
		if rerr != nil {
			return report, fmt.Errorf("acd explain: resolve commit %s: %w", rev, rerr)
		}
		report.Commit = resolved
		rows, err = state.DecisionsForCommit(ctx, db, resolved, limit)
	case since > 0:
		report.Mode = "recent"
		rows, err = state.DecisionsSince(ctx, db, since, limit)
	default:
		report.Mode = "summary"
		rows, err = state.RecentDecisions(ctx, db, limit)
	}
	if err != nil {
		return report, fmt.Errorf("acd explain: query decisions: %w", err)
	}
	report.DecisionCursor = maxDecisionCursor(rows, since)
	report.Decisions = make([]eventEntry, 0, len(rows))
	for _, row := range rows {
		report.Decisions = append(report.Decisions, decisionEntry(row))
	}
	report.Explanation, report.Recommended = summarizeExplain(report)
	return report, nil
}

func summarizeExplain(report explainReport) (string, string) {
	if len(report.Decisions) == 0 {
		switch report.Mode {
		case "path":
			if report.PendingEvents > 0 {
				return "ACD has pending captured work for this path, but no product decision has been recorded yet.", "Run `acd events --path <path>` or `acd status`; replay should publish it when unblocked."
			}
			return "No ACD decision is recorded for this path yet.", "Check whether the file is ignored, sensitive, unchanged, or outside the registered repo."
		case "commit":
			return "No ACD decision is linked to this commit.", "Use `acd events --limit 20` or `git log` to inspect nearby activity."
		default:
			return "No ACD decisions have been recorded yet.", "Start the daemon and make a tracked change, then run `acd events` or `acd explain --path <path>`."
		}
	}
	latest := report.Decisions[0]
	if report.Mode != "commit" && report.Mode != "summary" && len(report.Decisions) > 1 {
		// Path queries are newest-first; since queries are oldest-first. The
		// highest cursor is the most recent durable decision either way.
		for _, d := range report.Decisions[1:] {
			if d.ID > latest.ID {
				latest = d
			}
		}
	}
	explanation := latest.UserMessage
	if explanation == "" {
		explanation = explainByKind(latest)
	}
	next := nextStepByKind(latest)
	if report.Mode == "path" && report.PendingEvents > 0 {
		next = "Replay is still pending for this path; run `acd status` to check blockers."
	}
	return explanation, next
}

func explainByKind(d eventEntry) string {
	path := d.Path
	if path == "" {
		path = "this change"
	}
	switch d.Kind {
	case state.DecisionKindProtected, state.DecisionKindSkipped:
		return fmt.Sprintf("ACD skipped %s because %s.", path, fallback(d.Reason, "it matched a protection rule"))
	case state.DecisionKindHandledExternal:
		return fmt.Sprintf("ACD did not create a duplicate commit because an external commit already handled %s.", path)
	case state.DecisionKindSupersededExternal:
		return fmt.Sprintf("ACD treated queued work for %s as superseded by external history.", path)
	case state.DecisionKindCommitted:
		return fmt.Sprintf("ACD committed %s.", path)
	case state.DecisionKindBlocked:
		return fmt.Sprintf("ACD blocked replay for %s because %s.", path, fallback(d.Reason, "the before-state was unsafe"))
	default:
		return fmt.Sprintf("Latest ACD decision for %s: %s.", path, d.Kind)
	}
}

func nextStepByKind(d eventEntry) string {
	switch d.Kind {
	case state.DecisionKindProtected, state.DecisionKindSkipped:
		return "No action is needed unless you want to change ignore/sensitive settings."
	case state.DecisionKindHandledExternal, state.DecisionKindSupersededExternal, state.DecisionKindCommitted:
		return "No action needed."
	case state.DecisionKindBlocked:
		return "Run `acd diagnose` or `acd fix --dry-run`."
	default:
		return "Run `acd events --watch` for live decisions."
	}
}

func explainPathState(ctx context.Context, repo, path string) string {
	out, err := git.Run(ctx, git.RunOpts{Dir: repo}, "status", "--porcelain=v1", "--", path)
	if err == nil && strings.TrimSpace(string(out)) != "" {
		return "worktree changed"
	}
	if _, err := git.LsTreeBlobOID(ctx, repo, "HEAD", path); err == nil {
		return "matches HEAD or unmodified"
	}
	return "unknown"
}

func countPendingEventsForPath(ctx context.Context, db *state.DB, path string) int {
	var n int
	err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE path = ? AND state = ?`,
		path, state.EventStatePending).Scan(&n)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	return n
}

func renderExplainHuman(out io.Writer, report explainReport) error {
	fmt.Fprintf(out, "Repo: %s\n", report.Repo)
	switch report.Mode {
	case "path":
		fmt.Fprintf(out, "Path: %s\n", report.Path)
		if report.CurrentState != "" {
			fmt.Fprintf(out, "Current state: %s\n", report.CurrentState)
		}
	case "commit":
		fmt.Fprintf(out, "Commit: %s\n", shortOID(report.Commit, 12))
	}
	fmt.Fprintf(out, "Explanation: %s\n", report.Explanation)
	fmt.Fprintf(out, "Next: %s\n", report.Recommended)
	if len(report.Decisions) > 0 {
		fmt.Fprintln(out, "\nRecent decisions:")
		return renderEventsTable(out, report.Decisions)
	}
	return nil
}

func shortOID(oid string, n int) string {
	if len(oid) <= n {
		return oid
	}
	return oid[:n]
}

func fallback(v, fb string) string {
	if v != "" {
		return v
	}
	return fb
}

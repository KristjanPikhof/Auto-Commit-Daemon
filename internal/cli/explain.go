package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const defaultExplainLimit = 10

type explainReport struct {
	Repo                    string         `json:"repo"`
	Mode                    string         `json:"mode"`
	Path                    string         `json:"path,omitempty"`
	Commit                  string         `json:"commit,omitempty"`
	CurrentState            string         `json:"current_state,omitempty"`
	PendingEvents           int            `json:"pending_events,omitempty"`
	Explanation             string         `json:"explanation"`
	Recommended             string         `json:"recommended_next_step"`
	DecisionCursor          int64          `json:"decision_cursor,omitempty"`
	DecisionLedgerAvailable bool           `json:"decision_ledger_available"`
	Decisions               []eventEntry   `json:"decisions"`
	IntentV2                intentV2Report `json:"intent_v2"`
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
	db, err := openStateDBReadOnly(ctx, rec.StateDB)
	if err != nil {
		return fmt.Errorf("acd explain: open state.db read-only for repo %s: %w", rec.Path, err)
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

func buildExplainReport(ctx context.Context, repo string, db *sql.DB, path, commit string, last bool, since int64, limit int) (explainReport, error) {
	report := explainReport{Repo: repo}
	intentV2, err := loadIntentV2Report(ctx, db)
	if err != nil {
		return report, fmt.Errorf("acd explain: Intent v2 summary: %w", err)
	}
	report.IntentV2 = intentV2
	var rows []state.DecisionRecord
	hasLedger, err := sqliteTableExists(ctx, db, "decision_records")
	if err != nil {
		return report, fmt.Errorf("acd explain: decision table check: %w", err)
	}
	report.DecisionLedgerAvailable = hasLedger
	switch {
	case path != "":
		report.Mode = "path"
		report.Path = path
		report.CurrentState = explainPathState(ctx, repo, path)
		report.PendingEvents = countPendingEventsForPath(ctx, db, path)
		if !hasLedger {
			report.DecisionCursor = since
			report.Explanation, report.Recommended = summarizeExplain(report)
			return report, nil
		}
		if since > 0 {
			rows, err = loadDecisionEvents(ctx, db, path, since, limit)
		} else {
			rows, err = loadDecisionEvents(ctx, db, path, 0, limit)
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
		if !hasLedger {
			report.DecisionCursor = since
			report.Explanation, report.Recommended = summarizeExplain(report)
			return report, nil
		}
		rows, err = queryDecisionRecordsSQL(ctx, db, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE commit_oid = ?
ORDER BY id DESC
LIMIT ?`, resolved, limit)
	case since > 0:
		report.Mode = "recent"
		if !hasLedger {
			report.DecisionCursor = since
			report.Explanation, report.Recommended = summarizeExplain(report)
			return report, nil
		}
		rows, err = loadDecisionEvents(ctx, db, "", since, limit)
	default:
		report.Mode = "summary"
		if !hasLedger {
			report.DecisionCursor = since
			report.Explanation, report.Recommended = summarizeExplain(report)
			return report, nil
		}
		rows, err = loadDecisionEvents(ctx, db, "", 0, limit)
	}
	if err != nil {
		return report, fmt.Errorf("acd explain: query decisions: %w", err)
	}
	report.DecisionCursor = maxDecisionCursor(rows, since)
	report.Decisions = make([]eventEntry, 0, len(rows))
	for _, row := range rows {
		report.Decisions = append(report.Decisions, decisionEntry(row))
	}
	if err := enrichEventEntries(ctx, db, report.Decisions); err != nil {
		return report, fmt.Errorf("acd explain: enrich decisions: %w", err)
	}
	report.Explanation, report.Recommended = summarizeExplain(report)
	return report, nil
}

func summarizeExplain(report explainReport) (string, string) {
	if len(report.Decisions) == 0 {
		if !report.DecisionLedgerAvailable {
			return missingDecisionLedgerMessage, "Run `acd status`; start or restart acd with a current version if you need product decision history."
		}
		if report.DecisionCursor > 0 && report.Mode == "recent" {
			return "No ACD decisions have been recorded after that cursor.", "Run `acd events --watch` to stream newly appended decisions."
		}
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
	for _, d := range report.Decisions[1:] {
		if d.ID > latest.ID {
			latest = d
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
	case state.DecisionKindIntentDeferred:
		return fmt.Sprintf("ACD deferred %s from the current intent planning window because %s.", path, fallback(d.Reason, "the planner selected a different group"))
	case state.DecisionKindIntentForced:
		return fmt.Sprintf("ACD forced %s into a one-item intent planning window after repeated deferrals.", path)
	case state.DecisionKindIntentPlannerError:
		return fmt.Sprintf("ACD rejected an intent planner result for %s because %s.", path, fallback(d.Reason, "the planner output failed validation"))
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
	case state.DecisionKindIntentDeferred:
		return "No action needed; replay will reconsider this capture in a later intent planning window."
	case state.DecisionKindIntentForced:
		return "No action needed; this capture is being forced through a one-item planning window."
	case state.DecisionKindIntentPlannerError:
		return "No action needed unless this repeats; ACD falls back to deterministic planning for safety."
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

func countPendingEventsForPath(ctx context.Context, db *sql.DB, path string) int {
	ok, err := sqliteTableExists(ctx, db, "capture_events")
	if err != nil || !ok {
		return 0
	}
	var n int
	err = db.QueryRowContext(ctx,
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
	renderIntentV2Human(out, report.IntentV2)
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

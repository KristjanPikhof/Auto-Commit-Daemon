package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	defaultEventsLimit         = 50
	defaultEventsWatchInterval = 250 * time.Millisecond
)

var eventsWatchPollInterval = defaultEventsWatchInterval

// eventsWatchReadyHook is fired once after the watch path captures its cursor
// and before entering followEvents. Tests use it to synchronize appends with
// the moment the watcher is live; production leaves it nil.
var eventsWatchReadyHook func()

type eventsReport struct {
	Repo    string       `json:"repo"`
	Cursor  int64        `json:"cursor"`
	Events  []eventEntry `json:"events"`
	Message string       `json:"message,omitempty"`
}

type eventEntry struct {
	ID               int64                       `json:"id"`
	Timestamp        int64                       `json:"timestamp"`
	Time             string                      `json:"time"`
	Kind             string                      `json:"kind"`
	Path             string                      `json:"path,omitempty"`
	Reason           string                      `json:"reason,omitempty"`
	EventSeq         int64                       `json:"event_seq,omitempty"`
	HeadSHA          string                      `json:"head_sha,omitempty"`
	CommitOID        string                      `json:"commit_oid,omitempty"`
	BranchRef        string                      `json:"branch_ref,omitempty"`
	BranchGeneration int64                       `json:"branch_generation,omitempty"`
	ActionTaken      string                      `json:"action_taken,omitempty"`
	UserMessage      string                      `json:"user_message,omitempty"`
	DecisionTS       float64                     `json:"decision_ts"`
	GroupedSeqs      []int64                     `json:"grouped_seqs,omitempty"`
	GroupSize        int                         `json:"group_size,omitempty"`
	IntentGroup      bool                        `json:"intent_group,omitempty"`
	PlannerWindow    *intentPlannerWindowSummary `json:"planner_window,omitempty"`
	Deferred         bool                        `json:"deferred,omitempty"`
	ForcedAging      bool                        `json:"forced_aging,omitempty"`
	PlannerError     bool                        `json:"planner_error,omitempty"`
}

func newEventsCmd() *cobra.Command {
	var (
		path     string
		since    int64
		limit    int
		watch    bool
		interval time.Duration
	)

	cmd := &cobra.Command{
		Use:   "events",
		Short: "Show product decisions for the current repo",
		Long: `Show product-facing ACD decisions for the current repo.

The events command reads the durable decision ledger instead of raw daemon
JSONL logs. Use --path to focus on one path, --since with a decision cursor to
resume polling, and --watch to stream appended decisions until interrupted.
With --watch and no --since, events prints only decisions appended after watch starts.`,
		Example: `  acd events
  acd events --path internal/state/schema.go
  acd events --since 42 --limit 100
  acd events --watch
  acd events --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			ctx := cmd.Context()
			stop := func() {}
			if watch {
				ctx, stop = signal.NotifyContext(ctx, os.Interrupt)
			}
			defer stop()
			return runEvents(ctx, cmd.OutOrStdout(), repo, path, since, limit, watch, interval, jsonOut)
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "Only show decisions for this repo-relative path")
	cmd.Flags().Int64Var(&since, "since", 0, "Only show decisions after this decision cursor ID")
	cmd.Flags().IntVar(&limit, "limit", defaultEventsLimit, "Maximum decisions to print per query")
	cmd.Flags().BoolVar(&watch, "watch", false, "Poll for appended decisions until interrupted; without --since, start at the current ledger tail")
	cmd.Flags().DurationVar(&interval, "interval", defaultEventsWatchInterval, "Poll interval for --watch (Go duration)")
	return cmd
}

func runEvents(ctx context.Context, out io.Writer, repo, path string, since int64, limit int, watch bool, interval time.Duration, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if since < 0 {
		return fmt.Errorf("acd events: --since must be non-negative")
	}
	if limit <= 0 {
		return fmt.Errorf("acd events: --limit must be positive")
	}
	rec, err := eventsRepoRecord(repo)
	if err != nil {
		return err
	}
	db, err := openStateDBReadOnly(ctx, rec.StateDB)
	if err != nil {
		return fmt.Errorf("acd events: open state.db read-only for repo %s: %w", rec.Path, err)
	}
	defer db.Close()

	hasLedger, err := sqliteTableExists(ctx, db, "decision_records")
	if err != nil {
		return fmt.Errorf("acd events: decision table check: %w", err)
	}
	if !hasLedger {
		return renderEvents(ctx, out, db, rec.Path, nil, since, jsonOut, true, missingDecisionLedgerMessage)
	}

	var rows []state.DecisionRecord
	cursor := since
	if watch && since == 0 {
		cursor, err = latestDecisionIDSQL(ctx, db)
		if err != nil {
			return fmt.Errorf("acd events: latest cursor: %w", err)
		}
	} else {
		rows, err = loadDecisionEvents(ctx, db, path, since, limit)
		if err != nil {
			return err
		}
		cursor = maxDecisionCursor(rows, since)
		if !watch && since == 0 && cursor == 0 {
			cursor, err = latestDecisionIDSQL(ctx, db)
			if err != nil {
				return fmt.Errorf("acd events: latest cursor: %w", err)
			}
		}
	}
	if len(rows) > 0 || !watch {
		if err := renderEvents(ctx, out, db, rec.Path, rows, cursor, jsonOut, !watch, ""); err != nil {
			return err
		}
	}
	if !watch {
		return nil
	}
	if eventsWatchReadyHook != nil {
		eventsWatchReadyHook()
	}
	return followEvents(ctx, out, db, rec.Path, path, cursor, limit, interval, jsonOut)
}

const missingDecisionLedgerMessage = "Decision ledger is not available in this state.db yet; start or restart acd with a current version to record product decisions."

func loadDecisionEvents(ctx context.Context, db *sql.DB, path string, since int64, limit int) ([]state.DecisionRecord, error) {
	var (
		rows []state.DecisionRecord
		err  error
	)
	switch {
	case path != "" && since > 0:
		rows, err = queryDecisionRecordsSQL(ctx, db, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE path = ? AND id > ?
ORDER BY id ASC
LIMIT ?`, path, since, limit)
		if err != nil {
			return nil, fmt.Errorf("acd events: query path decisions since cursor: %w", err)
		}
	case path != "":
		rows, err = queryDecisionRecordsSQL(ctx, db, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE path = ?
ORDER BY id DESC
LIMIT ?`, path, limit)
		if err != nil {
			return nil, fmt.Errorf("acd events: query path decisions: %w", err)
		}
	case since > 0:
		rows, err = queryDecisionRecordsSQL(ctx, db, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE id > ?
ORDER BY id ASC
LIMIT ?`, since, limit)
		if err != nil {
			return nil, fmt.Errorf("acd events: query decisions since cursor: %w", err)
		}
	default:
		rows, err = queryDecisionRecordsSQL(ctx, db, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
ORDER BY id DESC
LIMIT ?`, limit)
		if err != nil {
			return nil, fmt.Errorf("acd events: query recent decisions: %w", err)
		}
	}
	return rows, nil
}

func queryDecisionRecordsSQL(ctx context.Context, db *sql.DB, q string, args ...any) ([]state.DecisionRecord, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []state.DecisionRecord
	for rows.Next() {
		var rec state.DecisionRecord
		if err := rows.Scan(&rec.ID, &rec.DecisionTS, &rec.Kind, &rec.Path, &rec.Reason,
			&rec.EventSeq, &rec.HeadSHA, &rec.CommitOID, &rec.BranchRef,
			&rec.BranchGeneration, &rec.ActionTaken, &rec.UserMessage); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func latestDecisionIDSQL(ctx context.Context, db *sql.DB) (int64, error) {
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(id) FROM decision_records`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func eventsRepoRecord(repo string) (central.RepoRecord, error) {
	rec, _, abs, err := lookupRegisteredRepo("events", repo)
	if err != nil {
		return central.RepoRecord{}, err
	}
	if !fileExists(rec.StateDB) {
		return central.RepoRecord{}, fmt.Errorf("acd events: state.db missing for repo %s (try `acd start --repo %s`)", abs, abs)
	}
	return rec, nil
}

func followEvents(ctx context.Context, out io.Writer, db *sql.DB, repo, path string, cursor int64, limit int, interval time.Duration, jsonOut bool) error {
	if interval <= 0 {
		return fmt.Errorf("acd events: --interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		rows, err := loadDecisionEvents(ctx, db, path, cursor, limit)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if len(rows) == 0 {
			continue
		}
		cursor = maxDecisionCursor(rows, cursor)
		if err := renderEvents(ctx, out, db, repo, rows, cursor, jsonOut, false, ""); err != nil {
			return err
		}
	}
}

func renderEvents(ctx context.Context, out io.Writer, db *sql.DB, repo string, rows []state.DecisionRecord, cursor int64, jsonOut bool, includeEnvelope bool, message string) error {
	entries := make([]eventEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, decisionEntry(row))
	}
	if db != nil {
		if err := enrichEventEntries(ctx, db, entries); err != nil {
			return err
		}
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		if includeEnvelope {
			enc.SetIndent("", "  ")
			return enc.Encode(eventsReport{Repo: repo, Cursor: cursor, Events: entries, Message: message})
		}
		for _, entry := range entries {
			if err := enc.Encode(entry); err != nil {
				return fmt.Errorf("acd events: write json event: %w", err)
			}
		}
		return nil
	}
	if includeEnvelope {
		fmt.Fprintf(out, "Repo: %s\n", repo)
		fmt.Fprintf(out, "Cursor: %d\n\n", cursor)
		if len(entries) == 0 {
			if message == "" {
				message = "No decisions recorded yet."
			}
			_, err := fmt.Fprintln(out, message)
			return err
		}
	}
	return renderEventsTable(out, entries)
}

func renderEventsTable(out io.Writer, entries []eventEntry) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tTIME\tKIND\tPATH\tACTION\tMESSAGE")
	for _, entry := range entries {
		path := entry.Path
		if path == "" {
			path = "-"
		}
		action := entry.ActionTaken
		if action == "" {
			action = "-"
		}
		message := entry.UserMessage
		if message == "" {
			message = entry.Reason
		}
		if message == "" {
			message = "-"
		}
		if len(entry.GroupedSeqs) > 1 {
			message = fmt.Sprintf("%s seqs=%s", message, formatSeqs(entry.GroupedSeqs))
		}
		if entry.PlannerWindow != nil {
			message = fmt.Sprintf("%s planner_window=%d offered=%s",
				message, entry.PlannerWindow.ID, formatSeqs(entry.PlannerWindow.OfferedSeqs))
			if len(entry.PlannerWindow.HiddenSeqs) > 0 {
				message = fmt.Sprintf("%s hidden=%s", message, formatSeqs(entry.PlannerWindow.HiddenSeqs))
			}
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n",
			entry.ID, entry.Time, entry.Kind, path, action, message)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("acd events: flush output: %w", err)
	}
	return nil
}

func formatSeqs(seqs []int64) string {
	if len(seqs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(seqs))
	for _, seq := range seqs {
		parts = append(parts, strconv.FormatInt(seq, 10))
	}
	return strings.Join(parts, ",")
}

func decisionEntry(row state.DecisionRecord) eventEntry {
	ts := int64(row.DecisionTS)
	entry := eventEntry{
		ID:         row.ID,
		Timestamp:  ts,
		Time:       time.Unix(ts, 0).Format(time.RFC3339),
		Kind:       row.Kind,
		DecisionTS: row.DecisionTS,
	}
	if row.Path.Valid {
		entry.Path = row.Path.String
	}
	if row.Reason.Valid {
		entry.Reason = row.Reason.String
	}
	if row.EventSeq.Valid {
		entry.EventSeq = row.EventSeq.Int64
	}
	if row.HeadSHA.Valid {
		entry.HeadSHA = row.HeadSHA.String
	}
	if row.CommitOID.Valid {
		entry.CommitOID = row.CommitOID.String
	}
	if row.BranchRef.Valid {
		entry.BranchRef = row.BranchRef.String
	}
	if row.BranchGeneration.Valid {
		entry.BranchGeneration = row.BranchGeneration.Int64
	}
	if row.ActionTaken.Valid {
		entry.ActionTaken = row.ActionTaken.String
	}
	if row.UserMessage.Valid {
		entry.UserMessage = row.UserMessage.String
	}
	entry.IntentGroup = entry.ActionTaken == "intent_group_committed" || strings.HasPrefix(entry.Reason, "intent_group:")
	entry.Deferred = row.Kind == state.DecisionKindIntentDeferred
	entry.ForcedAging = row.Kind == state.DecisionKindIntentForced
	entry.PlannerError = row.Kind == state.DecisionKindIntentPlannerError
	return entry
}

func enrichEventEntries(ctx context.Context, db *sql.DB, entries []eventEntry) error {
	if len(entries) == 0 || db == nil {
		return nil
	}
	cache := map[string]decisionCommitSummary{}
	hasPlannerWindows, err := sqliteTableExists(ctx, db, "intent_planner_window_events")
	if err != nil {
		return fmt.Errorf("acd events: intent planner window table check: %w", err)
	}
	for i := range entries {
		commit := entries[i].CommitOID
		if commit != "" {
			summary, ok := cache[commit]
			if !ok {
				var err error
				summary, err = decisionCommitSummarySQL(ctx, db, commit)
				if err != nil {
					return err
				}
				cache[commit] = summary
			}
			if len(summary.Seqs) > 1 {
				entries[i].GroupedSeqs = append([]int64(nil), summary.Seqs...)
				entries[i].GroupSize = len(summary.Seqs)
			}
			if summary.IntentGroup {
				entries[i].IntentGroup = true
			}
		}
		if hasPlannerWindows && entries[i].EventSeq > 0 {
			window, err := loadIntentPlannerWindowForEventSQL(ctx, db, entries[i].EventSeq)
			if err != nil {
				return err
			}
			entries[i].PlannerWindow = window
		}
	}
	return nil
}

type decisionCommitSummary struct {
	Seqs        []int64
	IntentGroup bool
}

func decisionCommitSummarySQL(ctx context.Context, db *sql.DB, commitOID string) (decisionCommitSummary, error) {
	rows, err := db.QueryContext(ctx, `
SELECT event_seq, action_taken, reason
FROM decision_records
WHERE commit_oid = ? AND event_seq IS NOT NULL
ORDER BY event_seq ASC`, commitOID)
	if err != nil {
		return decisionCommitSummary{}, fmt.Errorf("acd events: query commit grouped seqs: %w", err)
	}
	defer rows.Close()
	var summary decisionCommitSummary
	seen := map[int64]struct{}{}
	for rows.Next() {
		var seq int64
		var action, reason sql.NullString
		if err := rows.Scan(&seq, &action, &reason); err != nil {
			return decisionCommitSummary{}, fmt.Errorf("acd events: scan commit grouped seq: %w", err)
		}
		if _, ok := seen[seq]; !ok {
			summary.Seqs = append(summary.Seqs, seq)
			seen[seq] = struct{}{}
		}
		if action.String == "intent_group_committed" || strings.HasPrefix(reason.String, "intent_group:") {
			summary.IntentGroup = true
		}
	}
	if err := rows.Err(); err != nil {
		return decisionCommitSummary{}, fmt.Errorf("acd events: iter commit grouped seqs: %w", err)
	}
	return summary, nil
}

func maxDecisionCursor(rows []state.DecisionRecord, fallback int64) int64 {
	maxID := fallback
	for _, row := range rows {
		if row.ID > maxID {
			maxID = row.ID
		}
	}
	return maxID
}

func parseDecisionCursor(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid decision cursor %q: %w", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("decision cursor must be non-negative: %d", n)
	}
	return n, nil
}

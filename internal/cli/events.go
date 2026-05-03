package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	defaultEventsLimit         = 50
	defaultEventsWatchInterval = 250 * time.Millisecond
)

var eventsWatchPollInterval = defaultEventsWatchInterval

type eventsReport struct {
	Repo   string       `json:"repo"`
	Cursor int64        `json:"cursor"`
	Events []eventEntry `json:"events"`
}

type eventEntry struct {
	ID               int64   `json:"id"`
	Timestamp        int64   `json:"timestamp"`
	Time             string  `json:"time"`
	Kind             string  `json:"kind"`
	Path             string  `json:"path,omitempty"`
	Reason           string  `json:"reason,omitempty"`
	EventSeq         int64   `json:"event_seq,omitempty"`
	HeadSHA          string  `json:"head_sha,omitempty"`
	CommitOID        string  `json:"commit_oid,omitempty"`
	BranchRef        string  `json:"branch_ref,omitempty"`
	BranchGeneration int64   `json:"branch_generation,omitempty"`
	ActionTaken      string  `json:"action_taken,omitempty"`
	UserMessage      string  `json:"user_message,omitempty"`
	DecisionTS       float64 `json:"decision_ts"`
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
resume polling, and --watch to stream appended decisions until interrupted.`,
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
	cmd.Flags().BoolVar(&watch, "watch", false, "Poll for appended decisions until interrupted")
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
	db, err := state.Open(ctx, rec.StateDB)
	if err != nil {
		return fmt.Errorf("acd events: open state.db for repo %s: %w", rec.Path, err)
	}
	defer db.Close()

	rows, err := loadDecisionEvents(ctx, db, path, since, limit)
	if err != nil {
		return err
	}
	cursor := maxDecisionCursor(rows, since)
	if !watch && since == 0 && cursor == 0 {
		cursor, err = state.LatestDecisionID(ctx, db)
		if err != nil {
			return fmt.Errorf("acd events: latest cursor: %w", err)
		}
	}
	if err := renderEvents(out, rec.Path, rows, cursor, jsonOut, !watch); err != nil {
		return err
	}
	if !watch {
		return nil
	}
	return followEvents(ctx, out, db, rec.Path, path, cursor, limit, interval, jsonOut)
}

func eventsRepoRecord(repo string) (central.RepoRecord, error) {
	abs, err := resolveRepo(repo)
	if err != nil {
		return central.RepoRecord{}, err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return central.RepoRecord{}, fmt.Errorf("acd events: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return central.RepoRecord{}, fmt.Errorf("acd events: load registry: %w", err)
	}
	rec, ok := findRepo(reg, abs)
	if !ok {
		return central.RepoRecord{}, fmt.Errorf("acd events: repo %s is not registered (try `acd start --repo %s`)", abs, abs)
	}
	if !fileExists(rec.StateDB) {
		return central.RepoRecord{}, fmt.Errorf("acd events: state.db missing for repo %s (try `acd start --repo %s`)", abs, abs)
	}
	return rec, nil
}

func loadDecisionEvents(ctx context.Context, db *state.DB, path string, since int64, limit int) ([]state.DecisionRecord, error) {
	switch {
	case path != "" && since > 0:
		rows, err := state.DecisionsForPathSince(ctx, db, path, since, limit)
		if err != nil {
			return nil, fmt.Errorf("acd events: query path decisions since cursor: %w", err)
		}
		return rows, nil
	case path != "":
		rows, err := state.DecisionsForPath(ctx, db, path, limit)
		if err != nil {
			return nil, fmt.Errorf("acd events: query path decisions: %w", err)
		}
		return rows, nil
	case since > 0:
		rows, err := state.DecisionsSince(ctx, db, since, limit)
		if err != nil {
			return nil, fmt.Errorf("acd events: query decisions since cursor: %w", err)
		}
		return rows, nil
	default:
		rows, err := state.RecentDecisions(ctx, db, limit)
		if err != nil {
			return nil, fmt.Errorf("acd events: query recent decisions: %w", err)
		}
		return rows, nil
	}
}

func followEvents(ctx context.Context, out io.Writer, db *state.DB, repo, path string, cursor int64, limit int, interval time.Duration, jsonOut bool) error {
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
		if err := renderEvents(out, repo, rows, cursor, jsonOut, false); err != nil {
			return err
		}
	}
}

func renderEvents(out io.Writer, repo string, rows []state.DecisionRecord, cursor int64, jsonOut bool, includeEnvelope bool) error {
	entries := make([]eventEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, decisionEntry(row))
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		if includeEnvelope {
			enc.SetIndent("", "  ")
			return enc.Encode(eventsReport{Repo: repo, Cursor: cursor, Events: entries})
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
			_, err := fmt.Fprintln(out, "No decisions recorded yet.")
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
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n",
			entry.ID, entry.Time, entry.Kind, path, action, message)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("acd events: flush output: %w", err)
	}
	return nil
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
	return entry
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

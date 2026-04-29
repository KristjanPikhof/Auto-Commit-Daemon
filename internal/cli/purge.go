// purge.go implements `acd purge-events` — operator-driven cleanup of
// non-published capture_events rows.
//
// Why this exists: when a parallel committer (e.g. the atomic-commit
// hook plugin) lands the same edit acd captured, replay sees the
// captured `before_oid` no longer matching HEAD and terminally settles
// the event in `blocked_conflict`. Per architecture invariant the
// blocked row forms a seq barrier — every later pending row hides
// behind it in PendingEvents until an operator intervenes. The legacy
// recovery story was raw sqlite3 surgery, which is fine for engineers
// but unhelpful for users diagnosing a stuck queue.
//
// `acd recover` already exists but does a different job: it RETARGETS
// stale rows onto the current HEAD/generation (preserving them). When
// the captured edits are already in the working tree (committed by the
// parallel committer), retarget just produces another mismatch on the
// next replay pass. Outright deletion is the right move for that case
// — hence this command.
//
// Safety scaffolding mirrors recover.go:
//   - Refuse while the daemon is alive.
//   - Backup state.db before mutating.
//   - --yes required to apply, --dry-run prints the plan without
//     touching the DB.
package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// purgePlan is the JSON/human shape returned by `acd purge-events`.
type purgePlan struct {
	Repo        string         `json:"repo"`
	StateDB     string         `json:"state_db"`
	States      []string       `json:"states"`
	StateCounts map[string]int `json:"state_counts"`
	DryRun      bool           `json:"dry_run"`
	BackupPath  string         `json:"backup_path,omitempty"`
	RowsDeleted int64          `json:"rows_deleted"`
	BarrierLift bool           `json:"barrier_lift"`
}

func newPurgeEventsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "purge-events",
		Short: "Delete non-published capture_events rows (e.g. clear blocked + pending barrier)",
		Long: `Delete capture_events rows in the selected states.

Use this when a parallel committer (e.g. the atomic-commit hook plugin)
has already landed the work acd captured, leaving rows stuck in
'blocked_conflict' (forming a seq barrier) and a tail of 'pending' rows
that can never replay.

` + "`acd recover`" + ` retargets stale rows onto the current HEAD;
` + "`acd purge-events`" + ` deletes them outright. Pick the right tool
for the situation.`,
		RunE: func(c *cobra.Command, args []string) error {
			repo, _ := c.Flags().GetString("repo")
			blocked, _ := c.Flags().GetBool("blocked")
			pending, _ := c.Flags().GetBool("pending")
			failed, _ := c.Flags().GetBool("failed")
			all, _ := c.Flags().GetBool("all")
			yes, _ := c.Flags().GetBool("yes")
			dryRun, _ := c.Flags().GetBool("dry-run")
			jsonOut, _ := c.Flags().GetBool("json")
			return runPurgeEvents(c.Context(), c.OutOrStdout(),
				repo, blocked, pending, failed, all, yes, dryRun, jsonOut)
		},
	}
	cmd.Flags().Bool("blocked", false, "Include rows in state=blocked_conflict")
	cmd.Flags().Bool("pending", false, "Include rows in state=pending")
	cmd.Flags().Bool("failed", false, "Include rows in state=failed")
	cmd.Flags().Bool("all", false, "Shortcut for --blocked --pending --failed")
	cmd.Flags().Bool("yes", false, "Apply the deletion (without this, only --dry-run is allowed)")
	cmd.Flags().Bool("dry-run", false, "Show what would be deleted without mutating state.db")
	return cmd
}

func runPurgeEvents(ctx context.Context, out io.Writer,
	repo string, blocked, pending, failed, all, yes, dryRun, jsonOut bool,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	states := selectPurgeStates(blocked, pending, failed, all)
	if len(states) == 0 {
		return fmt.Errorf("acd purge-events: pass at least one of --blocked / --pending / --failed (or --all)")
	}
	if !dryRun && !yes {
		return fmt.Errorf("acd purge-events: refusing to mutate state without --yes (use --dry-run first)")
	}

	rec, err := recoverRepoRecord(repo)
	if err != nil {
		return err
	}

	plan, err := buildPurgePlan(ctx, rec.Path, rec.StateDB, states, dryRun)
	if err != nil {
		return err
	}
	if dryRun {
		return renderPurge(out, plan, jsonOut)
	}

	if err := applyPurgePlan(ctx, rec.StateDB, &plan); err != nil {
		return err
	}
	return renderPurge(out, plan, jsonOut)
}

// selectPurgeStates resolves the flags into a deterministic, sorted
// slice of state names. Tests pin the ordering, and downstream SQL
// substitutes positional parameters — both want a stable order.
func selectPurgeStates(blocked, pending, failed, all bool) []string {
	if all {
		blocked = true
		pending = true
		failed = true
	}
	set := map[string]bool{}
	if blocked {
		set[state.EventStateBlockedConflict] = true
	}
	if pending {
		set[state.EventStatePending] = true
	}
	if failed {
		set[state.EventStateFailed] = true
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func buildPurgePlan(ctx context.Context, repo, stateDB string, states []string, dryRun bool) (purgePlan, error) {
	conn, err := openStateDBReadOnly(ctx, stateDB)
	if err != nil {
		return purgePlan{}, fmt.Errorf("acd purge-events: open state.db read-only: %w", err)
	}
	defer conn.Close()

	if err := refuseRecoverWhenDaemonAliveSQL(ctx, conn); err != nil {
		// Reuse the recover-side guard verbatim; only the verb in the
		// error string differs, and that's tolerable for now.
		return purgePlan{}, err
	}

	counts, err := countEventsByState(ctx, conn, states)
	if err != nil {
		return purgePlan{}, err
	}
	plan := purgePlan{
		Repo:        repo,
		StateDB:     stateDB,
		States:      states,
		StateCounts: counts,
		DryRun:      dryRun,
		BarrierLift: counts[state.EventStateBlockedConflict] > 0,
	}
	return plan, nil
}

func countEventsByState(ctx context.Context, conn *sql.DB, states []string) (map[string]int, error) {
	counts := map[string]int{}
	for _, s := range states {
		var n int
		if err := conn.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM capture_events WHERE state = ?`, s,
		).Scan(&n); err != nil {
			return nil, fmt.Errorf("count state=%s: %w", s, err)
		}
		counts[s] = n
	}
	return counts, nil
}

// applyPurgePlan deletes capture_events rows matching plan.States and
// clears matching publish_state + last_replay_conflict breadcrumbs so
// `acd list` / `acd status` read clean immediately after.
//
// Wrapped in a single transaction. Backup created before opening the
// writable handle so a torn write does not leave the user without a
// recovery path.
func applyPurgePlan(ctx context.Context, stateDB string, plan *purgePlan) error {
	backup, err := backupStateDB(stateDB)
	if err != nil {
		return fmt.Errorf("acd purge-events: backup state.db: %w", err)
	}
	plan.BackupPath = backup

	db, err := state.Open(ctx, stateDB)
	if err != nil {
		return fmt.Errorf("acd purge-events: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := refuseRecoverWhenDaemonAlive(ctx, db); err != nil {
		return err
	}

	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("acd purge-events: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Build "state IN (?,?,…)" placeholder list deterministically.
	placeholders := make([]string, len(plan.States))
	args := make([]any, len(plan.States))
	for i, s := range plan.States {
		placeholders[i] = "?"
		args[i] = s
	}
	delQuery := fmt.Sprintf(`DELETE FROM capture_events WHERE state IN (%s)`,
		strings.Join(placeholders, ","))

	res, err := tx.ExecContext(ctx, delQuery, args...)
	if err != nil {
		return fmt.Errorf("acd purge-events: delete capture_events: %w", err)
	}
	if n, rerr := res.RowsAffected(); rerr == nil {
		plan.RowsDeleted = n
	}

	// Lift the barrier in publish_state if we deleted a blocked_conflict.
	// The singleton row is keyed at id=1; clear status + error and zero
	// the conflict cursor fields so `acd status` reads "ok".
	if plan.BarrierLift {
		if _, err := tx.ExecContext(ctx, `
UPDATE publish_state
SET status = 'ok', error = NULL, conflict_seq = 0
WHERE id = 1 AND status = 'blocked_conflict'`); err != nil {
			return fmt.Errorf("acd purge-events: clear publish_state: %w", err)
		}
		// Drop the human-readable breadcrumbs so `acd status` does not
		// keep nagging about a conflict that no longer has a row.
		for _, key := range []string{
			"last_replay_conflict",
			"last_replay_conflict_legacy",
			"last_replay_error",
		} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM daemon_meta WHERE key = ?`, key); err != nil {
				return fmt.Errorf("acd purge-events: clear daemon_meta %s: %w", key, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("acd purge-events: commit: %w", err)
	}
	return nil
}

func renderPurge(out io.Writer, plan purgePlan, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}
	mode := "planned"
	if !plan.DryRun {
		mode = "applied"
	}
	fmt.Fprintf(out, "purge-events %s for %s\n", mode, plan.Repo)
	fmt.Fprintf(out, "States: %s\n", strings.Join(plan.States, ", "))
	for _, s := range plan.States {
		fmt.Fprintf(out, "  %s: %d rows\n", s, plan.StateCounts[s])
	}
	if plan.BarrierLift {
		fmt.Fprintln(out, "Will lift the blocked_conflict barrier in publish_state.")
	}
	if plan.BackupPath != "" {
		fmt.Fprintf(out, "Backup: %s\n", plan.BackupPath)
	}
	if !plan.DryRun {
		fmt.Fprintf(out, "Rows deleted: %d (at %s)\n", plan.RowsDeleted,
			time.Now().UTC().Format(time.RFC3339))
	} else {
		fmt.Fprintln(out, "(dry-run; pass --yes to apply)")
	}
	return nil
}

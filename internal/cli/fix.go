package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	fixActionClearExpiredManualPause = "clear_expired_manual_pause"
	fixActionClearDrainedBackpressure = "clear_drained_backpressure"
	fixActionDeleteObsoleteBarrier    = "delete_obsolete_barrier"
	fixActionMarkExternalPublished    = "mark_external_published"
)

type fixPlan struct {
	Repo              string      `json:"repo"`
	StateDB           string      `json:"state_db"`
	GitDir            string      `json:"git_dir,omitempty"`
	CurrentBranchRef  string      `json:"current_branch_ref,omitempty"`
	CurrentHead       string      `json:"current_head,omitempty"`
	Generation        int64       `json:"generation,omitempty"`
	DryRun            bool        `json:"dry_run"`
	BackupPath        string      `json:"backup_path,omitempty"`
	Actions           []fixAction `json:"actions"`
	Unsafe            []string    `json:"unsafe,omitempty"`
	Suggestions        []string    `json:"suggestions,omitempty"`
	RowsChanged       int64       `json:"rows_changed"`
	ManualPauseRemoved bool       `json:"manual_pause_removed,omitempty"`
	ManualPausePath    string     `json:"manual_pause_path,omitempty"`
}

type fixAction struct {
	ID          string  `json:"id"`
	Kind        string  `json:"kind"`
	Description string  `json:"description"`
	Reason      string  `json:"reason,omitempty"`
	Seq         int64   `json:"seq,omitempty"`
	Path        string  `json:"path,omitempty"`
	DecisionID  int64   `json:"decision_id,omitempty"`
	CommitOID   string  `json:"commit_oid,omitempty"`
	RowsChanged int64   `json:"rows_changed,omitempty"`
	Applied     bool    `json:"applied,omitempty"`
	SetAt       string  `json:"set_at,omitempty"`
}

func newFixCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix",
		Short: "Plan or apply guided safe remediation for a stuck repo",
		Long: `Plan or apply conservative remediation for common stuck ACD states.

The fix planner is read-only by default with --dry-run. Applying changes
requires --yes, refuses while a live daemon owns the state DB, backs up
state.db first, and only mutates rows whose existing state already proves a
safe outcome.`,
		Example: `  acd fix --dry-run
  acd fix --dry-run --json
  acd fix --yes
  acd fix --repo /path/to/repo --yes --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			yes, _ := cmd.Flags().GetBool("yes")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runFix(cmd.Context(), cmd.OutOrStdout(), repo, dryRun, yes, jsonOut)
		},
	}
	cmd.Flags().Bool("dry-run", false, "Show the guided remediation plan without mutating state")
	cmd.Flags().Bool("yes", false, "Apply safe remediation actions")
	return cmd
}

func runFix(ctx context.Context, out io.Writer, repo string, dryRun, yes, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !dryRun && !yes {
		return fmt.Errorf("acd fix: refusing to mutate state without --yes (use --dry-run first)")
	}

	rec, err := recoverRepoRecord(repo)
	if err != nil {
		return err
	}
	plan, err := buildFixPlan(ctx, rec.Path, rec.StateDB, dryRun)
	if err != nil {
		return err
	}
	if dryRun {
		return renderFix(out, plan, jsonOut)
	}
	if len(plan.Actions) > 0 {
		if err := applyFixPlan(ctx, rec.StateDB, &plan); err != nil {
			return err
		}
	}
	return renderFix(out, plan, jsonOut)
}

func buildFixPlan(ctx context.Context, repo, stateDB string, dryRun bool) (fixPlan, error) {
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fixPlan{}, fmt.Errorf("acd fix: resolve git dir: %w", err)
	}
	branchRef, err := git.RunBranchRef(ctx, repo)
	if err != nil {
		return fixPlan{}, fmt.Errorf("acd fix: resolve HEAD branch: %w", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		return fixPlan{}, fmt.Errorf("acd fix: resolve HEAD: %w", err)
	}

	conn, err := openStateDBReadOnly(ctx, stateDB)
	if err != nil {
		return fixPlan{}, fmt.Errorf("acd fix: open state.db read-only: %w", err)
	}
	defer conn.Close()

	plan := fixPlan{
		Repo:             repo,
		StateDB:          stateDB,
		GitDir:           gitDir,
		CurrentBranchRef: branchRef,
		CurrentHead:      head,
		Generation:       1,
		DryRun:           dryRun,
	}
	if raw, ok, err := metaLookup(ctx, conn, "branch.generation"); err != nil {
		return fixPlan{}, fmt.Errorf("acd fix: load branch generation: %w", err)
	} else if ok {
		if gen, perr := strconv.ParseInt(raw, 10, 64); perr == nil && gen > 0 {
			plan.Generation = gen
		}
	}

	if alive, desc, err := daemonAliveSQL(ctx, conn); err != nil {
		return fixPlan{}, err
	} else if alive {
		plan.Unsafe = append(plan.Unsafe, desc)
		plan.Suggestions = append(plan.Suggestions, "Stop the daemon before applying `acd fix --yes`, or rerun with --dry-run for inspection only.")
	}
	if branchRef == "" {
		plan.Unsafe = append(plan.Unsafe, "detached HEAD is not safe for guided state mutation")
		plan.Suggestions = append(plan.Suggestions, "Checkout an attached branch before applying fixes.")
	}

	if err := planManualPauseFix(ctx, gitDir, &plan); err != nil {
		return fixPlan{}, err
	}
	if err := planBackpressureFix(ctx, conn, &plan); err != nil {
		return fixPlan{}, err
	}
	if err := planExternalDecisionFix(ctx, conn, repo, head, &plan); err != nil {
		return fixPlan{}, err
	}
	if err := planObsoleteBarrierFix(ctx, conn, &plan); err != nil {
		return fixPlan{}, err
	}
	return plan, nil
}

func daemonAliveSQL(ctx context.Context, conn *sql.DB) (bool, string, error) {
	var pid int
	var mode string
	err := conn.QueryRowContext(ctx, `SELECT pid, mode FROM daemon_state WHERE id = 1`).Scan(&pid, &mode)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("acd fix: load daemon state: %w", err)
	}
	switch mode {
	case "running", "starting", "draining":
		if pid > 0 && identityAlive(ctx, pid) {
			return true, fmt.Sprintf("daemon pid %d is alive in mode %s", pid, mode), nil
		}
	}
	return false, "", nil
}

func identityAlive(ctx context.Context, pid int) bool {
	return pid > 0 && identityAliveContext(ctx, pid)
}

var identityAliveContext = identityAliveContextDefault

func identityAliveContextDefault(ctx context.Context, pid int) bool {
	return identity.AliveContext(ctx, pid)
}

func planManualPauseFix(ctx context.Context, gitDir string, plan *fixPlan) error {
	marker, ok, err := ReadMarker(gitDir)
	if err != nil {
		return fmt.Errorf("acd fix: read manual pause marker: %w", err)
	}
	markerPath := pausepkg.Path(gitDir)
	if !ok {
		return nil
	}
	plan.ManualPausePath = markerPath
	if marker.ExpiresAt == nil || strings.TrimSpace(*marker.ExpiresAt) == "" {
		plan.Suggestions = append(plan.Suggestions, "Manual pause marker is active; run `acd resume --yes` when the pause is intentional to lift.")
		return nil
	}
	expiresAt, err := time.Parse(time.RFC3339, *marker.ExpiresAt)
	if err != nil {
		plan.Unsafe = append(plan.Unsafe, fmt.Sprintf("manual pause marker has malformed expires_at %q", *marker.ExpiresAt))
		return nil
	}
	if time.Now().UTC().Before(expiresAt) {
		plan.Suggestions = append(plan.Suggestions, fmt.Sprintf("Manual pause marker is still active until %s; rerun fix after it expires or use `acd resume --yes`.", expiresAt.Format(time.RFC3339)))
		return nil
	}
	plan.Actions = append(plan.Actions, fixAction{
		ID:          fixActionClearExpiredManualPause,
		Kind:        fixActionClearExpiredManualPause,
		Description: "remove expired manual pause marker",
		Reason:      "manual pause TTL has expired",
		SetAt:       marker.SetAt,
	})
	_ = ctx
	return nil
}

func planBackpressureFix(ctx context.Context, conn *sql.DB, plan *fixPlan) error {
	setAt, ok, err := metaLookup(ctx, conn, daemon.MetaKeyCaptureBackpressurePausedAt)
	if err != nil {
		return fmt.Errorf("acd fix: read backpressure meta: %w", err)
	}
	if !ok {
		return nil
	}
	pending := 0
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStatePending).Scan(&pending); err != nil {
		return fmt.Errorf("acd fix: count pending events: %w", err)
	}
	if pending > 0 {
		plan.Suggestions = append(plan.Suggestions, fmt.Sprintf("Capture backpressure is still active with %d pending events; wait for replay to drain or run `acd resume --accept-overflow` if accepting dropped events is intentional.", pending))
		return nil
	}
	plan.Actions = append(plan.Actions, fixAction{
		ID:          fixActionClearDrainedBackpressure,
		Kind:        fixActionClearDrainedBackpressure,
		Description: "clear drained capture backpressure gate",
		Reason:      "backpressure marker is set but no pending events remain",
		SetAt:       setAt,
	})
	return nil
}

func planObsoleteBarrierFix(ctx context.Context, conn *sql.DB, plan *fixPlan) error {
	rows, err := conn.QueryContext(ctx, `
SELECT seq, path, state
FROM capture_events e
WHERE state IN (?, ?)
  AND NOT EXISTS (
      SELECT 1
      FROM decision_records d
      WHERE d.event_seq = e.seq
        AND d.kind IN (?, ?)
        AND d.commit_oid IS NOT NULL
        AND d.commit_oid <> ''
  )
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events pending
      WHERE pending.branch_ref = e.branch_ref
        AND pending.branch_generation = e.branch_generation
        AND pending.seq > e.seq
        AND pending.state = ?
  )
ORDER BY seq ASC`,
		state.EventStateBlockedConflict, state.EventStateFailed,
		state.DecisionKindHandledExternal, state.DecisionKindSupersededExternal,
		state.EventStatePending)
	if err != nil {
		return fmt.Errorf("acd fix: query obsolete barriers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var seq int64
		var path, st string
		if err := rows.Scan(&seq, &path, &st); err != nil {
			return fmt.Errorf("acd fix: scan obsolete barrier: %w", err)
		}
		plan.Actions = append(plan.Actions, fixAction{
			ID:          fmt.Sprintf("%s:%d", fixActionDeleteObsoleteBarrier, seq),
			Kind:        fixActionDeleteObsoleteBarrier,
			Description: "delete obsolete terminal replay barrier with no pending successors",
			Reason:      st,
			Seq:         seq,
			Path:        path,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("acd fix: iterate obsolete barriers: %w", err)
	}

	var blocking int
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM capture_events e
WHERE state IN (?, ?)
  AND NOT EXISTS (
      SELECT 1
      FROM decision_records d
      WHERE d.event_seq = e.seq
        AND d.kind IN (?, ?)
        AND d.commit_oid IS NOT NULL
        AND d.commit_oid <> ''
  )
  AND EXISTS (
      SELECT 1
      FROM capture_events pending
      WHERE pending.branch_ref = e.branch_ref
        AND pending.branch_generation = e.branch_generation
        AND pending.seq > e.seq
        AND pending.state = ?
  )`,
		state.EventStateBlockedConflict, state.EventStateFailed,
		state.DecisionKindHandledExternal, state.DecisionKindSupersededExternal,
		state.EventStatePending).Scan(&blocking); err != nil {
		return fmt.Errorf("acd fix: count active barriers: %w", err)
	}
	if blocking > 0 {
		plan.Suggestions = append(plan.Suggestions, fmt.Sprintf("%d terminal barrier rows still have pending successors; inspect with `acd diagnose` before purging.", blocking))
	}
	return nil
}

type externalDecisionCandidate struct {
	seq        int64
	path       string
	branchRef  string
	generation int64
	decisionID int64
	kind       string
	reason     string
	commitOID  string
}

func planExternalDecisionFix(ctx context.Context, conn *sql.DB, repo, head string, plan *fixPlan) error {
	rows, err := conn.QueryContext(ctx, `
SELECT e.seq, e.path, e.branch_ref, e.branch_generation,
       d.id, d.kind, COALESCE(d.reason, ''), COALESCE(d.commit_oid, '')
FROM capture_events e
JOIN decision_records d ON d.event_seq = e.seq
WHERE e.state IN (?, ?)
  AND d.kind IN (?, ?)
  AND d.commit_oid IS NOT NULL
  AND d.commit_oid <> ''
  AND (d.branch_ref IS NULL OR d.branch_ref = e.branch_ref)
  AND (d.branch_generation IS NULL OR d.branch_generation = e.branch_generation)
ORDER BY e.seq ASC, d.id DESC`,
		state.EventStatePending, state.EventStateBlockedConflict,
		state.DecisionKindHandledExternal, state.DecisionKindSupersededExternal)
	if err != nil {
		return fmt.Errorf("acd fix: query external decisions: %w", err)
	}
	defer rows.Close()

	seen := map[int64]bool{}
	for rows.Next() {
		var c externalDecisionCandidate
		if err := rows.Scan(&c.seq, &c.path, &c.branchRef, &c.generation, &c.decisionID, &c.kind, &c.reason, &c.commitOID); err != nil {
			return fmt.Errorf("acd fix: scan external decision: %w", err)
		}
		if seen[c.seq] {
			continue
		}
		seen[c.seq] = true
		ok, err := git.IsAncestor(ctx, repo, c.commitOID, head)
		if err != nil {
			plan.Unsafe = append(plan.Unsafe, fmt.Sprintf("decision %d for event %d references unresolved commit %s", c.decisionID, c.seq, c.commitOID))
			continue
		}
		if !ok {
			plan.Unsafe = append(plan.Unsafe, fmt.Sprintf("decision %d for event %d references commit %s outside current HEAD history", c.decisionID, c.seq, shortOID(c.commitOID, 12)))
			continue
		}
		plan.Actions = append(plan.Actions, fixAction{
			ID:          fmt.Sprintf("%s:%d", fixActionMarkExternalPublished, c.seq),
			Kind:        fixActionMarkExternalPublished,
			Description: "mark externally handled queued event as published",
			Reason:      c.kind + ":" + c.reason,
			Seq:         c.seq,
			Path:        c.path,
			DecisionID:  c.decisionID,
			CommitOID:   c.commitOID,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("acd fix: iterate external decisions: %w", err)
	}
	return nil
}

func applyFixPlan(ctx context.Context, stateDB string, plan *fixPlan) error {
	if plan.CurrentBranchRef == "" {
		return fmt.Errorf("acd fix: refusing to mutate state while HEAD is detached")
	}
	if err := preflightFixFS(plan); err != nil {
		return err
	}
	backup, err := backupStateDB(stateDB)
	if err != nil {
		return fmt.Errorf("acd fix: backup state.db: %w", err)
	}
	plan.BackupPath = backup

	db, err := state.Open(ctx, stateDB)
	if err != nil {
		return fmt.Errorf("acd fix: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := refuseRecoverWhenDaemonAlive(ctx, db); err != nil {
		return err
	}

	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("acd fix: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	nowSec := float64(time.Now().UTC().UnixNano()) / 1e9
	for i := range plan.Actions {
		n, err := applyFixAction(ctx, tx, plan.Actions[i], plan, nowSec)
		if err != nil {
			return err
		}
		plan.Actions[i].RowsChanged = n
		plan.Actions[i].Applied = true
		plan.RowsChanged += n
	}
	if err := clearPublishBarrierIfSafe(ctx, tx, nowSec); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("acd fix: commit transaction: %w", err)
	}

	for _, action := range plan.Actions {
		if action.Kind != fixActionClearExpiredManualPause {
			continue
		}
		if err := os.Remove(plan.ManualPausePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("acd fix: remove manual pause marker: %w", err)
		}
		stampManualPauseResume(ctx, plan.GitDir)
		plan.ManualPauseRemoved = true
	}
	return nil
}

func preflightFixFS(plan *fixPlan) error {
	for _, action := range plan.Actions {
		if action.Kind != fixActionClearExpiredManualPause {
			continue
		}
		info, err := os.Lstat(plan.ManualPausePath)
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("acd fix: manual pause marker disappeared before apply")
		}
		if err != nil {
			return fmt.Errorf("acd fix: stat manual pause marker: %w", err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("acd fix: manual pause marker %s is not a regular file", plan.ManualPausePath)
		}
		if err := checkParentDirWritable(plan.ManualPausePath); err != nil {
			return fmt.Errorf("acd fix: manual pause marker parent not writable: %w", err)
		}
	}
	return nil
}

func applyFixAction(ctx context.Context, tx *sql.Tx, action fixAction, plan *fixPlan, nowSec float64) (int64, error) {
	switch action.Kind {
	case fixActionClearExpiredManualPause:
		return 0, nil
	case fixActionClearDrainedBackpressure:
		var changed int64
		res, err := tx.ExecContext(ctx, `DELETE FROM daemon_meta WHERE key = ?`, daemon.MetaKeyCaptureBackpressurePausedAt)
		if err != nil {
			return 0, fmt.Errorf("acd fix: clear backpressure: %w", err)
		}
		if n, rerr := res.RowsAffected(); rerr == nil {
			changed += n
		}
		res, err = tx.ExecContext(ctx, `
INSERT INTO daemon_meta(key, value, updated_ts) VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`,
			"capture.backpressure_overridden_at", time.Now().UTC().Format(time.RFC3339), nowSec)
		if err != nil {
			return 0, fmt.Errorf("acd fix: stamp backpressure override: %w", err)
		}
		if n, rerr := res.RowsAffected(); rerr == nil {
			changed += n
		}
		return changed, nil
	case fixActionDeleteObsoleteBarrier:
		res, err := tx.ExecContext(ctx, `
DELETE FROM capture_events
WHERE seq = ?
  AND state IN (?, ?)
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events pending
      WHERE pending.branch_ref = capture_events.branch_ref
        AND pending.branch_generation = capture_events.branch_generation
        AND pending.seq > capture_events.seq
        AND pending.state = ?
  )`,
			action.Seq, state.EventStateBlockedConflict, state.EventStateFailed, state.EventStatePending)
		if err != nil {
			return 0, fmt.Errorf("acd fix: delete obsolete barrier seq %d: %w", action.Seq, err)
		}
		n, _ := res.RowsAffected()
		return n, nil
	case fixActionMarkExternalPublished:
		res, err := tx.ExecContext(ctx, `
UPDATE capture_events
SET state = ?, commit_oid = ?, error = NULL, published_ts = ?
WHERE seq = ? AND state IN (?, ?)`,
			state.EventStatePublished, action.CommitOID, nowSec, action.Seq,
			state.EventStatePending, state.EventStateBlockedConflict)
		if err != nil {
			return 0, fmt.Errorf("acd fix: mark event %d published: %w", action.Seq, err)
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO publish_state(id, event_seq, branch_ref, branch_generation, source_head, target_commit_oid, status, error, updated_ts)
VALUES (1, ?, ?, ?, ?, ?, 'published', NULL, ?)
ON CONFLICT(id) DO UPDATE SET
    event_seq = excluded.event_seq,
    branch_ref = excluded.branch_ref,
    branch_generation = excluded.branch_generation,
    source_head = excluded.source_head,
    target_commit_oid = excluded.target_commit_oid,
    status = excluded.status,
    error = excluded.error,
    updated_ts = excluded.updated_ts`,
				action.Seq, plan.CurrentBranchRef, plan.Generation, plan.CurrentHead, action.CommitOID, nowSec); err != nil {
				return 0, fmt.Errorf("acd fix: update publish_state for event %d: %w", action.Seq, err)
			}
		}
		return n, nil
	default:
		return 0, fmt.Errorf("acd fix: unknown action kind %q", action.Kind)
	}
}

func clearPublishBarrierIfSafe(ctx context.Context, tx *sql.Tx, nowSec float64) error {
	var blocked int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStateBlockedConflict).Scan(&blocked); err != nil {
		return fmt.Errorf("acd fix: count remaining blocked conflicts: %w", err)
	}
	if blocked > 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE publish_state
SET status = CASE WHEN status = 'blocked_conflict' THEN 'ok' ELSE status END,
    error = CASE WHEN status = 'blocked_conflict' THEN NULL ELSE error END,
    updated_ts = ?
WHERE id = 1`, nowSec); err != nil {
		return fmt.Errorf("acd fix: clear publish_state barrier: %w", err)
	}
	for _, key := range []string{"last_replay_conflict", "last_replay_conflict_legacy", "last_replay_error"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM daemon_meta WHERE key = ?`, key); err != nil {
			return fmt.Errorf("acd fix: clear daemon_meta %s: %w", key, err)
		}
	}
	return nil
}

func renderFix(out io.Writer, plan fixPlan, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}
	mode := "planned"
	if !plan.DryRun {
		mode = "applied"
	}
	fmt.Fprintf(out, "Fix %s for %s\n", mode, plan.Repo)
	if plan.CurrentBranchRef != "" || plan.CurrentHead != "" {
		fmt.Fprintf(out, "Anchor: %s @ %s generation=%d\n",
			valueOrUnset(plan.CurrentBranchRef), valueOrUnset(plan.CurrentHead), plan.Generation)
	}
	if plan.BackupPath != "" {
		fmt.Fprintf(out, "Backup: %s\n", plan.BackupPath)
	}
	if len(plan.Actions) == 0 {
		fmt.Fprintln(out, "No safe automatic fixes found.")
	} else {
		fmt.Fprintln(out, "Actions:")
		for _, action := range plan.Actions {
			target := ""
			if action.Seq > 0 {
				target = fmt.Sprintf(" seq=%d", action.Seq)
			}
			if action.Path != "" {
				target += " path=" + action.Path
			}
			fmt.Fprintf(out, "- %s%s", action.Description, target)
			if action.Applied {
				fmt.Fprintf(out, " (rows=%d)", action.RowsChanged)
			}
			fmt.Fprintln(out)
		}
	}
	if len(plan.Unsafe) > 0 {
		fmt.Fprintln(out, "Unsafe:")
		for _, item := range plan.Unsafe {
			fmt.Fprintf(out, "- %s\n", item)
		}
	}
	if len(plan.Suggestions) > 0 {
		fmt.Fprintln(out, "Suggested manual steps:")
		for _, item := range plan.Suggestions {
			fmt.Fprintf(out, "- %s\n", item)
		}
	}
	if !plan.DryRun {
		fmt.Fprintf(out, "Rows changed: %d\n", plan.RowsChanged)
		if plan.ManualPauseRemoved {
			fmt.Fprintf(out, "Manual pause marker removed: %s\n", plan.ManualPausePath)
		}
	} else {
		fmt.Fprintln(out, "(dry-run; pass --yes to apply safe actions)")
	}
	return nil
}

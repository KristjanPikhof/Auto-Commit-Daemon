package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type recoverPlan struct {
	Repo                string   `json:"repo"`
	StateDB             string   `json:"state_db"`
	GitDir              string   `json:"git_dir,omitempty"`
	CurrentBranchRef    string   `json:"current_branch_ref"`
	CurrentHead         string   `json:"current_head"`
	Generation          int64    `json:"generation"`
	DryRun              bool     `json:"dry_run"`
	BackupPath          string   `json:"backup_path,omitempty"`
	Actions             []string `json:"actions"`
	RowsChanged         int64    `json:"rows_changed"`
	ManualMarkerRemoved bool     `json:"manual_marker_removed,omitempty"`
	ManualMarkerPath    string   `json:"manual_marker_path,omitempty"`
}

func newRecoverCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Retarget stale replay state after an anchored-branch incident",
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			auto, _ := cmd.Flags().GetBool("auto")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			yes, _ := cmd.Flags().GetBool("yes")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runRecover(cmd.Context(), cmd.OutOrStdout(), repo, auto, dryRun, yes, jsonOut)
		},
	}
	cmd.Flags().Bool("auto", false, "Plan recovery automatically from current HEAD")
	cmd.Flags().Bool("dry-run", false, "Show planned recovery without mutating state")
	cmd.Flags().Bool("yes", false, "Apply recovery without an interactive prompt")
	return cmd
}

func runRecover(ctx context.Context, out io.Writer, repo string, auto, dryRun, yes, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !auto && !dryRun {
		return fmt.Errorf("acd recover: pass --auto to derive a recovery plan, or --dry-run to inspect first")
	}
	if !dryRun && !yes {
		return fmt.Errorf("acd recover: refusing to mutate state without --yes")
	}

	rec, err := recoverRepoRecord(repo)
	if err != nil {
		return err
	}
	plan, err := buildRecoverPlan(ctx, rec, dryRun)
	if err != nil {
		return err
	}
	if dryRun {
		plan.DryRun = true
		return renderRecover(out, plan, jsonOut)
	}

	if err := applyRecoverPlan(ctx, rec.StateDB, &plan); err != nil {
		return err
	}
	return renderRecover(out, plan, jsonOut)
}

func recoverRepoRecord(repo string) (central.RepoRecord, error) {
	abs, err := resolveRepo(repo)
	if err != nil {
		return central.RepoRecord{}, err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return central.RepoRecord{}, fmt.Errorf("acd recover: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return central.RepoRecord{}, fmt.Errorf("acd recover: load registry: %w", err)
	}
	rec, ok := findRepo(reg, abs)
	if !ok {
		return central.RepoRecord{}, fmt.Errorf("acd recover: repo %s is not registered", abs)
	}
	if !fileExists(rec.StateDB) {
		return central.RepoRecord{}, fmt.Errorf("acd recover: state.db missing for repo %s", abs)
	}
	return rec, nil
}

func buildRecoverPlan(ctx context.Context, rec central.RepoRecord, dryRun bool) (recoverPlan, error) {
	branchRef, err := git.RunBranchRef(ctx, rec.Path)
	if err != nil {
		return recoverPlan{}, fmt.Errorf("acd recover: resolve HEAD branch: %w", err)
	}
	if branchRef == "" {
		return recoverPlan{}, fmt.Errorf("acd recover: detached HEAD is not recoverable; checkout a branch first")
	}
	head, err := git.RevParse(ctx, rec.Path, "HEAD")
	if err != nil {
		return recoverPlan{}, fmt.Errorf("acd recover: resolve HEAD: %w", err)
	}
	gitDir, err := resolveGitDir(ctx, rec.Path)
	if err != nil {
		return recoverPlan{}, fmt.Errorf("acd recover: resolve git dir: %w", err)
	}
	markerPath := pausepkg.Path(gitDir)

	conn, err := openStateDBReadOnly(ctx, rec.StateDB)
	if err != nil {
		return recoverPlan{}, fmt.Errorf("acd recover: open state.db read-only: %w", err)
	}
	defer conn.Close()

	if err := refuseRecoverWhenDaemonAliveSQL(ctx, conn); err != nil {
		return recoverPlan{}, err
	}
	gen := int64(1)
	if raw, ok, err := metaLookup(ctx, conn, "branch.generation"); err != nil {
		return recoverPlan{}, fmt.Errorf("acd recover: load branch generation: %w", err)
	} else if ok {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			gen = parsed
		}
	}

	plan := recoverPlan{
		Repo:             rec.Path,
		StateDB:          rec.StateDB,
		GitDir:           gitDir,
		CurrentBranchRef: branchRef,
		CurrentHead:      head,
		Generation:       gen,
		DryRun:           dryRun,
		ManualMarkerPath: markerPath,
		Actions: []string{
			"retarget capture_events to current branch/generation/head",
			"retarget shadow_paths to current branch/generation/head",
			"retarget publish_state to current branch/generation/head",
			"reset blocked_conflict rows to pending",
			"clear stale replay/pause daemon_meta breadcrumbs",
			"clear daemon_meta " + daemon.MetaKeyReplayPausedUntil + " (rewind grace)",
			"remove manual pause marker at " + markerPath + " if present",
		},
	}
	return plan, nil
}

func refuseRecoverWhenDaemonAliveSQL(ctx context.Context, conn *sql.DB) error {
	var pid int
	var mode string
	err := conn.QueryRowContext(ctx, `SELECT pid, mode FROM daemon_state WHERE id = 1`).Scan(&pid, &mode)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acd recover: load daemon state: %w", err)
	}
	return refuseRecoverWhenDaemonPIDAlive(ctx, pid, mode)
}

func refuseRecoverWhenDaemonAlive(ctx context.Context, db *state.DB) error {
	st, ok, err := state.LoadDaemonState(ctx, db)
	if err != nil {
		return fmt.Errorf("acd recover: load daemon state: %w", err)
	}
	if !ok || st.PID <= 0 {
		return nil
	}
	return refuseRecoverWhenDaemonPIDAlive(ctx, st.PID, st.Mode)
}

func refuseRecoverWhenDaemonPIDAlive(ctx context.Context, pid int, mode string) error {
	if pid <= 0 {
		return nil
	}
	switch mode {
	case "running", "starting", "draining":
		if identity.AliveContext(ctx, pid) {
			return fmt.Errorf("acd recover: refusing while daemon pid %d is alive in mode %s", pid, mode)
		}
	}
	return nil
}

func applyRecoverPlan(ctx context.Context, stateDB string, plan *recoverPlan) error {
	backup, err := backupStateDB(stateDB)
	if err != nil {
		return fmt.Errorf("acd recover: backup state.db: %w", err)
	}
	plan.BackupPath = backup

	db, err := state.Open(ctx, stateDB)
	if err != nil {
		return fmt.Errorf("acd recover: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := refuseRecoverWhenDaemonAlive(ctx, db); err != nil {
		return err
	}

	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("acd recover: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	exec := func(q string, args ...any) error {
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err == nil {
			plan.RowsChanged += n
		}
		return nil
	}

	if err := exec(`UPDATE capture_events
SET branch_ref = ?, branch_generation = ?, base_head = ?
WHERE state IN (?, ?)`,
		plan.CurrentBranchRef, plan.Generation, plan.CurrentHead,
		state.EventStatePending, state.EventStateBlockedConflict); err != nil {
		return fmt.Errorf("acd recover: retarget capture_events: %w", err)
	}
	if err := exec(`UPDATE capture_events
SET state = ?, published_ts = NULL, error = NULL
WHERE state = ?`,
		state.EventStatePending, state.EventStateBlockedConflict); err != nil {
		return fmt.Errorf("acd recover: reset blocked events: %w", err)
	}
	if err := exec(`DELETE FROM shadow_paths
WHERE rowid NOT IN (
    SELECT keep_rowid FROM (
        SELECT MAX(rowid) AS keep_rowid
        FROM shadow_paths
        GROUP BY path
    )
)`); err != nil {
		return fmt.Errorf("acd recover: dedupe shadow_paths: %w", err)
	}
	if err := exec(`UPDATE shadow_paths
SET branch_ref = ?, branch_generation = ?, base_head = ?`,
		plan.CurrentBranchRef, plan.Generation, plan.CurrentHead); err != nil {
		return fmt.Errorf("acd recover: retarget shadow_paths: %w", err)
	}
	if err := exec(`UPDATE publish_state
SET branch_ref = ?, branch_generation = ?, source_head = ?, status = 'idle', error = NULL`,
		plan.CurrentBranchRef, plan.Generation, plan.CurrentHead); err != nil {
		return fmt.Errorf("acd recover: retarget publish_state: %w", err)
	}
	if err := exec(`UPDATE daemon_state
SET branch_ref = ?, branch_generation = ?, mode = 'stopped', note = NULL`,
		plan.CurrentBranchRef, plan.Generation); err != nil {
		return fmt.Errorf("acd recover: retarget daemon_state: %w", err)
	}
	for _, key := range []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		"last_replay_error",
		"detached_head_paused",
		"operation_in_progress",
		daemon.MetaKeyReplayPausedUntil,
	} {
		if err := exec(`DELETE FROM daemon_meta WHERE key = ?`, key); err != nil {
			return fmt.Errorf("acd recover: clear %s: %w", key, err)
		}
	}
	if err := exec(`INSERT INTO daemon_meta(key, value, updated_ts) VALUES('branch_token', ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`,
		"rev:"+plan.CurrentHead+" "+plan.CurrentBranchRef, float64(time.Now().UnixNano())/1e9); err != nil {
		return fmt.Errorf("acd recover: set branch token: %w", err)
	}
	if err := exec(`INSERT INTO daemon_meta(key, value, updated_ts) VALUES('branch.generation', ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`,
		fmt.Sprintf("%d", plan.Generation), float64(time.Now().UnixNano())/1e9); err != nil {
		return fmt.Errorf("acd recover: set branch generation: %w", err)
	}
	if err := exec(`INSERT INTO daemon_meta(key, value, updated_ts) VALUES('branch.head', ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`,
		plan.CurrentHead, float64(time.Now().UnixNano())/1e9); err != nil {
		return fmt.Errorf("acd recover: set branch head: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("acd recover: commit transaction: %w", err)
	}

	// Remove the durable manual pause marker. It is owned by `acd pause` /
	// `acd resume` and is not stored in state.db, so it survives the SQL
	// transaction; do this last so a Commit failure cannot leave us with a
	// retargeted DB but an orphaned marker.
	if plan.ManualMarkerPath != "" {
		if err := os.Remove(plan.ManualMarkerPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("acd recover: remove manual pause marker %s: %w", plan.ManualMarkerPath, err)
			}
			log.Printf("acd recover: no manual pause marker present at %s", plan.ManualMarkerPath)
		} else {
			plan.ManualMarkerRemoved = true
			log.Printf("acd recover: removed manual pause marker at %s", plan.ManualMarkerPath)
		}
	}
	return nil
}

func backupStateDB(stateDB string) (string, error) {
	src, err := os.ReadFile(stateDB)
	if err != nil {
		return "", err
	}
	backup := filepath.Join(filepath.Dir(stateDB),
		fmt.Sprintf("state.db.recover-%s", time.Now().UTC().Format("20060102T150405.000000000Z")))
	if err := os.WriteFile(backup, src, 0o600); err != nil {
		return "", err
	}
	return backup, nil
}

func renderRecover(out io.Writer, plan recoverPlan, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}
	mode := "planned"
	if !plan.DryRun {
		mode = "applied"
	}
	fmt.Fprintf(out, "Recovery %s for %s\n", mode, plan.Repo)
	fmt.Fprintf(out, "Anchor: %s @ %s generation=%d\n", plan.CurrentBranchRef, plan.CurrentHead, plan.Generation)
	if plan.BackupPath != "" {
		fmt.Fprintf(out, "Backup: %s\n", plan.BackupPath)
	}
	for _, action := range plan.Actions {
		fmt.Fprintf(out, "- %s\n", action)
	}
	if !plan.DryRun {
		fmt.Fprintf(out, "Rows changed: %d\n", plan.RowsChanged)
		if plan.ManualMarkerRemoved {
			fmt.Fprintf(out, "Manual pause marker removed: %s\n", plan.ManualMarkerPath)
		} else if plan.ManualMarkerPath != "" {
			fmt.Fprintf(out, "Manual pause marker: not present at %s\n", plan.ManualMarkerPath)
		}
	}
	return nil
}

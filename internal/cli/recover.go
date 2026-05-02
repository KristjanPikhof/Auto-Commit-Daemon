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
	Repo                    string   `json:"repo"`
	StateDB                 string   `json:"state_db"`
	GitDir                  string   `json:"git_dir,omitempty"`
	CurrentBranchRef        string   `json:"current_branch_ref"`
	CurrentHead             string   `json:"current_head"`
	Generation              int64    `json:"generation"`
	DryRun                  bool     `json:"dry_run"`
	BackupPath              string   `json:"backup_path,omitempty"`
	Actions                 []string `json:"actions"`
	RowsChanged             int64    `json:"rows_changed"`
	ClearPause              bool     `json:"clear_pause,omitempty"`
	ManualMarkerRemoved     bool     `json:"manual_marker_removed,omitempty"`
	ManualMarkerPreserved   bool     `json:"manual_marker_preserved,omitempty"`
	ManualMarkerPath        string   `json:"manual_marker_path,omitempty"`
	ManualMarkerRemoveError string   `json:"manual_marker_remove_error,omitempty"`
	LiveIndexCandidates     int      `json:"live_index_candidates,omitempty"`
	LiveIndexApplied        int      `json:"live_index_applied,omitempty"`
	LiveIndexSkipped        int      `json:"live_index_skipped,omitempty"`
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
			clearPause, _ := cmd.Flags().GetBool("clear-pause")
			return runRecover(cmd.Context(), cmd.OutOrStdout(), repo, auto, dryRun, yes, jsonOut, clearPause)
		},
	}
	cmd.Flags().Bool("auto", false, "Plan recovery automatically from current HEAD")
	cmd.Flags().Bool("dry-run", false, "Show planned recovery without mutating state")
	cmd.Flags().Bool("yes", false, "Apply recovery without an interactive prompt")
	cmd.Flags().Bool("clear-pause", false, "Also remove the manual pause marker; without this flag, an existing marker is preserved")
	return cmd
}

func runRecover(ctx context.Context, out io.Writer, repo string, auto, dryRun, yes, jsonOut, clearPause bool) error {
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
	plan, err := buildRecoverPlan(ctx, rec, dryRun, clearPause)
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

func buildRecoverPlan(ctx context.Context, rec central.RepoRecord, dryRun, clearPause bool) (recoverPlan, error) {
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
	liveIndexPlan, err := planLiveIndexRepair(ctx, rec.Path, rec.StateDB, head)
	if err != nil {
		return recoverPlan{}, err
	}

	markerAction := "preserve manual pause marker at " + markerPath + " (use --clear-pause to remove)"
	if clearPause {
		markerAction = "remove manual pause marker at " + markerPath + " if present"
	}
	plan := recoverPlan{
		Repo:                rec.Path,
		StateDB:             rec.StateDB,
		GitDir:              gitDir,
		CurrentBranchRef:    branchRef,
		CurrentHead:         head,
		Generation:          gen,
		DryRun:              dryRun,
		ClearPause:          clearPause,
		ManualMarkerPath:    markerPath,
		LiveIndexCandidates: liveIndexPlan.Candidates,
		LiveIndexSkipped:    len(liveIndexPlan.Skipped),
		Actions: []string{
			"retarget capture_events to current branch/generation/head",
			"retarget shadow_paths to current branch/generation/head",
			"retarget publish_state to current branch/generation/head",
			"reset blocked_conflict rows to pending",
			"clear stale replay/pause daemon_meta breadcrumbs",
			"clear daemon_meta " + daemon.MetaKeyReplayPausedUntil + " (rewind grace)",
			"repair ACD-published live-index entries when HEAD and worktree still match captured after-state",
			markerAction,
		},
	}
	return plan, nil
}

func planLiveIndexRepair(ctx context.Context, repo, stateDB, head string) (daemon.LiveIndexRepairSummary, error) {
	db, err := state.Open(ctx, stateDB)
	if err != nil {
		return daemon.LiveIndexRepairSummary{}, fmt.Errorf("acd recover: open state.db for live-index repair plan: %w", err)
	}
	defer func() { _ = db.Close() }()
	plan, err := daemon.PlanPublishedLiveIndexRepair(ctx, repo, db, head, daemon.DefaultLiveIndexRepairLimit)
	if err != nil {
		return daemon.LiveIndexRepairSummary{}, fmt.Errorf("acd recover: plan live-index repair: %w", err)
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

	// Preflight FS probes BEFORE opening a write transaction. FS syscalls
	// (os.Lstat, parent-dir writability probe) can stall on network mounts;
	// performing them inside an open tx would hold the write lock during the
	// stall. If the marker is non-removable we abort here so the DB stays
	// untouched. Without --clear-pause the marker is always preserved and we
	// skip the removability check entirely.
	markerExists := false
	if plan.ManualMarkerPath != "" {
		if info, err := os.Lstat(plan.ManualMarkerPath); err == nil {
			markerExists = true
			if plan.ClearPause {
				if !info.Mode().IsRegular() {
					return fmt.Errorf("acd recover: manual pause marker %s is not a regular file", plan.ManualMarkerPath)
				}
				if err := checkParentDirWritable(plan.ManualMarkerPath); err != nil {
					return fmt.Errorf("acd recover: manual pause marker parent not writable: %w", err)
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("acd recover: stat manual pause marker %s: %w", plan.ManualMarkerPath, err)
		}
	}

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

	if repaired, err := daemon.RepairPublishedLiveIndex(ctx, plan.Repo, db, plan.CurrentHead, daemon.DefaultLiveIndexRepairLimit); err != nil {
		return fmt.Errorf("acd recover: repair live index: %w", err)
	} else {
		plan.LiveIndexCandidates = repaired.Candidates
		plan.LiveIndexApplied = repaired.Applied
		plan.LiveIndexSkipped = len(repaired.Skipped)
	}

	// Post-commit: handle the durable manual pause marker. The marker is owned
	// by `acd pause` / `acd resume` and is not stored in state.db. Without
	// --clear-pause we always preserve it. With --clear-pause, attempt removal;
	// if the post-commit os.Remove fails (race), demote to a warning rather
	// than aborting — the DB is already retargeted and rendering must run.
	if plan.ManualMarkerPath != "" {
		switch {
		case !plan.ClearPause:
			if markerExists {
				plan.ManualMarkerPreserved = true
				log.Printf("acd recover: preserved manual pause marker at %s (use --clear-pause to remove)", plan.ManualMarkerPath)
			}
		default:
			if err := os.Remove(plan.ManualMarkerPath); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					log.Printf("acd recover: no manual pause marker present at %s", plan.ManualMarkerPath)
				} else {
					plan.ManualMarkerRemoveError = err.Error()
					log.Printf("acd recover: WARNING: failed to remove manual pause marker %s after commit: %v", plan.ManualMarkerPath, err)
				}
			} else {
				plan.ManualMarkerRemoved = true
				log.Printf("acd recover: removed manual pause marker at %s", plan.ManualMarkerPath)
			}
		}
	}
	return nil
}

// checkParentDirWritable verifies the parent directory of path is writable by
// the current process. Used as a preflight BEFORE db.SQL().BeginTx so a
// known-bad removability state aborts cleanly without ever opening the SQLite
// write transaction. Slow FS syscalls on network mounts must not stall the
// write lock.
func checkParentDirWritable(path string) error {
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	probe, err := os.CreateTemp(dir, ".acd-recover-probe-*")
	if err != nil {
		return err
	}
	probePath := probe.Name()
	_ = probe.Close()
	_ = os.Remove(probePath)
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
	if plan.LiveIndexCandidates > 0 || plan.LiveIndexSkipped > 0 {
		fmt.Fprintf(out, "Live index repair plan: candidates=%d skipped=%d\n",
			plan.LiveIndexCandidates, plan.LiveIndexSkipped)
	}
	for _, action := range plan.Actions {
		fmt.Fprintf(out, "- %s\n", action)
	}
	if !plan.DryRun {
		fmt.Fprintf(out, "Rows changed: %d\n", plan.RowsChanged)
		switch {
		case plan.ManualMarkerRemoved:
			fmt.Fprintf(out, "Manual pause marker removed: %s\n", plan.ManualMarkerPath)
		case plan.ManualMarkerPreserved:
			fmt.Fprintf(out, "Manual pause marker: %s preserved (use --clear-pause to remove)\n", plan.ManualMarkerPath)
		case plan.ManualMarkerPath != "":
			fmt.Fprintf(out, "Manual pause marker: not present at %s\n", plan.ManualMarkerPath)
		}
		if plan.ManualMarkerRemoveError != "" {
			fmt.Fprintf(out, "WARNING: manual pause marker remove failed after commit: %s\n", plan.ManualMarkerRemoveError)
		}
		if plan.LiveIndexCandidates > 0 || plan.LiveIndexSkipped > 0 {
			fmt.Fprintf(out, "Live index repair: candidates=%d applied=%d skipped=%d\n",
				plan.LiveIndexCandidates, plan.LiveIndexApplied, plan.LiveIndexSkipped)
		}
	}
	return nil
}

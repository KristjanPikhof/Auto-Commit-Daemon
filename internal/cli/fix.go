package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
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
	fixActionClearExpiredManualPause   = "clear_expired_manual_pause"
	fixActionClearDrainedBackpressure  = "clear_drained_backpressure"
	fixActionDropGeneratedPending      = "drop_generated_pending"
	fixActionReconcileUnpublishedChain = "reconcile_unpublished_chain"
)

type fixPlan struct {
	Repo               string                  `json:"repo"`
	StateDB            string                  `json:"state_db"`
	GitDir             string                  `json:"git_dir,omitempty"`
	CurrentBranchRef   string                  `json:"current_branch_ref,omitempty"`
	CurrentHead        string                  `json:"current_head,omitempty"`
	Generation         int64                   `json:"generation,omitempty"`
	DryRun             bool                    `json:"dry_run"`
	Force              bool                    `json:"force,omitempty"`
	ClearPause         bool                    `json:"clear_pause,omitempty"`
	BackupPath         string                  `json:"backup_path,omitempty"`
	Actions            []fixAction             `json:"actions"`
	Unsafe             []string                `json:"unsafe,omitempty"`
	Suggestions        []string                `json:"suggestions,omitempty"`
	RowsChanged        int64                   `json:"rows_changed"`
	ForceRequired      bool                    `json:"force_required,omitempty"`
	Incomplete         bool                    `json:"incomplete,omitempty"`
	VerifyErrors       []string                `json:"verify_errors,omitempty"`
	RemainingBlockers  *fixBlockerVerification `json:"remaining_blockers,omitempty"`
	ManualPauseRemoved bool                    `json:"manual_pause_removed,omitempty"`
	ManualPausePath    string                  `json:"manual_pause_path,omitempty"`

	// Apply-only proof and hooks. These never cross the JSON plan boundary.
	plannedPauseMarker   *pausepkg.Marker
	plannedPauseInfo     os.FileInfo
	afterBackup          func()
	beforePauseRemove    func()
	afterPauseQuarantine func(string)
	commitTransaction    func(*sql.Tx) error
}

type fixBlockerVerification struct {
	TotalBlockedConflicts               int `json:"total_blocked_conflicts"`
	ActiveBlockedBarriersWithSuccessors int `json:"active_blocked_barriers_with_successors"`
	FailedBarriersWithSuccessors        int `json:"failed_barriers_with_successors"`
	PendingOnlyIntentDepth              int `json:"pending_only_intent_depth"`
}

type fixAction struct {
	ID                string  `json:"id"`
	Kind              string  `json:"kind"`
	Description       string  `json:"description"`
	Reason            string  `json:"reason,omitempty"`
	Seq               int64   `json:"seq,omitempty"`
	Path              string  `json:"path,omitempty"`
	DecisionID        int64   `json:"decision_id,omitempty"`
	CommitOID         string  `json:"commit_oid,omitempty"`
	BlobOID           string  `json:"blob_oid,omitempty"`
	CapturedAfterOID  string  `json:"captured_after_oid,omitempty"`
	BranchRef         string  `json:"branch_ref,omitempty"`
	BranchGeneration  int64   `json:"branch_generation,omitempty"`
	BaseHead          string  `json:"base_head,omitempty"`
	GeneratedRoot     string  `json:"generated_root,omitempty"`
	SafeIgnorePattern string  `json:"safe_ignore_pattern,omitempty"`
	PendingCount      int     `json:"pending_count,omitempty"`
	TrackedCount      int     `json:"tracked_count,omitempty"`
	OldestSeq         int64   `json:"oldest_seq,omitempty"`
	NewestSeq         int64   `json:"newest_seq,omitempty"`
	EventSeqs         []int64 `json:"event_seqs,omitempty"`
	RowsChanged       int64   `json:"rows_changed,omitempty"`
	Applied           bool    `json:"applied,omitempty"`
	SetAt             string  `json:"set_at,omitempty"`
	RequiresForce     bool    `json:"requires_force,omitempty"`
	State             string  `json:"state,omitempty"`
	ArchiveOnly       bool    `json:"archive_only,omitempty"`
	InvalidateShadow  bool    `json:"invalidate_shadow,omitempty"`
	RecoveryRef       string  `json:"recovery_ref,omitempty"`
}

func newFixCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix",
		Short: "Plan or apply guided remediation for a stuck repo",
		Long: `Plan or apply guided remediation for common stuck ACD states.

` + "`acd fix`" + ` is the single recovery entrypoint. Without --yes and
without --force it prints a dry-run plan only. --yes applies safe recovery:
each stuck unpublished branch/generation pair is either proven present at a
stable HEAD or preserved at a hidden recovery ref before its queue state
changes. --force selects archive-only recovery for otherwise unresolved pairs;
it never deletes captured work. --force without --yes is still dry-run. All
actions refuse while a live daemon owns the state DB, and state.db is backed
up before any mutation.`,
		Example: `  acd fix --dry-run
  acd fix --yes
  acd fix --force --dry-run
  acd fix --force --yes
  acd fix --yes --clear-pause
  acd fix --repo /path/to/repo --yes --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			yes, _ := cmd.Flags().GetBool("yes")
			force, _ := cmd.Flags().GetBool("force")
			clearPause, _ := cmd.Flags().GetBool("clear-pause")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runFix(cmd.Context(), cmd.OutOrStdout(), repo, dryRun, yes, force, clearPause, jsonOut)
		},
	}
	cmd.Flags().Bool("dry-run", false, "Show the guided remediation plan without mutating state")
	cmd.Flags().Bool("yes", false, "Apply safe remediation actions (auto/safe set)")
	cmd.Flags().Bool("force", false, "Use archive-only recovery for unresolved unpublished pairs; captured work is protected before state changes")
	cmd.Flags().Bool("clear-pause", false, "Also remove the manual pause marker after safe recovery")
	return cmd
}

func runFix(ctx context.Context, out io.Writer, repo string, dryRun, yes, force, clearPause, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Resolve effective dry-run vs apply mode. Neither --yes nor --force
	// mutates by itself: --yes authorizes apply, while --force only changes
	// planned pair reconciliation to archive-only recovery. An explicit
	// --dry-run always wins.
	if !yes {
		dryRun = true
	}

	rec, err := recoverRepoRecord(repo)
	if err != nil {
		return err
	}
	plan, err := buildFixPlan(ctx, rec.Path, rec.StateDB, dryRun, force, clearPause)
	if err != nil {
		return err
	}
	if dryRun {
		return renderFix(out, plan, jsonOut)
	}
	if len(plan.Unsafe) > 0 {
		if err := renderFix(out, plan, jsonOut); err != nil {
			return err
		}
		return fmt.Errorf("acd fix: refusing to mutate state while unsafe conditions remain")
	}
	if len(plan.Actions) > 0 {
		if err := applyFixPlan(ctx, rec.StateDB, &plan); err != nil {
			markFixIncomplete(&plan, err)
			if rerr := renderFix(out, plan, jsonOut); rerr != nil {
				return rerr
			}
			return err
		}
	} else if force {
		conn, err := openStateDBReadOnly(ctx, rec.StateDB)
		if err != nil {
			return fmt.Errorf("acd fix: open state.db read-only for post-apply verification: %w", err)
		}
		defer conn.Close()
		if err := verifyFixPostApply(ctx, conn, &plan); err != nil {
			if rerr := renderFix(out, plan, jsonOut); rerr != nil {
				return rerr
			}
			return err
		}
	}
	return renderFix(out, plan, jsonOut)
}

func markFixIncomplete(plan *fixPlan, err error) {
	if plan.Incomplete {
		return
	}
	plan.Incomplete = true
	plan.VerifyErrors = append(plan.VerifyErrors, err.Error())
}

func buildFixPlan(ctx context.Context, repo, stateDB string, dryRun, force, clearPause bool) (fixPlan, error) {
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fixPlan{}, fmt.Errorf("acd fix: resolve git dir: %w", err)
	}
	branchRef, err := git.RunBranchRef(ctx, repo)
	if err != nil {
		return fixPlan{}, fmt.Errorf("acd fix: resolve HEAD branch: %w", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil && !errors.Is(err, git.ErrRefNotFound) {
		return fixPlan{}, fmt.Errorf("acd fix: resolve HEAD: %w", err)
	}
	if errors.Is(err, git.ErrRefNotFound) {
		head = ""
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
		Force:            force,
		ClearPause:       clearPause,
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
	} else if head == "" && !force {
		plan.Unsafe = append(plan.Unsafe, "attached HEAD has no commit; archive-only recovery must be explicitly requested")
		plan.Suggestions = append(plan.Suggestions, "Rerun with `acd fix --force --yes` to preserve complete unpublished pairs at hidden recovery refs.")
	}
	if name, active := daemon.GitOperationInProgress(gitDir); active {
		plan.Unsafe = append(plan.Unsafe, fmt.Sprintf("Git operation %s is in progress", name))
		plan.Suggestions = append(plan.Suggestions, "Finish or abort the Git operation before applying `acd fix --yes`.")
	}

	if err := planManualPauseFix(ctx, gitDir, &plan); err != nil {
		return fixPlan{}, err
	}
	if err := planBackpressureFix(ctx, conn, &plan); err != nil {
		return fixPlan{}, err
	}
	hasDecisionRecords, err := sqliteTableExists(ctx, conn, "decision_records")
	if err != nil {
		return fixPlan{}, fmt.Errorf("acd fix: check decision ledger: %w", err)
	}
	if err := planGeneratedPendingCleanup(ctx, conn, repo, &plan); err != nil {
		return fixPlan{}, err
	}
	if err := planUnpublishedChainReconciliation(ctx, conn, branchRef, plan.Generation, head, force, hasDecisionRecords, &plan); err != nil {
		return fixPlan{}, err
	}
	return plan, nil
}

type unpublishedFixPair struct {
	branchRef   string
	generation  int64
	firstSeq    int64
	lastSeq     int64
	eventCount  int
	hasTerminal bool
	decisionLed bool
}

// planUnpublishedChainReconciliation emits one action per exact provenance
// pair. It never rewrites a row onto the live branch and never peels one root
// barrier away from its successors. The daemon reconciler owns the eventual
// all-or-none proof/archive transition.
func planUnpublishedChainReconciliation(
	ctx context.Context,
	conn *sql.DB,
	currentBranch string,
	currentGeneration int64,
	currentHead string,
	force bool,
	hasDecisionRecords bool,
	plan *fixPlan,
) error {
	decisionExpr := "0"
	if hasDecisionRecords {
		decisionExpr = `EXISTS (
    SELECT 1
    FROM decision_records d
    WHERE d.event_seq = e.seq
      AND d.kind IN ('handled_external', 'handled_external_after_block', 'superseded_external')
)`
	}
	rows, err := conn.QueryContext(ctx, `
SELECT e.seq, e.branch_ref, e.branch_generation, e.state, `+decisionExpr+`
FROM capture_events e
WHERE e.state IN (?, ?, ?)
ORDER BY e.branch_ref, e.branch_generation, e.seq`,
		state.EventStatePending, state.EventStateBlockedConflict, state.EventStateFailed)
	if err != nil {
		return fmt.Errorf("acd fix: scan unpublished recovery pairs: %w", err)
	}
	defer rows.Close()

	pairs := make(map[string]*unpublishedFixPair)
	var order []string
	for rows.Next() {
		var seq, generation int64
		var branchRef, eventState string
		var decisionLed bool
		if err := rows.Scan(&seq, &branchRef, &generation, &eventState, &decisionLed); err != nil {
			return fmt.Errorf("acd fix: scan unpublished recovery row: %w", err)
		}
		key := fmt.Sprintf("%s\x00%d", branchRef, generation)
		pair := pairs[key]
		if pair == nil {
			pair = &unpublishedFixPair{
				branchRef: branchRef, generation: generation,
				firstSeq: seq, lastSeq: seq,
			}
			pairs[key] = pair
			order = append(order, key)
		}
		pair.lastSeq = seq
		pair.eventCount++
		pair.hasTerminal = pair.hasTerminal || eventState == state.EventStateBlockedConflict || eventState == state.EventStateFailed
		pair.decisionLed = pair.decisionLed || decisionLed
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("acd fix: iterate unpublished recovery rows: %w", err)
	}

	for _, key := range order {
		pair := pairs[key]
		activePair := pair.branchRef == currentBranch && pair.generation == currentGeneration
		stalePair := !activePair
		if !pair.hasTerminal && !stalePair && !pair.decisionLed {
			continue
		}
		archiveOnly := force || currentHead == ""
		reasonParts := make([]string, 0, 3)
		if pair.hasTerminal {
			reasonParts = append(reasonParts, "terminal replay barrier")
		}
		if stalePair {
			reasonParts = append(reasonParts, "non-active provenance pair")
		}
		if pair.decisionLed {
			reasonParts = append(reasonParts, "external decision evidence")
		}
		plan.Actions = append(plan.Actions, fixAction{
			ID:               fmt.Sprintf("%s:%s:%d:%d", fixActionReconcileUnpublishedChain, pair.branchRef, pair.generation, pair.firstSeq),
			Kind:             fixActionReconcileUnpublishedChain,
			Description:      "prove or preserve the exact unpublished branch/generation chain",
			Reason:           strings.Join(reasonParts, "; "),
			Seq:              pair.firstSeq,
			BranchRef:        pair.branchRef,
			BranchGeneration: pair.generation,
			OldestSeq:        pair.firstSeq,
			NewestSeq:        pair.lastSeq,
			PendingCount:     pair.eventCount,
			ArchiveOnly:      archiveOnly,
			InvalidateShadow: activePair,
			RequiresForce:    force,
		})
	}
	return nil
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
		if pid > 0 && identity.AliveContext(ctx, pid) {
			return true, fmt.Sprintf("daemon pid %d is alive in mode %s", pid, mode), nil
		}
	}
	return false, "", nil
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
	markerInfo, err := os.Lstat(markerPath)
	if err != nil {
		return fmt.Errorf("acd fix: stat planned manual pause marker: %w", err)
	}
	if !markerInfo.Mode().IsRegular() {
		return fmt.Errorf("acd fix: manual pause marker %s is not a regular file", markerPath)
	}
	plan.ManualPausePath = markerPath
	plan.plannedPauseMarker = &marker
	plan.plannedPauseInfo = markerInfo
	if plan.ClearPause {
		plan.Actions = append(plan.Actions, fixAction{
			ID:          fixActionClearExpiredManualPause,
			Kind:        fixActionClearExpiredManualPause,
			Description: "remove manual pause marker after safe recovery",
			Reason:      "operator explicitly requested --clear-pause",
			SetAt:       marker.SetAt,
		})
		return nil
	}
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

func planGeneratedPendingCleanup(ctx context.Context, conn *sql.DB, repo string, plan *fixPlan) error {
	groups, err := state.ScanGeneratedPendingDeletes(ctx, conn, state.NewSafeIgnoreMatcher(), 0)
	if err != nil {
		return fmt.Errorf("acd fix: scan generated pending deletes: %w", err)
	}
	if len(groups) == 0 {
		return nil
	}
	roots := make([]string, 0, len(groups))
	for _, group := range groups {
		roots = append(roots, group.Root)
	}
	tracked, err := git.CountTrackedPathsUnder(ctx, repo, roots...)
	if err != nil {
		return fmt.Errorf("acd fix: count tracked generated roots: %w", err)
	}
	for _, group := range groups {
		trackedCount := tracked[group.Root]
		plan.Actions = append(plan.Actions, fixAction{
			ID:                fmt.Sprintf("%s:%s:%d:%d", fixActionDropGeneratedPending, group.Root, group.BranchGeneration, group.OldestSeq),
			Kind:              fixActionDropGeneratedPending,
			Description:       "drop protected generated pending deletes from ACD queue",
			Reason:            "pending delete paths match active safe-ignore generated-tree guard",
			Path:              group.Root,
			GeneratedRoot:     group.Root,
			SafeIgnorePattern: group.Pattern,
			BranchRef:         group.BranchRef,
			BranchGeneration:  group.BranchGeneration,
			BaseHead:          group.BaseHead,
			PendingCount:      group.PendingCount,
			TrackedCount:      trackedCount,
			OldestSeq:         group.OldestSeq,
			NewestSeq:         group.NewestSeq,
			EventSeqs:         append([]int64(nil), group.EventSeqs...),
		})
		plan.Suggestions = append(plan.Suggestions, fmt.Sprintf(
			"Generated root %s has %d queued delete(s) and %d tracked file(s); `acd fix --repo %s --yes` cleans ACD state only. To record the Git cleanup, review `git status -- %s`, then run `git add -u -- %s` and `git commit -m \"Remove tracked generated cache files\"`.",
			group.Root, group.PendingCount, trackedCount, plan.Repo, group.Root, group.Root))
	}
	return nil
}

func loadFixCaptureOps(ctx context.Context, conn *sql.DB, seq int64) ([]state.CaptureOp, error) {
	rows, err := conn.QueryContext(ctx, `
SELECT event_seq, ord, op, path, old_path,
       before_oid, before_mode, after_oid, after_mode, fidelity
FROM capture_ops
WHERE event_seq = ?
ORDER BY ord ASC`, seq)
	if err != nil {
		return nil, fmt.Errorf("acd fix: query capture ops for event %d: %w", seq, err)
	}
	defer rows.Close()
	var ops []state.CaptureOp
	for rows.Next() {
		var op state.CaptureOp
		if err := rows.Scan(&op.EventSeq, &op.Ord, &op.Op, &op.Path, &op.OldPath,
			&op.BeforeOID, &op.BeforeMode, &op.AfterOID, &op.AfterMode, &op.Fidelity); err != nil {
			return nil, fmt.Errorf("acd fix: scan capture op for event %d: %w", seq, err)
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("acd fix: iterate capture ops for event %d: %w", seq, err)
	}
	return ops, nil
}

func applyFixPlan(ctx context.Context, stateDB string, plan *fixPlan) error {
	if plan.CurrentBranchRef == "" {
		return fmt.Errorf("acd fix: refusing to mutate state while HEAD is detached")
	}
	// Defense-in-depth: refuse archive-only recovery actions unless the plan's
	// Force flag was set at build time. Planner gating already prevents these
	// actions from appearing without --force, so this catches re-hydrated plans
	// or alternate callers that try to bypass the explicit opt-in.
	for _, action := range plan.Actions {
		if action.RequiresForce && !plan.Force {
			return fmt.Errorf("acd fix: refusing to apply %s without --force (planner gating bypassed)", action.Kind)
		}
	}
	daemonLock, err := daemon.AcquireDaemonLock(plan.GitDir)
	if err != nil {
		if errors.Is(err, daemon.ErrDaemonLockHeld) {
			return fmt.Errorf("acd fix: refusing while the per-repo daemon owns daemon.lock: %w", err)
		}
		return fmt.Errorf("acd fix: acquire daemon.lock: %w", err)
	}
	defer func() { _ = daemonLock.Release() }()

	if err := revalidateFixApplySafety(ctx, plan); err != nil {
		return err
	}
	if err := preflightFixFS(plan); err != nil {
		return err
	}
	// Preserve the original WAL-consistent database through a raw connection.
	// state.Open may apply schema DDL, so it must not run until this verified
	// pre-migration backup exists.
	backupConn, err := openFixBackupDB(ctx, stateDB)
	if err != nil {
		return err
	}
	if err := refuseRecoverWhenDaemonAliveSQL(ctx, backupConn); err != nil {
		_ = backupConn.Close()
		return err
	}
	if err := revalidateFixApplySafety(ctx, plan); err != nil {
		_ = backupConn.Close()
		return err
	}
	backup, err := backupStateDB(ctx, backupConn, stateDB)
	if err != nil {
		_ = backupConn.Close()
		return fmt.Errorf("acd fix: backup state.db: %w", err)
	}
	plan.BackupPath = backup
	if err := backupConn.Close(); err != nil {
		return fmt.Errorf("acd fix: close pre-migration backup connection: %w", err)
	}
	if plan.afterBackup != nil {
		plan.afterBackup()
	}

	// daemon.lock excludes daemon startup, but external Git surgery does not use
	// it. Repeat every apply-safety check after the potentially long backup.
	if err := revalidateFixApplySafety(ctx, plan); err != nil {
		return err
	}
	db, err := state.Open(ctx, stateDB)
	if err != nil {
		return fmt.Errorf("acd fix: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := refuseRecoverWhenDaemonAlive(ctx, db); err != nil {
		return err
	}
	if err := revalidateFixApplySafety(ctx, plan); err != nil {
		return err
	}

	// Git-backed reconciliation owns its own ref-first/SQLite transaction. Run
	// it before housekeeping so generated-row cleanup or breadcrumb changes can
	// never remove captured work before the hidden proof/recovery ref exists.
	for i := range plan.Actions {
		action := &plan.Actions[i]
		if action.Kind != fixActionReconcileUnpublishedChain {
			continue
		}
		if err := revalidateFixApplySafety(ctx, plan); err != nil {
			return err
		}
		result, err := daemon.ReconcileUnpublishedChain(ctx, plan.Repo, db, daemon.RecoveryReconcileOptions{
			GitDir:           plan.GitDir,
			BranchRef:        action.BranchRef,
			BranchGeneration: action.BranchGeneration,
			FirstSeq:         action.Seq,
			Trigger:          "cli_fix",
			ArchiveOnly:      action.ArchiveOnly,
			InvalidateShadow: action.InvalidateShadow,
		})
		if err != nil {
			return fmt.Errorf("acd fix: reconcile %s generation %d from seq %d: %w",
				action.BranchRef, action.BranchGeneration, action.Seq, err)
		}
		action.Applied = true
		if result.Handled {
			action.RowsChanged = int64(result.EventCount)
			action.CommitOID = result.CommitOID
			action.RecoveryRef = result.RecoveryRef
			action.State = result.Outcome
			plan.RowsChanged += int64(result.EventCount)
		}
	}

	transactional := false
	for _, action := range plan.Actions {
		if action.Kind != fixActionReconcileUnpublishedChain && action.Kind != fixActionClearExpiredManualPause {
			transactional = true
			break
		}
	}
	if transactional {
		if err := revalidateFixApplySafety(ctx, plan); err != nil {
			return err
		}
		tx, err := db.SQL().BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("acd fix: begin transaction: %w", err)
		}
		defer func() { _ = tx.Rollback() }()

		type pendingActionResult struct {
			index int
			rows  int64
		}
		var pendingResults []pendingActionResult
		nowSec := float64(time.Now().UTC().UnixNano()) / 1e9
		for i := range plan.Actions {
			if plan.Actions[i].Kind == fixActionReconcileUnpublishedChain || plan.Actions[i].Kind == fixActionClearExpiredManualPause {
				continue
			}
			n, err := applyFixAction(ctx, tx, plan.Actions[i], nowSec)
			if err != nil {
				return err
			}
			pendingResults = append(pendingResults, pendingActionResult{index: i, rows: n})
		}
		if err := clearPublishBarrierIfSafe(ctx, tx, nowSec); err != nil {
			return err
		}
		commit := tx.Commit
		if plan.commitTransaction != nil {
			commit = func() error { return plan.commitTransaction(tx) }
		}
		if err := revalidateFixApplySafety(ctx, plan); err != nil {
			return err
		}
		if err := commit(); err != nil {
			return fmt.Errorf("acd fix: commit transaction: %w", err)
		}
		for _, result := range pendingResults {
			plan.Actions[result.index].RowsChanged = result.rows
			plan.Actions[result.index].Applied = true
			plan.RowsChanged += result.rows
		}
	}

	for _, action := range plan.Actions {
		if action.Kind != fixActionClearExpiredManualPause {
			continue
		}
		if err := revalidateFixApplySafety(ctx, plan); err != nil {
			return err
		}
		if plan.beforePauseRemove != nil {
			plan.beforePauseRemove()
		}
		if err := removePlannedPauseMarker(plan); err != nil {
			return err
		}
		stampManualPauseResume(ctx, plan.GitDir)
		for i := range plan.Actions {
			if plan.Actions[i].Kind == fixActionClearExpiredManualPause {
				plan.Actions[i].Applied = true
			}
		}
		plan.ManualPauseRemoved = true
	}
	if err := verifyFixPostApply(ctx, db.SQL(), plan); err != nil {
		return err
	}
	return nil
}

func openFixBackupDB(ctx context.Context, stateDB string) (*sql.DB, error) {
	q := url.Values{}
	q.Add("mode", "rw")
	q.Add("_pragma", "busy_timeout(5000)")
	conn, err := sql.Open("sqlite", "file:"+stateDB+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("acd fix: open state.db for pre-migration backup: %w", err)
	}
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acd fix: ping state.db for pre-migration backup: %w", err)
	}
	return conn, nil
}

func revalidateFixApplySafety(ctx context.Context, plan *fixPlan) error {
	branchRef, err := git.RunBranchRef(ctx, plan.Repo)
	if err != nil {
		return fmt.Errorf("acd fix: recheck HEAD branch: %w", err)
	}
	head, err := git.RevParse(ctx, plan.Repo, "HEAD")
	if err != nil && !errors.Is(err, git.ErrRefNotFound) {
		return fmt.Errorf("acd fix: recheck HEAD: %w", err)
	}
	if errors.Is(err, git.ErrRefNotFound) {
		head = ""
	}
	if branchRef != plan.CurrentBranchRef || head != plan.CurrentHead {
		return fmt.Errorf("acd fix: refusing to mutate state because HEAD changed during planning")
	}
	if name, active := daemon.GitOperationInProgress(plan.GitDir); active {
		return fmt.Errorf("acd fix: refusing to mutate state while Git operation %s is in progress", name)
	}
	if err := revalidateFixPauseState(plan); err != nil {
		return err
	}
	return nil
}

func revalidateFixPauseState(plan *fixPlan) error {
	marker, ok, err := pausepkg.Read(plan.GitDir)
	if err != nil {
		return fmt.Errorf("acd fix: re-read manual pause marker: %w", err)
	}
	if plan.plannedPauseMarker == nil {
		if ok {
			return fmt.Errorf("acd fix: manual pause marker appeared since planning; preserving current marker")
		}
		return nil
	}
	if !ok {
		return fmt.Errorf("acd fix: manual pause marker disappeared since planning")
	}
	info, err := os.Lstat(plan.ManualPausePath)
	if err != nil {
		return fmt.Errorf("acd fix: stat manual pause marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("acd fix: manual pause marker %s is not a regular file", plan.ManualPausePath)
	}
	if plan.plannedPauseInfo == nil || !os.SameFile(plan.plannedPauseInfo, info) ||
		!samePauseMarker(*plan.plannedPauseMarker, marker) {
		return fmt.Errorf("acd fix: manual pause marker changed since planning; preserving current marker")
	}
	return nil
}

func verifyFixPostApply(ctx context.Context, conn *sql.DB, plan *fixPlan) error {
	var errs []string
	counts, err := loadRecoveryBlockerCounts(ctx, conn, plan.CurrentBranchRef, plan.Generation)
	if err != nil {
		return fmt.Errorf("acd fix: verify post-apply blockers: %w", err)
	}
	if counts.ActiveBlockedBarriersWithSuccessors > 0 || counts.FailedBarriersWithSuccessors > 0 {
		plan.RemainingBlockers = &fixBlockerVerification{
			TotalBlockedConflicts:               counts.TotalBlockedConflicts,
			ActiveBlockedBarriersWithSuccessors: counts.ActiveBlockedBarriersWithSuccessors,
			FailedBarriersWithSuccessors:        counts.FailedBarriersWithSuccessors,
			PendingOnlyIntentDepth:              counts.PendingOnlyIntentDepth,
		}
		if counts.ActiveBlockedBarriersWithSuccessors > 0 {
			errs = append(errs, fmt.Sprintf("%d active blocked barrier row(s) still have pending successors", counts.ActiveBlockedBarriersWithSuccessors))
		}
		if counts.FailedBarriersWithSuccessors > 0 {
			errs = append(errs, fmt.Sprintf("%d failed barrier row(s) still have pending successors", counts.FailedBarriersWithSuccessors))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	plan.Incomplete = true
	plan.VerifyErrors = errs
	return fmt.Errorf("acd fix: post-apply verification incomplete: %s", strings.Join(errs, "; "))
}

func preflightFixFS(plan *fixPlan) error {
	for _, action := range plan.Actions {
		if action.Kind != fixActionClearExpiredManualPause {
			continue
		}
		if err := verifyPlannedPauseMarker(plan); err != nil {
			return err
		}
		if err := checkParentDirWritable(plan.ManualPausePath); err != nil {
			return fmt.Errorf("acd fix: manual pause marker parent not writable: %w", err)
		}
	}
	return nil
}

func verifyPlannedPauseMarker(plan *fixPlan) error {
	if plan.plannedPauseMarker == nil || plan.plannedPauseInfo == nil {
		return fmt.Errorf("acd fix: manual pause marker proof missing from plan")
	}
	return revalidateFixPauseState(plan)
}

func samePauseMarker(a, b pausepkg.Marker) bool {
	if a.Reason != b.Reason || a.SetAt != b.SetAt || a.SetBy != b.SetBy || a.Version != b.Version {
		return false
	}
	if a.ExpiresAt == nil || b.ExpiresAt == nil {
		return a.ExpiresAt == nil && b.ExpiresAt == nil
	}
	return *a.ExpiresAt == *b.ExpiresAt
}

func removePlannedPauseMarker(plan *fixPlan) error {
	if plan.plannedPauseMarker == nil || plan.plannedPauseInfo == nil {
		return fmt.Errorf("acd fix: manual pause marker proof missing from plan")
	}
	quarantineDir, err := os.MkdirTemp(filepath.Dir(plan.ManualPausePath), ".paused-fix-quarantine-")
	if err != nil {
		return fmt.Errorf("acd fix: create pause marker quarantine: %w", err)
	}
	quarantinePath := filepath.Join(quarantineDir, "paused")
	if err := os.Rename(plan.ManualPausePath, quarantinePath); err != nil {
		_ = os.Remove(quarantineDir)
		return fmt.Errorf("acd fix: quarantine manual pause marker: %w", err)
	}
	if plan.afterPauseQuarantine != nil {
		plan.afterPauseQuarantine(quarantinePath)
	}

	marker, info, err := readQuarantinedPauseMarker(quarantinePath)
	if err != nil {
		return preserveQuarantinedPauseMarker(plan.ManualPausePath, quarantinePath, err)
	}
	if !os.SameFile(plan.plannedPauseInfo, info) || !samePauseMarker(*plan.plannedPauseMarker, marker) {
		return preserveQuarantinedPauseMarker(plan.ManualPausePath, quarantinePath,
			fmt.Errorf("manual pause marker changed since planning"))
	}
	if err := os.Remove(quarantinePath); err != nil {
		return fmt.Errorf("acd fix: remove quarantined planned pause marker at %s: %w", quarantinePath, err)
	}
	_ = os.Remove(quarantineDir)
	if _, err := os.Lstat(plan.ManualPausePath); err == nil {
		return fmt.Errorf("acd fix: planned pause marker removed, but a new marker was created and preserved at %s", plan.ManualPausePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("acd fix: check for replacement pause marker after removal: %w", err)
	}
	return nil
}

func readQuarantinedPauseMarker(path string) (pausepkg.Marker, os.FileInfo, error) {
	var marker pausepkg.Marker
	info, err := os.Lstat(path)
	if err != nil {
		return marker, nil, fmt.Errorf("stat quarantined marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return marker, info, fmt.Errorf("quarantined marker is not a regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return marker, info, fmt.Errorf("read quarantined marker: %w", err)
	}
	if err := json.Unmarshal(body, &marker); err != nil {
		return marker, info, fmt.Errorf("decode quarantined marker: %w", err)
	}
	return marker, info, nil
}

func preserveQuarantinedPauseMarker(originalPath, quarantinePath string, cause error) error {
	if _, err := os.Lstat(originalPath); errors.Is(err, os.ErrNotExist) {
		if linkErr := os.Link(quarantinePath, originalPath); linkErr == nil {
			if removeErr := os.Remove(quarantinePath); removeErr != nil {
				return fmt.Errorf("acd fix: %v; marker restored at %s but quarantine cleanup failed at %s: %w",
					cause, originalPath, quarantinePath, removeErr)
			}
			_ = os.Remove(filepath.Dir(quarantinePath))
			return fmt.Errorf("acd fix: %v; current marker restored without removal at %s", cause, originalPath)
		} else if !errors.Is(linkErr, os.ErrExist) {
			return fmt.Errorf("acd fix: %v; marker preserved at %s after restore failed: %w",
				cause, quarantinePath, linkErr)
		}
	} else if err != nil {
		return fmt.Errorf("acd fix: %v; marker preserved at %s after original-path probe failed: %w",
			cause, quarantinePath, err)
	}
	return fmt.Errorf("acd fix: %v; marker preserved at quarantine path %s because a marker exists at %s",
		cause, quarantinePath, originalPath)
}

func applyFixAction(ctx context.Context, tx *sql.Tx, action fixAction, nowSec float64) (int64, error) {
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
	case fixActionDropGeneratedPending:
		return applyDropGeneratedPending(ctx, tx, action)
	default:
		return 0, fmt.Errorf("acd fix: unknown action kind %q", action.Kind)
	}
}

func applyDropGeneratedPending(ctx context.Context, tx *sql.Tx, action fixAction) (int64, error) {
	if len(action.EventSeqs) == 0 {
		return 0, nil
	}
	var changed int64
	for _, chunk := range chunkInt64s(action.EventSeqs, 500) {
		placeholders := placeholders(len(chunk))
		args := int64Args(chunk)
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM planner_state
WHERE event_seq IN (`+placeholders+`)
  AND EXISTS (
      SELECT 1 FROM capture_events e
      WHERE e.seq = planner_state.event_seq AND e.state = 'pending' AND e.operation = 'delete'
  )`, args...); err != nil {
			return 0, fmt.Errorf("acd fix: delete generated planner state for %s: %w", action.GeneratedRoot, err)
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM capture_ops
WHERE event_seq IN (`+placeholders+`)
  AND EXISTS (
      SELECT 1 FROM capture_events e
      WHERE e.seq = capture_ops.event_seq AND e.state = 'pending' AND e.operation = 'delete'
  )`, args...); err != nil {
			return 0, fmt.Errorf("acd fix: delete generated capture ops for %s: %w", action.GeneratedRoot, err)
		}
		eventArgs := append(int64Args(chunk), state.EventStatePending, "delete")
		res, err := tx.ExecContext(ctx,
			`DELETE FROM capture_events WHERE seq IN (`+placeholders+`) AND state = ? AND operation = ?`, eventArgs...)
		if err != nil {
			return 0, fmt.Errorf("acd fix: delete generated pending events for %s: %w", action.GeneratedRoot, err)
		}
		n, _ := res.RowsAffected()
		changed += n
	}
	return changed, nil
}

func chunkInt64s(values []int64, size int) [][]int64 {
	if size <= 0 {
		size = len(values)
	}
	var out [][]int64
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		out = append(out, values[start:end])
	}
	return out
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

func int64Args(values []int64) []any {
	args := make([]any, len(values))
	for i, v := range values {
		args[i] = v
	}
	return args
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
	for _, key := range []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		"last_replay_error",
		"replay.error_repeat_count",
		"replay.error_last_seen_ts",
	} {
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
	if plan.Incomplete {
		mode = "incomplete"
	} else if !plan.DryRun {
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
			if action.Kind == fixActionDropGeneratedPending {
				target = fmt.Sprintf(" root=%s pending=%d tracked=%d seq=%d..%d",
					action.GeneratedRoot, action.PendingCount, action.TrackedCount, action.OldestSeq, action.NewestSeq)
			} else if action.Kind == fixActionReconcileUnpublishedChain {
				target = fmt.Sprintf(" pair=%s/g%d seq=%d..%d",
					action.BranchRef, action.BranchGeneration, action.OldestSeq, action.NewestSeq)
				if action.ArchiveOnly {
					target += " archive-only"
				}
			} else if action.Seq > 0 {
				target = fmt.Sprintf(" seq=%d", action.Seq)
			}
			if action.Path != "" && action.Kind != fixActionDropGeneratedPending {
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
	if plan.Incomplete || len(plan.VerifyErrors) > 0 {
		fmt.Fprintln(out, "Fix incomplete:")
		for _, item := range plan.VerifyErrors {
			fmt.Fprintf(out, "- %s\n", item)
		}
		if plan.RemainingBlockers != nil {
			fmt.Fprintf(out, "Remaining blockers: total_blocked_conflicts=%d active_blocked_barriers_with_successors=%d failed_barriers_with_successors=%d pending_only_intent_depth=%d\n",
				plan.RemainingBlockers.TotalBlockedConflicts,
				plan.RemainingBlockers.ActiveBlockedBarriersWithSuccessors,
				plan.RemainingBlockers.FailedBarriersWithSuccessors,
				plan.RemainingBlockers.PendingOnlyIntentDepth)
		}
	}
	if !plan.DryRun {
		fmt.Fprintf(out, "Rows changed: %d\n", plan.RowsChanged)
		if plan.ManualPauseRemoved {
			fmt.Fprintf(out, "Manual pause marker removed: %s\n", plan.ManualPausePath)
		}
	} else {
		hint := "pass --yes to apply safe actions"
		if plan.Force {
			hint = "pass --yes to apply archive-only recovery; captured work will be protected first"
		}
		fmt.Fprintf(out, "(dry-run; %s)\n", hint)
	}
	return nil
}

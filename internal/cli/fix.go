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
	fixActionClearExpiredManualPause     = "clear_expired_manual_pause"
	fixActionClearDrainedBackpressure    = "clear_drained_backpressure"
	fixActionDeleteObsoleteBarrier       = "delete_obsolete_barrier"
	fixActionMarkExternalPublished       = "mark_external_published"
	fixActionResolveAlreadyLandedBarrier = "resolve_already_landed_barrier"
	fixActionRetargetStaleAnchor         = "retarget_stale_anchor"
	fixActionPurgeBarrierWithSuccessors  = "purge_barrier_with_successors"
)

type fixPlan struct {
	Repo               string      `json:"repo"`
	StateDB            string      `json:"state_db"`
	GitDir             string      `json:"git_dir,omitempty"`
	CurrentBranchRef   string      `json:"current_branch_ref,omitempty"`
	CurrentHead        string      `json:"current_head,omitempty"`
	Generation         int64       `json:"generation,omitempty"`
	DryRun             bool        `json:"dry_run"`
	Force              bool        `json:"force,omitempty"`
	ClearPause         bool        `json:"clear_pause,omitempty"`
	BackupPath         string      `json:"backup_path,omitempty"`
	Actions            []fixAction `json:"actions"`
	Unsafe             []string    `json:"unsafe,omitempty"`
	Suggestions        []string    `json:"suggestions,omitempty"`
	RowsChanged        int64       `json:"rows_changed"`
	ForceRequired      bool        `json:"force_required,omitempty"`
	Incomplete         bool        `json:"incomplete,omitempty"`
	VerifyErrors       []string    `json:"verify_errors,omitempty"`
	RemainingBlockers  *fixBlockerVerification `json:"remaining_blockers,omitempty"`
	ManualPauseRemoved bool        `json:"manual_pause_removed,omitempty"`
	ManualPausePath    string      `json:"manual_pause_path,omitempty"`
	// Retarget bookkeeping (mirrors recoverPlan fields so JSON callers can
	// follow ported acd recover semantics without losing data).
	ManualMarkerRemoved     bool   `json:"manual_marker_removed,omitempty"`
	ManualMarkerPreserved   bool   `json:"manual_marker_preserved,omitempty"`
	ManualMarkerRemoveError string `json:"manual_marker_remove_error,omitempty"`
	LiveIndexCandidates     int    `json:"live_index_candidates,omitempty"`
	LiveIndexApplied        int    `json:"live_index_applied,omitempty"`
	LiveIndexSkipped        int    `json:"live_index_skipped,omitempty"`
}

type fixBlockerVerification struct {
	TotalBlockedConflicts                 int `json:"total_blocked_conflicts"`
	ActiveBlockedBarriersWithSuccessors   int `json:"active_blocked_barriers_with_successors"`
	FailedBarriersWithSuccessors          int `json:"failed_barriers_with_successors"`
	PendingOnlyIntentDepth                int `json:"pending_only_intent_depth"`
}

type fixAction struct {
	ID               string `json:"id"`
	Kind             string `json:"kind"`
	Description      string `json:"description"`
	Reason           string `json:"reason,omitempty"`
	Seq              int64  `json:"seq,omitempty"`
	Path             string `json:"path,omitempty"`
	DecisionID       int64  `json:"decision_id,omitempty"`
	CommitOID        string `json:"commit_oid,omitempty"`
	BlobOID          string `json:"blob_oid,omitempty"`
	CapturedAfterOID string `json:"captured_after_oid,omitempty"`
	BranchRef        string `json:"branch_ref,omitempty"`
	BranchGeneration int64  `json:"branch_generation,omitempty"`
	BaseHead         string `json:"base_head,omitempty"`
	RowsChanged      int64  `json:"rows_changed,omitempty"`
	Applied          bool   `json:"applied,omitempty"`
	SetAt            string `json:"set_at,omitempty"`
	RequiresForce    bool   `json:"requires_force,omitempty"`
}

func newFixCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix",
		Short: "Plan or apply guided remediation for a stuck repo",
		Long: `Plan or apply guided remediation for common stuck ACD states.

` + "`acd fix`" + ` is the single recovery entrypoint. Without --yes and
without --force it prints a dry-run plan only. --yes applies the safe,
auto-resolvable actions (resolve already-landed barriers, retarget stale
anchors, clear obsolete barriers, mark externally-published rows, clear
expired manual pauses, clear drained backpressure). --force opts into the
destructive purge of blocked barriers that still have pending successors;
--force without --yes is still dry-run. All actions refuse while a live
daemon owns the state DB, and state.db is backed up before any mutation.`,
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
	cmd.Flags().Bool("force", false, "Include destructive purge of blocked barriers with pending successors in the plan; combine with --yes to apply")
	cmd.Flags().Bool("clear-pause", false, "Also remove the manual pause marker when retargeting a stale anchor")
	return cmd
}

func runFix(ctx context.Context, out io.Writer, repo string, dryRun, yes, force, clearPause, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Resolve effective dry-run vs apply mode. The rules per SPEC LOCK:
	//   neither --yes nor --force: dry-run default (same as --dry-run).
	//   --yes alone: apply auto/safe actions (no purge).
	//   --force without --yes: dry-run that INCLUDES purge plan.
	//   --yes --force: apply auto + purge.
	// An explicit --dry-run always wins (operator inspection).
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
			if rerr := renderFix(out, plan, jsonOut); rerr != nil {
				return rerr
			}
			return err
		}
	}
	return renderFix(out, plan, jsonOut)
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
	if hasDecisionRecords {
		if err := planExternalDecisionFix(ctx, conn, repo, head, &plan); err != nil {
			return fixPlan{}, err
		}
	}
	// resolve_already_landed_barrier must precede delete_obsolete_barrier:
	// a blocked row whose captured after-state matches HEAD is BOTH "obsolete"
	// (no pending successors) AND "already landed externally". The promote
	// path preserves the decision_records audit trail, so we want that
	// outcome instead of a silent delete. Build a skip-set keyed by seq so
	// planObsoleteBarrierFix never plans a delete for the same row.
	resolveSeqs := map[int64]struct{}{}
	if branchRef != "" {
		if err := planResolveAlreadyLandedBarrier(ctx, conn, repo, head, branchRef, plan.Generation, &plan); err != nil {
			return fixPlan{}, err
		}
		for _, a := range plan.Actions {
			if a.Kind == fixActionResolveAlreadyLandedBarrier {
				resolveSeqs[a.Seq] = struct{}{}
			}
		}
	}
	if err := planObsoleteBarrierFix(ctx, conn, hasDecisionRecords, &plan, resolveSeqs); err != nil {
		return fixPlan{}, err
	}
	if branchRef != "" {
		if force {
			// Destructive purges must be applied before retarget_stale_anchor can
			// reset blocked_conflict rows back to pending. Keep this order in the
			// action list so --force --yes is order-safe.
			if err := planPurgeBarrierWithSuccessors(ctx, conn, branchRef, plan.Generation, &plan); err != nil {
				return fixPlan{}, err
			}
		} else {
			// Without --force we never plan the purge, but we still nudge the
			// operator when a stuck barrier exists so they can opt in.
			n, err := countBarrierBlockedWithSuccessors(ctx, conn, branchRef, plan.Generation)
			if err != nil {
				return fixPlan{}, err
			}
			if n > 0 {
				plan.ForceRequired = true
				plan.Suggestions = append(plan.Suggestions, fmt.Sprintf(
					"%d blocked barrier row(s) still have pending successors; run `acd fix --repo %s --force --yes` to purge them.", n, plan.Repo))
			}
		}
		if err := planRetargetStaleAnchor(ctx, conn, repo, head, branchRef, plan.Generation, &plan); err != nil {
			return fixPlan{}, err
		}
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

func planObsoleteBarrierFix(ctx context.Context, conn *sql.DB, hasDecisionRecords bool, plan *fixPlan, skipSeqs map[int64]struct{}) error {
	decisionFilter := ""
	if hasDecisionRecords {
		decisionFilter = `
  AND NOT EXISTS (
      SELECT 1
      FROM decision_records d
      WHERE d.event_seq = e.seq
        AND d.kind IN (?, ?)
        AND d.commit_oid IS NOT NULL
        AND d.commit_oid <> ''
  )`
	}
	q := `
SELECT seq, path, state
FROM capture_events e
WHERE state IN (?, ?)
` + decisionFilter + `
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events pending
      WHERE pending.branch_ref = e.branch_ref
        AND pending.branch_generation = e.branch_generation
        AND pending.seq > e.seq
        AND pending.state = ?
  )
ORDER BY seq ASC`
	args := []any{state.EventStateBlockedConflict, state.EventStateFailed}
	if hasDecisionRecords {
		args = append(args, state.DecisionKindHandledExternal, state.DecisionKindSupersededExternal)
	}
	args = append(args, state.EventStatePending)
	rows, err := conn.QueryContext(ctx, q, args...)
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
		if _, skip := skipSeqs[seq]; skip {
			// resolve_already_landed_barrier already owns this seq; skip the
			// delete so the promote path can run and keep the audit trail.
			continue
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
	blockingDecisionFilter := ""
	if hasDecisionRecords {
		blockingDecisionFilter = `
  AND NOT EXISTS (
      SELECT 1
      FROM decision_records d
      WHERE d.event_seq = e.seq
        AND d.kind IN (?, ?)
        AND d.commit_oid IS NOT NULL
        AND d.commit_oid <> ''
  )`
	}
	blockingQ := `
SELECT COUNT(*)
FROM capture_events e
WHERE state IN (?, ?)
` + blockingDecisionFilter + `
  AND EXISTS (
      SELECT 1
      FROM capture_events pending
      WHERE pending.branch_ref = e.branch_ref
        AND pending.branch_generation = e.branch_generation
        AND pending.seq > e.seq
        AND pending.state = ?
  )`
	blockingArgs := []any{state.EventStateBlockedConflict, state.EventStateFailed}
	if hasDecisionRecords {
		blockingArgs = append(blockingArgs, state.DecisionKindHandledExternal, state.DecisionKindSupersededExternal)
	}
	blockingArgs = append(blockingArgs, state.EventStatePending)
	if err := conn.QueryRowContext(ctx, blockingQ, blockingArgs...).Scan(&blocking); err != nil {
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
		ops, err := loadFixCaptureOps(ctx, conn, c.seq)
		if err != nil {
			return err
		}
		if len(ops) == 0 {
			plan.Unsafe = append(plan.Unsafe, fmt.Sprintf("decision %d for event %d has no capture ops to verify", c.decisionID, c.seq))
			continue
		}
		ok, err := git.IsAncestor(ctx, repo, c.commitOID, head)
		if err != nil {
			plan.Unsafe = append(plan.Unsafe, fmt.Sprintf("decision %d for event %d references unresolved commit %s", c.decisionID, c.seq, c.commitOID))
			continue
		}
		if !ok {
			plan.Unsafe = append(plan.Unsafe, fmt.Sprintf("decision %d for event %d references commit %s outside current HEAD history", c.decisionID, c.seq, shortOID(c.commitOID, 12)))
			continue
		}
		matches, err := currentHeadMatchesFixDecision(ctx, repo, head, c.kind, ops)
		if err != nil {
			return err
		}
		if !matches {
			plan.Unsafe = append(plan.Unsafe, fmt.Sprintf("decision %d for event %d no longer matches current HEAD tree", c.decisionID, c.seq))
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

func currentHeadMatchesFixDecision(ctx context.Context, repo, head, kind string, ops []state.CaptureOp) (bool, error) {
	for _, op := range ops {
		var ok bool
		var err error
		switch kind {
		case state.DecisionKindHandledExternal:
			ok, err = treeMatchesCapturedAfter(ctx, repo, head, op)
		case state.DecisionKindSupersededExternal:
			ok, err = treeMatchesCapturedBeforeForFix(ctx, repo, head, op)
		default:
			ok = false
		}
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func treeMatchesCapturedAfter(ctx context.Context, repo, ref string, op state.CaptureOp) (bool, error) {
	if op.Op == "delete" {
		return treePathAbsent(ctx, repo, ref, op.Path)
	}
	if !op.AfterOID.Valid || op.AfterOID.String == "" {
		return false, nil
	}
	if ok, err := treeBlobMatches(ctx, repo, ref, op.Path, op.AfterOID.String, op.AfterMode); err != nil || !ok {
		return ok, err
	}
	if op.Op == "rename" && op.OldPath.Valid && op.OldPath.String != "" {
		return treePathAbsent(ctx, repo, ref, op.OldPath.String)
	}
	return true, nil
}

func treeMatchesCapturedBeforeForFix(ctx context.Context, repo, ref string, op state.CaptureOp) (bool, error) {
	switch op.Op {
	case "create":
		return treePathAbsent(ctx, repo, ref, op.Path)
	case "delete", "modify":
		if !op.BeforeOID.Valid || op.BeforeOID.String == "" {
			return false, nil
		}
		return treeBlobMatches(ctx, repo, ref, op.Path, op.BeforeOID.String, op.BeforeMode)
	default:
		return false, nil
	}
}

func treeBlobMatches(ctx context.Context, repo, ref, path, oid string, mode sql.NullString) (bool, error) {
	entries, err := git.LsTree(ctx, repo, ref, false, path)
	if err != nil {
		return false, fmt.Errorf("acd fix: ls-tree %s %s: %w", ref, path, err)
	}
	for _, entry := range entries {
		if entry.Path != path || entry.Type != "blob" {
			continue
		}
		if entry.OID != oid {
			return false, nil
		}
		if mode.Valid && mode.String != "" && entry.Mode != mode.String {
			return false, nil
		}
		return true, nil
	}
	return false, nil
}

func treePathAbsent(ctx context.Context, repo, ref, path string) (bool, error) {
	entries, err := git.LsTree(ctx, repo, ref, false, path)
	if err != nil {
		return false, fmt.Errorf("acd fix: ls-tree %s %s: %w", ref, path, err)
	}
	for _, entry := range entries {
		if entry.Path == path {
			return false, nil
		}
	}
	return true, nil
}

func applyFixPlan(ctx context.Context, stateDB string, plan *fixPlan) error {
	if plan.CurrentBranchRef == "" {
		return fmt.Errorf("acd fix: refusing to mutate state while HEAD is detached")
	}
	// Defense-in-depth: refuse to apply any RequiresForce action unless the
	// plan's Force flag was set at build time. Planner gating already prevents
	// these actions from appearing without --force, so this guard catches
	// future refactors (re-hydrated plans, alternate callers) before they can
	// silently apply a destructive purge.
	for _, action := range plan.Actions {
		if action.RequiresForce && !plan.Force {
			return fmt.Errorf("acd fix: refusing to apply %s without --force (planner gating bypassed)", action.Kind)
		}
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
	branchRef, err := git.RunBranchRef(ctx, plan.Repo)
	if err != nil {
		return fmt.Errorf("acd fix: recheck HEAD branch: %w", err)
	}
	head, err := git.RevParse(ctx, plan.Repo, "HEAD")
	if err != nil {
		return fmt.Errorf("acd fix: recheck HEAD: %w", err)
	}
	if branchRef != plan.CurrentBranchRef || head != plan.CurrentHead {
		return fmt.Errorf("acd fix: refusing to mutate state because HEAD changed during planning")
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

	// Retarget post-commit phase: when retarget_stale_anchor was applied,
	// run the live-index repair pass and reconcile the manual pause marker
	// the same way acd recover did. These steps must run AFTER tx.Commit so
	// the slow git ops never hold the SQLite write lock.
	if planHasAction(plan, fixActionRetargetStaleAnchor) {
		if err := finalizeRetargetPostCommit(ctx, db, plan); err != nil {
			return err
		}
	}
	if err := verifyFixPostApply(ctx, db.SQL(), plan); err != nil {
		return err
	}
	return nil
}

func planHasAction(plan *fixPlan, kind string) bool {
	for _, action := range plan.Actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

func verifyFixPostApply(ctx context.Context, conn *sql.DB, plan *fixPlan) error {
	var errs []string
	for _, action := range plan.Actions {
		if action.Kind == fixActionPurgeBarrierWithSuccessors && action.RowsChanged == 0 {
			errs = append(errs, fmt.Sprintf("planned purge seq=%d changed zero rows", action.Seq))
		}
	}
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
	// Retarget with --clear-pause also wants to remove the manual pause
	// marker; perform the same FS preflight BEFORE opening the SQLite
	// write tx so a slow stat on a network mount cannot hold the write
	// lock. The retarget path uses plan.ManualMarkerPath (computed by
	// planRetargetStaleAnchor) which may equal plan.ManualPausePath; the
	// stat is cheap and idempotent so duplication is fine.
	if plan.ClearPause && planHasAction(plan, fixActionRetargetStaleAnchor) {
		markerPath := plan.ManualPausePath
		if info, err := os.Lstat(markerPath); err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("acd fix: manual pause marker %s is not a regular file", markerPath)
			}
			if err := checkParentDirWritable(markerPath); err != nil {
				return fmt.Errorf("acd fix: manual pause marker parent not writable: %w", err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("acd fix: stat manual pause marker %s: %w", markerPath, err)
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
	case fixActionResolveAlreadyLandedBarrier:
		// Promote the blocked_conflict row to published(commit_oid=HEAD)
		// and record decision handled_external_after_block. Race-safe: the
		// UPDATE includes the state predicate so a concurrent transition
		// out of blocked_conflict cannot be overwritten.
		res, err := tx.ExecContext(ctx, `
UPDATE capture_events
SET state = ?, commit_oid = ?, error = NULL, published_ts = ?
WHERE seq = ? AND state = ?`,
			state.EventStatePublished, action.CommitOID, nowSec, action.Seq,
			state.EventStateBlockedConflict)
		if err != nil {
			return 0, fmt.Errorf("acd fix: promote blocked seq %d: %w", action.Seq, err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			// Row already moved out of blocked_conflict (race with another
			// recovery action or replay pass). Treat as a no-op — the row
			// is no longer ours to settle here.
			return 0, nil
		}
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
			action.Seq, action.BranchRef, action.BranchGeneration, action.BaseHead, action.CommitOID, nowSec); err != nil {
			return 0, fmt.Errorf("acd fix: upsert publish_state for self-heal seq %d: %w", action.Seq, err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO decision_records(
    decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
    branch_ref, branch_generation, action_taken, user_message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			nowSec, state.DecisionKindHandledExternalAfterBlock,
			nullableString(action.Path),
			nullableString("already_published_by_external_committer"),
			sql.NullInt64{Int64: action.Seq, Valid: true},
			nullableString(action.BaseHead),
			nullableString(action.CommitOID),
			nullableString(action.BranchRef),
			sql.NullInt64{Int64: action.BranchGeneration, Valid: true},
			nullableString("handled externally after block"),
			nullableString(fmt.Sprintf("External commit cleared blocked replay for %s.", action.Path)),
		); err != nil {
			return 0, fmt.Errorf("acd fix: append decision for self-heal seq %d: %w", action.Seq, err)
		}
		return n, nil
	case fixActionRetargetStaleAnchor:
		return applyRetargetStaleAnchorTx(ctx, tx, plan, nowSec)
	case fixActionPurgeBarrierWithSuccessors:
		// Delete one specific blocked_conflict row by seq (planner enumerates
		// rows with pending successors). Tightly scoped to seq + state so
		// concurrent transitions cannot accidentally clobber an unrelated row.
		res, err := tx.ExecContext(ctx, `
DELETE FROM capture_events
WHERE seq = ? AND state = ?`,
			action.Seq, state.EventStateBlockedConflict)
		if err != nil {
			return 0, fmt.Errorf("acd fix: purge blocked seq %d: %w", action.Seq, err)
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			// Clear publish_state breadcrumb if it pointed at this seq.
			if _, err := tx.ExecContext(ctx, `
UPDATE publish_state
SET status = 'ok', error = NULL, updated_ts = ?
WHERE id = 1
  AND status = 'blocked_conflict'
  AND event_seq = ?`, nowSec, action.Seq); err != nil {
				return 0, fmt.Errorf("acd fix: clear publish_state for purged seq %d: %w", action.Seq, err)
			}
		}
		return n, nil
	default:
		return 0, fmt.Errorf("acd fix: unknown action kind %q", action.Kind)
	}
}

func nullableString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
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

// planResolveAlreadyLandedBarrier appends resolve_already_landed_barrier
// actions for every blocked_conflict row whose self-heal predicate currently
// holds. Predicate parity with the daemon-side probe is enforced by
// scanAutoResolvableBlockedRows (internal/cli/recovery_probe.go).
func planResolveAlreadyLandedBarrier(ctx context.Context, conn *sql.DB, repo, head, branchRef string, generation int64, plan *fixPlan) error {
	candidates, err := scanAutoResolvableBlockedRows(ctx, conn, repo, head, branchRef, generation)
	if err != nil {
		return fmt.Errorf("acd fix: scan auto-resolvable blocked rows: %w", err)
	}
	for _, c := range candidates {
		afterOID := ""
		if len(c.Ops) > 0 && c.Ops[0].AfterOID.Valid {
			afterOID = c.Ops[0].AfterOID.String
		}
		blobOID := ""
		if len(c.Ops) > 0 {
			if oid, err := git.LsTreeBlobOID(ctx, repo, c.HeadOID, c.Ops[0].Path); err == nil {
				blobOID = oid
			}
		}
		plan.Actions = append(plan.Actions, fixAction{
			ID:               fmt.Sprintf("%s:%d", fixActionResolveAlreadyLandedBarrier, c.Seq),
			Kind:             fixActionResolveAlreadyLandedBarrier,
			Description:      "promote already-landed blocked barrier to published",
			Reason:           "captured after-state matches HEAD; external committer landed the change",
			Seq:              c.Seq,
			Path:             c.Path,
			CommitOID:        c.HeadOID,
			BlobOID:          shortOID(blobOID, 12),
			CapturedAfterOID: shortOID(afterOID, 12),
			BranchRef:        c.BranchRef,
			BranchGeneration: c.Generation,
			BaseHead:         c.BaseHead,
		})
	}
	return nil
}

// planRetargetStaleAnchor decides whether a retarget_stale_anchor action is
// warranted. We mirror acd recover's gating: refuse on detached HEAD (already
// recorded as plan.Unsafe at the start of buildFixPlan); otherwise plan the
// retarget if there are pending/blocked rows on a stale (branch_ref,
// generation) that no longer matches the live anchor, OR replay-pause meta
// keys remain that the daemon never cleared.
func planRetargetStaleAnchor(ctx context.Context, conn *sql.DB, repo, head, branchRef string, generation int64, plan *fixPlan) error {
	if branchRef == "" {
		return nil
	}
	// Two trigger conditions for retarget (either alone is enough):
	//   1. capture_events rows in (pending, blocked_conflict) reference a
	//      (branch_ref, branch_generation, base_head) tuple that no longer
	//      matches the current attached anchor;
	//   2. daemon_meta still carries stale replay/pause breadcrumbs the
	//      daemon should have cleared on resume.
	var staleRows int
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM capture_events
WHERE state IN (?, ?)
  AND (branch_ref <> ? OR branch_generation <> ? OR base_head <> ?)`,
		state.EventStatePending, state.EventStateBlockedConflict,
		branchRef, generation, head).Scan(&staleRows); err != nil {
		return fmt.Errorf("acd fix: count stale anchor rows: %w", err)
	}
	hasStaleMeta := false
	for _, key := range []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		"last_replay_error",
		"detached_head_paused",
		"operation_in_progress",
		daemon.MetaKeyReplayPausedUntil,
	} {
		if _, ok, err := metaLookup(ctx, conn, key); err != nil {
			return fmt.Errorf("acd fix: read meta %s: %w", key, err)
		} else if ok {
			hasStaleMeta = true
			break
		}
	}
	if staleRows == 0 && !hasStaleMeta {
		return nil
	}
	description := "retarget stale capture/shadow rows to current branch/generation/head and clear stale replay/pause meta"
	plan.Actions = append(plan.Actions, fixAction{
		ID:               fixActionRetargetStaleAnchor,
		Kind:             fixActionRetargetStaleAnchor,
		Description:      description,
		Reason:           fmt.Sprintf("stale_rows=%d stale_meta=%v", staleRows, hasStaleMeta),
		BranchRef:        branchRef,
		BranchGeneration: generation,
		BaseHead:         head,
	})
	return nil
}

// planPurgeBarrierWithSuccessors enumerates blocked_conflict rows whose
// (branch_ref, generation) still has pending successors. Only invoked when
// --force is set on the planner inputs.
func planPurgeBarrierWithSuccessors(ctx context.Context, conn *sql.DB, branchRef string, generation int64, plan *fixPlan) error {
	rows, err := scanBarrierBlockedRowsWithSuccessors(ctx, conn, branchRef, generation)
	if err != nil {
		return fmt.Errorf("acd fix: scan barrier-with-successors rows: %w", err)
	}
	for _, r := range rows {
		plan.Actions = append(plan.Actions, fixAction{
			ID:               fmt.Sprintf("%s:%d", fixActionPurgeBarrierWithSuccessors, r.Seq),
			Kind:             fixActionPurgeBarrierWithSuccessors,
			Description:      "purge blocked barrier with pending successors (destructive)",
			Reason:           "blocked_conflict row still has pending successors on same (branch_ref, generation)",
			Seq:              r.Seq,
			Path:             r.Path,
			BranchRef:        r.BranchRef,
			BranchGeneration: r.Generation,
			RequiresForce:    true,
		})
	}
	return nil
}

// applyRetargetStaleAnchorTx ports the in-tx half of applyRecoverPlan into
// acd fix. The mutations are deliberately identical so existing recover_test
// fixtures still describe the contract. Live-index repair and the manual
// pause marker reconciliation run AFTER tx.Commit in finalizeRetargetPostCommit
// so slow FS/git ops never hold the SQLite write lock.
func applyRetargetStaleAnchorTx(ctx context.Context, tx *sql.Tx, plan *fixPlan, nowSec float64) (int64, error) {
	var rowsChanged int64
	execTracked := func(q string, args ...any) error {
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return err
		}
		if n, rerr := res.RowsAffected(); rerr == nil {
			rowsChanged += n
		}
		return nil
	}

	if err := execTracked(`UPDATE capture_events
SET branch_ref = ?, branch_generation = ?, base_head = ?
WHERE state IN (?, ?)`,
		plan.CurrentBranchRef, plan.Generation, plan.CurrentHead,
		state.EventStatePending, state.EventStateBlockedConflict); err != nil {
		return 0, fmt.Errorf("acd fix: retarget capture_events: %w", err)
	}
	if err := execTracked(`UPDATE capture_events
SET state = ?, published_ts = NULL, error = NULL
WHERE state = ?`,
		state.EventStatePending, state.EventStateBlockedConflict); err != nil {
		return 0, fmt.Errorf("acd fix: reset blocked events: %w", err)
	}
	if err := execTracked(`DELETE FROM shadow_paths
WHERE rowid NOT IN (
    SELECT keep_rowid FROM (
        SELECT MAX(rowid) AS keep_rowid
        FROM shadow_paths
        GROUP BY path
    )
)`); err != nil {
		return 0, fmt.Errorf("acd fix: dedupe shadow_paths: %w", err)
	}
	if err := execTracked(`UPDATE shadow_paths
SET branch_ref = ?, branch_generation = ?, base_head = ?`,
		plan.CurrentBranchRef, plan.Generation, plan.CurrentHead); err != nil {
		return 0, fmt.Errorf("acd fix: retarget shadow_paths: %w", err)
	}
	if err := execTracked(`UPDATE publish_state
SET branch_ref = ?, branch_generation = ?, source_head = ?, status = 'idle', error = NULL`,
		plan.CurrentBranchRef, plan.Generation, plan.CurrentHead); err != nil {
		return 0, fmt.Errorf("acd fix: retarget publish_state: %w", err)
	}
	if err := execTracked(`UPDATE daemon_state
SET branch_ref = ?, branch_generation = ?, mode = 'stopped', note = NULL`,
		plan.CurrentBranchRef, plan.Generation); err != nil {
		return 0, fmt.Errorf("acd fix: retarget daemon_state: %w", err)
	}
	for _, key := range []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		"last_replay_error",
		"detached_head_paused",
		"operation_in_progress",
		daemon.MetaKeyReplayPausedUntil,
	} {
		if err := execTracked(`DELETE FROM daemon_meta WHERE key = ?`, key); err != nil {
			return 0, fmt.Errorf("acd fix: clear %s: %w", key, err)
		}
	}
	if err := execTracked(`INSERT INTO daemon_meta(key, value, updated_ts) VALUES('branch_token', ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`,
		"rev:"+plan.CurrentHead+" "+plan.CurrentBranchRef, nowSec); err != nil {
		return 0, fmt.Errorf("acd fix: set branch token: %w", err)
	}
	if err := execTracked(`INSERT INTO daemon_meta(key, value, updated_ts) VALUES('branch.generation', ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`,
		fmt.Sprintf("%d", plan.Generation), nowSec); err != nil {
		return 0, fmt.Errorf("acd fix: set branch generation: %w", err)
	}
	if err := execTracked(`INSERT INTO daemon_meta(key, value, updated_ts) VALUES('branch.head', ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts`,
		plan.CurrentHead, nowSec); err != nil {
		return 0, fmt.Errorf("acd fix: set branch head: %w", err)
	}
	return rowsChanged, nil
}

// finalizeRetargetPostCommit runs the live-index repair pass and reconciles
// the manual pause marker after the retarget tx commits. Failures here are
// reported but the DB transaction has already landed, so we never re-open it.
func finalizeRetargetPostCommit(ctx context.Context, db *state.DB, plan *fixPlan) error {
	repaired, err := daemon.RepairPublishedLiveIndex(ctx, plan.Repo, db, plan.CurrentHead, daemon.DefaultLiveIndexRepairLimit)
	if err != nil {
		return fmt.Errorf("acd fix: repair live index: %w", err)
	}
	plan.LiveIndexCandidates = repaired.Candidates
	plan.LiveIndexApplied = repaired.Applied
	plan.LiveIndexSkipped = len(repaired.Skipped)

	if plan.ManualPausePath == "" {
		return nil
	}
	if _, err := os.Lstat(plan.ManualPausePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("acd fix: stat manual pause marker after commit: %w", err)
	}
	if !plan.ClearPause {
		plan.ManualMarkerPreserved = true
		return nil
	}
	if err := os.Remove(plan.ManualPausePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		plan.ManualMarkerRemoveError = err.Error()
		return nil
	}
	plan.ManualMarkerRemoved = true
	stampManualPauseResume(ctx, plan.GitDir)
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
		switch {
		case plan.ManualMarkerRemoved:
			fmt.Fprintf(out, "Manual pause marker (retarget) removed: %s\n", plan.ManualPausePath)
		case plan.ManualMarkerPreserved:
			fmt.Fprintf(out, "Manual pause marker (retarget) preserved: %s (use --clear-pause to remove)\n", plan.ManualPausePath)
		}
		if plan.ManualMarkerRemoveError != "" {
			fmt.Fprintf(out, "WARNING: manual pause marker remove failed after commit: %s\n", plan.ManualMarkerRemoveError)
		}
		if plan.LiveIndexCandidates > 0 || plan.LiveIndexApplied > 0 || plan.LiveIndexSkipped > 0 {
			fmt.Fprintf(out, "Live index repair: candidates=%d applied=%d skipped=%d\n",
				plan.LiveIndexCandidates, plan.LiveIndexApplied, plan.LiveIndexSkipped)
		}
	} else {
		hint := "pass --yes to apply safe actions"
		if plan.Force {
			hint = "pass --yes to apply safe actions; combine --yes --force to also apply destructive purge"
		} else {
			for _, action := range plan.Actions {
				if action.RequiresForce {
					hint = "pass --yes to apply safe actions; rerun with --force to plan purge_barrier_with_successors"
					break
				}
			}
		}
		fmt.Fprintf(out, "(dry-run; %s)\n", hint)
	}
	return nil
}

package cli

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

type diagnoseAnchorReport struct {
	GitHEADBranch          string `json:"git_head_branch,omitempty"`
	GitHEADDetached        bool   `json:"git_head_detached"`
	DaemonBranchRef        string `json:"daemon_branch_ref,omitempty"`
	DaemonBranchGeneration int64  `json:"daemon_branch_generation,omitempty"`
	BranchToken            string `json:"branch_token,omitempty"`
	BranchGeneration       string `json:"branch_generation,omitempty"`
	BranchHead             string `json:"branch_head,omitempty"`
	Mismatch               bool   `json:"mismatch"`
}

type diagnoseBlockedClass struct {
	ErrorClass string `json:"error_class"`
	Count      int    `json:"count"`
}

type diagnoseBlockedEntry struct {
	Seq              int64  `json:"seq"`
	State            string `json:"state"`
	Path             string `json:"path"`
	Operation        string `json:"operation"`
	ErrorClass       string `json:"error_class"`
	Error            string `json:"error,omitempty"`
	PublishedTS      int64  `json:"published_ts,omitempty"`
	BranchRef        string `json:"branch_ref,omitempty"`
	BranchGeneration int64  `json:"branch_generation,omitempty"`
}

type diagnoseGeneratedPendingGroup struct {
	Root             string  `json:"root"`
	Pattern          string  `json:"pattern"`
	BranchRef        string  `json:"branch_ref"`
	BranchGeneration int64   `json:"branch_generation"`
	BaseHead         string  `json:"base_head,omitempty"`
	PendingCount     int     `json:"pending_count"`
	TrackedCount     int     `json:"tracked_count"`
	OldestSeq        int64   `json:"oldest_seq"`
	NewestSeq        int64   `json:"newest_seq"`
	EventSeqs        []int64 `json:"event_seqs,omitempty"`
}

type diagnoseReport struct {
	PublicationOutcome         publicationOutcome              `json:"publication_outcome"`
	Repo                       string                          `json:"repo"`
	RepoHash                   string                          `json:"repo_hash"`
	StateDB                    string                          `json:"state_db"`
	Anchor                     diagnoseAnchorReport            `json:"anchor"`
	PendingDepth               int                             `json:"pending_depth"`
	FailedEvents               int                             `json:"failed_events"`
	FailedBlockingPending      int                             `json:"failed_blocking_pending"`
	PendingHighWater           int64                           `json:"pending_high_water"`
	BackpressurePaused         bool                            `json:"backpressure_paused"`
	BackpressurePausedAt       string                          `json:"backpressure_paused_at,omitempty"`
	EventsDroppedTotal         int64                           `json:"events_dropped_total"`
	BlockedHistogram           []diagnoseBlockedClass          `json:"blocked_histogram"`
	RecentBlocked              []diagnoseBlockedEntry          `json:"recent_blocked"`
	GeneratedPending           []diagnoseGeneratedPendingGroup `json:"generated_pending,omitempty"`
	AutoResolvableBlockedCount int                             `json:"auto_resolvable_blocked_count"`
	BarrierWithSuccessorsCount int                             `json:"barrier_with_successors_count"`
	OperationInProgress        string                          `json:"operation_in_progress,omitempty"`
	StaleOperationMarker       bool                            `json:"stale_operation_marker"`
	OperationMarkerDuration    string                          `json:"operation_marker_duration,omitempty"`
	// DeadBranchPruneLastRunTS / DeadBranchPruneLastCount /
	// DeadBranchPruneLastRefs surface the most recent non-empty dead-branch
	// recovery action so operators can confirm stale-branch hygiene
	// is keeping pace. The two int fields are ALWAYS present in the JSON —
	// a zero value is the documented "daemon has never recorded a non-empty
	// recovery" sentinel, distinguishable from any real recovery (real actions
	// stamp last_run_ts to the wall-clock unix-second the recovery ran and
	// last_count >= 1). The slice keeps `omitempty` so an empty list
	// serializes as absent rather than `null`/`[]`.
	DeadBranchPruneLastRunTS int64                        `json:"dead_branch_prune_last_run_ts"`
	DeadBranchPruneLastCount int                          `json:"dead_branch_prune_last_count"`
	DeadBranchPruneLastRefs  []string                     `json:"dead_branch_prune_last_refs,omitempty"`
	Remediation              []string                     `json:"remediation"`
	StateDBChecksumBefore    string                       `json:"state_db_checksum_before"`
	StateDBChecksumAfter     string                       `json:"state_db_checksum_after"`
	StateDBChecksumVerified  bool                         `json:"state_db_checksum_verified"`
	Busy                     bool                         `json:"busy"`
	OperationalState         string                       `json:"operational_state"`
	WorktreeClean            bool                         `json:"worktree_clean"`
	AllChangesCommittedInGit bool                         `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD bool                         `json:"checkpoint_published_by_acd"`
	CaptureHealth            state.CaptureHealth          `json:"capture_health"`
	CheckpointMaintenance    checkpoint.MaintenanceStatus `json:"checkpoint_maintenance"`
}

func newDiagnoseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diagnose",
		Short: "Inspect replay blockers and branch anchors without mutating state",
		Long: `Inspect replay blockers, pending depth, branch anchor state, git-operation markers, and remediation hints for one repo.

The default repo is the current working directory. Diagnose opens state read-only and verifies the state DB checksum before and after inspection. Pending-only intent queues are reported as waiting/draining, while terminal barriers point to acd fix --dry-run and archive-only recovery previews when needed.`,
		Example: `  acd diagnose
  acd diagnose --repo /path/to/repo
  acd diagnose --json
  acd fix --dry-run
  acd fix --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runProductDiagnose(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
	return cmd
}

func runDiagnose(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	report, err := collectDiagnose(ctx, repo)
	if err != nil {
		return err
	}

	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	return renderDiagnoseHuman(out, report)
}

func collectDiagnose(ctx context.Context, repo string) (diagnoseReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rec, _, _, err := lookupRegisteredRepo("diagnose", repo)
	if err != nil {
		return diagnoseReport{}, err
	}

	report, err := buildDiagnoseReport(ctx, rec)
	if err != nil {
		return diagnoseReport{}, fmt.Errorf("acd diagnose: %w", err)
	}

	return report, nil
}

func buildDiagnoseReport(ctx context.Context, rec central.RepoRecord) (diagnoseReport, error) {
	report := diagnoseReport{Repo: rec.Path, RepoHash: rec.RepoHash, StateDB: rec.StateDB}
	before, err := fileSHA256(rec.StateDB)
	if err != nil {
		return report, err
	}
	report.StateDBChecksumBefore = before
	status, err := buildStatusReport(ctx, rec, time.Now())
	if err != nil {
		return report, err
	}
	report.PublicationOutcome = status.PublicationOutcome
	report.CaptureHealth = status.CaptureHealth
	report.CheckpointMaintenance = status.CheckpointMaintenance
	report.Busy, report.OperationalState = status.Busy, status.OperationalState
	report.WorktreeClean, report.AllChangesCommittedInGit = status.WorktreeClean, status.AllChangesCommittedInGit
	report.CheckpointPublishedByACD = status.CheckpointPublishedByACD
	control := controlResult{Enabled: true}
	applyFinishControlStatus(&control, status, status.PublicationProgress.WorkerResponsive)
	if control.NextAction != "No action needed." {
		report.Remediation = []string{control.NextAction}
	}
	after, err := fileSHA256(rec.StateDB)
	if err != nil {
		return report, err
	}
	report.StateDBChecksumAfter = after
	report.StateDBChecksumVerified = before == after
	if before != after {
		return report, errors.New("state.db changed while reading diagnostics")
	}
	return report, nil
}

func openStateDBReadOnly(ctx context.Context, dbPath string) (*sql.DB, error) {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	conn, err := sql.Open("sqlite", paths.DatabaseURI(dbPath, q))
	if err != nil {
		return nil, fmt.Errorf("open state.db read-only: %w", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping state.db read-only: %w", err)
	}
	return conn, nil
}

func fileSHA256(path string) (string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func renderDiagnoseHuman(out io.Writer, r diagnoseReport) error {
	if r.PublicationOutcome.CheckpointAvailable {
		fmt.Fprintf(out, "Repository: %s\n", r.Repo)
		renderFinishOutcomeHuman(out, r.PublicationOutcome)
		if r.CaptureHealth.Error != "" {
			fmt.Fprintf(out, "Protection: %s\n", r.CaptureHealth.Error)
		}
		for _, next := range r.Remediation {
			fmt.Fprintf(out, "Next: %s\n", next)
		}
		fmt.Fprintf(out, "Read-only state checksum unchanged: %s\n", yesNo(r.StateDBChecksumVerified))
		return nil
	}

	fmt.Fprintf(out, "Repo: %s\n", r.Repo)
	fmt.Fprintf(out, "State DB: %s\n", r.StateDB)
	current := r.Anchor.GitHEADBranch
	if current == "" {
		current = "detached"
	}
	daemon := r.Anchor.DaemonBranchRef
	if daemon == "" {
		daemon = "unset"
	}
	fmt.Fprintf(out, "Anchor: git HEAD=%s daemon=%s", current, daemon)
	if r.Anchor.DaemonBranchGeneration > 0 {
		fmt.Fprintf(out, " generation=%d", r.Anchor.DaemonBranchGeneration)
	}
	if r.Anchor.Mismatch {
		fmt.Fprint(out, " MISMATCH")
	}
	fmt.Fprintln(out)
	if r.Anchor.BranchToken != "" {
		fmt.Fprintf(out, "Branch token: %s\n", r.Anchor.BranchToken)
	}
	if r.Anchor.BranchGeneration != "" || r.Anchor.BranchHead != "" {
		fmt.Fprintf(out, "Persisted branch: generation=%s head=%s\n", valueOrUnset(r.Anchor.BranchGeneration), valueOrUnset(r.Anchor.BranchHead))
	}

	fmt.Fprintf(out, "Capture queue: pending=%d high_water=%d dropped_total=%d\n",
		r.PendingDepth, r.PendingHighWater, r.EventsDroppedTotal)
	if r.FailedEvents > 0 {
		fmt.Fprintf(out, "Failed terminal events: %d", r.FailedEvents)
		if r.FailedBlockingPending > 0 {
			fmt.Fprintf(out, " (%d archived pending successors; run acd support recover --dry-run)", r.FailedBlockingPending)
		}
		fmt.Fprintln(out)
	}
	if r.BackpressurePaused {
		fmt.Fprintf(out, "Backpressure: paused at %s\n", valueOrUnset(r.BackpressurePausedAt))
	}

	if r.OperationInProgress != "" {
		stale := ""
		if r.StaleOperationMarker {
			stale = " STALE"
		}
		fmt.Fprintf(out, "Git operation: %s present for %s%s\n",
			r.OperationInProgress, r.OperationMarkerDuration, stale)
	}

	if len(r.GeneratedPending) > 0 {
		fmt.Fprintln(out, "Generated pending deletes:")
		for _, group := range r.GeneratedPending {
			fmt.Fprintf(out, "  - root=%s pending=%d tracked=%d seq=%d..%d pattern=%s\n",
				group.Root, group.PendingCount, group.TrackedCount, group.OldestSeq, group.NewestSeq, group.Pattern)
		}
	}

	fmt.Fprintln(out, "Terminal replay barrier histogram:")
	if len(r.BlockedHistogram) == 0 {
		fmt.Fprintln(out, "  none")
	} else {
		for _, bucket := range r.BlockedHistogram {
			fmt.Fprintf(out, "  - %s: %d\n", bucket.ErrorClass, bucket.Count)
		}
	}

	fmt.Fprintln(out, "Recent replay barriers:")
	if len(r.RecentBlocked) == 0 {
		fmt.Fprintln(out, "  none")
	} else {
		for _, entry := range r.RecentBlocked {
			fmt.Fprintf(out, "  - seq %d %s %s %s (%s)", entry.Seq, entry.State, entry.Operation, entry.Path, entry.ErrorClass)
			if entry.Error != "" {
				fmt.Fprintf(out, ": %s", entry.Error)
			}
			fmt.Fprintln(out)
		}
	}

	// Dead-branch recovery surface. Zero last_run_ts means the daemon has
	// never recorded a non-empty recovery — render nothing in that case so a
	// fresh repo's diagnose output is not cluttered with "never ran" noise.
	if r.DeadBranchPruneLastRunTS > 0 {
		fmt.Fprintf(out, "Dead-branch recovery: %d row(s) preserved at %s, branches=%v\n",
			r.DeadBranchPruneLastCount,
			time.Unix(r.DeadBranchPruneLastRunTS, 0).Format(time.RFC3339),
			r.DeadBranchPruneLastRefs,
		)
	}

	fmt.Fprintln(out, "Suggested remediation:")
	for _, item := range r.Remediation {
		fmt.Fprintf(out, "  - %s\n", item)
	}
	status := "failed"
	if r.StateDBChecksumVerified {
		status = "verified"
	}
	fmt.Fprintf(out, "Read-only: %s (state.db sha256 %s)\n", status, r.StateDBChecksumAfter)
	return nil
}

func valueOrUnset(v string) string {
	if v == "" {
		return "unset"
	}
	return v
}

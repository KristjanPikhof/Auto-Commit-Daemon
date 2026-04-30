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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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
	Path             string `json:"path"`
	Operation        string `json:"operation"`
	ErrorClass       string `json:"error_class"`
	Error            string `json:"error,omitempty"`
	PublishedTS      int64  `json:"published_ts,omitempty"`
	BranchRef        string `json:"branch_ref,omitempty"`
	BranchGeneration int64  `json:"branch_generation,omitempty"`
}

type diagnoseReport struct {
	Repo                    string                 `json:"repo"`
	RepoHash                string                 `json:"repo_hash"`
	StateDB                 string                 `json:"state_db"`
	Anchor                  diagnoseAnchorReport   `json:"anchor"`
	PendingDepth            int                    `json:"pending_depth"`
	PendingHighWater        int64                  `json:"pending_high_water"`
	BlockedHistogram        []diagnoseBlockedClass `json:"blocked_histogram"`
	RecentBlocked           []diagnoseBlockedEntry `json:"recent_blocked"`
	OperationInProgress     string                 `json:"operation_in_progress,omitempty"`
	StaleOperationMarker    bool                   `json:"stale_operation_marker"`
	OperationMarkerDuration string                 `json:"operation_marker_duration,omitempty"`
	Remediation             []string               `json:"remediation"`
	StateDBChecksumBefore   string                 `json:"state_db_checksum_before"`
	StateDBChecksumAfter    string                 `json:"state_db_checksum_after"`
	StateDBChecksumVerified bool                   `json:"state_db_checksum_verified"`
}

type replayConflictMeta struct {
	Seq        int64  `json:"seq"`
	ErrorClass string `json:"error_class"`
}

func newDiagnoseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diagnose",
		Short: "Inspect replay blockers and branch anchors without mutating state",
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runDiagnose(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
	return cmd
}

func runDiagnose(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	abs, err := resolveRepo(repo)
	if err != nil {
		return err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd diagnose: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return fmt.Errorf("acd diagnose: load registry: %w", err)
	}
	rec, ok := findRepo(reg, abs)
	if !ok {
		return fmt.Errorf("acd diagnose: repo %s is not registered (try `acd start --repo %s`)", abs, abs)
	}

	report, err := buildDiagnoseReport(ctx, rec)
	if err != nil {
		return fmt.Errorf("acd diagnose: %w", err)
	}

	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	return renderDiagnoseHuman(out, report)
}

func buildDiagnoseReport(ctx context.Context, rec central.RepoRecord) (diagnoseReport, error) {
	report := diagnoseReport{
		Repo:     rec.Path,
		RepoHash: rec.RepoHash,
		StateDB:  rec.StateDB,
	}
	if !fileExists(rec.StateDB) {
		return report, fmt.Errorf("state.db missing for repo %s", rec.Path)
	}

	before, err := fileSHA256(rec.StateDB)
	if err != nil {
		return report, fmt.Errorf("checksum before read: %w", err)
	}
	report.StateDBChecksumBefore = before

	conn, err := openStateDBReadOnly(ctx, rec.StateDB)
	if err != nil {
		return report, err
	}
	defer conn.Close()

	report.Anchor, err = diagnoseAnchor(ctx, conn, rec.Path)
	if err != nil {
		return report, err
	}
	if err := diagnoseCapacity(ctx, conn, &report); err != nil {
		return report, err
	}
	if err := diagnoseBlocked(ctx, conn, &report); err != nil {
		return report, err
	}
	if err := diagnoseOperationMarker(ctx, conn, &report); err != nil {
		return report, err
	}
	report.Remediation = diagnoseRemediation(report)

	after, err := fileSHA256(rec.StateDB)
	if err != nil {
		return report, fmt.Errorf("checksum after read: %w", err)
	}
	report.StateDBChecksumAfter = after
	report.StateDBChecksumVerified = before == after
	if !report.StateDBChecksumVerified {
		return report, fmt.Errorf("state.db checksum changed during read-only diagnose")
	}
	return report, nil
}

func openStateDBReadOnly(ctx context.Context, dbPath string) (*sql.DB, error) {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	conn, err := sql.Open("sqlite", "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("open state.db read-only: %w", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping state.db read-only: %w", err)
	}
	return conn, nil
}

func diagnoseAnchor(ctx context.Context, conn *sql.DB, repo string) (diagnoseAnchorReport, error) {
	var anchor diagnoseAnchorReport
	branch, err := git.RunBranchRef(ctx, repo)
	if err != nil {
		return anchor, fmt.Errorf("git symbolic-ref HEAD: %w", err)
	}
	anchor.GitHEADBranch = branch
	anchor.GitHEADDetached = branch == ""

	var branchRef sql.NullString
	var branchGen sql.NullInt64
	row := conn.QueryRowContext(ctx, `SELECT branch_ref, branch_generation FROM daemon_state WHERE id = 1`)
	if err := row.Scan(&branchRef, &branchGen); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return anchor, fmt.Errorf("daemon_state anchor: %w", err)
	}
	if branchRef.Valid {
		anchor.DaemonBranchRef = branchRef.String
	}
	if branchGen.Valid {
		anchor.DaemonBranchGeneration = branchGen.Int64
	}
	if v, ok, err := metaLookup(ctx, conn, "branch_token"); err != nil {
		return anchor, fmt.Errorf("branch_token: %w", err)
	} else if ok {
		anchor.BranchToken = v
		if anchor.DaemonBranchRef == "" {
			anchor.DaemonBranchRef = branchRefFromToken(v)
		}
	}
	if v, ok, err := metaLookup(ctx, conn, "branch.generation"); err != nil {
		return anchor, fmt.Errorf("branch.generation: %w", err)
	} else if ok {
		anchor.BranchGeneration = v
	}
	if v, ok, err := metaLookup(ctx, conn, "branch.head"); err != nil {
		return anchor, fmt.Errorf("branch.head: %w", err)
	} else if ok {
		anchor.BranchHead = v
	}

	anchor.Mismatch = anchor.GitHEADBranch != anchor.DaemonBranchRef &&
		(anchor.GitHEADBranch != "" || anchor.DaemonBranchRef != "")
	return anchor, nil
}

func branchRefFromToken(token string) string {
	parts := strings.Fields(token)
	if len(parts) >= 2 && strings.HasPrefix(parts[1], "refs/") {
		return parts[1]
	}
	return ""
}

// diagnoseCapacity surfaces the per-repo pending FIFO depth and the
// daemon-recorded high watermark. Reads are best-effort: we do not abort
// diagnose when the table is empty or the meta key is unset (those are the
// "fresh repo" defaults).
func diagnoseCapacity(ctx context.Context, conn *sql.DB, report *diagnoseReport) error {
	var depth int
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStatePending).Scan(&depth); err != nil {
		return fmt.Errorf("pending depth: %w", err)
	}
	report.PendingDepth = depth

	v, ok, err := metaLookup(ctx, conn, "capture.pending_high_water")
	if err != nil {
		return fmt.Errorf("pending_high_water: %w", err)
	}
	if ok && v != "" {
		if hw, perr := strconv.ParseInt(v, 10, 64); perr == nil {
			report.PendingHighWater = hw
		}
	}
	return nil
}

func diagnoseBlocked(ctx context.Context, conn *sql.DB, report *diagnoseReport) error {
	lastMeta, err := loadLastReplayConflictMeta(ctx, conn)
	if err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx,
		`SELECT seq, branch_ref, branch_generation, operation, path, published_ts, error
		 FROM capture_events
		 WHERE state = ?
		 ORDER BY seq DESC`, state.EventStateBlockedConflict)
	if err != nil {
		return fmt.Errorf("blocked conflicts: %w", err)
	}
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var entry diagnoseBlockedEntry
		var published sql.NullFloat64
		var errMsg sql.NullString
		if err := rows.Scan(&entry.Seq, &entry.BranchRef, &entry.BranchGeneration,
			&entry.Operation, &entry.Path, &published, &errMsg); err != nil {
			return fmt.Errorf("scan blocked conflict: %w", err)
		}
		if published.Valid {
			entry.PublishedTS = int64(published.Float64)
		}
		if errMsg.Valid {
			entry.Error = errMsg.String
		}
		entry.ErrorClass = classifyDiagnoseError(entry.Seq, entry.Error, lastMeta)
		counts[entry.ErrorClass]++
		if len(report.RecentBlocked) < 5 {
			report.RecentBlocked = append(report.RecentBlocked, entry)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iter blocked conflicts: %w", err)
	}

	report.BlockedHistogram = make([]diagnoseBlockedClass, 0, len(counts))
	for cls, count := range counts {
		report.BlockedHistogram = append(report.BlockedHistogram, diagnoseBlockedClass{
			ErrorClass: cls,
			Count:      count,
		})
	}
	sort.Slice(report.BlockedHistogram, func(i, j int) bool {
		if report.BlockedHistogram[i].Count != report.BlockedHistogram[j].Count {
			return report.BlockedHistogram[i].Count > report.BlockedHistogram[j].Count
		}
		return report.BlockedHistogram[i].ErrorClass < report.BlockedHistogram[j].ErrorClass
	})
	return nil
}

func loadLastReplayConflictMeta(ctx context.Context, conn *sql.DB) (replayConflictMeta, error) {
	var meta replayConflictMeta
	v, ok, err := metaLookup(ctx, conn, "last_replay_conflict")
	if err != nil || !ok || strings.TrimSpace(v) == "" {
		return meta, err
	}
	trimmed := strings.TrimSpace(v)
	if !strings.HasPrefix(trimmed, "{") {
		return meta, nil
	}
	if err := json.Unmarshal([]byte(trimmed), &meta); err != nil {
		return replayConflictMeta{}, fmt.Errorf("last_replay_conflict metadata: %w", err)
	}
	return meta, nil
}

func classifyDiagnoseError(seq int64, message string, last replayConflictMeta) string {
	if last.Seq == seq && last.ErrorClass != "" {
		return last.ErrorClass
	}
	var structured struct {
		ErrorClass string `json:"error_class"`
	}
	if err := json.Unmarshal([]byte(message), &structured); err == nil && structured.ErrorClass != "" {
		return structured.ErrorClass
	}

	switch {
	case strings.Contains(message, "update-ref CAS failed"):
		return "cas_fail"
	case strings.Contains(message, "before-state mismatch"),
		strings.Contains(message, "missing-in-index"),
		strings.Contains(message, "create conflict"),
		strings.Contains(message, "rename source"),
		strings.Contains(message, "rename target"):
		return "before_state_mismatch"
	case strings.Contains(message, "commit-tree"),
		strings.Contains(message, "write-tree"),
		strings.Contains(message, "update-index"):
		return "commit_build_failure"
	case strings.Contains(message, "branch ref mismatch"):
		return "ref_missing"
	case strings.TrimSpace(message) == "":
		return "unknown"
	default:
		return "validation"
	}
}

func diagnoseRemediation(report diagnoseReport) []string {
	var remediation []string
	if report.Anchor.Mismatch {
		remediation = append(remediation,
			"Current git HEAD branch differs from the daemon anchor; switch back to the daemon branch or restart acd on the current branch.")
	}
	if len(report.RecentBlocked) > 0 {
		remediation = append(remediation,
			"Resolve the listed paths in the worktree/index, then remove terminal blocked_conflict rows only after the predecessor is safe to discard.")
	}
	if report.PendingHighWater > 0 && report.PendingDepth > 0 {
		remediation = append(remediation,
			"capture pending depth is non-zero; if depth keeps climbing toward ACD_MAX_PENDING_EVENTS, run acd resume / acd recover to drain replay.")
	}
	if len(remediation) == 0 {
		remediation = append(remediation, "No anchor mismatch or blocked replay conflicts detected.")
	}
	return remediation
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

	fmt.Fprintf(out, "Capture queue: pending=%d high_water=%d\n", r.PendingDepth, r.PendingHighWater)

	fmt.Fprintln(out, "Blocked conflict histogram:")
	if len(r.BlockedHistogram) == 0 {
		fmt.Fprintln(out, "  none")
	} else {
		for _, bucket := range r.BlockedHistogram {
			fmt.Fprintf(out, "  - %s: %d\n", bucket.ErrorClass, bucket.Count)
		}
	}

	fmt.Fprintln(out, "Recent blocked entries:")
	if len(r.RecentBlocked) == 0 {
		fmt.Fprintln(out, "  none")
	} else {
		for _, entry := range r.RecentBlocked {
			fmt.Fprintf(out, "  - seq %d %s %s (%s)", entry.Seq, entry.Operation, entry.Path, entry.ErrorClass)
			if entry.Error != "" {
				fmt.Fprintf(out, ": %s", entry.Error)
			}
			fmt.Fprintln(out)
		}
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

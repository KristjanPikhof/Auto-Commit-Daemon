package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// Historical fixture serialization keeps schema and projection assertions
// separate from product presentation. These adapters call buildStatusReport
// and summarizeRepo, the read-only services used by current product commands.
// Current command/output contracts are exercised in product_* and control tests.

func pauseStatusNote(info *pauseInfo) string {
	if info == nil {
		return ""
	}
	switch info.Source {
	case "manual":
		return "manual"
	case "manual_expired":
		return "manual pause expired (marker still on disk; run acd resume --yes to remove)"
	case "rewind_grace":
		if info.ExpiresAt != "" {
			return "rewind grace, expires in " + formatDurationCompact(time.Duration(info.RemainingSeconds)*time.Second)
		}
		return "rewind grace"
	}
	return strings.ReplaceAll(info.Source, "_", " ")
}

// listRowMissing reports rows without readable state.db summary data.
func listRowMissing(status string) bool {
	return status == "missing" || status == "unreadable"
}

// listLastCommitShort renders the HEAD/LAST_COMMIT column (7-char oid prefix).
func listLastCommitShort(oid string) string {
	if oid == "" {
		return "-"
	}
	if len(oid) > 7 {
		return oid[:7]
	}
	return oid
}

// listStatusCompact maps list status strings to short dashboard tokens.
func listStatusCompact(status string) string {
	switch status {
	case "OK":
		return "OK"
	case "waiting":
		return "wait"
	case "blocked":
		return "blk"
	case "paused":
		return "pause"
	case "missing":
		return "miss"
	case "unreadable":
		return "bad"
	case "stale":
		return "stale"
	default:
		return status
	}
}

// listRepoLabelCompact returns the REPO column label for compact list output.
func listRepoLabelCompact(path string, labels map[string]string) string {
	if label, ok := labels[path]; ok {
		return label
	}
	return pathTwoSegmentLabel(path)
}

// buildListRepoLabelsCompact assigns a compact REPO column label per path. When
// two entries share the same two-segment label, each colliding row gets a "#"
// suffix with the last four characters of repo_hash.
func buildListRepoLabelsCompact(entries []listEntry) map[string]string {
	baseCounts := make(map[string]int)
	bases := make(map[string]string, len(entries))
	for _, e := range entries {
		base := pathTwoSegmentLabel(e.Path)
		bases[e.Path] = base
		baseCounts[base]++
	}
	labels := make(map[string]string, len(entries))
	for _, e := range entries {
		base := bases[e.Path]
		if baseCounts[base] > 1 {
			tail := e.RepoHash
			if len(tail) > 4 {
				tail = tail[len(tail)-4:]
			}
			labels[e.Path] = base + "#" + tail
			continue
		}
		labels[e.Path] = base
	}
	return labels
}

// pathTwoSegmentLabel returns the last two path segments joined with "/"
// (e.g. Development/Auto-Commit-Daemon). A single-segment path uses that name.
func pathTwoSegmentLabel(path string) string {
	clean := filepath.Clean(path)
	parts := strings.Split(clean, string(filepath.Separator))
	var segs []string
	for _, p := range parts {
		if p != "" {
			segs = append(segs, p)
		}
	}
	switch len(segs) {
	case 0:
		return "?"
	case 1:
		return segs[0]
	default:
		return segs[len(segs)-2] + "/" + segs[len(segs)-1]
	}
}

// joinParens2 renders ["a1b2c3d", "47s ago", "\"Update auth.py\""] as
// `a1b2c3d (47s ago, "Update auth.py")`.
func joinParens2(parts []string) string { return joinParens(parts) }

// joinParens renders ["running", "pid 123", "heartbeat 2s ago"] as
// "running (pid 123, heartbeat 2s ago)".
func joinParens(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return parts[0] + " (" + strings.Join(parts[1:], ", ") + ")"
}

func formatDecisionCounts(counts map[string]int) string {
	order := []string{
		state.DecisionKindProtected,
		state.DecisionKindHandledExternal,
		state.DecisionKindSupersededExternal,
		state.DecisionKindBlocked,
		state.DecisionKindCommitted,
		state.DecisionKindCaptured,
		state.DecisionKindSkipped,
		state.DecisionKindPaused,
		state.DecisionKindResumed,
		state.DecisionKindIntentDeferred,
		state.DecisionKindIntentForced,
		state.DecisionKindIntentPlannerError,
		state.DecisionKindMessageQualityRewrite,
		state.DecisionKindMessageQualityFallback,
	}
	seen := make(map[string]bool, len(counts))
	var parts []string
	for _, kind := range order {
		if n, ok := counts[kind]; ok {
			parts = append(parts, fmt.Sprintf("%s=%d", kind, n))
			seen[kind] = true
		}
	}
	for kind, n := range counts {
		if !seen[kind] {
			parts = append(parts, fmt.Sprintf("%s=%d", kind, n))
		}
	}
	return strings.Join(parts, " ")
}

func renderStatusProjectionFixture(out io.Writer, r statusReport) error {
	fmt.Fprintf(out, "Repo: %s\n", r.Repo)

	daemon := r.Daemon
	if r.Stale {
		daemon = "stale"
	}
	parts := []string{daemon}
	if r.PID > 0 {
		parts = append(parts, fmt.Sprintf("pid %d", r.PID))
	}
	if r.HeartbeatTS > 0 {
		parts = append(parts, fmt.Sprintf("heartbeat %s ago",
			formatDurationCompact(time.Duration(r.HeartbeatAgeSeconds)*time.Second)))
	}
	if r.UptimeSeconds > 0 {
		parts = append(parts, fmt.Sprintf("started %s ago",
			formatDurationCompact(time.Duration(r.UptimeSeconds)*time.Second)))
	}
	fmt.Fprintf(out, "Daemon: %s\n", joinParens(parts))
	renderRuntimeConfigHuman(out, r.RuntimeConfig)
	renderConfigReadinessHuman(out, r.Configuration)
	renderReplayObservabilityHuman(out, r.Replay)
	renderIntentV2Human(out, r.IntentV2)
	renderSelfPublicationHuman(out, r.SelfPublication, "")
	renderPublicationDrainHuman(out, r.PublicationDrain)
	renderProductPublicationProgress(out, r.PublicationProgress)
	fmt.Fprintf(out, "Operational state: %s", valueOrUnset(r.OperationalState))
	if r.Busy {
		fmt.Fprint(out, " (worker responsive; progress shown separately)")
	}
	fmt.Fprintln(out)
	fmt.Fprintf(out, "Worktree clean: %s\n", yesNo(r.WorktreeClean))
	fmt.Fprintf(out, "All changes committed in Git: %s\n",
		yesNo(r.AllChangesCommittedInGit))
	fmt.Fprintf(out, "All protected checkpoints resolved in Git: %s\n",
		yesNo(r.CheckpointPublishedByACD))

	fmt.Fprintf(out, "Clients (%d):\n", len(r.Clients))
	for _, c := range r.Clients {
		ageStr := formatDurationCompact(time.Duration(c.LastSeenAgeS) * time.Second)
		sid := c.SessionID
		if len(sid) > 8 {
			sid = sid[:4] + "..."
		}
		fmt.Fprintf(out, "  - %-12s session %s last_seen %s ago\n", c.Harness, sid, ageStr)
	}

	fmt.Fprintf(out, "Pending events: %d\n", r.PendingEvents)
	if r.BlockedConflicts > 0 {
		fmt.Fprintf(out, "Blocked conflicts: %d (inspect with `acd diagnose`; preview safe cleanup with `acd fix --dry-run`)\n", r.BlockedConflicts)
		if r.ActiveBarriers > 0 {
			fmt.Fprintf(out, "Blocked barriers with pending replay: %d (archive-only recovery preview: `acd fix --force --dry-run`)\n", r.ActiveBarriers)
		}
	}
	if r.FailedEvents > 0 {
		fmt.Fprintf(out, "Failed terminal events: %d\n", r.FailedEvents)
		if r.FailedBlockingPending > 0 {
			fmt.Fprintf(out, "Failed barriers blocking pending replay: %d (inspect with `acd diagnose`; preview cleanup with `acd fix --dry-run`)\n",
				r.FailedBlockingPending)
		}
	}
	if r.BackpressurePaused {
		stamp := r.BackpressurePausedAt
		if stamp == "" {
			stamp = "unset"
		}
		fmt.Fprintf(out, "Backpressure: paused since %s (events dropped lifetime: %d)\n",
			stamp, r.EventsDroppedTotal)
	} else if r.EventsDroppedTotal > 0 {
		fmt.Fprintf(out, "Capture dropped lifetime: %d\n", r.EventsDroppedTotal)
	}

	if r.LastCommitOID != "" {
		oid := r.LastCommitOID
		if len(oid) > 7 {
			oid = oid[:7]
		}
		bits := []string{oid}
		if r.LastCommitTS > 0 {
			age := time.Since(time.Unix(r.LastCommitTS, 0))
			bits = append(bits, formatDurationCompact(age)+" ago")
		}
		if r.LastCommitMessage != "" {
			bits = append(bits, fmt.Sprintf("%q", r.LastCommitMessage))
		}
		fmt.Fprintf(out, "Last commit: %s\n", joinParens2(bits))
	} else {
		fmt.Fprintln(out, "Last commit: none")
	}

	if r.CaptureErrors == 0 {
		fmt.Fprintln(out, "Capture errors: none")
	} else {
		fmt.Fprintf(out, "Capture errors: %d\n", r.CaptureErrors)
	}

	if r.Pause != nil {
		fmt.Fprintln(out, "Pause:")
		fmt.Fprintf(out, "  Source: %s\n", strings.ReplaceAll(r.Pause.Source, "_", " "))
		if r.Pause.Reason != "" {
			fmt.Fprintf(out, "  Reason: %s\n", r.Pause.Reason)
		}
		if r.Pause.SetAt != "" {
			fmt.Fprintf(out, "  Set at: %s\n", r.Pause.SetAt)
		}
		if r.Pause.ExpiresAt != "" {
			fmt.Fprintf(out, "  Expires at: %s (%s remaining)\n",
				r.Pause.ExpiresAt,
				formatDurationCompact(time.Duration(r.Pause.RemainingSeconds)*time.Second))
		}
	}

	renderIntentStrategyHuman(out, r.IntentStrategy)

	if len(r.DecisionCounts) > 0 {
		fmt.Fprintf(out, "Decisions: %s\n", formatDecisionCounts(r.DecisionCounts))
		if len(r.RecentDecisions) > 0 {
			fmt.Fprintln(out, "Recent decisions:")
			for _, ev := range r.RecentDecisions {
				fmt.Fprintf(out, "  - #%d %s", ev.ID, ev.Kind)
				if ev.Path != "" {
					fmt.Fprintf(out, " %s", ev.Path)
				}
				if ev.ActionTaken != "" {
					fmt.Fprintf(out, " (%s)", ev.ActionTaken)
				} else if ev.Reason != "" {
					fmt.Fprintf(out, " (%s)", ev.Reason)
				}
				if len(ev.GroupedSeqs) > 1 {
					fmt.Fprintf(out, " seqs=%s", formatSeqs(ev.GroupedSeqs))
				}
				fmt.Fprintln(out)
			}
		}
		fmt.Fprintln(out, "Explain: acd explain --path FILE; stream: acd events --watch")
	}

	if r.BranchGenToken != "" {
		fmt.Fprintf(out, "Branch generation: %s\n", r.BranchGenToken)
	}
	return nil
}

func writeStatusProjectionFixture(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rec, _, _, err := lookupRegisteredRepo("status", repo)
	if err != nil {
		return err
	}

	report, err := buildStatusReport(ctx, rec, time.Now())
	if err != nil {
		return fmt.Errorf("acd status: %w", err)
	}

	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	return renderStatusProjectionFixture(out, report)
}

func blockedListStatusNote(blockedConflicts, activeBarriers int) string {
	if activeBarriers > 0 {
		return fmt.Sprintf("blocked conflicts=%d, barriers=%d; run acd diagnose; preview with acd fix --dry-run", blockedConflicts, activeBarriers)
	}
	return fmt.Sprintf("blocked conflicts=%d; run acd diagnose; preview with acd fix --dry-run", blockedConflicts)
}

// dashIfMissing returns "-" when the row represents a missing/unreadable
// repo so the table reads "no data yet" without lying about zero rows.
func dashIfMissing(status, val string) string {
	if status == "missing" || status == "unreadable" {
		return "-"
	}
	return val
}

func renderListTableVerbose(out io.Writer, entries []listEntry) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "REPO\tDAEMON\tCLIENTS\tPENDING\tBLOCKED\tLAST_COMMIT\tSTATUS")
	for _, e := range entries {
		clients := dashIfMissing(e.Status, fmt.Sprintf("%d", e.Clients))
		pending := dashIfMissing(e.Status, fmt.Sprintf("%d", e.PendingEvents))
		blocked := dashIfMissing(e.Status, fmt.Sprintf("%d", e.BlockedConflicts))
		statusCol := e.Status
		if e.StatusNote != "" {
			statusCol = e.Status + " (" + e.StatusNote + ")"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			homeShort(e.Path), e.Daemon, clients, pending, blocked,
			listLastCommitShort(e.LastCommitOID), statusCol)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("acd list: flush: %w", err)
	}
	return nil
}

func renderListTableCompact(out io.Writer, entries []listEntry) error {
	labels := buildListRepoLabelsCompact(entries)
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "REPO\tDAEMON\tPEND\tBLK\tHEAD\tSTATUS")
	for _, e := range entries {
		repo := listRepoLabelCompact(e.Path, labels)
		statusCol := listStatusCompact(e.Status)
		if e.Status == "waiting" && e.IntentWaitSeconds > 0 {
			statusCol = statusCol + " " + formatDurationCompact(time.Duration(e.IntentWaitSeconds)*time.Second)
		}
		if listRowMissing(e.Status) {
			fmt.Fprintf(tw, "%s\t-\t\t\t\t%s\n", repo, statusCol)
			continue
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\n",
			repo,
			e.Daemon,
			e.PendingEvents,
			e.BlockedConflicts,
			listLastCommitShort(e.LastCommitOID),
			statusCol,
		)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("acd list: flush: %w", err)
	}
	return nil
}

func renderListTable(out io.Writer, entries []listEntry, verbose bool) error {
	if verbose {
		return renderListTableVerbose(out, entries)
	}
	return renderListTableCompact(out, entries)
}

func renderListJSON(out io.Writer, entries []listEntry) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		Repos []listEntry `json:"repos"`
	}{Repos: entries})
}

func collectListSnapshot(ctx context.Context, errOut io.Writer) (listSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	roots, err := paths.Resolve()
	if err != nil {
		return listSnapshot{}, fmt.Errorf("acd list: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return listSnapshot{}, fmt.Errorf("acd list: load registry: %w", err)
	}

	now := time.Now()
	entries := make([]listEntry, 0, len(reg.Repos))

	for _, rec := range reg.Repos {
		if rec.LifecycleDisabled() {
			continue
		}
		e := listEntry{
			Path:           rec.Path,
			RepoHash:       rec.RepoHash,
			LifecycleState: rec.LifecycleStateName(),
			Daemon:         "-",
			Status:         "OK",
		}

		// Repo dir missing — we still emit a row so the user sees what gc
		// would prune.
		if !fileExists(rec.Path) {
			e.Status = "missing"
			e.StatusNote = "repo missing"
			entries = append(entries, e)
			continue
		}

		// State DB missing or unreadable — log the skip and emit a row
		// flagged so gc can pick it up.
		if !fileExists(rec.StateDB) {
			fmt.Fprintf(errOut, "acd list: state.db missing for %s\n", rec.Path)
			e.Status = "missing"
			e.StatusNote = "state.db missing"
			entries = append(entries, e)
			continue
		}

		summary, err := summarizeRepo(ctx, rec.StateDB, now, clientTTLForRepo(rec.Path))
		if err != nil {
			fmt.Fprintf(errOut, "acd list: skip %s: %v\n", rec.Path, err)
			e.Status = "unreadable"
			e.StatusNote = err.Error()
			entries = append(entries, e)
			continue
		}
		e.Daemon = summary.daemon
		e.PID = summary.pid
		e.Clients = summary.clients
		e.LastSeq = summary.lastSeq
		e.LastCommitOID = summary.lastCommitOID
		e.HeartbeatAgeSecs = summary.heartbeatAge.Seconds()
		e.PendingEvents = summary.pendingEvents
		e.BlockedConflicts = summary.blockedConflicts
		e.ActiveBarriers = summary.activeBarriers
		e.IntentV2 = summary.intentV2
		if summary.intentV2.NeedsAttention != "" {
			e.Status = "needs_attention"
			e.StatusNote = summary.intentV2.NeedsAttention
		}
		if e.Status != "needs_attention" &&
			(summary.blockedConflicts > 0 || summary.activeBarriers > 0) {
			e.Status = "blocked"
			e.StatusNote = blockedListStatusNote(summary.blockedConflicts, summary.activeBarriers)
		} else if e.Status != "needs_attention" && summary.pendingEvents > 0 {
			e.Status = "waiting"
			e.StatusNote = "pending captures queued; no recovery blockers"
			if summary.intentWait != nil {
				e.IntentWaitSeconds = summary.intentWait.waitSeconds
				e.IntentVisiblePending = summary.intentWait.visiblePending
				e.IntentMinPending = summary.intentWait.minPending
				if summary.intentWait.reason == "skipped_due_intent_settle_window" {
					e.StatusNote = fmt.Sprintf("intent settle wait: pending=%d, trigger in %s",
						summary.intentWait.visiblePending,
						formatDurationCompact(time.Duration(summary.intentWait.waitSeconds)*time.Second))
				} else {
					e.StatusNote = fmt.Sprintf("intent batch wait: pending=%d/%d, trigger in %s",
						summary.intentWait.visiblePending,
						summary.intentWait.minPending,
						formatDurationCompact(time.Duration(summary.intentWait.waitSeconds)*time.Second))
				}
			}
		}
		if summary.pause != nil {
			e.Status = "paused"
			e.StatusNote = pauseStatusNote(summary.pause)
			e.Paused = true
			e.Pause = summary.pause
		}
		if summary.daemon == "stale" {
			e.StaleHeartbeat = true
			staleNote := "daemon stale " + formatDurationCompact(summary.heartbeatAge)
			if e.Status == "paused" {
				// Combined paused+stale presentation: keep Status="paused"
				// (operator intent wins) but append the stale-heartbeat fact
				// so a paused-but-dead daemon never silently disappears.
				if e.StatusNote == "" {
					e.StatusNote = staleNote
				} else {
					e.StatusNote = e.StatusNote + "; " + staleNote
				}
			} else {
				if summary.clients == 0 {
					// Inactive stale daemon with zero live clients is hidden
					// from the default list (gc is responsible for it). When
					// a pause marker is present we already kept the row above.
					continue
				}
				e.Status = "stale"
				e.StatusNote = "stale heartbeat (" + formatDurationCompact(summary.heartbeatAge) + ")"
			}
		}
		entries = append(entries, e)
	}

	return listSnapshot{
		UpdatedAt: now,
		Entries:   entries,
	}, nil
}

func writeListProjectionFixture(ctx context.Context, out, errOut io.Writer, jsonOut, verbose bool) error {
	snapshot, err := collectListSnapshot(ctx, errOut)
	if err != nil {
		return err
	}
	if jsonOut {
		return renderListJSON(out, snapshot.Entries)
	}
	return renderListTable(out, snapshot.Entries, verbose)
}

type listSnapshot struct {
	UpdatedAt time.Time
	Entries   []listEntry
}

// listEntry is one row in the `acd list` output. JSON marshal tags match
// the §7.7 example shape.
type listEntry struct {
	Path                 string         `json:"path"`
	RepoHash             string         `json:"repo_hash"`
	LifecycleState       string         `json:"lifecycle_state"`
	Daemon               string         `json:"daemon"`
	PID                  int            `json:"pid,omitempty"`
	Clients              int            `json:"clients"`
	LastSeq              int64          `json:"last_seq"`
	LastCommitOID        string         `json:"last_commit_oid,omitempty"`
	HeartbeatAgeSecs     float64        `json:"heartbeat_age_seconds,omitempty"`
	PendingEvents        int            `json:"pending_events"`
	BlockedConflicts     int            `json:"blocked_conflicts"`
	ActiveBarriers       int            `json:"active_barriers,omitempty"`
	IntentWaitSeconds    int64          `json:"intent_wait_seconds,omitempty"`
	IntentVisiblePending int            `json:"intent_visible_pending,omitempty"`
	IntentMinPending     int            `json:"intent_min_pending,omitempty"`
	Status               string         `json:"status"`
	StatusNote           string         `json:"status_note,omitempty"`
	Paused               bool           `json:"paused,omitempty"`
	StaleHeartbeat       bool           `json:"stale_heartbeat,omitempty"`
	Pause                *pauseInfo     `json:"pause,omitempty"`
	IntentV2             intentV2Report `json:"intent_v2"`
}

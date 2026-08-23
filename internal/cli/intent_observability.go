package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// pathQuiescenceStaleness is the maximum age of a
// daemon_meta.path_quiescence.updated_at snapshot before status stops
// applying the gated_count subtraction. 30s is comfortably beyond a
// healthy daemon's replay tick (10s default + slack); a value older
// than this almost certainly means the daemon is dead and the snapshot
// is no longer tracking the live pending queue.
const pathQuiescenceStaleness = 30 * time.Second

const (
	plannerHealthInvalidWarning = "persisted planner health metadata is invalid; planner health details were omitted"
	plannerHealthVersionWarning = "persisted planner health metadata uses an unsupported version; planner health details were omitted"
)

// pathQuiescenceSnapshotFresh returns true when the daemon is alive AND
// the path_quiescence.updated_at meta value is newer than
// pathQuiescenceStaleness. A missing or unparseable timestamp is treated
// as stale: we would rather under-report the gated subtraction than
// over-count when the daemon is dead and meta is frozen.
//
// Reads daemon_state directly via raw SQL (rather than
// state.LoadDaemonState) so this stays compatible with the *sql.DB
// handle the read-only observability paths use.
func pathQuiescenceSnapshotFresh(ctx context.Context, conn *sql.DB) bool {
	var pid sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		`SELECT pid FROM daemon_state WHERE id = 1`).Scan(&pid); err != nil {
		return false
	}
	if !pid.Valid || pid.Int64 <= 0 || !identity.Alive(int(pid.Int64)) {
		return false
	}
	v, ok, err := metaLookup(ctx, conn, "path_quiescence.updated_at")
	if err != nil || !ok {
		return false
	}
	ts, perr := time.Parse(time.RFC3339, strings.TrimSpace(v))
	if perr != nil {
		return false
	}
	if time.Since(ts) > pathQuiescenceStaleness {
		return false
	}
	return true
}

type intentStrategyReport struct {
	Strategy                          string                              `json:"strategy"`
	CommitFormat                      string                              `json:"commit_format"`
	Active                            bool                                `json:"active"`
	Window                            int                                 `json:"window,omitempty"`
	RecentCommits                     int                                 `json:"recent_commits,omitempty"`
	DeferLimit                        int                                 `json:"defer_limit,omitempty"`
	MinPending                        int                                 `json:"min_pending,omitempty"`
	SettleWindowSeconds               int64                               `json:"settle_window_seconds"`
	MaxPendingAgeSeconds              int64                               `json:"max_pending_age_seconds,omitempty"`
	IntentStageDiffCap                int                                 `json:"intent_stage_diff_cap,omitempty"`
	VisiblePendingEvents              int                                 `json:"visible_pending_events,omitempty"`
	OldestPendingEventSeq             int64                               `json:"oldest_pending_event_seq,omitempty"`
	OldestPendingPath                 string                              `json:"oldest_pending_path,omitempty"`
	OldestPendingAgeSeconds           int64                               `json:"oldest_pending_age_seconds,omitempty"`
	NewestPendingEventSeq             int64                               `json:"newest_pending_event_seq,omitempty"`
	NewestPendingAgeSeconds           int64                               `json:"newest_pending_age_seconds,omitempty"`
	AgeTriggerTS                      int64                               `json:"age_trigger_ts,omitempty"`
	AgeTriggerInSeconds               int64                               `json:"age_trigger_in_seconds,omitempty"`
	SettleTriggerTS                   int64                               `json:"settle_trigger_ts,omitempty"`
	SettleTriggerInSeconds            int64                               `json:"settle_trigger_in_seconds,omitempty"`
	BatchWaitActive                   bool                                `json:"batch_wait_active,omitempty"`
	BatchWaitReason                   string                              `json:"batch_wait_reason,omitempty"`
	DeferredEvents                    int                                 `json:"deferred_events,omitempty"`
	MaxDeferCount                     int                                 `json:"max_defer_count,omitempty"`
	ForcedAgingReady                  int                                 `json:"forced_aging_ready,omitempty"`
	LastDeferredEventSeq              int64                               `json:"last_deferred_event_seq,omitempty"`
	LastDeferredPath                  string                              `json:"last_deferred_path,omitempty"`
	LastDeferredReason                string                              `json:"last_deferred_reason,omitempty"`
	LastPlannerErrorEventSeq          int64                               `json:"last_planner_error_event_seq,omitempty"`
	LastPlannerErrorPath              string                              `json:"last_planner_error_path,omitempty"`
	LastPlannerError                  string                              `json:"last_planner_error,omitempty"`
	RejectLogPath                     string                              `json:"reject_log_path,omitempty"`
	MessageQualityRewriteCountRecent  int                                 `json:"message_quality_rewrite_count_recent,omitempty"`
	MessageQualityFallbackCountRecent int                                 `json:"message_quality_fallback_count_recent,omitempty"`
	LastMessageQualityEventSeq        int64                               `json:"last_message_quality_event_seq,omitempty"`
	LastMessageQualityPath            string                              `json:"last_message_quality_path,omitempty"`
	LastMessageQualityAction          string                              `json:"last_message_quality_action,omitempty"`
	LastMessageQualityReason          string                              `json:"last_message_quality_reason,omitempty"`
	PlannerHealth                     *daemon.IntentPlannerHealthSnapshot `json:"planner_health,omitempty"`
	PlannerHealthWarning              string                              `json:"planner_health_warning,omitempty"`
	PlanAttempt                       int                                 `json:"plan_attempt,omitempty"`
	PlanAttemptLimit                  int                                 `json:"plan_attempt_limit,omitempty"`
	UnresolvedCaptureCount            int                                 `json:"unresolved_capture_count,omitempty"`
	PreservedGroupCount               int                                 `json:"preserved_group_count,omitempty"`
	ResolutionMode                    string                              `json:"resolution_mode,omitempty"`
	PreflightState                    string                              `json:"preflight_state,omitempty"`
	PreflightFindingCodes             []string                            `json:"preflight_finding_codes,omitempty"`
	ProviderCallSkippedReason         string                              `json:"provider_call_skipped_reason,omitempty"`
	RecoveryReady                     bool                                `json:"recovery_ready,omitempty"`
	LastPlannerWindow                 *intentPlannerWindowSummary         `json:"last_planner_window,omitempty"`
	// PlannerErrorRateRecent is the share of intent_planner_error rows in
	// the most recent IntentRecentDecisionWindow decisions. The denominator
	// is always IntentRecentDecisionWindow (default 100) regardless of how
	// many decisions have actually been recorded — the rate moves smoothly
	// as the ledger fills rather than oscillating wildly during the first
	// few decisions.
	//
	// JSON encoding: the field uses `,omitempty` so a zero rate is
	// absent from the payload. Operators consuming this metric should
	// treat absent and 0.0 identically — both mean "no planner errors
	// observed in the most recent window". The decision_records table
	// existence check upstream (see loadIntentRecentRates) already gates
	// the field off when no decisions have ever been recorded, so the
	// "never observed" vs "observed, exactly zero" distinction is not
	// meaningful from the JSON shape and operators should not try to
	// infer it.
	PlannerErrorRateRecent float64 `json:"planner_error_rate_recent,omitempty"`
	// SingletonCommitRateRecent is the share of one-event commits in the
	// most recent IntentRecentCommitWindow distinct commit OIDs. The
	// denominator follows the same "fixed 100 even when not yet filled"
	// policy as PlannerErrorRateRecent.
	SingletonCommitRateRecent float64 `json:"singleton_commit_rate_recent,omitempty"`
	// PlannerErrorRateRecentWarn surfaces the intent_strategy threshold
	// breach to operators in the human renderer. Set to true whenever
	// PlannerErrorRateRecent exceeds IntentPlannerErrorRateWarnThreshold
	// (default 0.05) so the diagnose remediation hint and the status human
	// output stay in sync without re-deriving the threshold separately.
	PlannerErrorRateRecentWarn bool `json:"planner_error_rate_recent_warn,omitempty"`
	// PathQuiescenceGatedEvents records the most recent count of pending
	// capture events held back by the per-path quiescence gate (see
	// ACD_PATH_QUIESCENCE_SECONDS in CLAUDE.md). The daemon stamps this
	// value once per replay pass to daemon_meta; status reads it
	// best-effort and adjusts VisiblePendingEvents downward so the
	// reported count reflects the planner-visible window. Absent when
	// the daemon has never recorded a snapshot.
	PathQuiescenceGatedEvents int `json:"path_quiescence_gated_events,omitempty"`
}

// IntentRecentDecisionWindow is the fixed denominator for
// PlannerErrorRateRecent — the number of most-recent decision_records rows
// considered when computing the planner-error share. The value is fixed at
// 100 so the metric is comparable across repos and over time; raising it
// would smooth the rate further at the cost of taking longer to react to
// new planner regressions.
const IntentRecentDecisionWindow = 100

// IntentRecentCommitWindow is the fixed denominator for
// SingletonCommitRateRecent — the number of most-recent unique commit OIDs
// considered when computing the singleton (one-event) commit share. Mirrors
// IntentRecentDecisionWindow.
const IntentRecentCommitWindow = 100

// IntentPlannerErrorRateWarnThreshold is the planner-error rate above which
// the diagnose remediation surfaces a warning. 0.05 (5%) reflects the
// observed noise floor of healthy planner deployments under the Wave 2
// retry+normalize stack; sustained rates above this are an operator signal
// to inspect <gitDir>/acd/planner-rejects.jsonl.
//
// Warn gating: PlannerErrorRateRecentWarn is only set when the
// decision_records table holds at least IntentRecentDecisionWindow
// rows. Below the window a fresh ledger can trip the threshold simply
// because the dilution denominator and the row count match (5 errors
// out of 5 decisions = 0.05 = threshold), which is a noise signal, not
// an operator-actionable regression.
const IntentPlannerErrorRateWarnThreshold = 0.05

type runtimeExperimentReport struct {
	ID                  int64  `json:"id"`
	BaselineRevisionID  int64  `json:"baseline_revision"`
	CandidateRevisionID int64  `json:"candidate_revision"`
	WindowBudget        int    `json:"window_budget"`
	CompletedWindows    int    `json:"completed_windows"`
	ExpiresTS           int64  `json:"expires_ts,omitempty"`
	FailurePolicy       string `json:"failure_policy"`
	Status              string `json:"status"`
}

type runtimeConfigReport struct {
	SavedGeneration         uint64                   `json:"saved_generation"`
	DesiredRevisionID       int64                    `json:"desired_revision"`
	AppliedRevisionID       int64                    `json:"applied_revision"`
	LastKnownGoodRevisionID int64                    `json:"last_known_good_revision"`
	Profile                 string                   `json:"profile,omitempty"`
	ApplyState              string                   `json:"apply_state"`
	PendingAgeSeconds       int64                    `json:"pending_age_seconds,omitempty"`
	Failure                 string                   `json:"failure,omitempty"`
	ApplyBoundary           string                   `json:"apply_boundary"`
	Experiment              *runtimeExperimentReport `json:"experiment,omitempty"`
}

type intentV2Report struct {
	Available                bool   `json:"available"`
	SchemaVersion            int    `json:"schema_version"`
	MigrationState           string `json:"migration_state,omitempty"`
	ReplayState              string `json:"replay_state,omitempty"`
	NeedsAttention           string `json:"needs_attention,omitempty"`
	PresetID                 string `json:"preset_id,omitempty"`
	PresetVersion            int    `json:"preset_version,omitempty"`
	Customized               bool   `json:"customized,omitempty"`
	VerificationMode         string `json:"verification_mode,omitempty"`
	RepairEnabled            bool   `json:"repair_enabled,omitempty"`
	RepairHorizon            string `json:"repair_horizon,omitempty"`
	RepairMaxCommits         int    `json:"repair_max_commits,omitempty"`
	ConfiguredRetryOnInvalid int    `json:"configured_retry_on_invalid,omitempty"`
	EffectiveCorrectionMax   int    `json:"effective_correction_max,omitempty"`
	OpenCandidates           int    `json:"open_candidates,omitempty"`
	ReadyCandidates          int    `json:"ready_candidates,omitempty"`
	WaitingCandidates        int    `json:"waiting_candidates,omitempty"`
	BlockedCandidates        int    `json:"blocked_candidates,omitempty"`
	SoftPublishedCandidates  int    `json:"soft_published_candidates,omitempty"`
	VerificationAttention    int    `json:"verification_attention,omitempty"`
	RecoverableRepairs       int    `json:"recoverable_repairs,omitempty"`
	LastBoundaryEpoch        int64  `json:"last_boundary_epoch,omitempty"`
	LatestCandidateStatus    string `json:"latest_candidate_status,omitempty"`
	LatestPlannerProtocol    string `json:"latest_planner_protocol,omitempty"`
	LatestAtomicityStatus    string `json:"latest_atomicity_status,omitempty"`
	LatestAtomicitySummary   string `json:"latest_atomicity_summary,omitempty"`
	LatestVerificationStatus string `json:"latest_verification_status,omitempty"`
	LatestRepairStatus       string `json:"latest_repair_status,omitempty"`
	LatestRepairError        string `json:"latest_repair_error,omitempty"`
}

const selfPublicationHeartbeatBudget = 3 * time.Second

// selfPublicationReport is the shared read-only operator projection rendered
// by status, diagnose, and doctor. Phase describes the operational state while
// JournalPhase preserves the exact durable boundary when recovery is pending.
type selfPublicationReport struct {
	Available           bool   `json:"available"`
	SchemaVersion       int    `json:"schema_version"`
	Phase               string `json:"phase"`
	JournalPhase        string `json:"journal_phase,omitempty"`
	PublicationID       string `json:"publication_id,omitempty"`
	SourceHead          string `json:"source_head_short,omitempty"`
	TargetHead          string `json:"target_head_short,omitempty"`
	RecoverableCount    int    `json:"recoverable_count"`
	PreparedCount       int    `json:"prepared_count"`
	GitAppliedCount     int    `json:"git_applied_count"`
	CompletedCount      int    `json:"completed_count"`
	AbandonedCount      int    `json:"abandoned_count"`
	CanonicalWriters    int    `json:"canonical_writer_count"`
	DaemonAlive         bool   `json:"daemon_alive"`
	HeartbeatAgeSeconds int64  `json:"heartbeat_age_seconds,omitempty"`
	HeartbeatStale      bool   `json:"heartbeat_stale"`
	PendingWakes        int    `json:"pending_wakes"`
	AcknowledgedWakes   int    `json:"acknowledged_wakes"`
	NeedsAttention      string `json:"needs_attention,omitempty"`
	RemediationKind     string `json:"remediation_kind"`
	Remediation         string `json:"remediation,omitempty"`
}

// loadSelfPublicationReport keeps all reads migration-free. The journal
// inventory comes from state's dedicated read-only loader; liveness is
// enriched from the caller's already read-only connection.
func loadSelfPublicationReport(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	now time.Time,
	canonicalWriters int,
) (selfPublicationReport, error) {
	projection, err := state.LoadSelfPublicationStateReadOnly(ctx, dbPath)
	if err != nil {
		return selfPublicationReport{}, err
	}
	report := selfPublicationReport{
		Available:        projection.Available,
		SchemaVersion:    projection.SchemaVersion,
		Phase:            "unavailable",
		CanonicalWriters: canonicalWriters,
		RemediationKind:  "none",
		PreparedCount:    projection.Prepared,
		GitAppliedCount:  projection.GitApplied,
		CompletedCount:   projection.Completed,
		AbandonedCount:   projection.Abandoned,
		RecoverableCount: len(projection.Recoverable),
		NeedsAttention: sanitizeObservabilityText(
			projection.NeedsAttention),
	}
	if !projection.Available {
		return report, nil
	}

	report.Phase = "idle"
	var pid int
	var heartbeat sql.NullFloat64
	if err := conn.QueryRowContext(ctx,
		`SELECT pid, heartbeat_ts FROM daemon_state WHERE id=1`,
	).Scan(&pid, &heartbeat); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, fmt.Errorf("read self-publication liveness: %w", err)
	}
	report.DaemonAlive = pid > 0 && identity.Alive(pid)
	if heartbeat.Valid && heartbeat.Float64 > 0 {
		heartbeatAt := time.Unix(
			0, int64(heartbeat.Float64*float64(time.Second)))
		age := now.Sub(heartbeatAt)
		if age < 0 {
			age = 0
		}
		report.HeartbeatAgeSeconds = int64(age.Seconds())
		report.HeartbeatStale = report.DaemonAlive &&
			age > selfPublicationHeartbeatBudget
	}

	if exists, inspectErr := sqliteTableExists(ctx, conn, "flush_requests"); inspectErr != nil {
		return report, fmt.Errorf("inspect self-publication wakes: %w", inspectErr)
	} else if exists {
		if err := conn.QueryRowContext(ctx, `
SELECT COALESCE(SUM(status='pending'),0),
       COALESCE(SUM(status='acknowledged'),0)
FROM flush_requests WHERE command='wake'`).Scan(
			&report.PendingWakes, &report.AcknowledgedWakes); err != nil {
			return report, fmt.Errorf("read self-publication wakes: %w", err)
		}
	}

	if len(projection.Recoverable) > 0 {
		current := projection.Recoverable[0]
		report.JournalPhase = current.Phase
		report.PublicationID = shortOID(sanitizeObservabilityText(current.ID), 12)
		report.SourceHead = shortOID(
			sanitizeObservabilityText(current.SourceHead), 12)
		report.TargetHead = shortOID(
			sanitizeObservabilityText(current.TargetCommitOID), 12)
		report.Phase = "recoverable"
		if current.Phase == state.SelfPublicationPrepared &&
			report.DaemonAlive && !report.HeartbeatStale {
			report.Phase = "active"
		}
	} else if report.DaemonAlive && !report.HeartbeatStale {
		report.Phase = "active"
	}
	if projection.UnknownRecoverable != nil {
		unknown := projection.UnknownRecoverable
		report.JournalPhase = unknown.Phase
		report.PublicationID = shortOID(
			sanitizeObservabilityText(unknown.ID), 12)
		report.SourceHead = shortOID(
			sanitizeObservabilityText(unknown.SourceHead), 12)
		report.TargetHead = shortOID(
			sanitizeObservabilityText(unknown.TargetCommitOID), 12)
		report.NeedsAttention = fmt.Sprintf(
			"Automatic recovery is blocked: publication=%s has unknown completion semantics after a v18 upgrade",
			valueOrUnset(report.PublicationID))
	}

	switch {
	case report.CanonicalWriters > 1:
		report.Phase = "stale"
		report.RemediationKind = "stop_old_owner"
		report.Remediation = "Stop the older ACD daemon owner; the stable repository lock permits one canonical writer."
	case report.NeedsAttention != "":
		report.Phase = "needs_attention"
		report.RemediationKind = "needs_attention"
		report.Remediation = report.NeedsAttention
	case report.HeartbeatStale &&
		(report.PendingWakes > 0 || report.AcknowledgedWakes > 0):
		report.Phase = "stale"
		report.RemediationKind = "needs_attention"
		report.Remediation = "Pending wakes are not advancing; inspect `acd diagnose`, then stop the stale owner and start ACD again."
	case report.RecoverableCount > 0:
		report.RemediationKind = "automatic_recovery"
		report.Remediation = "Automatic recovery will inspect the durable publication on the next daemon start or loop boundary."
	}
	return report, nil
}

func renderSelfPublicationHuman(
	out io.Writer,
	report selfPublicationReport,
	prefix string,
) {
	if !report.Available {
		fmt.Fprintf(out, "%sSelf-publication: unavailable (schema v%d; requires v18+)\n",
			prefix, report.SchemaVersion)
		return
	}
	fmt.Fprintf(out,
		"%sSelf-publication: phase=%s journal=%s source=%s target=%s recoverable=%d writers=%d wakes=%d/%d heartbeat=%s\n",
		prefix,
		report.Phase,
		valueOrUnset(report.JournalPhase),
		valueOrUnset(report.SourceHead),
		valueOrUnset(report.TargetHead),
		report.RecoverableCount,
		report.CanonicalWriters,
		report.PendingWakes,
		report.AcknowledgedWakes,
		selfPublicationHeartbeatLabel(report),
	)
	if report.Remediation != "" {
		fmt.Fprintf(out, "%s  remediation (%s): %s\n",
			prefix, report.RemediationKind, report.Remediation)
	}
}

func selfPublicationHeartbeatLabel(report selfPublicationReport) string {
	if !report.DaemonAlive {
		return "stopped"
	}
	label := formatDurationCompact(
		time.Duration(report.HeartbeatAgeSeconds) * time.Second)
	if report.HeartbeatStale {
		return label + " stale"
	}
	return label
}

func loadIntentV2Report(ctx context.Context, conn *sql.DB) (intentV2Report, error) {
	var report intentV2Report
	if conn == nil {
		return report, nil
	}
	if err := conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&report.SchemaVersion); err != nil {
		return report, errors.New("read Intent v2 schema version failed")
	}
	if report.SchemaVersion < 15 {
		return report, nil
	}
	for _, table := range []string{
		"intent_candidates", "intent_candidate_events",
		"intent_capture_dependencies", "intent_activity_boundaries",
		"intent_repairs", "intent_repair_commits",
	} {
		exists, err := sqliteTableExists(ctx, conn, table)
		if err != nil {
			return report, errors.New("inspect Intent v2 tables failed")
		}
		if !exists {
			return report, nil
		}
	}
	report.Available = true
	if value, ok, _ := metaLookup(ctx, conn, "intent.v2.migration_state"); ok {
		report.MigrationState = sanitizeObservabilityText(value)
	}
	if value, ok, _ := metaLookup(ctx, conn, "intent.v2.needs_attention"); ok {
		report.NeedsAttention = sanitizeObservabilityText(value)
	}
	if value, ok, _ := metaLookup(ctx, conn,
		"intent.v2.cutover_required"); ok && parseIntentV2MetaBool(value) {
		report.NeedsAttention = "Intent v2 cutover is required; run acd config edit"
		report.ReplayState = "needs_attention"
	}
	if strings.HasPrefix(strings.ToLower(report.MigrationState),
		"needs_attention") && report.NeedsAttention == "" {
		report.NeedsAttention = "Intent v2 migration needs attention; run acd config edit"
	}
	if report.NeedsAttention != "" {
		report.ReplayState = "needs_attention"
	} else {
		report.ReplayState = "active"
	}

	var revisionID, desiredRevision, appliedRevision sql.NullInt64
	var runtimeFailure sql.NullString
	if exists, _ := sqliteTableExists(ctx, conn, "runtime_config_state"); exists {
		_ = conn.QueryRowContext(ctx, `
SELECT COALESCE(desired_revision_id, applied_revision_id,
                last_known_good_revision_id),
       desired_revision_id, applied_revision_id, last_error
FROM runtime_config_state WHERE id=1`).Scan(
			&revisionID, &desiredRevision, &appliedRevision, &runtimeFailure)
	}
	if revisionID.Valid {
		var snapshot string
		if err := conn.QueryRowContext(ctx,
			`SELECT snapshot_json FROM config_revisions WHERE id=?`,
			revisionID.Int64).Scan(&snapshot); err == nil {
			decodeIntentV2Snapshot(snapshot, &report)
		}
	}
	if report.PresetID == "" {
		if value, ok, _ := metaLookup(ctx, conn, "intent.v2.preset_id"); ok {
			report.PresetID = sanitizeObservabilityText(value)
		}
	}
	if report.PresetVersion == 0 {
		if value, ok, _ := metaLookup(ctx, conn, "intent.v2.preset_version"); ok {
			report.PresetVersion, _ = strconv.Atoi(strings.TrimSpace(value))
		}
	}
	if report.NeedsAttention == "" && runtimeFailure.Valid &&
		strings.TrimSpace(runtimeFailure.String) != "" {
		report.NeedsAttention = sanitizeObservabilityText(runtimeFailure.String)
		report.ReplayState = "needs_attention"
	} else if report.NeedsAttention == "" && desiredRevision.Valid &&
		(!appliedRevision.Valid ||
			desiredRevision.Int64 != appliedRevision.Int64) {
		report.ReplayState = "pending"
	}

	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*),
       COALESCE(SUM(status='ready'),0),
       COALESCE(SUM(status IN ('open','waiting')),0),
       COALESCE(SUM(status='blocked'),0),
       COALESCE(SUM(status='soft_published'),0),
       COALESCE(SUM(
           (status='blocked'
            OR verification_status IN
               ('failed','timed_out','needs_attention'))
           AND EXISTS (
               SELECT 1
               FROM intent_candidate_events pending_membership
               JOIN capture_events pending_event
                 ON pending_event.seq=pending_membership.event_seq
                AND pending_event.state='pending'
               WHERE pending_membership.candidate_id=intent_candidates.id
                 AND pending_membership.membership_state='active'
           )
       ),0)
FROM intent_candidates
WHERE status IN ('open','waiting','ready','soft_published','blocked')
  AND EXISTS (
      SELECT 1 FROM intent_candidate_events active_membership
      WHERE active_membership.candidate_id=intent_candidates.id
        AND active_membership.membership_state='active'
  )`).Scan(
		&report.OpenCandidates, &report.ReadyCandidates,
		&report.WaitingCandidates, &report.BlockedCandidates,
		&report.SoftPublishedCandidates, &report.VerificationAttention,
	); err != nil {
		return report, errors.New("read Intent v2 candidate summary failed")
	}
	var candidateStatus, protocol, atomicity, atomicitySummary, verificationStatus sql.NullString
	err := conn.QueryRowContext(ctx, `
SELECT status, planner_protocol, atomicity_status, atomicity_summary,
       verification_status
FROM intent_candidates
ORDER BY updated_ts DESC, id DESC LIMIT 1`).Scan(
		&candidateStatus, &protocol, &atomicity, &atomicitySummary,
		&verificationStatus)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, errors.New("read latest Intent v2 candidate failed")
	}
	report.LatestCandidateStatus = sanitizeObservabilityText(candidateStatus.String)
	report.LatestPlannerProtocol = sanitizeObservabilityText(protocol.String)
	report.LatestAtomicityStatus = sanitizeObservabilityText(atomicity.String)
	report.LatestAtomicitySummary = sanitizeObservabilityText(atomicitySummary.String)
	report.LatestVerificationStatus = sanitizeObservabilityText(verificationStatus.String)

	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_repairs
WHERE status IN ('prepared','git_applied')`).Scan(
		&report.RecoverableRepairs); err != nil {
		return report, errors.New("read Intent v2 repair summary failed")
	}
	var repairStatus, repairError sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT status, error FROM intent_repairs
ORDER BY updated_ts DESC, id DESC LIMIT 1`).Scan(
		&repairStatus, &repairError)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, errors.New("read latest Intent v2 repair failed")
	}
	report.LatestRepairStatus = sanitizeObservabilityText(repairStatus.String)
	report.LatestRepairError = sanitizeObservabilityText(repairError.String)
	if err := conn.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(epoch),0) FROM intent_activity_boundaries`).Scan(
		&report.LastBoundaryEpoch); err != nil {
		return report, errors.New("read Intent v2 boundary summary failed")
	}
	if replay, replayErr := loadReplayObservabilityReport(ctx, conn); replayErr == nil {
		switch replay.State {
		case "needs_attention":
			report.ReplayState = "needs_attention"
			if report.NeedsAttention == "" {
				report.NeedsAttention = "Durable replay publication block"
				if replay.LastError != "" {
					report.NeedsAttention += ": " + replay.LastError
				}
			}
		case "degraded":
			if report.ReplayState == "" || report.ReplayState == "active" {
				report.ReplayState = "degraded"
			}
		}
	}
	return report, nil
}

func parseIntentV2MetaBool(value string) bool {
	parsed, _ := strconv.ParseBool(strings.TrimSpace(value))
	return parsed
}

func decodeIntentV2Snapshot(snapshot string, report *intentV2Report) {
	if report == nil {
		return
	}
	var values map[string]json.RawMessage
	if json.Unmarshal([]byte(snapshot), &values) != nil {
		return
	}
	_ = json.Unmarshal(values["preset_id"], &report.PresetID)
	_ = json.Unmarshal(values["preset_version"], &report.PresetVersion)
	report.Customized = decodeIntentV2SnapshotBool(values["customized"])
	_ = json.Unmarshal(values[config.FieldIntentVerification],
		&report.VerificationMode)
	report.ConfiguredRetryOnInvalid = decodeIntentV2SnapshotInt(
		values[config.FieldIntentRetryOnInvalid])
	report.EffectiveCorrectionMax = report.ConfiguredRetryOnInvalid
	if report.EffectiveCorrectionMax > 2 {
		report.EffectiveCorrectionMax = 2
	}
	report.RepairEnabled = decodeIntentV2SnapshotBool(
		values[config.FieldIntentRepairEnabled])
	if report.RepairHorizon == "" {
		_ = json.Unmarshal(values[config.FieldIntentRepairHorizon],
			&report.RepairHorizon)
	}
	var maxCommits string
	if json.Unmarshal(values[config.FieldIntentRepairMaxCommits],
		&maxCommits) == nil {
		report.RepairMaxCommits, _ = strconv.Atoi(maxCommits)
	} else {
		_ = json.Unmarshal(values[config.FieldIntentRepairMaxCommits],
			&report.RepairMaxCommits)
	}
	report.PresetID = sanitizeObservabilityText(report.PresetID)
	report.VerificationMode = sanitizeObservabilityText(report.VerificationMode)
	report.RepairHorizon = sanitizeObservabilityText(report.RepairHorizon)
}

func decodeIntentV2SnapshotInt(raw json.RawMessage) int {
	var value int
	if json.Unmarshal(raw, &value) == nil {
		if value < 0 {
			return 0
		}
		return value
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return 0
	}
	value, _ = strconv.Atoi(strings.TrimSpace(text))
	if value < 0 {
		return 0
	}
	return value
}

func plannerRejectLogPath(stateDB string) string {
	if strings.TrimSpace(stateDB) == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(stateDB), ai.IntentRejectsFileName)
}

func decodeIntentV2SnapshotBool(raw json.RawMessage) bool {
	var direct bool
	if json.Unmarshal(raw, &direct) == nil {
		return direct
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		value, _ := strconv.ParseBool(strings.TrimSpace(text))
		return value
	}
	return false
}

func renderIntentV2Human(out io.Writer, report intentV2Report) {
	if !report.Available {
		fmt.Fprintf(out, "Intent v2: unavailable (schema v%d; read-only compatibility mode)\n",
			report.SchemaVersion)
		return
	}
	customized := ""
	if report.Customized {
		customized = " customized"
	}
	fmt.Fprintf(out,
		"Intent v2: %s migration=%s preset=%s@%d%s verification=%s correction=%d/%d repair=%t/%s/%d candidates=%d ready=%d waiting=%d blocked=%d soft=%d verification_attention=%d recoverable_repairs=%d\n",
		valueOrUnset(report.ReplayState), valueOrUnset(report.MigrationState),
		valueOrUnset(report.PresetID), report.PresetVersion, customized,
		valueOrUnset(report.VerificationMode),
		report.ConfiguredRetryOnInvalid, report.EffectiveCorrectionMax,
		report.RepairEnabled,
		valueOrUnset(report.RepairHorizon), report.RepairMaxCommits,
		report.OpenCandidates,
		report.ReadyCandidates, report.WaitingCandidates,
		report.BlockedCandidates, report.SoftPublishedCandidates,
		report.VerificationAttention,
		report.RecoverableRepairs)
	if report.NeedsAttention != "" {
		fmt.Fprintf(out, "  Replay attention: %s\n", report.NeedsAttention)
	}
	if report.LatestCandidateStatus != "" {
		fmt.Fprintf(out,
			"  Latest candidate: status=%s protocol=%s atomicity=%s verification=%s\n",
			report.LatestCandidateStatus,
			valueOrUnset(report.LatestPlannerProtocol),
			valueOrUnset(report.LatestAtomicityStatus),
			valueOrUnset(report.LatestVerificationStatus))
	}
	if report.LatestRepairStatus != "" {
		fmt.Fprintf(out, "  Latest repair: %s", report.LatestRepairStatus)
		if report.LatestRepairError != "" {
			fmt.Fprintf(out, " (%s)", report.LatestRepairError)
		}
		fmt.Fprintln(out)
	}
}

func loadRuntimeConfigReport(ctx context.Context, conn *sql.DB, repoHash string, now time.Time) (runtimeConfigReport, error) {
	report := runtimeConfigReport{ApplyState: "unset", ApplyBoundary: string(config.ApplyHot)}
	if roots, err := paths.Resolve(); err == nil {
		if doc, err := config.NewStore(roots).Load(); err == nil {
			report.SavedGeneration = doc.Generation
			if repo := doc.Settings.Repositories[repoHash]; repo.Profile != "" {
				report.Profile = sanitizeObservabilityText(repo.Profile)
			}
		}
	}
	if conn == nil {
		return report, nil
	}
	hasState, err := sqliteTableExists(ctx, conn, "runtime_config_state")
	if err != nil {
		return report, errors.New("runtime settings table check failed")
	}
	if !hasState {
		return report, nil
	}
	var desired, applied, knownGood sql.NullInt64
	var desiredTS sql.NullFloat64
	var failure sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT desired_revision_id, applied_revision_id, last_known_good_revision_id,
       desired_ts, last_error
FROM runtime_config_state WHERE id=1`).Scan(&desired, &applied, &knownGood, &desiredTS, &failure)
	if errors.Is(err, sql.ErrNoRows) {
		return report, nil
	}
	if err != nil {
		return report, errors.New("read runtime settings state failed")
	}
	if desired.Valid {
		report.DesiredRevisionID = desired.Int64
	}
	if applied.Valid {
		report.AppliedRevisionID = applied.Int64
	}
	if knownGood.Valid {
		report.LastKnownGoodRevisionID = knownGood.Int64
	}
	report.Failure = sanitizeObservabilityText(failure.String)
	switch {
	case desired.Valid && applied.Valid && desired.Int64 == applied.Int64 && report.Failure == "":
		report.ApplyState = "active"
	case report.Failure != "":
		report.ApplyState = "rejected"
	case desired.Valid:
		report.ApplyState = "pending"
	case applied.Valid:
		report.ApplyState = "active"
	}
	if desiredTS.Valid && report.ApplyState != "active" {
		pending := time.Unix(0, int64(desiredTS.Float64*float64(time.Second)))
		report.PendingAgeSeconds = int64(now.Sub(pending).Seconds())
		if report.PendingAgeSeconds < 0 {
			report.PendingAgeSeconds = 0
		}
	}
	if ok, _ := sqliteTableExists(ctx, conn, "config_revisions"); ok {
		revision := report.DesiredRevisionID
		if revision == 0 {
			revision = report.AppliedRevisionID
		}
		if revision > 0 {
			var profile sql.NullString
			if err := conn.QueryRowContext(ctx, `SELECT profile FROM config_revisions WHERE id=?`, revision).Scan(&profile); err == nil && profile.Valid {
				report.Profile = sanitizeObservabilityText(profile.String)
			}
		}
	}
	if ok, _ := sqliteTableExists(ctx, conn, "config_experiments"); ok {
		var exp runtimeExperimentReport
		var expires sql.NullFloat64
		err := conn.QueryRowContext(ctx, `
SELECT id, baseline_revision_id, candidate_revision_id, window_budget,
       completed_windows, expires_ts, failure_policy, status
FROM config_experiments ORDER BY id DESC LIMIT 1`).Scan(
			&exp.ID, &exp.BaselineRevisionID, &exp.CandidateRevisionID,
			&exp.WindowBudget, &exp.CompletedWindows, &expires,
			&exp.FailurePolicy, &exp.Status)
		if err == nil {
			if expires.Valid {
				exp.ExpiresTS = int64(expires.Float64)
			}
			exp.FailurePolicy = sanitizeObservabilityText(exp.FailurePolicy)
			exp.Status = sanitizeObservabilityText(exp.Status)
			report.Experiment = &exp
		} else if !errors.Is(err, sql.ErrNoRows) {
			return report, errors.New("read runtime experiment state failed")
		}
	}
	return report, nil
}

var (
	observabilityANSI     = regexp.MustCompile(`\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))`)
	observabilityUserInfo = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^/@\s]+@`)
	observabilitySecret   = regexp.MustCompile(`(?i)\b(api[_ -]?key|access[_ -]?token|token|password|credential)\s*[:=]\s*[^\s,;]+`)
	observabilityPayload  = regexp.MustCompile(`(?i)\b(prompt|repository[_ -]?diff|raw[_ -]?response|provider[_ -]?response)\s*[:=]\s*[^\n]+`)
)

func sanitizeObservabilityText(value string) string {
	value = observabilityANSI.ReplaceAllString(value, "")
	value = observabilityUserInfo.ReplaceAllString(value, `${1}[redacted]@`)
	value = observabilitySecret.ReplaceAllString(value, `[redacted secret]`)
	value = observabilityPayload.ReplaceAllString(value, `[redacted payload]`)
	value = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) && r != '\u007f' {
			return r
		}
		return -1
	}, value)
	if len(value) > 1024 {
		value = value[:1024]
	}
	return strings.TrimSpace(value)
}

func renderRuntimeConfigHuman(out io.Writer, report runtimeConfigReport) {
	fmt.Fprintf(out, "Runtime settings: %s desired=%s applied=%s known_good=%s profile=%s boundary=%s saved_generation=%d",
		valueOrUnset(report.ApplyState), revisionOrUnset(report.DesiredRevisionID),
		revisionOrUnset(report.AppliedRevisionID), revisionOrUnset(report.LastKnownGoodRevisionID),
		valueOrUnset(report.Profile), valueOrUnset(report.ApplyBoundary), report.SavedGeneration)
	if report.PendingAgeSeconds > 0 {
		fmt.Fprintf(out, " pending_age=%s", formatDurationCompact(time.Duration(report.PendingAgeSeconds)*time.Second))
	}
	fmt.Fprintln(out)
	if report.Failure != "" {
		fmt.Fprintf(out, "  Activation failure: %s\n", report.Failure)
	}
	if report.Experiment != nil {
		exp := report.Experiment
		fmt.Fprintf(out, "  Experiment #%d: %s windows=%d/%d baseline=%d candidate=%d policy=%s",
			exp.ID, valueOrUnset(exp.Status), exp.CompletedWindows, exp.WindowBudget,
			exp.BaselineRevisionID, exp.CandidateRevisionID, valueOrUnset(exp.FailurePolicy))
		if exp.ExpiresTS > 0 {
			fmt.Fprintf(out, " expires=%s", time.Unix(exp.ExpiresTS, 0).UTC().Format(time.RFC3339))
		}
		fmt.Fprintln(out)
	}
}

func revisionOrUnset(value int64) string {
	if value <= 0 {
		return "unset"
	}
	return strconv.FormatInt(value, 10)
}

func renderIntentStrategyHuman(out io.Writer, r intentStrategyReport) {
	status := "event"
	if r.Strategy != "" {
		status = r.Strategy
	}
	if r.Active {
		fmt.Fprintf(out, "Commit strategy: %s format=%s (window %d, min pending %d, settle %s, max age %s, recent commits %d, defer limit %d)\n",
			status, valueOrUnset(r.CommitFormat), r.Window, r.MinPending, formatDurationCompact(time.Duration(r.SettleWindowSeconds)*time.Second), formatDurationCompact(time.Duration(r.MaxPendingAgeSeconds)*time.Second), r.RecentCommits, r.DeferLimit)
	} else {
		fmt.Fprintf(out, "Commit strategy: %s format=%s\n", status, valueOrUnset(r.CommitFormat))
	}
	if r.BatchWaitActive {
		if r.BatchWaitReason == "skipped_due_intent_settle_window" {
			fmt.Fprintf(out, "Intent settle wait: pending=%d newest_age=%s settle=%s trigger_in=%s\n",
				r.VisiblePendingEvents,
				formatDurationCompact(time.Duration(r.NewestPendingAgeSeconds)*time.Second),
				formatDurationCompact(time.Duration(r.SettleWindowSeconds)*time.Second),
				formatDurationCompact(time.Duration(r.SettleTriggerInSeconds)*time.Second))
		} else {
			fmt.Fprintf(out, "Intent batch wait: pending=%d min_pending=%d oldest_age=%s max_age=%s trigger_in=%s\n",
				r.VisiblePendingEvents,
				r.MinPending,
				formatDurationCompact(time.Duration(r.OldestPendingAgeSeconds)*time.Second),
				formatDurationCompact(time.Duration(r.MaxPendingAgeSeconds)*time.Second),
				formatDurationCompact(time.Duration(r.AgeTriggerInSeconds)*time.Second))
		}
	}
	if r.DeferredEvents > 0 || r.ForcedAgingReady > 0 || r.LastPlannerError != "" {
		fmt.Fprintf(out, "Intent planner: deferred=%d max_defer=%d forced_ready=%d\n",
			r.DeferredEvents, r.MaxDeferCount, r.ForcedAgingReady)
		if r.LastDeferredReason != "" {
			fmt.Fprintf(out, "  Last defer: seq %d %s (%s)\n",
				r.LastDeferredEventSeq, valueOrUnset(r.LastDeferredPath), r.LastDeferredReason)
		}
		if r.LastPlannerError != "" {
			fmt.Fprintf(out, "  Last planner error: seq %d %s (%s)\n",
				r.LastPlannerErrorEventSeq, valueOrUnset(r.LastPlannerErrorPath), r.LastPlannerError)
			if r.RejectLogPath != "" {
				fmt.Fprintf(out, "  Planner rejects: %s\n", r.RejectLogPath)
			}
		}
	}
	if r.MessageQualityRewriteCountRecent > 0 || r.MessageQualityFallbackCountRecent > 0 || r.LastMessageQualityReason != "" {
		fmt.Fprintf(out, "Message quality: rewrites=%d fallbacks=%d\n",
			r.MessageQualityRewriteCountRecent, r.MessageQualityFallbackCountRecent)
		if r.LastMessageQualityReason != "" {
			fmt.Fprintf(out, "  Last message quality action: seq %d %s %s (%s)\n",
				r.LastMessageQualityEventSeq,
				valueOrUnset(r.LastMessageQualityPath),
				valueOrUnset(r.LastMessageQualityAction),
				r.LastMessageQualityReason)
		}
	}
	if r.PlannerHealth != nil {
		health := r.PlannerHealth
		fmt.Fprintf(out, "Intent planner health: %s failures=%d bypasses=%d",
			valueOrUnset(string(health.State)), health.ConsecutiveFailures, health.BypassCount)
		if health.NextProbeTS > 0 {
			fmt.Fprintf(out, " next_probe=%s", time.Unix(int64(health.NextProbeTS), 0).UTC().Format(time.RFC3339))
		}
		if health.LastFailureClass != "" {
			fmt.Fprintf(out, " last_failure_class=%s", health.LastFailureClass)
		}
		fmt.Fprintln(out)
		if health.LastError != "" {
			fmt.Fprintf(out, "  Last circuit failure: %s\n", health.LastError)
		}
	}
	if r.PlannerHealthWarning != "" {
		fmt.Fprintf(out, "Intent planner health warning: %s\n", r.PlannerHealthWarning)
	}
	if r.LastPlannerWindow != nil {
		win := r.LastPlannerWindow
		fmt.Fprintf(out, "Last planner window: #%d provider=%s offered=%s selected_groups=%d deferred=%s\n",
			win.ID,
			valueOrUnset(win.Provider),
			formatSeqs(win.OfferedSeqs),
			len(win.SelectedGroups),
			valueOrUnset(formatSeqs(win.DeferredSeqs)))
		if win.PlanAttemptLimit > 0 || win.ResolutionMode != "" {
			fmt.Fprintf(out, "  Plan resolution: %s attempts=%d/%d unresolved=%d preserved_groups=%d\n",
				valueOrUnset(win.ResolutionMode), win.PlanAttempt,
				win.PlanAttemptLimit, win.UnresolvedCaptureCount,
				win.PreservedGroupCount)
		}
		if win.PreflightState != "" {
			fmt.Fprintf(out, "  Plan preflight: %s findings=%s provider_call_skipped=%s\n",
				win.PreflightState, valueOrUnset(strings.Join(win.FindingCodes, ",")),
				valueOrUnset(win.ProviderCallSkipped))
		}
		if len(win.HiddenSeqs) > 0 {
			fmt.Fprintf(out, "  Hidden/coalesced seqs: %s\n", formatSeqs(win.HiddenSeqs))
		}
		if win.ValidationFailure != "" {
			fmt.Fprintf(out, "  Validation fallback: %s\n", win.ValidationFailure)
		}
	}
	if r.PlannerErrorRateRecent > 0 || r.SingletonCommitRateRecent > 0 {
		warn := ""
		if r.PlannerErrorRateRecentWarn {
			warn = " WARN above " + formatRate(IntentPlannerErrorRateWarnThreshold)
		}
		fmt.Fprintf(out, "Intent rates (last %d): planner_error=%s singleton_commit=%s%s\n",
			IntentRecentDecisionWindow,
			formatRate(r.PlannerErrorRateRecent),
			formatRate(r.SingletonCommitRateRecent),
			warn,
		)
	}
}

// formatRate renders a rate in a stable two-decimal form. Avoids the
// language-dependent default formatting of fmt.Sprintf("%v", float64) so
// the human renderer is locale-stable.
func formatRate(r float64) string {
	return strconv.FormatFloat(r, 'f', 3, 64)
}

// ResolveEffectiveCommitStrategy returns the commit strategy currently in
// effect for a repo. When conn is nil, the result reflects only env
// (ACD_COMMIT_STRATEGY) and the canonical default. When daemon_meta
// carries a *recognized* commit.strategy value, that overlay wins;
// unrecognized values are loud (slog.Warn) and the env-derived value is
// used so corrupt meta cannot silently override the operator's intent.
func ResolveEffectiveCommitStrategy(ctx context.Context, conn *sql.DB) (ai.CommitStrategy, error) {
	cfg := ai.LoadProviderConfigFromEnv()
	strategy := cfg.CommitStrategy
	if conn == nil {
		return strategy, nil
	}
	raw, ok, err := metaLookup(ctx, conn, "commit.strategy")
	if err != nil {
		return strategy, fmt.Errorf("commit.strategy: %w", err)
	}
	if !ok {
		return strategy, nil
	}
	trimmed := strings.TrimSpace(strings.ToLower(raw))
	switch trimmed {
	case "":
		return strategy, nil
	case string(ai.CommitStrategyEvent):
		return ai.CommitStrategyEvent, nil
	case string(ai.CommitStrategyIntent):
		return ai.CommitStrategyIntent, nil
	default:
		// Daemon meta carries a value but it is not one of the known
		// commit strategies. Silently demoting to the env-derived
		// value would hide daemon misconfiguration; log a warning and
		// preserve the existing fallback so callers don't crash.
		slog.Default().Warn(
			"daemon meta commit.strategy has unrecognized value; falling back to env-derived strategy",
			slog.String("commit.strategy", raw),
			slog.String("fallback", string(strategy)),
		)
		return strategy, nil
	}
}

func intentStrategyFromEnv() intentStrategyReport {
	cfg := ai.LoadProviderConfigFromEnv()
	return intentStrategyReport{
		Strategy:      string(cfg.CommitStrategy),
		CommitFormat:  string(cfg.CommitFormat),
		Active:        cfg.CommitStrategy == ai.CommitStrategyIntent,
		Window:        cfg.IntentWindow,
		RecentCommits: cfg.IntentRecentCommits,
		DeferLimit:    cfg.IntentDeferLimit,
		MinPending:    cfg.IntentMinPending,
		SettleWindowSeconds: int64(
			cfg.IntentSettleWindow / time.Second,
		),
		MaxPendingAgeSeconds: int64(
			cfg.IntentMaxPendingAge / time.Second,
		),
		IntentStageDiffCap: ai.IntentStageDiffCap,
	}
}

func loadIntentStrategyReport(ctx context.Context, conn *sql.DB) (intentStrategyReport, error) {
	report := intentStrategyFromEnv()
	if conn == nil {
		return report, nil
	}
	strategy, err := ResolveEffectiveCommitStrategy(ctx, conn)
	if err != nil {
		return report, err
	}
	report.Strategy = string(strategy)
	report.Active = strategy == ai.CommitStrategyIntent
	if v, ok, err := metaLookup(ctx, conn, "commit.format"); err != nil {
		return report, fmt.Errorf("commit.format: %w", err)
	} else if ok {
		report.CommitFormat = normalizeCommitFormatForReport(v, report.CommitFormat)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.window"); err != nil {
		return report, fmt.Errorf("intent.window: %w", err)
	} else if ok {
		report.Window = parseIntentMetaInt(v, report.Window)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.recent_commits"); err != nil {
		return report, fmt.Errorf("intent.recent_commits: %w", err)
	} else if ok {
		report.RecentCommits = parseIntentMetaInt(v, report.RecentCommits)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.defer_limit"); err != nil {
		return report, fmt.Errorf("intent.defer_limit: %w", err)
	} else if ok {
		report.DeferLimit = parseIntentMetaInt(v, report.DeferLimit)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.min_pending"); err != nil {
		return report, fmt.Errorf("intent.min_pending: %w", err)
	} else if ok {
		report.MinPending = parseIntentMetaInt(v, report.MinPending)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.settle_window"); err != nil {
		return report, fmt.Errorf("intent.settle_window: %w", err)
	} else if ok {
		report.SettleWindowSeconds = parseIntentMetaNonNegativeDurationSeconds(v, report.SettleWindowSeconds)
	}
	if v, ok, err := metaLookup(ctx, conn, "intent.max_pending_age"); err != nil {
		return report, fmt.Errorf("intent.max_pending_age: %w", err)
	} else if ok {
		report.MaxPendingAgeSeconds = parseIntentMetaDurationSeconds(v, report.MaxPendingAgeSeconds)
	}
	if plannerHealth, warning, err := loadIntentPlannerHealth(ctx, conn); err != nil {
		return report, err
	} else {
		report.PlannerHealth = plannerHealth
		report.PlannerHealthWarning = warning
		if plannerHealth != nil {
			report.RecoveryReady = plannerHealth.RecoveryReady
		}
	}
	if err := loadLastIntentPlannerError(ctx, conn, &report); err != nil {
		return report, err
	}
	if err := loadIntentMessageQualitySummary(ctx, conn, &report); err != nil {
		return report, err
	}
	if lastWindow, err := loadLastIntentPlannerWindowSQL(ctx, conn); err != nil {
		return report, err
	} else {
		report.LastPlannerWindow = lastWindow
		if lastWindow != nil {
			report.PlanAttempt = lastWindow.PlanAttempt
			report.PlanAttemptLimit = lastWindow.PlanAttemptLimit
			report.UnresolvedCaptureCount = lastWindow.UnresolvedCaptureCount
			report.PreservedGroupCount = lastWindow.PreservedGroupCount
			report.ResolutionMode = lastWindow.ResolutionMode
			report.PreflightState = lastWindow.PreflightState
			report.PreflightFindingCodes = append([]string(nil),
				lastWindow.FindingCodes...)
			report.ProviderCallSkippedReason = lastWindow.ProviderCallSkipped
		}
	}
	ok, err := sqliteTableExists(ctx, conn, "planner_state")
	if err != nil {
		return report, fmt.Errorf("planner_state table check: %w", err)
	}
	if !ok {
		if err := loadIntentBatchWait(ctx, conn, &report); err != nil {
			return report, err
		}
		return report, nil
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(MAX(ps.defer_count), 0)
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.defer_count > 0
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events barrier
      WHERE barrier.branch_ref = e.branch_ref
        AND barrier.branch_generation = e.branch_generation
        AND barrier.seq < e.seq
        AND barrier.state IN (?, ?)
  )`, state.EventStatePending, state.EventStateFailed, state.EventStateBlockedConflict).Scan(&report.DeferredEvents, &report.MaxDeferCount); err != nil {
		return report, fmt.Errorf("planner deferred summary: %w", err)
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.defer_count >= ?
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events barrier
      WHERE barrier.branch_ref = e.branch_ref
        AND barrier.branch_generation = e.branch_generation
        AND barrier.seq < e.seq
        AND barrier.state IN (?, ?)
  )`, state.EventStatePending, report.DeferLimit, state.EventStateFailed, state.EventStateBlockedConflict).Scan(&report.ForcedAgingReady); err != nil {
		return report, fmt.Errorf("planner forced-aging summary: %w", err)
	}
	var lastDeferredSeq sql.NullInt64
	var lastDeferredPath, lastDeferredReason sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT ps.event_seq, e.path, ps.last_defer_reason
FROM planner_state ps
JOIN capture_events e ON e.seq = ps.event_seq
WHERE e.state = ? AND ps.last_defer_reason IS NOT NULL AND ps.last_defer_reason != ''
  AND NOT EXISTS (
      SELECT 1
      FROM capture_events barrier
      WHERE barrier.branch_ref = e.branch_ref
        AND barrier.branch_generation = e.branch_generation
        AND barrier.seq < e.seq
        AND barrier.state IN (?, ?)
  )
ORDER BY ps.last_planned_ts DESC, ps.event_seq DESC
LIMIT 1`, state.EventStatePending, state.EventStateFailed, state.EventStateBlockedConflict).Scan(&lastDeferredSeq, &lastDeferredPath, &lastDeferredReason)
	if err != nil && err != sql.ErrNoRows {
		return report, fmt.Errorf("planner last defer: %w", err)
	}
	if lastDeferredSeq.Valid {
		report.LastDeferredEventSeq = lastDeferredSeq.Int64
	}
	if lastDeferredPath.Valid {
		report.LastDeferredPath = lastDeferredPath.String
	}
	if lastDeferredReason.Valid {
		report.LastDeferredReason = lastDeferredReason.String
	}
	if err := loadIntentBatchWait(ctx, conn, &report); err != nil {
		return report, err
	}
	if err := loadIntentRecentRates(ctx, conn, &report); err != nil {
		return report, err
	}

	return report, nil
}

func loadIntentPlannerHealth(ctx context.Context, conn *sql.DB) (*daemon.IntentPlannerHealthSnapshot, string, error) {
	raw, ok, err := metaLookup(ctx, conn, daemon.MetaKeyIntentPlannerHealth)
	if err != nil {
		return nil, "", fmt.Errorf("intent planner health: %w", err)
	}
	if !ok {
		return nil, "", nil
	}
	if strings.TrimSpace(raw) == "" {
		return nil, plannerHealthInvalidWarning, nil
	}
	health, err := daemon.DecodeIntentPlannerHealthSnapshot(raw)
	if errors.Is(err, daemon.ErrIntentPlannerHealthUnsupportedVersion) {
		return nil, plannerHealthVersionWarning, nil
	}
	if err != nil {
		return nil, plannerHealthInvalidWarning, nil
	}
	return &health, "", nil
}

func normalizeCommitFormatForReport(raw, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(ai.CommitFormatConventional):
		return string(ai.CommitFormatConventional)
	case "", string(ai.CommitFormatImperative):
		return string(ai.CommitFormatImperative)
	default:
		return fallback
	}
}

// loadIntentRecentRates populates PlannerErrorRateRecent and
// SingletonCommitRateRecent on report. Reads are best-effort: a missing
// decision_records table (fresh repo, never committed) leaves both fields at
// their zero value rather than aborting the report.
//
// Denominator policy: both rates use a fixed denominator
// (IntentRecentDecisionWindow / IntentRecentCommitWindow). When the ledger
// holds fewer rows than the window, the rate dilutes toward zero — this
// keeps the metric stable and comparable across repos at the cost of
// understating short-term spikes during the first 100 decisions.
func loadIntentRecentRates(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	ok, err := sqliteTableExists(ctx, conn, "decision_records")
	if err != nil {
		return fmt.Errorf("intent rate decision table check: %w", err)
	}
	if !ok {
		return nil
	}
	if err := loadIntentPlannerErrorRate(ctx, conn, report); err != nil {
		return err
	}
	if err := loadIntentSingletonCommitRate(ctx, conn, report); err != nil {
		return err
	}
	if report.PlannerErrorRateRecent > IntentPlannerErrorRateWarnThreshold {
		// Suppress the warn flag while the ledger is still filling toward
		// IntentRecentDecisionWindow. With a fixed denominator a small
		// number of early errors dilutes against the full window — but a
		// burst of 5 errors in the first 5 decisions also reaches the
		// 0.05 threshold and is indistinguishable from sustained noise.
		// Wait for a representative sample before surfacing the
		// remediation hint to operators.
		var total int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_records`).Scan(&total); err != nil {
			return fmt.Errorf("planner error rate full-window check: %w", err)
		}
		if total >= IntentRecentDecisionWindow {
			report.PlannerErrorRateRecentWarn = true
		}
	}
	return nil
}

// loadIntentPlannerErrorRate counts intent_planner_error rows in the most
// recent IntentRecentDecisionWindow decisions. Uses a window-bounded
// subquery so the planner-error count never re-scans the full ledger.
func loadIntentPlannerErrorRate(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	const q = `
SELECT COUNT(*)
FROM (
    SELECT id, kind
    FROM decision_records
    ORDER BY id DESC
    LIMIT ?
) recent
WHERE recent.kind = ?`
	var errs int
	if err := conn.QueryRowContext(ctx, q, IntentRecentDecisionWindow, state.DecisionKindIntentPlannerError).Scan(&errs); err != nil {
		return fmt.Errorf("planner error rate: %w", err)
	}
	report.PlannerErrorRateRecent = float64(errs) / float64(IntentRecentDecisionWindow)
	return nil
}

// loadIntentSingletonCommitRate counts singleton commits among the most
// recent IntentRecentCommitWindow distinct commit OIDs. A singleton commit
// is defined as a committed-decision commit_oid that maps to exactly one
// committed decision row — i.e. exactly one capture event landed in that
// commit.
//
// The query first windows the commit OID list to the recent IntentRecentCommitWindow,
// then GROUP BYs to count rows per OID and counts how many groups have
// exactly one row. The denominator is the fixed IntentRecentCommitWindow
// (not the actual count of recent commits) so the rate dilutes toward zero
// while the ledger fills, mirroring the planner-error rate policy.
func loadIntentSingletonCommitRate(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	const q = `
SELECT COUNT(*)
FROM (
    SELECT commit_oid
    FROM decision_records
    WHERE commit_oid IS NOT NULL
      AND commit_oid != ''
      AND kind = ?
      AND commit_oid IN (
          SELECT commit_oid
          FROM decision_records
          WHERE commit_oid IS NOT NULL
            AND commit_oid != ''
            AND kind = ?
          GROUP BY commit_oid
          ORDER BY MAX(id) DESC
          LIMIT ?
      )
    GROUP BY commit_oid
    HAVING COUNT(*) = 1
)`
	var singletons int
	if err := conn.QueryRowContext(ctx, q,
		state.DecisionKindCommitted,
		state.DecisionKindCommitted,
		IntentRecentCommitWindow,
	).Scan(&singletons); err != nil {
		return fmt.Errorf("singleton commit rate: %w", err)
	}
	report.SingletonCommitRateRecent = float64(singletons) / float64(IntentRecentCommitWindow)
	return nil
}

func loadLastIntentPlannerError(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	ok, err := sqliteTableExists(ctx, conn, "decision_records")
	if err != nil {
		return fmt.Errorf("decision table check: %w", err)
	}
	if !ok {
		return nil
	}
	var lastErrorSeq sql.NullInt64
	var lastErrorPath, lastError sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT event_seq, path, COALESCE(NULLIF(reason, ''), NULLIF(user_message, ''), NULLIF(action_taken, ''))
FROM decision_records
WHERE kind = ?
ORDER BY decision_ts DESC, id DESC
LIMIT 1`, state.DecisionKindIntentPlannerError).Scan(&lastErrorSeq, &lastErrorPath, &lastError)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("planner last error: %w", err)
	}
	if lastErrorSeq.Valid {
		report.LastPlannerErrorEventSeq = lastErrorSeq.Int64
	}
	if lastErrorPath.Valid {
		report.LastPlannerErrorPath = lastErrorPath.String
	}
	if lastError.Valid {
		report.LastPlannerError = ai.SanitizePlannerError(lastError.String)
	}
	return nil
}

func loadIntentMessageQualitySummary(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	ok, err := sqliteTableExists(ctx, conn, "decision_records")
	if err != nil {
		return fmt.Errorf("message quality decision table check: %w", err)
	}
	if !ok {
		return nil
	}
	const recentQ = `
SELECT
  COALESCE(SUM(CASE WHEN kind = ? THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN kind = ? THEN 1 ELSE 0 END), 0)
FROM (
    SELECT kind
    FROM decision_records
    ORDER BY id DESC
    LIMIT ?
) recent`
	if err := conn.QueryRowContext(ctx, recentQ,
		state.DecisionKindMessageQualityRewrite,
		state.DecisionKindMessageQualityFallback,
		IntentRecentDecisionWindow,
	).Scan(&report.MessageQualityRewriteCountRecent, &report.MessageQualityFallbackCountRecent); err != nil {
		return fmt.Errorf("message quality recent counts: %w", err)
	}

	var seq sql.NullInt64
	var path, action, reason sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT event_seq, path, action_taken, COALESCE(NULLIF(reason, ''), NULLIF(user_message, ''))
FROM decision_records
WHERE kind IN (?, ?)
ORDER BY id DESC
LIMIT 1`, state.DecisionKindMessageQualityRewrite, state.DecisionKindMessageQualityFallback).Scan(&seq, &path, &action, &reason)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("message quality latest: %w", err)
	}
	if seq.Valid {
		report.LastMessageQualityEventSeq = seq.Int64
	}
	if path.Valid {
		report.LastMessageQualityPath = path.String
	}
	if action.Valid {
		report.LastMessageQualityAction = action.String
	}
	if reason.Valid {
		report.LastMessageQualityReason = reason.String
	}
	return nil
}

func loadIntentBatchWait(ctx context.Context, conn *sql.DB, report *intentStrategyReport) error {
	ok, err := sqliteTableExists(ctx, conn, "capture_events")
	if err != nil {
		return fmt.Errorf("capture_events table check: %w", err)
	}
	if !ok {
		return nil
	}
	var oldestSeq sql.NullInt64
	var oldestPath sql.NullString
	var oldestCaptured sql.NullFloat64
	var newestSeq sql.NullInt64
	var newestCaptured sql.NullFloat64
	if err := conn.QueryRowContext(ctx, `
WITH barriers AS (
    SELECT branch_ref, branch_generation, MIN(seq) AS first_seq
    FROM capture_events
    WHERE state IN (?, ?)
    GROUP BY branch_ref, branch_generation
), visible_pending AS (
    SELECT e.seq, e.path, e.captured_ts
    FROM capture_events e
    LEFT JOIN barriers b
           ON b.branch_ref = e.branch_ref
          AND b.branch_generation = e.branch_generation
    WHERE e.state = ?
      AND (b.first_seq IS NULL OR e.seq < b.first_seq)
)
SELECT COUNT(*), MIN(seq), (
    SELECT path FROM visible_pending ORDER BY seq ASC LIMIT 1
), (
    SELECT captured_ts FROM visible_pending ORDER BY seq ASC LIMIT 1
), (
    SELECT seq FROM visible_pending ORDER BY seq DESC LIMIT 1
), (
    SELECT captured_ts FROM visible_pending ORDER BY seq DESC LIMIT 1
)
FROM visible_pending`, state.EventStateBlockedConflict, state.EventStateFailed, state.EventStatePending).Scan(
		&report.VisiblePendingEvents,
		&oldestSeq,
		&oldestPath,
		&oldestCaptured,
		&newestSeq,
		&newestCaptured,
	); err != nil {
		return fmt.Errorf("intent batch wait summary: %w", err)
	}
	if blockers, err := loadRecoveryBlockerCounts(ctx, conn, "", 0); err != nil {
		return fmt.Errorf("intent blocker counts: %w", err)
	} else {
		report.VisiblePendingEvents = blockers.PendingOnlyIntentDepth
	}
	if oldestSeq.Valid {
		report.OldestPendingEventSeq = oldestSeq.Int64
	}
	if oldestPath.Valid {
		report.OldestPendingPath = oldestPath.String
	}
	if newestSeq.Valid {
		report.NewestPendingEventSeq = newestSeq.Int64
	}
	// Path-quiescence aware reporting: when the daemon stamped a recent
	// gated-count snapshot we subtract it from VisiblePendingEvents so the
	// number reflects the planner-visible window, not the durable FIFO
	// depth. OldestPendingAgeSeconds is intentionally NOT adjusted —
	// quiescence does not change the persistence timestamp on the oldest
	// row, only when the planner is offered the captures behind it.
	//
	// Freshness gate: the gated_count value is only meaningful while the
	// daemon is actively running. A dead daemon leaves the last snapshot
	// frozen on disk; subtracting it from VisiblePendingEvents would
	// chronically under-count pending work because new captures land in
	// capture_events while no replay pass refreshes the gated counter.
	// We surface PathQuiescenceGatedEvents (raw value, for forensic
	// inspection) but skip the subtraction when:
	//   - daemon_state shows no live process (LoadDaemonState false or
	//     identity.Alive returns false), OR
	//   - the snapshot timestamp is older than pathQuiescenceStaleness
	//     (default 30s, comfortably beyond the typical replay tick).
	if v, ok, err := metaLookup(ctx, conn, "path_quiescence.gated_count"); err == nil && ok {
		if gated, perr := strconv.Atoi(strings.TrimSpace(v)); perr == nil && gated > 0 {
			report.PathQuiescenceGatedEvents = gated
			if pathQuiescenceSnapshotFresh(ctx, conn) {
				adjusted := report.VisiblePendingEvents - gated
				if adjusted < 0 {
					adjusted = 0
				}
				report.VisiblePendingEvents = adjusted
			}
		}
	}
	if !oldestCaptured.Valid || report.VisiblePendingEvents == 0 || report.MaxPendingAgeSeconds <= 0 {
		return nil
	}
	nowSec := float64(time.Now().UnixNano()) / 1e9
	ageSeconds := int64(nowSec - oldestCaptured.Float64)
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	report.OldestPendingAgeSeconds = ageSeconds
	report.AgeTriggerTS = int64(oldestCaptured.Float64) + report.MaxPendingAgeSeconds
	if remaining := report.AgeTriggerTS - int64(nowSec); remaining > 0 {
		report.AgeTriggerInSeconds = remaining
	}
	if newestCaptured.Valid {
		newestAgeSeconds := int64(nowSec - newestCaptured.Float64)
		if newestAgeSeconds < 0 {
			newestAgeSeconds = 0
		}
		report.NewestPendingAgeSeconds = newestAgeSeconds
		if report.SettleWindowSeconds > 0 {
			report.SettleTriggerTS = int64(newestCaptured.Float64) + report.SettleWindowSeconds
			if remaining := report.SettleTriggerTS - int64(nowSec); remaining > 0 {
				report.SettleTriggerInSeconds = remaining
			}
		}
	}
	if report.Active &&
		report.ForcedAgingReady == 0 &&
		report.VisiblePendingEvents < report.MinPending &&
		report.OldestPendingAgeSeconds < report.MaxPendingAgeSeconds {
		report.BatchWaitActive = true
		report.BatchWaitReason = "skipped_due_intent_batch_wait"
	} else if report.Active &&
		report.ForcedAgingReady == 0 &&
		report.VisiblePendingEvents >= report.MinPending &&
		report.SettleWindowSeconds > 0 &&
		report.OldestPendingAgeSeconds < report.MaxPendingAgeSeconds &&
		report.NewestPendingAgeSeconds < report.SettleWindowSeconds {
		report.BatchWaitActive = true
		report.BatchWaitReason = "skipped_due_intent_settle_window"
	}
	return nil
}

func parseIntentMetaInt(raw string, fallback int) int {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func parseIntentMetaDurationSeconds(raw string, fallback int64) int64 {
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return int64(d / time.Second)
}

func parseIntentMetaNonNegativeDurationSeconds(raw string, fallback int64) int64 {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "0" {
		return 0
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil || d < 0 {
		return fallback
	}
	return int64(d / time.Second)
}

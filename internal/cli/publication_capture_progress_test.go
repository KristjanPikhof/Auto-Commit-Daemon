package cli

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestStatusAndListKeepFrozenPublicationProgressDuringCapture(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, phase, listPhase, listStatus string
		circuit                            daemon.IntentPlannerCircuitState
		drainPhase                         string
		goalReview                         bool
		wantHealth                         string
	}{
		{"provider cooldown", "provider_wait", "provider-wait:5m", "waiting", daemon.IntentPlannerCircuitOpen, state.PublicationDrainSemantic, false, controlHealthWaiting},
		{"provider probe", "provider_call", "provider-call", "working", daemon.IntentPlannerCircuitHalfOpen, state.PublicationDrainSemantic, false, controlHealthPublishing},
		{"goal review", "goal_review_wait", "goal-review:5m", "waiting", daemon.IntentPlannerCircuitClosed, state.PublicationDrainSemantic, true, controlHealthWaiting},
		{"unmoving publication", "stalled", "stalled", "stalled", daemon.IntentPlannerCircuitClosed, state.PublicationDrainSemantic, false, controlHealthDegraded},
		{"unmoving checkpoint target", "stalled", "stalled", "stalled", daemon.IntentPlannerCircuitClosed, state.PublicationDrainCheckpointing, false, controlHealthDegraded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			repo, dbPath, db := makeRepoStateDB(t)
			now := time.Now().UTC().Truncate(time.Second)
			oldProgress := float64(now.Add(-4 * time.Hour).Unix())
			const branch = "refs/heads/main"
			const worktree = "0123456789abcdef"
			members := make([]checkpointMemberFixture, 42)
			for i := range members {
				members[i].State = state.EventStatePending
				if i < 21 {
					members[i] = checkpointMemberFixture{State: state.EventStatePublished, CommitOID: "published"}
				}
			}
			seqs := insertCompletedCheckpoint(t, db, "cp-frozen-publication", worktree, members)
			// Later captures are outside the frozen target. Their fresh scan
			// cannot credit the earlier publication with new progress.
			later := make([]checkpointMemberFixture, 11)
			for i := range later {
				later[i].State = state.EventStatePending
			}
			insertCaptureEvents(t, db, later)
			drain := state.PublicationDrain{
				ID: "drain-frozen-publication", CheckpointID: "cp-frozen-publication", WorktreeID: worktree,
				BranchRef: branch, BranchGeneration: 7, CommitStrategy: "intent", CommitFormat: "imperative",
				Provider: "openai-compat", ProviderFingerprint: testPlannerHealthFingerprint(), Phase: tc.drainPhase,
				TargetEventCount: 42, PublishedEventCount: 21, CreatedTS: oldProgress, UpdatedTS: oldProgress,
				LastProgressTS: oldProgress, EventSeqs: seqs,
			}
			if created, err := state.PreparePublicationDrain(ctx, db, drain); err != nil || !created {
				t.Fatalf("prepare frozen drain=(%t,%v)", created, err)
			}
			seedCurrentReplayPair(t, db, branch, 7)
			if err := state.SaveDaemonState(ctx, db, state.DaemonState{
				PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()),
			}); err != nil {
				t.Fatal(err)
			}
			if err := state.MetaSetMany(ctx, db, map[string]string{
				daemon.MetaKeyProtectionObservationEpoch: "2", daemon.MetaKeyProtectionCoveredEpoch: "1",
				daemon.MetaKeyProtectionComplete: "false", daemon.MetaKeyProtectionCheckpointID: drain.CheckpointID,
				daemon.MetaKeyProtectionFullPollTS: strconv.FormatInt(now.Add(-2*time.Second).Unix(), 10),
				"commit.strategy":                  "intent", "ai.provider": "openai-compat", "ai.timeout": "1m",
			}); err != nil {
				t.Fatal(err)
			}
			health := daemon.IntentPlannerHealthSnapshot{
				State: tc.circuit, ProviderFingerprint: testPlannerHealthFingerprint(),
				ConsecutiveFailures: 1, LastFailureClass: daemon.IntentPlannerFailureTransport,
				NextProbeTS: float64(now.Add(5 * time.Minute).Unix()),
			}
			if err := state.MetaSetJSON(ctx, db, daemon.MetaKeyIntentPlannerHealth, struct {
				Version int `json:"version"`
				daemon.IntentPlannerHealthSnapshot
			}{Version: 1, IntentPlannerHealthSnapshot: health}); err != nil {
				t.Fatal(err)
			}
			if tc.goalReview {
				run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{
					Fingerprint: testPlannerHealthFingerprint(), BranchRef: branch, BranchGeneration: 7, AttemptLimit: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				run.Completed, run.AttemptCount = true, 1
				run.UnresolvedSeqs = seqs[21:]
				run.ProgressState = sql.NullString{String: "waiting_semantic_retry", Valid: true}
				run.ResolutionMode = run.ProgressState
				if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
					t.Fatal(err)
				}
				if _, err := state.AppendIntentPlannerWindow(ctx, db, state.IntentPlannerWindow{
					PlannedTS: oldProgress, BranchRef: branch, BranchGeneration: 7,
					OfferedSeqs: seqs[21:], VisibleOriginalSeqs: seqs[21:],
					PlanFingerprint: sql.NullString{String: run.Fingerprint, Valid: true},
					ResolutionMode:  run.ResolutionMode, PlanAttempt: 1, PlanAttemptLimit: 1,
				}); err != nil {
					t.Fatal(err)
				}
				if err := state.MetaSetJSON(ctx, db, daemon.MetaKeyIntentSemanticRetry, daemon.IntentSemanticRetrySnapshot{
					Version: 1, BranchRef: branch, BranchGeneration: 7,
					EvidenceFingerprint: testPlannerHealthFingerprint(), PlanFingerprint: testPlannerHealthFingerprint(),
					RetryAtTS: float64(now.Add(5 * time.Minute).Unix()),
				}); err != nil {
					t.Fatal(err)
				}
			}
			record := central.RepoRecord{Path: repo, StateDB: dbPath, RepositoryID: "publication-capture-repository", WorktreeID: worktree}
			report, err := buildStatusReport(ctx, record, now)
			if err != nil {
				t.Fatal(err)
			}
			overview, err := readProductListRepo(ctx, record, now)
			if err != nil {
				t.Fatal(err)
			}
			for name, got := range map[string]statusReport{"status": report, "list": overview.report} {
				progress := got.PublicationProgress
				if got.Protected || got.PendingEvents != 32 || !progress.WorkerResponsive ||
					progress.Phase != tc.phase || progress.Origin != "commit_all" ||
					progress.LastProgressTS != oldProgress || progress.LastProgressAgeSeconds != 4*60*60 ||
					progress.TargetTotal != 42 || progress.TargetRemaining != 21 ||
					got.FullPollTS != float64(now.Add(-2*time.Second).Unix()) || got.PublicationOutcome.ReasonCode != tc.phase {
					t.Fatalf("%s credited new capture to frozen publication: protected=%t pending=%d progress=%+v",
						name, got.Protected, got.PendingEvents, progress)
				}
				result := controlResult{OK: true, Enabled: true}
				applyControlStatusWithDaemonAlive(&result, got, true)
				if result.Protected || result.Health != tc.wantHealth ||
					strings.Contains(result.Summary, "checkpointing") || strings.Contains(result.Summary, "Your work remains protected") {
					t.Fatalf("%s summary masked publication or incomplete new capture: %+v", name, result)
				}
			}
			entry := productListEntryFromOverview(record, supervisor.WorkerStatus{
				RepositoryID: record.RepositoryID, State: "running",
			}, overview, nil)
			if entry.Protected || entry.ActionRequired || productListTarget(entry) != "commit-all:21/42" ||
				productListPhase(entry) != tc.listPhase || productListStatus(entry) != tc.listStatus ||
				strings.Contains(entry.Summary, "checkpointing") {
				t.Fatalf("list hid frozen target wait/progress: entry=%+v phase/status=%s/%s", entry, productListPhase(entry), productListStatus(entry))
			}
			// Publication status must still report a stopped worker truthfully.
			report.Stale = true
			result := controlResult{OK: true, Enabled: true}
			applyControlStatusWithDaemonAlive(&result, report, false)
			if result.OK || result.Health != controlHealthNeedsAttention {
				t.Fatalf("wait status hid an unavailable worker: %+v", result)
			}
		})
	}
}

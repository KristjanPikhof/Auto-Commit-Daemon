package cli

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestBuildStatusReportProviderCircuitProjectionAgreesWithList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo, dbPath, db := makeRepoStateDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	const branch = "refs/heads/main"
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()),
		BranchRef:        sql.NullString{String: branch, Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetMany(ctx, db, map[string]string{
		daemon.MetaKeyProtectionObservationEpoch: "1", daemon.MetaKeyProtectionCoveredEpoch: "1",
		daemon.MetaKeyProtectionCheckpointID: "checkpoint", daemon.MetaKeyProtectionComplete: "true",
		"commit.strategy": "intent", "ai.provider": "openai-compat", "ai.model": "gpt-test",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef: branch, BranchGeneration: 1, BaseHead: "head", Operation: "modify",
		Path: "SpeechEngine.swift", Fidelity: "exact", CapturedTS: float64(now.Add(-time.Hour).Unix()),
	}, nil); err != nil {
		t.Fatal(err)
	}
	record := central.RepoRecord{Path: repo, StateDB: dbPath,
		RepositoryID: "repository-id", WorktreeID: "worktree-id"}
	for _, tc := range []struct {
		circuit                                          daemon.IntentPlannerCircuitState
		phase, operational, listPhase, listStatus, label string
		remaining                                        int64
	}{
		{daemon.IntentPlannerCircuitOpen, "provider_wait", "waiting", "provider-wait:5m", "waiting",
			"waiting for the Intent provider retry (5m remaining); file capture continues", 300},
		{daemon.IntentPlannerCircuitHalfOpen, "provider_call", "busy", "provider-call", "working",
			"waiting for the current Intent provider response", 0},
	} {
		t.Run(string(tc.circuit), func(t *testing.T) {
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
			report, err := buildStatusReport(ctx, record, now)
			if err != nil {
				t.Fatal(err)
			}
			overview, err := readProductListRepo(ctx, record, now)
			if err != nil {
				t.Fatal(err)
			}
			for builder, got := range map[string]statusReport{"status": report, "list": overview.report} {
				if got.OperationalState != tc.operational || got.PublicationProgress.Phase != tc.phase ||
					got.PublicationProgress.WaitRemainingSeconds != tc.remaining || !got.Protected ||
					got.PendingEvents != 1 ||
					!got.PublicationProgress.WorkerResponsive || got.PublicationProgress.NeedsAttention ||
					publicationProgressPhaseLabel(got.PublicationProgress) != tc.label {
					t.Fatalf("%s circuit projection disagrees: operational=%s progress=%+v protected=%t",
						builder, got.OperationalState, got.PublicationProgress, got.Protected)
				}
				wantRetry := float64(0)
				if tc.circuit == daemon.IntentPlannerCircuitOpen {
					wantRetry = health.NextProbeTS
				}
				if got.PublicationOutcome.RetryAt != wantRetry || got.PublicationProgress.RetryAtTS != wantRetry {
					t.Fatalf("%s outcome/progress retry timestamps=%v/%v want=%v", builder,
						got.PublicationOutcome.RetryAt, got.PublicationProgress.RetryAtTS, wantRetry)
				}
			}
			entry := productListEntryFromOverview(record, supervisor.WorkerStatus{
				RepositoryID: record.RepositoryID, State: "running",
			}, overview, nil)
			if productListPhase(entry) != tc.listPhase || productListStatus(entry) != tc.listStatus {
				t.Fatalf("list phase/status=%s/%s want=%s/%s", productListPhase(entry), productListStatus(entry),
					tc.listPhase, tc.listStatus)
			}
		})
	}
}

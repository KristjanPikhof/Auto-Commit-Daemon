package cli

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestProviderRetryStatusExplainsCaptureAndDueRetry(t *testing.T) {
	for _, tc := range []struct {
		name          string
		phase         string
		remaining     int64
		responsive    bool
		wantLabel     string
		wantListPhase string
		wantStatus    string
	}{
		{"cooldown", "provider_wait", 300, true, "waiting for the Intent provider retry (5m remaining); file capture continues", "provider-wait:5m", "waiting"},
		{"due", "provider_wait", 0, true, "AI provider retry is due; file capture continues", "provider-retry-due", "waiting"},
		{"worker unavailable", "provider_wait", 0, false, "AI provider retry is due", "provider-retry-due", "waiting"},
		{"provider active", "provider_call", 0, true, "waiting for the current Intent provider response", "provider-call", "working"},
		{"history reconstruction", "history_reconstruction", 0, true, "reconstructing verified goals on a new branch; file capture continues", "history-reconstruct", "working"},
		{"semantic cooldown", "goal_review_wait", 3600, true, "waiting to review unresolved Intent goals (1h remaining); file capture continues", "goal-review:1h", "waiting"},
		{"semantic review due", "goal_review_wait", 0, true, "Intent goal review retry is due; file capture continues", "goal-review-due", "waiting"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			progress := publicationProgressReport{
				Phase: tc.phase, WaitRemainingSeconds: tc.remaining,
				WorkerResponsive: tc.responsive,
			}
			committed := false
			var out bytes.Buffer
			err := renderProductEnvelope(&out, productEnvelope{
				State: productStateWaiting,
				Data: productStatusData{
					Enabled: true, Protected: true,
					PublicationOutcome:  publicationOutcome{BranchCommitted: &committed, WaitingChanges: 1, ReasonCode: tc.phase},
					PublicationProgress: progress,
				},
			}, false)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "Status: "+tc.wantLabel+"\n") ||
				!strings.Contains(out.String(), "Current changes saved: yes") {
				t.Fatalf("status hid provider retry/protection: %s", out.String())
			}
			if !tc.responsive && strings.Contains(out.String(), "file capture continues") {
				t.Fatalf("unavailable worker claimed ongoing capture: %s", out.String())
			}
			entry := productListEntry{PublicationProgress: progress}
			if phase, status := productListPhase(entry), productListStatus(entry); phase != tc.wantListPhase || status != tc.wantStatus {
				t.Fatalf("list phase/status=%s/%s want=%s/%s", phase, status, tc.wantListPhase, tc.wantStatus)
			}
		})
	}
}

func TestSemanticRetryStatusAndListAgreeOnDurableGoalWait(t *testing.T) {
	ctx := context.Background()
	repo, dbPath, db := makeRepoStateDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	branch := "refs/heads/main"
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
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef: branch, BranchGeneration: 1, BaseHead: "head", Operation: "modify",
		Path: "SpeechEngine.swift", Fidelity: "exact", CapturedTS: float64(now.Add(-2 * time.Hour).Unix()),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{
		Fingerprint: testPlannerHealthFingerprint(), BranchRef: branch, BranchGeneration: 1,
		AttemptLimit: 1, Provider: sql.NullString{String: "openai-compat", Valid: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	run.Completed, run.AttemptCount = true, 1
	run.UnresolvedSeqs = []int64{seq}
	run.ProgressState = sql.NullString{String: "waiting_semantic_retry", Valid: true}
	run.ResolutionMode = run.ProgressState
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	if _, err := state.AppendIntentPlannerWindow(ctx, db, state.IntentPlannerWindow{
		PlannedTS: float64(now.Add(-time.Hour).Unix()), BranchRef: branch, BranchGeneration: 1,
		OfferedSeqs: []int64{seq}, VisibleOriginalSeqs: []int64{seq},
		PlanFingerprint: sql.NullString{String: run.Fingerprint, Valid: true},
		ResolutionMode:  run.ResolutionMode, PlanAttempt: 1, PlanAttemptLimit: 1,
	}); err != nil {
		t.Fatal(err)
	}
	retry := daemon.IntentSemanticRetrySnapshot{
		Version: 1, BranchRef: branch, BranchGeneration: 1,
		EvidenceFingerprint: run.Fingerprint, PlanFingerprint: run.Fingerprint,
		RetryAtTS: float64(now.Add(time.Hour).Unix()),
	}
	if err := state.MetaSetJSON(ctx, db, daemon.MetaKeyIntentSemanticRetry, retry); err != nil {
		t.Fatal(err)
	}
	record := central.RepoRecord{Path: repo, StateDB: dbPath, RepositoryID: "repository-id", WorktreeID: "worktree-id"}
	for _, checkAt := range []time.Time{now, now.Add(time.Hour)} {
		if err := state.SaveDaemonState(ctx, db, state.DaemonState{
			PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(checkAt.Unix()),
			BranchRef:        sql.NullString{String: branch, Valid: true},
			BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
		}); err != nil {
			t.Fatal(err)
		}
		report, err := buildStatusReport(ctx, record, checkAt)
		if err != nil {
			t.Fatal(err)
		}
		overview, err := readProductListRepo(ctx, record, checkAt)
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range []statusReport{report, overview.report} {
			if got.PublicationProgress.Phase != "goal_review_wait" || got.PublicationOutcome.RetryAt != retry.RetryAtTS || got.PublicationProgress.NeedsAttention {
				t.Fatalf("protected review wait was hidden or became a stall: %+v outcome=%+v", got.PublicationProgress, got.PublicationOutcome)
			}
		}
		entry := productListEntryFromOverview(record, supervisor.WorkerStatus{RepositoryID: record.RepositoryID, State: "running"}, overview, nil)
		wantPhase := "goal-review:1h"
		if checkAt.After(now) {
			wantPhase = "goal-review-due"
		}
		if productListPhase(entry) != wantPhase || productListStatus(entry) != "waiting" {
			t.Fatalf("review row phase/status=%s/%s want=%s/waiting", productListPhase(entry), productListStatus(entry), wantPhase)
		}
	}
}

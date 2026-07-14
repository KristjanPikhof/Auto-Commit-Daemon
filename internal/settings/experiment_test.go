package settings

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestSettingsExperimentLifecycleIsBoundedAndIdempotent(t *testing.T) {
	var nudges atomic.Int32
	svc, _ := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: "deterministic", Success: true}, nil
	}, func(context.Context, state.DaemonState) error { nudges.Add(1); return nil })
	ctx := context.Background()
	baseline := insertRevision(t, svc, "baseline", 0)
	request, ok, err := state.RequestConfigActivation(ctx, svc.db, baseline.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("baseline request: ok=%v err=%v", ok, err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, svc.db, request.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(ctx, svc.db, request.ID, baseline.ID)
	draft := map[string]string{config.FieldModel: "candidate", config.FieldCommitStrategy: string(ai.CommitStrategyIntent)}
	validation, tested := testedDraft(t, svc, draft, nil)
	started, err := svc.StartExperiment(ctx, ExperimentRequest{Values: draft,
		TestedFingerprint: tested.Fingerprint, ExpectedGeneration: validation.SourceGeneration,
		ExpectedDesiredRevision: baseline.ID, WindowBudget: 10,
		ExpiresAt: time.Now().Add(time.Hour), FailurePolicy: ExperimentPolicyRevert})
	if err != nil {
		t.Fatal(err)
	}
	var desiredWithoutExperiment int
	if err := svc.db.ReadSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_config_state s
WHERE s.desired_revision_id=? AND NOT EXISTS (
  SELECT 1 FROM config_experiments e WHERE e.candidate_revision_id=s.desired_revision_id
)`, started.Candidate.RevisionID).Scan(&desiredWithoutExperiment); err != nil {
		t.Fatal(err)
	}
	if desiredWithoutExperiment != 0 || started.Experiment.ID == 0 {
		t.Fatalf("candidate became desired without durable experiment: %+v", started)
	}
	if started.Experiment.BaselineRevisionID != baseline.ID ||
		started.Experiment.CandidateRevisionID != started.Candidate.RevisionID ||
		started.Experiment.WindowBudget != 10 || started.Experiment.Status != state.ExperimentActive {
		t.Fatalf("started experiment = %+v", started)
	}
	progress, err := svc.ExperimentProgress(ctx, started.Experiment.ID)
	if err != nil || progress.CompletedWindows != 0 || progress.FailurePolicy != ExperimentPolicyRevert {
		t.Fatalf("progress=%+v err=%v", progress, err)
	}
	cancelled, err := svc.CancelExperiment(ctx, started.Experiment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Experiment.Status != state.ExperimentCancelled || !cancelled.Revert.Queued ||
		cancelled.Revert.RevisionID == baseline.ID || cancelled.Revert.SnapshotHash != baseline.SnapshotHash {
		t.Fatalf("cancelled experiment = %+v", cancelled)
	}
	again, err := svc.RevertExperiment(ctx, started.Experiment.ID)
	if err != nil || again.Revert.Queued || again.Revert.RevisionID != 0 {
		t.Fatalf("idempotent revert = %+v err=%v", again, err)
	}
	var revisions int
	if err := svc.db.SQL().QueryRow(`SELECT COUNT(*) FROM config_revisions`).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 3 {
		t.Fatalf("immutable revision count=%d, want baseline+candidate+revert", revisions)
	}
}

func TestSettingsExperimentRejectsEventModeWithoutCreatingState(t *testing.T) {
	svc, _ := testService(t, nil, nil, nil)
	ctx := context.Background()
	baseline := insertRevision(t, svc, "baseline", 0)
	request, ok, err := state.RequestConfigActivation(ctx, svc.db, baseline.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("baseline request: ok=%v err=%v", ok, err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, svc.db, request.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(ctx, svc.db, request.ID, baseline.ID)
	draft := map[string]string{config.FieldModel: "candidate"}
	validation, tested := testedDraft(t, svc, draft, nil)

	_, err = svc.StartExperiment(ctx, ExperimentRequest{Values: draft,
		TestedFingerprint: tested.Fingerprint, ExpectedGeneration: validation.SourceGeneration,
		ExpectedDesiredRevision: baseline.ID, WindowBudget: 10})
	if err == nil || !strings.Contains(err.Error(), "intent") {
		t.Fatalf("event-mode experiment error=%v", err)
	}
	var experiments int
	if err := svc.db.ReadSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM config_experiments`).Scan(&experiments); err != nil {
		t.Fatal(err)
	}
	if experiments != 0 {
		t.Fatalf("event-mode experiment created %d rows", experiments)
	}
}

func TestSettingsExperimentRejectsInvalidBoundsPolicyExpiryAndBaseline(t *testing.T) {
	svc, _ := testService(t, nil, nil, nil)
	ctx := context.Background()
	for name, request := range map[string]ExperimentRequest{
		"zero":   {WindowBudget: 0},
		"large":  {WindowBudget: MaxExperimentWindows + 1},
		"policy": {WindowBudget: 1, FailurePolicy: "include raw provider error"},
		"expiry": {WindowBudget: 1, ExpiresAt: time.Now().Add(-time.Minute)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.StartExperiment(ctx, request); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	_, err := svc.StartExperiment(ctx, ExperimentRequest{WindowBudget: 1})
	if err == nil || !strings.Contains(err.Error(), "baseline") {
		t.Fatalf("missing baseline error = %v", err)
	}
	var experiments int
	_ = svc.db.SQL().QueryRow(`SELECT COUNT(*) FROM config_experiments`).Scan(&experiments)
	if experiments != 0 {
		t.Fatalf("invalid requests created %d experiments", experiments)
	}
}

func TestSettingsExperimentErrorsAndComparisonAreSecretFree(t *testing.T) {
	svc, _ := testService(t, nil, nil, nil)
	if _, err := svc.StartExperiment(context.Background(), ExperimentRequest{
		WindowBudget: 1, FailurePolicy: "token=sk-visible\x1b[31m",
	}); err == nil || strings.Contains(err.Error(), "sk-visible") || strings.Contains(err.Error(), "\x1b") {
		t.Fatalf("unsafe policy error = %v", err)
	}
	comparison, err := svc.Compare(context.Background())
	if err != nil || !strings.Contains(comparison.Interpretation, "not causal") {
		t.Fatalf("comparison=%+v err=%v", comparison, err)
	}
}

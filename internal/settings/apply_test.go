package settings

import (
	"context"

	"database/sql"
	"encoding/json"
	"errors"

	"os"
	"reflect"
	"strings"

	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func testedDraft(t *testing.T, svc *Service, draft map[string]string, confirmations []ai.ConfirmationRequirement) (Validation, ProviderTestResult) {
	t.Helper()
	validation, err := svc.Validate(context.Background(), draft, confirmations)
	if err != nil {
		t.Fatal(err)
	}
	tested, err := svc.TestProvider(context.Background(), draft, confirmations)
	if err != nil {
		t.Fatal(err)
	}
	return validation, tested
}

func TestFinishRemovalApplyStoppedWithoutStartOrWake(t *testing.T) {
	svc, _ := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: "deterministic", Success: true}, nil
	})
	draft := map[string]string{config.FieldModel: "pinned-model", config.FieldTimeout: "7s"}
	validation, tested := testedDraft(t, svc, draft, nil)
	result, err := svc.Apply(context.Background(), ApplyRequest{Values: draft,
		TestedFingerprint: tested.Fingerprint, ExpectedGeneration: validation.SourceGeneration})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !result.Applied || result.RevisionID == 0 || result.RequestID == 0 {
		t.Fatalf("stopped apply = %+v", result)
	}
	runtimeState, _ := state.RuntimeConfigActivationState(context.Background(), svc.db)
	if runtimeState.DesiredRevisionID.Int64 != result.RevisionID || runtimeState.AppliedRevisionID.Int64 != result.RevisionID {
		t.Fatalf("applied state = %+v", runtimeState)
	}
	var flushes int
	if err := svc.db.SQL().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='flush_requests'`).Scan(&flushes); err != nil || flushes != 0 {
		t.Fatalf("settings apply enqueued wake work: count=%d err=%v", flushes, err)
	}
	rev, err := state.ConfigRevisionByID(context.Background(), svc.db, result.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rev.SnapshotJSON), &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload[config.FieldAPIKey]; ok {
		t.Fatal("revision persisted API key")
	}
	if _, ok := payload["capture.max_file_bytes"]; ok {
		t.Fatal("revision persisted restart-only field")
	}
	if got := string(payload[config.FieldTimeout]); got != `"7s"` {
		t.Fatalf("timeout JSON = %s", got)
	}
	if got := string(payload["confirmations"]); got != `[]` {
		t.Fatalf("confirmations JSON = %s", got)
	}
}

func TestFinishRemovalApplyRunningWithoutWake(t *testing.T) {
	svc, _ := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: "deterministic", Success: true}, nil
	},
	)
	if err := state.SaveDaemonState(context.Background(), svc.db, state.DaemonState{PID: os.Getpid(), Mode: "running"}); err != nil {
		t.Fatal(err)
	}
	draft := map[string]string{config.FieldModel: "running-model"}
	validation, tested := testedDraft(t, svc, draft, nil)
	result, err := svc.Apply(context.Background(), ApplyRequest{Values: draft,
		TestedFingerprint: tested.Fingerprint, ExpectedGeneration: validation.SourceGeneration})
	if err != nil || !result.Applied {
		t.Fatalf("running Apply = %+v err=%v", result, err)
	}
	var flushes int
	_ = svc.db.SQL().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='flush_requests'`).Scan(&flushes)
	if flushes != 0 {
		t.Fatalf("settings apply created %d flushes", flushes)
	}
}

func TestSettingsActionApplyRejectsStaleTestGenerationDesiredAndRestart(t *testing.T) {
	svc, _ := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: "deterministic", Success: true}, nil
	})
	draft := map[string]string{config.FieldModel: "one"}
	validation, tested := testedDraft(t, svc, draft, nil)
	changed := map[string]string{config.FieldModel: "two"}
	_, err := svc.Apply(context.Background(), ApplyRequest{Values: changed,
		TestedFingerprint: tested.Fingerprint, ExpectedGeneration: validation.SourceGeneration})
	if err == nil || !strings.Contains(err.Error(), "reviewed settings are stale") {
		t.Fatalf("stale fingerprint error = %v", err)
	}
	restart := map[string]string{"capture.max_file_bytes": "1234"}
	restartValidation, restartTest := testedDraft(t, svc, restart, nil)
	_, err = svc.Apply(context.Background(), ApplyRequest{Values: restart,
		TestedFingerprint: restartTest.Fingerprint, ExpectedGeneration: restartValidation.SourceGeneration})
	if err == nil || !strings.Contains(err.Error(), "restart required") {
		t.Fatalf("restart error = %v", err)
	}
	// Advance desired independently; the stale expected desired must lose.
	rev := insertRevision(t, svc, "external", 0)
	if _, ok, err := state.RequestConfigActivation(context.Background(), svc.db, rev.ID, sql.NullInt64{}); err != nil || !ok {
		t.Fatalf("external desired = %v %v", ok, err)
	}
	validation, tested = testedDraft(t, svc, draft, nil)
	_, err = svc.Apply(context.Background(), ApplyRequest{Values: draft,
		TestedFingerprint: tested.Fingerprint, ExpectedGeneration: validation.SourceGeneration,
		ExpectedDesiredRevision: 0})
	if err == nil || !strings.Contains(err.Error(), "desired revision changed") {
		t.Fatalf("stale desired error = %v", err)
	}
}

func TestSettingsActionApplyRejectsConcurrentSavedGeneration(t *testing.T) {
	svc, roots := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: "deterministic", Success: true}, nil
	})
	draft := map[string]string{config.FieldModel: "draft"}
	validation, tested := testedDraft(t, svc, draft, nil)
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"newer"`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.Apply(context.Background(), ApplyRequest{Values: draft,
		TestedFingerprint: tested.Fingerprint, ExpectedGeneration: validation.SourceGeneration})
	if err == nil || !strings.Contains(err.Error(), "stale saved generation") {
		t.Fatalf("concurrent generation error = %v", err)
	}
	var revisions int
	_ = svc.db.SQL().QueryRow(`SELECT COUNT(*) FROM config_revisions`).Scan(&revisions)
	if revisions != 0 {
		t.Fatalf("stale generation created %d revisions", revisions)
	}
}

func TestSettingsActionApplyRequiresTypedRiskConfirmations(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "hidden", true
		}
		return "", false
	}
	svc, _ := testService(t, lookup, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: "openai-compat", Success: true}, nil
	})
	draft := map[string]string{config.FieldCommitStrategy: "intent", config.FieldProvider: "openai-compat", config.FieldBaseURL: "https://gateway.example/v1",
		config.FieldModel: "model", config.FieldDiffEgress: "true"}
	confirmed := []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials, ai.ConfirmationDiffEgress}
	validation, tested := testedDraft(t, svc, draft, confirmed)
	_, err := svc.Apply(context.Background(), ApplyRequest{Values: draft,
		TestedFingerprint: tested.Fingerprint, ExpectedGeneration: validation.SourceGeneration,
		Confirmations: []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials}})
	var confirmationErr *ConfirmationRequiredError
	if !errors.As(err, &confirmationErr) || !reflect.DeepEqual(confirmationErr.Missing, []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress}) {
		t.Fatalf("Apply confirmation error = %v", err)
	}
}

func TestSettingsIntentApplyRequiresCommandConsent(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "hidden", true
		}
		return "", false
	}
	svc, _ := testService(t, lookup, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: "openai-compat", Success: true}, nil
	})
	draft := map[string]string{
		config.FieldCommitStrategy:          "intent",
		config.FieldDiffEgress:              "true",
		config.FieldProvider:                "openai-compat",
		config.FieldIntentVerification:      "fast",
		config.FieldVerificationFastCommand: "go test ./...",
	}
	all := []ai.ConfirmationRequirement{
		ai.ConfirmationDiffEgress,
		ai.ConfirmationVerificationCommand,
	}
	validation, tested := testedDraft(t, svc, draft, all)
	_, err := svc.Apply(context.Background(), ApplyRequest{
		Values: draft, TestedFingerprint: tested.Fingerprint,
		ExpectedGeneration: validation.SourceGeneration,
		Confirmations:      []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress},
	})
	var confirmationErr *ConfirmationRequiredError
	if !errors.As(err, &confirmationErr) || !reflect.DeepEqual(confirmationErr.Missing,
		[]ai.ConfirmationRequirement{
			ai.ConfirmationVerificationCommand,
		}) {
		t.Fatalf("Apply confirmation error = %v", err)
	}
}

func TestSettingsActionRevertCreatesNewKnownGoodRevision(t *testing.T) {
	svc, _ := testService(t, nil, nil)
	ctx := context.Background()
	baseline := insertRevision(t, svc, "baseline", 0)
	req, ok, err := state.RequestConfigActivation(ctx, svc.db, baseline.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatal(err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, svc.db, req.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(ctx, svc.db, req.ID, baseline.ID)
	result, err := svc.Revert(ctx, RevertRequest{ExpectedGeneration: 0, ExpectedDesiredRevision: baseline.ID})
	if err != nil {
		t.Fatal(err)
	}
	if result.RevisionID == baseline.ID || !result.Applied {
		t.Fatalf("Revert = %+v", result)
	}
	reverted, _ := state.ConfigRevisionByID(ctx, svc.db, result.RevisionID)
	if reverted.SnapshotHash != baseline.SnapshotHash {
		t.Fatalf("reverted hash=%s baseline=%s", reverted.SnapshotHash, baseline.SnapshotHash)
	}
}

func TestSettingsActionGlobalSaveNeverSignalsOrFansOut(t *testing.T) {
	svc, _ := testService(t, nil, nil)
	value := "global"
	if _, err := svc.Save(context.Background(), SaveRequest{Scope: ScopeGlobal,
		Values: map[string]*string{config.FieldModel: &value}, ExpectedGeneration: 0}); err != nil {
		t.Fatal(err)
	}
	var revisions, requests int
	_ = svc.db.SQL().QueryRow(`SELECT COUNT(*) FROM config_revisions`).Scan(&revisions)
	_ = svc.db.SQL().QueryRow(`SELECT COUNT(*) FROM config_activation_requests`).Scan(&requests)
	if revisions != 0 || requests != 0 {
		t.Fatalf("global save created runtime rows revisions=%d requests=%d", revisions, requests)
	}
}

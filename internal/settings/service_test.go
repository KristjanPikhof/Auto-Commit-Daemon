package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func testService(t *testing.T, lookup func(string) (string, bool), probe ProbeFunc, nudge NudgeFunc) (*Service, paths.Roots) {
	t.Helper()
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("git", "init", "-q", repo)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	cmd = exec.Command("git", "-C", repo, "symbolic-ref", "HEAD", "refs/heads/main")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("symbolic-ref: %v: %s", err, out)
	}
	base := t.TempDir()
	roots := paths.Roots{State: filepath.Join(base, "state"), Share: filepath.Join(base, "share"), Config: filepath.Join(base, "config")}
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	svc, err := NewService(context.Background(), Options{Roots: roots, RepoPath: repo,
		LookupEnv: lookup, Probe: probe, Nudge: nudge, Now: func() time.Time { return time.Unix(200, 0) }})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc, roots
}

func fieldByName(t *testing.T, snapshot Snapshot, name string) FieldSnapshot {
	t.Helper()
	for _, field := range snapshot.Fields {
		if field.Name == name {
			return field
		}
	}
	t.Fatalf("field %q missing", name)
	return FieldSnapshot{}
}

func TestSettingsSnapshotReadOnlySecretSafeAndScoped(t *testing.T) {
	lookup := func(name string) (string, bool) {
		switch name {
		case ai.EnvAPIKey:
			return "do-not-display", true
		case ai.EnvModel:
			return "environment-model", true
		default:
			return "", false
		}
	}
	svc, roots := testService(t, lookup, nil, nil)
	store := config.NewStore(roots)
	if err := store.Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"global-model"`)
		doc.Settings.Profiles["fast"] = config.Profile{Fields: config.Overrides{config.FieldIntentWindow: json.RawMessage(`"4"`)}}
		doc.Settings.Repositories[svc.repoHash] = config.RepositorySettings{Profile: "fast", Fields: config.Overrides{config.FieldCommitStrategy: json.RawMessage(`"intent"`)}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	beforeConfig, err := os.ReadFile(roots.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	var beforeChanges int
	if err := svc.db.SQL().QueryRow(`SELECT total_changes()`).Scan(&beforeChanges); err != nil {
		t.Fatal(err)
	}
	snapshot, err := svc.Snapshot(context.Background(), ScopeRepository, "")
	if err != nil {
		t.Fatal(err)
	}
	afterConfig, _ := os.ReadFile(roots.ConfigPath())
	var afterChanges int
	_ = svc.db.SQL().QueryRow(`SELECT total_changes()`).Scan(&afterChanges)
	if string(beforeConfig) != string(afterConfig) || beforeChanges != afterChanges {
		t.Fatalf("Snapshot mutated state: configEqual=%v totalChanges=%d->%d", string(beforeConfig) == string(afterConfig), beforeChanges, afterChanges)
	}
	if snapshot.SavedGeneration != 1 || snapshot.Profile != "fast" || snapshot.RepoHash == "" {
		t.Fatalf("snapshot identity = %+v", snapshot)
	}
	model := fieldByName(t, snapshot, config.FieldModel)
	if model.DraftValue != "global-model" || model.Source != config.SourceGlobal ||
		model.ShadowedEnvironment != "environment-model" || !model.EnvironmentSet {
		t.Fatalf("model source/shadow = %+v", model)
	}
	key := fieldByName(t, snapshot, config.FieldAPIKey)
	if key.DraftValue != "set" || key.ActiveValue != "" || strings.Contains(mustJSON(t, snapshot), "do-not-display") {
		t.Fatalf("secret field leaked = %+v", key)
	}
	if got := fieldByName(t, snapshot, "capture.max_file_bytes").Boundary; got != config.ApplyRestart {
		t.Fatalf("restart boundary = %q", got)
	}
	globalSnapshot, err := svc.Snapshot(context.Background(), ScopeGlobal, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := fieldByName(t, globalSnapshot, config.FieldCommitStrategy); got.Source == config.SourceRepository || got.DraftValue == "intent" {
		t.Fatalf("global snapshot leaked repository override: %+v", got)
	}
	profileSnapshot, err := svc.Snapshot(context.Background(), ScopeProfile, "fast")
	if err != nil {
		t.Fatal(err)
	}
	if got := fieldByName(t, profileSnapshot, config.FieldCommitStrategy); got.Source == config.SourceRepository || got.DraftValue == "intent" {
		t.Fatalf("profile snapshot leaked repository override: %+v", got)
	}
	if len(snapshot.Profiles) != 1 || snapshot.Profiles[0] != "fast" {
		t.Fatalf("profiles=%v", snapshot.Profiles)
	}
}

func TestSettingsSnapshotProjectsRuntimeAndExperiment(t *testing.T) {
	svc, _ := testService(t, nil, nil, nil)
	ctx := context.Background()
	baseline := insertRevision(t, svc, "baseline", 0)
	candidate := insertRevision(t, svc, "candidate", 0)
	req, ok, err := state.RequestConfigActivation(ctx, svc.db, baseline.ID, stateNullID(0))
	if err != nil || !ok {
		t.Fatalf("request: %v %v", ok, err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, svc.db, req.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(ctx, svc.db, req.ID, baseline.ID)
	pending, ok, _ := state.RequestConfigActivation(ctx, svc.db, candidate.ID, stateNullID(baseline.ID))
	if !ok {
		t.Fatal("candidate request lost")
	}
	_, _ = state.RejectConfigActivation(ctx, svc.db, pending.ID, candidate.ID, "bad\x1b[31m provider")
	exp, err := state.CreateConfigExperiment(ctx, svc.db, state.ConfigExperimentInput{
		BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID,
		WindowBudget: 10, FailurePolicy: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := svc.Snapshot(ctx, ScopeRepository, "")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.AppliedRevisionID != baseline.ID || snapshot.LastKnownGoodRevisionID != baseline.ID ||
		snapshot.DesiredRevisionID != candidate.ID || snapshot.DesiredRequestID != pending.ID ||
		snapshot.Experiment == nil || snapshot.Experiment.ID != exp.ID || snapshot.Experiment.WindowBudget != 10 {
		t.Fatalf("runtime projection = %+v", snapshot)
	}
	if strings.Contains(snapshot.PendingError, "\x1b") || !strings.Contains(snapshot.PendingError, "provider") {
		t.Fatalf("pending error not sanitized = %q", snapshot.PendingError)
	}
}

func TestSettingsActionSaveRoutesScopesAndRejectsStaleGeneration(t *testing.T) {
	svc, roots := testService(t, nil, nil, nil)
	model := "repo-model"
	result, err := svc.Save(context.Background(), SaveRequest{Scope: ScopeRepository,
		Values: map[string]*string{config.FieldModel: &model}, ExpectedGeneration: 0})
	if err != nil || result.Generation != 1 {
		t.Fatalf("Save repository = %+v %v", result, err)
	}
	global := "global-model"
	if _, err := svc.Save(context.Background(), SaveRequest{Scope: ScopeGlobal,
		Values: map[string]*string{config.FieldModel: &global}, ExpectedGeneration: 0}); !errors.Is(err, config.ErrStaleGeneration) {
		t.Fatalf("stale Save error = %v", err)
	}
	profileValue := "3"
	if _, err := svc.Save(context.Background(), SaveRequest{Scope: ScopeProfile, Profile: "fast",
		Values: map[string]*string{config.FieldIntentWindow: &profileValue}, ExpectedGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	if doc.Generation != 2 || string(doc.Settings.Repositories[svc.repoHash].Fields[config.FieldModel]) != `"repo-model"` ||
		string(doc.Settings.Profiles["fast"].Fields[config.FieldIntentWindow]) != `"3"` || len(doc.Settings.Global) != 0 {
		t.Fatalf("scoped document = %+v", doc.Settings)
	}
}

func TestSettingsActionProviderTestRequiresRisksAndInvalidatesFingerprint(t *testing.T) {
	var probes atomic.Int32
	lookup := func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "hidden-key", true
		}
		return "", false
	}
	probe := func(_ context.Context, cfg ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		probes.Add(1)
		if cfg.APIKey != "hidden-key" || cfg.Model != "model-a" {
			t.Fatalf("probe config missing env secret or model: API key set=%v model=%q", cfg.APIKey != "", cfg.Model)
		}
		return ai.ProviderProbeResult{Provider: "openai-compat", Success: true, Latency: time.Millisecond}, nil
	}
	svc, _ := testService(t, lookup, probe, nil)
	draft := map[string]string{config.FieldProvider: "openai-compat", config.FieldBaseURL: "https://example.test/v1", config.FieldModel: "model-a"}
	_, err := svc.TestProvider(context.Background(), draft, nil)
	var confirmationErr *ConfirmationRequiredError
	if !errors.As(err, &confirmationErr) || !reflect.DeepEqual(confirmationErr.Missing, []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials}) || probes.Load() != 0 {
		t.Fatalf("confirmation error=%v probes=%d", err, probes.Load())
	}
	result, err := svc.TestProvider(context.Background(), draft, []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials})
	if err != nil || !result.Success || result.Fingerprint == "" || probes.Load() != 1 {
		t.Fatalf("TestProvider = %+v err=%v probes=%d", result, err, probes.Load())
	}
	draft[config.FieldModel] = "model-b"
	validation, err := svc.Validate(context.Background(), draft, []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials})
	if err != nil || validation.Fingerprint == result.Fingerprint {
		t.Fatalf("provider edit did not invalidate fingerprint: %+v %v", validation, err)
	}
}

func TestSettingsActionProviderTestDoesNotRequireDiffEgressConfirmation(t *testing.T) {
	var probes atomic.Int32
	lookup := func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "hidden-key", true
		}
		return "", false
	}
	svc, _ := testService(t, lookup, func(_ context.Context, cfg ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		probes.Add(1)
		if !cfg.DiffEgress {
			t.Fatal("test config lost the activation diff-egress setting")
		}
		return ai.ProviderProbeResult{Provider: "openai-compat", Success: true}, nil
	}, nil)
	draft := map[string]string{
		config.FieldProvider: "openai-compat", config.FieldBaseURL: "https://example.test/v1",
		config.FieldModel: "model-a", config.FieldDiffEgress: "true",
	}
	confirmed := []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials}
	result, err := svc.TestProvider(context.Background(), draft, confirmed)
	if err != nil || !result.Success || probes.Load() != 1 {
		t.Fatalf("TestProvider = %+v err=%v probes=%d", result, err, probes.Load())
	}
	if !reflect.DeepEqual(result.Confirmations, confirmed) {
		t.Fatalf("test confirmations=%v want=%v", result.Confirmations, confirmed)
	}
	validation, err := svc.Validate(context.Background(), draft, confirmed)
	if err != nil || !reflect.DeepEqual(validation.Missing, []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress}) {
		t.Fatalf("activation validation=%+v err=%v", validation, err)
	}
	withDiff, err := svc.Validate(context.Background(), draft, []ai.ConfirmationRequirement{
		ai.ConfirmationEndpointCredentials, ai.ConfirmationDiffEgress,
	})
	if err != nil || validation.Fingerprint != withDiff.Fingerprint {
		t.Fatalf("consent changed fingerprint: without=%s with=%s err=%v", validation.Fingerprint, withDiff.Fingerprint, err)
	}
}

func TestSettingsActionProviderTestRedactsProbeError(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "hidden-key", true
		}
		return "", false
	}
	svc, _ := testService(t, lookup, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: "openai-compat"}, errors.New("hidden-key\x1b[2J upstream failure")
	}, nil)
	_, err := svc.TestProvider(context.Background(), map[string]string{
		config.FieldProvider: "openai-compat", config.FieldModel: "model",
	}, nil)
	if err == nil || strings.Contains(err.Error(), "hidden-key") || strings.Contains(err.Error(), "\x1b") {
		t.Fatalf("probe error leaked unsafe content: %v", err)
	}
}

func TestSettingsActionRejectsSecretAndControlPersistence(t *testing.T) {
	svc, _ := testService(t, nil, nil, nil)
	secret := "must-not-appear"
	_, err := svc.Save(context.Background(), SaveRequest{Scope: ScopeGlobal,
		Values: map[string]*string{config.FieldAPIKey: &secret}, ExpectedGeneration: 0})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("secret persistence error = %v", err)
	}
	unsafe := "model\x1b[2J"
	_, err = svc.Save(context.Background(), SaveRequest{Scope: ScopeGlobal,
		Values: map[string]*string{config.FieldModel: &unsafe}, ExpectedGeneration: 0})
	if err == nil || strings.Contains(err.Error(), "\x1b") {
		t.Fatalf("control persistence error = %v", err)
	}
}

func insertRevision(t *testing.T, svc *Service, model string, generation int64) state.ConfigRevision {
	t.Helper()
	body, err := revisionSnapshotJSON(map[string]string{config.FieldModel: model}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := state.InsertConfigRevision(context.Background(), svc.db, state.ConfigRevisionInput{
		Snapshot: body, Profile: "default", Scope: "repository", SourceGeneration: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rev
}

func stateNullID(id int64) (out sql.NullInt64) {
	if id > 0 {
		out.Int64, out.Valid = id, true
	}
	return out
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

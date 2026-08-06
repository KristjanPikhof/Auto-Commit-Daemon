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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
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

func testGlobalService(t *testing.T, lookup func(string) (string, bool), probe ProbeFunc) (*Service, paths.Roots) {
	t.Helper()
	base := t.TempDir()
	roots := paths.Roots{
		State:  filepath.Join(base, "state"),
		Share:  filepath.Join(base, "share"),
		Config: filepath.Join(base, "config"),
	}
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	svc, err := NewGlobalService(context.Background(), Options{
		Roots: roots, RepoPath: filepath.Join(base, "not-a-repository"),
		LookupEnv: lookup, Probe: probe,
		Now: func() time.Time { return time.Unix(200, 0) },
	})
	if err != nil {
		t.Fatalf("NewGlobalService: %v", err)
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

func TestAuthoringPreviewReportsProviderSources(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == ai.EnvProvider {
			return "openai-compat", true
		}
		return "", false
	}
	svc, roots := testService(t, lookup, nil, nil)
	store := config.NewStore(roots)
	if err := store.Update(func(doc *config.Document) error {
		doc.Settings.Repositories[svc.repoHash] = config.RepositorySettings{
			Fields: config.Overrides{
				config.FieldBaseURL: json.RawMessage(
					`"https://gateway.example/v1"`,
				),
				config.FieldModel: json.RawMessage(`"gateway-model"`),
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	preview, err := svc.AuthoringPreview()
	if err != nil {
		t.Fatal(err)
	}
	if preview.Sources[config.FieldProvider] != config.SourceEnvironment ||
		preview.Sources[config.FieldBaseURL] != config.SourceRepository ||
		preview.Sources[config.FieldModel] != config.SourceRepository {
		t.Fatalf("sources=%+v", preview.Sources)
	}
}

func TestGlobalServiceAuthorsWithoutRepository(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == ai.EnvProvider {
			return "openai-compat", true
		}
		return "", false
	}
	svc, roots := testGlobalService(t, lookup, nil)
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"global-model"`)
		doc.Settings.Repositories["unrelated"] = config.RepositorySettings{
			Fields: config.Overrides{config.FieldModel: json.RawMessage(`"repository-model"`)},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	preview, err := svc.AuthoringPreview()
	if err != nil {
		t.Fatal(err)
	}
	if preview.Generation != 1 ||
		preview.Values[config.FieldModel] != "global-model" ||
		preview.Sources[config.FieldModel] != config.SourceGlobal ||
		preview.Sources[config.FieldProvider] != config.SourceEnvironment ||
		preview.Sources[config.FieldBaseURL] != config.SourceDefault {
		t.Fatalf("global preview = %+v", preview)
	}
	value := "changed"
	if _, err := svc.Save(context.Background(), SaveRequest{
		Scope: ScopeRepository, Values: map[string]*string{config.FieldModel: &value},
		ExpectedGeneration: preview.Generation,
	}); err == nil || !strings.Contains(err.Error(), "repository runtime service required") {
		t.Fatalf("repository save error = %v", err)
	}
	if _, err := svc.Snapshot(context.Background(), ScopeGlobal, ""); err == nil ||
		!strings.Contains(err.Error(), "repository runtime service required") {
		t.Fatalf("runtime snapshot error = %v", err)
	}
	if _, err := svc.Apply(context.Background(), ApplyRequest{}); err == nil ||
		!strings.Contains(err.Error(), "repository runtime service required") {
		t.Fatalf("runtime apply error = %v", err)
	}
}

func TestGlobalServiceSavesFingerprintBoundSetupApproval(t *testing.T) {
	var probes atomic.Int32
	svc, roots := testGlobalService(t, nil, func(_ context.Context, cfg ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		probes.Add(1)
		if cfg.APIKey != "protected-key" || cfg.BaseURL != "https://gateway.example/v1" {
			t.Fatalf("probe config = %+v", cfg)
		}
		return ai.ProviderProbeResult{Provider: cfg.Mode, Success: true}, nil
	})
	if err := credentials.NewStore(roots).Set("protected-key"); err != nil {
		t.Fatal(err)
	}
	draft := map[string]string{
		config.FieldCommitStrategy:      "intent",
		config.FieldCommitPreset:        "balanced",
		config.FieldProvider:            "openai-compat",
		config.FieldBaseURL:             "https://gateway.example/v1",
		config.FieldModel:               "gateway-model",
		config.FieldDiffEgress:          "true",
		config.FieldIntentVerification:  "structural",
		config.FieldIntentRepairEnabled: "true",
	}
	confirmed := []ai.ConfirmationRequirement{
		ai.ConfirmationEndpointCredentials,
		ai.ConfirmationDiffEgress,
		ai.ConfirmationIntentRepair,
	}
	validation, err := svc.Validate(context.Background(), draft, confirmed)
	if err != nil || len(validation.Missing) != 0 {
		t.Fatalf("Validate = %+v err=%v", validation, err)
	}
	tested, err := svc.TestProvider(context.Background(), draft, confirmed)
	if err != nil || !tested.Success || probes.Load() != 1 {
		t.Fatalf("TestProvider = %+v err=%v probes=%d", tested, err, probes.Load())
	}
	result, err := svc.SaveGlobalSetup(context.Background(), SaveGlobalSetupRequest{
		Values: draft, TestedFingerprint: tested.Fingerprint,
		Confirmations: confirmed, ExpectedGeneration: validation.SourceGeneration,
	})
	if err != nil || result.Generation != 1 || result.Fingerprint != tested.Fingerprint {
		t.Fatalf("SaveGlobalSetup = %+v err=%v", result, err)
	}
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	approval, ok := config.ActiveGlobalSetupApproval(doc)
	if !ok || approval.Generation != 1 ||
		!reflect.DeepEqual(approval.Confirmations,
			[]string{"diff_egress", "endpoint_credentials", "intent_repair"}) {
		t.Fatalf("approval = %+v ok=%v", approval, ok)
	}
	if string(doc.Settings.Global[config.FieldModel]) != `"gateway-model"` ||
		string(doc.Settings.Global[config.FieldIntentVerification]) != `"structural"` {
		t.Fatalf("saved globals = %+v", doc.Settings.Global)
	}

	changed := "new-model"
	if _, err := svc.Save(context.Background(), SaveRequest{
		Scope: ScopeGlobal, Values: map[string]*string{config.FieldModel: &changed},
		ExpectedGeneration: result.Generation,
	}); err != nil {
		t.Fatal(err)
	}
	doc, err = config.NewStore(roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	approval, ok = config.ActiveGlobalSetupApproval(doc)
	if !ok {
		t.Fatal("later write removed auditable global approval")
	}
	preview, err := svc.AuthoringPreview()
	if err != nil {
		t.Fatal(err)
	}
	currentFingerprint, err := config.SettingsFingerprint(
		preview.Values, preview.Preset,
	)
	if err != nil {
		t.Fatal(err)
	}
	if currentFingerprint == approval.Fingerprint {
		t.Fatal("changed global settings still matched prior approval")
	}
}

func TestGlobalServiceReplaceDropsStaleOverrides(t *testing.T) {
	svc, roots := testGlobalService(t, nil, nil)
	timeout := "30s"
	maxBytes := "1048576"
	saved, err := svc.Save(context.Background(), SaveRequest{
		Scope: ScopeGlobal,
		Values: map[string]*string{
			config.FieldTimeout:      &timeout,
			"capture.max_file_bytes": &maxBytes,
		},
		ExpectedGeneration: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	preview, err := svc.AuthoringPreview()
	if err != nil {
		t.Fatal(err)
	}
	draft := preview.Values
	draft[config.FieldCommitStrategy] = "event"
	draft[config.FieldCommitPreset] = "fast"
	draft[config.FieldProvider] = "deterministic"
	draft[config.FieldIntentVerification] = "none"
	draft[config.FieldTimeout] = ai.DefaultProviderTimeout.String()
	validation, err := svc.Validate(context.Background(), draft, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.SaveGlobalSetup(
		context.Background(),
		SaveGlobalSetupRequest{
			Values:             draft,
			TestedFingerprint:  validation.Fingerprint,
			ExpectedGeneration: saved.Generation,
			Replace:            true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Settings.Global["capture.max_file_bytes"]; ok {
		t.Fatalf("replacement retained restart override: %+v",
			doc.Settings.Global)
	}
	if got := string(doc.Settings.Global[config.FieldTimeout]); got != `"`+ai.DefaultProviderTimeout.String()+`"` {
		t.Fatalf("replacement timeout = %s", got)
	}
	if result.Generation != 2 {
		t.Fatalf("replacement generation = %d", result.Generation)
	}
}

func TestGlobalServiceRejectsRepositoryVerificationApproval(t *testing.T) {
	svc, _ := testGlobalService(t, nil, nil)
	draft := map[string]string{
		config.FieldCommitStrategy:     "event",
		config.FieldCommitPreset:       "fast",
		config.FieldIntentVerification: "structural",
	}
	validation, err := svc.Validate(context.Background(), draft, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.SaveGlobalSetup(context.Background(), SaveGlobalSetupRequest{
		Values: draft, TestedFingerprint: validation.Fingerprint,
		Confirmations:      []ai.ConfirmationRequirement{ai.ConfirmationVerificationCommand},
		ExpectedGeneration: validation.SourceGeneration,
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be global") {
		t.Fatalf("verification approval error = %v", err)
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

func TestSettingsActionClearsEmptyRepositoryOverride(t *testing.T) {
	svc, roots := testService(t, nil, nil, nil)
	model := "repo-model"
	profile := "strict"
	window := "20"
	if _, err := svc.Save(context.Background(), SaveRequest{
		Scope:   ScopeProfile,
		Profile: profile,
		Values: map[string]*string{
			config.FieldIntentWindow: &window,
		},
		ExpectedGeneration: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Save(context.Background(), SaveRequest{
		Scope: ScopeRepository,
		Values: map[string]*string{
			config.FieldModel: &model,
		},
		RepositoryProfile:  &profile,
		ExpectedGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	empty := ""
	if _, err := svc.Save(context.Background(), SaveRequest{
		Scope: ScopeRepository,
		Values: map[string]*string{
			config.FieldModel: nil,
		},
		RepositoryProfile:  &empty,
		ExpectedGeneration: 2,
	}); err != nil {
		t.Fatal(err)
	}
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Settings.Repositories[svc.repoHash]; ok {
		t.Fatalf("empty repository override was retained: %+v",
			doc.Settings.Repositories[svc.repoHash])
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

func TestSettingsIntentPresetMaterializesRevisionMetadata(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "hidden", true
		}
		return "", false
	}
	svc, _ := testService(t, lookup, nil, nil)
	draft := map[string]string{
		config.FieldCommitStrategy: "intent",
		config.FieldCommitPreset:   "balanced",
		config.FieldProvider:       "openai-compat",
	}
	validation, err := svc.Validate(context.Background(), draft, nil)
	if err != nil {
		t.Fatal(err)
	}
	if validation.Preset.Reference() != "intent.balanced@3" || validation.Preset.Customized {
		t.Fatalf("preset = %+v", validation.Preset)
	}
	if validation.ResolvedHot[config.FieldIntentWindow] != "20" ||
		validation.ResolvedHot[config.FieldIntentVerification] != "structural" ||
		validation.ResolvedHot[config.FieldIntentRepairEnabled] != "true" ||
		validation.ResolvedHot[config.FieldDiffEgress] != "true" {
		t.Fatalf("balanced values = %#v", validation.ResolvedHot)
	}
	body, err := revisionSnapshotJSON(validation.ResolvedHot, nil, validation.Preset)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["preset_id"] != "intent.balanced" ||
		payload["preset_version"] != float64(3) || payload["customized"] != false {
		t.Fatalf("revision metadata = %#v", payload)
	}

	draft[config.FieldIntentWindow] = "21"
	custom, err := svc.Validate(context.Background(), draft, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !custom.Preset.Customized || custom.Fingerprint == validation.Fingerprint {
		t.Fatalf("custom preset = %+v fingerprints=%q/%q",
			custom.Preset, validation.Fingerprint, custom.Fingerprint)
	}
}

func TestSettingsProviderUsesProtectedCredentialBelowEnvironment(t *testing.T) {
	var gotKeys []string
	probe := func(_ context.Context, cfg ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		gotKeys = append(gotKeys, cfg.APIKey)
		return ai.ProviderProbeResult{Provider: cfg.Mode, Success: true}, nil
	}
	environmentKey := ""
	lookup := func(name string) (string, bool) {
		if name == ai.EnvAPIKey && environmentKey != "" {
			return environmentKey, true
		}
		return "", false
	}
	svc, roots := testService(t, lookup, probe, nil)
	if err := credentials.NewStore(roots).Set("sk-protected"); err != nil {
		t.Fatal(err)
	}
	draft := map[string]string{config.FieldProvider: "openai-compat"}
	if _, err := svc.TestProvider(context.Background(), draft, nil); err != nil {
		t.Fatal(err)
	}
	environmentKey = "sk-environment"
	if _, err := svc.TestProvider(context.Background(), draft, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotKeys, []string{"sk-protected", "sk-environment"}) {
		t.Fatalf("provider keys = %v", gotKeys)
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
		return ai.ProviderProbeResult{Provider: "openai-compat"},
			errors.New("hidden-key\x1b[2J upstream rejected sk-HCKZa********UC3m")
	}, nil)
	_, err := svc.TestProvider(context.Background(), map[string]string{
		config.FieldProvider: "openai-compat", config.FieldModel: "model",
	}, nil)
	if err == nil || strings.Contains(err.Error(), "hidden-key") ||
		strings.Contains(err.Error(), "HCKZa") ||
		strings.Contains(err.Error(), "UC3m") ||
		strings.Contains(err.Error(), "\x1b") {
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
	_, err = svc.Save(context.Background(), SaveRequest{Scope: ScopeGlobal,
		Values: map[string]*string{config.FieldAPIKey: nil}, ExpectedGeneration: 0})
	if err == nil {
		t.Fatal("clearing an environment-only field succeeded")
	}
	_, err = svc.Save(context.Background(), SaveRequest{Scope: ScopeGlobal,
		Values: map[string]*string{"unknown.field": nil}, ExpectedGeneration: 0})
	if err == nil {
		t.Fatal("clearing an unsupported field succeeded")
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

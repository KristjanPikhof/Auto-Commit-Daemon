package settings

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishConfigLocalApplyNeedsNoProviderTestOrCredentials(t *testing.T) {
	var probes, credentialsRead atomic.Int32
	svc, _ := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		probes.Add(1)
		return ai.ProviderProbeResult{}, nil
	}, nil)
	svc.credentialLookup = func(string) (string, bool) { credentialsRead.Add(1); return "secret", true }
	// An unusable provider is inert in local mode, including credential lookup.
	draft := map[string]string{config.FieldProvider: "openai-compat", config.FieldBaseURL: "not-a-url", config.FieldCommitFormat: "conventional"}
	validation, err := svc.Validate(context.Background(), draft, nil)
	if err != nil || len(validation.Missing) != 0 || validation.ProviderConfig.APIKey != "" {
		t.Fatalf("offline validation=%+v error=%v", validation, err)
	}
	result, err := svc.Apply(context.Background(), ApplyRequest{Values: draft, ExpectedGeneration: validation.SourceGeneration})
	if err != nil || !result.Queued {
		t.Fatalf("offline apply=%+v error=%v", result, err)
	}
	if probes.Load() != 0 || credentialsRead.Load() != 0 {
		t.Fatalf("offline provider access: probes=%d credentials=%d", probes.Load(), credentialsRead.Load())
	}
	var snapshot string
	if err := svc.db.SQL().QueryRow("SELECT snapshot_json FROM config_revisions WHERE id=?", result.RevisionID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	var values map[string]any
	if err := json.Unmarshal([]byte(snapshot), &values); err != nil {
		t.Fatal(err)
	}
	if values[config.FieldCommitStrategy] != "local" || values[config.FieldCommitFormat] != "conventional" {
		t.Fatalf("saved defaults/styles=%v", values)
	}
	for _, name := range []string{"preset_id", "preset_version", config.FieldIntentWindow, config.FieldIntentRepairEnabled} {
		if _, exists := values[name]; exists {
			t.Fatalf("retired snapshot key %s", name)
		}
	}
	var publication int
	if err := svc.db.SQL().QueryRow("SELECT (SELECT COUNT(*) FROM publication_drains)+(SELECT COUNT(*) FROM flush_requests)").Scan(&publication); err != nil || publication != 0 {
		t.Fatalf("configuration requested publication: %d %v", publication, err)
	}
}

func TestFinishConfigIntentApplyDoesNotProbe(t *testing.T) {
	var probes atomic.Int32
	svc, _ := testService(t, func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "approved-key", true
		}
		return "", false
	}, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		probes.Add(1)
		return ai.ProviderProbeResult{}, nil
	}, nil)
	draft := map[string]string{config.FieldCommitStrategy: "intent", config.FieldProvider: "openai-compat", config.FieldDiffEgress: "true", config.FieldCommitFormat: "imperative"}
	v, err := svc.Validate(context.Background(), draft, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Missing) != 1 || v.Missing[0] != ai.ConfirmationDiffEgress {
		t.Fatalf("privacy approval=%v", v.Missing)
	}
	result, err := svc.Apply(context.Background(), ApplyRequest{Values: draft, ExpectedGeneration: v.SourceGeneration, Confirmations: []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress}})
	if err != nil || !result.Queued || probes.Load() != 0 {
		t.Fatalf("Intent apply=%+v error=%v probes=%d", result, err, probes.Load())
	}
}

func TestFinishConfigLegacySnapshotIsReadonlyAndScoped(t *testing.T) {
	var probes atomic.Int32
	svc, roots := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		probes.Add(1)
		return ai.ProviderProbeResult{}, nil
	}, nil)
	if err := os.MkdirAll(roots.Config, 0700); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"version":1,"settings":{"global":{"commit.strategy":"event","commit.format":"conventional"},"profiles":{"named":{"fields":{"commit.strategy":"intent","commit.preset":"quality","verification.full.command":"approved check","intent.window":4}}},"repositories":{"` + svc.repoHash + `":{"profile":"named","fields":{"ai.model":"repo-model","intent.repair.enabled":true}}}}}`)
	if err := os.WriteFile(roots.ConfigPath(), body, 0600); err != nil {
		t.Fatal(err)
	}
	var before, after int
	_ = svc.db.SQL().QueryRow("SELECT total_changes()").Scan(&before)
	repo, err := svc.Snapshot(context.Background(), ScopeRepository, "")
	if err != nil {
		t.Fatal(err)
	}
	global, err := svc.Snapshot(context.Background(), ScopeGlobal, "")
	if err != nil {
		t.Fatal(err)
	}
	if fieldByName(t, repo, config.FieldCommitStrategy).DraftValue != "intent" || fieldByName(t, global, config.FieldCommitStrategy).DraftValue != "local" || fieldByName(t, repo, config.FieldCommitFormat).DraftValue != "conventional" {
		t.Fatalf("scope choices lost: repo=%+v global=%+v", repo, global)
	}
	if fieldByName(t, repo, config.FieldIntentVerification).DraftValue != "full" || fieldByName(t, repo, config.FieldDiffEgress).DraftValue != "true" || fieldByName(t, global, config.FieldIntentVerification).DraftValue != "none" {
		t.Fatal("legacy scoped verification/privacy defaults lost")
	}
	for _, field := range repo.Fields {
		if field.Name == config.FieldIntentWindow || field.Name == config.FieldIntentRepairEnabled {
			t.Fatal("legacy control exposed")
		}
	}
	if repo.Experiment != nil || repo.PresetID != "" {
		t.Fatal("retired dashboard exposed")
	}
	_ = svc.db.SQL().QueryRow("SELECT total_changes()").Scan(&after)
	current, _ := os.ReadFile(roots.ConfigPath())
	if string(body) != string(current) || before != after || probes.Load() != 0 {
		t.Fatal("snapshot wrote state or contacted provider")
	}
	model := "changed"
	_, err = svc.Save(context.Background(), SaveRequest{Scope: ScopeRepository, Values: map[string]*string{config.FieldModel: &model}})
	if err != nil {
		t.Fatal(err)
	}
	current, _ = os.ReadFile(roots.ConfigPath())
	if !strings.Contains(string(current), "legacy_finish_settings") || !strings.Contains(string(current), "intent.window") {
		t.Fatal("explicit write discarded legacy provenance")
	}
}

func TestFinishConfigRemovedSettingsFailWithGuidance(t *testing.T) {
	svc, _ := testService(t, nil, nil, func(context.Context, state.DaemonState) error { return nil })
	value := "10"
	for _, name := range []string{config.FieldCommitPreset, config.FieldIntentWindow, config.FieldIntentRepairEnabled, "capture.max_pending_events"} {
		_, err := svc.Save(context.Background(), SaveRequest{Scope: ScopeGlobal, Values: map[string]*string{name: &value}})
		if err == nil || !strings.Contains(err.Error(), "was removed") || !strings.Contains(err.Error(), "acd finish") {
			t.Fatalf("%s error=%v", name, err)
		}
	}
}

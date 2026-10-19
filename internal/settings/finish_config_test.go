package settings

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	})
	svc.credentialLookup = func(string) (string, bool) { credentialsRead.Add(1); return "secret", true }
	// An unusable provider is inert in local mode, including credential lookup.
	draft := map[string]string{config.FieldProvider: "openai-compat", config.FieldBaseURL: "not-a-url", config.FieldCommitFormat: "conventional"}
	validation, err := svc.Validate(context.Background(), draft, nil)
	if err != nil || len(validation.Missing) != 0 || validation.ProviderConfig.APIKey != "" {
		t.Fatalf("offline validation=%+v error=%v", validation, err)
	}
	result, err := svc.Apply(context.Background(), ApplyRequest{Values: draft, ExpectedGeneration: validation.SourceGeneration})
	if err != nil || !result.Applied {
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
	if err := svc.db.SQL().QueryRow("SELECT (SELECT COUNT(*) FROM finish_requests)+(SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('publication_drains','flush_requests'))").Scan(&publication); err != nil || publication != 0 {
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
	})
	draft := map[string]string{config.FieldCommitStrategy: "intent", config.FieldDiffEgress: "true", config.FieldProvider: "openai-compat", config.FieldCommitFormat: "imperative"}
	v, err := svc.Validate(context.Background(), draft, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Missing) != 1 || v.Missing[0] != ai.ConfirmationDiffEgress {
		t.Fatalf("privacy approval=%v", v.Missing)
	}
	result, err := svc.Apply(context.Background(), ApplyRequest{Values: draft, ExpectedGeneration: v.SourceGeneration, Confirmations: []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress}})
	if err != nil || !result.Applied || probes.Load() != 0 {
		t.Fatalf("Intent apply=%+v error=%v probes=%d", result, err, probes.Load())
	}
}

func TestFinishConfigLegacySnapshotIsReadonlyAndScoped(t *testing.T) {
	var probes atomic.Int32
	svc, roots := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		probes.Add(1)
		return ai.ProviderProbeResult{}, nil
	})
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
	svc, _ := testService(t, nil, nil)
	value := "10"
	for _, name := range []string{config.FieldCommitPreset, config.FieldIntentWindow, config.FieldIntentRepairEnabled, "capture.max_pending_events"} {
		_, err := svc.Save(context.Background(), SaveRequest{Scope: ScopeGlobal, Values: map[string]*string{name: &value}})
		if err == nil || !strings.Contains(err.Error(), "was removed") || !strings.Contains(err.Error(), "acd finish") {
			t.Fatalf("%s error=%v", name, err)
		}
	}
}

func TestFinishConfigApplyApprovesCommandWithoutExecutingIt(t *testing.T) {
	for _, mode := range []string{"local", "intent"} {
		t.Run(mode, func(t *testing.T) {
			svc, _ := testService(t, nil, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
				t.Fatal("apply contacted provider")
				return ai.ProviderProbeResult{}, nil
			})
			marker := filepath.Join(t.TempDir(), "project-check-ran")
			command := "touch '" + marker + "'"
			draft := map[string]string{config.FieldCommitStrategy: mode, config.FieldDiffEgress: "true", config.FieldProvider: "openai-compat", config.FieldIntentVerification: "full", config.FieldVerificationFullCommand: command}
			svc.credentialLookup = func(string) (string, bool) { return "approved-key", true }
			validation, err := svc.Validate(context.Background(), draft, []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress})
			if err != nil {
				t.Fatal(err)
			}
			if len(validation.Missing) != 1 || validation.Missing[0] != ai.ConfirmationVerificationCommand {
				t.Fatalf("command approval = %v", validation.Missing)
			}
			if _, err := svc.Apply(context.Background(), ApplyRequest{Values: draft, ExpectedGeneration: validation.SourceGeneration}); err == nil {
				t.Fatal("unapproved command applied")
			}
			result, err := svc.Apply(context.Background(), ApplyRequest{Values: draft, TestedFingerprint: validation.Fingerprint, ExpectedGeneration: validation.SourceGeneration, Confirmations: []ai.ConfirmationRequirement{ai.ConfirmationVerificationCommand, ai.ConfirmationDiffEgress}})
			if err != nil || !result.Applied {
				t.Fatalf("approved configuration = %+v: %v", result, err)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("apply executed project check: %v", err)
			}
			revision, err := state.ConfigRevisionByID(context.Background(), svc.db, result.RevisionID)
			if err != nil || !strings.Contains(revision.SnapshotJSON, command) || !strings.Contains(revision.SnapshotJSON, "verification_command") {
				t.Fatalf("exact command/approval lost: %s: %v", revision.SnapshotJSON, err)
			}
		})
	}
}

func TestFinishConfigIntentSubprocessIgnoresUnrelatedCredentials(t *testing.T) {
	svc, _ := testService(t, nil, nil)
	svc.credentialLookup = func(string) (string, bool) { t.Fatal("subprocess read API credentials"); return "", false }
	draft := map[string]string{config.FieldCommitStrategy: "intent", config.FieldDiffEgress: "true", config.FieldProvider: "subprocess:local-ai"}
	validation, err := svc.Validate(context.Background(), draft, []ai.ConfirmationRequirement{ai.ConfirmationSubprocessExecution, ai.ConfirmationDiffEgress})
	if err != nil || validation.ProviderConfig.APIKey != "" || len(validation.Missing) != 0 {
		t.Fatalf("subprocess configuration = %+v: %v", validation, err)
	}
}

func TestFinishConfigRejectsIntentWithoutAITransport(t *testing.T) {
	svc, _ := testService(t, nil, nil)
	_, err := svc.Apply(context.Background(), ApplyRequest{Values: map[string]string{config.FieldCommitStrategy: "intent"}})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "local") {
		t.Fatalf("unusable Intent config approved: %v", err)
	}
	var revisions int
	if err := svc.db.SQL().QueryRow("SELECT COUNT(*) FROM config_revisions").Scan(&revisions); err != nil || revisions != 0 {
		t.Fatalf("invalid configuration stored: %d: %v", revisions, err)
	}
}

func TestFinishConfigGlobalHTTPApprovalRemainsExactAndDoesNotProbe(t *testing.T) {
	svc, roots := testGlobalService(t, func(name string) (string, bool) { return "approved-key", name == ai.EnvAPIKey }, func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		t.Fatal("saving approval contacted provider")
		return ai.ProviderProbeResult{}, nil
	})
	draft := map[string]string{config.FieldCommitStrategy: "intent", config.FieldDiffEgress: "true", config.FieldProvider: "openai-compat", config.FieldBaseURL: "http://127.0.0.1:12345/v1"}
	confirmations := []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials, ai.ConfirmationInsecureEndpointCredentials, ai.ConfirmationDiffEgress}
	validation, err := svc.Validate(context.Background(), draft, confirmations)
	if err != nil || len(validation.Missing) != 0 {
		t.Fatalf("HTTP consent = %+v: %v", validation, err)
	}
	_, err = svc.SaveGlobalSetup(context.Background(), SaveGlobalSetupRequest{Values: draft, TestedFingerprint: validation.Fingerprint, ExpectedGeneration: validation.SourceGeneration, Confirmations: confirmations})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	approval, ok := config.ActiveGlobalSetupApproval(doc)
	if !ok || approval.Fingerprint != validation.Fingerprint || len(approval.Confirmations) != 3 {
		t.Fatalf("exact HTTP approval = %+v: %t", approval, ok)
	}
}

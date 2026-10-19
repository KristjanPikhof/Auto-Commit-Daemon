package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestFinishConfigLegacyTranslationPreservesScopes(t *testing.T) {
	body := []byte(`{"version":1,"generation":9,"future":{"retained":true},"legacy_finish_settings":{"future_archive":true},"settings":{"global":{"commit.strategy":"event","commit.format":"conventional","intent.window":8,"ai.diff_egress":true},"profiles":{"named":{"fields":{"commit.strategy":"intent","intent.repair.enabled":true,"intent.verification":"full","verification.full.command":"go test ./..."}}},"repositories":{"repo":{"profile":"named","fields":{"commit.strategy":"event","ai.model":"explicit-model","capture.max_pending_events":20}}}}}`)
	doc, err := ParseDocument(body)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Version != 2 || doc.Generation != 9 {
		t.Fatalf("identity changed: %+v", doc)
	}
	if err := ValidateDocument(doc); err != nil {
		t.Fatal(err)
	}
	for _, fields := range []Overrides{doc.Settings.Global, doc.Settings.Profiles["named"].Fields, doc.Settings.Repositories["repo"].Fields} {
		for name := range fields {
			if retiredFinishFields[name] {
				t.Fatalf("retired %s retained in authoring", name)
			}
		}
	}
	if string(doc.Settings.Global[FieldCommitStrategy]) != `"local"` || string(doc.Settings.Global[FieldCommitFormat]) != `"conventional"` || string(doc.Settings.Global[FieldDiffEgress]) != "true" || string(doc.Settings.Profiles["named"].Fields[FieldCommitStrategy]) != `"intent"` || string(doc.Settings.Profiles["named"].Fields[FieldIntentVerification]) != `"full"` || string(doc.Settings.Repositories["repo"].Fields[FieldCommitStrategy]) != `"local"` {
		t.Fatalf("explicit scopes lost: %+v", doc.Settings)
	}
	archive := string(doc.Extra["legacy_finish_settings"])
	for _, want := range []string{"future_archive", "intent.window", "intent.repair.enabled", "capture.max_pending_events", "event"} {
		if !strings.Contains(archive, want) {
			t.Fatalf("archive lost %s: %s", want, archive)
		}
	}
	first, _ := json.Marshal(doc)
	TranslateLegacyDocument(doc)
	second, _ := json.Marshal(doc)
	if string(first) != string(second) {
		t.Fatal("translation not idempotent")
	}
}

func TestFinishConfigLegacyStoreLoadDoesNotWrite(t *testing.T) {
	roots := testConfigRoots(t)
	if err := os.MkdirAll(roots.Config, 0700); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"version":1,"settings":{"global":{"commit.strategy":"event","intent.window":4}}}`)
	if err := os.WriteFile(roots.ConfigPath(), body, 0600); err != nil {
		t.Fatal(err)
	}
	doc, err := NewStore(roots).Load()
	if err != nil || string(doc.Settings.Global[FieldCommitStrategy]) != `"local"` {
		t.Fatalf("load=%+v err=%v", doc, err)
	}
	after, err := os.ReadFile(roots.ConfigPath())
	if err != nil || string(after) != string(body) {
		t.Fatal("read wrote legacy configuration")
	}
	if _, err := os.Stat(roots.ConfigLockPath()); !os.IsNotExist(err) {
		t.Fatalf("read created lock: %v", err)
	}
}

func TestFinishConfigLegacyEnvironment(t *testing.T) {
	for _, value := range []string{"event", " EVENT ", "intent", "local"} {
		got, err := ResolveField(FieldCommitStrategy, ResolveInput{LookupEnv: func(name string) (string, bool) {
			if name == "ACD_COMMIT_STRATEGY" {
				return value, true
			}
			return "", false
		}})
		want := value
		if strings.EqualFold(strings.TrimSpace(value), "event") {
			want = "local"
		}
		if err != nil || got.EffectiveValue() != want || got.Source != SourceEnvironment {
			t.Fatalf("%q => %+v %v", value, got, err)
		}
	}
}

func TestFinishConfigLegacyPresetFallbackPreservesEffectiveChoices(t *testing.T) {
	cases := []struct {
		name, global, profile, repository, verification, egress string
		env                                                     map[string]string
	}{
		{name: "quality approved command", global: `"commit.strategy":"intent","commit.preset":"quality","verification.full.command":"approved check"`, verification: "full", egress: "true"},
		{name: "implicit balanced", global: `"commit.strategy":"intent"`, verification: "structural", egress: "true"},
		{name: "repository event overrides implicit balanced", global: `"commit.strategy":"intent"`, repository: `"commit.strategy":" event "`, verification: "none", egress: "false"},
		{name: "inherited profile quality", global: `"commit.strategy":"intent"`, profile: `"commit.preset":"quality"`, verification: "full", egress: "true"},
		{name: "explicit global policy overrides profile preset", global: `"commit.strategy":"intent","intent.verification":"fast","verification.fast.command":"approved check","ai.diff_egress":false`, profile: `"commit.preset":"quality"`, verification: "fast", egress: "false"},
		{name: "environment beats derived choice", global: `"commit.strategy":"intent","commit.preset":"quality"`, env: map[string]string{"ACD_INTENT_VERIFICATION": "none", "ACD_AI_DIFF_EGRESS": "false"}, verification: "none", egress: "false"},
		{name: "legacy environment preset", env: map[string]string{"ACD_COMMIT_STRATEGY": "intent", "ACD_COMMIT_PRESET": "quality"}, verification: "full", egress: "true"},
		{name: "event quality preserves privacy request", global: `"commit.strategy":"event","commit.preset":"quality"`, verification: "none", egress: "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := ParseDocument([]byte(`{"version":1,"settings":{"global":{` + tc.global + `},"profiles":{"named":{"fields":{` + tc.profile + `}}},"repositories":{"repo":{"profile":"named","fields":{` + tc.repository + `}}}}}`))
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateDocument(doc); err != nil {
				t.Fatal(err)
			}
			lookup := func(name string) (string, bool) { value, ok := tc.env[name]; return value, ok }
			input := ResolveInput{Global: doc.Settings.Global, Profile: doc.Settings.Profiles["named"].Fields, Repository: doc.Settings.Repositories["repo"].Fields, LookupEnv: lookup, LegacyDefaults: LegacyFinishDefaults(doc, []string{"repository:repo", "profile:named", "global"}, lookup)}
			fields, err := ResolveAll(input)
			if err != nil || fields[FieldIntentVerification].EffectiveValue() != tc.verification || fields[FieldDiffEgress].EffectiveValue() != tc.egress {
				t.Fatalf("verification=%+v egress=%+v err=%v", fields[FieldIntentVerification], fields[FieldDiffEgress], err)
			}
			if doc.Settings.GlobalSetupApproval != nil {
				t.Fatal("translation invented provider approval")
			}
			// Fallback survives a supported save without promoting it over scopes.
			body, _ := json.Marshal(doc)
			reloaded, err := ParseDocument(body)
			if err != nil || string(reloaded.Extra["legacy_finish_settings"]) != string(doc.Extra["legacy_finish_settings"]) {
				t.Fatalf("archive round-trip: %v", err)
			}
		})
	}
}

func TestFinishConfigInactiveLegacyDefaultsPreserveProvenance(t *testing.T) {
	if len(LegacyFinishDefaults(NewDocument(), []string{"global"}, nil)) != 0 {
		t.Fatal("new document received old preset defaults")
	}
	archived := NewDocument()
	archive := json.RawMessage(`{"version":1,"retired":true,"scopes":{"global":{"commit.strategy":"intent","commit.preset":"quality"}}}`)
	archived.Extra["legacy_finish_settings"] = archive
	body, err := json.Marshal(archived)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := ParseDocument(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(LegacyFinishDefaults(loaded, []string{"global"}, nil)) != 0 || string(loaded.Extra["legacy_finish_settings"]) != string(archive) {
		t.Fatal("retired archive was reactivated or its provenance changed")
	}
}

func TestFinishConfigInvalidEnvironmentStrategyDoesNotSelectLocal(t *testing.T) {
	input := ResolveInput{LookupEnv: func(name string) (string, bool) { return "intnet", name == "ACD_COMMIT_STRATEGY" }}
	if _, err := ResolveField(FieldCommitStrategy, input); err == nil || !strings.Contains(err.Error(), "ACD_COMMIT_STRATEGY") {
		t.Fatalf("invalid strategy silently selected a default: %v", err)
	}
	input.Global = Overrides{FieldCommitStrategy: json.RawMessage(`"intent"`)}
	resolved, err := ResolveField(FieldCommitStrategy, input)
	if err != nil || resolved.EffectiveValue() != "intent" || resolved.Source != SourceGlobal {
		t.Fatalf("authored strategy must override environment: %+v %v", resolved, err)
	}
}

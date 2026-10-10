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

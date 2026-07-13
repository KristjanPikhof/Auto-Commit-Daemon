package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestConfigDocumentPreservesKnownAndUnknownFields(t *testing.T) {
	doc, err := ParseDocument([]byte(`{
  "repo_lifecycle":{"autodiscovery":false,"future":"kept"},
  "future_top":{"also":"kept"},
  "settings":{
    "future_settings":17,
    "global":{"ai.model":"global-model"},
    "profiles":{"fast":{"fields":{"intent.window":4},"future_profile":true}},
    "repositories":{"abc123":{"profile":"fast","fields":{"commit.strategy":"intent"},"future_repo":"kept"}}
  }
}`))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Version != SettingsSchemaVersion || doc.Settings.Repositories["abc123"].Profile != "fast" {
		t.Fatalf("unexpected document: %#v", doc)
	}
	doc.Settings.Global[FieldModel] = rawString("changed")
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"future_top", "future_settings", "future_profile", "future_repo", "future", "changed"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("round trip lost %q: %s", want, body)
		}
	}
}

func TestConfigDocumentRejectsMalformedAndNewerVersion(t *testing.T) {
	for _, body := range []string{``, `{`, `{} {}`, `{"version":2}`} {
		if _, err := ParseDocument([]byte(body)); err == nil {
			t.Fatalf("ParseDocument(%q) succeeded", body)
		}
	}
}

func TestConfigStoreConcurrentWritersPreserveUpdatesAndPermissions(t *testing.T) {
	roots := testConfigRoots(t)
	store := NewStore(roots)
	const writers = 20
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errCh <- store.Update(func(doc *Document) error {
				doc.Settings.Profiles[fmt.Sprintf("profile-%02d", i)] = Profile{
					Fields: Overrides{FieldIntentWindow: json.RawMessage(fmt.Sprintf("%d", i+1))},
				}
				return nil
			})
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	doc, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Settings.Profiles) != writers {
		t.Fatalf("profiles = %d, want %d", len(doc.Settings.Profiles), writers)
	}
	assertMode(t, roots.Config, 0o700)
	assertMode(t, roots.ConfigPath(), 0o600)
	assertMode(t, roots.ConfigLockPath(), 0o600)
}

func TestConfigStoreRejectsSymlinkAndNonRegularTargets(t *testing.T) {
	t.Run("config symlink", func(t *testing.T) {
		roots := testConfigRoots(t)
		if err := os.MkdirAll(roots.Config, 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "target")
		if err := os.WriteFile(target, []byte(`{}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, roots.ConfigPath()); err != nil {
			t.Fatal(err)
		}
		if _, err := NewStore(roots).Load(); err == nil {
			t.Fatal("Load accepted symlink")
		}
	})
	t.Run("lock symlink", func(t *testing.T) {
		roots := testConfigRoots(t)
		if err := os.MkdirAll(roots.Config, 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "target")
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, roots.ConfigLockPath()); err != nil {
			t.Fatal(err)
		}
		if err := NewStore(roots).Update(func(*Document) error { return nil }); err == nil {
			t.Fatal("Update accepted symlink lock")
		}
	})
}

func TestConfigStoreCallbackErrorLeavesDiskUnchanged(t *testing.T) {
	roots := testConfigRoots(t)
	store := NewStore(roots)
	if err := store.Update(func(doc *Document) error {
		doc.Extra["kept"] = rawString("yes")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(roots.ConfigPath())
	want := errors.New("stop")
	if err := store.Update(func(doc *Document) error {
		doc.Extra["lost"] = rawString("no")
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("Update error = %v", err)
	}
	after, _ := os.ReadFile(roots.ConfigPath())
	if string(after) != string(before) {
		t.Fatal("callback error changed config")
	}
}

func TestResolvePrecedenceAndShadowedEnvironment(t *testing.T) {
	input := ResolveInput{
		Experiment: Overrides{FieldModel: rawString("experiment")},
		Repository: Overrides{FieldModel: rawString("repository")},
		Profile:    Overrides{FieldModel: rawString("profile")},
		Global:     Overrides{FieldModel: rawString("global")},
		LookupEnv: func(name string) (string, bool) {
			if name == "ACD_AI_MODEL" {
				return "environment", true
			}
			return "", false
		},
	}
	resolved, err := ResolveField(FieldModel, input)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Value != "experiment" || resolved.Source != SourceExperiment || resolved.ShadowedEnvironment == nil || resolved.ShadowedEnvironment.Value != "environment" {
		t.Fatalf("resolved = %#v", resolved)
	}

	delete(input.Experiment, FieldModel)
	delete(input.Repository, FieldModel)
	delete(input.Profile, FieldModel)
	delete(input.Global, FieldModel)
	resolved, err = ResolveField(FieldModel, input)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Value != "environment" || resolved.Source != SourceEnvironment || resolved.ShadowedEnvironment != nil {
		t.Fatalf("resolved inherited env = %#v", resolved)
	}

	input.LookupEnv = func(string) (string, bool) { return "", false }
	resolved, err = ResolveField(FieldModel, input)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Value != "gpt-5.4-mini" || resolved.Source != SourceDefault {
		t.Fatalf("resolved default = %#v", resolved)
	}
}

func TestResolveClearToInheritAndSecretExclusion(t *testing.T) {
	doc := NewDocument()
	doc.Settings.Global[FieldAPIKey] = rawString("must-not-persist")
	if err := ValidateDocument(doc); err == nil || !strings.Contains(err.Error(), "cannot be persisted") {
		t.Fatalf("ValidateDocument secret error = %v", err)
	}
	delete(doc.Settings.Global, FieldAPIKey)
	doc.Settings.Global[FieldModel] = json.RawMessage("null")
	if err := ValidateDocument(doc); err == nil || !strings.Contains(err.Error(), "remove the key to inherit") {
		t.Fatalf("ValidateDocument null error = %v", err)
	}
	delete(doc.Settings.Global, FieldModel)
	if err := ValidateDocument(doc); err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveField(FieldAPIKey, ResolveInput{LookupEnv: func(string) (string, bool) { return "super-secret", true }})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Value != "set" || resolved.Source != SourceEnvironment {
		t.Fatalf("secret resolution exposed or lost state: %#v", resolved)
	}
}

func TestFieldCatalogClassifiesHotRestartAndValidates(t *testing.T) {
	apiKey, ok := LookupField(FieldAPIKey)
	if !ok || !apiKey.Sensitive || apiKey.Persistable {
		t.Fatalf("API key metadata = %#v", apiKey)
	}
	sensitive, ok := LookupField("capture.sensitive_globs")
	if !ok || sensitive.Boundary != ApplyRestart || !sensitive.Sensitive {
		t.Fatalf("sensitive globs metadata = %#v", sensitive)
	}
	strategy, _ := LookupField(FieldCommitStrategy)
	if _, err := normalizeValue(strategy, "random"); err == nil {
		t.Fatal("invalid strategy accepted")
	}
}

func rawString(value string) json.RawMessage {
	body, _ := json.Marshal(value)
	return body
}

func testConfigRoots(t *testing.T) paths.Roots {
	t.Helper()
	root := t.TempDir()
	return paths.Roots{State: filepath.Join(root, "state"), Share: filepath.Join(root, "share"), Config: filepath.Join(root, "config")}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %o, want %o", path, got, want)
	}
}

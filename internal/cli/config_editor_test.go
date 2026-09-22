package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func editorFixture(t *testing.T) *configEditor {
	t.Helper()
	base := t.TempDir()
	editor := &configEditor{roots: paths.Roots{Config: filepath.Join(base, "config"), Share: filepath.Join(base, "share"), State: filepath.Join(base, "state")}, lookup: func(string) (string, bool) { return "", false }, probe: func(_ context.Context, cfg ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		return ai.ProviderProbeResult{Provider: cfg.Mode, Success: true}, nil
	}, nudge: func(context.Context, state.DaemonState) error { return nil }}
	return editor
}
func editorString(value string) *string { return &value }
func editorSeed(t *testing.T, e *configEditor, update func(*config.Document)) {
	t.Helper()
	if err := config.NewStore(e.roots).Update(func(doc *config.Document) error { update(doc); return nil }); err != nil {
		t.Fatal(err)
	}
}
func editorDraft(t *testing.T, e *configEditor, scope string, changes map[string]*string) settingsui.EditorDraft {
	t.Helper()
	snapshot, err := e.Load(context.Background(), scope, nil)
	if err != nil {
		t.Fatal(err)
	}
	return settingsui.EditorDraft{Scope: scope, Generation: snapshot.Generation, Changes: changes}
}
func editorModel(t *testing.T, e *configEditor, scope string) settingsui.EditorField {
	t.Helper()
	snapshot, err := e.Load(context.Background(), scope, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range snapshot.Fields {
		if field.Key == config.FieldModel {
			return field
		}
	}
	t.Fatal("model missing")
	return settingsui.EditorField{}
}

func TestConfigEditorGlobalModelSavePreservesSparseOverrides(t *testing.T) {
	e := editorFixture(t)
	editorSeed(t, e, func(doc *config.Document) { doc.Settings.Global[config.FieldModel] = json.RawMessage(`"old-model"`) })
	draft := editorDraft(t, e, "global", map[string]*string{config.FieldModel: editorString("new-model")})
	review, err := e.Review(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(review.Text, "Global defaults") || !strings.Contains(review.Text, "new-model") {
		t.Fatal(review.Text)
	}
	if editorModel(t, e, "global").Value != "old-model" {
		t.Fatal("review wrote settings")
	}
	result, err := review.Save(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "Settings saved") {
		t.Fatal(result)
	}
	doc, err := config.NewStore(e.roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Settings.Global) != 1 || len(doc.Settings.Repositories) != 0 || editorModel(t, e, "global").Value != "new-model" {
		t.Fatalf("save flattened inheritance: %+v", doc.Settings)
	}
	if doc.Settings.GlobalSetupApproval == nil {
		t.Fatal("global approval was not saved")
	}
}

func TestConfigEditorRepositoryResetOnlyModelAndQueuesActivation(t *testing.T) {
	e := editorFixture(t)
	e.repo = materializeTestRepo(t, false)
	editorSeed(t, e, func(doc *config.Document) {
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"global-model"`)
		doc.Settings.Repositories[central.CanonicalID(e.repo)] = config.RepositorySettings{Fields: config.Overrides{config.FieldModel: json.RawMessage(`"repo-model"`), config.FieldCommitFormat: json.RawMessage(`"conventional"`)}}
	})
	draft := editorDraft(t, e, "repo", map[string]*string{config.FieldModel: nil})
	review, err := e.Review(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	result, err := review.Save(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "Waiting to apply") {
		t.Fatal(result)
	}
	model := editorModel(t, e, "repo")
	if model.Overridden || model.Value != "global-model" {
		t.Fatalf("model=%+v", model)
	}
	doc, err := config.NewStore(e.roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	fields := doc.Settings.Repositories[central.CanonicalID(e.repo)].Fields
	if len(fields) != 1 || string(fields[config.FieldCommitFormat]) != `"conventional"` {
		t.Fatalf("unrelated overrides changed: %v", fields)
	}
	db, err := state.OpenReadOnly(context.Background(), filepath.Join(e.repo, ".git", "acd", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	runtime, err := state.RuntimeConfigActivationState(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.DesiredRevisionID.Int64 == 0 || runtime.AppliedRevisionID.Int64 != 0 {
		t.Fatalf("runtime=%+v", runtime)
	}
}

func TestConfigEditorCredentialReplacementTestsBeforeSaving(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "rejected"}[reject], func(t *testing.T) {
			e := editorFixture(t)
			editorSeed(t, e, func(doc *config.Document) {
				doc.Settings.Global[config.FieldProvider] = json.RawMessage(`"openai-compat"`)
				doc.Settings.Global[config.FieldModel] = json.RawMessage(`"old-model"`)
			})
			if err := credentials.NewStore(e.roots).Set("old-test-key"); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(e.roots.ConfigPath())
			calls := 0
			e.probe = func(_ context.Context, cfg ai.ProviderConfig) (ai.ProviderProbeResult, error) {
				calls++
				if cfg.APIKey != "new-test-key" {
					t.Fatal("probe did not use staged key")
				}
				stored, _ := credentials.NewStore(e.roots).Read()
				if stored != "old-test-key" {
					t.Fatal("credential written before test")
				}
				if reject {
					return ai.ProviderProbeResult{}, errors.New("provider rejected new-test-key")
				}
				return ai.ProviderProbeResult{Provider: cfg.Mode, Success: true}, nil
			}
			draft := editorDraft(t, e, "global", map[string]*string{config.FieldModel: editorString("new-model")})
			draft.Credential = "new-test-key"
			review, err := e.Review(context.Background(), draft)
			if err != nil {
				t.Fatal(err)
			}
			if calls != 0 || strings.Contains(review.Text, "new-test-key") {
				t.Fatal("review probed or exposed credential")
			}
			result, err := review.Save(context.Background())
			if strings.Contains(result, "new-test-key") || err != nil && strings.Contains(err.Error(), "new-test-key") {
				t.Fatal("credential leaked")
			}
			stored, _ := credentials.NewStore(e.roots).Read()
			if reject {
				after, _ := os.ReadFile(e.roots.ConfigPath())
				if err == nil || stored != "old-test-key" || !bytes.Equal(before, after) {
					t.Fatal("failed probe changed saved state")
				}
			} else if err != nil || stored != "new-test-key" {
				t.Fatalf("replacement failed: %v", err)
			}
		})
	}
}

func TestConfigEditorRejectsStaleReviewAndEnvironmentKey(t *testing.T) {
	e := editorFixture(t)
	draft := editorDraft(t, e, "global", map[string]*string{config.FieldModel: editorString("new-model")})
	review, err := e.Review(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	editorSeed(t, e, func(doc *config.Document) {
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"concurrent-model"`)
	})
	if _, err := review.Save(context.Background()); err == nil {
		t.Fatal("stale review saved")
	}
	if editorModel(t, e, "global").Value != "concurrent-model" {
		t.Fatal("concurrent change overwritten")
	}
	e.lookup = func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "environment-key", true
		}
		return "", false
	}
	draft = editorDraft(t, e, "global", nil)
	draft.Credential = "replacement-key"
	if _, err := e.Review(context.Background(), draft); err == nil || !strings.Contains(err.Error(), "ACD_AI_API_KEY") {
		t.Fatalf("environment override hidden: %v", err)
	}
}

func TestConfigEditorPublicRoutesAndOutsideRepository(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("TERM", "dumb")
	old := runConfigEditorUI
	t.Cleanup(func() { runConfigEditorUI = old })
	calls := 0
	runConfigEditorUI = func(_ context.Context, _ settingsui.EditorBackend, opts settingsui.EditorOptions) error {
		calls++
		if opts.Scope != "global" || opts.Repo != "" || !opts.Accessible {
			t.Fatalf("options=%+v", opts)
		}
		return nil
	}
	for _, args := range [][]string{{"config"}, {"config", "edit"}, {"config", "--scope", "global"}} {
		root := newRootCmd()
		root.SetOut(&bytes.Buffer{})
		root.SetErr(&bytes.Buffer{})
		root.SetArgs(args)
		if err := root.Execute(); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 {
		t.Fatalf("calls=%d", calls)
	}
	for _, args := range [][]string{{"config", "--json"}, {"config", "edit", "--json"}, {"config", "--scope", "global", "--repo", "."}} {
		root := newRootCmd()
		root.SetOut(&bytes.Buffer{})
		root.SetErr(&bytes.Buffer{})
		root.SetArgs(args)
		if err := root.Execute(); err == nil {
			t.Fatalf("invalid flags accepted: %v", args)
		}
	}
	if calls != 3 {
		t.Fatal("invalid invocation opened editor")
	}
}

// Keep this compile-time assertion beside tests that exercise the real service.
var _ settings.ProbeFunc = (&configEditor{}).probe

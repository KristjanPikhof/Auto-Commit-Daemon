package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
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
	}}
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

func TestFinishRemovalConfigEditorGlobalModelSavePreservesSparseOverrides(t *testing.T) {
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

func TestFinishRemovalConfigEditorRepositoryResetAppliesExactRevision(t *testing.T) {
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
	if !strings.Contains(result, "Applied:") {
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
	if runtime.DesiredRevisionID.Int64 == 0 || runtime.AppliedRevisionID.Int64 != runtime.DesiredRevisionID.Int64 {
		t.Fatalf("runtime=%+v", runtime)
	}
}

func TestFinishConfigEditorSaveSupersedesInterruptedApproval(t *testing.T) {
	for _, scenario := range []string{"initial_pending", "pending", "acknowledged", "desired_changed_after_review"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			e := editorFixture(t)
			e.repo = materializeTestRepo(t, false)
			e.probe = func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
				t.Fatal("reviewed settings save ran a provider")
				return ai.ProviderProbeResult{}, nil
			}
			db, err := state.Open(ctx, filepath.Join(e.repo, ".git", "acd", "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var baseline state.ConfigRevision
			if scenario != "initial_pending" {
				baseline, _ = finishConfigRevision(t, db, map[string]any{config.FieldCommitFormat: "imperative"}, sql.NullInt64{}, true)
			}
			pending, request := finishConfigRevision(t, db, map[string]any{config.FieldCommitFormat: "conventional"}, sql.NullInt64{Int64: baseline.ID, Valid: baseline.ID != 0}, false)
			if scenario == "acknowledged" {
				if ok, err := state.AcknowledgeConfigActivation(ctx, db, request.ID, pending.ID); err != nil || !ok {
					t.Fatalf("acknowledge interrupted approval %t: %v", ok, err)
				}
			}
			draft := editorDraft(t, e, "repo", map[string]*string{config.FieldCommitFormat: editorString("conventional")})
			review, err := e.Review(ctx, draft)
			if err != nil {
				t.Fatalf("interrupted approval prevented review: %v", err)
			}
			approved, found, err := finishApprovedRevision(ctx, db)
			if scenario == "initial_pending" {
				if found || err == nil || !strings.Contains(err.Error(), "not approved") {
					t.Fatalf("interrupted first approval became usable: %d/%t/%v", approved.ID, found, err)
				}
			} else if err != nil || !found || approved.ID != baseline.ID {
				t.Fatalf("review changed approved settings: %d/%t/%v", approved.ID, found, err)
			}
			if scenario == "desired_changed_after_review" {
				concurrent, _ := finishConfigRevision(t, db, map[string]any{config.FieldCommitFormat: "imperative"}, sql.NullInt64{Int64: pending.ID, Valid: true}, false)
				if _, err := review.Save(ctx); err == nil || !strings.Contains(err.Error(), "review again") {
					t.Fatalf("stale reviewed target saved: %v", err)
				}
				runtime, err := state.RuntimeConfigActivationState(ctx, db)
				if err != nil || runtime.DesiredRevisionID.Int64 != concurrent.ID || runtime.AppliedRevisionID.Int64 != baseline.ID {
					t.Fatalf("stale save changed runtime: %+v/%v", runtime, err)
				}
				if freshDraft := editorDraft(t, e, "repo", nil); freshDraft.Generation != draft.Generation {
					t.Fatal("stale save wrote configuration")
				}
				return
			}
			result, err := review.Save(ctx)
			if err != nil || !strings.Contains(result, "Applied:") || strings.Contains(result, "could not apply") {
				t.Fatalf("explicit save did not recover approval: %q/%v", result, err)
			}
			runtime, err := state.RuntimeConfigActivationState(ctx, db)
			if err != nil || runtime.DesiredRevisionID.Int64 == pending.ID || runtime.AppliedRevisionID.Int64 != runtime.DesiredRevisionID.Int64 {
				t.Fatalf("runtime=%+v/%v", runtime, err)
			}
			approved, found, err = finishApprovedRevision(ctx, db)
			if err != nil || !found || approved.ID != runtime.AppliedRevisionID.Int64 {
				t.Fatalf("new approved settings=%d/%t/%v", approved.ID, found, err)
			}
			values, _, err := finishSnapshotValues(approved.SnapshotJSON)
			if err != nil || values[config.FieldCommitFormat] != "conventional" {
				t.Fatalf("approved style=%v/%v", values, err)
			}
			original, err := state.ConfigRevisionByID(ctx, db, pending.ID)
			if err != nil || original.SnapshotJSON != pending.SnapshotJSON || original.SnapshotHash != pending.SnapshotHash {
				t.Fatal("interrupted revision changed")
			}
			if ok, err := state.ApplyConfigActivation(ctx, db, request.ID, pending.ID); err != nil || ok {
				t.Fatalf("superseded request can still apply: %t/%v", ok, err)
			}
		})
	}
}

func TestFinishRemovalConfigEditorCredentialSaveDoesNotProbe(t *testing.T) {
	e := editorFixture(t)
	editorSeed(t, e, func(doc *config.Document) {
		doc.Settings.Global[config.FieldCommitStrategy] = json.RawMessage(`"intent"`)
		doc.Settings.Global[config.FieldProvider] = json.RawMessage(`"openai-compat"`)
	})
	if err := credentials.NewStore(e.roots).Set("old-test-key"); err != nil {
		t.Fatal(err)
	}
	e.probe = func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		t.Fatal("saving tested a connection")
		return ai.ProviderProbeResult{}, nil
	}
	draft := editorDraft(t, e, "global", map[string]*string{config.FieldModel: editorString("new-model")})
	draft.Credential = "new-test-key"
	review, err := e.Review(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(review.Text, "new-test-key") {
		t.Fatal("review leaked credential")
	}
	result, err := review.Save(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stored, err := credentials.NewStore(e.roots).Read()
	if err != nil || stored != "new-test-key" || strings.Contains(result, "new-test-key") {
		t.Fatalf("credential save failed or leaked: %v", err)
	}
}

func TestFinishRemovalConfigEditorLocalIgnoresBrokenCredentials(t *testing.T) {
	e := editorFixture(t)
	editorSeed(t, e, func(doc *config.Document) {
		doc.Settings.Global[config.FieldCommitStrategy] = json.RawMessage(`"local"`)
	})
	credentialPath := credentials.NewStore(e.roots).Path()
	if err := os.WriteFile(credentialPath, []byte("malformed protected credential"), 0600); err != nil {
		t.Fatal(err)
	}
	e.lookup = func(key string) (string, bool) {
		if key == ai.EnvAPIKey {
			return "invalid\nkey", true
		}
		return "", false
	}
	e.probe = func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		t.Fatal("Local probed provider")
		return ai.ProviderProbeResult{}, nil
	}
	draft := editorDraft(t, e, "global", map[string]*string{config.FieldCommitFormat: editorString("conventional")})
	review, err := e.Review(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = review.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(credentialPath)
	if err != nil || string(body) != "malformed protected credential" {
		t.Fatal("Local modified unrelated credential")
	}
}

func TestFinishRemovalConfigEditorRejectsStaleReviewAndEnvironmentKey(t *testing.T) {
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

func TestFinishRemovalConfigEditorPublicRoutesAndOutsideRepository(t *testing.T) {
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

func TestFinishRemovalConfigEditorGlobalSaveUpdatesInheritingRuntimeOnly(t *testing.T) {
	for _, scenario := range []string{"editor_change", "saved_cli_set", "saved_cli_reset"} {
		t.Run(scenario, func(t *testing.T) { checkEditorGlobalActivation(t, scenario) })
	}
}
func checkEditorGlobalActivation(t *testing.T, scenario string) {
	ctx := context.Background()
	e := editorFixture(t)
	e.repo = materializeTestRepo(t, false)
	overrideRepo := materializeTestRepo(t, false)
	overrideDB := filepath.Join(overrideRepo, ".git", "acd", "state.db")
	beforeOverride, _ := os.ReadFile(overrideDB)
	editorSeed(t, e, func(doc *config.Document) {
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"old-model"`)
		doc.Settings.Repositories[central.CanonicalID(overrideRepo)] = config.RepositorySettings{Fields: config.Overrides{config.FieldModel: json.RawMessage(`"custom-model"`)}}
	})
	// Establish the inheriting repository's applied runtime, then add an unrelated
	// saved draft to prove a global model edit does not activate that draft.
	service, err := settings.NewService(ctx, e.options(e.repo, ""))
	if err != nil {
		t.Fatal(err)
	}
	validation, err := service.Validate(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := service.Apply(ctx, settings.ApplyRequest{TestedFingerprint: validation.Fingerprint, Confirmations: validation.Confirmations, ExpectedGeneration: validation.SourceGeneration})
	if err != nil {
		t.Fatal(err)
	}
	service.Close()
	dbPath := filepath.Join(e.repo, ".git", "acd", "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	editorSeed(t, e, func(doc *config.Document) {
		doc.Settings.Repositories[central.CanonicalID(e.repo)] = config.RepositorySettings{Fields: config.Overrides{config.FieldCommitFormat: json.RawMessage(`"conventional"`)}}
	})
	registry := &central.Registry{Version: central.RegistryVersion, Repos: []central.RepoRecord{
		{Path: e.repo, RepoHash: central.CanonicalID(e.repo), StateDB: dbPath, LifecycleState: central.RepoLifecycleEnabled},
		{Path: overrideRepo, RepoHash: central.CanonicalID(overrideRepo), StateDB: filepath.Join(overrideRepo, ".git", "acd", "state.db"), LifecycleState: central.RepoLifecycleEnabled},
	}}
	if err := central.Save(e.roots, registry); err != nil {
		t.Fatal(err)
	}
	changes := map[string]*string{config.FieldModel: editorString("new-model")}
	expectedModel := "new-model"
	if scenario == "saved_cli_set" {
		editorSeed(t, e, func(doc *config.Document) { doc.Settings.Global[config.FieldModel] = json.RawMessage(`"new-model"`) })
		changes = nil
	} else if scenario == "saved_cli_reset" {
		editorSeed(t, e, func(doc *config.Document) { delete(doc.Settings.Global, config.FieldModel) })
		changes = nil
		definition, _ := config.LookupField(config.FieldModel)
		expectedModel = definition.Default
	}
	draft := editorDraft(t, e, "global", changes)
	review, err := e.Review(ctx, draft)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(review.Text, "Keeps repository/profile overrides") || !strings.Contains(review.Text, "Apply inherited changes") {
		t.Fatal(review.Text)
	}
	result, err := review.Save(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result, "could not apply") {
		t.Fatal(result)
	}
	runtime, err := state.RuntimeConfigActivationState(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.DesiredRevisionID.Int64 == baseline.RevisionID || runtime.AppliedRevisionID.Int64 != runtime.DesiredRevisionID.Int64 {
		t.Fatalf("runtime=%+v", runtime)
	}
	revision, err := state.ConfigRevisionByID(ctx, db, runtime.DesiredRevisionID.Int64)
	if err != nil {
		t.Fatal(err)
	}
	values, _, err := credentialRuntimeContract(revision.SnapshotJSON)
	if err != nil {
		t.Fatal(err)
	}
	if values[config.FieldModel] != expectedModel || values[config.FieldCommitFormat] != "imperative" {
		t.Fatalf("global change activated unrelated draft: %v", values)
	}
	if editorModel(t, e, "repo").Overridden {
		t.Fatal("global activation created repository model override")
	}
	scoped := *e
	scoped.repo = overrideRepo
	if editorModel(t, &scoped, "repo").Value != "custom-model" {
		t.Fatal("repository override changed")
	}
	afterOverride, _ := os.ReadFile(overrideDB)
	if !bytes.Equal(beforeOverride, afterOverride) {
		t.Fatal("overridden repository state changed")
	}
}

func TestFinishRemovalConfigEditorGlobalVerificationNeedsRepositoryScopeBeforeProbe(t *testing.T) {
	e := editorFixture(t)
	calls := 0
	e.probe = func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		calls++
		return ai.ProviderProbeResult{Success: true}, nil
	}
	draft := editorDraft(t, e, "global", map[string]*string{config.FieldIntentVerification: editorString("full"), config.FieldVerificationFullCommand: editorString("make test")})
	if _, err := e.Review(context.Background(), draft); err == nil || !strings.Contains(err.Error(), "repository") {
		t.Fatalf("err=%v", err)
	}
	if calls != 0 {
		t.Fatal("invalid scope reached provider")
	}
}

func TestFinishRemovalConfigEditorGlobalModelResetUsesCurrentDefault(t *testing.T) {
	e := editorFixture(t)
	editorSeed(t, e, func(doc *config.Document) { doc.Settings.Global[config.FieldModel] = json.RawMessage(`"custom-model"`) })
	before := editorModel(t, e, "global")
	if before.Value != "custom-model" || before.Inherited != "gpt-6-luna" {
		t.Fatalf("model reset choice = %+v", before)
	}
	review, err := e.Review(context.Background(), editorDraft(t, e, "global", map[string]*string{config.FieldModel: nil}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(review.Text, "gpt-6-luna") {
		t.Fatal(review.Text)
	}
	if _, err := review.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := editorModel(t, e, "global")
	if after.Value != "gpt-6-luna" || after.Overridden {
		t.Fatalf("reset model = %+v", after)
	}
}

func TestFinishRemovalLegacyConfigReadOnlyScopeParity(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	worktree, err := git.ResolveWorktree(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	repo = worktree.Root
	id := central.CanonicalID(repo)
	body := []byte(`{"version":1,"generation":7,"settings":{"global":{"commit.strategy":"intent","commit.preset":"balanced"},"profiles":{"quality":{"fields":{"commit.preset":"quality"}}},"repositories":{"` + id + `":{"profile":"quality","fields":{"commit.strategy":"event"}}}}}`)
	if err := os.MkdirAll(roots.Config, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ConfigPath(), body, 0600); err != nil {
		t.Fatal(err)
	}
	credentialPath := credentials.NewStore(roots).Path()
	if err := os.WriteFile(credentialPath, []byte("broken credential"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(ai.EnvAPIKey, "invalid\nkey")
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ scope, profile, want string }{{"global", "", "structural"}, {"profile", "quality", "full"}, {"repo", "", "none"}} {
		t.Run(tc.scope, func(t *testing.T) {
			target := resolvedConfigTarget{Document: doc, Scope: tc.scope, Profile: tc.profile, WorktreeID: id}
			input, _ := configInput(target)
			resolved, err := config.ResolveAll(input)
			if err != nil || resolved[config.FieldIntentVerification].Value != tc.want {
				t.Fatalf("legacy scope %s: %v err=%v", tc.scope, resolved, err)
			}
			var out bytes.Buffer
			argRepo := ""
			if tc.scope == "repo" {
				argRepo = repo
			}
			if err := runConfigGet(t.Context(), &out, argRepo, configScopeOptions{Scope: tc.scope, Profile: tc.profile}, config.FieldIntentVerification, true); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), `"value": "`+tc.want+`"`) {
				t.Fatal(out.String())
			}
		})
	}
	e := &configEditor{roots: roots, repo: repo, lookup: func(string) (string, bool) { return "", false }}
	projection, err := e.project("repo", nil)
	if err != nil || projection.values[config.FieldIntentVerification] != "none" {
		t.Fatalf("editor=%v err=%v", projection.values, err)
	}
	for _, f := range projection.fields {
		if f.Key == config.FieldIntentVerification && f.Inherited != "full" {
			t.Fatalf("inherited scope=%+v", f)
		}
	}
	after, err := os.ReadFile(roots.ConfigPath())
	if err != nil || !bytes.Equal(body, after) {
		t.Fatal("read rewrote legacy settings")
	}
	if _, err := os.Stat(filepath.Join(repo, ".git", "acd")); !os.IsNotExist(err) {
		t.Fatal("config read created state")
	}
	if _, err := os.Stat(roots.ConfigLockPath()); !os.IsNotExist(err) {
		t.Fatal("config read created lock")
	}
}

func TestFinishRemovalPublicHelpAndRetiredCommands(t *testing.T) {
	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "acd finish") || strings.Contains(out.String(), "commit-all  ") {
		t.Fatal(out.String())
	}
	for _, name := range []string{"start", "stop", "wake", "touch", "flush", "settings", "prompt", "pause", "resume"} {
		root := newRootCmd()
		command, remaining, err := root.Find([]string{name})
		if err != nil || command != root || len(remaining) != 1 || remaining[0] != name {
			t.Fatalf("retired command %s matched %v with args %v err=%v", name, command, remaining, err)
		}
	}
	command, remaining, err := newRootCmd().Find([]string{"internal", "session"})
	if err != nil || command.CommandPath() != "acd internal" || len(remaining) != 1 || remaining[0] != "session" {
		t.Fatalf("session protocol remains: command=%v args=%v err=%v", command, remaining, err)
	}

}

func TestFinishRemovalIntentSetupKeepsConnectionTestOptional(t *testing.T) {
	roots := withIsolatedHome(t)
	t.Setenv(ai.EnvAPIKey, "approved-test-key")
	old := openConfigureGlobalSettingsService
	t.Cleanup(func() { openConfigureGlobalSettingsService = old })
	openConfigureGlobalSettingsService = func(ctx context.Context, opts settings.Options) (configureGlobalSettingsService, error) {
		opts.Probe = func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
			t.Fatal("setup probed provider")
			return ai.ProviderProbeResult{}, nil
		}
		return settings.NewGlobalService(ctx, opts)
	}
	cmd := newSetupCommand(false)
	if err := cmd.Flags().Set("expect-plan", "reviewed-plan"); err != nil {
		t.Fatal(err)
	}
	state, err := prepareSetupOnboarding(cmd, roots, setupOnboardingOptions{Strategy: "intent", Provider: "openai-compat", ConfirmDiffEgress: true}, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if state.Configuration.ProviderTestStatus != "optional_connection_test" {
		t.Fatalf("test is not optional: %+v", state.Configuration)
	}
	if _, err := os.Stat(roots.ConfigPath()); !os.IsNotExist(err) {
		t.Fatal("preparation wrote settings")
	}
}

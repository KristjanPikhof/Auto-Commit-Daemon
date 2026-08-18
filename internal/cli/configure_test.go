package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

type configureFakeService struct {
	order       *[]string
	snapshot    settings.Snapshot
	saveErr     error
	applyErr    error
	providerErr error
	globalSave  func(settings.SaveGlobalSetupRequest)
}

func TestDryRunConfigureSelectionUsesProviderDefaultTimeout(t *testing.T) {
	t.Parallel()
	selection := dryRunConfigureSelection(
		"intent",
		"balanced",
		configureVerificationDetection{},
	)
	if selection.ProviderTimeout != ai.DefaultProviderTimeout.String() {
		t.Fatalf("provider timeout=%q want=%q",
			selection.ProviderTimeout, ai.DefaultProviderTimeout)
	}
}

func (f *configureFakeService) Close() error { return nil }
func (f *configureFakeService) AuthoringPreview() (settings.AuthoringPreview, error) {
	*f.order = append(*f.order, "authoring")
	values := map[string]string{}
	for _, field := range f.snapshot.Fields {
		values[field.Name] = field.DraftValue
	}
	return settings.AuthoringPreview{Values: values, Generation: f.snapshot.SavedGeneration}, nil
}
func (f *configureFakeService) Snapshot(context.Context, settings.Scope, string) (settings.Snapshot, error) {
	*f.order = append(*f.order, "snapshot")
	return f.snapshot, nil
}
func (f *configureFakeService) Save(context.Context, settings.SaveRequest) (settings.SaveResult, error) {
	*f.order = append(*f.order, "settings")
	if f.saveErr != nil {
		return settings.SaveResult{}, f.saveErr
	}
	return settings.SaveResult{Generation: f.snapshot.SavedGeneration + 1, Scope: settings.ScopeRepository}, nil
}
func (f *configureFakeService) SaveGlobalSetup(_ context.Context, req settings.SaveGlobalSetupRequest) (settings.SaveGlobalSetupResult, error) {
	*f.order = append(*f.order, "global_settings")
	if f.globalSave != nil {
		f.globalSave(req)
	}
	if f.saveErr != nil {
		return settings.SaveGlobalSetupResult{}, f.saveErr
	}
	return settings.SaveGlobalSetupResult{
		Generation:  req.ExpectedGeneration + 1,
		Fingerprint: req.TestedFingerprint,
	}, nil
}
func (f *configureFakeService) Validate(_ context.Context, draft map[string]string, confirmed []ai.ConfirmationRequirement) (settings.Validation, error) {
	*f.order = append(*f.order, "validate")
	strategy := config.CommitStrategy(draft[config.FieldCommitStrategy])
	presetName := config.PresetName(draft[config.FieldCommitPreset])
	preset, _ := config.LookupPreset(strategy, presetName)
	resolved := map[string]string{}
	for key, value := range preset.Values {
		resolved[key] = value
	}
	for key, value := range draft {
		resolved[key] = value
	}
	for _, field := range config.Catalog() {
		if _, ok := resolved[field.Name]; !ok {
			resolved[field.Name] = field.Default
		}
	}
	var required []ai.ConfirmationRequirement
	if resolved[config.FieldIntentVerification] == "fast" ||
		resolved[config.FieldIntentVerification] == "full" {
		required = append(required, ai.ConfirmationVerificationCommand)
	}
	if resolved[config.FieldIntentRepairEnabled] == "true" {
		required = append(required, ai.ConfirmationIntentRepair)
	}
	seen := map[ai.ConfirmationRequirement]bool{}
	for _, item := range confirmed {
		seen[item] = true
	}
	var missing []ai.ConfirmationRequirement
	for _, item := range required {
		if !seen[item] {
			missing = append(missing, item)
		}
	}
	return settings.Validation{
		Fingerprint: "reviewed-fingerprint", SourceGeneration: f.snapshot.SavedGeneration,
		ResolvedHot: resolved, Preset: config.PresetResolution{Definition: preset},
		Missing: missing, Confirmations: required,
	}, nil
}

func TestConfigureGlobalDryRunWorksOutsideRepository(t *testing.T) {
	t.Chdir(t.TempDir())
	withIsolatedHome(t)
	restoreConfigureFakes(t)
	called := false
	openConfigureGlobalSettingsService = func(context.Context, settings.Options) (configureGlobalSettingsService, error) {
		called = true
		return nil, errors.New("must not open")
	}
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		called = true
		return nil, errors.New("must not open")
	}
	openConfigureValidationService = func(context.Context, settings.Options) (configureValidationService, error) {
		called = true
		return nil, errors.New("must not open")
	}
	configureEnable = func(context.Context, io.Writer, string, bool) error {
		called = true
		return nil
	}

	out, _, err := executeConfigureCommand(
		t, "", "--dry-run", "--json",
	)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("global dry-run reached an operational seam")
	}
	var report configureReport
	if err := jsonUnmarshalStrict([]byte(out), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, out)
	}
	if report.Version != configureReportVersion ||
		report.Scope != "global" || report.Repo != "" ||
		report.Experience != "Everyday" ||
		report.Verification.Mode != "structural" ||
		report.Verification.Command != "" ||
		report.ExecutionMode != "global defaults only" ||
		report.Daemon != "not_started" ||
		report.RuntimeRevision != 0 {
		t.Fatalf("report=%+v", report)
	}

	out, _, err = executeConfigureCommand(
		t, "", "--strategy", "event", "--preset", "fast",
		"--dry-run", "--json",
	)
	if err != nil {
		t.Fatal(err)
	}
	report = configureReport{}
	if err := jsonUnmarshalStrict([]byte(out), &report); err != nil {
		t.Fatalf("decode Maximum Speed report: %v\n%s", err, out)
	}
	if report.Experience != "Maximum Speed" ||
		report.Strategy != "event" || report.Preset != "fast" ||
		report.Provider != "deterministic" ||
		report.Verification.Mode != "none" ||
		report.RepairEnabled {
		t.Fatalf("Maximum Speed report=%+v", report)
	}
}

func TestConfigureInheritDryRunUsesSavedGlobalDefaults(t *testing.T) {
	repo := materializeTestRepo(t, true)
	roots := withIsolatedHome(t)
	store := config.NewStore(roots)
	if err := store.UpdateExpected(0, func(doc *config.Document) error {
		doc.Settings.Global[config.FieldCommitStrategy] =
			json.RawMessage(`"event"`)
		doc.Settings.Global[config.FieldCommitPreset] =
			json.RawMessage(`"fast"`)
		doc.Settings.Global[config.FieldProvider] =
			json.RawMessage(`"deterministic"`)
		doc.Settings.Global[config.FieldTimeout] =
			json.RawMessage(`"5m"`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	out, _, err := executeConfigureCommand(
		t, "", "--repo", repo, "--inherit", "--dry-run", "--json",
	)
	if err != nil {
		t.Fatal(err)
	}
	var report configureReport
	if err := jsonUnmarshalStrict([]byte(out), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, out)
	}
	if report.ConfigurationAction != "inherit_global" ||
		report.Strategy != "event" || report.Preset != "fast" ||
		report.Provider != "deterministic" ||
		report.ProviderTimeout != "5m0s" ||
		report.ExecutionMode !=
			"remove repository override and activate inheritance" {
		t.Fatalf("inherit report=%+v", report)
	}
}

func TestConfigureInheritRemovesOverrideAndCreatesRevision(t *testing.T) {
	repo := materializeTestRepo(t, true)
	roots := withIsolatedHome(t)
	restoreConfigureFakes(t)
	repoHash := central.CanonicalID(repo)
	store := config.NewStore(roots)
	if err := store.UpdateExpected(0, func(doc *config.Document) error {
		doc.Settings.Global[config.FieldCommitStrategy] =
			json.RawMessage(`"event"`)
		doc.Settings.Global[config.FieldCommitPreset] =
			json.RawMessage(`"fast"`)
		doc.Settings.Global[config.FieldProvider] =
			json.RawMessage(`"deterministic"`)
		doc.Settings.Global[config.FieldIntentVerification] =
			json.RawMessage(`"none"`)
		doc.Settings.Repositories[repoHash] = config.RepositorySettings{
			Fields: config.Overrides{
				config.FieldTimeout: json.RawMessage(`"30s"`),
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runConfigureWizard = func(
		context.Context,
		settingsui.ConfigureWizardOptions,
	) (settingsui.ConfigureSelection, error) {
		t.Fatal("inheritance opened the repository setup wizard")
		return settingsui.ConfigureSelection{}, nil
	}
	confirmConfigurePreview = func(
		context.Context, io.Reader, io.Writer, bool,
		settingsui.ConfigurePreviewApprovalOptions,
	) (settingsui.ConfigurePreviewApproval, error) {
		return settingsui.ConfigurePreviewApproval{Apply: true}, nil
	}
	configureCredentialStatus = func(
		paths.Roots, func(string) (string, bool),
	) (credentials.Source, bool, error) {
		return credentials.SourceNone, false, nil
	}
	configureEnable = func(
		context.Context, io.Writer, string, bool,
	) error {
		return nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }

	out, _, err := executeConfigureCommand(
		t, "", "--repo", repo, "--inherit",
	)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Settings.Repositories[repoHash]; ok {
		t.Fatalf("repository override still exists: %+v",
			doc.Settings.Repositories[repoHash])
	}
	db, err := state.Open(
		context.Background(),
		state.DBPathFromGitDir(filepath.Join(repo, ".git")),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	runtime, err := state.RuntimeConfigActivationState(
		context.Background(), db,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.DesiredRevisionID.Valid ||
		!strings.Contains(out, "inheriting global defaults") {
		t.Fatalf("runtime=%+v output=%s", runtime, out)
	}
}

func TestClearRepositoryValuesCoversSensitiveOverrides(t *testing.T) {
	values := clearRepositoryValues()
	for _, name := range []string{
		"capture.sensitive_globs",
		"trace.prompt",
	} {
		if value, ok := values[name]; !ok || value != nil {
			t.Fatalf("clear value %q = %v, present=%v",
				name, value, ok)
		}
	}
}

func TestConfigureGlobalSavesOnlyReviewedDefaults(t *testing.T) {
	t.Chdir(t.TempDir())
	withIsolatedHome(t)
	restoreConfigureFakes(t)
	var order []string
	service := &configureFakeService{
		order: &order,
		snapshot: settings.Snapshot{
			SavedGeneration: 4,
			Fields: []settings.FieldSnapshot{
				{Name: config.FieldProvider, DraftValue: "openai-compat"},
				{Name: config.FieldModel, DraftValue: "model"},
				{Name: config.FieldBaseURL, DraftValue: ai.DefaultOpenAIBaseURL},
				{Name: config.FieldTimeout, DraftValue: "30s"},
				{Name: config.FieldCommitFormat, DraftValue: "imperative"},
			},
		},
	}
	service.globalSave = func(req settings.SaveGlobalSetupRequest) {
		if req.Values[config.FieldIntentVerification] != "structural" {
			t.Fatalf(
				"global verification=%q",
				req.Values[config.FieldIntentVerification],
			)
		}
		if strings.TrimSpace(
			req.Values[config.FieldVerificationFastCommand],
		) != "" ||
			strings.TrimSpace(
				req.Values[config.FieldVerificationFullCommand],
			) != "" {
			t.Fatalf("global setup saved a project command: %+v", req.Values)
		}
		for _, confirmation := range req.Confirmations {
			if confirmation == ai.ConfirmationVerificationCommand {
				t.Fatal("global setup saved repository command approval")
			}
		}
		if req.TestedFingerprint != "reviewed-fingerprint" ||
			req.ExpectedGeneration != 4 {
			t.Fatalf("save request=%+v", req)
		}
	}
	openConfigureGlobalSettingsService = func(_ context.Context, opts settings.Options) (configureGlobalSettingsService, error) {
		if opts.RepoPath != "" {
			t.Fatalf("global service received repo %q", opts.RepoPath)
		}
		return service, nil
	}
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		t.Fatal("global setup opened repository settings")
		return nil, nil
	}
	openConfigureValidationService = func(context.Context, settings.Options) (configureValidationService, error) {
		t.Fatal("global setup opened repository validation")
		return nil, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		order = append(order, "credential_status")
		return credentials.SourceEnvironment, true, nil
	}
	runConfigureWizard = func(_ context.Context, opts settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		order = append(order, "wizard")
		if opts.RepositoryScoped {
			t.Fatal("global wizard marked repository-scoped")
		}
		if opts.DetectedQuickCommand != "" ||
			opts.DetectedFullCommand != "" {
			t.Fatal("global wizard received project commands")
		}
		return settingsui.ConfigureSelection{
			Experience: "everyday", Strategy: "intent", Preset: "balanced",
			CommitFormat: "imperative", Provider: "openai-compat",
			Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", DiffContextApproved: true,
			VerificationMode: "structural",
		}, nil
	}
	confirmConfigurePreview = func(_ context.Context, _ io.Reader, _ io.Writer, _ bool, opts settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		order = append(order, "confirm")
		if !opts.Global {
			t.Fatal("global preview used repository confirmation")
		}
		if opts.VerificationCommand != "" {
			t.Fatalf("global preview requested command %q", opts.VerificationCommand)
		}
		return settingsui.ConfigurePreviewApproval{
			Apply: true, Repair: opts.RepairEnabled,
		}, nil
	}
	configureCredentialWrite = func(paths.Roots, string) error {
		t.Fatal("environment credential was rewritten")
		return nil
	}
	configureEnable = func(context.Context, io.Writer, string, bool) error {
		t.Fatal("global setup started a daemon")
		return nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }

	out, progress, err := executeConfigureCommand(t, "")
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := "credential_status,authoring,wizard,validate,confirm,validate,provider,global_settings"
	if got := strings.Join(order, ","); got != wantOrder {
		t.Fatalf("order=%s want=%s", got, wantOrder)
	}
	for _, want := range []string{
		"Scope: Global defaults",
		"Verification: structural",
		"Project tests: not configured",
		"Daemon: not started",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out+progress, "runtime revision") ||
		strings.Contains(out+progress, "validation job") {
		t.Fatalf("global setup created repository work:\n%s\n%s", out, progress)
	}
}

func TestConfigureGlobalProviderFailureWritesNothing(t *testing.T) {
	t.Chdir(t.TempDir())
	withIsolatedHome(t)
	restoreConfigureFakes(t)
	var order []string
	service := &configureFakeService{
		order:       &order,
		providerErr: errors.New("provider rejected synthetic request"),
		snapshot: settings.Snapshot{
			SavedGeneration: 1,
			Fields: []settings.FieldSnapshot{
				{Name: config.FieldProvider, DraftValue: "openai-compat"},
				{Name: config.FieldModel, DraftValue: "model"},
				{Name: config.FieldBaseURL, DraftValue: ai.DefaultOpenAIBaseURL},
				{Name: config.FieldTimeout, DraftValue: "30s"},
				{Name: config.FieldCommitFormat, DraftValue: "imperative"},
			},
		},
	}
	openConfigureGlobalSettingsService = func(context.Context, settings.Options) (configureGlobalSettingsService, error) {
		return service, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		return credentials.SourceNone, false, nil
	}
	runConfigureWizard = func(context.Context, settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		return settingsui.ConfigureSelection{
			Experience: "everyday", Strategy: "intent", Preset: "balanced",
			CommitFormat: "imperative", Provider: "openai-compat",
			Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", Credential: "staged-secret",
			DiffContextApproved: true, VerificationMode: "structural",
		}, nil
	}
	confirmConfigurePreview = func(_ context.Context, _ io.Reader, _ io.Writer, _ bool, opts settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		return settingsui.ConfigurePreviewApproval{
			Apply: true, Repair: opts.RepairEnabled,
		}, nil
	}
	configureCredentialWrite = func(paths.Roots, string) error {
		t.Fatal("provider failure wrote credential")
		return nil
	}
	configureEnable = func(context.Context, io.Writer, string, bool) error {
		t.Fatal("provider failure started daemon")
		return nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }

	_, _, err := executeConfigureCommand(t, "")
	if err == nil ||
		!strings.Contains(err.Error(), "no configuration was changed") {
		t.Fatalf("err=%v", err)
	}
	if got := strings.Join(order, ","); got !=
		"authoring,validate,validate,provider" {
		t.Fatalf("order=%s", got)
	}
}

func TestConfigureGlobalRejectsStrictReview(t *testing.T) {
	t.Chdir(t.TempDir())
	withIsolatedHome(t)
	_, _, err := executeConfigureCommand(
		t, "", "--strategy", "intent", "--preset", "quality", "--dry-run",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "requires an explicit --repo") {
		t.Fatalf("err=%v", err)
	}
}
func (f *configureFakeService) TestProvider(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.ProviderTestResult, error) {
	*f.order = append(*f.order, "provider")
	if f.providerErr != nil {
		return settings.ProviderTestResult{}, f.providerErr
	}
	return settings.ProviderTestResult{Fingerprint: "reviewed-fingerprint", Provider: "openai-compat", Success: true}, nil
}
func (f *configureFakeService) Apply(_ context.Context, req settings.ApplyRequest) (settings.ApplyResult, error) {
	*f.order = append(*f.order, "revision")
	if f.applyErr != nil {
		return settings.ApplyResult{}, f.applyErr
	}
	result := settings.ApplyResult{RevisionID: 42, RequestID: 7, Queued: true}
	if req.SetupValidation != nil {
		result.ValidationRunID = 9
		result.ValidationStatus = state.ConfigValidationQueued
	}
	return result, nil
}
func (f *configureFakeService) Revert(context.Context, settings.RevertRequest) (settings.ApplyResult, error) {
	return settings.ApplyResult{}, errors.New("not used")
}

func executeConfigureCommand(t *testing.T, input string, args ...string) (string, string, error) {
	t.Helper()
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetIn(strings.NewReader(input))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"configure"}, args...))
	err := root.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}

func TestConfigureDryRunJSONHasNoOperationalSideEffects(t *testing.T) {
	repo := materializeTestRepo(t, true)
	withIsolatedHome(t)
	oldOpen, oldWizard, oldConfirm := openConfigureSettingsService, runConfigureWizard, confirmConfigurePreview
	oldStatus, oldWrite := configureCredentialStatus, configureCredentialWrite
	oldEnable := configureEnable
	defer func() {
		openConfigureSettingsService, runConfigureWizard, confirmConfigurePreview = oldOpen, oldWizard, oldConfirm
		configureCredentialStatus, configureCredentialWrite = oldStatus, oldWrite
		configureEnable = oldEnable
	}()
	called := false
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		called = true
		return nil, errors.New("must not open")
	}
	runConfigureWizard = func(context.Context, settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		called = true
		return settingsui.ConfigureSelection{}, nil
	}
	confirmConfigurePreview = func(context.Context, io.Reader, io.Writer, bool, settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		called = true
		return settingsui.ConfigurePreviewApproval{}, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		called = true
		return credentials.SourceNone, false, nil
	}
	configureCredentialWrite = func(paths.Roots, string) error { called = true; return nil }
	configureEnable = func(context.Context, io.Writer, string, bool) error { called = true; return nil }

	out, _, err := executeConfigureCommand(t, "", "--repo", repo, "--strategy", "intent",
		"--preset", "balanced", "--dry-run", "--json")
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("dry-run reached an operational seam")
	}
	var report configureReport
	if err := jsonUnmarshalStrict([]byte(out), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, out)
	}
	if !report.DryRun || report.Version != configureReportVersion ||
		report.Scope != "repository" ||
		report.Experience != "Everyday" ||
		report.PresetID != "intent.balanced" ||
		report.Verification.Mode != "structural" ||
		report.Verification.Command != "" ||
		report.Verification.CommandSource != "" ||
		report.Verification.Status != "internal_only" ||
		report.Daemon != "unchanged" || len(report.Risks) != 2 ||
		report.HarnessGuidance == nil {
		t.Fatalf("report=%+v", report)
	}
}

func TestConfigureApplyOrderCreatesOneRevisionAndOnlyReportsHarness(t *testing.T) {
	repo := materializeTestRepo(t, true)
	withIsolatedHome(t)
	var order []string
	service := &configureFakeService{order: &order, snapshot: settings.Snapshot{
		SavedGeneration: 7,
		Fields: []settings.FieldSnapshot{
			{Name: config.FieldProvider, DraftValue: "deterministic"},
			{Name: config.FieldModel, DraftValue: "gpt-5.4-mini"},
			{Name: config.FieldBaseURL, DraftValue: ai.DefaultOpenAIBaseURL},
			{Name: config.FieldTimeout, DraftValue: "30s"},
			{Name: config.FieldCommitFormat, DraftValue: "imperative"},
		},
	}}
	restoreConfigureFakes(t)
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		return service, nil
	}
	openConfigureValidationService = func(context.Context, settings.Options) (configureValidationService, error) {
		return service, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		order = append(order, "credential_status")
		return credentials.SourceNone, false, nil
	}
	runConfigureWizard = func(context.Context, settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		order = append(order, "wizard")
		return settingsui.ConfigureSelection{
			Strategy: "intent", Preset: "quality", CommitFormat: "imperative",
			Provider: "openai-compat", Model: "gpt-5.4-mini",
			BaseURL: ai.DefaultOpenAIBaseURL, ProviderTimeout: "30s",
			Credential: "staged-secret", DiffContextApproved: true,
			VerificationMode: "full", VerificationCommand: "make test",
			VerificationApproved: true,
		}, nil
	}
	confirmConfigurePreview = func(_ context.Context, _ io.Reader, _ io.Writer, _ bool, opts settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		order = append(order, "confirm")
		return settingsui.ConfigurePreviewApproval{
			Verification: opts.VerificationMode != "none",
			Repair:       opts.RepairEnabled,
			Apply:        true,
		}, nil
	}
	configureCredentialWrite = func(_ paths.Roots, secret string) error {
		order = append(order, "credential")
		if secret != "staged-secret" {
			t.Fatal("credential changed")
		}
		return nil
	}
	configureEnable = func(context.Context, io.Writer, string, bool) error {
		order = append(order, "on")
		return nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }

	out, progress, err := executeConfigureCommand(t, "", "--repo", repo)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"credential_status", "authoring", "wizard", "validate", "confirm",
		"validate", "snapshot", "validate", "provider",
		"credential", "settings", "revision", "on",
	}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order=%v want=%v", order, want)
	}
	if strings.Count(strings.Join(order, ","), "revision") != 1 {
		t.Fatalf("revision count in %v", order)
	}
	if !strings.Contains(out, "Configuration saved.") ||
		!strings.Contains(out, "Commit publishing: waiting for full validation") ||
		!strings.Contains(out, "Safe to close this terminal.") ||
		!strings.Contains(out, "no external hook file will be edited") ||
		!strings.Contains(out, "repository command will run in an ephemeral worktree: make test") ||
		!strings.Contains(out, "eligible recent ACD-owned commits may be repaired automatically") ||
		!strings.Contains(out, "`acd setup codex`") {
		t.Fatalf("output=%s", out)
	}
	progressLines := []string{
		"Applying reviewed configuration...",
		"[1/6] Testing provider with synthetic content...",
		"[1/6] Provider test passed.",
		"[2/6] Background validation target prepared.",
		"[3/6] Storing protected credential...",
		"[3/6] Protected credential stored.",
		"[4/6] Saving repository settings...",
		"[4/6] Repository settings saved.",
		"[5/6] Creating immutable runtime revision...",
		"[5/6] Runtime revision 42 created; validation job 9 queued.",
		"[6/6] Enabling ACD...",
		"[6/6] ACD enabled.",
	}
	at := 0
	for _, want := range progressLines {
		next := strings.Index(progress[at:], want)
		if next < 0 {
			t.Fatalf("progress missing %q after byte %d:\n%s", want, at, progress)
		}
		at += next + len(want)
	}
	if strings.Contains(progress, "staged-secret") {
		t.Fatalf("progress leaked credential: %q", progress)
	}
}

func TestConfigureDeclinedPreviewStopsBeforeCallsAndWrites(t *testing.T) {
	repo := materializeTestRepo(t, true)
	withIsolatedHome(t)
	var order []string
	service := &configureFakeService{order: &order, snapshot: settings.Snapshot{}}
	restoreConfigureFakes(t)
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		return service, nil
	}
	openConfigureValidationService = func(context.Context, settings.Options) (configureValidationService, error) {
		return service, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		return credentials.SourceEnvironment, true, nil
	}
	runConfigureWizard = func(context.Context, settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		return settingsui.ConfigureSelection{
			Strategy: "intent", Preset: "balanced", CommitFormat: "imperative",
			Provider: "openai-compat", Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", DiffContextApproved: true, VerificationMode: "structural",
		}, nil
	}
	confirmConfigurePreview = func(context.Context, io.Reader, io.Writer, bool, settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		return settingsui.ConfigurePreviewApproval{}, nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }
	_, _, err := executeConfigureCommand(t, "", "--repo", repo)
	if err == nil || !strings.Contains(err.Error(), "no provider call, command, or write") {
		t.Fatalf("err=%v", err)
	}
	if strings.Join(order, ",") != "authoring,validate" {
		t.Fatalf("decline performed operations: %v", order)
	}
}

func TestConfigureDeclineDoesNotCreateConfigOrState(t *testing.T) {
	repo := materializeTestRepo(t, true)
	if err := os.RemoveAll(filepath.Join(repo, ".git", "acd")); err != nil {
		t.Fatal(err)
	}
	roots := withIsolatedHome(t)
	restoreConfigureFakes(t)
	runConfigureWizard = func(context.Context, settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		return settingsui.ConfigureSelection{
			Strategy: "event", Preset: "fast", CommitFormat: "imperative",
			Provider: "deterministic", Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", VerificationMode: "none",
		}, nil
	}
	confirmConfigurePreview = func(context.Context, io.Reader, io.Writer, bool, settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		return settingsui.ConfigurePreviewApproval{}, nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }

	_, _, err := executeConfigureCommand(t, "", "--repo", repo, "--strategy", "event", "--preset", "fast")
	if err == nil || !strings.Contains(err.Error(), "final preview declined") {
		t.Fatalf("err=%v", err)
	}
	for _, path := range []string{
		filepath.Join(repo, ".git", "acd", "state.db"),
		roots.ConfigPath(),
		roots.ConfigLockPath(),
	} {
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("decline created %s: %v", path, statErr)
		}
	}
}

func TestConfigureQueuesVerificationWithoutBlockingSetup(t *testing.T) {
	repo := materializeTestRepo(t, true)
	withIsolatedHome(t)
	var order []string
	service := &configureFakeService{order: &order, snapshot: settings.Snapshot{
		SavedGeneration: 3,
		Fields: []settings.FieldSnapshot{
			{Name: config.FieldProvider, DraftValue: "openai-compat"},
			{Name: config.FieldModel, DraftValue: "model"},
			{Name: config.FieldBaseURL, DraftValue: ai.DefaultOpenAIBaseURL},
			{Name: config.FieldTimeout, DraftValue: "30s"},
			{Name: config.FieldCommitFormat, DraftValue: "imperative"},
		},
	}}
	restoreConfigureFakes(t)
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		return service, nil
	}
	openConfigureValidationService = func(context.Context, settings.Options) (configureValidationService, error) {
		return service, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		return credentials.SourceEnvironment, true, nil
	}
	runConfigureWizard = func(context.Context, settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		return settingsui.ConfigureSelection{
			Strategy: "intent", Preset: "quality", CommitFormat: "imperative",
			Provider: "openai-compat", Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", DiffContextApproved: true,
			VerificationMode: "full", VerificationCommand: "make test",
			VerificationApproved: true,
		}, nil
	}
	confirmConfigurePreview = func(_ context.Context, _ io.Reader, _ io.Writer, _ bool, opts settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		return settingsui.ConfigurePreviewApproval{
			Verification: opts.VerificationMode != "none",
			Repair:       opts.RepairEnabled,
			Apply:        true,
		}, nil
	}
	configureCredentialWrite = func(paths.Roots, string) error {
		order = append(order, "credential")
		return nil
	}
	configureEnable = func(context.Context, io.Writer, string, bool) error {
		order = append(order, "on")
		return nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }
	out, progress, err := executeConfigureCommand(t, "", "--repo", repo)
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	for _, want := range []string{
		"[1/6] Testing provider with synthetic content...",
		"[1/6] Provider test passed.",
		"[2/6] Background validation target prepared.",
		"validation job 9 queued",
	} {
		if !strings.Contains(progress+out, want) {
			t.Errorf("output missing %q:\n%s\n%s", want, progress, out)
		}
	}
	got := strings.Join(order, ",")
	if got != "authoring,validate,validate,snapshot,validate,provider,settings,revision,on" {
		t.Fatalf("order=%v", order)
	}
}

func TestConfigureRepositoryEverydaySkipsProjectValidation(t *testing.T) {
	repo := materializeTestRepo(t, true)
	withIsolatedHome(t)
	var order []string
	service := &configureFakeService{
		order: &order,
		snapshot: settings.Snapshot{
			SavedGeneration: 3,
			Fields: []settings.FieldSnapshot{
				{Name: config.FieldProvider, DraftValue: "openai-compat"},
				{Name: config.FieldModel, DraftValue: "model"},
				{Name: config.FieldBaseURL, DraftValue: ai.DefaultOpenAIBaseURL},
				{Name: config.FieldTimeout, DraftValue: "30s"},
				{Name: config.FieldCommitFormat, DraftValue: "imperative"},
			},
		},
	}
	restoreConfigureFakes(t)
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		return service, nil
	}
	openConfigureValidationService = func(context.Context, settings.Options) (configureValidationService, error) {
		return service, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		return credentials.SourceEnvironment, true, nil
	}
	runConfigureWizard = func(_ context.Context, opts settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		if !opts.RepositoryScoped {
			t.Fatal("repository wizard was not scoped")
		}
		return settingsui.ConfigureSelection{
			Experience: "everyday", Strategy: "intent", Preset: "balanced",
			CommitFormat: "imperative", Provider: "openai-compat",
			Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", DiffContextApproved: true,
			VerificationMode: "structural",
		}, nil
	}
	confirmConfigurePreview = func(_ context.Context, _ io.Reader, _ io.Writer, _ bool, opts settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		if opts.VerificationCommand != "" {
			t.Fatalf("Everyday requested project command %q", opts.VerificationCommand)
		}
		return settingsui.ConfigurePreviewApproval{
			Apply: true, Repair: opts.RepairEnabled,
		}, nil
	}
	configureEnable = func(context.Context, io.Writer, string, bool) error {
		order = append(order, "on")
		return nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }

	out, progress, err := executeConfigureCommand(t, "", "--repo", repo)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out+progress, "validation job") ||
		strings.Contains(out+progress, "Background validation target") ||
		strings.Contains(out+progress, "make test") {
		t.Fatalf("Everyday queued project validation:\n%s\n%s", out, progress)
	}
	if !strings.Contains(out, "Verification: structural") ||
		!strings.Contains(out, "Configuration ready:") {
		t.Fatalf("output=%s", out)
	}
	if got := strings.Join(order, ","); got !=
		"authoring,validate,validate,snapshot,validate,provider,settings,revision,on" {
		t.Fatalf("order=%s", got)
	}
}

func TestConfigureCredentialStdinLeavesWizardInputAndNeverRendersSecret(t *testing.T) {
	secret, rest, err := readConfigureCredentialLine(strings.NewReader("staged-secret\nnext-answer\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer clearBytes(secret)
	if string(secret) != "staged-secret" {
		t.Fatalf("secret=%q", secret)
	}
	body, err := io.ReadAll(rest)
	if err != nil || string(body) != "next-answer\n" {
		t.Fatalf("remaining=%q err=%v", body, err)
	}
}

func TestConfigureEnvironmentCredentialRetainsPriority(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "environment-secret")
	lookup := configureLookupEnv("protected-file-secret")
	value, set := lookup(ai.EnvAPIKey)
	if !set || value != "environment-secret" {
		t.Fatalf("API key=%q set=%t", value, set)
	}
	t.Setenv(ai.EnvAPIKey, "")
	value, set = configureLookupEnv("protected-file-secret")(ai.EnvAPIKey)
	if !set || value != "protected-file-secret" {
		t.Fatalf("staged API key=%q set=%t", value, set)
	}
}

func TestDetectVerificationCommandPrefersMakeTarget(t *testing.T) {
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "Makefile"),
		[]byte(".PHONY: test\n\ntest:\n\tgo test ./...\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "go.mod"),
		[]byte("module example.test/repo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := detectVerificationCommand(repo); got != "make test" {
		t.Fatalf("detected command=%q", got)
	}
}

func TestDetectVerificationCommandUsesGoFallback(t *testing.T) {
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "go.mod"),
		[]byte("module example.test/repo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := detectVerificationCommand(repo); got != "go test ./..." {
		t.Fatalf("detected command=%q", got)
	}
}

func TestDetectVerificationCommandsSeparatesQuickAndFull(t *testing.T) {
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "Makefile"), []byte(
		"verify-fast:\n\tgo test ./... -run '^$'\n\n"+
			"test:\n\tgo test ./...\n",
	), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "go.mod"),
		[]byte("module example.test/repo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got := detectVerificationCommands(repo)
	if got.QuickCommand != "make verify-fast" ||
		got.QuickSource != "Makefile target verify-fast" ||
		got.FullCommand != "make test" ||
		got.FullSource != "Makefile target test" {
		t.Fatalf("detected=%+v", got)
	}
}

func TestDetectVerificationManifestWinsWithProvenance(t *testing.T) {
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "acd.verification.json"),
		[]byte(`{"version":1,"quick":"./check quick","full":"./check full"}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "Makefile"),
		[]byte("test:\n\tfalse\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got := detectVerificationCommands(repo)
	if got.QuickCommand != "./check quick" ||
		got.FullCommand != "./check full" ||
		got.QuickSource != "repository ACD verification manifest" ||
		got.FullSource != "repository ACD verification manifest" {
		t.Fatalf("detected=%+v", got)
	}
}

func TestConfigureWizardErrorIsReturnedWithoutOperationalCalls(t *testing.T) {
	repo := materializeTestRepo(t, true)
	withIsolatedHome(t)
	var order []string
	service := &configureFakeService{order: &order}
	restoreConfigureFakes(t)
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		return service, nil
	}
	openConfigureValidationService = func(context.Context, settings.Options) (configureValidationService, error) {
		return service, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		return credentials.SourceEnvironment, true, nil
	}
	runConfigureWizard = func(context.Context, settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		return settingsui.ConfigureSelection{}, errors.New("terminal restored: interrupted")
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }
	_, _, err := executeConfigureCommand(t, "", "--repo", repo)
	if err == nil || !strings.Contains(err.Error(), "terminal restored: interrupted") {
		t.Fatalf("err=%v", err)
	}
	if strings.Join(order, ",") != "authoring" {
		t.Fatalf("unexpected operations=%v", order)
	}
}

func TestConfigureRiskPreviewNamesCustomEndpointAndExactCommand(t *testing.T) {
	preset, _ := config.LookupPreset(config.StrategyIntent, config.PresetQuality)
	risks := configureRisks(settingsui.ConfigureSelection{
		Provider: "openai-compat", BaseURL: "https://provider.example/v1",
		VerificationMode: "full", VerificationCommand: "make test",
	}, preset)
	joined := strings.Join(risks, "\n")
	for _, want := range []string{
		"openai-compat", "https://provider.example/v1", "make test",
		"repaired automatically",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("risks missing %q:\n%s", want, joined)
		}
	}
}

func TestConfigureResolvedPreviewShowsRetainedCustomization(t *testing.T) {
	preset, _ := config.LookupPreset(config.StrategyIntent, config.PresetBalanced)
	values := map[string]string{}
	for key, value := range preset.Values {
		values[key] = value
	}
	values[config.FieldCommitStrategy] = "intent"
	values[config.FieldCommitPreset] = "balanced"
	values[config.FieldCommitFormat] = "imperative"
	values[config.FieldProvider] = "openai-compat"
	values[config.FieldModel] = "model"
	values[config.FieldBaseURL] = ai.DefaultOpenAIBaseURL
	values[config.FieldTimeout] = "30s"
	values[config.FieldIntentWindow] = "17"
	values[config.FieldIntentRepairEnabled] = "false"
	values[config.FieldVerificationFastCommand] = "go test ./internal/... \nignored"
	validation := settings.Validation{
		ResolvedHot: values,
		Preset:      config.PresetResolution{Definition: preset, Customized: true},
	}
	report, err := buildResolvedConfigureReport("/repo", settingsui.ConfigureSelection{}, credentials.SourceEnvironment, false, validation)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Customized || report.RepairEnabled ||
		report.Verification.Mode != "structural" ||
		report.Verification.Command != "" ||
		strings.Contains(strings.Join(report.Risks, "\n"), "\nignored") {
		t.Fatalf("report=%+v", report)
	}
}

func TestConfigureDeterministicProviderContract(t *testing.T) {
	base := settingsui.ConfigureSelection{
		CommitFormat: "imperative", Provider: "deterministic",
		Model: "model", BaseURL: ai.DefaultOpenAIBaseURL, ProviderTimeout: "30s",
		VerificationMode: "none",
	}
	for _, mode := range [][2]string{{"event", "fast"}, {"intent", "fast"}, {"intent", "balanced"}} {
		candidate := base
		candidate.Strategy, candidate.Preset = mode[0], mode[1]
		if err := validateConfigureSelection(candidate, true); err != nil {
			t.Fatalf("%s.%s err=%v", mode[0], mode[1], err)
		}
	}
	base.Strategy, base.Preset = "intent", "quality"
	if err := validateConfigureSelection(base, true); err == nil ||
		!strings.Contains(err.Error(), "Strict Review requires a semantic provider") {
		t.Fatalf("Intent Quality err=%v", err)
	}
}

func TestConfigureApplyFailureReportsStagesAndRestoresCredential(t *testing.T) {
	repo := materializeTestRepo(t, true)
	withIsolatedHome(t)
	var order []string
	service := &configureFakeService{order: &order, applyErr: errors.New("activation unavailable"),
		snapshot: settings.Snapshot{SavedGeneration: 2}}
	restoreConfigureFakes(t)
	openConfigureValidationService = func(context.Context, settings.Options) (configureValidationService, error) {
		return service, nil
	}
	openConfigureSettingsService = func(context.Context, settings.Options) (settingsCLIService, error) {
		return service, nil
	}
	configureCredentialStatus = func(paths.Roots, func(string) (string, bool)) (credentials.Source, bool, error) {
		return credentials.SourceNone, false, nil
	}
	runConfigureWizard = func(context.Context, settingsui.ConfigureWizardOptions) (settingsui.ConfigureSelection, error) {
		return settingsui.ConfigureSelection{
			Strategy: "event", Preset: "fast", CommitFormat: "imperative",
			Provider: "deterministic", Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", Credential: "new-secret", VerificationMode: "none",
		}, nil
	}
	confirmConfigurePreview = func(context.Context, io.Reader, io.Writer, bool, settingsui.ConfigurePreviewApprovalOptions) (settingsui.ConfigurePreviewApproval, error) {
		return settingsui.ConfigurePreviewApproval{Apply: true}, nil
	}
	configureCredentialRead = func(paths.Roots) (string, error) { return "old-secret", nil }
	var credentialWrites []string
	configureCredentialWrite = func(_ paths.Roots, secret string) error {
		credentialWrites = append(credentialWrites, secret)
		return nil
	}
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }

	_, _, err := executeConfigureCommand(t, "", "--repo", repo, "--strategy", "event", "--preset", "fast")
	if err == nil || !strings.Contains(err.Error(), "completed stages: provider_test:passed") ||
		!strings.Contains(err.Error(), "settings:saved") ||
		!strings.Contains(err.Error(), "protected credential restored") ||
		!strings.Contains(err.Error(), "saved but not activated") {
		t.Fatalf("err=%v", err)
	}
	if strings.Join(credentialWrites, ",") != "new-secret,old-secret" {
		t.Fatalf("credential writes=%v", credentialWrites)
	}
}

func TestConfigureHelpAndInvalidFlags(t *testing.T) {
	help := commandHelp(t, "configure")
	for _, want := range []string{
		"--accessible", "--strategy", "--preset", "--credential-stdin",
		"--dry-run", "--wait", "--replace", "--inherit", "Everyday work",
		"repository Strict Review",
		"edits harness hook",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("help missing %q", want)
		}
	}
	repo := materializeTestRepo(t, true)
	for _, args := range [][]string{
		{"--repo", repo, "--strategy", "batch", "--dry-run"},
		{"--repo", repo, "--preset", "huge", "--dry-run"},
		{"--repo", repo, "--json"},
		{"--repo", repo, "--dry-run", "--credential-stdin"},
		{"--repo", repo, "--dry-run", "--wait"},
		{"--replace", "--repo", repo, "--dry-run"},
		{"--inherit", "--dry-run"},
		{"--repo", repo, "--inherit", "--strategy", "intent", "--dry-run"},
	} {
		if _, _, err := executeConfigureCommand(t, "", args...); err == nil {
			t.Fatalf("args %v succeeded", args)
		}
	}
}

func restoreConfigureFakes(t *testing.T) {
	t.Helper()
	oldOpen, oldWizard, oldConfirm := openConfigureSettingsService, runConfigureWizard, confirmConfigurePreview
	oldPreview := openConfigureValidationService
	oldGlobal := openConfigureGlobalSettingsService
	oldStatus, oldWrite := configureCredentialStatus, configureCredentialWrite
	oldRead, oldRemove := configureCredentialRead, configureCredentialRemove
	oldEnable := configureEnable
	oldIn, oldOut := settingsInputTTY, settingsOutputTTY
	t.Cleanup(func() {
		openConfigureSettingsService, runConfigureWizard, confirmConfigurePreview = oldOpen, oldWizard, oldConfirm
		openConfigureValidationService = oldPreview
		openConfigureGlobalSettingsService = oldGlobal
		configureCredentialStatus, configureCredentialWrite = oldStatus, oldWrite
		configureCredentialRead, configureCredentialRemove = oldRead, oldRemove
		configureEnable = oldEnable
		settingsInputTTY, settingsOutputTTY = oldIn, oldOut
	})
}

func jsonUnmarshalStrict(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

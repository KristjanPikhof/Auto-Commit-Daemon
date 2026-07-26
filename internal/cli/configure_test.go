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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

type configureFakeService struct {
	order    *[]string
	snapshot settings.Snapshot
	saveErr  error
	applyErr error
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
	if resolved[config.FieldIntentVerification] != "none" {
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
func (f *configureFakeService) TestProvider(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.ProviderTestResult, error) {
	*f.order = append(*f.order, "provider")
	return settings.ProviderTestResult{Fingerprint: "reviewed-fingerprint", Provider: "openai-compat", Success: true}, nil
}
func (f *configureFakeService) Apply(context.Context, settings.ApplyRequest) (settings.ApplyResult, error) {
	*f.order = append(*f.order, "revision")
	if f.applyErr != nil {
		return settings.ApplyResult{}, f.applyErr
	}
	return settings.ApplyResult{RevisionID: 42, Queued: true}, nil
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
	oldVerify, oldEnable := configureRunVerification, configureEnable
	defer func() {
		openConfigureSettingsService, runConfigureWizard, confirmConfigurePreview = oldOpen, oldWizard, oldConfirm
		configureCredentialStatus, configureCredentialWrite = oldStatus, oldWrite
		configureRunVerification, configureEnable = oldVerify, oldEnable
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
	configureRunVerification = func(context.Context, string, string, string, string, string) (verification.Result, error) {
		called = true
		return verification.Result{}, nil
	}
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
	if !report.DryRun || report.PresetID != "intent.balanced" ||
		report.Verification.Mode != "fast" || report.Verification.Status != "approval_required" ||
		report.Daemon != "unchanged" || len(report.Risks) != 3 ||
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
			Strategy: "intent", Preset: "balanced", CommitFormat: "imperative",
			Provider: "openai-compat", Model: "gpt-5.4-mini",
			BaseURL: ai.DefaultOpenAIBaseURL, ProviderTimeout: "30s",
			Credential: "staged-secret", DiffContextApproved: true,
			VerificationMode: "fast", VerificationCommand: "make test",
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
	configureRunVerification = func(_ context.Context, _, mode, command, timeout, _ string) (verification.Result, error) {
		order = append(order, "verification")
		if mode != "fast" || command != "make test" || timeout != "2m" {
			t.Fatalf("verification=%q %q %q", mode, command, timeout)
		}
		return verification.Result{Status: verification.StatusPassed}, nil
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

	out, _, err := executeConfigureCommand(t, "", "--repo", repo)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"credential_status", "authoring", "wizard", "validate", "confirm",
		"validate", "snapshot", "validate", "provider", "verification",
		"credential", "settings", "revision", "on",
	}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order=%v want=%v", order, want)
	}
	if strings.Count(strings.Join(order, ","), "revision") != 1 {
		t.Fatalf("revision count in %v", order)
	}
	if !strings.Contains(out, "runtime revision 42") ||
		!strings.Contains(out, "no external hook file will be edited") ||
		!strings.Contains(out, "repository command will run in an ephemeral worktree: make test") ||
		!strings.Contains(out, "eligible recent ACD-owned commits may be repaired automatically") ||
		!strings.Contains(out, "`acd setup codex`") {
		t.Fatalf("output=%s", out)
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
			Strategy: "intent", Preset: "fast", CommitFormat: "imperative",
			Provider: "openai-compat", Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", DiffContextApproved: true, VerificationMode: "none",
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

func TestConfigureVerificationFailureLeavesCredentialAndSettingsUntouched(t *testing.T) {
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
			Strategy: "intent", Preset: "balanced", CommitFormat: "imperative",
			Provider: "openai-compat", Model: "model", BaseURL: ai.DefaultOpenAIBaseURL,
			ProviderTimeout: "30s", DiffContextApproved: true,
			VerificationMode: "fast", VerificationCommand: "make test",
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
	configureRunVerification = func(context.Context, string, string, string, string, string) (verification.Result, error) {
		order = append(order, "verification")
		return verification.Result{Status: verification.StatusFailed, NeedsAttention: true}, nil
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
	_, _, err := executeConfigureCommand(t, "", "--repo", repo)
	if err == nil || !strings.Contains(err.Error(), "No configuration was changed") {
		t.Fatalf("err=%v", err)
	}
	got := strings.Join(order, ",")
	if got != "authoring,validate,validate,snapshot,validate,provider,verification" {
		t.Fatalf("failure order=%v", order)
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
		report.Verification.Command != "go test ./internal/... ignored" ||
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
	for _, mode := range [][2]string{{"event", "balanced"}, {"event", "quality"}, {"intent", "fast"}} {
		candidate := base
		candidate.Strategy, candidate.Preset = mode[0], mode[1]
		candidate.DiffContextApproved = true
		if err := validateConfigureSelection(candidate, true); err == nil ||
			!strings.Contains(err.Error(), "only by Event Fast") {
			t.Fatalf("%s.%s err=%v", mode[0], mode[1], err)
		}
	}
	base.Strategy, base.Preset = "event", "fast"
	if err := validateConfigureSelection(base, false); err != nil {
		t.Fatalf("Event Fast rejected: %v", err)
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
	for _, want := range []string{"--accessible", "--strategy", "--preset", "--credential-stdin", "--dry-run", "one immutable", "never edits harness"} {
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
	oldStatus, oldWrite := configureCredentialStatus, configureCredentialWrite
	oldRead, oldRemove := configureCredentialRead, configureCredentialRemove
	oldVerify, oldEnable := configureRunVerification, configureEnable
	oldIn, oldOut := settingsInputTTY, settingsOutputTTY
	t.Cleanup(func() {
		openConfigureSettingsService, runConfigureWizard, confirmConfigurePreview = oldOpen, oldWizard, oldConfirm
		openConfigureValidationService = oldPreview
		configureCredentialStatus, configureCredentialWrite = oldStatus, oldWrite
		configureCredentialRead, configureCredentialRemove = oldRead, oldRemove
		configureRunVerification, configureEnable = oldVerify, oldEnable
		settingsInputTTY, settingsOutputTTY = oldIn, oldOut
	})
}

func jsonUnmarshalStrict(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

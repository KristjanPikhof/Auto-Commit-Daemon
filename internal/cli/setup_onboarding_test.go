package cli

import (
	"bytes"
	"context"

	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
)

func TestSetupNonInteractiveDefaultsToOfflineLocal(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "")
	cmd := newSetupCommand(false)
	roots := paths.Roots{Config: filepath.Join(t.TempDir(), "acd")}
	for _, opts := range []setupOnboardingOptions{{}, {Provider: "deterministic"}} {
		state, err := prepareSetupOnboarding(cmd, roots, opts, true, true)
		if err != nil {
			t.Fatal(err)
		}
		if state.Selection.Strategy != "local" || state.Selection.Provider != "deterministic" || state.Configuration.Values[config.FieldDiffEgress] != "false" || state.Credential != "" {
			t.Fatalf("offline setup changed: %+v", state.Selection)
		}
	}
}

func TestSetupRepositoryConsentIsSeparateAndReportsPartialSuccess(t *testing.T) {
	for _, tc := range []struct {
		name, answer string
		failure      bool
		wantCall     bool
	}{
		{name: "decline", answer: "n\n"},
		{name: "default", answer: "\n"},
		{name: "accept", answer: "yes\n", wantCall: true},
		{name: "checkpoint failure", answer: "y\n", wantCall: true, failure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newSetupCommand(false)
			cmd.SetIn(strings.NewReader(tc.answer))
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			called := false
			err := offerSetupProtection(cmd, "/project", func(_ context.Context, _ io.Writer, repo string, jsonOut bool) error {
				called = true
				if repo != "/project" || jsonOut {
					t.Fatalf("wrong enable scope: %q, %v", repo, jsonOut)
				}
				if tc.failure {
					return errors.New("checkpoint unavailable")
				}
				return nil
			})
			if called != tc.wantCall {
				t.Fatalf("enable called=%v", called)
			}
			if tc.failure {
				if err == nil || !strings.Contains(err.Error(), "installation succeeded") {
					t.Fatalf("partial result: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSetupDryRunIntentIncludesExactTransportConsents(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "")
	cmd := newSetupCommand(false)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	state, err := prepareSetupOnboarding(cmd, paths.Roots{Config: filepath.Join(t.TempDir(), "acd")}, setupOnboardingOptions{
		Strategy: "intent", CommitFormat: "conventional",
		Provider: "openai-compat", BaseURL: "http://gateway.example/v1/", Model: "model-one",
	}, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Configuration.Values[config.FieldBaseURL]; got != "http://gateway.example/v1" {
		t.Fatalf("normalized endpoint = %q", got)
	}
	want := map[ai.ConfirmationRequirement]bool{
		ai.ConfirmationEndpointCredentials:         true,
		ai.ConfirmationInsecureEndpointCredentials: true,
		ai.ConfirmationDiffEgress:                  true,
	}
	for _, confirmation := range state.Configuration.Confirmations {
		delete(want, confirmation)
	}
	if len(want) != 0 {
		t.Fatalf("missing confirmations = %v", want)
	}
	if state.Configuration.Values[config.FieldCommitFormat] != "conventional" {
		t.Fatalf("commit format = %q", state.Configuration.Values[config.FieldCommitFormat])
	}
	if state.Configuration.Values[config.FieldDiffEgress] != "true" {
		t.Fatal("Intent setup omitted required code sharing from its review")
	}
}

func TestSetupNonTTYUsesAccessibleWizard(t *testing.T) {
	oldWizard := runConfigureWizard
	oldInputTTY := settingsInputTTY
	oldOutputTTY := settingsOutputTTY
	t.Cleanup(func() {
		runConfigureWizard = oldWizard
		settingsInputTTY = oldInputTTY
		settingsOutputTTY = oldOutputTTY
	})
	settingsInputTTY = func(io.Reader) bool { return false }
	settingsOutputTTY = func(io.Writer) bool { return false }
	accessible := false
	runConfigureWizard = func(
		_ context.Context,
		opts settingsui.ConfigureWizardOptions,
	) (settingsui.ConfigureSelection, error) {
		accessible = opts.Accessible
		if opts.Defaults[config.FieldProvider] != "deterministic" || opts.Defaults[config.FieldCommitStrategy] != "local" || opts.Defaults[config.FieldCommitFormat] != "imperative" {
			t.Fatalf("fresh accessible setup lost offline defaults: %+v", opts.Defaults)
		}
		selection := configureSelectionFromValues(opts.Defaults)
		return selection, nil
	}

	cmd := newSetupCommand(false)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	_, err := prepareSetupOnboarding(cmd,
		paths.Roots{Config: filepath.Join(t.TempDir(), "acd")},
		setupOnboardingOptions{}, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !accessible {
		t.Fatal("non-terminal setup did not select the accessible wizard")
	}
}

func TestSetupRejectsUnsafeEndpointBeforeProviderCall(t *testing.T) {
	cmd := newSetupCommand(false)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	for _, endpoint := range []string{
		"https://user:pass@example.test/v1",
		"https://example.test/v1?token=value",
		"https://example.test/v1#fragment",
		"ftp://example.test/v1",
	} {
		_, err := prepareSetupOnboarding(cmd, paths.Roots{Config: filepath.Join(t.TempDir(), "acd")}, setupOnboardingOptions{
			Strategy: "intent", Provider: "openai-compat", BaseURL: endpoint, Model: "model-one",
		}, true, false)
		if err == nil {
			t.Fatalf("unsafe endpoint accepted: %s", endpoint)
		}
	}
}

func TestSetupPlanCredentialSummary(t *testing.T) {
	for _, tc := range []struct {
		name, strategy, provider, want string
		source                         credentials.Source
		store                          bool
	}{
		{name: "local", strategy: "local", provider: "deterministic", want: "Credential: not needed for Local mode."},
		{name: "missing", strategy: "intent", provider: "openai-compat", want: "Credential: required; interactive setup will ask for an API key", source: credentials.SourceNone},
		{name: "saved", strategy: "intent", provider: "openai-compat", want: "Credential: reuse the protected user credential file.", source: credentials.SourceFile},
		{name: "new", strategy: "intent", provider: "openai-compat", want: "API key: ACD will save it in a file only your user account can read.", source: credentials.SourceFile, store: true},
		{name: "environment", strategy: "intent", provider: "openai-compat", want: "API key: Uses ACD_AI_API_KEY from this environment.", source: credentials.SourceEnvironment},
		{name: "subprocess", strategy: "intent", provider: "subprocess:acd-ai", want: "Credential: not needed for this provider."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newSetupCommand(false)
			var out bytes.Buffer
			cmd.SetOut(&out)
			plan := installer.Plan{Configuration: &installer.SetupConfiguration{Values: map[string]string{
				config.FieldCommitStrategy: tc.strategy, config.FieldProvider: tc.provider,
			}, CredentialSource: tc.source, StoreCredential: tc.store}}
			if err := renderSetupPlan(cmd, plan, false); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Fatalf("credential guidance missing %q:\n%s", tc.want, out.String())
			}
		})
	}
}

func TestSetupDryRunMissingCredentialRemainsMissing(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "")
	cmd := newSetupCommand(false)
	var out bytes.Buffer
	cmd.SetOut(&out)
	state, err := prepareSetupOnboarding(cmd, paths.Roots{Config: filepath.Join(t.TempDir(), "acd")}, setupOnboardingOptions{Strategy: "intent", Provider: "openai-compat"}, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if state.Configuration.CredentialSource != credentials.SourceNone || state.Configuration.StoreCredential || state.Credential != "" {
		t.Fatalf("preview invented a saved credential: %+v", state.Configuration)
	}
	if err := renderSetupPlan(cmd, installer.Plan{Configuration: &state.Configuration}, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Credential: required;") {
		t.Fatalf("missing-key preview guidance:\n%s", out.String())
	}
}

func TestSetupExistingInstallRejectsFirstSetupOptions(t *testing.T) {
	roots := withIsolatedHome(t)
	chdirForTest(t, t.TempDir())
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldCommitStrategy] = []byte(`"local"`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(roots.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	for _, option := range []string{
		"--strategy=intent", "--commit-format=conventional", "--provider=openai-compat",
		"--base-url=https://example.test/v1", "--model=example-model", "--ca-file=example.pem",
		"--credential-stdin", "--confirm-endpoint-credentials", "--confirm-insecure-http", "--confirm-diff-egress",
	} {
		t.Run(option, func(t *testing.T) {
			cmd := newRootCmd()
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs([]string{"setup", "--dry-run", "--integrations=none", option})
			if err := cmd.ExecuteContext(t.Context()); err == nil || !strings.Contains(err.Error(), "use `acd config`") {
				t.Fatalf("existing setup ignored %s: %v", option, err)
			}
		})
	}
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"setup", "--dry-run", "--json", "--integrations=none"})
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	plan := decodeSetupPlanEnvelope(t, out.Bytes())
	if !plan.ExistingInstall || plan.FreshDefaults || plan.Configuration != nil {
		t.Fatalf("existing upgrade replaced preferences: %+v", plan)
	}
	if after, err := fileSHA256(roots.ConfigPath()); err != nil || after != before {
		t.Fatalf("setup preview changed existing preferences: %v", err)
	}
}

func TestSetupIntentRequiresExplicitCodeSharingApproval(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "setup-test-key")
	cmd := newSetupCommand(false)
	if err := cmd.Flags().Set("expect-plan", "reviewed-plan"); err != nil {
		t.Fatal(err)
	}
	roots := paths.Roots{Config: filepath.Join(t.TempDir(), "acd")}
	opts := setupOnboardingOptions{Strategy: "intent"}
	if _, err := prepareSetupOnboarding(cmd, roots, opts, false, true); err == nil || !strings.Contains(err.Error(), "--confirm-diff-egress") {
		t.Fatalf("noninteractive Intent setup skipped sharing approval: %v", err)
	}
	opts.ConfirmDiffEgress = true
	state, err := prepareSetupOnboarding(cmd, roots, opts, false, true)
	if err != nil || state.Configuration.Values[config.FieldDiffEgress] != "true" {
		t.Fatalf("approved Intent setup lost code sharing: %+v %v", state, err)
	}
}

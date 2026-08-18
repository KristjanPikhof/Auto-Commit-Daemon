package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
)

func TestSetupDryRunDefaultsAreEverydayLocalAndSecretFree(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "")
	cmd := newSetupCommand(false)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	state, err := prepareSetupOnboarding(cmd, paths.Roots{Config: filepath.Join(t.TempDir(), "acd")}, setupOnboardingOptions{}, true, false)
	if err != nil {
		t.Fatal(err)
	}
	values := state.Configuration.Values
	if values[config.FieldCommitStrategy] != "intent" ||
		values[config.FieldCommitPreset] != "balanced" ||
		values[config.FieldCommitFormat] != "imperative" ||
		values[config.FieldProvider] != "deterministic" ||
		values[config.FieldDiffEgress] != "false" ||
		values[config.FieldIntentRepairEnabled] != "true" {
		t.Fatalf("fresh values = %+v", values)
	}
	body, err := json.Marshal(state.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "API_KEY") || strings.Contains(string(body), "sk-") {
		t.Fatalf("serialized setup contains credential material: %s", body)
	}
}

func TestSetupDryRunOpenAIIncludesPublicConsents(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "")
	cmd := newSetupCommand(false)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	state, err := prepareSetupOnboarding(cmd, paths.Roots{Config: filepath.Join(t.TempDir(), "acd")}, setupOnboardingOptions{
		Experience: "everyday", CommitFormat: "conventional",
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
		ai.ConfirmationIntentRepair:                true,
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
		return configureSelectionFromValues(opts.Defaults), nil
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
			Provider: "openai-compat", BaseURL: endpoint, Model: "model-one",
		}, true, false)
		if err == nil {
			t.Fatalf("unsafe endpoint accepted: %s", endpoint)
		}
	}
}

package settingsui

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestFinishOnboardingLocalSkipsCredentialAndProviderPrompts(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{Input: &bytewiseReader{r: strings.NewReader("\n\n")}, Output: &out, Accessible: true, IncludeCommitFormat: true, CredentialStatus: func() (bool, error) { return false, errors.New("credential store must not be read") }})
	if err != nil || selection.Strategy != "local" || selection.Provider != "deterministic" || selection.VerificationMode != "none" {
		t.Fatalf("local %+v: %v\n%s", selection, err, out.String())
	}
	for _, forbidden := range []string{"API key", "Which provider", "Repair", "preset", "Strict review", "Model"} {
		if strings.Contains(out.String(), forbidden) {
			t.Fatalf("local prompted %q", forbidden)
		}
	}
}

func TestFinishOnboardingSavedIntentRetainsProviderAndExplicitChecks(t *testing.T) {
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{Input: &bytewiseReader{r: strings.NewReader("")}, Output: &out, Accessible: true, ExplicitMode: true, ProviderConfigured: true, OpenAIConfigured: true, HasCredential: true, Defaults: map[string]string{"commit.strategy": "intent", "commit.format": "conventional", "ai.provider": "openai-compat", "ai.model": "saved-model", "ai.base_url": "https://saved.example/v1", "intent.verification": "full", "verification.full.command": "make verify"}})
	if err != nil || selection.Strategy != "intent" || selection.Model != "saved-model" || selection.CommitFormat != "conventional" || selection.VerificationCommand != "make verify" {
		t.Fatalf("saved %+v: %v", selection, err)
	}
	if out.Len() != 0 {
		t.Fatalf("explicit saved choices prompted: %s", out.String())
	}
}

func TestFinishRemovalWizardIntentRequiresTransportAfterLocal(t *testing.T) {
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{Input: &bytewiseReader{r: strings.NewReader("2\n\nhttps://api.openai.com/v1\nintent-model\nsecret-key\n")}, Output: &out, Accessible: true, ProviderConfigured: true, Defaults: map[string]string{"commit.strategy": "local", "ai.provider": "deterministic"}})
	if err != nil || selection.Strategy != "intent" || selection.Provider != "openai-compat" {
		t.Fatalf("Intent connection=%+v: %v\n%s", selection, err, out.String())
	}
	if !strings.Contains(out.String(), "Which provider should Intent use?") || strings.Contains(out.String(), "secret-key") {
		t.Fatalf("connection prompt/secret: %s", out.String())
	}
}

func TestFinishRemovalWizardSubprocessSkipsAPICredentialStatus(t *testing.T) {
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{Input: &bytewiseReader{r: strings.NewReader("\n")}, Output: &out, Accessible: true, ExplicitMode: true, ProviderConfigured: true, Defaults: map[string]string{"commit.strategy": "intent", "ai.provider": "subprocess:approved-ai"}, CredentialStatus: func() (bool, error) { return false, errors.New("unrelated API credential store unreadable") }})
	if err != nil || selection.Provider != "subprocess:approved-ai" {
		t.Fatalf("subprocess configuration=%+v: %v", selection, err)
	}
	if strings.Contains(out.String(), "API key") {
		t.Fatalf("subprocess prompted API key: %s", out.String())
	}
}

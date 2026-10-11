package settingsui

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestFinishRemovalWizardStagesIntentWithoutTests(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	input := strings.Join([]string{
		"", // Keep Intent
		"", // openai-compat
		"https://gateway.example/v1",
		"gateway-model",
		"staged-secret",
		"",
	}, "\n")
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input: &bytewiseReader{r: strings.NewReader(input)}, Output: &out,
		Accessible: true, DetectedQuickCommand: "go test ./... -run '^$'",
		DetectedQuickSource: "Go language default",
		Defaults: map[string]string{
			"commit.strategy": "intent", "intent.verification": "structural", "ai.diff_egress": "true",
			"commit.format": "imperative", "ai.provider": "openai-compat",
			"ai.model": "gpt-6-luna", "ai.base_url": "https://api.openai.com/v1",
			"ai.timeout": "30s",
		},
	})
	if err != nil {
		t.Fatalf("wizard: %v\n%s", err, out.String())
	}
	if selection.Strategy != "intent" ||
		selection.VerificationMode != "structural" ||
		selection.VerificationCommand != "" ||
		selection.ExecutionMode != "explicit finish" ||
		selection.VerificationApproved || !selection.DiffContextApproved ||
		selection.Credential != "staged-secret" ||
		selection.BaseURL != "https://gateway.example/v1" ||
		selection.Model != "gateway-model" {
		t.Fatalf("selection=%+v\n%s", selection, out.String())
	}
	if strings.Contains(out.String(), "staged-secret") || strings.Contains(out.String(), "\x1b[") {
		t.Fatalf("accessible output leaked secret or color: %q", out.String())
	}
	for _, want := range []string{
		"How should finish create commits?", "Which provider should Intent use?",
		"AI service URL (OpenAI compatible)", "Model", "API key",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q\n%s", want, out.String())
		}
	}
	for _, forbidden := range []string{
		"verification command", "Commit message format", "Provider timeout",
		"Strict review", "background check",
	} {
		if strings.Contains(out.String(), forbidden) {
			t.Errorf("regular wizard rendered technical prompt %q:\n%s",
				forbidden, out.String())
		}
	}
}

func TestFinishRemovalFinalApprovalBindsCommand(t *testing.T) {
	var out bytes.Buffer
	approval, err := ConfirmConfigurePreview(context.Background(),
		&bytewiseReader{r: strings.NewReader("y\n")}, &out, true,
		ConfigurePreviewApprovalOptions{
			VerificationMode: "full", VerificationCommand: "make test\nunsafe",
		})
	if err != nil || !approval.Verification || !approval.Apply {
		t.Fatalf("approval=%+v err=%v\n%s", approval, err, out.String())
	}
	view := out.String()
	if !strings.Contains(view, "Approve these permissions") {
		t.Errorf("single approval missing:\n%s", view)
	}
	if strings.Contains(view, "make test") || strings.Contains(view, "30m") {
		t.Fatalf("single approval repeated preview details: %q", view)
	}
	if strings.Contains(view, "\x1b[") {
		t.Fatalf("final approval used rich terminal rendering: %q", view)
	}
}

func TestConfigureGlobalFinalApprovalDoesNotPromiseActivation(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var out bytes.Buffer
	approval, err := ConfirmConfigurePreview(
		context.Background(),
		&bytewiseReader{r: strings.NewReader("y\n")},
		&out,
		true,
		ConfigurePreviewApprovalOptions{Global: true},
	)
	if err != nil || !approval.Apply {
		t.Fatalf("approval=%+v err=%v\n%s", approval, err, out.String())
	}
	if !strings.Contains(out.String(), "save the global defaults") ||
		strings.Contains(out.String(), "enable ACD") {
		t.Fatalf("global approval prompt=%q", out.String())
	}
}

func TestConfigureSecretReaderDoesNotConsumeFollowingAnswers(t *testing.T) {
	input := strings.NewReader("staged-secret\nnext-answer\n")
	value, err := readConfigureSecretLine(input, 64)
	if err != nil || value != "staged-secret" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	remaining := new(strings.Builder)
	if _, err := io.Copy(remaining, input); err != nil || remaining.String() != "next-answer\n" {
		t.Fatalf("remaining=%q err=%v", remaining.String(), err)
	}
}

func TestIntentWizardRequiresCredentialForConfiguredExternalProvider(t *testing.T) {
	var out bytes.Buffer
	selection, err := RunConfigureWizard(t.Context(), ConfigureWizardOptions{
		Input: &bytewiseReader{r: strings.NewReader("\n\n\nnew-provider-key\n")}, Output: &out, Accessible: true,
		ExplicitMode: true, ProviderConfigured: true, OpenAIConfigured: true, HasCredential: true,
		CredentialStatus: func() (bool, error) { return false, nil },
		Defaults:         map[string]string{"commit.strategy": "intent", "ai.provider": "openai-compat", "ai.base_url": "https://gateway.example/v1", "ai.model": "gateway-model"},
	})
	if err != nil {
		t.Fatalf("wizard: %v\n%s", err, out.String())
	}
	if selection.Credential != "new-provider-key" || selection.Provider != "openai-compat" {
		t.Fatal("external provider was left without a credential")
	}
	if !strings.Contains(out.String(), "requires an API key") || strings.Contains(out.String(), "new-provider-key") {
		t.Fatalf("missing explanation or secret leaked: %s", out.String())
	}
}

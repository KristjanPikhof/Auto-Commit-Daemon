package ai

import (
	"errors"
	"strings"
	"testing"
)

func TestFinishRemovalTransportRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		cfg  ProviderConfig
		want string
	}{
		{name: "unknown", cfg: ProviderConfig{Mode: "mystery"}, want: "unknown provider"},
		{name: "Intent without AI", cfg: ProviderConfig{Mode: "deterministic", CommitStrategy: CommitStrategyIntent}, want: "choose local mode"},
		{name: "missing credential", cfg: ProviderConfig{Mode: "openai-compat", Model: "model"}, want: "missing API key"},
		{name: "invalid endpoint", cfg: ProviderConfig{Mode: "openai-compat", APIKey: "secret", Model: "model", BaseURL: "ftp://example.test/v1"}, want: "unsupported ACD_AI_BASE_URL scheme"},
		{name: "invalid model", cfg: ProviderConfig{Mode: "openai-compat", APIKey: "secret", Model: "bad\nmodel"}, want: "model is invalid"},
		{name: "missing subprocess", cfg: ProviderConfig{Mode: "subprocess:missing", subprocessLookPath: func(string) (string, error) { return "", errors.New("not found") }}, want: "not found"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider, closer, err := buildFinishTransport(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
			if provider != nil || closer != nil {
				t.Fatalf("provider=%v closer=%v", provider, closer)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaked API key: %q", err)
			}
		})
	}
}

func TestValidateProviderConfigConfirmationsAreDistinct(t *testing.T) {
	validation, err := ValidateProviderConfig(ProviderConfig{
		Mode:         "openai-compat",
		BaseURL:      "https://gateway.example/v1",
		APIKey:       "secret-value",
		Model:        "model",
		DiffEgress:   true,
		CommitFormat: CommitFormatImperative,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []ConfirmationRequirement{ConfirmationEndpointCredentials, ConfirmationDiffEgress}
	if len(validation.Confirmations) != len(want) {
		t.Fatalf("confirmations=%v", validation.Confirmations)
	}
	for i := range want {
		if validation.Confirmations[i] != want[i] {
			t.Fatalf("confirmations=%v want=%v", validation.Confirmations, want)
		}
	}

	subprocess, err := ValidateProviderConfig(ProviderConfig{Mode: "subprocess:local", DiffEgress: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(subprocess.Confirmations) != 2 ||
		subprocess.Confirmations[0] != ConfirmationSubprocessExecution ||
		subprocess.Confirmations[1] != ConfirmationDiffEgress {
		t.Fatalf("subprocess confirmations=%v", subprocess.Confirmations)
	}
}

func TestValidateProviderConfigRequiresHTTPWarningAndRejectsUnsafeURLs(t *testing.T) {
	validation, err := ValidateProviderConfig(ProviderConfig{
		Mode: "openai-compat", BaseURL: "http://gateway.example/v1/",
		APIKey: "secret-value", Model: "model",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []ConfirmationRequirement{
		ConfirmationEndpointCredentials,
		ConfirmationInsecureEndpointCredentials,
	}
	if len(validation.Confirmations) != len(want) {
		t.Fatalf("HTTP confirmations = %v", validation.Confirmations)
	}
	for index := range want {
		if validation.Confirmations[index] != want[index] {
			t.Fatalf("HTTP confirmations = %v, want %v", validation.Confirmations, want)
		}
	}
	for _, endpoint := range []string{
		"https://user:pass@example.test/v1",
		"https://example.test/v1?token=value",
		"https://example.test/v1#fragment",
		"ftp://example.test/v1",
	} {
		_, err := ValidateProviderConfig(ProviderConfig{
			Mode: "openai-compat", BaseURL: endpoint,
			APIKey: "secret-value", Model: "model",
		})
		if err == nil || strings.Contains(err.Error(), "secret-value") {
			t.Fatalf("unsafe endpoint %q error = %v", endpoint, err)
		}
		if strings.Contains(endpoint, "user:pass") {
			if !strings.Contains(err.Error(), "embedded credentials") {
				t.Fatalf("userinfo error = %v", err)
			}
			if strings.Contains(strings.ToLower(err.Error()), "password") {
				t.Fatalf("userinfo error must use credential-safe wording: %v", err)
			}
		}
	}
}

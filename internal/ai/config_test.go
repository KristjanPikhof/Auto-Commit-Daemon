package ai

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLoadProviderConfigFromEnv_AllVars(t *testing.T) {
	t.Setenv(EnvProvider, "  Subprocess:CustomPlugin  ")
	t.Setenv(EnvBaseURL, "  https://gateway.example/v1  ")
	t.Setenv(EnvAPIKey, "  sk-abc123  ")
	t.Setenv(EnvModel, "  gpt-4.1-mini  ")
	t.Setenv(EnvTimeout, "45s")
	t.Setenv(EnvCAFile, "  /tmp/acd-test-ca.pem  ")
	t.Setenv(EnvCommitStrategy, "  intent  ")
	t.Setenv(EnvCommitFormat, "  conventional  ")

	cfg := LoadProviderConfigFromEnv()

	if cfg.Mode != "subprocess:CustomPlugin" {
		t.Fatalf("Mode=%q want subprocess:CustomPlugin", cfg.Mode)
	}
	if cfg.BaseURL != "https://gateway.example/v1" {
		t.Fatalf("BaseURL=%q", cfg.BaseURL)
	}
	if cfg.APIKey != "sk-abc123" {
		t.Fatalf("APIKey=%q", cfg.APIKey)
	}
	if cfg.Model != "gpt-4.1-mini" {
		t.Fatalf("Model=%q", cfg.Model)
	}
	if cfg.Timeout != 45*time.Second {
		t.Fatalf("Timeout=%v want 45s", cfg.Timeout)
	}
	if cfg.CAFile != "/tmp/acd-test-ca.pem" {
		t.Fatalf("CAFile=%q", cfg.CAFile)
	}
	if cfg.CommitStrategy != CommitStrategyIntent {
		t.Fatalf("CommitStrategy=%q want intent", cfg.CommitStrategy)
	}
	if cfg.CommitFormat != CommitFormatConventional {
		t.Fatalf("CommitFormat=%q want conventional", cfg.CommitFormat)
	}
}

// TestLoadProviderConfigFromEnv_Defaults: an empty env yields the
// documented defaults (mode empty, base URL + model + timeout populated).
func TestLoadProviderConfigFromEnv_Defaults(t *testing.T) {
	t.Setenv(EnvProvider, "")
	t.Setenv(EnvBaseURL, "")
	t.Setenv(EnvAPIKey, "")
	t.Setenv(EnvModel, "")
	t.Setenv(EnvTimeout, "")
	t.Setenv(EnvCommitStrategy, "")
	t.Setenv(EnvCommitFormat, "")

	cfg := LoadProviderConfigFromEnv()
	if cfg.Mode != "" {
		t.Fatalf("Mode=%q want empty", cfg.Mode)
	}
	if cfg.BaseURL != DefaultOpenAIBaseURL {
		t.Fatalf("BaseURL=%q want default", cfg.BaseURL)
	}
	if cfg.Model != DefaultOpenAIModel {
		t.Fatalf("Model=%q want default", cfg.Model)
	}
	if cfg.Timeout != DefaultProviderTimeout {
		t.Fatalf("Timeout=%v want default", cfg.Timeout)
	}
	if cfg.CommitStrategy != CommitStrategyLocal {
		t.Fatalf("CommitStrategy=%q want local", cfg.CommitStrategy)
	}
	if cfg.CommitFormat != CommitFormatImperative {
		t.Fatalf("CommitFormat=%q want imperative", cfg.CommitFormat)
	}
}

// TestLoadProviderConfigFromEnv_TimeoutSeconds: a bare integer is parsed
// as seconds (compatibility with ACD_CLIENT_TTL_SECONDS conventions
// elsewhere in the codebase).
func TestLoadProviderConfigFromEnv_TimeoutSeconds(t *testing.T) {
	t.Setenv(EnvTimeout, "12.5")
	cfg := LoadProviderConfigFromEnv()
	want := time.Duration(12.5 * float64(time.Second))
	if cfg.Timeout != want {
		t.Fatalf("Timeout=%v want %v", cfg.Timeout, want)
	}
}

func TestLoadProviderConfigFromEnv_CommitStrategy(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want CommitStrategy
	}{
		{raw: "", want: CommitStrategyLocal},
		{raw: "event", want: CommitStrategyLocal},
		{raw: " EVENT ", want: CommitStrategyLocal},
		{raw: "intent", want: CommitStrategyIntent},
		{raw: " INTENT ", want: CommitStrategyIntent},
		{raw: "unknown", want: CommitStrategyLocal},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			t.Setenv(EnvCommitStrategy, tc.raw)
			cfg := LoadProviderConfigFromEnv()
			if cfg.CommitStrategy != tc.want {
				t.Fatalf("CommitStrategy=%q want %q", cfg.CommitStrategy, tc.want)
			}
		})
	}
}

func TestLoadProviderConfigFromEnv_CommitFormat(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want CommitFormat
	}{
		{raw: "", want: CommitFormatImperative},
		{raw: "imperative", want: CommitFormatImperative},
		{raw: " IMPERATIVE ", want: CommitFormatImperative},
		{raw: "conventional", want: CommitFormatConventional},
		{raw: " CONVENTIONAL ", want: CommitFormatConventional},
		{raw: "unknown", want: CommitFormatImperative},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			t.Setenv(EnvCommitFormat, tc.raw)
			cfg := LoadProviderConfigFromEnv()
			if cfg.CommitFormat != tc.want {
				t.Fatalf("CommitFormat=%q want %q", cfg.CommitFormat, tc.want)
			}
		})
	}
}

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

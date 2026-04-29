package ai

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureHandler is a minimal slog.Handler that records every record so
// tests can assert which warning fired on the degraded paths.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *captureHandler) findWarn(substr string) (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level >= slog.LevelWarn && strings.Contains(r.Message, substr) {
			return r, true
		}
	}
	return slog.Record{}, false
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// TestLoadProviderConfigFromEnv_AllVars: every ACD_AI_* env var is read,
// trimmed, and the prefix lowercased; missing vars take their defaults.
func TestLoadProviderConfigFromEnv_AllVars(t *testing.T) {
	t.Setenv(EnvProvider, "  Subprocess:CustomPlugin  ")
	t.Setenv(EnvBaseURL, "  https://gateway.example/v1  ")
	t.Setenv(EnvAPIKey, "  sk-abc123  ")
	t.Setenv(EnvModel, "  gpt-4.1-mini  ")
	t.Setenv(EnvTimeout, "45s")
	t.Setenv(EnvCAFile, "  /tmp/acd-test-ca.pem  ")

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
}

// TestLoadProviderConfigFromEnv_Defaults: an empty env yields the
// documented defaults (mode empty, base URL + model + timeout populated).
func TestLoadProviderConfigFromEnv_Defaults(t *testing.T) {
	t.Setenv(EnvProvider, "")
	t.Setenv(EnvBaseURL, "")
	t.Setenv(EnvAPIKey, "")
	t.Setenv(EnvModel, "")
	t.Setenv(EnvTimeout, "")

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

// TestBuildProvider_DeterministicDefault: empty mode and "deterministic"
// both yield DeterministicProvider with no closer.
func TestBuildProvider_DeterministicDefault(t *testing.T) {
	for _, mode := range []string{"", "deterministic"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			h := &captureHandler{}
			cfg := ProviderConfig{Mode: mode, Logger: slog.New(h)}
			p, closer, err := BuildProvider(cfg)
			if err != nil {
				t.Fatalf("BuildProvider: %v", err)
			}
			if closer != nil {
				t.Fatalf("closer non-nil for deterministic")
			}
			if p.Name() != "deterministic" {
				t.Fatalf("Name=%q want deterministic", p.Name())
			}
			if _, found := h.findWarn(""); found {
				t.Fatalf("unexpected warning on deterministic path")
			}
		})
	}
}

// TestBuildProvider_OpenAICompatComposed: a populated APIKey produces a
// composed chain whose primary identifies as openai-compat.
func TestBuildProvider_OpenAICompatComposed(t *testing.T) {
	cfg := ProviderConfig{
		Mode:    "openai-compat",
		BaseURL: "https://api.example/v1",
		APIKey:  "sk-test",
		Model:   "gpt-test",
		Logger:  quietLogger(),
	}
	p, closer, err := BuildProvider(cfg)
	if err != nil {
		t.Fatalf("BuildProvider: %v", err)
	}
	if closer != nil {
		t.Fatalf("closer non-nil for openai-compat (no subprocess)")
	}
	want := "openai-compat+deterministic"
	if p.Name() != want {
		t.Fatalf("Name=%q want %q", p.Name(), want)
	}
}

func TestBuildProvider_OpenAICompatRejectsInvalidBaseURL(t *testing.T) {
	for _, tc := range []struct {
		name      string
		baseURL   string
		wantError string
	}{
		{
			name:      "http rejected",
			baseURL:   "http://gateway.example/v1",
			wantError: "must use https",
		},
		{
			name:      "relative rejected",
			baseURL:   "/v1",
			wantError: "must be an absolute URL",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ProviderConfig{
				Mode:    "openai-compat",
				BaseURL: tc.baseURL,
				APIKey:  "sk-test",
				Logger:  quietLogger(),
			}
			p, closer, err := BuildProvider(cfg)
			if err == nil {
				t.Fatalf("BuildProvider returned nil error")
			}
			if p != nil || closer != nil {
				t.Fatalf("provider=%v closer=%v, want nils on invalid URL", p, closer)
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error=%q want substring %q", err, tc.wantError)
			}
		})
	}
}

// TestBuildProvider_OpenAICompatNoKeyDegrades: an empty APIKey logs a
// warning and falls back to DeterministicProvider so misconfiguration
// can never silently disable commit messages.
func TestBuildProvider_OpenAICompatNoKeyDegrades(t *testing.T) {
	h := &captureHandler{}
	cfg := ProviderConfig{
		Mode:   "openai-compat",
		APIKey: "",
		Logger: slog.New(h),
	}
	p, closer, err := BuildProvider(cfg)
	if err != nil {
		t.Fatalf("BuildProvider: %v", err)
	}
	if closer != nil {
		t.Fatalf("closer non-nil for degraded openai-compat")
	}
	if p.Name() != "deterministic" {
		t.Fatalf("Name=%q want deterministic", p.Name())
	}
	if _, found := h.findWarn("ACD_AI_API_KEY empty"); !found {
		t.Fatalf("warning about empty API key not fired; records=%v", h.records)
	}
}

// TestBuildProvider_Subprocess: a subprocess:<name> mode wraps the
// SubprocessProvider in Compose with deterministic fallback, returns a
// non-nil closer, and Close drains cleanly even on a missing binary.
func TestBuildProvider_Subprocess(t *testing.T) {
	cfg := ProviderConfig{
		Mode:    "subprocess:foo",
		Timeout: 3 * time.Second,
		Logger:  quietLogger(),
	}
	p, closer, err := BuildProvider(cfg)
	if err != nil {
		t.Fatalf("BuildProvider: %v", err)
	}
	if closer == nil {
		t.Fatalf("closer nil for subprocess mode")
	}
	defer closer.Close()
	want := "subprocess:foo+deterministic"
	if p.Name() != want {
		t.Fatalf("Name=%q want %q", p.Name(), want)
	}

	// Subprocess binary likely doesn't exist on the test host; the chain
	// must still satisfy Generate via the deterministic fallback.
	r, err := p.Generate(context.Background(), CommitContext{
		Path: "hello.txt", Op: "create",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if r.Subject == "" {
		t.Fatalf("empty subject; chain did not fall through to deterministic")
	}
	if !strings.Contains(r.Subject, "hello.txt") {
		t.Fatalf("subject=%q does not mention hello.txt", r.Subject)
	}
}

// TestBuildProvider_SubprocessEmptyName: a colon with no plugin name is
// a misconfiguration; degrade to deterministic with a warning rather
// than spawning anything.
func TestBuildProvider_SubprocessEmptyName(t *testing.T) {
	h := &captureHandler{}
	cfg := ProviderConfig{
		Mode:   "subprocess:",
		Logger: slog.New(h),
	}
	p, closer, err := BuildProvider(cfg)
	if err != nil {
		t.Fatalf("BuildProvider: %v", err)
	}
	if closer != nil {
		t.Fatalf("closer non-nil")
	}
	if p.Name() != "deterministic" {
		t.Fatalf("Name=%q want deterministic", p.Name())
	}
	if _, found := h.findWarn("missing plugin name"); !found {
		t.Fatalf("warning about empty plugin name not fired")
	}
}

// TestBuildProvider_UnknownModeDegrades: any unrecognized value warns
// and falls back to deterministic.
func TestBuildProvider_UnknownModeDegrades(t *testing.T) {
	h := &captureHandler{}
	cfg := ProviderConfig{
		Mode:   "garbage",
		Logger: slog.New(h),
	}
	p, _, err := BuildProvider(cfg)
	if err != nil {
		t.Fatalf("BuildProvider: %v", err)
	}
	if p.Name() != "deterministic" {
		t.Fatalf("Name=%q want deterministic", p.Name())
	}
	if _, found := h.findWarn("unrecognized ACD_AI_PROVIDER"); !found {
		t.Fatalf("warning about unknown mode not fired")
	}
}

// TestBuildProvider_NilLoggerDoesNotPanic: a nil cfg.Logger falls back
// to slog.Default() inside BuildProvider — tests must not panic when
// callers forget to wire a logger.
func TestBuildProvider_NilLoggerDoesNotPanic(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	cfg := ProviderConfig{Mode: "garbage"}
	p, _, err := BuildProvider(cfg)
	if err != nil {
		t.Fatalf("BuildProvider: %v", err)
	}
	if p.Name() != "deterministic" {
		t.Fatalf("Name=%q", p.Name())
	}
	if !strings.Contains(buf.String(), "unrecognized") {
		t.Fatalf("default logger did not receive warning; buf=%q", buf.String())
	}
}

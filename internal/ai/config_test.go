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
	t.Setenv(EnvCommitStrategy, "  intent  ")
	t.Setenv(EnvCommitFormat, "  conventional  ")
	t.Setenv(EnvIntentWindow, "25")
	t.Setenv(EnvIntentMinPending, "12")
	t.Setenv(EnvIntentSettleWindow, "12s")
	t.Setenv(EnvIntentMaxPendingAge, "90s")
	t.Setenv(EnvIntentRecentCommits, "8")
	t.Setenv(EnvIntentDeferLimit, "4")

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
	if cfg.IntentWindow != 25 {
		t.Fatalf("IntentWindow=%d want 25", cfg.IntentWindow)
	}
	if cfg.IntentMinPending != 12 {
		t.Fatalf("IntentMinPending=%d want 12", cfg.IntentMinPending)
	}
	if cfg.IntentSettleWindow != 12*time.Second {
		t.Fatalf("IntentSettleWindow=%v want 12s", cfg.IntentSettleWindow)
	}
	if cfg.IntentMaxPendingAge != 90*time.Second {
		t.Fatalf("IntentMaxPendingAge=%v want 90s", cfg.IntentMaxPendingAge)
	}
	if cfg.IntentRecentCommits != 8 {
		t.Fatalf("IntentRecentCommits=%d want 8", cfg.IntentRecentCommits)
	}
	if cfg.IntentDeferLimit != 4 {
		t.Fatalf("IntentDeferLimit=%d want 4", cfg.IntentDeferLimit)
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
	t.Setenv(EnvIntentWindow, "")
	t.Setenv(EnvIntentMinPending, "")
	t.Setenv(EnvIntentSettleWindow, "")
	t.Setenv(EnvIntentMaxPendingAge, "")
	t.Setenv(EnvIntentRecentCommits, "")
	t.Setenv(EnvIntentDeferLimit, "")

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
	if cfg.CommitStrategy != CommitStrategyEvent {
		t.Fatalf("CommitStrategy=%q want event", cfg.CommitStrategy)
	}
	if cfg.CommitFormat != CommitFormatImperative {
		t.Fatalf("CommitFormat=%q want imperative", cfg.CommitFormat)
	}
	if cfg.IntentWindow != DefaultIntentWindow {
		t.Fatalf("IntentWindow=%d want %d", cfg.IntentWindow, DefaultIntentWindow)
	}
	if cfg.IntentMinPending != DefaultIntentMinPending {
		t.Fatalf("IntentMinPending=%d want %d", cfg.IntentMinPending, DefaultIntentMinPending)
	}
	if cfg.IntentSettleWindow != DefaultIntentSettleWindow {
		t.Fatalf("IntentSettleWindow=%v want %v", cfg.IntentSettleWindow, DefaultIntentSettleWindow)
	}
	if cfg.IntentMaxPendingAge != DefaultIntentMaxPendingAge {
		t.Fatalf("IntentMaxPendingAge=%v want %v", cfg.IntentMaxPendingAge, DefaultIntentMaxPendingAge)
	}
	if cfg.IntentRecentCommits != DefaultIntentRecentCommits {
		t.Fatalf("IntentRecentCommits=%d want %d", cfg.IntentRecentCommits, DefaultIntentRecentCommits)
	}
	if cfg.IntentDeferLimit != DefaultIntentDeferLimit {
		t.Fatalf("IntentDeferLimit=%d want %d", cfg.IntentDeferLimit, DefaultIntentDeferLimit)
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
		{raw: "", want: CommitStrategyEvent},
		{raw: "event", want: CommitStrategyEvent},
		{raw: " EVENT ", want: CommitStrategyEvent},
		{raw: "intent", want: CommitStrategyIntent},
		{raw: " INTENT ", want: CommitStrategyIntent},
		{raw: "unknown", want: CommitStrategyEvent},
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

func TestLoadProviderConfigFromEnv_InvalidIntentNumbersFallBack(t *testing.T) {
	t.Setenv(EnvIntentWindow, "0")
	t.Setenv(EnvIntentMinPending, "0")
	t.Setenv(EnvIntentSettleWindow, "not-a-duration")
	t.Setenv(EnvIntentMaxPendingAge, "0")
	t.Setenv(EnvIntentRecentCommits, "not-a-number")
	t.Setenv(EnvIntentDeferLimit, "-1")

	cfg := LoadProviderConfigFromEnv()
	if cfg.IntentWindow != DefaultIntentWindow {
		t.Fatalf("IntentWindow=%d want %d", cfg.IntentWindow, DefaultIntentWindow)
	}
	if cfg.IntentMinPending != DefaultIntentMinPending {
		t.Fatalf("IntentMinPending=%d want %d", cfg.IntentMinPending, DefaultIntentMinPending)
	}
	if cfg.IntentSettleWindow != DefaultIntentSettleWindow {
		t.Fatalf("IntentSettleWindow=%v want %v", cfg.IntentSettleWindow, DefaultIntentSettleWindow)
	}
	if cfg.IntentMaxPendingAge != DefaultIntentMaxPendingAge {
		t.Fatalf("IntentMaxPendingAge=%v want %v", cfg.IntentMaxPendingAge, DefaultIntentMaxPendingAge)
	}
	if cfg.IntentRecentCommits != DefaultIntentRecentCommits {
		t.Fatalf("IntentRecentCommits=%d want %d", cfg.IntentRecentCommits, DefaultIntentRecentCommits)
	}
	if cfg.IntentDeferLimit != DefaultIntentDeferLimit {
		t.Fatalf("IntentDeferLimit=%d want %d", cfg.IntentDeferLimit, DefaultIntentDeferLimit)
	}
}

func TestLoadProviderConfigFromEnv_InvalidIntentMaxPendingAgeFallsBack(t *testing.T) {
	t.Setenv(EnvIntentMaxPendingAge, "not-a-duration")
	cfg := LoadProviderConfigFromEnv()
	if cfg.IntentMaxPendingAge != DefaultIntentMaxPendingAge {
		t.Fatalf("IntentMaxPendingAge=%v want %v", cfg.IntentMaxPendingAge, DefaultIntentMaxPendingAge)
	}
}

func TestLoadProviderConfigFromEnv_ZeroIntentDeferLimitAllowed(t *testing.T) {
	t.Setenv(EnvIntentDeferLimit, "0")
	cfg := LoadProviderConfigFromEnv()
	if cfg.IntentDeferLimit != 0 {
		t.Fatalf("IntentDeferLimit=%d want 0", cfg.IntentDeferLimit)
	}
}

func TestLoadProviderConfigFromEnv_ZeroIntentSettleWindowAllowed(t *testing.T) {
	for _, raw := range []string{"0", "0s"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(EnvIntentSettleWindow, raw)
			cfg := LoadProviderConfigFromEnv()
			if cfg.IntentSettleWindow != 0 {
				t.Fatalf("IntentSettleWindow=%v want 0", cfg.IntentSettleWindow)
			}
		})
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

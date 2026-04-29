// config.go — env-driven Provider selection for the daemon's replay path.
//
// Per spec §10.4, the run loop picks a commit-message provider by reading
// ACD_AI_* environment variables. The defaults are conservative: an empty
// or unrecognized ACD_AI_PROVIDER falls back to the deterministic
// generator so a misconfigured environment never silently disables
// commit-message generation.
//
// Selection table:
//
//	ACD_AI_PROVIDER             | Resolved Provider chain
//	----------------------------|--------------------------------------
//	"" (unset)                  | DeterministicProvider
//	"deterministic"             | DeterministicProvider
//	"openai-compat" + APIKey    | Compose(OpenAIProvider, Deterministic)
//	"openai-compat" no APIKey   | DeterministicProvider (warn)
//	"subprocess:<name>"         | Compose(Subprocess(name), Deterministic)
//	any other value             | DeterministicProvider (warn)
//
// The returned io.Closer is non-nil only when the chain holds a
// SubprocessProvider; the daemon must Close() it on shutdown so the child
// process is reaped cleanly.
package ai

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env var names — kept exported so the CLI / docs can reference them
// without re-typing the literal strings.
const (
	EnvProvider = "ACD_AI_PROVIDER"
	EnvBaseURL  = "ACD_AI_BASE_URL"
	EnvAPIKey   = "ACD_AI_API_KEY"
	EnvModel    = "ACD_AI_MODEL"
	EnvTimeout  = "ACD_AI_TIMEOUT"
)

// DefaultProviderTimeout is the per-request timeout applied to the
// OpenAI-compat HTTP provider when ACD_AI_TIMEOUT is unset or invalid.
const DefaultProviderTimeout = 30 * time.Second

// ProviderConfig captures the env-driven configuration for the replay
// provider chain. Mode is the user-facing selector; the remaining fields
// are consumed by the OpenAI-compat provider (or ignored when irrelevant).
type ProviderConfig struct {
	// Mode is one of "deterministic", "openai-compat", or
	// "subprocess:<name>". An empty Mode is treated as "deterministic".
	// Unknown values fall back to "deterministic" with a warning log.
	Mode string

	// BaseURL is the OpenAI-compatible chat-completions root. Empty
	// resolves to DefaultOpenAIBaseURL.
	BaseURL string

	// APIKey is the bearer token for the OpenAI-compat provider. Empty
	// when ACD_AI_PROVIDER=openai-compat causes a warn-and-degrade to
	// deterministic.
	APIKey string

	// Model is the OpenAI-compat model name. Empty resolves to
	// DefaultOpenAIModel.
	Model string

	// Timeout is the per-request timeout for the OpenAI-compat HTTP
	// provider and the subprocess plugin's per-request budget. Zero
	// resolves to DefaultProviderTimeout.
	Timeout time.Duration

	// Logger receives warning logs from BuildProvider's degraded paths.
	// Nil falls back to slog.Default().
	Logger *slog.Logger
}

// LoadProviderConfigFromEnv reads ACD_AI_* env vars and returns a
// ProviderConfig with safe defaults applied. The mode prefix is
// lowercased and trimmed; the subprocess plugin name (after the colon) is
// preserved verbatim because plugin binaries on $PATH are case-sensitive
// on Linux.
func LoadProviderConfigFromEnv() ProviderConfig {
	cfg := ProviderConfig{
		Mode:    normalizeMode(os.Getenv(EnvProvider)),
		BaseURL: strings.TrimSpace(os.Getenv(EnvBaseURL)),
		APIKey:  strings.TrimSpace(os.Getenv(EnvAPIKey)),
		Model:   strings.TrimSpace(os.Getenv(EnvModel)),
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultOpenAIBaseURL
	}
	if cfg.Model == "" {
		cfg.Model = DefaultOpenAIModel
	}
	if raw := strings.TrimSpace(os.Getenv(EnvTimeout)); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			cfg.Timeout = d
		} else if secs, err := strconv.ParseFloat(raw, 64); err == nil && secs > 0 {
			cfg.Timeout = time.Duration(secs * float64(time.Second))
		}
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultProviderTimeout
	}
	return cfg
}

// normalizeMode trims whitespace, lowercases the prefix (the part before
// any colon), and preserves the rest of the string (e.g. the subprocess
// plugin name) verbatim. An empty string remains empty so callers can
// distinguish "unset" from "explicitly deterministic" if needed.
func normalizeMode(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if i := strings.IndexByte(raw, ':'); i >= 0 {
		return strings.ToLower(raw[:i]) + raw[i:]
	}
	return strings.ToLower(raw)
}

// BuildProvider returns a Provider chain matching cfg. The io.Closer is
// non-nil only when the chain owns a SubprocessProvider — the daemon must
// call Close on shutdown so the child process is reaped cleanly. The
// error return is reserved for future use (today every degraded path
// resolves to deterministic rather than failing); callers should still
// check it.
//
// Degraded paths log a single warning via cfg.Logger so an operator can
// see why the OpenAI-compat or subprocess provider was skipped.
func BuildProvider(cfg ProviderConfig) (Provider, io.Closer, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	det := DeterministicProvider{}

	mode := cfg.Mode
	switch {
	case mode == "" || mode == "deterministic":
		return det, nil, nil

	case mode == "openai-compat":
		baseURL, err := normalizeOpenAIBaseURL(cfg.BaseURL, true)
		if err != nil {
			return nil, nil, err
		}
		if cfg.APIKey == "" {
			logger.Warn("ai: ACD_AI_PROVIDER=openai-compat but ACD_AI_API_KEY empty; falling back to deterministic",
				slog.String("provider", "openai-compat"))
			return det, nil, nil
		}
		primary := &OpenAIProvider{
			BaseURL: baseURL,
			APIKey:  cfg.APIKey,
			Model:   cfg.Model,
			HTTP:    nil, // provider lazy-builds a redirect-refusing client
		}
		return Compose(primary, det), nil, nil

	case strings.HasPrefix(mode, "subprocess:"):
		name := strings.TrimPrefix(mode, "subprocess:")
		if strings.TrimSpace(name) == "" {
			logger.Warn("ai: ACD_AI_PROVIDER=subprocess: missing plugin name; falling back to deterministic",
				slog.String("mode", mode))
			return det, nil, nil
		}
		sp := NewSubprocessProvider(name, SubprocessOptions{
			Timeout: cfg.Timeout,
			Logger:  logger,
		})
		return Compose(sp, det), sp, nil

	default:
		logger.Warn("ai: unrecognized ACD_AI_PROVIDER; falling back to deterministic",
			slog.String("mode", mode))
		return det, nil, nil
	}
}

// errProviderUnused is reserved so future BuildProvider expansion (e.g. a
// hard-fail mode) can surface a typed error without changing the
// signature. Currently unused.
var errProviderUnused = errors.New("ai: provider configuration error") //nolint:unused

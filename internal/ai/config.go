// Provider settings validate locally; transports are built only for explicit requests.
package ai

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// Env var names — kept exported so the CLI / docs can reference them
// without re-typing the literal strings.
const (
	EnvProvider       = "ACD_AI_PROVIDER"
	EnvBaseURL        = "ACD_AI_BASE_URL"
	EnvAPIKey         = "ACD_AI_API_KEY"
	EnvModel          = "ACD_AI_MODEL"
	EnvTimeout        = "ACD_AI_TIMEOUT"
	EnvCAFile         = "ACD_AI_CA_FILE"
	EnvDiffEgress     = "ACD_AI_DIFF_EGRESS"
	EnvCommitStrategy = "ACD_COMMIT_STRATEGY"
	EnvCommitFormat   = "ACD_COMMIT_FORMAT"
)

// DefaultProviderTimeout is the per-request timeout applied to the
// OpenAI-compat HTTP provider when ACD_AI_TIMEOUT is unset or invalid.
const DefaultProviderTimeout = 5 * time.Minute

// ErrIntentDiffsRequired keeps AI planning from substituting metadata for code.
var ErrIntentDiffsRequired = errors.New("Intent requires redacted code diffs; enable ai.diff_egress and approve sharing in acd config, or choose Local mode")

// CommitStrategy selects how an explicit finish plans its commits.
type CommitStrategy string

const (
	CommitStrategyLocal  CommitStrategy = "local"
	CommitStrategyIntent CommitStrategy = "intent"
)

// CommitFormat selects the subject-line contract providers must satisfy.
// Imperative is the historical default and remains unchanged unless the
// operator explicitly opts into another supported format.
type CommitFormat string

const (
	CommitFormatImperative   CommitFormat = "imperative"
	CommitFormatConventional CommitFormat = "conventional"
)

// ProviderConfig contains the connection and privacy settings for explicit requests.
type ProviderConfig struct {
	// Mode is one of "deterministic", "openai-compat", or
	// "subprocess:<name>". An empty Mode is treated as "deterministic".
	// Unknown values are rejected. Local mode never constructs a transport.
	Mode string

	// BaseURL is the OpenAI-compatible chat-completions root. Empty
	// resolves to DefaultOpenAIBaseURL.
	BaseURL string

	// APIKey is required for OpenAI-compatible requests.
	APIKey string

	// Model is the OpenAI-compat model name. Empty resolves to
	// DefaultOpenAIModel.
	Model string

	// Timeout is the per-request timeout for the OpenAI-compat HTTP
	// provider and the subprocess plugin's per-request budget. Zero
	// resolves to DefaultProviderTimeout.
	Timeout time.Duration

	// MaxInputBytes bounds complete diff evidence and encoded planning requests.
	// Zero uses the default. It does not alter the provider's context window.
	MaxInputBytes int

	// CAFile is an optional PEM bundle used to trust a private
	// OpenAI-compatible HTTPS gateway.
	CAFile string

	// DiffEgress records whether repository diffs may be sent to a provider.
	// Provider construction does not consume it; validation surfaces a
	// separate confirmation requirement when it is enabled.
	DiffEgress bool

	// CommitStrategy chooses local or Intent planning for explicit finish.
	CommitStrategy CommitStrategy

	// CommitFormat chooses the commit-message subject format. The default
	// is imperative, preserving the existing ACD message contract.
	CommitFormat CommitFormat

	// Logger receives subprocess lifecycle diagnostics.
	// Nil falls back to slog.Default().
	Logger *slog.Logger

	// subprocessLookPath and subprocessStderr are test seams kept private so
	// runtime callers cannot accidentally replace subprocess security policy.
	subprocessLookPath LookPathFunc
	subprocessStderr   io.Writer
}

// ConfirmationRequirement identifies an effect that needs its own explicit
// operator consent before a provider can be tested or applied.
type ConfirmationRequirement string

const (
	ConfirmationEndpointCredentials         ConfirmationRequirement = "endpoint_credentials"
	ConfirmationInsecureEndpointCredentials ConfirmationRequirement = "insecure_endpoint_credentials"
	ConfirmationSubprocessExecution         ConfirmationRequirement = "subprocess_execution"
	ConfirmationDiffEgress                  ConfirmationRequirement = "diff_egress"
	ConfirmationVerificationCommand         ConfirmationRequirement = "verification_command"
)

// ProviderValidation is the non-secret result of validating provider
// settings. Confirmations are stable identifiers suitable for a UI; they
// never contain endpoint paths, credentials, or provider response text.
type ProviderValidation struct {
	Confirmations []ConfirmationRequirement
}

func normalizeCommitFormat(raw string) CommitFormat {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(CommitFormatImperative):
		return CommitFormatImperative
	case string(CommitFormatConventional):
		return CommitFormatConventional
	default:
		return CommitFormatImperative
	}
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

// buildFinishTransport validates the selected connection, including subprocess
// availability, and returns the primary provider without planning retries.
// Callers own and must close closer.
func buildFinishTransport(cfg ProviderConfig) (FinishTransport, io.Closer, error) {
	mode := normalizeMode(cfg.Mode)
	if mode == "" {
		mode = "deterministic"
	}
	cfg.Mode = mode
	if _, err := ValidateProviderConfig(cfg); err != nil {
		return nil, nil, err
	}
	primary, closer, err := buildPrimaryFinishTransport(cfg)
	if err != nil {
		return nil, nil, sanitizeProviderError(err)
	}
	if sp, ok := primary.(*SubprocessProvider); ok && sp.resolveErr != nil {
		_ = sp.Close()
		return nil, nil, sanitizeProviderError(sp.resolveErr)
	}
	return primary, closer, nil
}

// ValidateProviderConfig validates provider construction inputs and returns
// effects requiring separate explicit confirmation. It performs no network
// request and does not start a subprocess.
func ValidateProviderConfig(cfg ProviderConfig) (ProviderValidation, error) {
	mode := normalizeMode(cfg.Mode)
	if mode == "" {
		mode = "deterministic"
	}
	if cfg.CommitStrategy == CommitStrategyIntent && mode == "deterministic" {
		return ProviderValidation{}, errors.New("ai: Intent requires an AI provider; choose local mode or configure an approved OpenAI-compatible or subprocess provider")
	}
	if cfg.CommitStrategy == CommitStrategyIntent && !cfg.DiffEgress {
		return ProviderValidation{}, ErrIntentDiffsRequired
	}
	validation := ProviderValidation{}
	switch {
	case mode == "deterministic":
	case mode == "openai-compat":
		baseURL, err := normalizeOpenAIBaseURL(cfg.BaseURL, false)
		if err != nil {
			return ProviderValidation{}, sanitizeProviderError(err)
		}
		if strings.TrimSpace(cfg.APIKey) == "" {
			return ProviderValidation{}, errors.New("openai-compat: missing API key")
		}
		if err := validateProviderModel(cfg.Model); err != nil {
			return ProviderValidation{}, err
		}
		if _, err := openAIHTTPClient(cfg.Timeout, cfg.CAFile); err != nil {
			return ProviderValidation{}, sanitizeProviderError(err)
		}
		defaultURL, _ := normalizeOpenAIBaseURL(DefaultOpenAIBaseURL, true)
		if baseURL != defaultURL {
			validation.Confirmations = append(validation.Confirmations, ConfirmationEndpointCredentials)
		}
		if strings.HasPrefix(baseURL, "http://") {
			validation.Confirmations = append(validation.Confirmations, ConfirmationInsecureEndpointCredentials)
		}
	case strings.HasPrefix(mode, "subprocess:"):
		if strings.TrimSpace(strings.TrimPrefix(mode, "subprocess:")) == "" {
			return ProviderValidation{}, errors.New("subprocess: plugin name is empty")
		}
		validation.Confirmations = append(validation.Confirmations, ConfirmationSubprocessExecution)
	default:
		return ProviderValidation{}, fmt.Errorf("ai: unknown provider mode %q", sanitizeProviderLabel(mode))
	}
	if cfg.DiffEgress {
		validation.Confirmations = append(validation.Confirmations, ConfirmationDiffEgress)
	}
	return validation, nil
}

func buildPrimaryFinishTransport(cfg ProviderConfig) (FinishTransport, io.Closer, error) {
	mode := normalizeMode(cfg.Mode)
	if mode == "" || mode == "deterministic" {
		return nil, nil, errors.New("ai: explicit provider request requires a configured transport")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if mode == "openai-compat" {
		baseURL, err := normalizeOpenAIBaseURL(cfg.BaseURL, false)
		if err != nil {
			return nil, nil, err
		}
		httpClient, err := openAIHTTPClient(cfg.Timeout, cfg.CAFile)
		if err != nil {
			return nil, nil, err
		}
		primary := &OpenAIProvider{BaseURL: baseURL, APIKey: cfg.APIKey,
			Model: cfg.Model, HTTP: httpClient}
		if strings.TrimSpace(cfg.CAFile) != "" {
			return primary, httpIdleConnectionCloser{client: httpClient}, nil
		}
		return primary, nil, nil
	}
	if strings.HasPrefix(mode, "subprocess:") {
		name := strings.TrimPrefix(mode, "subprocess:")
		sp := NewSubprocessProvider(name, SubprocessOptions{
			Timeout: cfg.Timeout, Logger: logger,
			LookPath: cfg.subprocessLookPath, Stderr: cfg.subprocessStderr,
		})
		return sp, sp, nil
	}
	return nil, nil, fmt.Errorf("ai: unknown provider mode %q", sanitizeProviderLabel(mode))
}

type httpIdleConnectionCloser struct{ client *http.Client }

func (c httpIdleConnectionCloser) Close() error {
	if c.client != nil {
		c.client.CloseIdleConnections()
	}
	return nil
}

func validateProviderModel(model string) error {
	model = strings.TrimSpace(model)
	if model == "" {
		return errors.New("openai-compat: model is required")
	}
	if len(model) > 256 || strings.ContainsAny(model, "\r\n\t") {
		return errors.New("openai-compat: model is invalid")
	}
	return nil
}

func sanitizeProviderLabel(label string) string {
	clean := SanitizePlannerError(label)
	if clean == "" {
		return "[invalid]"
	}
	return clean
}

func sanitizeProviderError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(SanitizePlannerError(err.Error()))
}

func openAIHTTPClient(timeout time.Duration, caFile string) (*http.Client, error) {
	client := defaultOpenAIClient()
	if timeout > 0 {
		client.Timeout = timeout
	}
	caFile = strings.TrimSpace(caFile)
	if caFile == "" {
		return client, nil
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("openai-compat: read ACD_AI_CA_FILE: %w", err)
	}
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("openai-compat: ACD_AI_CA_FILE contained no PEM certificates")
	}
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("openai-compat: default HTTP transport has unexpected type")
	}
	cloned := transport.Clone()
	cloned.TLSClientConfig = &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
	client.Transport = cloned
	return client, nil
}

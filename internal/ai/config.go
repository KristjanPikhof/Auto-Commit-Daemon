// config.go — env-driven Provider selection for the daemon's replay path.
//
// Per spec §10.4, the run loop picks a commit-message provider by reading
// ACD_AI_* environment variables. The defaults are conservative: an empty
// or unrecognized ACD_AI_PROVIDER falls back to the deterministic generator
// in event mode. Intent mode keeps explicitly semantic configuration fail
// closed so setup errors cannot silently change commit semantics.
//
// Selection table:
//
//	ACD_AI_PROVIDER             | Resolved Provider chain
//	----------------------------|--------------------------------------
//	"" (unset)                  | DeterministicProvider
//	"deterministic"             | DeterministicProvider
//	"openai-compat" + APIKey    | Compose(OpenAIProvider, Deterministic)
//	"openai-compat" no APIKey   | Event: deterministic; Intent: error
//	"subprocess:<name>"         | Compose(Subprocess(name), Deterministic)
//	any other value             | Event: deterministic; Intent: error
//
// The returned io.Closer is non-nil only when the chain holds a
// SubprocessProvider; the daemon must Close() it on shutdown so the child
// process is reaped cleanly.
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
	"strconv"
	"strings"
	"time"
)

// Env var names — kept exported so the CLI / docs can reference them
// without re-typing the literal strings.
const (
	EnvProvider             = "ACD_AI_PROVIDER"
	EnvBaseURL              = "ACD_AI_BASE_URL"
	EnvAPIKey               = "ACD_AI_API_KEY"
	EnvModel                = "ACD_AI_MODEL"
	EnvTimeout              = "ACD_AI_TIMEOUT"
	EnvCAFile               = "ACD_AI_CA_FILE"
	EnvDiffEgress           = "ACD_AI_DIFF_EGRESS"
	EnvCommitStrategy       = "ACD_COMMIT_STRATEGY"
	EnvCommitFormat         = "ACD_COMMIT_FORMAT"
	EnvIntentWindow         = "ACD_INTENT_WINDOW"
	EnvIntentMinPending     = "ACD_INTENT_MIN_PENDING"
	EnvIntentSettleWindow   = "ACD_INTENT_SETTLE_WINDOW"
	EnvIntentMaxPendingAge  = "ACD_INTENT_MAX_PENDING_AGE"
	EnvIntentRecentCommits  = "ACD_INTENT_RECENT_COMMITS"
	EnvIntentDeferLimit     = "ACD_INTENT_DEFER_LIMIT"
	EnvIntentRetryOnInvalid = "ACD_INTENT_RETRY_ON_INVALID"
)

// DefaultProviderTimeout is the per-request timeout applied to the
// OpenAI-compat HTTP provider when ACD_AI_TIMEOUT is unset or invalid.
const DefaultProviderTimeout = 5 * time.Minute

const (
	DefaultIntentWindow        = 10
	DefaultIntentMinPending    = 10
	DefaultIntentSettleWindow  = 10 * time.Second
	DefaultIntentMaxPendingAge = 5 * time.Minute
	DefaultIntentRecentCommits = 5
	// DefaultIntentRetryOnInvalid caps correction retries after typed
	// planner validation errors. The first planner call is not counted as
	// a retry, so the default allows up to three total planner attempts:
	// initial attempt plus two correction attempts with RetryCorrection.
	DefaultIntentRetryOnInvalid = 2
	// DefaultIntentDeferLimit was 2 prior to the Wave 2 planner-atomicity
	// epic. The retry loop in composed.PlanIntent (typed validation errors
	// can trigger correction re-prompts) plus the rejects-log forensic surface mean
	// an event that has already been deferred once is overwhelmingly
	// likely to be planner churn rather than a legitimate "wait for
	// related work" decision. Lowering the default to 1 forces the
	// daemon's forced-aging singleton path sooner so deferred work lands
	// promptly. Operators who want the historical behaviour can still set
	// ACD_INTENT_DEFER_LIMIT=2 (or higher) explicitly; the env override
	// is unchanged.
	DefaultIntentDeferLimit = 1
)

// CommitStrategy selects how pending capture events are turned into commits.
type CommitStrategy string

const (
	CommitStrategyEvent  CommitStrategy = "event"
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

	// CAFile is an optional PEM bundle used to trust a private
	// OpenAI-compatible HTTPS gateway.
	CAFile string

	// DiffEgress records whether repository diffs may be sent to a provider.
	// Provider construction does not consume it; validation surfaces a
	// separate confirmation requirement when it is enabled.
	DiffEgress bool

	// CommitStrategy chooses the replay planner. The default is event,
	// preserving one capture event per commit until intent planning is
	// explicitly enabled.
	CommitStrategy CommitStrategy

	// CommitFormat chooses the commit-message subject format. The default
	// is imperative, preserving the existing ACD message contract.
	CommitFormat CommitFormat

	// IntentWindow caps how many pending events the intent planner may
	// consider at once.
	IntentWindow int

	// IntentMinPending is the preferred pending-count gate before a normal
	// intent planning pass starts.
	IntentMinPending int

	// IntentSettleWindow is the extra burst-settle delay after the pending
	// queue reaches IntentMinPending. Zero disables the settle gate.
	IntentSettleWindow time.Duration

	// IntentMaxPendingAge is the bounded wait escape hatch for sparse
	// pending queues that have not reached IntentMinPending.
	IntentMaxPendingAge time.Duration

	// IntentRecentCommits caps recent commit context supplied to intent
	// planning.
	IntentRecentCommits int

	// IntentDeferLimit caps how many times an event may be deferred before
	// the planner must surface it as overdue.
	IntentDeferLimit int

	// Logger receives warning logs from BuildProvider's degraded paths.
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
	ConfirmationIntentRepair                ConfirmationRequirement = "intent_repair"
)

// ProviderValidation is the non-secret result of validating provider
// settings. Confirmations are stable identifiers suitable for a UI; they
// never contain endpoint paths, credentials, or provider response text.
type ProviderValidation struct {
	Confirmations []ConfirmationRequirement
}

// LoadProviderConfigFromEnv reads ACD_AI_* env vars and returns a
// ProviderConfig with safe defaults applied. The mode prefix is
// lowercased and trimmed; the subprocess plugin name (after the colon) is
// preserved verbatim because plugin binaries on $PATH are case-sensitive
// on Linux.
func LoadProviderConfigFromEnv() ProviderConfig {
	cfg := ProviderConfig{
		Mode:                normalizeMode(os.Getenv(EnvProvider)),
		BaseURL:             strings.TrimSpace(os.Getenv(EnvBaseURL)),
		APIKey:              strings.TrimSpace(os.Getenv(EnvAPIKey)),
		Model:               strings.TrimSpace(os.Getenv(EnvModel)),
		CAFile:              strings.TrimSpace(os.Getenv(EnvCAFile)),
		DiffEgress:          envTruthy(os.Getenv(EnvDiffEgress)),
		CommitStrategy:      normalizeCommitStrategy(os.Getenv(EnvCommitStrategy)),
		CommitFormat:        normalizeCommitFormat(os.Getenv(EnvCommitFormat)),
		IntentWindow:        parsePositiveIntEnv(EnvIntentWindow, DefaultIntentWindow),
		IntentMinPending:    parsePositiveIntEnv(EnvIntentMinPending, DefaultIntentMinPending),
		IntentSettleWindow:  parseNonNegativeDurationEnv(EnvIntentSettleWindow, DefaultIntentSettleWindow),
		IntentMaxPendingAge: parsePositiveDurationEnv(EnvIntentMaxPendingAge, DefaultIntentMaxPendingAge),
		IntentRecentCommits: parsePositiveIntEnv(EnvIntentRecentCommits, DefaultIntentRecentCommits),
		IntentDeferLimit:    parseNonNegativeIntEnv(EnvIntentDeferLimit, DefaultIntentDeferLimit),
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

func envTruthy(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on", "enabled":
		return true
	default:
		return false
	}
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

func normalizeCommitStrategy(raw string) CommitStrategy {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(CommitStrategyEvent):
		return CommitStrategyEvent
	case string(CommitStrategyIntent):
		return CommitStrategyIntent
	default:
		return CommitStrategyEvent
	}
}

func parsePositiveIntEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func parseNonNegativeIntEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func parsePositiveDurationEnv(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func parseNonNegativeDurationEnv(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	if raw == "0" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return fallback
	}
	return d
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
// Event mode retains the historical deterministic degradation. Intent mode
// fails construction when the operator explicitly selected a semantic
// provider, preventing provider setup failures from changing commit semantics.
//
// Degraded paths log a single warning via cfg.Logger so an operator can
// see why the OpenAI-compat or subprocess provider was skipped.
func BuildProvider(cfg ProviderConfig) (Provider, io.Closer, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	det := DeterministicProvider{CommitFormat: cfg.CommitFormat}

	mode := cfg.Mode
	switch {
	case mode == "" || mode == "deterministic":
		return det, nil, nil

	case mode == "openai-compat":
		if strings.TrimSpace(cfg.APIKey) == "" {
			if cfg.CommitStrategy == CommitStrategyIntent {
				return nil, nil, errors.New(
					"openai-compat: missing API key for Intent commits")
			}
			logger.Warn("ai: ACD_AI_PROVIDER=openai-compat but ACD_AI_API_KEY empty; falling back to deterministic",
				slog.String("provider", "openai-compat"))
			return det, nil, nil
		}
		primary, closer, err := buildPrimaryProvider(cfg)
		if err != nil {
			return nil, nil, err
		}
		return Compose(primary, det), closer, nil

	case strings.HasPrefix(mode, "subprocess:"):
		name := strings.TrimPrefix(mode, "subprocess:")
		if strings.TrimSpace(name) == "" {
			if cfg.CommitStrategy == CommitStrategyIntent {
				return nil, nil, errors.New(
					"subprocess: missing plugin name for Intent commits")
			}
			logger.Warn("ai: ACD_AI_PROVIDER=subprocess: missing plugin name; falling back to deterministic",
				slog.String("mode", mode))
			return det, nil, nil
		}
		primary, closer, err := buildPrimaryProvider(cfg)
		if err != nil {
			return nil, nil, err
		}
		return Compose(primary, det), closer, nil

	default:
		if cfg.CommitStrategy == CommitStrategyIntent {
			return nil, nil, fmt.Errorf(
				"unrecognized Intent provider %q", mode)
		}
		logger.Warn("ai: unrecognized ACD_AI_PROVIDER; falling back to deterministic",
			slog.String("mode", mode))
		return det, nil, nil
	}
}

// BuildStrictProvider constructs the selected provider without a deterministic
// fallback. Unlike BuildProvider it rejects degraded configuration and an
// unavailable subprocess immediately, so a settings test cannot report a
// synthetic fallback as provider success. Callers own and must close closer.
func BuildStrictProvider(cfg ProviderConfig) (Provider, io.Closer, error) {
	mode := normalizeMode(cfg.Mode)
	if mode == "" {
		mode = "deterministic"
	}
	cfg.Mode = mode
	if _, err := ValidateProviderConfig(cfg); err != nil {
		return nil, nil, err
	}
	primary, closer, err := buildPrimaryProvider(cfg)
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

func buildPrimaryProvider(cfg ProviderConfig) (Provider, io.Closer, error) {
	mode := normalizeMode(cfg.Mode)
	if mode == "" || mode == "deterministic" {
		return DeterministicProvider{CommitFormat: cfg.CommitFormat}, nil, nil
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
			Model: cfg.Model, HTTP: httpClient, Format: cfg.CommitFormat}
		if strings.TrimSpace(cfg.CAFile) != "" {
			return primary, httpIdleConnectionCloser{client: httpClient}, nil
		}
		return primary, nil, nil
	}
	if strings.HasPrefix(mode, "subprocess:") {
		name := strings.TrimPrefix(mode, "subprocess:")
		sp := NewSubprocessProvider(name, SubprocessOptions{
			Timeout: cfg.Timeout, Logger: logger, CommitFormat: cfg.CommitFormat,
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

// errProviderUnused is reserved so future BuildProvider expansion (e.g. a
// hard-fail mode) can surface a typed error without changing the
// signature. Currently unused.
var errProviderUnused = errors.New("ai: provider configuration error") //nolint:unused

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

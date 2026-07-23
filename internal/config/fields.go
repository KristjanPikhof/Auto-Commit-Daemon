package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type ApplyBoundary string

const (
	ApplyHot     ApplyBoundary = "next_work_boundary"
	ApplyRestart ApplyBoundary = "restart_required"
)

type ValueKind string

const (
	KindString   ValueKind = "string"
	KindBool     ValueKind = "boolean"
	KindInteger  ValueKind = "integer"
	KindDuration ValueKind = "duration"
)

const (
	FieldProvider             = "ai.provider"
	FieldBaseURL              = "ai.base_url"
	FieldAPIKey               = "ai.api_key"
	FieldModel                = "ai.model"
	FieldTimeout              = "ai.timeout"
	FieldCAFile               = "ai.ca_file"
	FieldDiffEgress           = "ai.diff_egress"
	FieldCommitStrategy       = "commit.strategy"
	FieldCommitFormat         = "commit.format"
	FieldIntentWindow         = "intent.window"
	FieldIntentMinPending     = "intent.min_pending"
	FieldIntentSettleWindow   = "intent.settle_window"
	FieldIntentMaxPendingAge  = "intent.max_pending_age"
	FieldIntentRecentCommits  = "intent.recent_commits"
	FieldIntentDeferLimit     = "intent.defer_limit"
	FieldIntentRetryOnInvalid = "intent.retry_on_invalid"
	FieldIntentPathCoalescing = "intent.path_coalescing"
)

type FieldDefinition struct {
	Name         string
	Environment  string
	Default      string
	Kind         ValueKind
	Choices      []string
	Minimum      int64
	AllowZero    bool
	PlainSeconds bool
	Boundary     ApplyBoundary
	Sensitive    bool
	Persistable  bool
}

var fieldCatalog = []FieldDefinition{
	{Name: FieldProvider, Environment: "ACD_AI_PROVIDER", Default: "deterministic", Kind: KindString, Boundary: ApplyHot, Persistable: true},
	{Name: FieldBaseURL, Environment: "ACD_AI_BASE_URL", Default: "https://api.openai.com/v1", Kind: KindString, Boundary: ApplyHot, Persistable: true},
	{Name: FieldAPIKey, Environment: "ACD_AI_API_KEY", Kind: KindString, Boundary: ApplyHot, Sensitive: true, Persistable: false},
	{Name: FieldModel, Environment: "ACD_AI_MODEL", Default: "gpt-5.4-mini", Kind: KindString, Boundary: ApplyHot, Persistable: true},
	{Name: FieldTimeout, Environment: "ACD_AI_TIMEOUT", Default: "30s", Kind: KindDuration, PlainSeconds: true, Boundary: ApplyHot, Persistable: true},
	{Name: FieldCAFile, Environment: "ACD_AI_CA_FILE", Kind: KindString, Boundary: ApplyHot, Persistable: true},
	{Name: FieldDiffEgress, Environment: "ACD_AI_DIFF_EGRESS", Default: "false", Kind: KindBool, Boundary: ApplyHot, Persistable: true},
	{Name: FieldCommitStrategy, Environment: "ACD_COMMIT_STRATEGY", Default: "event", Kind: KindString, Choices: []string{"event", "intent"}, Boundary: ApplyHot, Persistable: true},
	{Name: FieldCommitFormat, Environment: "ACD_COMMIT_FORMAT", Default: "imperative", Kind: KindString, Choices: []string{"imperative", "conventional"}, Boundary: ApplyHot, Persistable: true},
	{Name: FieldIntentWindow, Environment: "ACD_INTENT_WINDOW", Default: "10", Kind: KindInteger, Minimum: 1, Boundary: ApplyHot, Persistable: true},
	{Name: FieldIntentMinPending, Environment: "ACD_INTENT_MIN_PENDING", Default: "10", Kind: KindInteger, Minimum: 1, Boundary: ApplyHot, Persistable: true},
	{Name: FieldIntentSettleWindow, Environment: "ACD_INTENT_SETTLE_WINDOW", Default: "10s", Kind: KindDuration, AllowZero: true, Boundary: ApplyHot, Persistable: true},
	{Name: FieldIntentMaxPendingAge, Environment: "ACD_INTENT_MAX_PENDING_AGE", Default: "5m", Kind: KindDuration, Boundary: ApplyHot, Persistable: true},
	{Name: FieldIntentRecentCommits, Environment: "ACD_INTENT_RECENT_COMMITS", Default: "5", Kind: KindInteger, Minimum: 1, Boundary: ApplyHot, Persistable: true},
	{Name: FieldIntentDeferLimit, Environment: "ACD_INTENT_DEFER_LIMIT", Default: "1", Kind: KindInteger, AllowZero: true, Boundary: ApplyHot, Persistable: true},
	{Name: FieldIntentRetryOnInvalid, Environment: "ACD_INTENT_RETRY_ON_INVALID", Default: "2", Kind: KindInteger, AllowZero: true, Boundary: ApplyHot, Persistable: true},
	{Name: FieldIntentPathCoalescing, Environment: "ACD_INTENT_PATH_COALESCE", Default: "false", Kind: KindBool, Boundary: ApplyHot, Persistable: true},
	{Name: "capture.max_file_bytes", Environment: "ACD_MAX_FILE_BYTES", Default: "5242880", Kind: KindInteger, Minimum: 1, Boundary: ApplyRestart, Persistable: true},
	{Name: "capture.max_pending_events", Environment: "ACD_MAX_PENDING_EVENTS", Default: "50000", Kind: KindInteger, Minimum: 1, Boundary: ApplyRestart, Persistable: true},
	{Name: "capture.sensitive_globs", Environment: "ACD_SENSITIVE_GLOBS", Kind: KindString, Boundary: ApplyRestart, Sensitive: true, Persistable: true},
	{Name: "capture.safe_ignore", Environment: "ACD_SAFE_IGNORE", Default: "true", Kind: KindBool, Boundary: ApplyRestart, Persistable: true},
	{Name: "capture.safe_ignore_extra", Environment: "ACD_SAFE_IGNORE_EXTRA", Kind: KindString, Boundary: ApplyRestart, Persistable: true},
	{Name: "watch.fsnotify", Environment: "ACD_FSNOTIFY_ENABLED", Default: "false", Kind: KindBool, Boundary: ApplyRestart, Persistable: true},
	{Name: "trace.enabled", Environment: "ACD_TRACE", Default: "false", Kind: KindBool, Boundary: ApplyRestart, Persistable: true},
	{Name: "trace.prompt", Environment: "ACD_AI_PROMPT_TRACE", Default: "false", Kind: KindBool, Boundary: ApplyRestart, Sensitive: true, Persistable: true},
	{Name: "retention.event_days", Environment: "ACD_EVENT_RETENTION_DAYS", Default: "7", Kind: KindInteger, Minimum: 1, Boundary: ApplyRestart, Persistable: true},
	{Name: "recovery.rewind_grace", Environment: "ACD_REWIND_GRACE_SECONDS", Default: "60", Kind: KindInteger, AllowZero: true, Boundary: ApplyRestart, Persistable: true},
	{Name: "recovery.shadow_generations", Environment: "ACD_SHADOW_RETENTION_GENERATIONS", Default: "1", Kind: KindInteger, AllowZero: true, Boundary: ApplyRestart, Persistable: true},
	{Name: "client.ttl", Environment: "ACD_CLIENT_TTL_SECONDS", Default: "1800", Kind: KindInteger, Minimum: 1, Boundary: ApplyRestart, Persistable: true},
}

func Catalog() []FieldDefinition { return append([]FieldDefinition(nil), fieldCatalog...) }

func LookupField(name string) (FieldDefinition, bool) {
	for _, field := range fieldCatalog {
		if field.Name == name {
			return field, true
		}
	}
	return FieldDefinition{}, false
}

type Source string

const (
	SourceExperiment  Source = "experiment"
	SourceRepository  Source = "repository"
	SourceProfile     Source = "profile"
	SourceGlobal      Source = "global"
	SourceEnvironment Source = "environment"
	SourceDefault     Source = "default"
)

type ResolveInput struct {
	Experiment Overrides
	Repository Overrides
	Profile    Overrides
	Global     Overrides
	LookupEnv  func(string) (string, bool)
}

type ShadowedEnvironment struct {
	Set   bool
	Value string
}
type ResolvedField struct {
	Definition          FieldDefinition
	Value               string
	Source              Source
	ShadowedEnvironment *ShadowedEnvironment
	effectiveValue      string
}

// EffectiveValue returns the normalized value used by runtime consumers.
// Value remains presentation-safe and may contain only "set"/"unset" for
// sensitive fields, so callers must never render EffectiveValue directly.
func (r ResolvedField) EffectiveValue() string { return r.effectiveValue }

func ResolveField(name string, input ResolveInput) (ResolvedField, error) {
	field, ok := LookupField(name)
	if !ok {
		return ResolvedField{}, fmt.Errorf("acd config: unknown field %q", name)
	}
	lookup := input.LookupEnv
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	envValue, envSet := lookup(field.Environment)
	layers := []struct {
		source Source
		values Overrides
	}{
		{SourceExperiment, input.Experiment}, {SourceRepository, input.Repository},
		{SourceProfile, input.Profile}, {SourceGlobal, input.Global},
	}
	for _, layer := range layers {
		if raw, exists := layer.values[name]; exists {
			value, err := validateRaw(field, raw, true)
			if err != nil {
				return ResolvedField{}, err
			}
			result := ResolvedField{
				Definition: field, Value: displayValue(field, value),
				Source: layer.source, effectiveValue: value,
			}
			if envSet {
				result.ShadowedEnvironment = &ShadowedEnvironment{Set: true, Value: displayValue(field, envValue)}
			}
			return result, nil
		}
	}
	if envSet {
		value, err := normalizeValue(field, envValue)
		if err == nil {
			return ResolvedField{
				Definition: field, Value: displayValue(field, value),
				Source: SourceEnvironment, effectiveValue: value,
			}, nil
		}
	}
	value, err := normalizeValue(field, field.Default)
	if err != nil {
		return ResolvedField{}, err
	}
	return ResolvedField{
		Definition: field, Value: displayValue(field, value),
		Source: SourceDefault, effectiveValue: value,
	}, nil
}

// ResolveRestartEnvironment resolves the restart-bound catalog for one
// repository using the same precedence as the settings lab. Returned keys are
// environment variable names because the existing daemon consumers parse
// their established environment contracts at process startup.
func ResolveRestartEnvironment(
	doc *Document,
	repoHash string,
	lookupEnv func(string) (string, bool),
) (map[string]string, error) {
	if doc == nil {
		return nil, errors.New("acd config: nil document")
	}
	repo := doc.Settings.Repositories[repoHash]
	profile := doc.Settings.Profiles[repo.Profile]
	out := make(map[string]string)
	for _, field := range fieldCatalog {
		if field.Boundary != ApplyRestart {
			continue
		}
		resolved, err := ResolveField(field.Name, ResolveInput{
			Repository: repo.Fields,
			Profile:    profile.Fields,
			Global:     doc.Settings.Global,
			LookupEnv:  lookupEnv,
		})
		if err != nil {
			return nil, err
		}
		out[field.Environment] = resolved.EffectiveValue()
	}
	return out, nil
}

func ValidateDocument(doc *Document) error {
	if doc == nil {
		return errors.New("acd config: nil document")
	}
	if doc.Version != SettingsSchemaVersion {
		return fmt.Errorf("acd config: unsupported version %d", doc.Version)
	}
	if err := validateOverrides("global", doc.Settings.Global); err != nil {
		return err
	}
	for name, profile := range doc.Settings.Profiles {
		if strings.TrimSpace(name) == "" {
			return errors.New("acd config: empty profile name")
		}
		if err := validateOverrides("profile "+name, profile.Fields); err != nil {
			return err
		}
	}
	for hash, repo := range doc.Settings.Repositories {
		if strings.TrimSpace(hash) == "" {
			return errors.New("acd config: empty repository hash")
		}
		if repo.Profile != "" {
			if _, ok := doc.Settings.Profiles[repo.Profile]; !ok {
				return fmt.Errorf("acd config: repository %s selects unknown profile %q", hash, repo.Profile)
			}
		}
		if err := validateOverrides("repository "+hash, repo.Fields); err != nil {
			return err
		}
	}
	return nil
}

func validateOverrides(scope string, values Overrides) error {
	for name, raw := range values {
		field, ok := LookupField(name)
		if !ok {
			return fmt.Errorf("acd config: %s: unsupported field %q", scope, name)
		}
		if !field.Persistable {
			return fmt.Errorf("acd config: %s: field %q is secret or environment-only and cannot be persisted", scope, name)
		}
		if _, err := validateRaw(field, raw, true); err != nil {
			return fmt.Errorf("acd config: %s: %w", scope, err)
		}
	}
	return nil
}

func validateRaw(field FieldDefinition, raw json.RawMessage, persisted bool) (string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", fmt.Errorf("field %q: null is not persisted; remove the key to inherit", field.Name)
	}
	if persisted && !field.Persistable {
		return "", fmt.Errorf("field %q cannot be persisted", field.Name)
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("field %q: %w", field.Name, err)
	}
	var text string
	switch v := value.(type) {
	case string:
		text = v
	case bool:
		text = strconv.FormatBool(v)
	case float64:
		if v != float64(int64(v)) {
			return "", fmt.Errorf("field %q requires an integer", field.Name)
		}
		text = strconv.FormatInt(int64(v), 10)
	default:
		return "", fmt.Errorf("field %q requires a scalar value", field.Name)
	}
	return normalizeValue(field, text)
}

func normalizeValue(field FieldDefinition, raw string) (string, error) {
	value := strings.TrimSpace(raw)
	switch field.Kind {
	case KindString:
		if len(field.Choices) > 0 {
			for _, choice := range field.Choices {
				if value == choice {
					return value, nil
				}
			}
			return "", fmt.Errorf("field %q must be one of %s", field.Name, strings.Join(field.Choices, ", "))
		}
		if field.Name == FieldProvider && value != "" && value != "deterministic" && value != "openai-compat" && !strings.HasPrefix(value, "subprocess:") {
			return "", fmt.Errorf("field %q has unsupported provider %q", field.Name, value)
		}
		return value, nil
	case KindBool:
		switch strings.ToLower(value) {
		case "1", "true", "yes", "on", "enabled":
			return "true", nil
		case "0", "false", "no", "off", "disabled":
			return "false", nil
		default:
			return "", fmt.Errorf("field %q requires a boolean", field.Name)
		}
	case KindInteger:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < field.Minimum || (n == 0 && !field.AllowZero && field.Minimum == 0) {
			return "", fmt.Errorf("field %q requires an integer >= %d", field.Name, field.Minimum)
		}
		return strconv.FormatInt(n, 10), nil
	case KindDuration:
		if field.PlainSeconds {
			if seconds, err := strconv.ParseFloat(value, 64); err == nil &&
				!math.IsNaN(seconds) && !math.IsInf(seconds, 0) && seconds > 0 &&
				seconds <= float64(math.MaxInt64)/float64(time.Second) {
				duration := time.Duration(seconds * float64(time.Second))
				if duration > 0 {
					return duration.String(), nil
				}
			}
		}
		if field.AllowZero && value == "0" {
			return "0s", nil
		}
		duration, err := time.ParseDuration(value)
		if err != nil || duration < 0 || (!field.AllowZero && duration == 0) {
			return "", fmt.Errorf("field %q requires a valid duration", field.Name)
		}
		return duration.String(), nil
	default:
		return "", fmt.Errorf("field %q has unsupported type", field.Name)
	}
}

func displayValue(field FieldDefinition, value string) string {
	if field.Sensitive {
		if value == "" {
			return "unset"
		}
		return "set"
	}
	return value
}

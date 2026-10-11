package settingsui

import (
	"regexp"
	"strings"
	"unicode"
)

type FieldDescriptor struct {
	Key         string
	Label       string
	Description string
	Choices     []string
}

var fieldDescriptors = []FieldDescriptor{
	{Key: "ai.provider", Label: "AI provider", Description: "Provider used by Intent finish"},
	{Key: "ai.model", Label: "Model", Description: "Provider model identifier"},
	{Key: "ai.base_url", Label: "Endpoint", Description: "OpenAI-compatible endpoint"},
	{Key: "ai.timeout", Label: "Timeout", Description: "Bound for provider operations"},
	{Key: "ai.ca_file", Label: "CA file", Description: "Custom TLS certificate authority path"},
	{Key: "ai.api_key", Label: "API key", Description: "Environment or protected credential file"},
	{Key: "ai.diff_egress", Label: "Diff egress", Description: "Allow redacted repository diffs to eligible providers", Choices: []string{"false", "true"}},
	{Key: "commit.strategy", Label: "Finish mode", Description: "Local batch or AI goal grouping", Choices: []string{"local", "intent"}},
	{Key: "commit.format", Label: "Commit format", Description: "Imperative or conventional", Choices: []string{"imperative", "conventional"}},
	{Key: "intent.verification", Label: "Finish verification", Description: "Checks run before finish publishes commits", Choices: []string{"none", "structural", "fast", "full"}},
	{Key: "verification.fast.command", Label: "Fast verification command", Description: "Exact repository-approved shell command"},
	{Key: "verification.fast.timeout", Label: "Fast verification timeout", Description: "Bound for the approved fast command"},
	{Key: "verification.full.command", Label: "Full verification command", Description: "Exact repository-approved shell command"},
	{Key: "verification.full.timeout", Label: "Full verification timeout", Description: "Bound for the approved full command"},
	{Key: "capture.max_file_bytes", Label: "Maximum file bytes", Description: "Capture size limit per file"},
	{Key: "capture.sensitive_globs", Label: "Sensitive globs", Description: "Capture exclusion patterns"},
	{Key: "capture.safe_ignore", Label: "Safe ignore", Description: "Prune known generated directories", Choices: []string{"false", "true"}},
	{Key: "capture.safe_ignore_extra", Label: "Extra safe ignores", Description: "Additional generated directory patterns"},
	{Key: "watch.fsnotify", Label: "Filesystem notifications", Description: "Opt-in filesystem watcher", Choices: []string{"false", "true"}},
	{Key: "trace.enabled", Label: "Trace", Description: "Best-effort runtime JSONL trace", Choices: []string{"false", "true"}},
	{Key: "trace.prompt", Label: "Prompt trace", Description: "Sensitive prompt diagnostics", Choices: []string{"false", "true"}},
}

func safeText(s string) string {
	s = ansiRE.ReplaceAllString(s, "")
	s = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) && r != '\u007f' {
			return r
		}
		return ' '
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

func safePreviewValue(value string, limit int) string {
	value = safeText(value)
	if limit > 0 && len(value) > limit {
		value = value[:limit] + "..."
	}
	return value
}

var ansiRE = regexp.MustCompile(`\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))`)

func descriptor(key string) FieldDescriptor {
	for _, f := range fieldDescriptors {
		if f.Key == key {
			return f
		}
	}
	return FieldDescriptor{Key: safeText(key), Label: safeText(key)}
}

func fallback(value, defaultValue string) string {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	return value
}

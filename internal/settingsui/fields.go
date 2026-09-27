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
	{Key: "ai.provider", Label: "AI provider", Description: "Provider used for synthetic tests and future work"},
	{Key: "ai.model", Label: "Model", Description: "Provider model identifier"},
	{Key: "ai.base_url", Label: "Endpoint", Description: "OpenAI-compatible endpoint"},
	{Key: "ai.timeout", Label: "Timeout", Description: "Bound for provider operations"},
	{Key: "ai.ca_file", Label: "CA file", Description: "Custom TLS certificate authority path"},
	{Key: "ai.api_key", Label: "API key", Description: "Environment or protected credential file"},
	{Key: "ai.diff_egress", Label: "Diff egress", Description: "Allow redacted repository diffs to eligible providers", Choices: []string{"false", "true"}},
	{Key: "commit.strategy", Label: "Commit strategy", Description: "Event or intent grouping", Choices: []string{"event", "intent"}},
	{Key: "commit.preset", Label: "Commit preset", Description: "Fast, Balanced, or Quality preset identity", Choices: []string{"fast", "balanced", "quality"}},
	{Key: "commit.format", Label: "Commit format", Description: "Imperative or conventional", Choices: []string{"imperative", "conventional"}},
	{Key: "intent.window", Label: "Intent window", Description: "Maximum planner window size"},
	{Key: "intent.min_pending", Label: "Minimum pending", Description: "Pending events required before planning"},
	{Key: "intent.settle_window", Label: "Settle window", Description: "Quiet period before planning"},
	{Key: "intent.max_pending_age", Label: "Maximum pending age", Description: "Oldest event age before planning"},
	{Key: "intent.recent_commits", Label: "Recent commits", Description: "Recent history supplied to planner"},
	{Key: "intent.defer_limit", Label: "Defer limit", Description: "Maximum repeated event deferrals"},
	{Key: "intent.retry_on_invalid", Label: "Invalid retries", Description: "Planner validation retry budget"},
	{Key: "intent.path_coalescing", Label: "Path coalescing", Description: "Legacy same-path event coalescing", Choices: []string{"false", "true"}},
	{Key: "intent.repair.enabled", Label: "Automatic repair", Description: "Repair eligible recent soft-published ACD commits", Choices: []string{"false", "true"}},
	{Key: "intent.repair.horizon", Label: "Repair horizon", Description: "Maximum age of eligible soft-published commits"},
	{Key: "intent.repair.max_commits", Label: "Repair commit limit", Description: "Maximum automatic rewrite chain, capped at five"},
	{Key: "intent.verification", Label: "Intent verification", Description: "Structural, fast, or full verification", Choices: []string{"none", "structural", "fast", "full"}},
	{Key: "verification.fast.command", Label: "Fast verification command", Description: "Exact repository-approved shell command"},
	{Key: "verification.fast.timeout", Label: "Fast verification timeout", Description: "Bound for the approved fast command"},
	{Key: "verification.full.command", Label: "Full verification command", Description: "Exact repository-approved shell command"},
	{Key: "verification.full.timeout", Label: "Full verification timeout", Description: "Bound for the approved full command"},
	{Key: "capture.max_file_bytes", Label: "Maximum file bytes", Description: "Capture size limit per file"},
	{Key: "capture.max_pending_events", Label: "Maximum pending events", Description: "Backpressure event limit"},
	{Key: "capture.sensitive_globs", Label: "Sensitive globs", Description: "Capture exclusion patterns"},
	{Key: "capture.safe_ignore", Label: "Safe ignore", Description: "Prune known generated directories", Choices: []string{"false", "true"}},
	{Key: "capture.safe_ignore_extra", Label: "Extra safe ignores", Description: "Additional generated directory patterns"},
	{Key: "watch.fsnotify", Label: "Filesystem notifications", Description: "Opt-in filesystem watcher", Choices: []string{"false", "true"}},
	{Key: "trace.enabled", Label: "Trace", Description: "Best-effort runtime JSONL trace", Choices: []string{"false", "true"}},
	{Key: "trace.prompt", Label: "Prompt trace", Description: "Sensitive prompt diagnostics", Choices: []string{"false", "true"}},
	{Key: "retention.event_days", Label: "Event retention days", Description: "Published event retention period"},
	{Key: "recovery.rewind_grace", Label: "Rewind grace seconds", Description: "Same-branch rewind safety period"},
	{Key: "recovery.shadow_generations", Label: "Shadow generations", Description: "Old shadow generation retention"},
	{Key: "client.ttl", Label: "Client TTL", Description: "Inactive client expiry seconds"},
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

package settingsui

import (
	"regexp"
	"sort"
	"strings"
	"unicode"
)

type FieldDescriptor struct {
	Key         string
	Label       string
	Description string
	Apply       string
	Sensitive   bool
	Choices     []string
}

var fieldDescriptors = []FieldDescriptor{
	{Key: "ai.provider", Label: "Provider", Description: "Provider used for synthetic tests and future work", Apply: "next safe boundary"},
	{Key: "ai.model", Label: "Model", Description: "Provider model identifier", Apply: "next safe boundary"},
	{Key: "ai.base_url", Label: "Base URL", Description: "OpenAI-compatible endpoint", Apply: "next safe boundary"},
	{Key: "ai.timeout", Label: "Timeout", Description: "Bound for provider operations", Apply: "next safe boundary"},
	{Key: "ai.ca_file", Label: "CA file", Description: "Custom TLS certificate authority path", Apply: "next safe boundary"},
	{Key: "ai.api_key", Label: "API key", Description: "Environment or protected credential file", Apply: "managed by acd auth", Sensitive: true},
	{Key: "ai.diff_egress", Label: "Diff egress", Description: "Allow redacted repository diffs to eligible providers", Apply: "next safe boundary", Choices: []string{"off", "on"}},
	{Key: "commit.strategy", Label: "Commit strategy", Description: "Event or intent grouping", Apply: "next safe boundary", Choices: []string{"event", "intent"}},
	{Key: "commit.preset", Label: "Commit preset", Description: "Fast, Balanced, or Quality preset identity", Apply: "next safe boundary", Choices: []string{"fast", "balanced", "quality"}},
	{Key: "commit.format", Label: "Commit format", Description: "Imperative or conventional", Apply: "next safe boundary", Choices: []string{"imperative", "conventional"}},
	{Key: "intent.window", Label: "Intent window", Description: "Maximum planner window size", Apply: "next safe boundary"},
	{Key: "intent.min_pending", Label: "Minimum pending", Description: "Pending events required before planning", Apply: "next safe boundary"},
	{Key: "intent.settle_window", Label: "Settle window", Description: "Quiet period before planning", Apply: "next safe boundary"},
	{Key: "intent.max_pending_age", Label: "Maximum pending age", Description: "Oldest event age before planning", Apply: "next safe boundary"},
	{Key: "intent.recent_commits", Label: "Recent commits", Description: "Recent history supplied to planner", Apply: "next safe boundary"},
	{Key: "intent.defer_limit", Label: "Defer limit", Description: "Maximum repeated event deferrals", Apply: "next safe boundary"},
	{Key: "intent.retry_on_invalid", Label: "Invalid retries", Description: "Planner validation retry budget", Apply: "next safe boundary"},
	{Key: "intent.path_coalescing", Label: "Path coalescing", Description: "Legacy same-path event coalescing", Apply: "next safe boundary", Choices: []string{"false", "true"}},
	{Key: "intent.repair.enabled", Label: "Automatic repair", Description: "Repair eligible recent soft-published ACD commits", Apply: "next safe boundary", Choices: []string{"false", "true"}},
	{Key: "intent.repair.horizon", Label: "Repair horizon", Description: "Maximum age of eligible soft-published commits", Apply: "next safe boundary"},
	{Key: "intent.repair.max_commits", Label: "Repair commit limit", Description: "Maximum automatic rewrite chain, capped at five", Apply: "next safe boundary"},
	{Key: "intent.verification", Label: "Intent verification", Description: "Structural, fast, or full verification", Apply: "next safe boundary", Choices: []string{"none", "structural", "fast", "full"}},
	{Key: "verification.fast.command", Label: "Fast verification command", Description: "Exact repository-approved shell command", Apply: "next safe boundary"},
	{Key: "verification.fast.timeout", Label: "Fast verification timeout", Description: "Bound for the approved fast command", Apply: "next safe boundary"},
	{Key: "verification.full.command", Label: "Full verification command", Description: "Exact repository-approved shell command", Apply: "next safe boundary"},
	{Key: "verification.full.timeout", Label: "Full verification timeout", Description: "Bound for the approved full command", Apply: "next safe boundary"},
	{Key: "capture.max_file_bytes", Label: "Maximum file bytes", Description: "Capture size limit per file", Apply: "restart required"},
	{Key: "capture.max_pending_events", Label: "Maximum pending events", Description: "Backpressure event limit", Apply: "restart required"},
	{Key: "capture.sensitive_globs", Label: "Sensitive globs", Description: "Capture exclusion patterns", Apply: "restart required"},
	{Key: "capture.safe_ignore", Label: "Safe ignore", Description: "Prune known generated directories", Apply: "restart required", Choices: []string{"false", "true"}},
	{Key: "capture.safe_ignore_extra", Label: "Extra safe ignores", Description: "Additional generated directory patterns", Apply: "restart required"},
	{Key: "watch.fsnotify", Label: "Filesystem notifications", Description: "Opt-in filesystem watcher", Apply: "restart required", Choices: []string{"off", "on"}},
	{Key: "trace.enabled", Label: "Trace", Description: "Best-effort runtime JSONL trace", Apply: "restart required", Choices: []string{"false", "true"}},
	{Key: "trace.prompt", Label: "Prompt trace", Description: "Sensitive prompt diagnostics", Apply: "restart required", Sensitive: true, Choices: []string{"false", "true"}},
	{Key: "retention.event_days", Label: "Event retention days", Description: "Published event retention period", Apply: "restart required"},
	{Key: "recovery.rewind_grace", Label: "Rewind grace seconds", Description: "Same-branch rewind safety period", Apply: "restart required"},
	{Key: "recovery.shadow_generations", Label: "Shadow generations", Description: "Old shadow generation retention", Apply: "restart required"},
	{Key: "client.ttl", Label: "Client TTL", Description: "Inactive client expiry seconds", Apply: "restart required"},
}

func Fields() []FieldDescriptor {
	out := make([]FieldDescriptor, len(fieldDescriptors))
	copy(out, fieldDescriptors)
	return out
}

func visibleFields(query string) []FieldDescriptor {
	query = strings.ToLower(strings.TrimSpace(query))
	var out []FieldDescriptor
	for _, f := range fieldDescriptors {
		if query == "" || strings.Contains(strings.ToLower(f.Key+" "+f.Label+" "+f.Description), query) {
			out = append(out, f)
		}
	}
	return out
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

func sanitizedDraft(draft map[string]string) map[string]string {
	out := make(map[string]string, len(draft))
	keys := make([]string, 0, len(draft))
	for key := range draft {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if descriptor(key).Sensitive {
			continue
		}
		out[safeText(key)] = safeText(draft[key])
	}
	return out
}

func descriptor(key string) FieldDescriptor {
	for _, f := range fieldDescriptors {
		if f.Key == key {
			return f
		}
	}
	return FieldDescriptor{Key: safeText(key), Label: safeText(key), Apply: "next safe boundary"}
}

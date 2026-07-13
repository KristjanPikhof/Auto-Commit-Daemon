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
	{Key: "ai.api_key", Label: "API key", Description: "Environment-only credential", Apply: "never persisted", Sensitive: true},
	{Key: "ai.diff_egress", Label: "Diff egress", Description: "Allow redacted repository diffs to eligible providers", Apply: "next safe boundary", Choices: []string{"off", "on"}},
	{Key: "commit.strategy", Label: "Commit strategy", Description: "Event or intent grouping", Apply: "next safe boundary", Choices: []string{"event", "intent"}},
	{Key: "commit.format", Label: "Commit format", Description: "Imperative or conventional", Apply: "next safe boundary", Choices: []string{"imperative", "conventional"}},
	{Key: "capture.sensitive_globs", Label: "Sensitive globs", Description: "Capture exclusion patterns", Apply: "restart required"},
	{Key: "watch.fsnotify", Label: "Filesystem notifications", Description: "Opt-in filesystem watcher", Apply: "restart required", Choices: []string{"off", "on"}},
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
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || (unicode.IsPrint(r) && r != '\u007f') {
			return r
		}
		return -1
	}, s)
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

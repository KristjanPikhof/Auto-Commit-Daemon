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
	{Key: "ai.provider", Label: "AI provider", Description: "AI service used to group commits in Intent mode"},
	{Key: "ai.model", Label: "Model", Description: "Model name supported by your AI service"},
	{Key: "ai.base_url", Label: "AI service URL", Description: "AI service URL (OpenAI compatible)"},
	{Key: "ai.timeout", Label: "AI time limit", Description: "Maximum time to wait for the AI service"},
	{Key: "ai.ca_file", Label: "Trusted certificate file", Description: "Certificate file used to trust your AI service"},
	{Key: "ai.api_key", Label: "API key", Description: "Uses ACD_AI_API_KEY or the key saved by ACD"},
	{Key: "ai.diff_egress", Label: "Share changes with AI", Description: "Allow ACD to send filtered changes to your AI service; filtering cannot guarantee that all secrets are removed", Choices: []string{"false", "true"}},
	{Key: "commit.strategy", Label: "Finish mode", Description: "Local makes one commit; Intent asks AI to group completed work", Choices: []string{"local", "intent"}},
	{Key: "commit.format", Label: "Commit format", Description: "Imperative or conventional", Choices: []string{"imperative", "conventional"}},
	{Key: "intent.verification", Label: "Checks before committing", Description: "Checks ACD runs before adding finished work to Git", Choices: []string{"none", "structural", "fast", "full"}},
	{Key: "verification.fast.command", Label: "Quick check command", Description: "Command ACD may run in this repository before committing"},
	{Key: "verification.fast.timeout", Label: "Quick check time limit", Description: "Maximum time allowed for the quick check"},
	{Key: "verification.full.command", Label: "Full check command", Description: "Command ACD may run in this repository before committing"},
	{Key: "verification.full.timeout", Label: "Full check time limit", Description: "Maximum time allowed for the full check"},
	{Key: "capture.max_file_bytes", Label: "Maximum saved file size (bytes)", Description: "Files larger than this limit are not saved by ACD"},
	{Key: "capture.sensitive_globs", Label: "Sensitive globs", Description: "Capture exclusion patterns"},
	{Key: "capture.safe_ignore", Label: "Skip generated directories", Description: "Skip common generated directories when saving changes", Choices: []string{"false", "true"}},
	{Key: "capture.safe_ignore_extra", Label: "Extra directories to skip", Description: "Additional directory patterns ACD must skip"},
	{Key: "watch.fsnotify", Label: "Watch file changes", Description: "Use file change notifications (enabled by default)", Choices: []string{"false", "true"}},
	{Key: "trace.enabled", Label: "Detailed logs", Description: "Record detailed ACD activity for troubleshooting", Choices: []string{"false", "true"}},
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

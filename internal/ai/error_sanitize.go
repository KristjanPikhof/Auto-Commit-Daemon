package ai

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

const plannerErrorCap = 512

var (
	plannerErrorURLPattern        = regexp.MustCompile(`(?i)https?://[^\s]+`)
	plannerErrorAuthPattern       = regexp.MustCompile(`(?i)(["']?authorization["']?\s*:\s*["']?(?:bearer|basic)\s+)[^\s,;"'}]+`)
	plannerErrorCredentialPattern = regexp.MustCompile(`(?i)(["']?(?:api[-_]?key|access[-_]?token|auth[-_]?token|token|secret|password)["']?\s*(?:[=:]|\s)\s*["']?)[^\s,;"'}]+`)
	plannerErrorBearerPattern     = regexp.MustCompile(`(?i)\bbearer\s+[^\s,;"'}]+`)
	plannerErrorOpenAIKeyPattern  = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}\b`)
)

// SanitizePlannerError returns a bounded, single-line planner error suitable
// for durable metadata, traces, and operator output. It removes common secret
// forms without requiring callers to know which provider produced the error.
func SanitizePlannerError(raw string) string {
	clean := redactPlannerJSONSuffix(raw)
	clean = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, clean)
	clean = plannerErrorURLPattern.ReplaceAllStringFunc(clean, sanitizePlannerErrorURL)
	clean = plannerErrorAuthPattern.ReplaceAllString(clean, `${1}[REDACTED]`)
	clean = plannerErrorCredentialPattern.ReplaceAllString(clean, `${1}[REDACTED]`)
	clean = plannerErrorBearerPattern.ReplaceAllString(clean, `Bearer [REDACTED]`)
	clean = plannerErrorOpenAIKeyPattern.ReplaceAllString(clean, `[REDACTED]`)
	clean = strings.Join(strings.Fields(clean), " ")
	runes := []rune(clean)
	if len(runes) > plannerErrorCap {
		clean = string(runes[:plannerErrorCap])
	}
	return clean
}

func redactPlannerJSONSuffix(raw string) string {
	start := strings.IndexAny(raw, "{[")
	if start < 0 {
		return raw
	}
	suffix := strings.TrimSpace(raw[start:])
	var value any
	if err := json.Unmarshal([]byte(suffix), &value); err != nil {
		return raw
	}
	redactPlannerJSONValue(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return raw[:start] + string(encoded)
}

func redactPlannerJSONValue(value any) {
	switch current := value.(type) {
	case map[string]any:
		for key, child := range current {
			if plannerJSONCredentialKey(key) {
				current[key] = "[REDACTED]"
				continue
			}
			redactPlannerJSONValue(child)
		}
	case []any:
		for _, child := range current {
			redactPlannerJSONValue(child)
		}
	}
}

func plannerJSONCredentialKey(key string) bool {
	normalized := strings.ToLower(strings.NewReplacer("-", "", "_", "", " ", "").Replace(key))
	return normalized == "authorization" ||
		strings.HasSuffix(normalized, "apikey") ||
		strings.HasSuffix(normalized, "token") ||
		strings.HasSuffix(normalized, "secret") ||
		strings.HasSuffix(normalized, "password")
}

func sanitizePlannerErrorURL(raw string) string {
	urlText, suffix := splitPlannerURLPunctuation(raw)
	u, err := url.Parse(urlText)
	if err != nil {
		if i := strings.IndexAny(urlText, "?#"); i >= 0 {
			urlText = urlText[:i]
		}
		return urlText + suffix
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.String() + suffix
}

func splitPlannerURLPunctuation(raw string) (string, string) {
	cut := len(raw)
	for cut > 0 && strings.ContainsRune("\"'.,;:)]}", rune(raw[cut-1])) {
		cut--
	}
	return raw[:cut], raw[cut:]
}

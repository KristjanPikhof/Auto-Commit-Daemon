package ai

import (
	"path/filepath"
	"regexp"
	"strings"
)

var genericSubjectPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^(update|change|modify|fix|adjust|improve|refactor)\s+(file|files|stuff|things|changes|code|content|logic|data)$`),
	regexp.MustCompile(`(?i)^(wip|changes|misc|miscellaneous|updates?)$`),
	regexp.MustCompile(`(?i)^update\s+\d+\s+files?(?:\s+in\s+.+)?$`),
	regexp.MustCompile(`(?i)^(add|remove|rename|update|fix|refactor|modify|change)\s+.+\s+(code|test|documentation|configuration|asset)\s+changes$`),
}

func isGenericSubject(subject string) bool {
	subject = strings.TrimSpace(subjectTailForQuality(subject))
	for _, re := range genericSubjectPatterns {
		if re.MatchString(subject) {
			return true
		}
	}
	return false
}

// Conventional subjects use the same semantic quality bar as imperative ones.
func subjectTailForQuality(subject string) string {
	if prefix, rest, ok := strings.Cut(subject, ":"); ok &&
		!strings.ContainsAny(prefix, " /\\") {
		return strings.TrimSpace(rest)
	}
	return subject
}

func isFilenameOnlySubject(subject string, paths map[string]struct{}) bool {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return false
	}
	tail := subjectTail(subjectTailForQuality(subject))
	if looksLikePathToken(tail) {
		return true
	}
	for p := range paths {
		if tail == filepath.Base(p) || tail == strings.Trim(p, "/") {
			return true
		}
	}
	return false
}

func subjectTail(subject string) string {
	fields := strings.Fields(subject)
	if len(fields) <= 1 {
		return subject
	}
	switch strings.ToLower(fields[0]) {
	case "add", "fix", "refactor", "remove", "rename", "simplify", "update", "document", "change", "modify", "adjust", "improve":
		return strings.TrimSpace(strings.TrimPrefix(subject, fields[0]))
	default:
		return subject
	}
}

func looksLikePathToken(s string) bool {
	s = strings.Trim(s, "`'\" ")
	if s == "" || strings.Contains(s, " ") {
		return false
	}
	if strings.Contains(s, "/") {
		return true
	}
	ext := filepath.Ext(s)
	return len(ext) > 1 && len(strings.TrimSuffix(s, ext)) > 0
}

func isTokenOnlySubject(subject string) bool {
	tail := strings.Trim(subjectTail(subjectTailForQuality(subject)), "`'\" ")
	if tail == "" || strings.Contains(tail, " ") || looksLikePathToken(tail) {
		return false
	}
	if strings.Contains(tail, "_") || strings.Contains(tail, "-") {
		return true
	}
	if hasInternalUpper(tail) {
		return true
	}
	switch strings.ToLower(tail) {
	case "parsed", "total", "value", "data", "state", "result", "item", "items", "helper", "logic", "flow":
		return true
	}
	return false
}

func hasInternalUpper(s string) bool {
	for i := 1; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return true
		}
	}
	return false
}

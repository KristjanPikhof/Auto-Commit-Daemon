package ai

import (
	"regexp"
	"strings"
	"unicode"
)

const redactedSecret = "[REDACTED_SECRET]"

var (
	reAWSAccessKey     = regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`)
	reSlackToken       = regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{10,}\b`)
	reGitHubToken      = regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9_]{20,}\b`)
	reBearerToken      = regexp.MustCompile(`(?i)\b(Bearer)\s+[A-Za-z0-9._~+/=-]{8,}`)
	reJWT              = regexp.MustCompile(`\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`)
	rePrivateKeyMarker = regexp.MustCompile(`-----(?:BEGIN|END) [A-Z0-9 ]*PRIVATE KEY-----`)
	reAssignedSecret   = regexp.MustCompile(`(?i)\b([A-Z0-9_.-]*(?:api[_-]?key|secret|token|password|passwd|pwd|client[_-]?secret|private[_-]?key)[A-Z0-9_.-]*\s*[:=]\s*["']?)([^"'\s#]{8,})(["']?)`)
	reHighEntropy      = regexp.MustCompile(`[A-Za-z0-9_+/=.-]{32,}`)
)

// RedactDiffSecrets scrubs obvious credential shapes from unified diff text
// before it is handed to AI providers. It intentionally errs toward keeping
// surrounding syntax intact so the model can still infer what kind of config
// changed without receiving the secret value.
func RedactDiffSecrets(diff string) string {
	if diff == "" {
		return ""
	}

	out := reAWSAccessKey.ReplaceAllString(diff, redactedSecret)
	out = reSlackToken.ReplaceAllString(out, redactedSecret)
	out = reGitHubToken.ReplaceAllString(out, redactedSecret)
	out = reBearerToken.ReplaceAllString(out, `${1} `+redactedSecret)
	out = reJWT.ReplaceAllString(out, redactedSecret)
	out = rePrivateKeyMarker.ReplaceAllString(out, "-----"+redactedSecret+"-----")
	out = reAssignedSecret.ReplaceAllString(out, `${1}`+redactedSecret+`${3}`)
	out = reHighEntropy.ReplaceAllStringFunc(out, func(s string) string {
		if looksLikeHighEntropySecret(s) {
			return redactedSecret
		}
		return s
	})
	return out
}

func looksLikeHighEntropySecret(s string) bool {
	var hasLower, hasUpper, hasDigit bool
	for _, r := range s {
		switch {
		case unicode.IsLower(r):
			hasLower = true
		case unicode.IsUpper(r):
			hasUpper = true
		case unicode.IsDigit(r):
			hasDigit = true
		}
	}
	if !(hasLower && hasUpper && hasDigit) {
		return false
	}
	return strings.ContainsAny(s, "_+/=.-") || len(s) >= 40
}

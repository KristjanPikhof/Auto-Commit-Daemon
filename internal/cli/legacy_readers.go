package cli

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	observabilityANSI     = regexp.MustCompile(`\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))`)
	observabilityUserInfo = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^/@\s]+@`)
	observabilitySecret   = regexp.MustCompile(`(?i)\b(api[_ -]?key|access[_ -]?token|token|password|credential)\s*[:=]\s*[^\s,;]+`)
	observabilityPayload  = regexp.MustCompile(`(?i)\b(prompt|repository[_ -]?diff|raw[_ -]?response|provider[_ -]?response)\s*[:=]\s*[^\n]+`)
)

func sanitizeObservabilityText(value string) string {
	value = observabilityANSI.ReplaceAllString(value, "")
	value = observabilityUserInfo.ReplaceAllString(value, `${1}[redacted]@`)
	value = observabilitySecret.ReplaceAllString(value, `[redacted secret]`)
	value = observabilityPayload.ReplaceAllString(value, `[redacted payload]`)
	value = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) && r != '\u007f' {
			return r
		}
		return -1
	}, value)
	if len(value) > 1024 {
		value = value[:1024]
	}
	return strings.TrimSpace(value)
}

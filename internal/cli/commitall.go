package cli

import (
	"strings"
)

func shortenSHA(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "(none)"
	}
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

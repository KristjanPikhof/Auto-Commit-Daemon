package ai

import (
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

// Reflow generated bullets without spending a provider correction on layout.
// Subjects, words, and invalid message structures are never repaired.
func reflowGeneratedMessage(style, message string) string {
	if finish.ValidateMessage(style, message) == nil || finish.ValidatePurpose(message) != nil {
		return message
	}
	subject, body, ok := strings.Cut(message, "\n\n")
	if !ok {
		return message
	}
	var bullets []string
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "- ") && strings.TrimSpace(line[2:]) != "":
			bullets = append(bullets, line[2:])
		case len(bullets) > 0 && strings.HasPrefix(line, "  ") && strings.TrimSpace(line) != "" && !strings.HasPrefix(strings.TrimSpace(line), "- "):
			bullets[len(bullets)-1] += " " + strings.TrimSpace(line)
		default:
			return message
		}
	}
	var lines []string
	for _, bullet := range bullets {
		line := "- "
		for _, word := range strings.Fields(bullet) {
			if utf8.RuneCountInString(word) > finish.BodyLineLimit-2 {
				return message
			}
			if utf8.RuneCountInString(line+word) > finish.BodyLineLimit {
				lines = append(lines, strings.TrimRight(line, " "))
				line = "  "
			}
			line += word + " "
		}
		lines = append(lines, strings.TrimRight(line, " "))
	}
	return subject + "\n\n" + strings.Join(lines, "\n")
}

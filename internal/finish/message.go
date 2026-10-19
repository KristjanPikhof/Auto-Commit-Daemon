package finish

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	StyleImperative   = "imperative"
	StyleConventional = "conventional"
	SubjectLimit      = 50
	BodyLineLimit     = 72
)

func ValidatePurpose(purpose string) error {
	if !messageTextSafe(purpose) {
		return fmt.Errorf("finish: purpose contains invalid text or control characters")
	}
	return nil
}

// ValidateMessage checks the shared message contract without replacing,
// truncating, or silently repairing the supplied text.
func ValidateMessage(style, message string) error {
	if !messageTextSafe(message) {
		return fmt.Errorf("finish: message must contain valid text without control characters")
	}
	lines := strings.Split(strings.TrimRight(message, "\n"), "\n")
	subject := lines[0]
	if strings.TrimSpace(subject) != subject || subject == "" || utf8.RuneCountInString(subject) > SubjectLimit || strings.HasSuffix(subject, ".") {
		return fmt.Errorf("finish: subject must be nonempty, at most %d characters, and have no trailing period", SubjectLimit)
	}
	switch style {
	case StyleImperative:
		words := strings.Fields(subject)
		if len(words) < 2 || strings.HasSuffix(words[0], ":") || !unicode.IsLetter([]rune(subject)[0]) {
			return fmt.Errorf("finish: imperative subject must name an action and what changed")
		}
	case StyleConventional:
		typ, description, ok := strings.Cut(subject, ": ")
		if !ok || strings.TrimSpace(description) == "" || !conventionalType(typ) {
			return fmt.Errorf("finish: conventional subject must use a supported scope-free 'type: description'")
		}
	default:
		return fmt.Errorf("finish: unsupported message style %q", style)
	}
	if len(lines) == 1 {
		return nil
	}
	if len(lines) < 3 || lines[1] != "" {
		return fmt.Errorf("finish: separate the subject and body with one blank line")
	}
	bulletSeen := false
	for _, line := range lines[2:] {
		if strings.TrimSpace(line) == "" || utf8.RuneCountInString(line) > BodyLineLimit {
			return fmt.Errorf("finish: body lines must be nonempty and at most %d characters", BodyLineLimit)
		}
		if strings.HasPrefix(line, "- ") && strings.TrimSpace(line[2:]) != "" {
			bulletSeen = true
			continue
		}
		if !bulletSeen || !strings.HasPrefix(line, "  ") || strings.HasPrefix(strings.TrimSpace(line), "- ") {
			return fmt.Errorf("finish: body must use '- ' bullets with indented continuations")
		}
	}
	return nil
}

func conventionalType(value string) bool {
	switch value {
	case "feat", "fix", "docs", "refactor", "test", "build", "ci", "chore", "perf", "style", "revert":
		return true
	default:
		return false
	}
}

func messageTextSafe(text string) bool {
	if !utf8.ValidString(text) {
		return false
	}
	for _, character := range text {
		if unicode.IsControl(character) && character != '\n' {
			return false
		}
	}
	return true
}

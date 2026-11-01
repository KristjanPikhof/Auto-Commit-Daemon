package finish

import (
	"fmt"
	"maps"
	"strings"
	"unicode"
	"unicode/utf8"
)

type LocalOptions struct {
	Style   string
	Purpose string
	Message string
}

// Local creates one complete offline batch. It has no provider, credentials,
// process, or network dependency. Recorded author boundaries remain enforced.
func Local(evidence Evidence, options LocalOptions) (Plan, error) {
	if options.Style == "" {
		options.Style = StyleImperative
	}
	if options.Style != StyleImperative && options.Style != StyleConventional {
		return Plan{}, fmt.Errorf("finish: unsupported message style %q", options.Style)
	}
	if err := ValidatePurpose(options.Purpose); err != nil {
		return Plan{}, err
	}
	message := options.Message
	if message != "" {
		if err := ValidateMessage(options.Style, message); err != nil {
			return Plan{}, err
		}
	}
	if maps.Equal(evidence.Base, evidence.Target) {
		return Build(evidence, nil)
	}
	if message == "" {
		message = localMessage(options)
		if err := ValidateMessage(options.Style, message); err != nil {
			return Plan{}, err
		}
	}
	purpose := strings.TrimSpace(options.Purpose)
	if purpose == "" {
		purpose = strings.SplitN(message, "\n", 2)[0]
	}
	ids := make([]string, 0, len(evidence.Changes))
	for _, change := range evidence.Changes {
		ids = append(ids, change.ID)
	}
	reason := strings.TrimSpace(options.Purpose)
	if options.Message != "" {
		_, reason, _ = strings.Cut(message, "\n\n")
	}
	return Build(evidence, []Goal{{Purpose: purpose, Reason: reason, Message: message, ChangeIDs: ids}})
}

func localMessage(options LocalOptions) string {
	purpose := strings.Join(strings.Fields(options.Purpose), " ")
	subject := strings.TrimSpace(strings.TrimRight(purpose, "."))
	if purpose == "" {
		subject = "Save protected batch"
		purpose = "Save the complete frozen batch in one offline commit."
	}
	prefix := ""
	if options.Style == StyleConventional {
		prefix = "chore: "
	}
	if utf8.RuneCountInString(subject)+len(prefix) > SubjectLimit || ValidateMessage(StyleImperative, subject) != nil {
		subject = "Complete requested work"
	}
	if prefix != "" {
		characters := []rune(subject)
		characters[0] = unicode.ToLower(characters[0])
		subject = prefix + string(characters)
	}
	return subject + "\n\n" + strings.Join(wrapLocalBullet(purpose), "\n")
}

func wrapLocalBullet(text string) []string {
	var lines []string
	line := "- "
	for _, word := range strings.Fields(text) {
		if utf8.RuneCountInString(line)+utf8.RuneCountInString(word)+1 > BodyLineLimit && len(line) > 2 {
			lines = append(lines, line)
			line = "  "
		}
		if len(line) > 2 {
			line += " "
		}
		for _, character := range word {
			if utf8.RuneCountInString(line) == BodyLineLimit {
				lines = append(lines, line)
				line = "  "
			}
			line += string(character)
		}
	}
	if len(line) > 2 {
		lines = append(lines, line)
	}
	return lines
}

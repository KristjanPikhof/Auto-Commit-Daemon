package finish

import (
	"fmt"
	"maps"
	"strings"
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
		message = localMessage(evidence, options)
		if err := ValidateMessage(options.Style, message); err != nil {
			return Plan{}, err
		}
	}
	purpose := strings.TrimSpace(options.Purpose)
	if purpose == "" {
		purpose = "Save the complete protected batch"
	}
	ids := make([]string, 0, len(evidence.Changes))
	for _, change := range evidence.Changes {
		ids = append(ids, change.ID)
	}
	return Build(evidence, []Goal{{Purpose: purpose, Message: message, ChangeIDs: ids}})
}

func localMessage(evidence Evidence, options LocalOptions) string {
	var added, updated, removed int
	for _, name := range changedPaths(evidence.Base, evidence.Target) {
		switch {
		case evidence.Base[name] == (Entry{}):
			added++
		case evidence.Target[name] == (Entry{}):
			removed++
		default:
			updated++
		}
	}
	purpose := strings.Join(strings.Fields(options.Purpose), " ")
	count := added + updated + removed
	noun := "paths"
	if count == 1 {
		noun = "path"
	}
	subject := fmt.Sprintf("Save %d changed %s", count, noun)
	if label := strings.TrimSpace(strings.TrimRight(purpose, ".")); label != "" {
		subject = "Complete " + label
	}
	prefix := ""
	if options.Style == StyleConventional {
		prefix = "chore: "
	}
	if utf8.RuneCountInString(subject)+len(prefix) > SubjectLimit {
		subject = "Complete protected batch"
	}
	if prefix != "" {
		subject = prefix + strings.ToLower(subject[:1]) + subject[1:]
	}
	var body []string
	if purpose != "" {
		body = append(body, wrapLocalBullet("Purpose: "+purpose)...)
	}
	counts := fmt.Sprintf("Include %d added, %d updated, and %d removed paths.", added, updated, removed)
	body = append(body, wrapLocalBullet(counts)...)
	body = append(body, "- Save the entire frozen batch in one local commit.")
	return subject + "\n\n" + strings.Join(body, "\n")
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

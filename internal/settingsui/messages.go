package settingsui

import (
	"io"
	"os"
	"regexp"
	"strings"

	"charm.land/bubbles/v2/key"
	"charm.land/huh/v2"
)

func readableKeyMap() *huh.KeyMap {
	keys := huh.NewDefaultKeyMap()
	for _, binding := range []*key.Binding{&keys.Input.Submit, &keys.Text.Submit, &keys.Select.Submit, &keys.MultiSelect.Submit, &keys.FilePicker.Submit, &keys.Note.Submit, &keys.Confirm.Submit} {
		binding.SetHelp(binding.Help().Key, "continue")
	}
	return keys
}

var accessibleMessages = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`Enter a number between (\d+) and (\d+): `), "Choose a number from $1 to $2 (Enter keeps the current choice): "},
	{regexp.MustCompile(`There is only one option available; enter the number 1:`), "Enter 1 to select the only option:"},
	{regexp.MustCompile(`Input cannot exceed (\d+) characters`), "Use no more than $1 characters."},
	{regexp.MustCompile(`Invalid: must be a number between (\d+) and (\d+)`), "Enter a number from $1 to $2."},
	{regexp.MustCompile(`Invalid: must be (\d+)`), "Enter $1."},
}

type accessibleMessageWriter struct{ io.Writer }

func (w accessibleMessageWriter) Write(body []byte) (int, error) {
	text := string(body)
	for _, message := range accessibleMessages {
		text = message.pattern.ReplaceAllString(text, message.replacement)
	}
	text = strings.ReplaceAll(text, "invalid input. please try again", "Enter y for yes or n for no.")
	written, err := io.WriteString(w.Writer, text)
	if err != nil {
		return 0, err
	}
	if written != len(text) {
		return 0, io.ErrShortWrite
	}
	return len(body), nil
}

func readableOutput(out io.Writer, accessible bool) io.Writer {
	if out == nil {
		out = os.Stdout
	}
	if accessible {
		return accessibleMessageWriter{out}
	}
	return out
}

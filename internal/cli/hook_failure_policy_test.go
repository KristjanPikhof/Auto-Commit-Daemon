package cli

import (
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

func TestHookTemplatesUseFailOpenUnifiedEvents(t *testing.T) {
	files := []string{
		"pi/hooks.snippet.yaml",
		"opencode/hooks.snippet.yaml",
		"claude-code/settings.snippet.json",
		"codex/hooks.json",
		"cursor/hooks.json",
	}
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			body, err := templates.FS.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			text := string(body)
			if !strings.Contains(text, "acd internal integration event") {
				t.Fatal("template does not use the unified integration event command")
			}
			for _, forbidden := range []string{
				"acd internal integration stdin-extract",
				"acd internal integration cursor-extract",
				"acd internal session open",
				"acd internal hint",
				"exit 1",
				"rc=$?",
			} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("template contains obsolete hook logic %q", forbidden)
				}
			}
		})
	}
}

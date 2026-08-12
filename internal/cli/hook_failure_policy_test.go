package cli

import (
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

func TestHookTemplatesPreserveFailureCodesAndFailOnlyCriticalHooks(t *testing.T) {
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
			if strings.Contains(text, `"$(date +%FT%T%z)" "$?"`) {
				t.Fatal("hook formats a command substitution before saving the failing exit code")
			}
			if !strings.Contains(text, "rc=$?") || !strings.Contains(text, "exit 1") {
				t.Fatal("session-start and active hooks must save the real exit code and fail closed")
			}
			for _, marker := range []string{"logical_boundary", "soft_boundary", "session close"} {
				index := strings.Index(text, marker)
				if index < 0 {
					continue
				}
				end := min(len(text), index+900)
				if !strings.Contains(text[index:end], "exit 0") && !strings.Contains(text[index:end], "|| true") {
					t.Fatalf("noncritical %s hook does not log and return success", marker)
				}
			}
		})
	}
}

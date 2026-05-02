package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestMajorCommandHelpIncludesWorkflowExamples(t *testing.T) {
	tests := []struct {
		command string
		want    []string
	}{
		{"start", []string{"acd start --repo /path/to/repo", "--session-id", "acd status"}},
		{"stop", []string{"acd stop --session-id", "acd stop --all --json", "active sessions"}},
		{"status", []string{"current working directory", "acd list", "acd diagnose"}},
		{"list", []string{"acd list --watch --interval 5s", "--watch", "--interval"}},
		{"logs", []string{"raw JSONL", "acd logs --repo /path/to/repo --lines 50 --follow", "--lines"}},
		{"doctor", []string{"acd doctor --bundle", "sanitized diagnostic files", "acd diagnose"}},
		{"diagnose", []string{"state read-only", "acd recover --repo . --auto --dry-run --json"}},
		{"recover", []string{"acd recover --auto --dry-run", "requires --yes", "--clear-pause"}},
		{"pause", []string{"acd pause --ttl 1h", "acd resume", "acd status"}},
		{"resume", []string{"acd resume --repo /path/to/repo --yes", "--accept-overflow", "acd status"}},
		{"init", []string{"acd init codex", "Supported harnesses", "prints snippets only"}},
		{"wake", []string{"acd wake --session-id", "acd touch", "current working directory"}},
		{"stats", []string{"acd stats --since 30d", "all registered repos", "--json"}},
		{"gc", []string{"acd gc --json", "30 days", "acd list"}},
	}

	for _, tt := range tests {
		t.Run(tt.command, func(t *testing.T) {
			help := commandHelp(t, tt.command)
			for _, want := range tt.want {
				if !strings.Contains(help, want) {
					t.Fatalf("%s help missing %q:\n%s", tt.command, want, help)
				}
			}
		})
	}
}

func commandHelp(t *testing.T, command string) string {
	t.Helper()

	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{command, "--help"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute %s help: %v\nstderr:\n%s", command, err, errOut.String())
	}
	return out.String()
}

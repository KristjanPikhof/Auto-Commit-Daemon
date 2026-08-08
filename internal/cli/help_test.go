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
		{"setup", []string{"acd setup --dry-run", "--non-interactive", "--integrations"}},
		{"status", []string{"protected and published", "--repo"}},
		{"on", []string{"Enable checkpoint protection", "--repo"}},
		{"off", []string{"final durable checkpoint", "--force"}},
		{"history", []string{"protected checkpoints", "--activity", "rewrite"}},
		{"restore", []string{"restore ID", "--yes"}},
		{"doctor", []string{"protection problems", "--bundle", "--output"}},
		{"uninstall", []string{"preserving protected repository data", "--dry-run", "--purge-data"}},
		{"config", []string{"get", "set", "credentials"}},
		{"support", []string{"diagnose", "logs", "repair", "bundle"}},
		{"repo", []string{"list", "remove", "gc"}},
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
	root.SetArgs(append(strings.Fields(command), "--help"))
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute %s help: %v\nstderr:\n%s", command, err, errOut.String())
	}
	return out.String()
}

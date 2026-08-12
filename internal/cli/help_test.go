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
		{"list", []string{"checkpoint protection and local Git", "--watch", "--once"}},
		{"commit-all", []string{"managed", "--dry-run", "--yes"}},
		{"history", []string{"protected checkpoints", "--activity", "activity", "rewrite"}},
		{"restore", []string{"restore ID", "--yes"}},
		{"doctor", []string{"protection problems", "--bundle", "--output"}},
		{"uninstall", []string{"preserving protected repository data", "--dry-run", "--purge-data"}},
		{"config", []string{"get", "set", "credentials"}},
		{"support", []string{"diagnose", "logs", "repair", "recover", "prompt", "bundle"}},
		{"repo", []string{"list", "remove", "gc", "stats"}},
		{"config credentials", []string{"protected provider credential", "set", "status", "remove"}},
		{"support diagnose", []string{"acd support diagnose", "acd support recover", "--repo"}},
		{"support logs", []string{"acd support logs", "--follow", "--repo"}},
		{"history explain", []string{"acd history explain", "--path", "--commit"}},
		{"history activity", []string{"acd history activity", "--watch", "--since"}},
		{"support prompt", []string{"acd support prompt", "--seq", "--json"}},
		{"support recover", []string{"acd support recover", "--dry-run", "--force"}},
		{"repo stats", []string{"acd repo stats", "--since", "--json"}},
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

func TestCanonicalAdvancedHelpDoesNotRecommendHiddenAliases(t *testing.T) {
	tests := []struct {
		command string
		hidden  []string
	}{
		{"config credentials", []string{"acd auth"}},
		{"support diagnose", []string{"acd diagnose", "acd fix"}},
		{"support logs", []string{"acd logs", "acd doctor", "acd prompt"}},
		{"history explain", []string{"acd explain"}},
		{"history activity", []string{"acd events"}},
		{"support prompt", []string{"acd prompt"}},
		{"support recover", []string{"acd fix", "acd recover"}},
		{"repo stats", []string{"acd stats"}},
	}
	for _, test := range tests {
		help := commandHelp(t, test.command)
		for _, hidden := range test.hidden {
			if strings.Contains(help, hidden) {
				t.Errorf("%s help recommends hidden alias %q:\n%s", test.command, hidden, help)
			}
		}
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

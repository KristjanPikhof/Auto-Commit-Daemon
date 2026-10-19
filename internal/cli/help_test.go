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
		{"status", []string{"current changes are protected", "exact next command", "--repo"}},
		{"on", []string{"Start saving changes", "offline defaults", "saves the current files", "acd finish", "--repo"}},
		{"off", []string{"confirm the final save", "protection stays on", "--force"}},
		{"list", []string{"last hour", "unfinished work", "need action", "Ctrl-C", "--watch", "--once", "--all"}},
		{"finish", []string{"acd finish", "--dry-run", "--include-staged", "--retry", "--message"}},
		{"history", []string{"recent protected checkpoints", "--activity", "activity", "rewrite"}},
		{"restore", []string{"restore [ID]", "checkpoint picker", "--yes"}},
		{"doctor", []string{"safe next step", "--bundle", "--output"}},
		{"uninstall", []string{"kept by default", "--dry-run", "--purge-data"}},
		{"config", []string{"get", "set", "credentials"}},
		{"support", []string{"diagnose", "logs", "repair", "recover", "bundle"}},
		{"repo", []string{"list", "remove", "gc", "stats"}},
		{"config credentials", []string{"protected provider credential", "set", "status", "remove"}},
		{"support diagnose", []string{"acd support diagnose", "acd support recover", "--repo"}},
		{"support logs", []string{"acd support logs", "--follow", "--repo"}},
		{"history explain", []string{"acd history explain", "--path", "--commit"}},
		{"history activity", []string{"acd history activity", "--watch", "--since"}},
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

func TestRootHelpLinksPublicationAndRewriteWorkflows(t *testing.T) {
	help := commandHelp(t, "")
	for _, want := range []string{
		"acd finish",
		"acd history rewrite --help",
		"acd config",
		"acd repo --help",
		"acd support --help",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("root help missing %q:\n%s", want, help)
		}
	}
}

func TestHistoryRewriteExplainsExplicitSavedPlanWorkflow(t *testing.T) {
	help := commandHelp(t, "history rewrite")
	for _, want := range []string{"acd history rewrite", "--plan-only", "--plan-out", "--apply", "--dry-run", "--yes"} {
		if !strings.Contains(help, want) {
			t.Errorf("rewrite-all help missing %q:\n%s", want, help)
		}
	}
}

func TestCanonicalHelpIsCompleteAndPlain(t *testing.T) {
	commands := []string{
		"setup", "status", "on", "off", "list", "finish", "history",
		"history activity", "history explain", "history rewrite", "restore", "doctor", "uninstall",
		"config", "config get", "config set", "config edit", "config reset", "config credentials",
		"config credentials set", "config credentials status", "config credentials remove",
		"support", "support diagnose", "support logs", "support repair", "support recover",
		"support bundle", "repo", "repo list", "repo remove", "repo gc", "repo stats",
	}
	for _, path := range commands {
		t.Run(path, func(t *testing.T) {
			root := newRootCmd()
			command, _, err := root.Find(strings.Fields(path))
			if err != nil {
				t.Fatalf("find command: %v", err)
			}
			if strings.TrimSpace(command.Long) == "" {
				t.Error("missing plain-language explanation")
			}
			if strings.TrimSpace(command.Example) == "" {
				t.Error("missing example")
			}
			help := commandHelp(t, path)
			if strings.ContainsAny(help, "—–") {
				t.Errorf("help contains an em or en dash:\n%s", help)
			}
			if strings.Contains(help, "acd configure") || strings.Contains(help, "acd rewrite-commits") {
				t.Errorf("help recommends a hidden compatibility command:\n%s", help)
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

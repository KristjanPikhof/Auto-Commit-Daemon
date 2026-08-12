package cli

import (
	"bytes"
	"testing"
)

func TestDocumentedConfigurationCommands(t *testing.T) {
	withIsolatedHome(t)
	repo := materializeTestRepo(t, false)
	t.Chdir(repo)

	commands := [][]string{
		{"config", "get"},
		{"config", "get", "commit.preset"},
		{"config", "set", "commit.preset", "fast"},
		{"config", "set", "--scope", "global", "ai.provider", "deterministic"},
	}
	for _, args := range commands {
		cmd := newRootCmd()
		var output bytes.Buffer
		cmd.SetOut(&output)
		cmd.SetErr(&output)
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("acd %v: %v\n%s", args, err, output.String())
		}
	}
}

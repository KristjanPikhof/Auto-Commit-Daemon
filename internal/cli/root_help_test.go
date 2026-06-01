package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRootHelpIsCompactAndWorkflowGrouped(t *testing.T) {
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"--help"})

	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("root help: %v", err)
	}
	if errOut.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", errOut.String())
	}
	got := out.String()

	for _, want := range []string{
		"Common workflow:",
		"Diagnostics and recovery:",
		"Setup:",
		"Advanced:",
		"acd logs",
		"acd prompt --last",
		"acd fix --dry-run",
		"acd repo init",
		"acd repo disable",
		"acd repo enable",
		"acd repo list",
		"acd repo remove --dry-run",
		"acd list --once",
		"acd diagnose",
		"acd doctor",
		"acd pause",
		"acd resume",
		"acd setup",
		"--repo string",
		"--json",
		"--quiet",
		"--log-level string",
		`Use "acd <command> --help" for command details.`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("root help missing %q:\n%s", want, got)
		}
	}

	for _, noisy := range []string{
		"Available Commands:",
		"acd daemon",
		"acd hook-stdin-extract",
		"acd completion",
		// Deprecated commands must not be advertised in the hand-written
		// help. The cobra deprecation warnings on actual invocation still
		// fire (recover.go / purge.go log to stderr), but the discovery
		// path should steer new users to the supported `acd fix`
		// entrypoint instead. Regression target: g1.
		"acd recover",
		"acd purge-events",
	} {
		if strings.Contains(got, noisy) {
			t.Fatalf("root help contains internal/generated noise %q:\n%s", noisy, got)
		}
	}
}

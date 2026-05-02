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
		"acd list --watch",
		"acd diagnose",
		"acd recover",
		"acd doctor",
		"acd init",
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
	} {
		if strings.Contains(got, noisy) {
			t.Fatalf("root help contains internal/generated noise %q:\n%s", noisy, got)
		}
	}
}

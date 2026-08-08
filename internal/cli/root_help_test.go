package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRootHelpExposesExactlyEightProductCommands(t *testing.T) {
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"--help"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if errOut.Len() != 0 {
		t.Fatalf("stderr=%q", errOut.String())
	}
	got := out.String()
	for _, command := range []string{"setup", "status", "on", "off", "history", "restore", "doctor", "uninstall"} {
		if !strings.Contains(got, "  "+command) {
			t.Fatalf("help missing %s:\n%s", command, got)
		}
	}
	for _, internal := range []string{"daemon", "start", "stop", "wake", "flush", "events", "configure", "settings", "support", "repo"} {
		if strings.Contains(got, "  "+internal+" ") {
			t.Fatalf("help exposes %s:\n%s", internal, got)
		}
	}
	visible := 0
	for _, command := range root.Commands() {
		if !command.Hidden && command.Name() != "help" && command.Name() != "completion" {
			visible++
		}
	}
	if visible != 8 {
		t.Fatalf("visible root commands=%d want=8", visible)
	}
}

func TestRootVersionFlagAndHiddenAliasMatch(t *testing.T) {
	run := func(args ...string) string {
		t.Helper()
		root := newRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&bytes.Buffer{})
		root.SetArgs(args)
		if err := root.ExecuteContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	if flag, alias := run("--version"), run("version"); flag != alias {
		t.Fatalf("version outputs differ: flag=%q alias=%q", flag, alias)
	}
}

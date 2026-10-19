package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRootHelpExposesSettingsAndProductCommands(t *testing.T) {
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
	for _, command := range []string{"setup", "config", "status", "on", "off", "list", "finish", "history", "restore", "doctor", "uninstall"} {
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
	if visible != 12 {
		t.Fatalf("visible root commands=%d want=12", visible)
	}
}

func TestRootVersionFlagAndFiniteCommandCatalog(t *testing.T) {
	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"--version"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "acd version "+root.Version+"\n"; got != want {
		t.Fatalf("version=%q want=%q", got, want)
	}
	expected := map[string]bool{"setup": false, "status": false, "on": false, "off": false, "list": false, "finish": false, "commit-all": false, "history": false, "restore": false, "doctor": false, "uninstall": false, "config": false, "support": true, "repo": true, "internal": true}
	for _, command := range newRootCmd().Commands() {
		hidden, ok := expected[command.Name()]
		if !ok || command.Hidden != hidden {
			t.Fatalf("unexpected root command %s hidden=%t", command.Name(), command.Hidden)
		}
		delete(expected, command.Name())
	}
	if len(expected) != 0 {
		t.Fatalf("missing commands %v", expected)
	}
	for _, retired := range []string{"version", "init", "events", "prompt", "fix", "recover", "stats", "start", "stop", "flush", "rewrite-commits"} {
		command, _, err := newRootCmd().Find([]string{retired})
		if err == nil && command.Name() == retired {
			t.Fatalf("retired root command %s remains", retired)
		}
	}
}

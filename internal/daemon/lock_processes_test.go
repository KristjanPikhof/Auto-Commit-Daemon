package daemon

import (
	"strings"
	"testing"
)

func TestParseDaemonOwnerProcessesKeepsLongCommands(t *testing.T) {
	command := "/usr/local/bin/tool --input=" + strings.Repeat("x", 128*1024)
	output := " 123 " + command + "\nmalformed\n 456 /usr/local/bin/acd --repo /tmp/project\n"
	processes := parseDaemonOwnerProcesses(output)
	if len(processes) != 2 || processes[0].pid != 123 || processes[0].command != command ||
		processes[1].pid != 456 || processes[1].command != "/usr/local/bin/acd --repo /tmp/project" {
		t.Fatal("long unrelated command prevented complete owner discovery")
	}
}

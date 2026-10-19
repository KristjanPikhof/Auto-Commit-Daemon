package cli

import (
	"strings"
	"testing"
)

func TestParseDoctorProcessesKeepsLongCommands(t *testing.T) {
	command := "/usr/local/bin/tool --input=" + strings.Repeat("x", 128*1024)
	output := " 123 " + command + "\nmalformed\n 456 /usr/local/bin/acd --repo /tmp/project\n"
	processes := parseDoctorProcesses(output)
	if len(processes) != 2 || processes[0].PID != 123 || processes[0].Command != command ||
		processes[1].PID != 456 || processes[1].Command != "/usr/local/bin/acd --repo /tmp/project" {
		t.Fatal("long unrelated command prevented complete diagnostic discovery")
	}
}

//go:build integration

package integration_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPTYCommandPreservesChildExitStatus(t *testing.T) {
	for _, exitCode := range []int{0, 7} {
		t.Run(strconv.Itoa(exitCode), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result := runPTYCommand(t, ctx, os.Environ(), 80, 24, 0, 0, "",
				"/bin/sh", "-c", `echo 'ACD SETTINGS'; exit "$1"`, "sh", strconv.Itoa(exitCode))
			if result.ExitCode != exitCode || !strings.Contains(result.Stdout, "ACD SETTINGS") {
				t.Fatalf("PTY exit=%d want %d with child output\n%s", result.ExitCode, exitCode, result.Stdout)
			}
		})
	}
}

func TestPTYListDefaultsToScrollableSnapshot(t *testing.T) {
	f := newFinishFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	result := runPTYCommand(t, ctx, f.Env, 80, 4, 0, 0, "", f.Bin, "list")
	if result.ExitCode != 0 || ctx.Err() != nil || strings.Count(result.Stdout, "REPOSITORY") != 1 || !strings.Contains(result.Stdout, "SAVED") {
		t.Fatalf("default list did not exit with one snapshot: %+v", result)
	}
	if strings.Contains(result.Stdout, "\x1b[2J") || strings.Contains(result.Stdout, "\x1b[?1049h") || strings.Contains(result.Stdout, "Updated:") {
		t.Fatalf("default list took over the terminal: %q", result.Stdout)
	}
}

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

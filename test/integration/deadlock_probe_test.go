//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestDaemon_GoroutineDeadlockProbe(t *testing.T) {
	f := newFinishFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		writeFile(t, filepath.Join(f.Repo, fmt.Sprintf("dead-%02d.txt", i)), fmt.Sprintf("body %d\n", i))
		res := f.Run(t, ctx, "internal", "hint")
		if res.ExitCode != 0 {
			t.Fatalf("wake: %+v", res)
		}
	}
	f.Barrier(t, ctx)
	result := f.Run(t, ctx, "off", "--json")
	if result.ExitCode != 0 {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		artifact := filepath.Join(t.TempDir(), "deadlock-stacks.log")
		_ = os.WriteFile(artifact, buf[:n], 0600)
		t.Fatalf("owned worker failed to stop within 30s: %+v; stacks %s", result, artifact)
	}
	if readDaemonStateMode(f.Repo) == "running" {
		t.Fatal("off left checkpoint worker running")
	}
	// The final stop checkpoint must retain the whole burst, without branch publication.
	for i := 0; i < 5; i++ {
		finishWaitProtectedFile(t, f, fmt.Sprintf("dead-%02d.txt", i), fmt.Sprintf("body %d\n", i), time.Second)
	}
}

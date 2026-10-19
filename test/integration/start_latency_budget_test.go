//go:build integration

package integration_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestStartLatencyBudget_RepeatedActiveHooks(t *testing.T) {
	f := newFinishFixture(t, "ACD_FSNOTIFY_ENABLED=0")
	const hooks = 10
	const perCallBudget = 200 * time.Millisecond
	const aggregateBudget = 1500 * time.Millisecond
	var total time.Duration
	for i := 0; i < hooks; i++ {
		body := fmt.Sprintf("hook edit %d\n", i)
		writeFile(t, filepath.Join(f.Repo, "hook.txt"), body)
		start := time.Now()
		res := f.Run(t, t.Context(), "internal", "integration", "event", "--harness", "claude-code", "--event", "session_open")
		elapsed := time.Since(start)
		if res.ExitCode != 0 {
			t.Fatalf("hook %d failed: %+v", i, res)
		}
		if elapsed > perCallBudget {
			t.Fatalf("hook %d took %s, budget %s", i, elapsed, perCallBudget)
		}
		total += elapsed
		// A fail-open hook exit alone is insufficient: prove its exact bytes reached protection, without a barrier or a watcher wake.
		finishWaitProtectedFile(t, f, "hook.txt", body, 1500*time.Millisecond)
	}
	if total > aggregateBudget {
		t.Fatalf("ten hooks took %s, budget %s", total, aggregateBudget)
	}
}

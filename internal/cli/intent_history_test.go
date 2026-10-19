package cli

import (
	"context"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"os"
	"testing"
	"time"
)

func serveIntentHistoryWorker(t *testing.T, lookup controlRepoLookup, handler supervisor.WorkerHandler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- supervisor.ServeWorker(ctx, supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID), handler)
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker socket: %v", err)
		}
	})
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID)); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("worker socket did not start")
		}
		time.Sleep(time.Millisecond)
	}
}

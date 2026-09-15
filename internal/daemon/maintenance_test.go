package daemon

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRunMaintenanceFailureKeepsPublicationMoving(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := checkpoint.Store{DB: f.db}
	_, err := store.Create(ctx, checkpoint.Request{RepoRoot: f.dir, WorktreeID: checkpoint.WorktreeID(f.dir), Reason: state.CheckpointReasonManualBarrier, ObservationEpoch: 1, CoverageEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSet(ctx, f.db, MetaKeyProtectionRetentionOverBudget, "needs_action"); err != nil {
		t.Fatal(err)
	}
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	flag := filepath.Join(shim, "license")
	if err := os.WriteFile(flag, nil, 0600); err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("#!/bin/sh\nif [ -e '%s' ] && [ \"$1\" = rev-list ] && [ \"$2\" = --objects ]; then\n echo 'You have not agreed to the Xcode license agreements.' >&2\n exit 69\nfi\nexec '%s' \"$@\"\n", flag, gitPath)
	if err := os.WriteFile(filepath.Join(shim, "git"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	startHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	var offset atomic.Int64
	wake := make(chan struct{}, 4)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{RepoPath: f.dir, GitDir: f.gitDir, DB: f.db, Scheduler: fastScheduler(), BootGrace: 30 * time.Minute,
			MessageFn: DeterministicMessage, WakeCh: wake, SkipSignals: true, Now: func() time.Time { return time.Now().Add(time.Duration(offset.Load()) * time.Second) }})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("daemon: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("daemon failed to stop")
		}
	})
	waitForDaemonMode(t, f.db, "running", 5*time.Second)
	waitMaintenance := func(want string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			m, err := store.LoadMaintenance(ctx)
			if err == nil && m.State == want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("maintenance did not reach %s", want)
	}
	waitMaintenance("prerequisite")
	if err := os.WriteFile(filepath.Join(f.dir, "during-maintenance.txt"), []byte("protected and published\n"), 0644); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	waitForCommit(t, f.dir, startHead, 10*time.Second)
	if err := os.Remove(flag); err != nil {
		t.Fatal(err)
	}
	offset.Store(61)
	wake <- struct{}{}
	waitMaintenance("healthy")
}

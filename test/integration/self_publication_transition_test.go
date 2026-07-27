//go:build integration
// +build integration

package integration_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
)

// TestDuplicateWriterAfterStateMove pins the split-brain sequence observed in
// the live repository: a daemon holds the legacy lock inside .git/acd, that
// movable state directory is quarantined, and a second daemon attempts to
// acquire ownership through the newly-created state directory.
//
// No process timing is involved. The held flock, directory rename, contention,
// release, and reacquisition form explicit boundaries. The regression contract
// is that ownership remains fenced by the stable Git common directory even
// while the legacy state path moves.
func TestDuplicateWriterAfterStateMove(t *testing.T) {
	repo := tempRepo(t)
	gitDir := filepath.Join(repo, ".git")
	stateDir := filepath.Join(gitDir, "acd")
	movedStateDir := filepath.Join(gitDir, "acd-quarantined")

	first, err := daemon.AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("acquire first daemon lock: %v", err)
	}
	firstReleased := false
	t.Cleanup(func() {
		if !firstReleased {
			_ = first.Release()
		}
	})

	if err := os.Rename(stateDir, movedStateDir); err != nil {
		t.Fatalf("quarantine movable state directory: %v", err)
	}

	second, err := daemon.AcquireDaemonLock(gitDir)
	if second != nil {
		_ = second.Release()
		t.Fatal("second writer acquired ownership after .git/acd moved")
	}
	if !errors.Is(err, daemon.ErrDaemonLockHeld) {
		t.Fatalf("second writer error=%v want ErrDaemonLockHeld", err)
	}

	if err := first.Release(); err != nil {
		t.Fatalf("release first daemon lock: %v", err)
	}
	firstReleased = true

	reacquired, err := daemon.AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("reacquire ownership after first writer released: %v", err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatalf("release reacquired daemon lock: %v", err)
	}
}

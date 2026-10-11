//go:build integration

package integration_test

import (
	"errors"
	"fmt"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"testing"
	"time"
)

// TestCanonicalWriterAfterStateMove pins the split-brain sequence observed in
// the live repository: a daemon holds the legacy lock inside .git/acd, that
// movable state directory is quarantined, and a second daemon attempts to
// acquire ownership through the newly-created state directory.
//
// No process timing is involved. The held flock, directory rename, contention,
// release, and reacquisition form explicit boundaries. The regression contract
// is that ownership remains fenced by the stable Git common directory even
// while the legacy state path moves.
func TestFinishFaultCanonicalWriterAfterStateMove(t *testing.T) {
	t.Parallel()

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

func TestFinishFaultCanonicalWriterAcrossLinkedWorktree(t *testing.T) {
	t.Parallel()

	repo := tempRepo(t)
	linked := filepath.Join(t.TempDir(), "linked")
	runGitOK(t, repo, "worktree", "add", "-q", "-b", "linked", linked)
	mainGitDir := strings.TrimSpace(runGitOK(
		t, repo, "rev-parse", "--absolute-git-dir"))
	linkedGitDir := strings.TrimSpace(runGitOK(
		t, linked, "rev-parse", "--absolute-git-dir"))

	first, err := daemon.AcquireDaemonLock(mainGitDir)
	if err != nil {
		t.Fatalf("acquire main-worktree writer: %v", err)
	}
	defer func() { _ = first.Release() }()
	second, err := daemon.AcquireDaemonLock(linkedGitDir)
	if second != nil {
		_ = second.Release()
		t.Fatal("linked worktree acquired a second canonical writer")
	}
	if !errors.Is(err, daemon.ErrDaemonLockHeld) {
		t.Fatalf("linked writer error=%v want ErrDaemonLockHeld", err)
	}
}

// sqliteScalar runs `sqlite3 db -- "SELECT ..."` and returns the trimmed
// stdout. Failures fail the test (these are pure read-only probes).
func sqliteScalar(t *testing.T, dbPath, query string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", dbPath, query).CombinedOutput()
	if err != nil {
		t.Fatalf("sqlite3 %s: %v\n%s", query, err, out)
	}
	return strings.TrimSpace(string(out))
}

func nowFloatSeconds() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func gitHashObjectStdin(t *testing.T, repo, body string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", repo, "hash-object", "-w", "--stdin")
	cmd.Stdin = strings.NewReader(body)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git hash-object -w --stdin: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	out, err := exec.Command("ps", "-p", fmt.Sprintf("%d", pid), "-o", "stat=").CombinedOutput()
	if err != nil {
		return false
	}
	return !strings.HasPrefix(strings.TrimSpace(string(out)), "Z")
}

func requireSQLite(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 required")
	}
}

func sqliteExec(t *testing.T, dbPath, statement string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", dbPath, statement).CombinedOutput()
	if err != nil {
		t.Fatalf("sqlite statement: %v %s", err, out)
	}
	return strings.TrimSpace(string(out))
}

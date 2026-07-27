package daemon

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestDaemonLockCanonicalWriterContendsAndReacquires(t *testing.T) {
	repo, gitDir := initDaemonLockRepo(t)

	first, err := AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("first AcquireDaemonLock: %v", err)
	}
	defer func() { _ = first.Release() }()

	if _, err := AcquireDaemonLock(gitDir); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("second AcquireDaemonLock error = %v, want ErrDaemonLockHeld (EX_TEMPFAIL %d)", err, ExitTempFail)
	}
	if _, err := os.Stat(filepath.Join(gitDir, "acd-daemon.lock")); err != nil {
		t.Fatalf("stable lock under common dir for %s: %v", repo, err)
	}
	if _, err := os.Stat(filepath.Join(gitDir, "acd", "daemon.lock")); err != nil {
		t.Fatalf("legacy lock under git dir: %v", err)
	}

	if err := first.Release(); err != nil {
		t.Fatalf("release first lock: %v", err)
	}
	second, err := AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release reacquired lock: %v", err)
	}
}

func TestDaemonLockStateMoveRetainsCanonicalWriter(t *testing.T) {
	_, gitDir := initDaemonLockRepo(t)
	first, err := AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("first AcquireDaemonLock: %v", err)
	}
	defer func() { _ = first.Release() }()

	stateDir := filepath.Join(gitDir, "acd")
	movedDir := filepath.Join(gitDir, "acd-moved")
	if err := os.Rename(stateDir, movedDir); err != nil {
		t.Fatalf("rename state dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("replace state dir: %v", err)
	}

	if _, err := AcquireDaemonLock(gitDir); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("AcquireDaemonLock after state move error = %v, want ErrDaemonLockHeld", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release first lock: %v", err)
	}
	second, err := AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("reacquire after owner release: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release second lock: %v", err)
	}
}

func TestDaemonLockLinkedWorktreeOwner(t *testing.T) {
	repo, mainGitDir := initDaemonLockRepo(t)
	linked := filepath.Join(t.TempDir(), "linked")
	runDaemonLockGit(t, repo, "worktree", "add", "-b", "linked-lock-test", linked)
	linkedGitDir := strings.TrimSpace(runDaemonLockGit(t, linked, "rev-parse", "--absolute-git-dir"))
	if linkedGitDir == mainGitDir {
		t.Fatalf("linked git dir unexpectedly equals main git dir %q", mainGitDir)
	}

	first, err := AcquireDaemonLock(mainGitDir)
	if err != nil {
		t.Fatalf("main AcquireDaemonLock: %v", err)
	}
	defer func() { _ = first.Release() }()
	if _, err := AcquireDaemonLock(linkedGitDir); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("linked AcquireDaemonLock error = %v, want ErrDaemonLockHeld", err)
	}

	if err := first.Release(); err != nil {
		t.Fatalf("release main lock: %v", err)
	}
	linkedLock, err := AcquireDaemonLock(linkedGitDir)
	if err != nil {
		t.Fatalf("linked reacquire after release: %v", err)
	}
	if err := linkedLock.Release(); err != nil {
		t.Fatalf("release linked lock: %v", err)
	}
}

func TestDaemonLockMixedVersionLegacyContentionReleasesStableFence(t *testing.T) {
	_, gitDir := initDaemonLockRepo(t)
	legacyDir := filepath.Join(gitDir, "acd")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy lock dir: %v", err)
	}
	legacy, err := os.OpenFile(filepath.Join(legacyDir, "daemon.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open legacy lock: %v", err)
	}
	defer legacy.Close()
	if err := syscall.Flock(int(legacy.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock legacy lock: %v", err)
	}
	defer syscall.Flock(int(legacy.Fd()), syscall.LOCK_UN) //nolint:errcheck

	if _, err := AcquireDaemonLock(gitDir); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("AcquireDaemonLock error = %v, want mixed-version ErrDaemonLockHeld", err)
	}

	stable, err := acquireFileLock(filepath.Join(gitDir, "acd-daemon.lock"))
	if err != nil {
		t.Fatalf("stable lock remained held after legacy contention: %v", err)
	}
	if err := releaseFileLock(stable); err != nil {
		t.Fatalf("release stable verification lock: %v", err)
	}
}

func initDaemonLockRepo(t *testing.T) (repo, gitDir string) {
	t.Helper()
	repo = t.TempDir()
	runDaemonLockGit(t, repo, "init")
	runDaemonLockGit(t, repo, "symbolic-ref", "HEAD", "refs/heads/main")
	if err := os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("initial\n"), 0o600); err != nil {
		t.Fatalf("write tracked file: %v", err)
	}
	runDaemonLockGit(t, repo, "add", "tracked.txt")
	runDaemonLockGit(t, repo, "-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid", "commit", "-m", "initial")
	gitDir = strings.TrimSpace(runDaemonLockGit(t, repo, "rev-parse", "--absolute-git-dir"))
	return repo, gitDir
}

func runDaemonLockGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, dir, err, out)
	}
	return string(out)
}

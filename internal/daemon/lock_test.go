package daemon

import (
	"context"
	"errors"
	"fmt"
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

func TestDaemonLockMixedVersionStateMoveRetainsOldWriter(t *testing.T) {
	_, gitDir := initDaemonLockRepo(t)
	stateDir := filepath.Join(gitDir, "acd")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy state dir: %v", err)
	}
	legacy, err := acquireFileLock(filepath.Join(stateDir, "daemon.lock"))
	if err != nil {
		t.Fatalf("acquire old-version lock: %v", err)
	}
	defer releaseFileLock(legacy) //nolint:errcheck

	movedDir := filepath.Join(gitDir, "acd-moved")
	if err := os.Rename(stateDir, movedDir); err != nil {
		t.Fatalf("rename locked state dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("replace state dir: %v", err)
	}

	if _, err := AcquireDaemonLock(gitDir); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("AcquireDaemonLock after old-version state move error = %v, want ErrDaemonLockHeld", err)
	}
}

func TestDaemonLockMixedVersionMoveBetweenScanAndAcquire(t *testing.T) {
	_, gitDir := initDaemonLockRepo(t)
	stateDir := filepath.Join(gitDir, "acd")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy state dir: %v", err)
	}
	oldWriter, err := acquireFileLock(filepath.Join(stateDir, "daemon.lock"))
	if err != nil {
		t.Fatalf("acquire old-version lock: %v", err)
	}
	defer releaseFileLock(oldWriter) //nolint:errcheck

	moved := false
	oldHook := daemonLockAfterLegacyScan
	daemonLockAfterLegacyScan = func() {
		if moved {
			return
		}
		moved = true
		if err := os.Rename(stateDir, filepath.Join(gitDir, "acd-moved-during-acquire")); err != nil {
			t.Fatalf("rename locked state dir from scan hook: %v", err)
		}
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			t.Fatalf("replace state dir from scan hook: %v", err)
		}
	}
	t.Cleanup(func() { daemonLockAfterLegacyScan = oldHook })

	if _, err := AcquireDaemonLock(gitDir); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("AcquireDaemonLock after scan/acquire move error = %v, want ErrDaemonLockHeld", err)
	}

	stable, err := acquireFileLock(filepath.Join(gitDir, "acd-daemon.lock"))
	if err != nil {
		t.Fatalf("stable lock remained held after fixed-point contention: %v", err)
	}
	if err := releaseFileLock(stable); err != nil {
		t.Fatalf("release stable verification lock: %v", err)
	}
}

func TestDaemonLockOwnerProbeClosesPostScanRelocation(t *testing.T) {
	_, gitDir := initDaemonLockRepo(t)
	var oldWriter *os.File
	oldBeforeProbe := daemonLockBeforeOwnerProbe
	oldProbe := daemonLockOwnerProbe
	daemonLockBeforeOwnerProbe = func() {
		stateDir := filepath.Join(gitDir, "acd")
		if err := os.Rename(
			stateDir, filepath.Join(gitDir, "acd-moved-after-final-scan")); err != nil {
			t.Fatalf("rename state after final scan: %v", err)
		}
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			t.Fatalf("replace state after final scan: %v", err)
		}
		var err error
		oldWriter, err = acquireFileLock(filepath.Join(stateDir, "daemon.lock"))
		if err != nil {
			t.Fatalf("simulate old writer after final scan: %v", err)
		}
	}
	daemonLockOwnerProbe = func(
		context.Context, string, int,
	) ([]int, error) {
		return []int{4242}, nil
	}
	t.Cleanup(func() {
		daemonLockBeforeOwnerProbe = oldBeforeProbe
		daemonLockOwnerProbe = oldProbe
		_ = releaseFileLock(oldWriter)
	})

	if _, err := AcquireDaemonLock(gitDir); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("AcquireDaemonLock after post-scan old owner error = %v, want ErrDaemonLockHeld", err)
	}
	stable, err := acquireFileLock(filepath.Join(gitDir, "acd-daemon.lock"))
	if err != nil {
		t.Fatalf("stable lock remained held after owner proof: %v", err)
	}
	if err := releaseFileLock(stable); err != nil {
		t.Fatalf("release stable verification lock: %v", err)
	}
}

func TestDaemonLockOwnerProbeIgnoresUnrelatedStaleRepos(t *testing.T) {
	repo, gitDir := initDaemonLockRepo(t)
	oldList := daemonLockProcessList
	daemonLockProcessList = func(
		context.Context,
	) ([]daemonOwnerProcess, error) {
		processes := make([]daemonOwnerProcess, 0, 25)
		for i := 0; i < 24; i++ {
			processes = append(processes, daemonOwnerProcess{
				pid: i + 1,
				command: fmt.Sprintf(
					"acd daemon run --repo /removed/unrelated-%d", i),
			})
		}
		processes = append(processes, daemonOwnerProcess{
			pid: 9001, command: "acd daemon run --repo " + repo,
		})
		return processes, nil
	}
	t.Cleanup(func() { daemonLockProcessList = oldList })

	_, commonDir, err := daemonLockDirs(gitDir)
	if err != nil {
		t.Fatal(err)
	}
	owners, err := findLegacyDaemonOwners(
		context.Background(), commonDir, 0)
	if err != nil {
		t.Fatalf("findLegacyDaemonOwners: %v", err)
	}
	if fmt.Sprint(owners) != "[9001]" {
		t.Fatalf("owners=%v want [9001]", owners)
	}
}

func TestDaemonOwnerKnownRootsIsBoundedAndCancellable(t *testing.T) {
	_, gitDir := initDaemonLockRepo(t)
	_, commonDir, err := daemonLockDirs(gitDir)
	if err != nil {
		t.Fatal(err)
	}
	worktreesDir := filepath.Join(commonDir, "worktrees")
	for i := 0; i < 3; i++ {
		adminDir := filepath.Join(worktreesDir, fmt.Sprintf("linked-%d", i))
		if err := os.MkdirAll(adminDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(adminDir, "gitdir"),
			[]byte(filepath.Join(t.TempDir(), ".git")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	oldLimit := daemonOwnerWorktreeEntryLimit
	daemonOwnerWorktreeEntryLimit = 2
	t.Cleanup(func() { daemonOwnerWorktreeEntryLimit = oldLimit })

	if _, err := daemonOwnerKnownRoots(
		context.Background(), commonDir); err == nil ||
		!strings.Contains(err.Error(), "exceeds 2 entries") {
		t.Fatalf("bounded owner root scan err=%v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := daemonOwnerKnownRoots(
		cancelled, commonDir); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled owner root scan err=%v want context.Canceled", err)
	}
}

func TestParseDaemonRunRepoArgUsesRawProductionSuffix(t *testing.T) {
	for _, tc := range []struct {
		command string
		want    string
		ok      bool
	}{
		{`/usr/local/bin/acd daemon run --repo /tmp/repo - archive`,
			`/tmp/repo - archive`, true},
		{`acd daemon run --repo=/tmp/repo\backslash`,
			`/tmp/repo\backslash`, true},
		{`acd daemon run --repo "/tmp/unmatched`,
			`"/tmp/unmatched`, true},
		{`/bin/sh -c 'acd daemon run --repo /tmp/repo'`, "", false},
		{`env acd daemon run --repo /tmp/repo`, "", false},
	} {
		got, ok := ParseDaemonRunRepoArg(tc.command)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("ParseDaemonRunRepoArg(%q)=(%q,%t), want (%q,%t)",
				tc.command, got, ok, tc.want, tc.ok)
		}
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

func TestDaemonLockMixedVersionLinkedWorktreeContention(t *testing.T) {
	repo, mainGitDir := initDaemonLockRepo(t)
	linked := filepath.Join(t.TempDir(), "linked")
	runDaemonLockGit(t, repo, "worktree", "add", "-b", "linked-mixed-version-test", linked)
	linkedGitDir := strings.TrimSpace(runDaemonLockGit(t, linked, "rev-parse", "--absolute-git-dir"))
	legacyPath := filepath.Join(linkedGitDir, "acd", "daemon.lock")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir linked legacy state dir: %v", err)
	}
	oldWriter, err := acquireFileLock(legacyPath)
	if err != nil {
		t.Fatalf("acquire linked old-version lock: %v", err)
	}
	defer releaseFileLock(oldWriter) //nolint:errcheck

	if _, err := AcquireDaemonLock(mainGitDir); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("main AcquireDaemonLock with linked old writer error = %v, want ErrDaemonLockHeld", err)
	}
}

func TestDaemonLockFencesOldWriterInEveryExistingWorktree(t *testing.T) {
	repo, mainGitDir := initDaemonLockRepo(t)
	linked := filepath.Join(t.TempDir(), "linked")
	runDaemonLockGit(t, repo, "worktree", "add", "-b", "linked-new-owner-test", linked)
	linkedGitDir := strings.TrimSpace(runDaemonLockGit(t, linked, "rev-parse", "--absolute-git-dir"))

	lock, err := AcquireDaemonLock(mainGitDir)
	if err != nil {
		t.Fatalf("AcquireDaemonLock: %v", err)
	}
	defer lock.Release() //nolint:errcheck

	if _, err := acquireFileLock(filepath.Join(linkedGitDir, "acd", "daemon.lock")); !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("old writer linked lock error = %v, want ErrDaemonLockHeld", err)
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

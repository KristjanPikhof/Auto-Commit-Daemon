// lock.go provides per-repo flock primitives:
//
//   - <git-common-dir>/acd-daemon.lock — held exclusively by the live daemon
//     for its entire run. The stable location keeps ownership fenced if the
//     movable <git-dir>/acd state directory is replaced and makes linked
//     worktrees contend as one repository.
//   - <git-dir>/acd/daemon.lock — acquired after the stable lock for
//     compatibility with older ACD versions.
//     Contention on either lock => another daemon is already alive; the caller
//     exits with EX_TEMPFAIL (75) so wrappers can distinguish "peer running"
//     from "started cleanly".
//   - control.lock — held briefly by `acd start`/`stop`/`wake`/`touch` to
//     serialize read-modify-write of daemon_clients. The daemon itself does
//     NOT hold this except during sweeps where the GC needs an atomic view
//     of the table. Brief acquisition/release pattern.
//
// Implementation uses syscall.Flock (no cgo). LOCK_EX | LOCK_NB returns
// immediately if the lock is contended, which is what every caller wants.
package daemon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrDaemonLockHeld is returned by AcquireDaemonLock when another daemon
// already holds the per-repo daemon.lock. Callers can check via errors.Is
// to map onto EX_TEMPFAIL (75).
var ErrDaemonLockHeld = errors.New("daemon: daemon.lock held by another process")

// ErrControlLockHeld is the equivalent for the brief control lock used by
// CLI subcommands.
var ErrControlLockHeld = errors.New("daemon: control.lock held by another process")

// ExitTempFail is the EX_TEMPFAIL exit code from sysexits.h. Callers that
// observe ErrDaemonLockHeld should exit with this code.
const ExitTempFail = 75

// DaemonLock is the held-for-life-of-daemon flock handle.
type DaemonLock struct {
	stable *os.File
	legacy *os.File
}

// AcquireDaemonLock first acquires <git-common-dir>/acd-daemon.lock, then the
// legacy <gitDir>/acd/daemon.lock, both with LOCK_EX|LOCK_NB. Linked worktree
// git dirs identify their common dir through Git's commondir file. Keeping the
// stable lock outside the replaceable acd state directory prevents a rename or
// deletion of that directory from creating a second lock inode.
//
// Returns an error matching ErrDaemonLockHeld on contention; any other error is
// a hard failure that callers should surface verbatim.
func AcquireDaemonLock(gitDir string) (*DaemonLock, error) {
	if gitDir == "" {
		return nil, fmt.Errorf("daemon: AcquireDaemonLock: empty gitDir")
	}
	gitDir, commonDir, err := daemonLockDirs(gitDir)
	if err != nil {
		return nil, err
	}

	stablePath := filepath.Join(commonDir, "acd-daemon.lock")
	stable, err := acquireFileLock(stablePath)
	if err != nil {
		if errors.Is(err, ErrDaemonLockHeld) {
			return nil, fmt.Errorf(
				"%w: stable repository lock %s is held; another ACD daemon, linked worktree, or mixed-version owner is active",
				ErrDaemonLockHeld, stablePath,
			)
		}
		return nil, fmt.Errorf("daemon: acquire stable repository lock: %w", err)
	}

	dir := filepath.Join(gitDir, stateSubdir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		_ = releaseFileLock(stable)
		return nil, fmt.Errorf("daemon: mkdir lock parent: %w", err)
	}
	legacyPath := filepath.Join(dir, "daemon.lock")
	legacy, err := acquireFileLock(legacyPath)
	if err != nil {
		_ = releaseFileLock(stable)
		if errors.Is(err, ErrDaemonLockHeld) {
			return nil, fmt.Errorf(
				"%w: legacy worktree lock %s is held; an older or concurrently starting ACD daemon may own this repository",
				ErrDaemonLockHeld, legacyPath,
			)
		}
		return nil, fmt.Errorf("daemon: acquire legacy worktree lock: %w", err)
	}
	return &DaemonLock{stable: stable, legacy: legacy}, nil
}

func daemonLockDirs(gitDir string) (canonicalGitDir, commonDir string, retErr error) {
	abs, err := filepath.Abs(gitDir)
	if err != nil {
		return "", "", fmt.Errorf("daemon: resolve git dir %q: %w", gitDir, err)
	}
	canonicalGitDir = filepath.Clean(abs)
	if real, err := filepath.EvalSymlinks(canonicalGitDir); err == nil {
		canonicalGitDir = filepath.Clean(real)
	}

	commonDir = canonicalGitDir
	body, err := os.ReadFile(filepath.Join(canonicalGitDir, "commondir"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return canonicalGitDir, commonDir, nil
		}
		return "", "", fmt.Errorf("daemon: read Git commondir: %w", err)
	}
	value := string(body)
	for len(value) > 0 && (value[len(value)-1] == '\n' || value[len(value)-1] == '\r') {
		value = value[:len(value)-1]
	}
	if value == "" {
		return "", "", errors.New("daemon: Git commondir is empty")
	}
	if filepath.IsAbs(value) {
		commonDir = filepath.Clean(value)
	} else {
		commonDir = filepath.Clean(filepath.Join(canonicalGitDir, value))
	}
	if real, err := filepath.EvalSymlinks(commonDir); err == nil {
		commonDir = filepath.Clean(real)
	}
	return canonicalGitDir, commonDir, nil
}

func acquireFileLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrDaemonLockHeld
		}
		return nil, fmt.Errorf("flock %s: %w", path, err)
	}
	return f, nil
}

func releaseFileLock(f *os.File) error {
	if f == nil {
		return nil
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return f.Close()
}

// Release drops the daemon lock and closes the underlying file. Safe to call
// multiple times; subsequent calls are no-ops. Locks are released in reverse
// acquisition order so the legacy compatibility fence remains held until the
// stable repository fence is ready to drop.
func (l *DaemonLock) Release() error {
	if l == nil {
		return nil
	}
	legacyErr := releaseFileLock(l.legacy)
	l.legacy = nil
	stableErr := releaseFileLock(l.stable)
	l.stable = nil
	return errors.Join(legacyErr, stableErr)
}

// ControlLock is the brief flock held by CLI subcommands.
type ControlLock struct {
	f *os.File
}

// AcquireControlLock acquires <gitDir>/acd/control.lock with LOCK_EX|LOCK_NB.
// Returns ErrControlLockHeld on contention.
func AcquireControlLock(gitDir string) (*ControlLock, error) {
	if gitDir == "" {
		return nil, fmt.Errorf("daemon: AcquireControlLock: empty gitDir")
	}
	dir := filepath.Join(gitDir, stateSubdir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("daemon: mkdir control lock parent: %w", err)
	}
	path := filepath.Join(dir, "control.lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("daemon: open control.lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrControlLockHeld
		}
		return nil, fmt.Errorf("daemon: flock control.lock: %w", err)
	}
	return &ControlLock{f: f}, nil
}

// Release drops the control lock and closes the file.
func (l *ControlLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}

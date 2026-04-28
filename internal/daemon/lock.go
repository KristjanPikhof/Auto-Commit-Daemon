// lock.go provides per-repo flock primitives:
//
//   - daemon.lock — held exclusively by the live daemon for its entire run.
//     Contention => another daemon is already alive; the caller exits with
//     EX_TEMPFAIL (75) so wrappers can distinguish "peer running" from
//     "started cleanly".
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
	f *os.File
}

// AcquireDaemonLock acquires <gitDir>/acd/daemon.lock with LOCK_EX|LOCK_NB.
// Returns ErrDaemonLockHeld on contention; any other error is a hard failure
// (mkdir, open) that callers should surface verbatim.
func AcquireDaemonLock(gitDir string) (*DaemonLock, error) {
	if gitDir == "" {
		return nil, fmt.Errorf("daemon: AcquireDaemonLock: empty gitDir")
	}
	dir := filepath.Join(gitDir, stateSubdir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("daemon: mkdir lock parent: %w", err)
	}
	path := filepath.Join(dir, "daemon.lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("daemon: open daemon.lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrDaemonLockHeld
		}
		return nil, fmt.Errorf("daemon: flock daemon.lock: %w", err)
	}
	return &DaemonLock{f: f}, nil
}

// Release drops the daemon lock and closes the underlying file. Safe to call
// multiple times; subsequent calls are no-ops.
func (l *DaemonLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	// flock is released automatically when the fd closes, but we issue an
	// explicit LOCK_UN so the unlock happens before any other goroutine
	// might race on Close.
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
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

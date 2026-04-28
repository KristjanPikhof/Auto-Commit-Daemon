// Package identity verifies process liveness + identity.
//
// Two primitives:
//   - Alive(pid) — fast-path liveness via kill(pid, 0). Per D20, the daemon
//     uses heartbeat as primary liveness; this is the cheap PID probe used
//     before doing the heartbeat lookup.
//   - Fingerprint{} — defends against PID reuse by binding pid → process
//     start time + argv hash. See fingerprint.go.
//
// Implementation per §3.4.
package identity

import (
	"errors"
	"syscall"
)

// Alive reports whether a process with the given pid currently exists.
//
// Implemented via signal 0 ("null signal"): the kernel performs all the
// permission checks but does not deliver the signal. The classic POSIX
// idiom for liveness without side effects.
//
// EPERM means the process exists but we lack permission to signal it; we
// still treat that as "alive" — for refcount-style consumers, "exists owned
// by someone else" is the same answer as "exists owned by us".
//
// pid <= 0 is rejected up-front: kill(0|-1, 0) signals process groups, not
// what we want.
func Alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	// EPERM → process exists, owned by someone else. ESRCH → no such pid.
	return errors.Is(err, syscall.EPERM)
}

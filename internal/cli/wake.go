package cli

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// signalProcess is the injection point used by tests to verify that wake
// would have sent SIGUSR1 without involving real OS signals.
var captureProcessFingerprint = identity.Capture
var killProcess = syscall.Kill

var signalProcess = func(pid int, sig syscall.Signal, expectedFingerprint string) error {
	if pid <= 0 {
		return errors.New("invalid pid")
	}
	if expectedFingerprint != "" {
		fp, err := captureProcessFingerprint(pid)
		if err == nil && daemon.FingerprintToken(fp) != expectedFingerprint {
			return fmt.Errorf("verify process identity for pid %d: fingerprint mismatch", pid)
		}
	}
	return killProcess(pid, sig)
}

func daemonFingerprintToken(st state.DaemonState) string {
	if !st.DaemonFingerprint.Valid {
		return ""
	}
	return st.DaemonFingerprint.String
}

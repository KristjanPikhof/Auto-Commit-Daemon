package cli

import (
	"errors"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
)

func TestSignalProcessRejectsFingerprintMismatchBeforeKill(t *testing.T) {
	prevCapture := captureProcessFingerprint
	prevKill := killProcess
	t.Cleanup(func() {
		captureProcessFingerprint = prevCapture
		killProcess = prevKill
	})

	captureProcessFingerprint = func(pid int) (identity.Fingerprint, error) {
		return identity.Fingerprint{StartTime: "new", ArgvHash: "new"}, nil
	}
	var killCalls atomic.Int32
	killProcess = func(pid int, sig syscall.Signal) error {
		killCalls.Add(1)
		return nil
	}

	stored := daemon.FingerprintToken(identity.Fingerprint{StartTime: "old", ArgvHash: "old"})
	err := signalProcess(424242, syscall.SIGKILL, stored)
	if err == nil || !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("signalProcess error=%v, want fingerprint mismatch", err)
	}
	if killCalls.Load() != 0 {
		t.Fatalf("kill called despite fingerprint mismatch")
	}
}

func TestSignalProcessContinuesWhenFingerprintUnresolvable(t *testing.T) {
	prevCapture := captureProcessFingerprint
	prevKill := killProcess
	t.Cleanup(func() {
		captureProcessFingerprint = prevCapture
		killProcess = prevKill
	})

	captureProcessFingerprint = func(pid int) (identity.Fingerprint, error) {
		return identity.Fingerprint{}, errors.New("ps unavailable")
	}
	var killCalls atomic.Int32
	killProcess = func(pid int, sig syscall.Signal) error {
		killCalls.Add(1)
		return nil
	}

	stored := daemon.FingerprintToken(identity.Fingerprint{StartTime: "old", ArgvHash: "old"})
	if err := signalProcess(424242, syscall.SIGTERM, stored); err != nil {
		t.Fatalf("signalProcess returned error on unresolvable fingerprint: %v", err)
	}
	if killCalls.Load() != 1 {
		t.Fatalf("kill calls=%d want 1", killCalls.Load())
	}
}

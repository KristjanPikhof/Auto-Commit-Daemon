package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// installFakeSignal replaces signalProcess with a stub that records signal
// invocations rather than touching real OS processes.
type fakeSignalCall struct {
	pid int
	sig syscall.Signal
}

func installFakeSignal(t *testing.T) (*atomic.Int32, *[]fakeSignalCall, func()) {
	t.Helper()
	prev := signalProcess
	var count atomic.Int32
	calls := []fakeSignalCall{}
	signalProcess = func(pid int, sig syscall.Signal, expectedFingerprint string) error {
		count.Add(1)
		calls = append(calls, fakeSignalCall{pid: pid, sig: sig})
		return nil
	}
	return &count, &calls, func() { signalProcess = prev }
}

func TestWake_RefreshesAndSignals(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	// Pre-register a session and a live daemon (PID = current process so
	// identity.Alive returns true without spawning anything).
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "s1", Harness: "claude-code",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = db.Close()

	count, calls, restore := installFakeSignal(t)
	defer restore()

	var stdout bytes.Buffer
	if err := runWake(ctx, &stdout, repoDir, "s1", true); err != nil {
		t.Fatalf("runWake: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("expected one SIGUSR1 call, got %d", count.Load())
	}
	if (*calls)[0].sig != syscall.SIGUSR1 {
		t.Fatalf("expected SIGUSR1, got %v", (*calls)[0].sig)
	}

	// Reopen the DB to verify a flush_request was enqueued.
	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	fr, ok, err := state.ClaimNextFlushRequest(ctx, d2)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !ok || fr.Command != "wake" {
		t.Fatalf("expected wake flush_request, got ok=%v fr=%+v", ok, fr)
	}

	var got wakeResult
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || !got.SentSignal {
		t.Fatalf("expected ok+sent_signal true, got %+v", got)
	}
}

func TestWake_LazyRegisterIdempotent(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	_ = db.Close()
	_, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runWake(ctx, &out, repoDir, "new-session", true); err != nil {
		t.Fatalf("first wake: %v", err)
	}
	out.Reset()
	if err := runWake(ctx, &out, repoDir, "new-session", true); err != nil {
		t.Fatalf("second wake: %v", err)
	}
	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	clients, err := state.ListClients(ctx, d2)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("expected 1 client, got %d", len(clients))
	}
}

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

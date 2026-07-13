package cli

import (
	"bytes"
	"context"
	"database/sql"
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

func TestSignalDaemonSettingsActivationDoesNotEnqueueWake(t *testing.T) {
	count, calls, restore := installFakeSignal(t)
	defer restore()
	sent, err := signalSettingsActivation(state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		DaemonFingerprint: sql.NullString{String: "expected-fingerprint", Valid: true},
	})
	if err != nil || !sent || count.Load() != 1 || len(*calls) != 1 || (*calls)[0].sig != syscall.SIGUSR1 {
		t.Fatalf("signalSettingsActivation sent=%v err=%v calls=%+v", sent, err, *calls)
	}
	// The helper receives state and signals only; it has no DB handle and
	// therefore cannot enqueue the ordinary wake/flush protocol.
}

func TestSignalDaemonSettingsActivationStoppedAndFingerprintRequired(t *testing.T) {
	count, _, restore := installFakeSignal(t)
	defer restore()
	if sent, err := signalSettingsActivation(state.DaemonState{PID: os.Getpid(), Mode: "stopped"}); err != nil || sent {
		t.Fatalf("stopped signal sent=%v err=%v", sent, err)
	}
	if sent, err := signalSettingsActivation(state.DaemonState{PID: os.Getpid(), Mode: "running"}); err == nil || sent {
		t.Fatalf("missing fingerprint sent=%v err=%v", sent, err)
	}
	if count.Load() != 0 {
		t.Fatalf("unsafe settings activation signaled %d times", count.Load())
	}
}

// TestWake_SkipsWhenControlLockHeld covers the best-effort hook path: when a
// concurrent control caller holds control.lock, wake must return success with
// Skipped=true, must not signal any process, and must not enqueue a flush.
// The daemon's next tick reconciles state.
func TestWake_SkipsWhenControlLockHeld(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
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

	count, _, restore := installFakeSignal(t)
	defer restore()

	held, err := daemon.AcquireControlLock(repoDir + "/.git")
	if err != nil {
		t.Fatalf("pre-acquire control.lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	var out bytes.Buffer
	if err := runWake(ctx, &out, repoDir, "s1", true); err != nil {
		t.Fatalf("runWake must not error on control.lock contention, got: %v", err)
	}

	if count.Load() != 0 {
		t.Fatalf("contended wake must not signal, got %d signal calls", count.Load())
	}

	var got wakeResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || !got.Skipped || got.SkippedReason != "control_lock_held" || got.SentSignal {
		t.Fatalf("expected ok+skipped+reason=control_lock_held, got %+v", got)
	}

	// No flush_request should be queued — the contended path returns before
	// opening state.db.
	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if _, ok, err := state.ClaimNextFlushRequest(ctx, d2); err != nil {
		t.Fatalf("claim: %v", err)
	} else if ok {
		t.Fatalf("contended wake must not enqueue flush_request")
	}
}

// TestWake_PropagatesUnexpectedLockError ensures we only swallow the specific
// contention sentinel — any other lock-acquisition failure (e.g. permission
// or filesystem error) must still surface as an error.
func TestWake_PropagatesUnexpectedLockError(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	_ = db.Close()
	_, _, restore := installFakeSignal(t)
	defer restore()

	// Point the resolver at a path that doesn't exist on disk so the
	// underlying open call inside resolveGitDir fails. Use a clearly bogus
	// repo to keep the test focused.
	var out bytes.Buffer
	err := runWake(ctx, &out, repoDir+"/does-not-exist", "s1", true)
	if err == nil {
		t.Fatalf("expected error for missing repo, got nil")
	}
	if errors.Is(err, daemon.ErrControlLockHeld) {
		t.Fatalf("error must not be ErrControlLockHeld: %v", err)
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

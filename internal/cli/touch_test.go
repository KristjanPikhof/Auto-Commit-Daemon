package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"syscall"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestTouch_RefreshesLastSeenOnly(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	registerEnabledStartRepo(t, repoDir)
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "s1", Harness: "claude-code",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_ = db.Close()

	// Track signal calls — touch must NOT signal.
	count, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runTouch(ctx, &out, repoDir, "s1", true); err != nil {
		t.Fatalf("runTouch: %v", err)
	}
	if count.Load() != 0 {
		t.Fatalf("touch must not signal, got %d signal calls", count.Load())
	}

	// Verify there's no flush_request queued.
	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	_, ok, err := state.ClaimNextFlushRequest(ctx, d2)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if ok {
		t.Fatalf("touch must not enqueue flush_request")
	}

	var got touchResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK {
		t.Fatalf("expected ok=true, got %+v", got)
	}
}

// TestTouch_SkipsWhenControlLockHeld guards the best-effort behaviour added
// to handle hook contention: when another control caller holds control.lock,
// touch must return success with Skipped=true rather than a hook error.
func TestTouch_SkipsWhenControlLockHeld(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	registerEnabledStartRepo(t, repoDir)
	_ = db.Close()

	// Hold the control lock for the duration of the call.
	held, err := daemon.AcquireControlLock(repoDir + "/.git")
	if err != nil {
		t.Fatalf("pre-acquire control.lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	var out bytes.Buffer
	if err := runTouch(ctx, &out, repoDir, "s1", true); err != nil {
		t.Fatalf("runTouch must not error on control.lock contention, got: %v", err)
	}

	var got touchResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || !got.Skipped || got.SkippedReason != "control_lock_held" {
		t.Fatalf("expected ok+skipped+reason=control_lock_held, got %+v", got)
	}

	// No client row should have been written — the contended path bails out
	// before opening state.db.
	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	clients, err := state.ListClients(ctx, d2)
	if err != nil {
		t.Fatalf("list clients: %v", err)
	}
	if len(clients) != 0 {
		t.Fatalf("contended touch must not register clients, got %d", len(clients))
	}
}

func TestTouch_LazyRegistersUnknownSession(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	registerEnabledStartRepo(t, repoDir)
	_ = db.Close()
	_, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runTouch(ctx, &out, repoDir, "fresh", true); err != nil {
		t.Fatalf("runTouch: %v", err)
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
	if len(clients) != 1 || clients[0].SessionID != "fresh" {
		t.Fatalf("expected lazy-register, got %+v", clients)
	}
}

func TestTouch_SoftBoundaryPersistsRepoWideEpochAndSignals(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	registerEnabledStartRepo(t, repoDir)
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "codex-session", Harness: "codex",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:  os.Getpid(),
		Mode: "running",
		BranchRef: sql.NullString{
			String: "refs/heads/main",
			Valid:  true,
		},
		BranchGeneration: sql.NullInt64{Int64: 7, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = db.Close()

	count, calls, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runTouchWithBoundary(ctx, &out, repoDir, "codex-session",
		true, true); err != nil {
		t.Fatalf("runTouchWithBoundary: %v", err)
	}
	if count.Load() != 1 || len(*calls) != 1 ||
		(*calls)[0].sig != syscall.SIGUSR1 {
		t.Fatalf("soft boundary signal calls=%+v count=%d", *calls, count.Load())
	}

	var got touchResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || !got.SoftBoundary || got.BoundaryEpoch != 1 ||
		!got.SentSignal || got.DaemonPID != os.Getpid() {
		t.Fatalf("result=%+v", got)
	}

	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	boundaries, err := state.PendingIntentActivityBoundaries(ctx, d2, 0, 10)
	if err != nil {
		t.Fatalf("pending boundaries: %v", err)
	}
	if len(boundaries) != 1 ||
		boundaries[0].Kind != state.IntentBoundarySoft ||
		boundaries[0].Source != "touch_soft_boundary" ||
		boundaries[0].BranchRef.Valid ||
		boundaries[0].BranchGeneration.Valid {
		t.Fatalf("repo-wide soft boundary=%+v", boundaries)
	}
	fr, ok, err := state.ClaimNextFlushRequest(ctx, d2)
	if err != nil {
		t.Fatalf("claim flush request: %v", err)
	}
	if ok {
		t.Fatalf("soft boundary must not enqueue a bypass request: %+v", fr)
	}
}

func TestTouchCommandExposesSoftBoundaryFlag(t *testing.T) {
	cmd := newTouchCmd()
	flag := cmd.Flags().Lookup("soft-boundary")
	if flag == nil {
		t.Fatal("touch command missing --soft-boundary")
	}
	if flag.DefValue != "false" {
		t.Fatalf("--soft-boundary default=%q want false", flag.DefValue)
	}
}

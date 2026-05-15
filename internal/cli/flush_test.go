package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// TestFlush_SessionIDRequired guards the early-validation contract: missing
// --session-id must short-circuit before any FS / SQLite work so a misconfigured
// hook fails fast with a clear message instead of opening state.db unnecessarily.
func TestFlush_SessionIDRequired(t *testing.T) {
	_ = withIsolatedHome(t)
	var out bytes.Buffer
	err := runFlush(context.Background(), &out, "", "", true, true)
	if err == nil {
		t.Fatalf("expected error for missing session-id, got nil")
	}
	if !strings.Contains(err.Error(), "--session-id is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// TestFlush_HeartbeatOnlyDoesNotEnqueueOrSignal mirrors TestTouch_RefreshesLastSeenOnly:
// without --logical, flush must behave exactly like touch — no flush_request,
// no signal. This is the contract that lets `acd flush` safely substitute for
// `acd touch` in the legacy heartbeat-only call sites.
func TestFlush_HeartbeatOnlyDoesNotEnqueueOrSignal(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "s1", Harness: "claude-code",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_ = db.Close()

	count, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runFlush(ctx, &out, repoDir, "s1", false, true); err != nil {
		t.Fatalf("runFlush heartbeat-only: %v", err)
	}
	if count.Load() != 0 {
		t.Fatalf("heartbeat-only flush must not signal, got %d signal calls", count.Load())
	}

	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if _, ok, err := state.ClaimNextFlushRequest(ctx, d2); err != nil {
		t.Fatalf("claim: %v", err)
	} else if ok {
		t.Fatalf("heartbeat-only flush must not enqueue flush_request")
	}

	var got flushResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || got.Logical || got.SentSignal || got.FlushRequestID != 0 {
		t.Fatalf("expected ok+!logical+!sent+request_id=0, got %+v", got)
	}
}

// TestFlush_LogicalEnqueuesAndSignals covers the new --logical mode end-to-end.
// On a healthy repo with a pretend-live daemon, runFlush must:
//   - refresh the heartbeat,
//   - enqueue a flush_request labeled "flush_logical" (so the run loop's
//     IntentBypassBatchWait kicks in on the next drain),
//   - signal SIGUSR1 to the daemon pid.
func TestFlush_LogicalEnqueuesAndSignals(t *testing.T) {
	ctx := context.Background()
	// Need an initial commit so HEAD resolves to a branch ref (not detached).
	repoDir, _, _ := makeRegisteredGitRepoStateDB(t)
	dbPath := state.DBPathFromGitDir(repoDir + "/.git")
	d2, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := state.RegisterClient(ctx, d2, state.Client{
		SessionID: "s1", Harness: "claude-code",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := state.SaveDaemonState(ctx, d2, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = d2.Close()

	count, calls, restore := installFakeSignal(t)
	defer restore()

	var stdout bytes.Buffer
	if err := runFlush(ctx, &stdout, repoDir, "s1", true, true); err != nil {
		t.Fatalf("runFlush logical: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("expected one SIGUSR1 call, got %d", count.Load())
	}
	if (*calls)[0].pid != os.Getpid() {
		t.Fatalf("signal pid=%d want %d", (*calls)[0].pid, os.Getpid())
	}

	d3, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d3.Close()
	fr, ok, err := state.ClaimNextFlushRequest(ctx, d3)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !ok || fr.Command != "flush_logical" {
		t.Fatalf("expected flush_logical flush_request, got ok=%v fr=%+v", ok, fr)
	}
	if !fr.NonBlocking {
		t.Fatalf("logical flush request must be non_blocking=true, got %+v", fr)
	}

	var got flushResult
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || !got.Logical || !got.SentSignal || got.FlushRequestID == 0 || !got.BypassMinPending {
		t.Fatalf("expected ok+logical+sent+request_id+bypass, got %+v", got)
	}
}

// TestFlush_LogicalRefusesOnDetachedHEAD guards the commit-all-style refusal.
// A detached HEAD must NOT enqueue a flush_request — there is no branch
// anchor to commit against — but the heartbeat refresh must still happen so
// the harness hook stays a clean no-op signal.
func TestFlush_LogicalRefusesOnDetachedHEAD(t *testing.T) {
	ctx := context.Background()
	repoDir, _, _ := makeRegisteredGitRepoStateDB(t)
	head, err := git.RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "checkout", "--detach", head); err != nil {
		t.Fatalf("git checkout --detach: %v", err)
	}

	_, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runFlush(ctx, &out, repoDir, "s1", true, true); err != nil {
		t.Fatalf("runFlush detached: %v", err)
	}

	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if _, ok, err := state.ClaimNextFlushRequest(ctx, d2); err != nil {
		t.Fatalf("claim: %v", err)
	} else if ok {
		t.Fatalf("detached-HEAD logical flush must not enqueue flush_request")
	}

	var got flushResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || got.RefusedReason != "detached_head" || got.FlushRequestID != 0 {
		t.Fatalf("expected ok+refused=detached_head, got %+v", got)
	}
}

// TestFlush_LogicalRefusesOnManualPause guards the pause-marker refusal. The
// harness hook should never push the daemon to drain while the operator has
// the repo paused for surgery.
func TestFlush_LogicalRefusesOnManualPause(t *testing.T) {
	ctx := context.Background()
	repoDir, _, _ := makeRegisteredGitRepoStateDB(t)
	gitDir := repoDir + "/.git"
	markerDir := filepath.Join(gitDir, "acd")
	if err := os.MkdirAll(markerDir, 0o700); err != nil {
		t.Fatalf("mkdir marker parent: %v", err)
	}
	markerPath := filepath.Join(markerDir, "paused")
	if err := os.WriteFile(markerPath, []byte(`{"reason":"test","set_at":"now","set_by":"test"}`), 0o600); err != nil {
		t.Fatalf("write pause marker: %v", err)
	}

	_, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runFlush(ctx, &out, repoDir, "s1", true, true); err != nil {
		t.Fatalf("runFlush paused: %v", err)
	}

	d2, err := state.Open(ctx, state.DBPathFromGitDir(gitDir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if _, ok, err := state.ClaimNextFlushRequest(ctx, d2); err != nil {
		t.Fatalf("claim: %v", err)
	} else if ok {
		t.Fatalf("paused logical flush must not enqueue flush_request")
	}

	var got flushResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || got.RefusedReason != "manual_pause" || got.FlushRequestID != 0 {
		t.Fatalf("expected ok+refused=manual_pause, got %+v", got)
	}
}

// TestFlush_LogicalRefusesOnGitOperation guards the rebase/merge/cherry-pick
// refusal. A mid-rebase repo has a transient HEAD and must not have its
// pending captures published until the operation finishes.
func TestFlush_LogicalRefusesOnGitOperation(t *testing.T) {
	ctx := context.Background()
	repoDir, _, _ := makeRegisteredGitRepoStateDB(t)

	// MERGE_HEAD is a recognised git-op marker; touching it inside .git is
	// the simplest way to simulate "operation in progress" without driving
	// a real merge through the test.
	gitDir := repoDir + "/.git"
	if err := os.WriteFile(filepath.Join(gitDir, "MERGE_HEAD"), []byte("0000000000000000000000000000000000000000\n"), 0o644); err != nil {
		t.Fatalf("write MERGE_HEAD: %v", err)
	}

	_, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runFlush(ctx, &out, repoDir, "s1", true, true); err != nil {
		t.Fatalf("runFlush git-op: %v", err)
	}

	d2, err := state.Open(ctx, state.DBPathFromGitDir(gitDir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if _, ok, err := state.ClaimNextFlushRequest(ctx, d2); err != nil {
		t.Fatalf("claim: %v", err)
	} else if ok {
		t.Fatalf("git-op logical flush must not enqueue flush_request")
	}

	var got flushResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || !strings.HasPrefix(got.RefusedReason, "git_op_in_progress:") || got.FlushRequestID != 0 {
		t.Fatalf("expected ok+refused=git_op_in_progress:..., got %+v", got)
	}
}

// TestFlush_SkipsWhenControlLockHeld mirrors TestWake_SkipsWhenControlLockHeld:
// best-effort hook semantics under contention. The lock-held path must not
// surface as an error to the harness — the in-flight control caller will
// reconcile state on its own.
func TestFlush_SkipsWhenControlLockHeld(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	_ = db.Close()

	count, _, restore := installFakeSignal(t)
	defer restore()

	held, err := daemon.AcquireControlLock(repoDir + "/.git")
	if err != nil {
		t.Fatalf("pre-acquire control.lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	var out bytes.Buffer
	if err := runFlush(ctx, &out, repoDir, "s1", true, true); err != nil {
		t.Fatalf("runFlush must not error on control.lock contention, got: %v", err)
	}
	if count.Load() != 0 {
		t.Fatalf("contended flush must not signal, got %d signal calls", count.Load())
	}
	var got flushResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK || !got.Skipped || got.SkippedReason != "control_lock_held" {
		t.Fatalf("expected ok+skipped+reason=control_lock_held, got %+v", got)
	}
}

// TestFlush_PropagatesUnexpectedLockError mirrors TestWake_PropagatesUnexpectedLockError.
// We deliberately miss the lock-contention sentinel so any other failure
// (missing repo, permission denial) still surfaces.
func TestFlush_PropagatesUnexpectedLockError(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	_ = db.Close()
	_, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	err := runFlush(ctx, &out, repoDir+"/does-not-exist", "s1", true, true)
	if err == nil {
		t.Fatalf("expected error for missing repo, got nil")
	}
	if errors.Is(err, daemon.ErrControlLockHeld) {
		t.Fatalf("error must not be ErrControlLockHeld: %v", err)
	}
}

// TestFlush_HelpListsLogicalFlag is a smoke test for the cobra command
// surface so a future refactor that drops --logical surfaces immediately.
func TestFlush_HelpListsLogicalFlag(t *testing.T) {
	cmd := newFlushCmd()
	help := cmd.UsageString()
	for _, want := range []string{"--logical", "--session-id"} {
		if !strings.Contains(help, want) {
			t.Errorf("flush help missing %q:\n%s", want, help)
		}
	}
}

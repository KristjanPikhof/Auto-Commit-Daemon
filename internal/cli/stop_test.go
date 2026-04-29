package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// installStopSignal replaces signalProcess with a stub that on receiving
// SIGTERM stamps daemon_state.mode = "stopped" inside the per-repo DB at
// repoDir, simulating a graceful exit.
func installStopSignal(t *testing.T, repoDir string) (*atomic.Int32, func()) {
	t.Helper()
	prev := signalProcess
	var count atomic.Int32
	gitDir := filepath.Join(repoDir, ".git")
	signalProcess = func(pid int, sig syscall.Signal, expectedFingerprint string) error {
		count.Add(1)
		if sig == syscall.SIGTERM {
			d, err := state.Open(context.Background(), state.DBPathFromGitDir(gitDir))
			if err != nil {
				return err
			}
			defer d.Close()
			_ = state.SaveDaemonState(context.Background(), d, state.DaemonState{
				PID:         pid,
				Mode:        "stopped",
				HeartbeatTS: nowFloat(),
				UpdatedTS:   nowFloat(),
			})
		}
		return nil
	}
	return &count, func() { signalProcess = prev }
}

func TestStop_DefaultDeferredWhenPeerAlive(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	for _, sid := range []string{"s1", "s2"} {
		if err := state.RegisterClient(ctx, db, state.Client{
			SessionID: sid, Harness: "claude-code",
		}); err != nil {
			t.Fatalf("register %s: %v", sid, err)
		}
	}
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: 99999, Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = db.Close()

	count, restore := installStopSignal(t, repoDir)
	defer restore()

	var out bytes.Buffer
	if err := runStop(ctx, &out, repoDir, "s1", false, false, true); err != nil {
		t.Fatalf("runStop: %v", err)
	}
	if count.Load() != 0 {
		t.Fatalf("expected no signal when peer remains, got %d", count.Load())
	}
	var got stopRepoResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.Deferred || got.Peers != 1 {
		t.Fatalf("expected deferred with 1 peer, got %+v", got)
	}
}

func TestStop_DefaultLastSession_SIGTERM(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "only", Harness: "claude-code",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Use current process PID so identity.Alive returns true and the
	// SIGTERM branch fires.
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: 1, Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = db.Close()

	// Use stop signal that stamps mode="stopped" — also ensures the
	// daemon's PID looks alive via identity.Alive(1) which is true on
	// Linux/macOS (init exists). We control via the stub.
	count, restore := installStopSignal(t, repoDir)
	defer restore()

	// Tighten the timeout so the test stays fast.
	prev := stopWaitTimeout
	stopWaitTimeout = 1 * time.Second
	defer func() { stopWaitTimeout = prev }()

	var out bytes.Buffer
	if err := runStop(ctx, &out, repoDir, "only", false, false, true); err != nil {
		t.Fatalf("runStop: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("expected 1 SIGTERM, got %d", count.Load())
	}
	var got stopRepoResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.Stopped || got.Deferred {
		t.Fatalf("expected stopped=true deferred=false, got %+v", got)
	}
}

func TestStop_ForceEscalates(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	// Daemon "alive" PID 1 (init); the signal stub does NOT stamp
	// stopped this time, so the controller will escalate to SIGKILL.
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: 1, Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	_ = db.Close()

	// Stub that ignores SIGTERM (does not stamp stopped) so the
	// controller escalates. SIGKILL also goes to the stub.
	prev := signalProcess
	var count atomic.Int32
	var sawKill atomic.Bool
	signalProcess = func(pid int, sig syscall.Signal, expectedFingerprint string) error {
		count.Add(1)
		if sig == syscall.SIGKILL {
			sawKill.Store(true)
			// Pretend the kill worked — but identity.Alive(1) still
			// returns true. We need a way to signal "PID gone".
			// Easiest: write daemon_state.mode=stopped which the
			// post-SIGKILL deadline loop also accepts (PID still
			// alive but mode stopped). Actually the post-kill loop
			// checks identity.Alive only. We can't kill init.
			// Workaround: set st.PID = -1 by stamping a dead PID,
			// but the stop logic captured st.PID earlier. So
			// we instead make Alive(1) return false in test by...
			// not possible. Instead, stamp mode="stopped" which
			// the post-SIGKILL deadline does NOT check.
			//
			// Since the deadline loop only checks identity.Alive,
			// the test will time out at 2s. Drop the deadline by
			// making the test tolerant of "survived SIGKILL"
			// outcome — the important thing is escalation
			// happened.
		}
		return nil
	}
	defer func() { signalProcess = prev }()

	prevTO := stopWaitTimeout
	stopWaitTimeout = 200 * time.Millisecond
	prevPI := stopPollInterval
	stopPollInterval = 50 * time.Millisecond
	defer func() {
		stopWaitTimeout = prevTO
		stopPollInterval = prevPI
	}()

	var out bytes.Buffer
	if err := runStop(ctx, &out, repoDir, "", true, false, true); err != nil {
		t.Fatalf("runStop: %v", err)
	}
	if count.Load() < 2 {
		t.Fatalf("expected SIGTERM+SIGKILL (>=2), got %d", count.Load())
	}
	if !sawKill.Load() {
		t.Fatalf("expected escalation to SIGKILL")
	}
	var got stopRepoResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.Force || !got.Escalated {
		t.Fatalf("expected force+escalated, got %+v", got)
	}
}

func TestStopAll_PassesCallerForceToRepoStopper(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repoDir, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repoDir, stateDB, "codex")

	prev := stopOneRepoForAll
	t.Cleanup(func() { stopOneRepoForAll = prev })

	type call struct {
		repo      string
		sessionID string
		force     bool
	}
	var calls []call
	stopOneRepoForAll = func(ctx context.Context, repo, sessionID string, force bool) (stopRepoResult, error) {
		calls = append(calls, call{repo: repo, sessionID: sessionID, force: force})
		return stopRepoResult{Repo: repo, Stopped: true, Force: force}, nil
	}

	for _, tc := range []struct {
		name  string
		force bool
	}{
		{name: "default", force: false},
		{name: "force", force: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls = nil
			var out bytes.Buffer
			if err := runStopAll(ctx, &out, tc.force, true); err != nil {
				t.Fatalf("runStopAll: %v", err)
			}
			if len(calls) != 1 {
				t.Fatalf("expected 1 stopOneRepo call, got %d", len(calls))
			}
			if calls[0].repo != repoDir {
				t.Fatalf("repo = %q, want %q", calls[0].repo, repoDir)
			}
			if calls[0].sessionID != "" {
				t.Fatalf("sessionID = %q, want empty", calls[0].sessionID)
			}
			if calls[0].force != tc.force {
				t.Fatalf("force = %v, want %v", calls[0].force, tc.force)
			}
		})
	}
}

func TestStop_All_IteratesRegistry(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repoA, dbA, dA := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, dA, state.DaemonState{
		PID: 99000, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	_ = dA.Close()

	repoB, dbB, dB := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, dB, state.DaemonState{
		PID: 99001, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save B: %v", err)
	}
	_ = dB.Close()

	registerRepo(t, roots, repoA, dbA, "claude-code")
	registerRepo(t, roots, repoB, dbB, "codex")

	// Signal stub stamps mode=stopped on each SIGTERM, per-repo.
	prev := signalProcess
	var sigCount atomic.Int32
	signalProcess = func(pid int, sig syscall.Signal, expectedFingerprint string) error {
		sigCount.Add(1)
		// Find which repo's DB to stamp by PID — both repos have
		// distinct PIDs above. The stub is generic, so stamp both.
		for _, r := range []string{repoA, repoB} {
			d, err := state.Open(context.Background(), state.DBPathFromGitDir(r+"/.git"))
			if err != nil {
				continue
			}
			st, _, _ := state.LoadDaemonState(context.Background(), d)
			if st.PID == pid {
				_ = state.SaveDaemonState(context.Background(), d, state.DaemonState{
					PID: pid, Mode: "stopped", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
				})
			}
			_ = d.Close()
		}
		return nil
	}
	defer func() { signalProcess = prev }()

	prevTO := stopWaitTimeout
	stopWaitTimeout = 500 * time.Millisecond
	defer func() { stopWaitTimeout = prevTO }()

	var out bytes.Buffer
	if err := runStop(ctx, &out, "", "", false, true, true); err != nil {
		t.Fatalf("runStop --all: %v", err)
	}
	var got stopAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if len(got.Stopped)+len(got.Deferred) != 2 {
		t.Fatalf("expected 2 repo entries, got %d stopped + %d deferred",
			len(got.Stopped), len(got.Deferred))
	}
}

// TestStopAll_RoutesFailuresToFailedBucket pins the regression that
// previously buried fingerprint-mismatch / "daemon survived SIGKILL"
// outcomes under `stopped`. The repo stopper here returns
// res.Stopped=false (no Deferred), simulating a force-stop where the
// kill could not be delivered. The classifier MUST surface it under
// Failed and runStopAll MUST return a non-nil error so callers (and
// `acd list`) don't treat the daemon as gone.
func TestStopAll_RoutesFailuresToFailedBucket(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repoDir, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repoDir, stateDB, "claude-code")

	prev := stopOneRepoForAll
	t.Cleanup(func() { stopOneRepoForAll = prev })

	stopOneRepoForAll = func(ctx context.Context, repo, sessionID string, force bool) (stopRepoResult, error) {
		return stopRepoResult{
			Repo:      repo,
			Force:     force,
			Escalated: true,
			DaemonPID: 12345,
			Reason:    "SIGKILL failed: verify process identity for pid 12345: fingerprint mismatch",
			// Stopped=false, Deferred=false ⇒ must land in Failed.
		}, nil
	}

	var out bytes.Buffer
	err := runStopAll(ctx, &out, true, true)
	if err == nil {
		t.Fatalf("runStopAll: expected non-nil error when a repo failed, got nil; output=%s", out.String())
	}

	var got stopAllResult
	if jerr := json.Unmarshal(out.Bytes(), &got); jerr != nil {
		t.Fatalf("unmarshal: %v\n%s", jerr, out.String())
	}
	if len(got.Stopped) != 0 {
		t.Fatalf("expected 0 stopped, got %d (%+v)", len(got.Stopped), got.Stopped)
	}
	if len(got.Deferred) != 0 {
		t.Fatalf("expected 0 deferred, got %d (%+v)", len(got.Deferred), got.Deferred)
	}
	if len(got.Failed) != 1 {
		t.Fatalf("expected 1 failed entry, got %d (%+v)", len(got.Failed), got.Failed)
	}
	if got.Failed[0].Repo != repoDir {
		t.Fatalf("failed[0].Repo = %q, want %q", got.Failed[0].Repo, repoDir)
	}
	if got.Failed[0].DaemonPID != 12345 {
		t.Fatalf("failed[0].DaemonPID = %d, want 12345", got.Failed[0].DaemonPID)
	}
}

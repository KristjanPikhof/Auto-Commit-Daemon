package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// daemonFixture wires up a temp git repo + open per-repo state DB so the
// run-loop tests don't have to repeat the boilerplate. Mirrors the
// captureFixture pattern but exposes the absolute git dir + database.
type daemonFixture struct {
	dir    string
	gitDir string
	db     *state.DB
}

func newDaemonFixture(t *testing.T) *daemonFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	if err := git.Init(ctx, dir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	for _, kv := range [][]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	// Initial commit so HEAD resolves.
	seed := filepath.Join(dir, ".gitignore")
	if err := os.WriteFile(seed, []byte("# acd test seed\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "add", ".gitignore"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "commit", "-q", "-m", "seed"); err != nil {
		t.Fatalf("git commit: %v", err)
	}

	gitDir, err := git.AbsoluteGitDir(ctx, dir)
	if err != nil {
		t.Fatalf("AbsoluteGitDir: %v", err)
	}
	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &daemonFixture{dir: dir, gitDir: gitDir, db: db}
}

// fastScheduler keeps the test loop responsive (~10ms ticks).
func fastScheduler() Scheduler {
	return Scheduler{
		Base:         10 * time.Millisecond,
		IdleCeiling:  20 * time.Millisecond,
		ErrorCeiling: 50 * time.Millisecond,
	}
}

// registerLiveClient inserts a daemon_clients row keyed to the test process
// itself so SweepClients sees alive>0 and the run loop does not
// self-terminate during the happy-path test.
func registerLiveClient(t *testing.T, db *state.DB) {
	t.Helper()
	pid := os.Getpid()
	fp, err := identity.CaptureSelf()
	if err != nil {
		t.Fatalf("CaptureSelf: %v", err)
	}
	c := state.Client{
		SessionID: "test-session",
		Harness:   "test",
		WatchPID:  sql.NullInt64{Int64: int64(pid), Valid: true},
		WatchFP:   sql.NullString{String: FingerprintToken(fp), Valid: true},
	}
	if err := state.RegisterClient(context.Background(), db, c); err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
}

// waitForCommit polls HEAD until it differs from start (or timeout). Returns
// the new HEAD OID on success.
func waitForCommit(t *testing.T, dir, start string, deadline time.Duration) string {
	t.Helper()
	until := time.Now().Add(deadline)
	for time.Now().Before(until) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		head, err := git.RevParse(ctx, dir, "HEAD")
		cancel()
		if err == nil && head != start {
			return head
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("HEAD did not advance from %s within %v", start, deadline)
	return ""
}

// daemonMode reads daemon_state.mode for assertion.
func daemonMode(t *testing.T, db *state.DB) string {
	t.Helper()
	st, _, err := state.LoadDaemonState(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadDaemonState: %v", err)
	}
	return st.Mode
}

// TestRun_LifecycleHappyPath: a full capture+replay cycle drives a commit
// onto HEAD when the test triggers a wake; ctx cancel exits with mode=stopped.
func TestRun_LifecycleHappyPath(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	startHead, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = Run(ctx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second, // never trigger self-terminate
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()

	// Write a file and signal a wake.
	if err := os.WriteFile(filepath.Join(f.dir, "hello.txt"), []byte("hi\n"), 0o644); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	wakeCh <- struct{}{}

	newHead := waitForCommit(t, f.dir, startHead, 3*time.Second)
	if newHead == startHead {
		t.Fatalf("HEAD did not advance")
	}

	// Inspect the commit message — should be Phase 1 deterministic.
	out, err := git.Run(context.Background(), git.RunOpts{Dir: f.dir},
		"log", "-1", "--pretty=%s", newHead)
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	if !strings.Contains(string(out), "hello.txt") {
		t.Fatalf("commit subject does not mention hello.txt: %q", out)
	}

	// Cancel ctx -> graceful shutdown.
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run returned %v", runErr)
	}
	if mode := daemonMode(t, f.db); mode != "stopped" {
		t.Fatalf("daemon_state.mode=%q want stopped", mode)
	}
}

// TestRun_WakeBurstCoalesced: many rapid wakes don't crash and only produce
// one capture+replay cycle (idempotent — the second pass sees no changes).
func TestRun_WakeBurstCoalesced(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	startHead, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	wakeCh := make(chan struct{}, 1) // cap 1 mimics the real signal channel
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()

	// Write a single file and burst-signal 100 wakes.
	if err := os.WriteFile(filepath.Join(f.dir, "burst.txt"), []byte("once\n"), 0o644); err != nil {
		t.Fatalf("write burst: %v", err)
	}
	for i := 0; i < 100; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
			// drop — same coalescing the signal handler does.
		}
	}

	newHead := waitForCommit(t, f.dir, startHead, 3*time.Second)
	if newHead == startHead {
		t.Fatalf("HEAD did not advance")
	}

	// Wait for the loop to settle (no further commits arrive).
	time.Sleep(200 * time.Millisecond)
	settled, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if settled != newHead {
		t.Fatalf("HEAD kept advancing past coalesce: %s -> %s", newHead, settled)
	}

	cancel()
	wg.Wait()
}

// TestRun_GracefulShutdownSignal: triggering the shutdown channel makes the
// daemon return cleanly with mode=stopped within a short window.
func TestRun_GracefulShutdownSignal(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()

	// Give Run a moment to install state.
	time.Sleep(50 * time.Millisecond)
	shutdownCh <- struct{}{}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not exit on shutdown signal")
	}

	if mode := daemonMode(t, f.db); mode != "stopped" {
		t.Fatalf("daemon_state.mode=%q want stopped", mode)
	}
}

// TestRun_SelfTerminateNoClients: with no daemon_clients rows past the boot
// grace, the daemon self-terminates after 2 empty sweeps and writes mode=stopped.
func TestRun_SelfTerminateNoClients(t *testing.T) {
	f := newDaemonFixture(t)
	// NOTE: deliberately do NOT registerLiveClient.

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			RepoPath:            f.dir,
			GitDir:              f.gitDir,
			DB:                  f.db,
			Scheduler:           fastScheduler(),
			BootGrace:           20 * time.Millisecond,
			ClientSweepInterval: 10 * time.Millisecond,
			EmptySweepThreshold: 2,
			WakeCh:              wakeCh,
			ShutdownCh:          shutdownCh,
			SkipSignals:         true,
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("daemon did not self-terminate")
	}
	if mode := daemonMode(t, f.db); mode != "stopped" {
		t.Fatalf("daemon_state.mode=%q want stopped", mode)
	}
}

// TestRun_FlockContention: a second Run call against the same gitDir
// returns ErrDaemonLockHeld so the wrapping CLI can exit with EX_TEMPFAIL.
func TestRun_FlockContention(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First daemon: acquire and hold the lock.
	first := make(chan error, 1)
	go func() {
		first <- Run(ctx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()

	// Wait until the first daemon has clearly acquired the lock by polling
	// the daemon_state row (the run loop stamps mode=running before
	// entering the loop body).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mode := daemonMode(t, f.db); mode == "running" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Second daemon: must hit ErrDaemonLockHeld immediately.
	secondDB, err := state.Open(context.Background(), state.DBPathFromGitDir(f.gitDir))
	if err != nil {
		t.Fatalf("secondary state.Open: %v", err)
	}
	defer secondDB.Close()
	err = Run(ctx, Options{
		RepoPath:    f.dir,
		GitDir:      f.gitDir,
		DB:          secondDB,
		Scheduler:   fastScheduler(),
		BootGrace:   30 * time.Second,
		WakeCh:      make(chan struct{}, 1),
		ShutdownCh:  make(chan struct{}, 1),
		SkipSignals: true,
	})
	if !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("second Run returned %v want ErrDaemonLockHeld", err)
	}

	cancel()
	select {
	case <-first:
	case <-time.After(2 * time.Second):
		t.Fatalf("first daemon did not exit")
	}
}

// TestRun_RealSIGUSR1: covers the real-OS signal path. Sends SIGUSR1 to the
// current process and asserts the loop wakes and produces a commit. Skipped
// on Windows (which we don't target anyway).
func TestRun_RealSIGUSR1(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGUSR1 unavailable on windows")
	}
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	startHead, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath:  f.dir,
			GitDir:    f.gitDir,
			DB:        f.db,
			Scheduler: fastScheduler(),
			BootGrace: 30 * time.Second,
			// SkipSignals=false so we exercise the real handler.
		})
	}()

	// Wait until the daemon stamps mode=running to be sure signals are
	// installed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mode := daemonMode(t, f.db); mode == "running" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := os.WriteFile(filepath.Join(f.dir, "sig.txt"), []byte("real\n"), 0o644); err != nil {
		t.Fatalf("write sig: %v", err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("send SIGUSR1: %v", err)
	}

	newHead := waitForCommit(t, f.dir, startHead, 3*time.Second)
	if newHead == startHead {
		t.Fatalf("HEAD did not advance after SIGUSR1")
	}

	cancel()
	wg.Wait()
}

// TestPruneCaptureEvents_DropsOldPublished: published rows older than the
// retention window are pruned; pending rows survive.
func TestPruneCaptureEvents_DropsOldPublished(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	// Insert one old published row and one fresh pending row.
	old, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "create", Path: "old.txt",
		Fidelity: "full", CapturedTS: 1,
		State: "published",
	}, []state.CaptureOp{{
		Op: "create", Path: "old.txt", Fidelity: "full",
		AfterMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:  sql.NullString{String: "abcd", Valid: true},
	}})
	if err != nil {
		t.Fatalf("insert old: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "create", Path: "fresh.txt",
		Fidelity: "full",
		// captured_ts default = now()
		State: "pending",
	}, []state.CaptureOp{{
		Op: "create", Path: "fresh.txt", Fidelity: "full",
		AfterMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:  sql.NullString{String: "ef01", Valid: true},
	}}); err != nil {
		t.Fatalf("insert fresh: %v", err)
	}

	n, err := PruneCaptureEvents(ctx, f.db, time.Now(), 1*time.Second)
	if err != nil {
		t.Fatalf("PruneCaptureEvents: %v", err)
	}
	if n != 1 {
		t.Fatalf("pruned=%d want 1", n)
	}

	// The pending row should still be present; the published one gone.
	var seqs []int64
	rows, err := f.db.SQL().QueryContext(ctx,
		`SELECT seq FROM capture_events ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var s int64
		_ = rows.Scan(&s)
		seqs = append(seqs, s)
	}
	if len(seqs) != 1 {
		t.Fatalf("remaining seqs=%v want 1", seqs)
	}
	if seqs[0] == old {
		t.Fatalf("old published row not pruned (seq=%d)", old)
	}
}

// TestRun_RollupHookAdvancesLastDay: the daemon loop's daily rollup hook
// (§8.10) fires once per RollupInterval, attributes a synthetic event to a
// completed UTC day, and advances rollup.last_day. This test confirms the
// hook is wired to Run with no central stats handle (per-repo only, which
// matches existing fixture defaults).
func TestRun_RollupHookAdvancesLastDay(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	// Seed a synthetic event on 2026-04-01.
	yesterday := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	if _, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: "deadbeef",
		Operation: "create", Path: "rollup-seed.txt", Fidelity: "full",
		CapturedTS:  float64(yesterday.Unix()),
		PublishedTS: sql.NullFloat64{Float64: float64(yesterday.Unix()), Valid: true},
		State:       "published",
		CommitOID:   sql.NullString{String: "c1", Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "rollup-seed.txt", Fidelity: "full",
		AfterMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:  sql.NullString{String: "abcd", Valid: true},
	}}); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	// Pin "now" to 2026-04-02 12:00 UTC so yesterday is fully complete.
	fakeNow := func() time.Time { return time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC) }

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(runCtx, Options{
			RepoPath:       f.dir,
			GitDir:         f.gitDir,
			DB:             f.db,
			Scheduler:      fastScheduler(),
			BootGrace:      30 * time.Second,
			RollupInterval: 1 * time.Millisecond, // fire on every iteration
			Now:            fakeNow,
			WakeCh:         wakeCh,
			ShutdownCh:     shutdownCh,
			SkipSignals:    true,
		})
	}()

	// Poll for rollup.last_day to land.
	deadline := time.Now().Add(2 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		v, present, err := state.MetaGet(ctx, f.db, "rollup.last_day")
		if err == nil && present && v != "" {
			got = v
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got != "2026-04-01" {
		t.Fatalf("rollup.last_day=%q want 2026-04-01", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not exit on cancel")
	}
}

// TestBranchGenerationToken_RevAndMissing: token shape covers both ref-present
// and orphan-HEAD cases.
func TestBranchGenerationToken_RevAndMissing(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	tok, err := BranchGenerationToken(ctx, f.dir)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	if !strings.HasPrefix(tok, "rev:") {
		t.Fatalf("token=%q want rev:* prefix", tok)
	}
	if !SameGeneration(tok, tok) {
		t.Fatalf("SameGeneration(t,t) false")
	}
	if SameGeneration(tok, BranchTokenMissing) {
		t.Fatalf("SameGeneration(rev,missing) true")
	}

	// Build a fresh empty repo to cover the "missing" branch.
	empty := t.TempDir()
	if err := git.Init(ctx, empty); err != nil {
		t.Fatalf("init empty: %v", err)
	}
	tok2, err := BranchGenerationToken(ctx, empty)
	if err != nil {
		t.Fatalf("token empty: %v", err)
	}
	if tok2 != BranchTokenMissing {
		t.Fatalf("empty token=%q want %q", tok2, BranchTokenMissing)
	}
}

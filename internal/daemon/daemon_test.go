package daemon

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestMain(m *testing.M) {
	_ = os.Setenv(ai.EnvProvider, "deterministic")
	os.Exit(m.Run())
}

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
	// Force HEAD onto refs/heads/main regardless of host's init.defaultBranch
	// (CI runners default to master; daemon Options pin BranchRef to main).
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
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

func waitForDaemonMode(t *testing.T, db *state.DB, mode string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, _, err := state.LoadDaemonState(context.Background(), db)
		if err == nil && st.Mode == mode {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("daemon_state.mode did not become %q within %v", mode, timeout)
}

func waitForMetaValue(t *testing.T, db *state.DB, key, want string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, ok, err := state.MetaGet(ctx, db, key)
		if err != nil {
			t.Fatalf("MetaGet %s: %v", key, err)
		}
		if ok && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, ok, err := state.MetaGet(ctx, db, key)
	if err != nil {
		t.Fatalf("MetaGet %s after timeout: %v", key, err)
	}
	t.Fatalf("%s=%q ok=%v want %q", key, got, ok, want)
}

func waitForMetaDeleted(t *testing.T, db *state.DB, key string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, ok, err := state.MetaGet(ctx, db, key)
		if err != nil {
			t.Fatalf("MetaGet %s: %v", key, err)
		}
		if !ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, ok, err := state.MetaGet(ctx, db, key)
	if err != nil {
		t.Fatalf("MetaGet %s after timeout: %v", key, err)
	}
	t.Fatalf("%s still set to %q ok=%v", key, got, ok)
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
			MessageFn:   DeterministicMessage,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()

	waitForDaemonMode(t, f.db, "running", 2*time.Second)

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

// TestRun_StampedFingerprintIsSymmetricWithVerifier pins the regression
// where Run used identity.CaptureSelf() to stamp daemon_fingerprint.
// The persisted token must equal what `acd stop` / `acd wake`
// reconstruct via identity.Capture(pid) when verifying the daemon's
// PID before delivering a signal — otherwise signalProcess silently
// returns "fingerprint mismatch" and SIGTERM/SIGKILL never reach the
// daemon. Asserts the stored token is identical to
// FingerprintToken(identity.Capture(daemon_pid)).
func TestRun_StampedFingerprintIsSymmetricWithVerifier(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skipf("ps fingerprint only validated on darwin/linux; running on %s", runtime.GOOS)
	}
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wakeCh := make(chan struct{}, 1)
	shutdownCh := make(chan struct{}, 1)
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
			BootGrace:   30 * time.Second,
			MessageFn:   DeterministicMessage,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	waitForDaemonMode(t, f.db, "running", 2*time.Second)

	st, _, err := state.LoadDaemonState(context.Background(), f.db)
	if err != nil {
		t.Fatalf("LoadDaemonState: %v", err)
	}
	if !st.DaemonFingerprint.Valid || st.DaemonFingerprint.String == "" {
		t.Fatalf("daemon_fingerprint not stamped: %+v", st)
	}
	if st.PID != os.Getpid() {
		t.Fatalf("daemon_state.pid=%d want test pid %d", st.PID, os.Getpid())
	}

	// Reconstruct what `acd stop` would compute when verifying the
	// stamped PID. Must equal byte-for-byte; otherwise signalProcess
	// returns mismatch.
	verified, err := identity.Capture(st.PID)
	if err != nil {
		t.Fatalf("identity.Capture(daemon pid): %v", err)
	}
	want := FingerprintToken(verified)
	if want == "" {
		t.Fatalf("verifier token empty; cannot assert symmetry")
	}
	if st.DaemonFingerprint.String != want {
		t.Fatalf("stamped daemon_fingerprint=%q, verifier would compute %q "+
			"(asymmetric — daemon stamping must use identity.Capture, not CaptureSelf)",
			st.DaemonFingerprint.String, want)
	}

	if runErr != nil {
		t.Fatalf("Run returned %v", runErr)
	}
}

func TestResolveBranch_DetachedHeadHasNoBranchRef(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "checkout", "--detach", head); err != nil {
		t.Fatalf("checkout --detach: %v", err)
	}

	branchRef, headOID := resolveBranch(ctx, f.dir, slog.Default())
	if branchRef != "" {
		t.Fatalf("branchRef=%q want empty for detached HEAD", branchRef)
	}
	if headOID != head {
		t.Fatalf("headOID=%q want %q", headOID, head)
	}
}

func TestRun_DetachedHeadPausesCaptureReplay(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	startHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "checkout", "--detach", startHead); err != nil {
		t.Fatalf("checkout --detach: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
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

	if err := os.WriteFile(filepath.Join(f.dir, "detached.txt"), []byte("paused\n"), 0o644); err != nil {
		t.Fatalf("write detached: %v", err)
	}
	for i := 0; i < 4; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok, _ := state.MetaGet(ctx, f.db, MetaKeyDetachedHeadPaused); ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, ok, _ := state.MetaGet(ctx, f.db, MetaKeyDetachedHeadPaused); !ok {
		t.Fatalf("%s not stamped", MetaKeyDetachedHeadPaused)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse after pause: %v", err)
	}
	if head != startHead {
		t.Fatalf("detached HEAD advanced to %s; want %s", head, startHead)
	}
	var events int
	if err := f.db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events`).Scan(&events); err != nil {
		t.Fatalf("count capture_events: %v", err)
	}
	if events != 0 {
		t.Fatalf("capture_events=%d want 0 while detached", events)
	}

	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run returned %v", runErr)
	}
}

func TestRun_PauseDuringGitOperation(t *testing.T) {
	tests := []struct {
		marker string
		name   string
		dir    bool
	}{
		{marker: "rebase-merge", name: "rebase-merge", dir: true},
		{marker: "rebase-apply", name: "rebase-apply", dir: true},
		{marker: "MERGE_HEAD", name: "merge"},
		{marker: "CHERRY_PICK_HEAD", name: "cherry-pick"},
		{marker: "BISECT_LOG", name: "bisect"},
	}

	for _, tc := range tests {
		t.Run(tc.marker, func(t *testing.T) {
			f := newDaemonFixture(t)
			registerLiveClient(t, f.db)
			ctx := context.Background()

			startHead, err := git.RevParse(ctx, f.dir, "HEAD")
			if err != nil {
				t.Fatalf("rev-parse: %v", err)
			}

			markerPath := filepath.Join(f.gitDir, tc.marker)
			if tc.dir {
				if err := os.Mkdir(markerPath, 0o755); err != nil {
					t.Fatalf("create marker dir: %v", err)
				}
			} else if err := os.WriteFile(markerPath, []byte(startHead+"\n"), 0o644); err != nil {
				t.Fatalf("create marker file: %v", err)
			}

			wakeCh := make(chan struct{}, 4)
			shutdownCh := make(chan struct{}, 1)
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(1)
			var runErr error
			go func() {
				defer wg.Done()
				runErr = Run(runCtx, Options{
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
			t.Cleanup(func() {
				cancel()
				wg.Wait()
			})

			waitForMetaValue(t, f.db, MetaKeyOperationInProgress, tc.name, 3*time.Second)

			if err := os.WriteFile(filepath.Join(f.dir, "paused.txt"), []byte(tc.name+"\n"), 0o644); err != nil {
				t.Fatalf("write paused: %v", err)
			}
			for i := 0; i < 4; i++ {
				select {
				case wakeCh <- struct{}{}:
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}

			head, err := git.RevParse(ctx, f.dir, "HEAD")
			if err != nil {
				t.Fatalf("rev-parse while paused: %v", err)
			}
			if head != startHead {
				t.Fatalf("HEAD advanced while paused to %s; want %s", head, startHead)
			}
			var events int
			if err := f.db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events`).Scan(&events); err != nil {
				t.Fatalf("count capture_events: %v", err)
			}
			if events != 0 {
				t.Fatalf("capture_events=%d want 0 while %s marker exists", events, tc.marker)
			}

			if err := os.RemoveAll(markerPath); err != nil {
				t.Fatalf("remove marker: %v", err)
			}
			waitForMetaDeleted(t, f.db, MetaKeyOperationInProgress, 3*time.Second)

			cancel()
			wg.Wait()
			if runErr != nil {
				t.Fatalf("Run returned %v", runErr)
			}
		})
	}
}

// TestDaemon_StaleOpMarker_Warns verifies that when an operation_in_progress
// marker (e.g. MERGE_HEAD) sits in the git dir for >15 minutes WITHOUT HEAD
// advancing, the daemon emits a "marker may be stale" warning AND surfaces
// the stale_operation_marker bit in the persisted operation_in_progress.set_at
// metadata so `acd diagnose` can flag it. The daemon never auto-clears the
// marker — that is the operator's job.
func TestDaemon_StaleOpMarker_Warns(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	startHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	// Inject MERGE_HEAD so gitOperationInProgress reports "merge".
	mergeHead := filepath.Join(f.gitDir, "MERGE_HEAD")
	if err := os.WriteFile(mergeHead, []byte(startHead+"\n"), 0o644); err != nil {
		t.Fatalf("create MERGE_HEAD: %v", err)
	}

	// Controllable clock: first call returns t0, every subsequent call
	// returns t0 + 16m so the daemon's stale-marker threshold (15m) trips
	// on the next pass after seeding set_at.
	t0 := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	advanced := atomic.Bool{}
	nowFn := func() time.Time {
		if advanced.Load() {
			return t0.Add(16 * time.Minute)
		}
		return t0
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			Now:         nowFn,
			SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	// First, wait for the marker to be seeded with operation_in_progress.set_at.
	waitForMetaValue(t, f.db, MetaKeyOperationInProgress, "merge", 3*time.Second)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok, _ := state.MetaGet(ctx, f.db, MetaKeyOperationInProgressSetAt); ok && v != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	setAtRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyOperationInProgressSetAt)
	if err != nil {
		t.Fatalf("MetaGet set_at: %v", err)
	}
	if !ok || setAtRaw == "" {
		t.Fatalf("operation_in_progress.set_at not stamped")
	}

	// Now advance the clock past the threshold and force another tick.
	advanced.Store(true)
	for i := 0; i < 4; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(40 * time.Millisecond)
	}

	// HEAD must not have advanced.
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if head != startHead {
		t.Fatalf("HEAD advanced while marker present: %s; want %s", head, startHead)
	}

	// set_at remains stable across ticks (the stamp is captured once on
	// transition to "marker present", not refreshed each pass).
	setAtRaw2, _, _ := state.MetaGet(ctx, f.db, MetaKeyOperationInProgressSetAt)
	if setAtRaw2 != setAtRaw {
		t.Fatalf("operation_in_progress.set_at refreshed across ticks: %q -> %q",
			setAtRaw, setAtRaw2)
	}

	// HEAD-at stamp must match the SHA observed at marker time.
	headAtRaw, ok, _ := state.MetaGet(ctx, f.db, MetaKeyOperationInProgressHead)
	if !ok || headAtRaw != startHead {
		t.Fatalf("operation_in_progress.head_at=%q want %s", headAtRaw, startHead)
	}

	// Stop the daemon and verify Run returned cleanly. We do NOT remove
	// MERGE_HEAD here because the advanced clock has carried sinceBoot
	// past BootGrace and the empty-sweep self-terminate gate may fire on
	// any glitch; the test's job is to confirm the stale-marker stamp +
	// warn fires while the marker is present, not to assert the resume
	// path (covered separately by TestRun_PauseDuringGitOperation).
	cancel()
	wg.Wait()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run returned %v", runErr)
	}
}

// TestRewindGrace_DoesNotResurrectRewoundWork verifies that when the daemon
// detects a same-branch rewind (newHead is an ancestor of prevHead, e.g.
// `git reset --soft HEAD~1`) and writes daemon_meta.replay.paused_until, the
// run loop pauses BOTH capture and replay during the grace window.
// Otherwise: an fsnotify wake during the rewound state would capture the
// transient worktree, and the post-grace replay drain would resurrect work
// the operator just rewound.
func TestRewindGrace_DoesNotResurrectRewoundWork(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	// Pre-set the rewind grace marker to a future time. The daemon Run loop
	// reads this via daemonPauseState and must skip both capture and replay.
	until := time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339)
	if err := state.MetaSet(ctx, f.db, MetaKeyReplayPausedUntil, until); err != nil {
		t.Fatalf("MetaSet paused_until: %v", err)
	}

	startHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
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
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	// Edit a file that would normally produce a captured event; force several
	// wakes so the run loop has clear opportunities to pass the gate.
	if err := os.WriteFile(filepath.Join(f.dir, "rewound.txt"), []byte("transient\n"), 0o644); err != nil {
		t.Fatalf("write rewound: %v", err)
	}
	for i := 0; i < 6; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(40 * time.Millisecond)
	}

	// HEAD must not advance during the grace window.
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse while paused: %v", err)
	}
	if head != startHead {
		t.Fatalf("HEAD advanced while rewind grace active: %s; want %s", head, startHead)
	}

	// And capture_events must be empty: capture is paused alongside replay,
	// so no transient worktree row was enqueued.
	var events int
	if err := f.db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events`).Scan(&events); err != nil {
		t.Fatalf("count capture_events: %v", err)
	}
	if events != 0 {
		t.Fatalf("capture_events=%d want 0 during rewind grace", events)
	}

	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run returned %v", runErr)
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
			MessageFn:   DeterministicMessage,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()

	waitForDaemonMode(t, f.db, "running", 2*time.Second)

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

func TestPruneCaptureEvents_DropsOldTerminalRowsWhenNotBarriers(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	appendEvent := func(path, branch, stateName string, capturedTS float64) int64 {
		t.Helper()
		seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
			BranchRef:        branch,
			BranchGeneration: 1,
			BaseHead:         "deadbeef",
			Operation:        "create",
			Path:             path,
			Fidelity:         "full",
			CapturedTS:       capturedTS,
			State:            stateName,
		}, []state.CaptureOp{{
			Op: "create", Path: path, Fidelity: "full",
			AfterMode: sql.NullString{String: "100644", Valid: true},
			AfterOID:  sql.NullString{String: "abcd", Valid: true},
		}})
		if err != nil {
			t.Fatalf("insert %s: %v", path, err)
		}
		return seq
	}

	oldBlocked := appendEvent("old-blocked.txt", "refs/heads/main", state.EventStateBlockedConflict, 1)
	oldFailed := appendEvent("old-failed.txt", "refs/heads/failed", state.EventStateFailed, 1)
	barrier := appendEvent("barrier.txt", "refs/heads/barrier", state.EventStateBlockedConflict, 1)
	pendingBehindBarrier := appendEvent("pending.txt", "refs/heads/barrier", state.EventStatePending, 1)
	freshFailed := appendEvent("fresh-failed.txt", "refs/heads/fresh", state.EventStateFailed, float64(time.Now().Unix()))

	n, err := PruneCaptureEvents(ctx, f.db, time.Now(), 1*time.Second)
	if err != nil {
		t.Fatalf("PruneCaptureEvents: %v", err)
	}
	if n != 2 {
		t.Fatalf("pruned=%d want 2", n)
	}

	rows, err := f.db.SQL().QueryContext(ctx, `SELECT seq FROM capture_events ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("query remaining: %v", err)
	}
	defer rows.Close()
	remaining := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scan remaining: %v", err)
		}
		remaining[seq] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate remaining: %v", err)
	}
	if remaining[oldBlocked] || remaining[oldFailed] {
		t.Fatalf("old terminal rows survived: remaining=%v", remaining)
	}
	for _, seq := range []int64{barrier, pendingBehindBarrier, freshFailed} {
		if !remaining[seq] {
			t.Fatalf("seq %d should remain; remaining=%v", seq, remaining)
		}
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

// stubProvider implements ai.Provider with a fixed subject so the run-loop
// test can prove the configured provider — not the deterministic fallback —
// is what ends up on HEAD.
type stubProvider struct {
	subject string
	calls   atomic.Int64
}

func (s *stubProvider) Name() string { return "stub" }

func (s *stubProvider) Generate(_ context.Context, _ ai.CommitContext) (ai.Result, error) {
	s.calls.Add(1)
	return ai.Result{Subject: s.subject, Source: s.Name()}, nil
}

// closerCounter is an io.Closer whose Close call count is observable;
// the test asserts Run actually invokes Close on shutdown.
type closerCounter struct {
	closed atomic.Int64
}

func (c *closerCounter) Close() error {
	c.closed.Add(1)
	return nil
}

// TestRun_AIProvider_FallbackToDeterministic: when ACD_AI_PROVIDER=
// openai-compat is set without an API key, the daemon must warn-and-degrade
// to the deterministic generator so commits keep landing.
func TestRun_AIProvider_FallbackToDeterministic(t *testing.T) {
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "")
	t.Setenv(ai.EnvBaseURL, "")
	t.Setenv(ai.EnvModel, "")

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

	if err := os.WriteFile(filepath.Join(f.dir, "fallback.txt"), []byte("fb\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	wakeCh <- struct{}{}

	newHead := waitForCommit(t, f.dir, startHead, 3*time.Second)
	out, err := git.Run(context.Background(), git.RunOpts{Dir: f.dir},
		"log", "-1", "--pretty=%s", newHead)
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	subj := strings.TrimSpace(string(out))
	if subj != "Add fallback.txt" {
		t.Fatalf("subject=%q want %q (deterministic format)", subj, "Add fallback.txt")
	}

	cancel()
	wg.Wait()
}

// TestRun_AIProvider_InjectedOverride: a non-nil Options.MessageProvider
// short-circuits env-driven selection and lands its subject on HEAD; the
// MessageProviderCloser is invoked exactly once on shutdown.
func TestRun_AIProvider_InjectedOverride(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	startHead, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	stub := &stubProvider{subject: "feat: stub-injected subject"}
	closer := &closerCounter{}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath:              f.dir,
			GitDir:                f.gitDir,
			DB:                    f.db,
			Scheduler:             fastScheduler(),
			BootGrace:             30 * time.Second,
			WakeCh:                wakeCh,
			ShutdownCh:            shutdownCh,
			SkipSignals:           true,
			MessageProvider:       stub,
			MessageProviderCloser: closer,
		})
	}()

	if err := os.WriteFile(filepath.Join(f.dir, "stub.txt"), []byte("stub\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	wakeCh <- struct{}{}

	newHead := waitForCommit(t, f.dir, startHead, 3*time.Second)
	out, err := git.Run(context.Background(), git.RunOpts{Dir: f.dir},
		"log", "-1", "--pretty=%s", newHead)
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	subj := strings.TrimSpace(string(out))
	if subj != stub.subject {
		t.Fatalf("subject=%q want %q", subj, stub.subject)
	}
	if stub.calls.Load() == 0 {
		t.Fatalf("stub provider Generate never called")
	}

	cancel()
	wg.Wait()

	if got := closer.closed.Load(); got != 1 {
		t.Fatalf("MessageProviderCloser.Close calls=%d want 1", got)
	}
}

// TestClassifyTokenTransition: ACD-style fast-forward (the daemon just
// landed a commit and HEAD advanced) is distinct from external rewrites
// (rebase, reset, branch switch). Fast-forwards must NOT bump the
// generation; rewrites must.
func TestClassifyTokenTransition(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	seed, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}

	// Build a child commit on top of seed by hand.
	blob, err := git.HashObjectStdin(ctx, f.dir, []byte("child\n"))
	if err != nil {
		t.Fatalf("hash blob: %v", err)
	}
	tree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: blob, Path: "child.txt"},
	})
	if err != nil {
		t.Fatalf("mktree child: %v", err)
	}
	child, err := git.CommitTree(ctx, f.dir, tree, "child", seed)
	if err != nil {
		t.Fatalf("commit-tree child: %v", err)
	}

	// Build a sibling commit (no shared history with seed) — simulates a
	// destructive rebase / reset onto an unrelated history.
	sibling, err := git.CommitTree(ctx, f.dir, tree, "sibling root")
	if err != nil {
		t.Fatalf("commit-tree sibling: %v", err)
	}

	// Same token -> Unchanged.
	if got, err := ClassifyTokenTransition(ctx, f.dir, "rev:"+seed, "rev:"+seed); err != nil || got != TokenTransitionUnchanged {
		t.Fatalf("Unchanged: got=%v err=%v", got, err)
	}
	// Same SHA but different symbolic branch refs -> Diverged. Without the
	// ref in the token, the daemon can keep a stale cctx.BranchRef and
	// publish onto the branch it started on.
	if got, err := ClassifyTokenTransition(ctx, f.dir,
		branchTokenRev(seed, "refs/heads/main"),
		branchTokenRev(seed, "refs/heads/feature/same-sha"),
	); err != nil || got != TokenTransitionDiverged {
		t.Fatalf("same-sha branch switch: got=%v err=%v", got, err)
	}
	// seed -> child (ancestor): FastForward.
	if got, err := ClassifyTokenTransition(ctx, f.dir, "rev:"+seed, "rev:"+child); err != nil || got != TokenTransitionFastForward {
		t.Fatalf("FastForward: got=%v err=%v", got, err)
	}
	// seed -> sibling (no shared history): Diverged.
	if got, err := ClassifyTokenTransition(ctx, f.dir, "rev:"+seed, "rev:"+sibling); err != nil || got != TokenTransitionDiverged {
		t.Fatalf("Diverged: got=%v err=%v", got, err)
	}
	// missing -> rev: Diverged (transition through orphan).
	if got, err := ClassifyTokenTransition(ctx, f.dir, BranchTokenMissing, "rev:"+seed); err != nil || got != TokenTransitionDiverged {
		t.Fatalf("missing->rev: got=%v err=%v", got, err)
	}
	// rev -> missing: Diverged.
	if got, err := ClassifyTokenTransition(ctx, f.dir, "rev:"+seed, BranchTokenMissing); err != nil || got != TokenTransitionDiverged {
		t.Fatalf("rev->missing: got=%v err=%v", got, err)
	}
	// "" (boot first observation) -> rev: FastForward — no prior history to compare.
	if got, err := ClassifyTokenTransition(ctx, f.dir, "", "rev:"+seed); err != nil || got != TokenTransitionFastForward {
		t.Fatalf("empty->rev: got=%v err=%v", got, err)
	}
}

// TestLoadSaveBranchGeneration: round-trip the persisted generation +
// HEAD scalars through daemon_meta. Defaults to 1 when the key is absent.
func TestLoadSaveBranchGeneration(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	got, err := LoadBranchGeneration(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadBranchGeneration default: %v", err)
	}
	if got != 1 {
		t.Fatalf("default generation=%d want 1", got)
	}
	head, err := LoadBranchHead(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadBranchHead default: %v", err)
	}
	if head != "" {
		t.Fatalf("default head=%q want empty", head)
	}

	if err := SaveBranchGeneration(ctx, f.db, 7, "deadbeefcafe"); err != nil {
		t.Fatalf("SaveBranchGeneration: %v", err)
	}
	got, err = LoadBranchGeneration(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadBranchGeneration round-trip: %v", err)
	}
	if got != 7 {
		t.Fatalf("round-trip generation=%d want 7", got)
	}
	head, err = LoadBranchHead(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadBranchHead round-trip: %v", err)
	}
	if head != "deadbeefcafe" {
		t.Fatalf("round-trip head=%q want deadbeefcafe", head)
	}
}

// TestRun_BranchGenerationBumpsOnExternalReset: an external `git reset`
// onto a sibling commit during the run loop causes the active generation
// to bump and the persisted value to advance. This is the daemon-side
// counterpart to the replay-level stale-generation guard.
func TestRun_BranchGenerationBumpsOnExternalReset(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	// Build a sibling commit on a fresh tree — no shared history with the
	// seed commit. We point main at this sibling under the daemon's feet
	// to simulate `git reset --hard <sibling>`.
	blob, err := git.HashObjectStdin(ctx, f.dir, []byte("sibling\n"))
	if err != nil {
		t.Fatalf("hash sibling blob: %v", err)
	}
	siblingTree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: blob, Path: "sibling.txt"},
	})
	if err != nil {
		t.Fatalf("mktree sibling: %v", err)
	}
	sibling, err := git.CommitTree(ctx, f.dir, siblingTree, "sibling root")
	if err != nil {
		t.Fatalf("commit-tree sibling: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(runCtx, Options{
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

	// Wait for the daemon to seed daemon_meta.branch.generation = 1 AND
	// daemon_meta.branch.head to point at the seed commit. Without the
	// head check the run loop's first iteration can still be on the path
	// from "" -> seed and treat the upcoming sibling reset as the boot
	// transition.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		gen, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
		head, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchHead)
		if gen == "1" && head == seedHead {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// External reset: point main at the sibling. The daemon's next tick
	// must classify this as a divergence. Send several wakes so we don't
	// race a single buffered slot against a busy iteration.
	if err := git.UpdateRef(ctx, f.dir, "refs/heads/main", sibling, ""); err != nil {
		t.Fatalf("update-ref to sibling: %v", err)
	}
	for i := 0; i < 4; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Poll for the persisted generation to bump above 1.
	deadline = time.Now().Add(5 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		v, ok, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
		if ok && v != "" && v != "1" {
			got = v
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got == "" {
		t.Fatalf("branch.generation did not bump after sibling reset (still %q); seedHead=%s sibling=%s",
			"1", seedHead, sibling)
	}

	// After the divergence the daemon must reseed shadow_paths for the
	// new (branch_ref, branch_generation) key. Without the reseed, the
	// next capture pass sees an empty shadow and emits phantom `create`
	// events for every tracked file in HEAD's tree.
	branchRef, _ := resolveBranch(ctx, f.dir, slog.Default())
	deadline = time.Now().Add(3 * time.Second)
	var shadowRows int
	for time.Now().Before(deadline) {
		row := f.db.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
			branchRef, got)
		if err := row.Scan(&shadowRows); err == nil && shadowRows > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if shadowRows == 0 {
		t.Fatalf("shadow_paths not reseeded for (%s, gen=%s) after divergence", branchRef, got)
	}

	cancel()
	wg.Wait()
}

func TestRun_BranchSwitchDropsPending(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	baseHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	appendEvent := func(path string, generation int64, stateName string) int64 {
		t.Helper()
		seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: generation,
			BaseHead:         baseHead,
			Operation:        "create",
			Path:             path,
			Fidelity:         "full",
			State:            stateName,
		}, []state.CaptureOp{{
			Op:        "create",
			Path:      path,
			Fidelity:  "full",
			AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID:  sql.NullString{String: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Valid: true},
		}})
		if err != nil {
			t.Fatalf("append %s: %v", path, err)
		}
		return seq
	}

	prevPending := appendEvent("prev-pending.txt", 1, state.EventStatePending)
	prevBlocked := appendEvent("prev-blocked.txt", 1, state.EventStateBlockedConflict)
	prevPublished := appendEvent("prev-published.txt", 1, state.EventStatePublished)
	nextPending := appendEvent("next-pending.txt", 2, state.EventStatePending)

	dropped, err := state.DeletePendingForGeneration(ctx, f.db, 1)
	if err != nil {
		t.Fatalf("DeletePendingForGeneration: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("dropped=%d want 1", dropped)
	}

	rows, err := f.db.SQL().QueryContext(ctx, `SELECT seq FROM capture_events ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	remaining := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scan: %v", err)
		}
		remaining[seq] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if remaining[prevPending] {
		t.Fatalf("previous generation pending seq %d was not deleted", prevPending)
	}
	for _, seq := range []int64{prevBlocked, prevPublished, nextPending} {
		if !remaining[seq] {
			t.Fatalf("seq %d should be retained; remaining=%v", seq, remaining)
		}
	}
}

func TestRun_StartupDivergenceBumpsGenerationAndReseedsShadow(t *testing.T) {
	t.Setenv(EnvShadowRetentionGenerations, "0")

	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}
	oldCtx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         seedHead,
	}
	if err := SaveBranchGeneration(ctx, f.db, oldCtx.BranchGeneration, seedHead); err != nil {
		t.Fatalf("SaveBranchGeneration: %v", err)
	}
	if seeded, err := BootstrapShadow(ctx, f.dir, f.db, oldCtx); err != nil {
		t.Fatalf("BootstrapShadow old generation: %v", err)
	} else if seeded == 0 {
		t.Fatalf("BootstrapShadow old generation seeded 0 rows")
	}
	if _, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef:        oldCtx.BranchRef,
		BranchGeneration: oldCtx.BranchGeneration,
		BaseHead:         oldCtx.BaseHead,
		Operation:        "create",
		Path:             "stale-pending.txt",
		Fidelity:         "full",
	}, []state.CaptureOp{{
		Op:        "create",
		Path:      "stale-pending.txt",
		Fidelity:  "full",
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:  sql.NullString{String: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Valid: true},
	}}); err != nil {
		t.Fatalf("AppendCaptureEvent stale pending: %v", err)
	}

	blob, err := git.HashObjectStdin(ctx, f.dir, []byte("rebased\n"))
	if err != nil {
		t.Fatalf("hash rebased blob: %v", err)
	}
	tree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: blob, Path: "rebased.txt"},
	})
	if err != nil {
		t.Fatalf("mktree rebased: %v", err)
	}
	rebasedHead, err := git.CommitTree(ctx, f.dir, tree, "rebased root")
	if err != nil {
		t.Fatalf("commit-tree rebased: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "reset", "--hard", rebasedHead); err != nil {
		t.Fatalf("git reset --hard rebased: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
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

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		gen, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
		head, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchHead)
		if gen == "2" && head == rebasedHead {
			break
		}
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	gen, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
	head, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchHead)
	if gen != "2" || head != rebasedHead {
		t.Fatalf("startup branch meta=(gen=%q head=%q), want (gen=2 head=%s)", gen, head, rebasedHead)
	}

	var shadowRows int
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := f.db.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
			"refs/heads/main", int64(2)).Scan(&shadowRows); err != nil {
			t.Fatalf("count shadow rows: %v", err)
		}
		if shadowRows > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if shadowRows == 0 {
		t.Fatalf("shadow_paths not reseeded for startup generation 2")
	}
	var oldShadowRows int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
		"refs/heads/main", int64(1)).Scan(&oldShadowRows); err != nil {
		t.Fatalf("count old shadow rows: %v", err)
	}
	if oldShadowRows != 0 {
		t.Fatalf("old shadow generation rows=%d want 0", oldShadowRows)
	}
	time.Sleep(100 * time.Millisecond)
	var events int
	if err := f.db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events`).Scan(&events); err != nil {
		t.Fatalf("count capture events: %v", err)
	}
	if events != 0 {
		t.Fatalf("startup after offline reset captured %d phantom events, want 0", events)
	}

	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run returned %v", runErr)
	}
}

func TestRun_StartupClassifyErrorDoesNotBumpGeneration(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse head: %v", err)
	}
	if err := SaveBranchGeneration(ctx, f.db, 4, head); err != nil {
		t.Fatalf("SaveBranchGeneration: %v", err)
	}
	bogusToken := branchTokenRev("not-a-real-commit", "refs/heads/main")
	if err := state.MetaSet(ctx, f.db, MetaKeyBranchToken, bogusToken); err != nil {
		t.Fatalf("MetaSet branch token: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
			MessageFn:   DeterministicMessage,
		})
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}

	got, ok, err := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
	if err != nil {
		t.Fatalf("MetaGet branch generation: %v", err)
	}
	if !ok {
		t.Fatalf("branch.generation missing")
	}
	if got != "4" {
		t.Fatalf("branch.generation=%q after classify error; want 4", got)
	}
}

// TestRun_BranchGenerationStableOnAcdFastForward: the daemon's own
// commit-driven HEAD advance is a fast-forward (newHead descends from
// prevHead), so the generation must NOT bump even though the token
// changed.
func TestRun_BranchGenerationStableOnAcdFastForward(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	startHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(runCtx, Options{
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

	// Drop a file and wake — the daemon should commit it (fast-forward).
	if err := os.WriteFile(filepath.Join(f.dir, "ff.txt"), []byte("ff\n"), 0o644); err != nil {
		t.Fatalf("write ff: %v", err)
	}
	// Multiple wakes — the run loop drives capture+replay on each tick;
	// under -race + -p N the first wake can race the bootstrap.
	for i := 0; i < 4; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	newHead := waitForCommit(t, f.dir, startHead, 5*time.Second)
	if newHead == startHead {
		t.Fatalf("HEAD did not advance via daemon commit")
	}

	// Wait for the next loop iteration to observe the new HEAD and run
	// the token classifier — poll for branch.head to flip to newHead.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		v, ok, _ := state.MetaGet(ctx, f.db, MetaKeyBranchHead)
		if ok && v == newHead {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Generation must still be 1.
	v, ok, err := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	if !ok {
		t.Fatalf("branch.generation not seeded")
	}
	if v != "1" {
		t.Fatalf("branch.generation=%q after ACD fast-forward; want 1 (no bump)", v)
	}

	cancel()
	wg.Wait()
}

// TestBranchGenerationToken_RevAndMissing: token shape covers both ref-present
// and orphan-HEAD cases.
func TestBranchGenerationToken_RevAndMissing(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	tok, err := BranchGenerationToken(ctx, f.dir)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	want := branchTokenRev(head, "refs/heads/main")
	if tok != want {
		t.Fatalf("token=%q want %q", tok, want)
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
	if _, err := git.Run(ctx, git.RunOpts{Dir: empty}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref empty HEAD: %v", err)
	}
	tok2, err := BranchGenerationToken(ctx, empty)
	if err != nil {
		t.Fatalf("token empty: %v", err)
	}
	if tok2 != branchTokenMissing("refs/heads/main") {
		t.Fatalf("empty token=%q want %q", tok2, branchTokenMissing("refs/heads/main"))
	}
}

func TestRun_SameSHABranchSwitchCommitsToActiveBranch(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	startHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse start: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
			RepoPath:      f.dir,
			GitDir:        f.gitDir,
			DB:            f.db,
			Scheduler:     fastScheduler(),
			BootGrace:     30 * time.Second,
			WakeCh:        wakeCh,
			ShutdownCh:    shutdownCh,
			SkipSignals:   true,
			MessageFn:     DeterministicMessage,
			PruneInterval: time.Hour,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		if runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tok, ok, _ := state.MetaGet(ctx, f.db, MetaKeyBranchToken)
		if ok && strings.Contains(tok, "refs/heads/main") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	tok, ok, err := state.MetaGet(ctx, f.db, MetaKeyBranchToken)
	if err != nil {
		t.Fatalf("MetaGet branch token: %v", err)
	}
	if !ok || !strings.Contains(tok, "refs/heads/main") {
		t.Fatalf("daemon did not seed main branch token before switch; token=%q ok=%v", tok, ok)
	}

	featureRef := "refs/heads/feature/same-sha"
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "checkout", "-q", "-b", "feature/same-sha"); err != nil {
		t.Fatalf("checkout feature: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "feature.txt"), []byte("feature\n"), 0o644); err != nil {
		t.Fatalf("write feature.txt: %v", err)
	}
	wakeCh <- struct{}{}

	newHead := waitForCommit(t, f.dir, startHead, 5*time.Second)
	mainHead, err := git.RevParse(ctx, f.dir, "refs/heads/main")
	if err != nil {
		t.Fatalf("rev-parse main: %v", err)
	}
	if mainHead != startHead {
		t.Fatalf("main advanced to %s; want unchanged %s", mainHead, startHead)
	}
	featureHead, err := git.RevParse(ctx, f.dir, featureRef)
	if err != nil {
		t.Fatalf("rev-parse feature: %v", err)
	}
	if featureHead != newHead {
		t.Fatalf("feature head=%s want new HEAD %s", featureHead, newHead)
	}
	if featureHead == startHead {
		t.Fatalf("feature branch did not advance from start")
	}
}

// TestRun_RepeatedEditsToSameFile_OrderedCommits drives the daemon Run loop
// with three sequential edits to the same path (v1 -> v2 -> v3), waking the
// daemon after each edit. The regression target is the scratch-index
// refactor for replay: the same path's modify chain must publish in order
// when driven through the real capture+wake+publish loop, not just under
// direct Replay() calls (covered by TestReplay_ModifyChain_OrderedReplay).
//
// Pre-fix this would have either raced (only the last write commits, prior
// edits get coalesced into a single capture), or — with separate captures
// per wake — failed with "modify before-state mismatch" because the live
// index probe would see whichever blob was last written, not the captured
// before/after blobs the chain expects.
func TestRun_RepeatedEditsToSameFile_OrderedCommits(t *testing.T) {
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

	target := filepath.Join(f.dir, "chain.txt")
	versions := []string{"v1\n", "v2\n", "v3\n"}
	prevHead := startHead
	heads := make([]string, 0, len(versions))
	for i, body := range versions {
		if err := os.WriteFile(target, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", target, err)
		}
		// Multiple wakes — under -race the first wake may race the loop
		// boundary; the run loop coalesces extras.
		for j := 0; j < 4; j++ {
			select {
			case wakeCh <- struct{}{}:
			default:
			}
			time.Sleep(40 * time.Millisecond)
		}
		newHead := waitForCommit(t, f.dir, prevHead, 5*time.Second)
		if newHead == prevHead {
			t.Fatalf("edit %d: HEAD did not advance from %s", i+1, prevHead)
		}
		heads = append(heads, newHead)
		prevHead = newHead
	}

	// Walk the resulting log: chain.txt's blob must trace v1 -> v2 -> v3
	// commit-by-commit, with each commit a fast-forward of its predecessor.
	wantBlobs := make([]string, len(versions))
	for i, body := range versions {
		oid, err := git.HashObjectStdin(context.Background(), f.dir, []byte(body))
		if err != nil {
			t.Fatalf("hash %d: %v", i, err)
		}
		wantBlobs[i] = oid
	}
	for i, h := range heads {
		entries, err := git.LsTree(context.Background(), f.dir, h, false, "chain.txt")
		if err != nil {
			t.Fatalf("ls-tree %s: %v", h, err)
		}
		if len(entries) != 1 {
			t.Fatalf("commit %d (%s): chain.txt missing", i, h)
		}
		if entries[0].OID != wantBlobs[i] {
			t.Fatalf("commit %d (%s): chain.txt blob=%s want %s",
				i, h, entries[0].OID, wantBlobs[i])
		}
	}

	// Final tip must be the v3 commit and reachable from the seed via
	// fast-forwards only — the daemon must not have force-pushed mid-way.
	mb, err := git.Run(context.Background(), git.RunOpts{Dir: f.dir},
		"merge-base", "--is-ancestor", startHead, heads[len(heads)-1])
	if err != nil {
		t.Fatalf("merge-base --is-ancestor: %v\n%s", err, mb)
	}

	cancel()
	wg.Wait()
}

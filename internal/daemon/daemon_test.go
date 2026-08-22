package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestMain(m *testing.M) {
	_ = os.Setenv(ai.EnvProvider, "deterministic")
	// Ordinary daemon tests exercise run-loop behavior, not host process
	// discovery. Keep their lock acquisition deterministic and leave the real
	// and injected owner-probe paths to lock_test.go.
	daemonLockOwnerProbe = func(context.Context, string, int) ([]int, error) {
		return nil, nil
	}
	runtimeRoot, err := os.MkdirTemp("", "acd-daemon-runtime-*")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "daemon test runtime setup: %v\n", err)
		os.Exit(1)
	}
	for name, path := range map[string]string{
		"HOME":            filepath.Join(runtimeRoot, "home"),
		"XDG_CONFIG_HOME": filepath.Join(runtimeRoot, "config"),
		"XDG_STATE_HOME":  filepath.Join(runtimeRoot, "state"),
		"XDG_DATA_HOME":   filepath.Join(runtimeRoot, "share"),
	} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			_, _ = fmt.Fprintf(os.Stderr,
				"daemon test runtime directory %s: %v\n", name, err)
			_ = os.RemoveAll(runtimeRoot)
			os.Exit(1)
		}
		if err := os.Setenv(name, path); err != nil {
			_, _ = fmt.Fprintf(os.Stderr,
				"daemon test runtime environment %s: %v\n", name, err)
			_ = os.RemoveAll(runtimeRoot)
			os.Exit(1)
		}
	}
	templateRoot, err := setupDaemonTestRepoTemplates()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "daemon test template setup: %v\n", err)
		_ = os.RemoveAll(runtimeRoot)
		os.Exit(1)
	}
	code := m.Run()
	if err := os.RemoveAll(templateRoot); err != nil && code == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "daemon test template cleanup: %v\n", err)
		code = 1
	}
	if err := os.RemoveAll(runtimeRoot); err != nil && code == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "daemon test runtime cleanup: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

func TestWorktreeRejectWritersStayInExactGitDir(t *testing.T) {
	repo, mainGitDir := initDaemonLockRepo(t)
	linked := filepath.Join(t.TempDir(), "linked")
	runDaemonLockGit(t, repo, "worktree", "add", "-b", "linked-reject-log-test", linked)
	linkedGitDir := strings.TrimSpace(runDaemonLockGit(t, linked, "rev-parse", "--absolute-git-dir"))
	if linkedGitDir == mainGitDir {
		t.Fatalf("linked git dir unexpectedly equals main git dir %q", mainGitDir)
	}

	type run struct {
		name   string
		ctx    context.Context
		gitDir string
		start  int64
	}
	runs := []run{
		{name: "main", ctx: withIntentRejectsWriter(context.Background(), mainGitDir), gitDir: mainGitDir, start: 1},
		{name: "linked", ctx: withIntentRejectsWriter(context.Background(), linkedGitDir), gitDir: linkedGitDir, start: 101},
	}
	var wg sync.WaitGroup
	for _, current := range runs {
		current := current
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int64(0); i < 40; i++ {
				ai.LogRejectedIntentPlan(current.ctx, current.name,
					ai.IntentPlanRequest{OfferedCaptures: []ai.OfferedCapture{{Seq: current.start + i}}},
					`{"selected_seqs":[]}`, errors.New("worktree reject"))
			}
		}()
	}
	wg.Wait()

	for _, current := range runs {
		path := filepath.Join(current.gitDir, "acd", ai.IntentRejectsFileName)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s rejects at %s: %v", current.name, path, err)
		}
		if got := strings.Count(string(body), "\n"); got != 40 {
			t.Fatalf("%s rows=%d want 40 at %s", current.name, got, path)
		}
		other := map[string]string{"main": "linked", "linked": "main"}[current.name]
		if strings.Contains(string(body), `"provider":"`+other+`"`) {
			t.Fatalf("%s rejects contain %s cross-directory row", current.name, other)
		}
	}
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
	repo := cloneDaemonTestRepo(t, daemonRepoTemplate)
	return &daemonFixture{dir: repo.dir, gitDir: repo.gitDir, db: repo.db}
}

func removeAllWithRetry(t *testing.T, path string) {
	t.Helper()
	var err error
	for i := 0; i < 10; i++ {
		err = os.RemoveAll(path)
		if err == nil {
			return
		}
		time.Sleep(time.Duration(i+1) * 25 * time.Millisecond)
	}
	t.Fatalf("remove temp dir %s: %v", path, err)
}

// fastScheduler keeps the test loop responsive (~10ms ticks).
func fastScheduler() Scheduler {
	return Scheduler{
		Base:         10 * time.Millisecond,
		IdleCeiling:  20 * time.Millisecond,
		ErrorCeiling: 50 * time.Millisecond,
	}
}

func oneShotBranchTokenCheckGate() (hook func(), entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	hook = func() {
		enterOnce.Do(func() {
			close(enteredCh)
			<-releaseCh
		})
	}
	release = func() {
		releaseOnce.Do(func() { close(releaseCh) })
	}
	return hook, enteredCh, release
}

func waitForBranchTokenCheckGate(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not reach branch-token check gate")
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
	var lastMode string
	var lastOK bool
	var lastUpdated float64
	var lastErr error
	for time.Now().Before(deadline) {
		st, ok, err := state.LoadDaemonState(context.Background(), db)
		if err == nil && st.Mode == mode {
			return
		}
		lastMode = st.Mode
		lastOK = ok
		lastUpdated = st.UpdatedTS
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("daemon_state.mode did not become %q within %v (last mode=%q ok=%v updated_ts=%.6f err=%v)",
		mode, timeout, lastMode, lastOK, lastUpdated, lastErr)
}

func waitForDaemonModeFresh(t *testing.T, dbPath, mode string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout+2*time.Second)
	defer cancel()

	fresh, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open fresh state db: %v", err)
	}
	defer func() {
		if err := fresh.Close(); err != nil {
			t.Fatalf("close fresh state db: %v", err)
		}
	}()
	waitForDaemonMode(t, fresh, mode, timeout)
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

func waitForCaptureEventCount(t *testing.T, db *state.DB, want int, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var got int
		if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events`).Scan(&got); err != nil {
			t.Fatalf("count capture_events: %v", err)
		}
		if got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var got int
	if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events`).Scan(&got); err != nil {
		t.Fatalf("count capture_events: %v", err)
	}
	if got == want {
		return
	}
	if want == 0 && got > 0 {
		rows, err := db.SQL().QueryContext(ctx, `SELECT seq, operation, path, state, base_head FROM capture_events ORDER BY seq`)
		if err != nil {
			t.Fatalf("capture_events=%d want 0; query events: %v", got, err)
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var seq int64
			var operation, path, stateName, baseHead string
			if err := rows.Scan(&seq, &operation, &path, &stateName, &baseHead); err != nil {
				t.Fatalf("scan captured event: %v", err)
			}
			details = append(details, fmt.Sprintf("seq=%d op=%s path=%s state=%s base=%s", seq, operation, path, stateName, baseHead))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate captured events: %v", err)
		}
		t.Fatalf("capture_events=%d want 0 after %v: %s", got, timeout, strings.Join(details, "; "))
	}
	t.Fatalf("capture_events=%d want %d after %v", got, want, timeout)
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
	// Run returned after graceful() persisted mode=stopped. Use a fresh DB
	// handle to mirror an external controller and avoid stale read-pool
	// snapshots from the long-lived fixture handle under macOS broad runs.
	waitForDaemonModeFresh(t, f.db.Path(), "stopped", 5*time.Second)
}

func TestRunStartupCompletesResolvedDrainFromOlderGeneration(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	tree, err := git.RevParse(ctx, f.dir, "HEAD^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,
 state,commit_oid,published_ts
) VALUES('refs/heads/main',474,?,'modify','resolved.txt','exact',1,
         'published',?,2)`, head, head)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	const checkpointID = "cp-1787439100000-0123456789abcdef"
	worktreeID := checkpointpkg.WorktreeID(f.dir)
	checkpointRef := "refs/acd/checkpoints/v1/" + worktreeID + "/" + checkpointID
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"update-ref", checkpointRef, head); err != nil {
		t.Fatal(err)
	}
	checkpoint := state.Checkpoint{
		ID: checkpointID, OperationID: "op-daemon-startup-resolved-drain",
		WorktreeID: worktreeID, Reason: state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: head,
		ObservedRef: "refs/heads/main", TreeOID: tree, CommitOID: head,
		Ref: checkpointRef, CreatedTS: 1, EventSeqs: []int64{seq},
	}
	if created, err := state.PrepareCheckpoint(
		ctx, f.db, checkpoint, publicationDrainTestDigest); err != nil || !created {
		t.Fatalf("prepare checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, f.db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	drain := state.PublicationDrain{
		ID: "drain-" + checkpointID, CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 474, Phase: state.PublicationDrainNeedsAction,
		TargetEventCount: 1, LastError: "forced_capture_deferred",
		StagedConsent: true, StagedConsumed: true,
		CreatedTS: 3, UpdatedTS: 3, LastProgressTS: 3,
		EventSeqs: []int64{seq},
	}
	if created, err := state.PreparePublicationDrain(
		ctx, f.db, drain); err != nil || !created {
		t.Fatalf("prepare drain=(%t,%v)", created, err)
	}
	statusBefore, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "status", "--porcelain=v1")
	if err != nil {
		t.Fatal(err)
	}

	shutdownCh := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			MessageFn: DeterministicMessage, WakeCh: make(chan struct{}, 1),
			ShutdownCh: shutdownCh, SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	completed, err := state.PublicationDrainByID(ctx, f.db, drain.ID)
	if err != nil || completed.Phase != state.PublicationDrainCompleted ||
		completed.PublishedEventCount != 1 || completed.LastError != "" {
		t.Fatalf("startup drain=%+v err=%v", completed, err)
	}
	headAfter, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	statusAfter, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "status", "--porcelain=v1")
	if err != nil {
		t.Fatal(err)
	}
	if headAfter != head || string(statusAfter) != string(statusBefore) {
		t.Fatalf("startup reconciliation changed Git: head=%s want=%s status=%q want=%q",
			headAfter, head, statusAfter, statusBefore)
	}
}

func TestRun_IntentV2MissingPrerequisitesCapturesWithoutReplay(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyIntent))
	t.Setenv("ACD_COMMIT_PRESET", "quality")
	t.Setenv(ai.EnvProvider, "deterministic")
	t.Setenv(ai.EnvDiffEgress, "false")
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))

	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	startHead, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	wakeCh := make(chan struct{}, 4)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			MessageFn: DeterministicMessage, WakeCh: wakeCh,
			ShutdownCh: make(chan struct{}, 1), SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	waitForDaemonMode(t, f.db, "running", 2*time.Second)

	if err := os.WriteFile(filepath.Join(f.dir, "blocked-intent.txt"),
		[]byte("durable capture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	wakeCh <- struct{}{}
	waitForCaptureEventCount(t, f.db, 1, 3*time.Second)
	waitForMetaValue(t, f.db, metaIntentV2MigrationState,
		"needs_attention", 2*time.Second)

	var eventState string
	if err := f.db.SQL().QueryRow(
		`SELECT state FROM capture_events ORDER BY seq DESC LIMIT 1`,
	).Scan(&eventState); err != nil {
		t.Fatal(err)
	}
	if eventState != "pending" {
		t.Fatalf("captured event state=%q want pending", eventState)
	}
	time.Sleep(100 * time.Millisecond)
	head, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if head != startHead {
		t.Fatalf("blocked Intent v2 replay advanced HEAD: %s -> %s",
			startHead, head)
	}
	attention, ok, err := state.MetaGet(context.Background(), f.db,
		"intent.v2.needs_attention")
	if err != nil || !ok || !strings.Contains(attention, "acd configure") {
		t.Fatalf("needs-attention guidance=%q ok=%v err=%v",
			attention, ok, err)
	}
}

func TestRun_IntentV2CutoverFailureRecoversAfterConfigure(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	t.Setenv(ai.EnvProvider, "deterministic")
	configPath := filepath.Join(t.TempDir(), "config-is-a-file")
	if err := os.WriteFile(configPath, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configPath)
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))

	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	if err := state.MetaSetMany(context.Background(), f.db,
		map[string]string{
			metaIntentV2CutoverRequired: "true",
			"commit.strategy":           "intent",
		}); err != nil {
		t.Fatal(err)
	}
	startHead, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	wakeCh := make(chan struct{}, 4)
	ctx, cancel := context.WithCancel(context.Background())
	passDone := make(chan struct{}, 1)
	passRelease := make(chan struct{})
	waitForPass := func(label string) {
		t.Helper()
		select {
		case <-passDone:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %s pass", label)
		}
	}
	releasePass := func(label string) {
		t.Helper()
		select {
		case passRelease <- struct{}{}:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out releasing %s pass", label)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			MessageFn: DeterministicMessage, WakeCh: wakeCh,
			ShutdownCh: make(chan struct{}, 1), SkipSignals: true,
			runtimeBuildProvider: func(
				cfg ai.ProviderConfig,
			) (ai.Provider, io.Closer, error) {
				return &runtimeTestProvider{name: cfg.Mode}, nil, nil
			},
			afterRunLoopWorkDecision: func(_, _ bool) {
				select {
				case passDone <- struct{}{}:
				case <-ctx.Done():
					return
				}
				select {
				case <-passRelease:
				case <-ctx.Done():
				}
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	waitForPass("startup cutover failure")
	if err := os.WriteFile(filepath.Join(f.dir, "cutover-error.txt"),
		[]byte("captured\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	wakeCh <- struct{}{}
	releasePass("capture")
	waitForPass("capture")
	waitForCaptureEventCount(t, f.db, 1, 3*time.Second)
	head, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if head != startHead {
		t.Fatalf("cutover failure replay advanced HEAD: %s -> %s",
			startHead, head)
	}
	attention, ok, err := state.MetaGet(context.Background(), f.db,
		"intent.v2.needs_attention")
	if err != nil || !ok || !strings.Contains(attention, "acd configure") {
		t.Fatalf("cutover attention=%q ok=%v err=%v", attention, ok, err)
	}

	revision := runtimeRevision(t, f.db, "configured", 1,
		map[string]any{
			"commit.strategy": "intent",
			"intent.window":   10,
		})
	if _, activated, err := state.RequestConfigActivation(
		context.Background(), f.db, revision.ID, sql.NullInt64{},
	); err != nil || !activated {
		t.Fatalf("request configured revision: activated=%v err=%v",
			activated, err)
	}
	flushID, err := state.EnqueueFlushRequest(context.Background(), f.db,
		"flush_logical", true,
		sql.NullString{String: "test-session", Valid: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Force one complete pass while configuration storage is still unusable.
	// The pass must acknowledge the only logical flush without consuming its
	// batch-wait bypass through the blocked replay path.
	wakeCh <- struct{}{}
	releasePass("blocked logical flush")
	waitForPass("blocked logical flush")
	var flushStatus string
	if err := f.db.SQL().QueryRow(
		`SELECT status FROM flush_requests WHERE id=?`, flushID,
	).Scan(&flushStatus); err != nil {
		t.Fatal(err)
	}
	if flushStatus != "completed" {
		t.Fatalf("logical flush status=%q want completed before activation", flushStatus)
	}
	var logicalFlushes int
	if err := f.db.SQL().QueryRow(
		`SELECT COUNT(*) FROM flush_requests WHERE command='flush_logical'`,
	).Scan(&logicalFlushes); err != nil {
		t.Fatal(err)
	}
	if logicalFlushes != 1 {
		t.Fatalf("logical flush rows=%d want exactly 1", logicalFlushes)
	}
	head, err = git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if head != startHead {
		t.Fatalf("blocked logical flush advanced HEAD: %s -> %s", startHead, head)
	}

	// Make configuration usable only after the flush row is completed, then
	// drive a pass with a plain wake. The retained bypass must publish without
	// enqueueing a second logical flush.
	if err := os.Remove(configPath); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(configPath, 0o700); err != nil {
		t.Fatal(err)
	}
	wakeCh <- struct{}{}
	releasePass("configuration recovery")
	newHead := waitForCommit(t, f.dir, startHead, 5*time.Second)
	if newHead == startHead {
		t.Fatal("configured v2 revision did not resume replay")
	}
	waitForPass("configuration recovery")
	t.Logf("retained logical flush advanced HEAD after activation: %s -> %s",
		startHead, newHead)
	if err := f.db.SQL().QueryRow(
		`SELECT COUNT(*) FROM flush_requests WHERE command='flush_logical'`,
	).Scan(&logicalFlushes); err != nil {
		t.Fatal(err)
	}
	if logicalFlushes != 1 {
		t.Fatalf("logical flush rows after replay=%d want exactly 1", logicalFlushes)
	}
	waitForMetaValue(t, f.db, metaIntentV2MigrationState,
		"active", 2*time.Second)
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

func TestRun_DetachedReattachLogsOnceWithContext(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	logSink := &captureLogHandler{level: slog.LevelInfo}
	logger := slog.New(logSink)
	wakeCh := make(chan struct{}, 16)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Logger: logger, Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			WakeCh: wakeCh, ShutdownCh: make(chan struct{}, 1), SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitForDaemonMode(t, f.db, "running", 2*time.Second)

	branchOut, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "--short", "HEAD")
	if err != nil {
		t.Fatalf("resolve attached branch: %v", err)
	}
	branch := strings.TrimSpace(string(branchOut))
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "checkout", "--detach", head); err != nil {
		t.Fatalf("detach HEAD: %v", err)
	}
	wakeCh <- struct{}{}
	waitFor(t, 3*time.Second, "single detached transition log", func() bool {
		return countLogMessage(logSink.Records(), "detached HEAD detected; capture and publication paused for this worktree") == 1
	})
	for i := 0; i < 6; i++ {
		wakeCh <- struct{}{}
	}
	time.Sleep(100 * time.Millisecond)
	if got := countLogMessage(logSink.Records(), "detached HEAD detected; capture and publication paused for this worktree"); got != 1 {
		t.Fatalf("detached transition logs=%d want 1", got)
	}

	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "checkout", branch); err != nil {
		t.Fatalf("reattach HEAD: %v", err)
	}
	wakeCh <- struct{}{}
	waitFor(t, 3*time.Second, "single reattach transition log", func() bool {
		return countLogMessage(logSink.Records(), "HEAD reattached; capture and publication resumed for this worktree") == 1
	})

	records := logSink.Records()
	for _, record := range records {
		if record.Attrs["repo_hash"] == "" || record.Attrs["worktree"] != f.dir || record.Attrs["git_dir"] != f.gitDir {
			t.Fatalf("daemon record %q missing stable context: %v", record.Message, record.Attrs)
		}
	}
	for _, message := range []string{
		"detached HEAD detected; capture and publication paused for this worktree",
		"HEAD reattached; capture and publication resumed for this worktree",
	} {
		matched := recordsByMessage(records, message)
		if len(matched) != 1 {
			t.Fatalf("%q records=%d want 1", message, len(matched))
		}
		attrs := matched[0].Attrs
		if attrs["repo_hash"] == "" || attrs["worktree"] != f.dir || attrs["git_dir"] != f.gitDir {
			t.Fatalf("%q context=%v", message, attrs)
		}
		if generation, ok := attrs["branch_generation"].(int64); !ok || generation < 1 {
			t.Fatalf("%q branch_generation=%v", message, attrs["branch_generation"])
		}
	}
	reattach := recordsByMessage(records,
		"HEAD reattached; capture and publication resumed for this worktree")[0]
	if reattach.Attrs["branch_ref"] != "refs/heads/"+branch {
		t.Fatalf("reattach branch_ref=%v want refs/heads/%s", reattach.Attrs["branch_ref"], branch)
	}
}

func TestWorktreeLogContextDistinguishesDetachedRuns(t *testing.T) {
	logSink := &captureLogHandler{level: slog.LevelInfo}
	base := slog.New(logSink)
	type worktree struct {
		path   string
		gitDir string
		logger *slog.Logger
	}
	runs := make([]worktree, 0, 2)
	for _, name := range []string{"main", "linked"} {
		path := filepath.Join(t.TempDir(), name)
		gitDir := filepath.Join(path, ".git")
		logger, logContext := newDaemonLogger(base, Options{RepoPath: path, GitDir: gitDir})
		logContext.SetBranch("", 3)
		runs = append(runs, worktree{path: path, gitDir: gitDir, logger: logger})
	}
	var wg sync.WaitGroup
	for _, current := range runs {
		current := current
		wg.Add(1)
		go func() {
			defer wg.Done()
			current.logger.Warn("detached worktree")
		}()
	}
	wg.Wait()

	records := recordsByMessage(logSink.Records(), "detached worktree")
	if len(records) != 2 {
		t.Fatalf("detached records=%d want 2", len(records))
	}
	seen := make(map[string]bool)
	for _, record := range records {
		worktreePath, _ := record.Attrs["worktree"].(string)
		gitDir, _ := record.Attrs["git_dir"].(string)
		repoHash, _ := record.Attrs["repo_hash"].(string)
		if worktreePath == "" || gitDir == "" || repoHash == "" || record.Attrs["branch_generation"] != int64(3) {
			t.Fatalf("incomplete detached context: %v", record.Attrs)
		}
		seen[worktreePath+"|"+gitDir+"|"+repoHash] = true
	}
	if len(seen) != 2 {
		t.Fatalf("worktree contexts are not distinguishable: %v", seen)
	}
}

func TestRun_LongReplayHeartbeatStaysFreshAndJoinsOnCancellation(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	started := make(chan struct{})
	returned := make(chan struct{})
	var once sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			WakeCh: make(chan struct{}, 1), ShutdownCh: make(chan struct{}, 1), SkipSignals: true,
			progressHeartbeatEvery: 10 * time.Millisecond,
			replay: func(replayCtx context.Context, _ string, _ *state.DB, _ CaptureContext, _ ReplayOpts) (ReplaySummary, error) {
				once.Do(func() { close(started) })
				<-replayCtx.Done()
				close(returned)
				return ReplaySummary{}, replayCtx.Err()
			},
		})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("long replay did not start")
	}
	before, ok, err := state.LoadDaemonState(context.Background(), f.db)
	if err != nil || !ok {
		cancel()
		t.Fatalf("load heartbeat before wait: ok=%v err=%v", ok, err)
	}
	waitFor(t, time.Second, "heartbeat during blocked replay", func() bool {
		current, found, loadErr := state.LoadDaemonState(context.Background(), f.db)
		return loadErr == nil && found && current.HeartbeatTS > before.HeartbeatTS
	})
	current, _, err := state.LoadDaemonState(context.Background(), f.db)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	maxAge := time.Since(time.Unix(0, int64(current.HeartbeatTS*1e9)))
	if maxAge < 0 || maxAge > 250*time.Millisecond {
		cancel()
		t.Fatalf("blocked replay heartbeat age=%v want <=250ms", maxAge)
	}

	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("mock planner did not observe cancellation")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not join long-replay heartbeat on shutdown")
	}
	stopped, ok, err := state.LoadDaemonState(context.Background(), f.db)
	if err != nil || !ok || stopped.Mode != "stopped" {
		t.Fatalf("stopped state: %+v ok=%v err=%v", stopped, ok, err)
	}
	time.Sleep(40 * time.Millisecond)
	after, _, err := state.LoadDaemonState(context.Background(), f.db)
	if err != nil || after.HeartbeatTS != stopped.HeartbeatTS {
		t.Fatalf("heartbeat continued after joined shutdown: before=%f after=%f err=%v",
			stopped.HeartbeatTS, after.HeartbeatTS, err)
	}
}

func TestRun_LongReplayHeartbeatGuardJoinsOnPanic(t *testing.T) {
	var beats atomic.Int64
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		runWithProgressHeartbeat(context.Background(), 5*time.Millisecond, func() {
			beats.Add(1)
		}, func() {
			time.Sleep(15 * time.Millisecond)
			panic("test panic")
		})
	}()
	joined := beats.Load()
	if joined == 0 {
		t.Fatal("heartbeat did not run before panic")
	}
	time.Sleep(20 * time.Millisecond)
	if after := beats.Load(); after != joined {
		t.Fatalf("heartbeat goroutine leaked after panic: before=%d after=%d", joined, after)
	}
}

func TestRun_PauseDuringGitOperation(t *testing.T) {
	t.Parallel()

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
			runBoundedParallel(t)

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

	// Capture slog output through a custom handler so the test can assert
	// that the "marker may be stale" warning fires after the injected clock
	// crosses the staleness threshold while HEAD has not moved. The handler
	// is goroutine-safe because Run can emit concurrently with the polling
	// goroutine that reads recordedLogs below.
	logSink := &captureLogHandler{level: slog.LevelWarn}
	logger := slog.New(logSink)

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
			Logger:      logger,
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

	// Visibility assertion 1: the slog warning ("marker may be stale; verify
	// git status") must have fired at least once after the clock advanced
	// past staleOpMarkerThreshold. This proves the rate-limited warn lane
	// is wired up; without it, an abandoned rebase silently mutes capture/
	// replay forever and the operator never sees a hint.
	staleDeadline := time.Now().Add(2 * time.Second)
	var sawWarn bool
	var sawAttrs map[string]any
	for time.Now().Before(staleDeadline) {
		records := logSink.Records()
		for _, rec := range records {
			if rec.Level != slog.LevelWarn {
				continue
			}
			if !strings.Contains(rec.Message, "marker may be stale") {
				continue
			}
			sawWarn = true
			sawAttrs = rec.Attrs
			break
		}
		if sawWarn {
			break
		}
		// Force more ticks to give the staleness branch a chance to fire.
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(40 * time.Millisecond)
	}
	if !sawWarn {
		t.Fatalf("stale operation marker warn never fired after clock advance; recorded warns=%+v",
			logSink.Records())
	}
	if got := sawAttrs["operation"]; got != "merge" {
		t.Fatalf("stale-warn attr operation=%v want merge; attrs=%+v", got, sawAttrs)
	}
	if got := sawAttrs["head"]; got != startHead {
		t.Fatalf("stale-warn attr head=%v want %s; attrs=%+v", got, startHead, sawAttrs)
	}

	// Visibility assertion 2: diagnose-equivalent computation must classify
	// this marker as stale. We mirror diagnoseOperationMarker's logic locally
	// (elapsed >= staleOpMarkerThreshold AND HEAD == head_at). Couples with
	// the diagnose-side test in internal/cli/diagnose_test.go which asserts
	// the JSON `stale_operation_marker` field flips true under the same shape.
	setAtSec, parseErr := strconv.ParseFloat(setAtRaw, 64)
	if parseErr != nil {
		t.Fatalf("parse set_at %q: %v", setAtRaw, parseErr)
	}
	markerSetAt := time.Unix(0, int64(setAtSec*float64(time.Second)))
	elapsed := nowFn().Sub(markerSetAt)
	staleByDiagnose := elapsed >= 15*time.Minute && headAtRaw == startHead
	if !staleByDiagnose {
		t.Fatalf("diagnose-equivalent stale check false: elapsed=%v head_at=%s startHead=%s",
			elapsed, headAtRaw, startHead)
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
	// 90s into the future stays within ClampRewindGraceAtStartup's tolerance
	// (2 * defaultRewindGrace = 120s) so the daemon's startup clamp leaves
	// our pre-set marker in place; otherwise the clamp normalizes the
	// timestamp and the test loses track of the value it stamped.
	until := time.Now().UTC().Add(90 * time.Second).Format(time.RFC3339)
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

	waitForDaemonModeFresh(t, f.db.Path(), "stopped", 5*time.Second)
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
	waitForDaemonModeFresh(t, f.db.Path(), "stopped", 5*time.Second)
}

func TestRun_ReturnsErrorWhenStoppedStateCannotPersist(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	wakeCh := make(chan struct{}, 1)
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

	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	if err := f.db.Close(); err != nil {
		t.Fatalf("close db before shutdown: %v", err)
	}
	shutdownCh <- struct{}{}

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("Run returned nil after stopped-state persist failure")
		}
		if !strings.Contains(err.Error(), "stamp stopped state") {
			t.Fatalf("Run error %q does not identify stopped-state persist failure", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not exit after shutdown with closed state DB")
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

func TestRun_FlockContentionDoesNotResetIntentPlannerHealth(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyIntent))
	f := newDaemonFixture(t)
	seed := intentPlannerHealthRecord{
		Version: intentPlannerHealthVersion,
		IntentPlannerHealthSnapshot: IntentPlannerHealthSnapshot{
			State:               IntentPlannerCircuitOpen,
			ProviderFingerprint: IntentPlannerProviderFingerprint(openAIIntentHealthIdentity("https://previous.example/v1")),
			ConsecutiveFailures: 1,
			BackoffLevel:        0,
			LastFailureClass:    IntentPlannerFailureTransport,
			LastError:           "previous provider unavailable",
		},
	}
	if err := state.MetaSetJSON(context.Background(), f.db, MetaKeyIntentPlannerHealth, seed); err != nil {
		t.Fatalf("seed intent planner health: %v", err)
	}
	before, ok, err := state.MetaGet(context.Background(), f.db, MetaKeyIntentPlannerHealth)
	if err != nil || !ok {
		t.Fatalf("read seeded intent planner health: ok=%v err=%v", ok, err)
	}

	lock, err := AcquireDaemonLock(f.gitDir)
	if err != nil {
		t.Fatalf("AcquireDaemonLock: %v", err)
	}
	defer func() { _ = lock.Release() }()

	err = Run(context.Background(), Options{
		RepoPath: f.dir,
		GitDir:   f.gitDir,
		DB:       f.db,
		MessageFn: func(context.Context, EventContext) (string, error) {
			return "unused", nil
		},
		IntentPlanner: &recordingIntentPlanner{name: "openai-compat"},
		SkipSignals:   true,
	})
	if !errors.Is(err, ErrDaemonLockHeld) {
		t.Fatalf("Run returned %v want ErrDaemonLockHeld", err)
	}
	after, ok, err := state.MetaGet(context.Background(), f.db, MetaKeyIntentPlannerHealth)
	if err != nil || !ok {
		t.Fatalf("read intent planner health after contention: ok=%v err=%v", ok, err)
	}
	if after != before {
		t.Fatalf("intent planner health changed under lock contention\nbefore=%s\nafter=%s", before, after)
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
		BaseHead: "old-base", Operation: "create", Path: "old.txt",
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
		BaseHead: "fresh-base", Operation: "create", Path: "fresh.txt",
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

	n, err := PruneCaptureEvents(ctx, f.dir, f.db, time.Now(), 1*time.Second)
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

func TestPruneCaptureEvents_PreservesUnprotectedTerminalRows(t *testing.T) {
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
	freshTS := float64(time.Now().Add(time.Hour).UnixNano()) / 1e9
	freshFailed := appendEvent("fresh-failed.txt", "refs/heads/fresh", state.EventStateFailed, freshTS)

	n, err := PruneCaptureEvents(ctx, f.dir, f.db, time.Now(), 1*time.Second)
	if err != nil {
		t.Fatalf("PruneCaptureEvents: %v", err)
	}
	if n != 0 {
		t.Fatalf("pruned=%d want 0 without durable recovery refs", n)
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
	for _, seq := range []int64{oldBlocked, oldFailed, barrier, pendingBehindBarrier, freshFailed} {
		if !remaining[seq] {
			t.Fatalf("seq %d should remain; remaining=%v", seq, remaining)
		}
	}
}

func TestPruneCaptureEvents_VerifiesSnapshotGitRefsForEveryOutcome(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	type snapshotCase struct {
		name       string
		state      string
		ref        string
		commitOID  string
		createRef  bool
		wantRemain bool
	}
	cases := []snapshotCase{
		{name: "valid-recovered", state: state.EventStateRecovered, ref: "refs/acd/recovery/prune-valid-recovered", commitOID: head, createRef: true},
		{name: "missing-recovered", state: state.EventStateRecovered, ref: "refs/acd/recovery/prune-missing-recovered", commitOID: head, wantRemain: true},
		{name: "corrupt-recovered", state: state.EventStateRecovered, ref: "refs/acd/recovery/prune-corrupt-recovered", commitOID: "deadbeef", createRef: true, wantRemain: true},
		{name: "valid-published", state: state.EventStatePublished, ref: "refs/acd/recovery/prune-valid-published", commitOID: head, createRef: true},
		{name: "missing-published", state: state.EventStatePublished, ref: "refs/acd/recovery/prune-missing-published", commitOID: head, wantRemain: true},
		{name: "corrupt-published", state: state.EventStatePublished, ref: "refs/acd/recovery/prune-corrupt-published", commitOID: "deadbeef", createRef: true, wantRemain: true},
	}
	seqs := make(map[string]int64, len(cases))
	for _, tc := range cases {
		seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
			Operation: "create", Path: tc.name + ".txt", Fidelity: "full",
			CapturedTS: 1, State: tc.state,
		}, []state.CaptureOp{{Op: "create", Path: tc.name + ".txt", Fidelity: "full"}})
		if err != nil {
			t.Fatalf("append %s: %v", tc.name, err)
		}
		seqs[tc.name] = seq
		res, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO recovery_snapshots(
    created_ts, outcome, branch_ref, branch_generation,
    first_event_seq, last_event_seq, event_count,
    commit_oid, recovery_ref, reason
) VALUES (2, ?, 'refs/heads/main', 1, ?, ?, 1, ?, ?, 'prune test')`,
			tc.state, seq, seq, tc.commitOID, tc.ref)
		if err != nil {
			t.Fatalf("insert %s snapshot: %v", tc.name, err)
		}
		snapshotID, _ := res.LastInsertId()
		if _, err := f.db.SQL().ExecContext(ctx,
			`INSERT INTO recovery_snapshot_events(snapshot_id, ord, event_seq) VALUES (?, 0, ?)`,
			snapshotID, seq); err != nil {
			t.Fatalf("insert %s membership: %v", tc.name, err)
		}
		if tc.createRef {
			if err := git.UpdateRef(ctx, f.dir, tc.ref, head, ""); err != nil {
				t.Fatalf("create %s ref: %v", tc.name, err)
			}
		}
	}

	n, err := PruneCaptureEvents(ctx, f.dir, f.db, time.Now(), time.Second)
	if err != nil {
		t.Fatalf("PruneCaptureEvents: %v", err)
	}
	if n != 2 {
		t.Fatalf("pruned=%d want valid recovered and published rows", n)
	}
	for _, tc := range cases {
		var count int
		if err := f.db.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, seqs[tc.name]).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", tc.name, err)
		}
		want := 0
		if tc.wantRemain {
			want = 1
		}
		if count != want {
			t.Fatalf("%s event count=%d want %d", tc.name, count, want)
		}
	}
	var validMembership int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM recovery_snapshot_events WHERE event_seq = ?`, seqs["valid-recovered"]).Scan(&validMembership); err != nil {
		t.Fatalf("count valid membership: %v", err)
	}
	if validMembership != 1 {
		t.Fatalf("valid snapshot membership=%d want 1", validMembership)
	}
}

func TestPruneCaptureEvents_ReportsUnexpectedGitErrors(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "create", Path: "retained-on-git-error.txt", Fidelity: "full",
		CapturedTS: 1, State: state.EventStateRecovered,
	}, []state.CaptureOp{{Op: "create", Path: "retained-on-git-error.txt", Fidelity: "full"}})
	if err != nil {
		t.Fatalf("append recovered event: %v", err)
	}
	res, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO recovery_snapshots(
    created_ts, outcome, branch_ref, branch_generation,
    first_event_seq, last_event_seq, event_count,
    commit_oid, recovery_ref, reason
) VALUES (2, 'recovered', 'refs/heads/main', 1, ?, ?, 1, ?,
          'refs/acd/recovery/git-error', 'git error test')`, seq, seq, head)
	if err != nil {
		t.Fatalf("insert recovery snapshot: %v", err)
	}
	snapshotID, _ := res.LastInsertId()
	if _, err := f.db.SQL().ExecContext(ctx,
		`INSERT INTO recovery_snapshot_events(snapshot_id, ord, event_seq) VALUES (?, 0, ?)`,
		snapshotID, seq); err != nil {
		t.Fatalf("insert recovery membership: %v", err)
	}

	_, err = PruneCaptureEvents(ctx, filepath.Join(f.dir, "not-a-repository"), f.db, time.Now(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "verify protected snapshot") {
		t.Fatalf("PruneCaptureEvents err=%v want surfaced Git failure", err)
	}
	var count int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, seq).Scan(&count); err != nil {
		t.Fatalf("count recovered event: %v", err)
	}
	if count != 1 {
		t.Fatalf("recovered event count=%d want 1 after Git failure", count)
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

// hangingCloser blocks Close indefinitely until release is closed. Used
// to prove daemon shutdown is bounded by providerCloseTimeout even when
// the AI provider's Close hangs.
type hangingCloser struct {
	released chan struct{}
	calls    atomic.Int64
}

func (c *hangingCloser) Close() error {
	c.calls.Add(1)
	<-c.released
	return nil
}

// TestRun_ShutdownCompletesWithin5sUnderHungProvider proves that a
// wedged AI provider Close cannot stall daemon shutdown past
// providerCloseTimeout (5s) plus a small slack budget. Pre-fix,
// closeProviderOnce called Close synchronously with no deadline, so a
// subprocess plugin that hangs on Close would wedge the daemon.
func TestRun_ShutdownCompletesWithin5sUnderHungProvider(t *testing.T) {
	runBoundedParallel(t)

	if providerCloseTimeout != 5*time.Second {
		t.Fatalf("providerCloseTimeout=%v want 5s", providerCloseTimeout)
	}
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	stub := &stubProvider{subject: "feat: stub-injected subject"}
	closer := &hangingCloser{released: make(chan struct{})}
	t.Cleanup(func() { close(closer.released) })

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
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
			providerCloseTimeout:  100 * time.Millisecond,
		})
	}()

	// Give Run a beat to install the closer.
	time.Sleep(100 * time.Millisecond)

	cancel()
	start := time.Now()
	select {
	case <-runDone:
		elapsed := time.Since(start)
		// The production timeout is asserted above; use a shorter injected
		// budget here so the same timeout path stays fast under -race.
		if elapsed > 2*time.Second {
			t.Fatalf("Run shutdown took %v with hung provider closer; want <= %v",
				elapsed, 2*time.Second)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not exit on cancel within 5s — hung provider regressed shutdown bound")
	}

	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("hangingCloser.Close calls=%d want 1", got)
	}
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

func TestRun_ExternalFastForwardReseedsShadowWithoutCapturingUpstream(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var runErr error
	checkHook, checkEntered, releaseCheck := oneShotBranchTokenCheckGate()
	defer releaseCheck()
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
			RepoPath:               f.dir,
			GitDir:                 f.gitDir,
			DB:                     f.db,
			Scheduler:              fastScheduler(),
			BootGrace:              30 * time.Second,
			WakeCh:                 wakeCh,
			ShutdownCh:             shutdownCh,
			SkipSignals:            true,
			MessageFn:              DeterministicMessage,
			beforeBranchTokenCheck: checkHook,
		})
	}()

	waitForMetaValue(t, f.db, MetaKeyBranchHead, seedHead, 3*time.Second)
	waitForBranchTokenCheckGate(t, checkEntered)

	upstreamBody := []byte("from upstream\n")
	upstreamBlob, err := git.HashObjectStdin(ctx, f.dir, upstreamBody)
	if err != nil {
		t.Fatalf("hash upstream blob: %v", err)
	}
	seedEntries, err := git.LsTree(ctx, f.dir, seedHead, false)
	if err != nil {
		t.Fatalf("ls-tree seed: %v", err)
	}
	mkEntries := make([]git.MktreeEntry, 0, len(seedEntries)+1)
	for _, e := range seedEntries {
		mkEntries = append(mkEntries, git.MktreeEntry{Mode: e.Mode, Type: e.Type, OID: e.OID, Path: e.Path})
	}
	mkEntries = append(mkEntries, git.MktreeEntry{Mode: git.RegularFileMode, Type: "blob", OID: upstreamBlob, Path: "upstream.txt"})
	upstreamTree, err := git.Mktree(ctx, f.dir, mkEntries)
	if err != nil {
		t.Fatalf("mktree upstream: %v", err)
	}
	upstreamHead, err := git.CommitTree(ctx, f.dir, upstreamTree, "upstream fast-forward", seedHead)
	if err != nil {
		t.Fatalf("commit-tree upstream: %v", err)
	}

	if err := git.UpdateRef(ctx, f.dir, "refs/heads/main", upstreamHead, seedHead); err != nil {
		t.Fatalf("update-ref upstream: %v", err)
	}
	// Sync worktree from the fast-forward commit. Writing the file by hand
	// after UpdateRef races fsnotify on Linux CI and produces a spurious
	// create capture even though shadow reseed should satisfy HEAD.
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "checkout", "-q", upstreamHead, "--", "upstream.txt"); err != nil {
		t.Fatalf("checkout upstream worktree: %v", err)
	}
	releaseCheck()
	for i := 0; i < 4; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}

	waitForMetaValue(t, f.db, MetaKeyBranchHead, upstreamHead, 3*time.Second)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var oid string
		err := f.db.SQL().QueryRowContext(ctx,
			`SELECT oid FROM shadow_paths
			 WHERE branch_ref = ? AND branch_generation = ? AND path = ?`,
			"refs/heads/main", int64(1), "upstream.txt").Scan(&oid)
		if err == nil && oid == upstreamBlob {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var shadowOID string
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT oid FROM shadow_paths
		 WHERE branch_ref = ? AND branch_generation = ? AND path = ?`,
		"refs/heads/main", int64(1), "upstream.txt").Scan(&shadowOID); err != nil {
		t.Fatalf("upstream.txt not reseeded into shadow: %v", err)
	}
	if shadowOID != upstreamBlob {
		t.Fatalf("shadow upstream oid=%s want %s", shadowOID, upstreamBlob)
	}

	waitForCaptureEventCount(t, f.db, 0, 3*time.Second)

	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run returned %v", runErr)
	}
}

func TestRun_FastForwardRollbackInvalidatesProspectiveShadow(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()
	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}
	upstreamBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("upstream\n"))
	if err != nil {
		t.Fatalf("hash upstream: %v", err)
	}
	seedEntries, err := git.LsTree(ctx, f.dir, seedHead, false)
	if err != nil {
		t.Fatalf("ls-tree seed: %v", err)
	}
	entries := make([]git.MktreeEntry, 0, len(seedEntries)+1)
	for _, entry := range seedEntries {
		entries = append(entries, git.MktreeEntry{Mode: entry.Mode, Type: entry.Type, OID: entry.OID, Path: entry.Path})
	}
	entries = append(entries, git.MktreeEntry{Mode: git.RegularFileMode, Type: "blob", OID: upstreamBlob, Path: "upstream.txt"})
	upstreamTree, err := git.Mktree(ctx, f.dir, entries)
	if err != nil {
		t.Fatalf("mktree upstream: %v", err)
	}
	upstreamHead, err := git.CommitTree(ctx, f.dir, upstreamTree, "upstream", seedHead)
	if err != nil {
		t.Fatalf("commit-tree upstream: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	hookDone := make(chan error, 1)
	rollbackDone := make(chan struct{}, 1)
	var hookOnce sync.Once
	var rollbackOnce sync.Once
	trace := &memoryTraceLogger{}
	var wg sync.WaitGroup
	checkHook, checkEntered, releaseCheck := oneShotBranchTokenCheckGate()
	defer releaseCheck()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(runCtx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			WakeCh: wakeCh, ShutdownCh: make(chan struct{}), SkipSignals: true,
			Trace:                  trace,
			beforeBranchTokenCheck: checkHook,
			beforeBranchTransitionAccept: func() {
				hookOnce.Do(func() {
					if err := git.UpdateRef(ctx, f.dir, "refs/heads/main", seedHead, upstreamHead); err != nil {
						hookDone <- err
						return
					}
					_, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "reset", "--hard", seedHead)
					hookDone <- err
				})
			},
			afterBranchTransitionRollback: func() {
				rollbackOnce.Do(func() { rollbackDone <- struct{}{} })
			},
		})
	}()
	t.Cleanup(func() { cancel(); wg.Wait() })
	wantToken := branchTokenRev(seedHead, "refs/heads/main")
	waitForMetaValue(t, f.db, MetaKeyBranchToken, wantToken, 5*time.Second)
	waitForBranchTokenCheckGate(t, checkEntered)
	if err := git.UpdateRef(ctx, f.dir, "refs/heads/main", upstreamHead, seedHead); err != nil {
		t.Fatalf("fast-forward main: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "reset", "--hard", upstreamHead); err != nil {
		t.Fatalf("reset worktree upstream: %v", err)
	}
	releaseCheck()
	wakeCh <- struct{}{}
	select {
	case err := <-hookDone:
		if err != nil {
			t.Fatalf("rollback hook: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("branch transition hook did not run")
	}
	select {
	case <-rollbackDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("branch transition rollback did not finish")
	}
	transitionEvents := traceEventsByClass(trace.Events(), "branch_token.transition")
	rolledBack := 0
	for _, event := range transitionEvents {
		switch event.Decision {
		case "rolled_back":
			rolledBack++
		case TokenTransitionFastForward.String(), TokenTransitionDiverged.String():
			t.Fatalf("trace reported unaccepted transition as successful: %+v", event)
		}
	}
	if rolledBack != 1 {
		t.Fatalf("rolled_back traces=%d want 1; events=%+v", rolledBack, transitionEvents)
	}
	wakeCh <- struct{}{}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		bootstrapped, _ := IsShadowBootstrapped(ctx, f.db, "refs/heads/main", 1)
		var upstreamRows int
		_ = f.db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM shadow_paths
WHERE branch_ref = 'refs/heads/main' AND branch_generation = 1 AND path = 'upstream.txt'`).Scan(&upstreamRows)
		if bootstrapped && upstreamRows == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if bootstrapped, err := IsShadowBootstrapped(ctx, f.db, "refs/heads/main", 1); err != nil || !bootstrapped {
		t.Fatalf("old shadow not re-established: bootstrapped=%v err=%v", bootstrapped, err)
	}
	var upstreamRows int
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM shadow_paths
WHERE branch_ref = 'refs/heads/main' AND branch_generation = 1 AND path = 'upstream.txt'`).Scan(&upstreamRows); err != nil {
		t.Fatalf("count upstream shadow: %v", err)
	}
	if upstreamRows != 0 {
		t.Fatalf("prospective upstream shadow rows=%d want 0 after rollback", upstreamRows)
	}
}

func TestRun_BranchRollbackPreservesOldShadowAtZeroRetention(t *testing.T) {
	t.Setenv(EnvShadowRetentionGenerations, "0")
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()
	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}
	if err := git.UpdateRef(ctx, f.dir, "refs/heads/feature", seedHead, ""); err != nil {
		t.Fatalf("create feature: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	hookDone := make(chan error, 1)
	var hookOnce sync.Once
	var wg sync.WaitGroup
	checkHook, checkEntered, releaseCheck := oneShotBranchTokenCheckGate()
	defer releaseCheck()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(runCtx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			WakeCh: wakeCh, ShutdownCh: make(chan struct{}), SkipSignals: true,
			beforeBranchTokenCheck: checkHook,
			beforeBranchTransitionAccept: func() {
				hookOnce.Do(func() {
					_, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", "refs/heads/main")
					hookDone <- err
				})
			},
		})
	}()
	t.Cleanup(func() { cancel(); wg.Wait() })
	waitForMetaValue(t, f.db, MetaKeyBranchToken,
		branchTokenRev(seedHead, "refs/heads/main"), 5*time.Second)
	waitForBranchTokenCheckGate(t, checkEntered)
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", "refs/heads/feature"); err != nil {
		t.Fatalf("switch symbolic ref: %v", err)
	}
	releaseCheck()
	wakeCh <- struct{}{}
	select {
	case err := <-hookDone:
		if err != nil {
			t.Fatalf("rollback hook: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("branch transition hook did not run")
	}
	wakeCh <- struct{}{}
	time.Sleep(300 * time.Millisecond)
	gen, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
	if gen != "1" {
		t.Fatalf("branch generation=%q want 1 after rollback", gen)
	}
	if bootstrapped, err := IsShadowBootstrapped(ctx, f.db, "refs/heads/main", 1); err != nil || !bootstrapped {
		t.Fatalf("old shadow marker lost: bootstrapped=%v err=%v", bootstrapped, err)
	}
	var oldRows, prospectiveRows int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = 'refs/heads/main' AND branch_generation = 1`).Scan(&oldRows); err != nil {
		t.Fatalf("count old shadow: %v", err)
	}
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = 'refs/heads/feature' AND branch_generation = 2`).Scan(&prospectiveRows); err != nil {
		t.Fatalf("count prospective shadow: %v", err)
	}
	if oldRows == 0 || prospectiveRows != 0 {
		t.Fatalf("shadow rows old=%d prospective=%d", oldRows, prospectiveRows)
	}
}

// TestRun_RuntimeDivergedRecoversDeadBranchTerminals exercises the runtime
// Diverged-hook end-to-end: the daemon boots on refs/heads/feat-x (created
// + checked out before the run loop starts), accumulates a blocked_conflict
// + pending capture_events row tied to that ref, the worktree is then
// switched back to refs/heads/main AND refs/heads/feat-x deleted from
// underneath the daemon, and on the next wake the runtime Diverged path
// prunes the dead-branch rows. This is the regression-against-"P2 #7"
// coverage gap (the prior dead_branch_sweep_test.go test invoked the helper
// directly rather than driving the run loop into the runtime Diverged path).
func TestRun_RuntimeDivergedRecoversDeadBranchTerminals(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	// Create refs/heads/feat-x at HEAD and check it out so the daemon's
	// boot sees feat-x as the active branch.
	if err := git.UpdateRef(ctx, f.dir, "refs/heads/feat-x", seedHead, ""); err != nil {
		t.Fatalf("update-ref refs/heads/feat-x: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", "refs/heads/feat-x"); err != nil {
		t.Fatalf("symbolic-ref to feat-x: %v", err)
	}

	// Pre-seed the rows tied to feat-x at generation 1. Boot will load
	// generation 1 from daemon_meta as well (the very first tick's
	// SaveBranchGeneration writes 1).
	seedTerminalEvent(t, f.db, "refs/heads/feat-x", 1, seedHead, "feat-x-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, "refs/heads/feat-x", 1, seedHead, "feat-x-pending.txt", state.EventStatePending)

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	checkHook, checkEntered, releaseCheck := oneShotBranchTokenCheckGate()
	defer releaseCheck()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(runCtx, Options{
			RepoPath:               f.dir,
			GitDir:                 f.gitDir,
			DB:                     f.db,
			Scheduler:              fastScheduler(),
			BootGrace:              30 * time.Second,
			WakeCh:                 wakeCh,
			ShutdownCh:             shutdownCh,
			SkipSignals:            true,
			beforeBranchTokenCheck: checkHook,
		})
	}()

	// Wait for the daemon to publish the running mode with branch.head ==
	// seedHead AND the in-memory token pointing at feat-x. branch.token
	// is the closure variable observable through MetaKeyBranchToken meta.
	want := "rev:" + seedHead + " refs/heads/feat-x"
	waitForMetaValue(t, f.db, MetaKeyBranchToken, want, 5*time.Second)
	waitForBranchTokenCheckGate(t, checkEntered)

	// Now switch the worktree back to refs/heads/main and delete feat-x.
	// The daemon's next tick classifies the change as Diverged (ref change
	// refs/heads/feat-x -> refs/heads/main on a divergent rev).
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref back to main: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "update-ref", "-d", "refs/heads/feat-x"); err != nil {
		t.Fatalf("delete refs/heads/feat-x: %v", err)
	}
	releaseCheck()
	for i := 0; i < 4; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for the runtime hook to stamp last_run_ts (the observable
	// side-effect of a non-empty dead-branch prune). 5s is generous; the
	// Diverged path runs synchronously inside processBranchTokenChange.
	deadline := time.Now().Add(5 * time.Second)
	var sawTS bool
	for time.Now().Before(deadline) {
		v, ok, _ := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRunTS)
		if ok && v != "" && v != "0" {
			sawTS = true
			break
		}
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawTS {
		t.Fatalf("runtime Diverged hook did not stamp %s within 5s", MetaKeyDeadBranchPruneLastRunTS)
	}

	// Both rows remain as recovered provenance under a durable recovery ref.
	var total int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE branch_ref = ?`, "refs/heads/feat-x",
	).Scan(&total); err != nil {
		t.Fatalf("count feat-x rows: %v", err)
	}
	if total != 2 {
		t.Fatalf("feat-x rows=%d want 2 retained after runtime recovery", total)
	}
	if recovered := countEventsByRefState(t, f.db, "refs/heads/feat-x", state.EventStateRecovered); recovered != 2 {
		t.Fatalf("feat-x recovered rows=%d want 2", recovered)
	}

	cancel()
	wg.Wait()
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
	staleOID, err := git.HashObjectStdin(ctx, f.dir, []byte("stale pending\n"))
	if err != nil {
		t.Fatalf("hash stale pending: %v", err)
	}
	staleSeq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
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
		AfterOID:  sql.NullString{String: staleOID, Valid: true},
	}})
	if err != nil {
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
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

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
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := f.db.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
			"refs/heads/main", int64(1)).Scan(&oldShadowRows); err != nil {
			t.Fatalf("count old shadow rows: %v", err)
		}
		if oldShadowRows == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if oldShadowRows != 0 {
		t.Fatalf("old shadow generation rows=%d want 0", oldShadowRows)
	}
	time.Sleep(100 * time.Millisecond)
	var staleState string
	var staleCommit sql.NullString
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT state, commit_oid FROM capture_events WHERE seq = ?`, staleSeq,
	).Scan(&staleState, &staleCommit); err != nil {
		t.Fatalf("query preserved stale capture: %v", err)
	}
	if staleState != state.EventStateRecovered || !staleCommit.Valid || staleCommit.String == "" {
		t.Fatalf("stale capture state=%q commit=%v, want durable recovered provenance", staleState, staleCommit)
	}
	var recoveryRef, snapshotCommit string
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT rs.recovery_ref, rs.commit_oid
FROM recovery_snapshots rs
JOIN recovery_snapshot_events rse ON rse.snapshot_id = rs.id
WHERE rse.event_seq = ?`, staleSeq).Scan(&recoveryRef, &snapshotCommit); err != nil {
		t.Fatalf("query stale capture recovery snapshot: %v", err)
	}
	if snapshotCommit != staleCommit.String {
		t.Fatalf("snapshot commit=%s want recovered commit=%s", snapshotCommit, staleCommit.String)
	}
	resolvedRecovery, err := git.RevParse(ctx, f.dir, recoveryRef)
	if err != nil {
		t.Fatalf("resolve stale capture recovery ref: %v", err)
	}
	if resolvedRecovery != snapshotCommit {
		t.Fatalf("recovery ref commit=%s want %s", resolvedRecovery, snapshotCommit)
	}
	var newEvents int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq != ?`, staleSeq,
	).Scan(&newEvents); err != nil {
		t.Fatalf("count captures created after startup: %v", err)
	}
	if newEvents != 0 {
		t.Fatalf("startup after offline reset captured %d phantom events, want 0", newEvents)
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
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))
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
	newHead := waitForCommit(t, f.dir, startHead, 15*time.Second)
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

	newHead := waitForCommit(t, f.dir, startHead, 15*time.Second)
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

// TestClearRewindGraceMeta_DeletesPersistedKey is a focused unit test for
// the helper used by the detached-HEAD reattach and operation-cleared
// transitions. Both transitions are explicit operator events; the rewind
// heuristic must NOT survive them, otherwise capture/replay stay muted up
// to ACD_REWIND_GRACE_SECONDS post-resume.
func TestClearRewindGraceMeta_DeletesPersistedKey(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	// 90s into the future stays within ClampRewindGraceAtStartup's tolerance
	// (2 * defaultRewindGrace = 120s) so the daemon's startup clamp leaves
	// our pre-set marker in place; otherwise the clamp normalizes the
	// timestamp and the test loses track of the value it stamped.
	until := time.Now().UTC().Add(90 * time.Second).Format(time.RFC3339)
	if err := state.MetaSet(ctx, f.db, MetaKeyReplayPausedUntil, until); err != nil {
		t.Fatalf("MetaSet paused_until: %v", err)
	}

	clearRewindGraceMeta(ctx, f.db, f.dir, CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
	}, nil, slog.Default(), "unit-test reattach")

	if _, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil); err != nil {
		t.Fatalf("MetaGet: %v", err)
	} else if ok {
		t.Fatalf("replay.paused_until still present after clear")
	}
}

// TestClearRewindGraceMeta_NoOpWhenAbsent ensures the helper is safe to
// call on every tick — when the marker is not set, it must not error or
// emit a trace event (the trace probe has no input to log).
func TestClearRewindGraceMeta_NoOpWhenAbsent(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	// No pre-existing key.
	clearRewindGraceMeta(ctx, f.db, f.dir, CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
	}, nil, slog.Default(), "unit-test no-op")

	if _, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil); err != nil {
		t.Fatalf("MetaGet: %v", err)
	} else if ok {
		t.Fatalf("replay.paused_until unexpectedly present")
	}
}

// TestRun_ReattachClearsStaleRewindGrace pins the regression where a
// rewind-grace marker set before a detached-HEAD transition silently
// muted capture/replay for up to ACD_REWIND_GRACE_SECONDS after the
// operator reattached HEAD. The reattach branch in the run loop must
// strip MetaKeyReplayPausedUntil so the resumed worktree is observable
// immediately.
func TestRun_ReattachClearsStaleRewindGrace(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	// Detach HEAD: persisted detached marker + stale rewind grace marker.
	headSHA, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "checkout", "--detach", headSHA); err != nil {
		t.Fatalf("checkout --detach: %v", err)
	}
	if err := state.MetaSet(ctx, f.db, MetaKeyDetachedHeadPaused, "1"); err != nil {
		t.Fatalf("MetaSet detached: %v", err)
	}
	if err := state.MetaSet(
		ctx, f.db, MetaKeyBranchToken, branchTokenRev(headSHA, "")); err != nil {
		t.Fatalf("MetaSet detached branch token: %v", err)
	}
	if err := state.MetaSet(ctx, f.db, MetaKeyBranchHead, headSHA); err != nil {
		t.Fatalf("MetaSet detached branch head: %v", err)
	}
	// 90s into the future stays within ClampRewindGraceAtStartup's tolerance
	// (2 * defaultRewindGrace = 120s) so the daemon's startup clamp leaves
	// our pre-set marker in place; otherwise the clamp normalizes the
	// timestamp and the test loses track of the value it stamped.
	until := time.Now().UTC().Add(90 * time.Second).Format(time.RFC3339)
	if err := state.MetaSet(ctx, f.db, MetaKeyReplayPausedUntil, until); err != nil {
		t.Fatalf("MetaSet paused_until: %v", err)
	}

	// Reattach before startup samples HEAD. The persisted detached token and
	// marker must still authorize exact recovery and clear the stale grace
	// only after the startup transition is accepted.
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "checkout", "main"); err != nil {
		t.Fatalf("checkout main: %v", err)
	}
	shutdownCh := make(chan struct{})
	close(shutdownCh)
	if err := Run(ctx, Options{
		RepoPath:    f.dir,
		GitDir:      f.gitDir,
		DB:          f.db,
		Scheduler:   fastScheduler(),
		BootGrace:   30 * time.Second,
		ShutdownCh:  shutdownCh,
		SkipSignals: true,
		recoverSelfPublications: func(
			ctx context.Context, _ string, db *state.DB,
			_ CaptureContext, _ ReplayOpts,
		) (SelfPublicationRecoverySummary, error) {
			// Completion persists the recovered attached publication token
			// before startup later reloads its previous token.
			if err := state.MetaSet(ctx, db, MetaKeyBranchToken,
				branchTokenRev(headSHA, "refs/heads/main")); err != nil {
				return SelfPublicationRecoverySummary{}, err
			}
			return SelfPublicationRecoverySummary{}, nil
		},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if _, ok, err := state.MetaGet(ctx, f.db, MetaKeyDetachedHeadPaused); err != nil {
		t.Fatalf("MetaGet detached marker: %v", err)
	} else if ok {
		t.Fatal("detached marker was not cleared")
	}
	if _, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil); err != nil {
		t.Fatalf("MetaGet rewind grace: %v", err)
	} else if ok {
		t.Fatal("stale detached rewind grace was not cleared")
	}
}

func TestRun_StartupStaleDetachedMarkerPreservesAttachedRewindGrace(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	headSHA, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if err := state.MetaSetMany(ctx, f.db, map[string]string{
		MetaKeyBranchToken:        branchTokenRev(headSHA, "refs/heads/main"),
		MetaKeyBranchHead:         headSHA,
		MetaKeyDetachedHeadPaused: "stale",
		MetaKeyReplayPausedUntil: time.Now().UTC().
			Add(90 * time.Second).Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("MetaSetMany: %v", err)
	}

	shutdownCh := make(chan struct{})
	close(shutdownCh)
	recoveryCalled := false
	if err := Run(ctx, Options{
		RepoPath:    f.dir,
		GitDir:      f.gitDir,
		DB:          f.db,
		BootGrace:   time.Hour,
		ShutdownCh:  shutdownCh,
		SkipSignals: true,
		recoverSelfPublications: func(
			context.Context, string, *state.DB, CaptureContext, ReplayOpts,
		) (SelfPublicationRecoverySummary, error) {
			recoveryCalled = true
			return SelfPublicationRecoverySummary{}, nil
		},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if recoveryCalled {
		t.Fatal("startup recovery bypassed a genuine attached rewind grace")
	}
	if _, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil); err != nil {
		t.Fatalf("MetaGet rewind grace: %v", err)
	} else if !ok {
		t.Fatal("genuine attached rewind grace was cleared")
	}
	if _, ok, err := state.MetaGet(ctx, f.db, MetaKeyDetachedHeadPaused); err != nil {
		t.Fatalf("MetaGet detached marker: %v", err)
	} else if ok {
		t.Fatal("stale detached marker was not cleared")
	}
}

func TestRun_PublicationHoldSkipsStartupRecovery(t *testing.T) {
	f := newDaemonFixture(t)
	shutdownCh := make(chan struct{})
	close(shutdownCh)
	recoveryCalled := false
	if err := Run(context.Background(), Options{
		RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
		BootGrace: time.Hour, ShutdownCh: shutdownCh, SkipSignals: true,
		PublicationHeld: func() bool { return true },
		recoverSelfPublications: func(
			context.Context, string, *state.DB, CaptureContext, ReplayOpts,
		) (SelfPublicationRecoverySummary, error) {
			recoveryCalled = true
			return SelfPublicationRecoverySummary{}, nil
		},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if recoveryCalled {
		t.Fatal("startup recovery ran while publication was held")
	}
}

func TestRun_PublicationHoldBlocksReplayUnderOperationGate(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	cctx := CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head}
	if _, err := BootstrapShadow(ctx, f.dir, f.db, cctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "held.txt"), []byte("held\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	checker := git.NewIgnoreChecker(f.dir)
	store := checkpointpkg.Store{DB: f.db}
	summary, err := Capture(ctx, f.dir, f.db, cctx, CaptureOpts{
		IgnoreChecker: checker, SensitiveMatcher: state.NewSensitiveMatcher(),
		CheckpointStore: &store, WorktreeID: checkpointpkg.WorktreeID(f.dir),
	})
	_ = checker.Close()
	if err != nil || summary.EventsAppended != 1 || summary.CheckpointID == "" {
		t.Fatalf("capture summary=%+v err=%v", summary, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending events=%d err=%v", len(pending), err)
	}

	shutdownCh := make(chan struct{})
	var stopOnce sync.Once
	gate := &sync.RWMutex{}
	if err := Run(ctx, Options{
		RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
		Scheduler: fastScheduler(), BootGrace: time.Hour,
		ShutdownCh: shutdownCh, SkipSignals: true,
		OperationGate: gate, PublicationHeld: func() bool { return true },
		afterRunLoopWorkDecision: func(_, _ bool) {
			stopOnce.Do(func() { close(shutdownCh) })
		},
	}); err != nil {
		t.Fatal(err)
	}
	afterHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if afterHead != head {
		t.Fatalf("publication hold advanced HEAD from %s to %s", head, afterHead)
	}
	var eventState string
	if err := f.db.ReadSQL().QueryRowContext(ctx,
		"SELECT state FROM capture_events WHERE seq=?", pending[0].Seq).Scan(&eventState); err != nil {
		t.Fatal(err)
	}
	if eventState != state.EventStatePending {
		t.Fatalf("held event state=%q want pending", eventState)
	}
}

// TestRun_OperationClearedClearsStaleRewindGrace asserts the symmetric
// behavior for the operation-in-progress marker (rebase / merge /
// cherry-pick / bisect). When the marker disappears the run loop's
// resume path must strip a co-existing rewind-grace marker.
func TestRun_OperationClearedClearsStaleRewindGrace(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	// Simulate a stale rebase-merge marker so the run loop sees an
	// operation-in-progress on its first tick. Pre-stamp the persisted
	// marker rows so the operation-cleared branch will fire on the next
	// tick once we remove the on-disk marker.
	rebaseDir := filepath.Join(f.gitDir, "rebase-merge")
	if err := os.MkdirAll(rebaseDir, 0o755); err != nil {
		t.Fatalf("mkdir rebase-merge: %v", err)
	}
	// 90s into the future stays within ClampRewindGraceAtStartup's tolerance
	// (2 * defaultRewindGrace = 120s) so the daemon's startup clamp leaves
	// our pre-set marker in place; otherwise the clamp normalizes the
	// timestamp and the test loses track of the value it stamped.
	until := time.Now().UTC().Add(90 * time.Second).Format(time.RFC3339)
	if err := state.MetaSet(ctx, f.db, MetaKeyReplayPausedUntil, until); err != nil {
		t.Fatalf("MetaSet paused_until: %v", err)
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
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	// First wake will record the in-progress marker.
	wakeCh <- struct{}{}
	waitForMetaValue(t, f.db, MetaKeyOperationInProgress, "rebase-merge", 3*time.Second)
	// Rewind grace marker must still be present while paused.
	if got, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil); err != nil {
		t.Fatalf("MetaGet during pause: %v", err)
	} else if !ok || got != until {
		t.Fatalf("replay.paused_until=(%q,%v) want (%q,true)", got, ok, until)
	}

	// Clear the operation marker — the next tick must enter the
	// operation-cleared branch and strip both daemon_meta keys.
	if err := os.RemoveAll(rebaseDir); err != nil {
		t.Fatalf("remove rebase-merge: %v", err)
	}
	wakeCh <- struct{}{}

	waitForMetaDeleted(t, f.db, MetaKeyOperationInProgress, 3*time.Second)
	waitForMetaDeleted(t, f.db, MetaKeyReplayPausedUntil, 3*time.Second)
}

// TestRun_SameSHARewindAcrossTicksTriggersGrace pins the regression where
// a same-SHA cross-tick rewind (operator does `git reset --hard HEAD~1`
// followed by `git reset --hard ORIG_HEAD` between two daemon ticks)
// slipped past processBranchTokenChange because SameGeneration short-
// circuits on identical token strings. The daemon now stamps
// MetaKeyBranchHead on every tick; if the persisted value diverges from
// both the in-memory token's SHA and the freshly-read live HEAD AND
// IsAncestor(liveHEAD, persistedHEAD) reports backward motion, the
// run loop classifies the move as a same-SHA rewind and arms
// MetaKeyReplayPausedUntil via maybeSetRewindGrace.
//
// The test boots the daemon at seedHead, lets it stamp persisted = seed
// during the first SameGeneration tick, then simulates an out-of-band
// observer (e.g. acd recover or another tool) overwriting persisted to
// `advanced` while the in-memory currentToken still points at seedHead.
// On the next wake, the cross-tick probe must observe persisted !=
// currentToken's SHA != liveHead, run IsAncestor(seedHead, advanced) =
// true, and arm the grace marker.
func TestRun_SameSHARewindAcrossTicksTriggersGrace(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}
	// Build a child commit on top of seed; this represents the "advanced"
	// HEAD an out-of-band observer recorded into MetaKeyBranchHead between
	// ticks before the operator reset back to seedHead. Live HEAD remains
	// at seedHead — commitSingleFile only writes the commit object.
	advanced := commitSingleFile(t, ctx, f.dir, seedHead, "advanced.txt", "ahead\n", "advanced")
	if h, _ := git.RevParse(ctx, f.dir, "HEAD"); h != seedHead {
		t.Fatalf("HEAD moved to %s want %s", h, seedHead)
	}
	// IsAncestor(seedHead, advanced) must be true (seedHead is parent).
	ok, err := git.IsAncestor(ctx, f.dir, seedHead, advanced)
	if err != nil {
		t.Fatalf("IsAncestor: %v", err)
	}
	if !ok {
		t.Fatal("ancestry probe expected seedHead to be ancestor of advanced")
	}
	advancedOID, err := git.LsTreeBlobOID(ctx, f.dir, advanced, "advanced.txt")
	if err != nil {
		t.Fatalf("resolve advanced.txt blob: %v", err)
	}
	if err := state.UpsertShadowPath(ctx, f.db, state.ShadowPath{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Path: "advanced.txt", Operation: "modify",
		Mode:     sql.NullString{String: "100644", Valid: true},
		OID:      sql.NullString{String: advancedOID, Valid: true},
		BaseHead: advanced, Fidelity: "full",
	}); err != nil {
		t.Fatalf("seed stale published shadow row: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	manual := Scheduler{
		Base:         1 * time.Hour,
		IdleCeiling:  1 * time.Hour,
		ErrorCeiling: 1 * time.Hour,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	checkHook, checkEntered, releaseCheck := oneShotBranchTokenCheckGate()
	defer releaseCheck()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(runCtx, Options{
			RepoPath:               f.dir,
			GitDir:                 f.gitDir,
			DB:                     f.db,
			Scheduler:              manual,
			BootGrace:              30 * time.Second,
			WakeCh:                 wakeCh,
			ShutdownCh:             shutdownCh,
			SkipSignals:            true,
			beforeBranchTokenCheck: checkHook,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	// Wait until startup has settled — persisted MetaKeyBranchHead =
	// seedHead, currentToken in-memory = "rev:seedHead refs/heads/main".
	waitForMetaValue(t, f.db, MetaKeyBranchHead, seedHead, 2*time.Second)
	waitForMetaValue(t, f.db, MetaKeyBranchToken,
		"rev:"+seedHead+" refs/heads/main", 2*time.Second)
	waitForBranchTokenCheckGate(t, checkEntered)

	// Simulate an out-of-band observer (acd recover, manual sqlite edit,
	// or a previous-tick observation that has since been lost from
	// in-memory currentToken) overwriting persisted to the advanced
	// commit. Live HEAD is still seedHead; in-memory currentToken is
	// still seedHead; only persisted differs.
	if err := state.MetaSet(ctx, f.db, MetaKeyBranchHead, advanced); err != nil {
		t.Fatalf("MetaSet branch.head=advanced: %v", err)
	}
	// Release the gated token check. processBranchTokenChange should observe
	// SameGeneration (live token == currentToken), enter the cross-tick
	// probe, and arm the grace marker.
	releaseCheck()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil)
		if err != nil {
			t.Fatalf("MetaGet paused_until: %v", err)
		}
		if ok && got != "" {
			waitForMetaValue(t, f.db, MetaKeyBranchHead, seedHead, 2*time.Second)
			if _, found, shadowErr := state.GetShadowPath(ctx, f.db,
				"refs/heads/main", 1, "advanced.txt"); shadowErr != nil {
				t.Fatalf("read advanced.txt shadow after rewind: %v", shadowErr)
			} else if found {
				t.Fatal("cross-tick rewind retained shadow content from the abandoned published head")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replay.paused_until never set after cross-tick same-SHA rewind")
}

// loggedRecord is a single slog record snapshot captured by captureLogHandler.
type loggedRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

// captureLogHandler is a minimal slog.Handler that copies records into an
// in-memory buffer for assertions. It honors only Level and the textual
// Message + flat Attr key/value pairs; Group/WithAttrs nesting is rare in
// daemon logging and out of scope for the tests that use this handler.
type captureLogHandler struct {
	mu      sync.Mutex
	level   slog.Level
	records []loggedRecord
	root    *captureLogHandler
	attrs   []slog.Attr
}

func (h *captureLogHandler) Enabled(_ context.Context, lvl slog.Level) bool {
	return lvl >= h.level
}

func (h *captureLogHandler) Handle(_ context.Context, r slog.Record) error {
	rec := loggedRecord{
		Level:   r.Level,
		Message: r.Message,
		Attrs:   make(map[string]any, r.NumAttrs()),
	}
	for _, attr := range h.attrs {
		rec.Attrs[attr.Key] = attr.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.Attrs[a.Key] = a.Value.Any()
		return true
	})
	root := h.sharedRoot()
	root.mu.Lock()
	root.records = append(root.records, rec)
	root.mu.Unlock()
	return nil
}

func (h *captureLogHandler) sharedRoot() *captureLogHandler {
	if h.root != nil {
		return h.root
	}
	return h
}

func (h *captureLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	combined := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	combined = append(combined, h.attrs...)
	combined = append(combined, attrs...)
	return &captureLogHandler{
		level: h.level,
		root:  h.sharedRoot(),
		attrs: combined,
	}
}
func (h *captureLogHandler) WithGroup(_ string) slog.Handler { return h }

// Records returns a snapshot copy of the captured log records.
func (h *captureLogHandler) Records() []loggedRecord {
	root := h.sharedRoot()
	root.mu.Lock()
	defer root.mu.Unlock()
	out := make([]loggedRecord, len(root.records))
	copy(out, root.records)
	return out
}

func recordsByMessage(records []loggedRecord, message string) []loggedRecord {
	out := make([]loggedRecord, 0, 1)
	for _, record := range records {
		if record.Message == message {
			out = append(out, record)
		}
	}
	return out
}

func countLogMessage(records []loggedRecord, message string) int {
	return len(recordsByMessage(records, message))
}

// countFlushByStatus counts flush_requests rows in the given status.
func countFlushByStatus(t *testing.T, db *state.DB, status string) int {
	t.Helper()
	var n int
	if err := db.SQL().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM flush_requests WHERE status = ?`, status,
	).Scan(&n); err != nil {
		t.Fatalf("count flush_requests status=%s: %v", status, err)
	}
	return n
}

// TestRun_PostFlushBranchTokenReCheck pins the post-flush branch-token
// re-check restored after a code review found the original "HEAD cannot
// move between the two calls without a wake" assumption was false. git
// reset/rebase/checkout move HEAD without touching wakeCh; if the daemon
// only checks the token before the flush drain, capture/replay can run
// with a stale BranchRef/BaseHead/generation after a long drain that ran
// concurrently with operator git surgery.
//
// We exercise the guard deterministically by:
//  1. Booting the daemon with a manual-only scheduler (no auto-ticks).
//  2. Pre-creating a second commit on disk so HEAD is ahead of the seed.
//  3. Resetting the worktree HEAD back to the seed, then forward, in a
//     way that bumps the branch generation between the pre-flush and
//     post-flush token checks.
//  4. Waking once and asserting the post-flush re-check observed the
//     transition: branch.generation incremented and capture/replay were
//     skipped that iteration (no commits produced).
func TestRun_PostFlushBranchTokenReCheck(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	manual := Scheduler{
		Base:         1 * time.Hour,
		IdleCeiling:  1 * time.Hour,
		ErrorCeiling: 1 * time.Hour,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   manual,
			BootGrace:   30 * time.Second,
			MessageFn:   DeterministicMessage,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	waitForMetaValue(t, f.db, MetaKeyBranchHead, seedHead, 2*time.Second)

	// Force a divergence on disk in two deterministic phases:
	//
	//   Phase 1: create a second commit (HEAD -> aheadHead) and wake the
	//   daemon so its in-memory currentToken advances to rev:<aheadHead>.
	//   Without this synchronization the boot iteration can race with the
	//   commit on slow runners (Linux CI under -race + GOMAXPROCS pressure):
	//   if the boot iteration finishes before the commit lands, the daemon
	//   never observes aheadHead, and the subsequent reset back to seedHead
	//   leaves currentToken byte-identical to the live token (both
	//   rev:<seedHead>) so no transition is classified.
	//
	//   Phase 2: reset HEAD back to seedHead and wake. Now currentToken is
	//   rev:<aheadHead> while the live token resolves to rev:<seedHead>;
	//   the iteration's branch-token check (pre- or post-flush) classifies
	//   the move as Diverged and bumps the generation.
	if err := os.WriteFile(filepath.Join(f.dir, "ahead.txt"), []byte("ahead\n"), 0o644); err != nil {
		t.Fatalf("write ahead.txt: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "ahead.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "ahead"); err != nil {
		t.Fatalf("git commit: %v", err)
	}
	aheadHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse ahead: %v", err)
	}
	if aheadHead == seedHead {
		t.Fatalf("test setup: ahead commit did not advance HEAD")
	}

	// Phase 1: wake so the daemon classifies the fast-forward and persists
	// branch.head=aheadHead. We wait on the persisted value rather than a
	// fixed sleep so this is robust to slow runners.
	wakeCh <- struct{}{}
	waitForMetaValue(t, f.db, MetaKeyBranchHead, aheadHead, 3*time.Second)

	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "reset", "--hard", seedHead); err != nil {
		t.Fatalf("git reset: %v", err)
	}

	// Phase 2: one wake; manual scheduler guarantees this is the only
	// iteration that fires after the divergence is in place.
	wakeCh <- struct{}{}

	// Wait for the generation to bump. The guard catching the transition
	// is what produces the bump; without it, branch.generation would stay
	// at "1" and the daemon would run capture/replay against stale state.
	waitFor(t, 3*time.Second, "branch.generation bumps after divergence", func() bool {
		v, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
		return v == "2"
	})
	if v, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration); v != "2" {
		t.Fatalf("branch.generation=%q want 2 (post-flush guard did not catch divergence)", v)
	}
}

// TestRun_SelfPublicationBoundary_MultiGroupHeadAdvanceBeforeTokenAdoption
// reproduces the liveness window seen when one publication pass advances HEAD
// more than once while the run loop still holds its pre-pass branch token.
//
// The existing beforeBranchTokenCheck seam is the closest production boundary
// available before journaled publication hooks land. Holding it gives the test
// a deterministic boundary: no scheduler sleeps are involved, and the daemon
// cannot sample or adopt either group while the gate is closed. The assertions
// pin the observable pre-adoption signature (stale durable token and heartbeat,
// plus a pending flush), then prove the loop converges without an archive or a
// duplicate commit after the gate opens.
func TestRun_SelfPublicationBoundary_MultiGroupHeadAdvanceBeforeTokenAdoption(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	manual := Scheduler{
		Base:         time.Hour,
		IdleCeiling:  time.Hour,
		ErrorCeiling: time.Hour,
	}
	checkHook, checkEntered, releaseCheck := oneShotBranchTokenCheckGate()
	defer releaseCheck()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath:               f.dir,
			GitDir:                 f.gitDir,
			DB:                     f.db,
			Scheduler:              manual,
			BootGrace:              30 * time.Second,
			MessageFn:              DeterministicMessage,
			WakeCh:                 wakeCh,
			ShutdownCh:             shutdownCh,
			SkipSignals:            true,
			beforeBranchTokenCheck: checkHook,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	waitForBranchTokenCheckGate(t, checkEntered)
	waitForMetaValue(t, f.db, MetaKeyBranchHead, seedHead, 2*time.Second)
	waitForMetaValue(t, f.db, MetaKeyBranchToken,
		branchTokenRev(seedHead, "refs/heads/main"), 2*time.Second)

	before, ok, err := state.LoadDaemonState(ctx, f.db)
	if err != nil || !ok {
		t.Fatalf("load gated daemon state: ok=%v err=%v", ok, err)
	}
	if _, err := state.EnqueueFlushRequest(ctx, f.db, "wake", false,
		sql.NullString{String: "self-publication boundary", Valid: true}); err != nil {
		t.Fatalf("enqueue boundary flush: %v", err)
	}

	// These two commits model two Intent groups published in one replay pass.
	// The daemon remains parked before its next token sample throughout.
	if err := os.WriteFile(filepath.Join(f.dir, "group-one.txt"),
		[]byte("group one\n"), 0o644); err != nil {
		t.Fatalf("write group one: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "group-one.txt"); err != nil {
		t.Fatalf("add group one: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "group one"); err != nil {
		t.Fatalf("commit group one: %v", err)
	}
	groupOneHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse group one: %v", err)
	}

	if err := os.WriteFile(filepath.Join(f.dir, "group-two.txt"),
		[]byte("group two\n"), 0o644); err != nil {
		t.Fatalf("write group two: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "group-two.txt"); err != nil {
		t.Fatalf("add group two: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "group two"); err != nil {
		t.Fatalf("commit group two: %v", err)
	}
	groupTwoHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse group two: %v", err)
	}
	if groupOneHead == seedHead || groupTwoHead == groupOneHead {
		t.Fatalf("test setup did not produce two HEAD advances: seed=%s group1=%s group2=%s",
			seedHead, groupOneHead, groupTwoHead)
	}

	// The closed gate is the synchronization proof: these observations are
	// made while the run loop cannot possibly have sampled the new HEAD.
	if got, _, err := state.MetaGet(ctx, f.db, MetaKeyBranchHead); err != nil || got != seedHead {
		t.Fatalf("gated branch.head=%q err=%v want stale seed %s", got, err, seedHead)
	}
	if got := countFlushByStatus(t, f.db, "pending"); got != 1 {
		t.Fatalf("gated pending flushes=%d want 1", got)
	}
	during, ok, err := state.LoadDaemonState(ctx, f.db)
	if err != nil || !ok {
		t.Fatalf("load daemon state during gate: ok=%v err=%v", ok, err)
	}
	if during.HeartbeatTS != before.HeartbeatTS {
		t.Fatalf("heartbeat advanced across closed boundary: before=%f during=%f",
			before.HeartbeatTS, during.HeartbeatTS)
	}

	releaseCheck()

	waitForMetaValue(t, f.db, MetaKeyBranchHead, groupTwoHead, 3*time.Second)
	waitFor(t, 3*time.Second, "boundary flush completes", func() bool {
		return countFlushByStatus(t, f.db, "completed") == 1
	})

	if got, _, err := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration); err != nil || got != "1" {
		t.Fatalf("branch generation=%q err=%v want stable generation 1", got, err)
	}
	var archives int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM recovery_snapshots`).Scan(&archives); err != nil {
		t.Fatalf("count recovery snapshots: %v", err)
	}
	if archives != 0 {
		t.Fatalf("recovery snapshots=%d want 0 for the linear two-group advance", archives)
	}
	out, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "rev-list", "--count",
		seedHead+".."+groupTwoHead)
	if err != nil {
		t.Fatalf("count publication commits: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "2" {
		t.Fatalf("publication commit count=%s want exactly 2", got)
	}
}

func TestRun_HeartbeatAndWakeAfterSelfPublication(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "publication-one.txt"),
		[]byte("one\n"), 0o644); err != nil {
		t.Fatalf("write publication one: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "publication-two.txt"),
		[]byte("two\n"), 0o644); err != nil {
		t.Fatalf("write publication two: %v", err)
	}

	type adoptionSnapshot struct {
		cctx        CaptureContext
		token       string
		headOID     string
		stampedHead string
	}
	adoptedCh := make(chan adoptionSnapshot, 1)
	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	trace := &memoryTraceLogger{}
	manual := Scheduler{
		Base:         time.Hour,
		IdleCeiling:  time.Hour,
		ErrorCeiling: time.Hour,
	}

	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = Run(ctx, Options{
			RepoPath:      f.dir,
			GitDir:        f.gitDir,
			DB:            f.db,
			Scheduler:     manual,
			BootGrace:     30 * time.Second,
			MessageFn:     DeterministicMessage,
			WakeCh:        wakeCh,
			ShutdownCh:    shutdownCh,
			SkipSignals:   true,
			PruneInterval: time.Hour,
			Trace:         trace,
			afterSelfPublicationAdoption: func(
				cctx CaptureContext,
				token string,
				headOID string,
				stampedHead string,
			) {
				select {
				case adoptedCh <- adoptionSnapshot{
					cctx:        cctx,
					token:       token,
					headOID:     headOID,
					stampedHead: stampedHead,
				}:
				default:
				}
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		if runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	})

	var adopted adoptionSnapshot
	select {
	case adopted = <-adoptedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("run loop did not adopt journaled self-publication")
	}
	targetHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse target: %v", err)
	}
	if targetHead == seedHead {
		t.Fatalf("HEAD stayed at seed %s", seedHead)
	}
	wantToken := branchTokenRev(targetHead, "refs/heads/main")
	if adopted.cctx.BaseHead != targetHead ||
		adopted.token != wantToken ||
		adopted.headOID != targetHead ||
		adopted.stampedHead != targetHead {
		t.Fatalf(
			"adopted snapshot cctx=%s token=%q headOID=%s stamped=%s want target=%s token=%q",
			adopted.cctx.BaseHead, adopted.token, adopted.headOID,
			adopted.stampedHead, targetHead, wantToken)
	}
	t.Logf("adopted target=%s token=%q generation=%d",
		targetHead, wantToken, adopted.cctx.BranchGeneration)
	waitForMetaValue(t, f.db, MetaKeyBranchHead, targetHead, 2*time.Second)
	waitForMetaValue(t, f.db, MetaKeyBranchToken, wantToken, 2*time.Second)
	waitForMetaValue(t, f.db, MetaKeyBranchGeneration, "1", 2*time.Second)

	var completedPublications int
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM self_publications
WHERE phase = 'completed' AND target_commit_oid = ?`, targetHead).
		Scan(&completedPublications); err != nil {
		t.Fatalf("count completed self-publications: %v", err)
	}
	if completedPublications != 1 {
		t.Fatalf("completed publications for target=%d want 1", completedPublications)
	}
	var archives int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM recovery_snapshots`).Scan(&archives); err != nil {
		t.Fatalf("count recovery snapshots: %v", err)
	}
	if archives != 0 {
		t.Fatalf("recovery snapshots=%d want 0", archives)
	}

	// Wait for the publication iteration's heartbeat, then prove the next
	// queued wake is both claimed and followed by another heartbeat within
	// the epic's three-second liveness budget.
	waitFor(t, 3*time.Second, "publication heartbeat", func() bool {
		st, ok, loadErr := state.LoadDaemonState(ctx, f.db)
		return loadErr == nil && ok && st.HeartbeatTS > 0
	})
	beforeWake, ok, err := state.LoadDaemonState(ctx, f.db)
	if err != nil || !ok {
		t.Fatalf("load heartbeat before wake: ok=%v err=%v", ok, err)
	}
	flushID, err := state.EnqueueFlushRequest(ctx, f.db, "wake", false,
		sql.NullString{String: "post-publication liveness", Valid: true})
	if err != nil {
		t.Fatalf("enqueue post-publication wake: %v", err)
	}
	wakeStarted := time.Now()
	wakeCh <- struct{}{}
	waitFor(t, 3*time.Second, "post-publication wake and heartbeat", func() bool {
		var status string
		if scanErr := f.db.SQL().QueryRowContext(ctx,
			`SELECT status FROM flush_requests WHERE id = ?`, flushID).
			Scan(&status); scanErr != nil || status != "completed" {
			return false
		}
		st, stateOK, loadErr := state.LoadDaemonState(ctx, f.db)
		return loadErr == nil && stateOK &&
			st.HeartbeatTS > beforeWake.HeartbeatTS
	})
	if elapsed := time.Since(wakeStarted); elapsed >= 3*time.Second {
		t.Fatalf("post-publication wake latency=%v want <3s", elapsed)
	} else {
		t.Logf("post-publication wake latency=%v", elapsed)
	}
	if transitions := traceEventsByClass(
		trace.Events(), "branch_token.transition"); len(transitions) != 0 {
		t.Fatalf("journaled target entered external transition path: %+v",
			transitions)
	}
}

func TestRun_SelfPublicationTokenRetryFailsClosed(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := os.WriteFile(filepath.Join(f.dir, "publication-before-retry.txt"),
		[]byte("publication\n"), 0o644); err != nil {
		t.Fatalf("write initial publication: %v", err)
	}
	var remainingTokenFailures atomic.Int32
	var injectOnce sync.Once
	targetCh := make(chan string, 1)
	adoptedCh := make(chan CaptureContext, 1)
	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)

	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = Run(ctx, Options{
			RepoPath:      f.dir,
			GitDir:        f.gitDir,
			DB:            f.db,
			Scheduler:     fastScheduler(),
			BootGrace:     30 * time.Second,
			MessageFn:     DeterministicMessage,
			WakeCh:        wakeCh,
			ShutdownCh:    shutdownCh,
			SkipSignals:   true,
			PruneInterval: time.Hour,
			selfPublicationCheckpoint: func(
				event SelfPublicationCheckpointEvent,
			) error {
				if event.Checkpoint != SelfPublicationAfterCompletion {
					return nil
				}
				var injectErr error
				injectOnce.Do(func() {
					if err := os.WriteFile(
						filepath.Join(f.dir, "captured-after-token-retry.txt"),
						[]byte("after retry\n"), 0o644); err != nil {
						injectErr = err
						return
					}
					remainingTokenFailures.Store(2)
					targetCh <- event.TargetOID
				})
				return injectErr
			},
			branchGenerationToken: func(
				resolveCtx context.Context,
				repoPath string,
			) (string, error) {
				for {
					remaining := remainingTokenFailures.Load()
					if remaining == 0 {
						return BranchGenerationToken(resolveCtx, repoPath)
					}
					if remainingTokenFailures.CompareAndSwap(
						remaining, remaining-1) {
						return "", errors.New(
							"injected self-publication token read failure")
					}
				}
			},
			afterSelfPublicationAdoption: func(
				cctx CaptureContext, _, _, _ string,
			) {
				select {
				case adoptedCh <- cctx:
				default:
				}
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		if runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	})

	var targetOID string
	select {
	case targetOID = <-targetCh:
	case <-time.After(10 * time.Second):
		t.Fatal("publication checkpoint did not inject token failures")
	}
	var adopted CaptureContext
	select {
	case adopted = <-adoptedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("pending self-publication target was not retried")
	}
	if adopted.BaseHead != targetOID {
		t.Fatalf("adopted base=%s want journal target %s",
			adopted.BaseHead, targetOID)
	}
	waitFor(t, 5*time.Second, "post-retry capture", func() bool {
		var baseHead string
		err := f.db.SQL().QueryRowContext(ctx, `
SELECT base_head
FROM capture_events
WHERE path = ?
ORDER BY seq DESC
LIMIT 1`, "captured-after-token-retry.txt").Scan(&baseHead)
		return err == nil && baseHead == targetOID
	})
	var staleCaptures int
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM capture_events
WHERE path = ? AND base_head <> ?`,
		"captured-after-token-retry.txt", targetOID).Scan(&staleCaptures); err != nil {
		t.Fatalf("count stale captures: %v", err)
	}
	if staleCaptures != 0 {
		t.Fatalf("post-retry stale captures=%d want 0", staleCaptures)
	}
	waitFor(t, 5*time.Second, "post-retry capture publishes", func() bool {
		var eventState string
		err := f.db.SQL().QueryRowContext(ctx, `
SELECT state
FROM capture_events
WHERE path = ?
ORDER BY seq DESC
LIMIT 1`, "captured-after-token-retry.txt").Scan(&eventState)
		return err == nil && eventState == state.EventStatePublished
	})
}

func TestRun_UnknownHeadAfterSelfPublicationUsesTransitionClassifier(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := os.WriteFile(filepath.Join(f.dir, "published-before-external.txt"),
		[]byte("published\n"), 0o644); err != nil {
		t.Fatalf("write publication: %v", err)
	}
	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	externalMovedCh := make(chan string, 1)
	adoptedCh := make(chan struct{}, 1)
	trace := &memoryTraceLogger{}
	manual := Scheduler{
		Base:         time.Hour,
		IdleCeiling:  time.Hour,
		ErrorCeiling: time.Hour,
	}
	var moveOnce sync.Once

	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = Run(ctx, Options{
			RepoPath:      f.dir,
			GitDir:        f.gitDir,
			DB:            f.db,
			Scheduler:     manual,
			BootGrace:     30 * time.Second,
			MessageFn:     DeterministicMessage,
			WakeCh:        wakeCh,
			ShutdownCh:    shutdownCh,
			SkipSignals:   true,
			PruneInterval: time.Hour,
			Trace:         trace,
			afterSelfPublicationAdoption: func(
				CaptureContext, string, string, string,
			) {
				select {
				case adoptedCh <- struct{}{}:
				default:
				}
			},
			selfPublicationCheckpoint: func(
				event SelfPublicationCheckpointEvent,
			) error {
				if event.Checkpoint != SelfPublicationAfterCompletion {
					return nil
				}
				var moveErr error
				moveOnce.Do(func() {
					externalHead, err := git.CommitTree(
						ctx, f.dir, event.TreeOID,
						"external child after publication", event.TargetOID)
					if err != nil {
						moveErr = err
						return
					}
					if err := git.UpdateRef(
						ctx, f.dir, "refs/heads/main",
						externalHead, event.TargetOID); err != nil {
						moveErr = err
						return
					}
					externalMovedCh <- externalHead
				})
				return moveErr
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		if runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	})

	var externalHead string
	select {
	case externalHead = <-externalMovedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("checkpoint did not create external HEAD movement")
	}
	// Queue the next iteration while Replay is returning. The live HEAD no
	// longer equals its completed journal target, so the run loop must not
	// invoke the self-publication adoption boundary.
	wakeCh <- struct{}{}
	waitForMetaValue(t, f.db, MetaKeyBranchHead, externalHead, 5*time.Second)
	waitForMetaValue(t, f.db, MetaKeyBranchToken,
		branchTokenRev(externalHead, "refs/heads/main"), 5*time.Second)
	select {
	case <-adoptedCh:
		t.Fatal("external child was incorrectly adopted as self-publication")
	default:
	}
	if got, _, err := state.MetaGet(
		ctx, f.db, MetaKeyBranchGeneration); err != nil || got != "1" {
		t.Fatalf("branch generation=%q err=%v want stable fast-forward generation 1",
			got, err)
	}
	var archives int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM recovery_snapshots`).Scan(&archives); err != nil {
		t.Fatalf("count recovery snapshots: %v", err)
	}
	if archives != 0 {
		t.Fatalf("recovery snapshots=%d want 0", archives)
	}
	waitFor(t, 5*time.Second, "external fast-forward trace", func() bool {
		for _, event := range traceEventsByClass(
			trace.Events(), "branch_token.transition") {
			if event.Decision == TokenTransitionFastForward.String() {
				return true
			}
		}
		return false
	})
}

// TestRun_FlushDrainBoundedByLimit pins the regression where the flush
// drain loop ran without an upper bound. A 1500-row burst would block the
// rest of the run loop (capture/replay, refcount sweep, heartbeat) until
// the entire queue drained, and shutdowns mid-drain were starved.
//
// We pre-enqueue 600 rows with FlushLimit=64 and a manual-only scheduler
// (very long Base/Ceilings → no auto-tick). The boot iteration drains
// exactly 64; we then drive one wake to exercise a second bounded pass
// and assert exact counts. No timing window — the assertion is on
// observable database state after each deterministic tick.
func TestRun_FlushDrainBoundedByLimit(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pre-enqueue 600 flush requests before booting the daemon.
	for i := 0; i < 600; i++ {
		if _, err := state.EnqueueFlushRequest(ctx, f.db, "wake", false, sql.NullString{}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// Manual-only scheduler: Base/Ceilings far exceed the test runtime so
	// the only iterations that fire are the boot iteration plus our
	// explicit wakeCh sends. This removes the previous timing-window flake
	// risk on slow CI runners.
	manual := Scheduler{
		Base:         1 * time.Hour,
		IdleCeiling:  1 * time.Hour,
		ErrorCeiling: 1 * time.Hour,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   manual,
			BootGrace:   30 * time.Second,
			MessageFn:   DeterministicMessage,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
			FlushLimit:  64,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 2*time.Second)

	// Boot iteration drains exactly FlushLimit=64. Wait for completed to
	// reach 64 (which it will, deterministically — manual scheduler means
	// no extra ticks fire). Then assert pending==536: the bound held.
	waitFor(t, 2*time.Second, "boot iteration drains 64", func() bool {
		return countFlushByStatus(t, f.db, "completed") >= 64
	})
	if got := countFlushByStatus(t, f.db, "completed"); got != 64 {
		t.Fatalf("after boot iteration: completed=%d want 64 (bound did not hold)", got)
	}
	if got := countFlushByStatus(t, f.db, "pending"); got != 536 {
		t.Fatalf("after boot iteration: pending=%d want 536", got)
	}
	if got := countFlushByStatus(t, f.db, "acknowledged"); got != 0 {
		t.Fatalf("after boot iteration: acknowledged=%d want 0", got)
	}

	// Drive a second iteration via wakeCh. Bound still holds → 128/472.
	wakeCh <- struct{}{}
	waitFor(t, 2*time.Second, "second iteration drains 64", func() bool {
		return countFlushByStatus(t, f.db, "completed") >= 128
	})
	if got := countFlushByStatus(t, f.db, "completed"); got != 128 {
		t.Fatalf("after second iteration: completed=%d want 128", got)
	}
	if got := countFlushByStatus(t, f.db, "pending"); got != 472 {
		t.Fatalf("after second iteration: pending=%d want 472", got)
	}
	if got := countFlushByStatus(t, f.db, "acknowledged"); got != 0 {
		t.Fatalf("after second iteration: acknowledged=%d want 0", got)
	}
	var flushedNotes int
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM flush_requests
WHERE status = 'completed' AND note = 'flushed'`).Scan(&flushedNotes); err != nil {
		t.Fatalf("count flushed notes: %v", err)
	}
	if flushedNotes != 128 {
		t.Fatalf("completed flushed notes=%d want 128", flushedNotes)
	}
}

func TestRun_WakeOnlyFlushCannotBypassIntentV2Cutover(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyIntent))
	t.Setenv(ai.EnvIntentMinPending, "4")
	t.Setenv(ai.EnvIntentMaxPendingAge, "1h")
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))

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
	manual := Scheduler{
		Base:         time.Hour,
		IdleCeiling:  time.Hour,
		ErrorCeiling: time.Hour,
	}
	planner := &recordingIntentPlanner{}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, Options{
			RepoPath:      f.dir,
			GitDir:        f.gitDir,
			DB:            f.db,
			Scheduler:     manual,
			BootGrace:     30 * time.Second,
			MessageFn:     DeterministicMessage,
			WakeCh:        wakeCh,
			ShutdownCh:    shutdownCh,
			SkipSignals:   true,
			IntentPlanner: planner,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	waitForDaemonMode(t, f.db, "running", 2*time.Second)

	if err := os.WriteFile(filepath.Join(f.dir, "wake-only.txt"), []byte("v1\n"), 0o644); err != nil {
		t.Fatalf("write wake-only: %v", err)
	}
	if _, err := state.EnqueueFlushRequest(ctx, f.db, "wake", false, sql.NullString{}); err != nil {
		t.Fatalf("enqueue wake flush request: %v", err)
	}
	wakeCh <- struct{}{}

	waitFor(t, 2*time.Second, "wake flush request completed", func() bool {
		return countFlushByStatus(t, f.db, "completed") >= 1
	})
	waitFor(t, 2*time.Second, "capture event remains pending", func() bool {
		pending, err := state.PendingEvents(context.Background(), f.db, 0)
		return err == nil && len(pending) == 1
	})
	if planner.calls != 0 {
		t.Fatalf("planner calls=%d want 0 before immutable v2 activation",
			planner.calls)
	}
	head, err := git.RevParse(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse after wake: %v", err)
	}
	if head != startHead {
		t.Fatalf("HEAD advanced on wake-only drain below MinPending: got %s want %s", head, startHead)
	}
	attention, ok, err := state.MetaGet(context.Background(), f.db,
		"intent.v2.needs_attention")
	if err != nil || !ok || !strings.Contains(attention, "acd configure") {
		t.Fatalf("needs-attention guidance=%q ok=%v err=%v",
			attention, ok, err)
	}
}

// waitFor polls cond until it returns true or the deadline elapses, then
// fails. The 20ms granularity keeps the test responsive without burning
// CPU on a tight loop. Used by deterministic tests to wait for the daemon
// to settle a single observable database transition.
func waitFor(t *testing.T, budget time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waitFor: %s did not become true within %v", what, budget)
}

// TestRun_FlushDrainCancelable pins the regression where SIGTERM during a
// large flush drain was starved until the entire queue drained. Each drain is
// one bounded statement and the run loop checks cancellation around it.
func TestRun_FlushDrainCancelable(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enqueue a large burst to make the drain take meaningful time even
	// with FlushLimit=DefaultFlushLimit (256).
	for i := 0; i < 1500; i++ {
		if _, err := state.EnqueueFlushRequest(ctx, f.db, "wake", false, sql.NullString{}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	exited := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(exited)
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

	// Send shutdown signal and assert Run returns within a generous budget.
	// Without the ctx.Err check inside the drain loop, an unbounded drain
	// of 1500 rows would dwarf this budget on slow hosts; the bounded +
	// cancelable drain exits within at most one bounded pass plus
	// shutdown overhead. The 5s budget accommodates a slow Linux runner
	// under -race -count=3 finishing the in-progress bounded statement
	// (DefaultFlushLimit=256 rows) before the cancel check fires.
	start := time.Now()
	shutdownCh <- struct{}{}
	select {
	case <-exited:
		elapsed := time.Since(start)
		if elapsed > 5*time.Second {
			t.Fatalf("Run took %v to exit after shutdown; flush drain not cancelable", elapsed)
		}
		t.Logf("graceful shutdown in %v", elapsed)
	case <-time.After(6 * time.Second):
		cancel()
		<-exited
		t.Fatalf("Run did not exit within 6s after shutdown signal")
	}
	wg.Wait()
}

// TestRun_OrphanAckedFlushSweepOnStartup pins the orphan sweep: rows that
// sat in "acknowledged" past OrphanFlushAckThreshold are marked failed at
// daemon startup so `acd status` doesn't accumulate ghosts forever.
//
// Test setup uses the real EnqueueFlushRequest path, then UPDATEs status
// + acknowledged_ts to the desired age. This mirrors the production
// schema (column types, default ts unit) instead of guessing at the
// timestamp format with raw INSERTs.
func TestRun_OrphanAckedFlushSweepOnStartup(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	orphanID, err := state.EnqueueFlushRequest(ctx, f.db, "wake", false, sql.NullString{})
	if err != nil {
		t.Fatalf("enqueue orphan: %v", err)
	}
	old := float64(time.Now().Add(-10*time.Minute).UnixNano()) / 1e9
	if _, err := f.db.SQL().ExecContext(ctx,
		`UPDATE flush_requests SET status='acknowledged', acknowledged_ts=?, requested_ts=? WHERE id=?`,
		old, old, orphanID,
	); err != nil {
		t.Fatalf("age orphan row: %v", err)
	}

	freshID, err := state.EnqueueFlushRequest(ctx, f.db, "wake", false, sql.NullString{})
	if err != nil {
		t.Fatalf("enqueue fresh: %v", err)
	}
	fresh := float64(time.Now().Add(-30*time.Second).UnixNano()) / 1e9
	if _, err := f.db.SQL().ExecContext(ctx,
		`UPDATE flush_requests SET status='acknowledged', acknowledged_ts=?, requested_ts=? WHERE id=?`,
		fresh, fresh, freshID,
	); err != nil {
		t.Fatalf("age fresh row: %v", err)
	}

	registerLiveClient(t, f.db)

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
			MessageFn:   DeterministicMessage,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 2*time.Second)

	// The orphan was old enough to be swept to "failed".
	deadline := time.Now().Add(2 * time.Second)
	var failed, acked int
	for time.Now().Before(deadline) {
		failed = countFlushByStatus(t, f.db, "failed")
		acked = countFlushByStatus(t, f.db, "acknowledged")
		if failed >= 1 && acked == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("after sweep: failed=%d acknowledged=%d; want failed>=1 and acknowledged=1 (fresh row preserved)", failed, acked)
}

// TestRun_DeprecatedSendDiffEnvVarWarns pins the user-visible contract
// that ACD_AI_SEND_DIFF emits a one-shot deprecation warning at daemon
// startup when set, and stays silent when unset. Operators rely on the
// log line as their migration hint after the env var was removed in favor
// of the explicit ACD_AI_DIFF_EGRESS opt-in.
func TestRun_DeprecatedSendDiffEnvVarWarns(t *testing.T) {
	t.Run("env_set_warns", func(t *testing.T) {
		t.Setenv("ACD_AI_SEND_DIFF", "1")
		records := bootDaemonAndCaptureWarns(t)
		if !hasLogMessage(records, "ACD_AI_SEND_DIFF is deprecated") {
			t.Fatalf("expected deprecation warn for ACD_AI_SEND_DIFF; got records=%+v", records)
		}
	})
	t.Run("env_unset_silent", func(t *testing.T) {
		// t.Setenv with empty does not unset, so unset explicitly. The
		// deprecation warn must NOT fire when the variable is absent.
		t.Setenv("ACD_AI_SEND_DIFF", "")
		_ = os.Unsetenv("ACD_AI_SEND_DIFF")
		records := bootDaemonAndCaptureWarns(t)
		if hasLogMessage(records, "ACD_AI_SEND_DIFF is deprecated") {
			t.Fatalf("did not expect deprecation warn when env unset; got records=%+v", records)
		}
	})
}

// bootDaemonAndCaptureWarns boots a daemon long enough to observe startup
// logs, then returns the captured warn-level records.
func bootDaemonAndCaptureWarns(t *testing.T) []loggedRecord {
	t.Helper()
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	logSink := &captureLogHandler{level: slog.LevelWarn}
	logger := slog.New(logSink)

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
			MessageFn:   DeterministicMessage,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
			Logger:      logger,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	// Brief settle to give startup-emitted warns time to land in the sink.
	time.Sleep(50 * time.Millisecond)
	return logSink.Records()
}

func hasLogMessage(records []loggedRecord, substr string) bool {
	for _, r := range records {
		if strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

// TestGitOperationInProgress_StatErrorTreatedAsAbsent pins the fix that
// turns a non-ErrNotExist stat error (EACCES, ELOOP, EIO) into "marker
// absent" rather than "marker present, paused forever". Before the fix,
// a single transient stat hiccup latched capture/replay into permanent
// pause because the only auto-clear path is the reverse "marker absent"
// branch in the run loop.
//
// We exercise this by creating a self-referential symlink at MERGE_HEAD;
// os.Stat follows symlinks and returns ELOOP for the cycle. The function
// must report (_, false) for that marker (logged + skipped) and continue
// scanning the remaining markers.
func TestGitOperationInProgress_StatErrorTreatedAsAbsent(t *testing.T) {
	gitDir := t.TempDir()

	// Self-referential symlink at MERGE_HEAD triggers ELOOP on stat.
	mergePath := filepath.Join(gitDir, "MERGE_HEAD")
	if err := os.Symlink(mergePath, mergePath); err != nil {
		t.Fatalf("symlink loop: %v", err)
	}

	name, paused := gitOperationInProgress(gitDir)
	if paused {
		t.Fatalf("gitOperationInProgress=(%q,%v); want (_, false) when stat errors are treated as absent",
			name, paused)
	}
	if name != "" {
		t.Fatalf("gitOperationInProgress returned name=%q for absent markers", name)
	}

	// Sanity: with a real marker present, the function still reports paused.
	rebasePath := filepath.Join(gitDir, "rebase-merge")
	if err := os.Mkdir(rebasePath, 0o755); err != nil {
		t.Fatalf("mkdir rebase-merge: %v", err)
	}
	name, paused = gitOperationInProgress(gitDir)
	if !paused || name != "rebase-merge" {
		t.Fatalf("gitOperationInProgress with real marker = (%q, %v); want (rebase-merge, true)",
			name, paused)
	}
}

// TestRun_GitOperationStatErrorRecovers exercises the same fix via the full
// run loop: a marker path that produces a non-ErrNotExist stat error must
// not wedge capture/replay. After the bad path is replaced by a real
// absence, the daemon must process subsequent edits and produce a commit.
func TestRun_GitOperationStatErrorRecovers(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	startHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	// Create a self-referential symlink so os.Stat returns ELOOP (a non-
	// ErrNotExist error). Pre-fix this would latch the daemon into
	// "operation in progress; capture/replay paused" forever.
	bad := filepath.Join(f.gitDir, "MERGE_HEAD")
	if err := os.Symlink(bad, bad); err != nil {
		t.Fatalf("symlink loop: %v", err)
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
			MessageFn:   DeterministicMessage,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 2*time.Second)

	// The "operation in progress" gate must NEVER latch on the bad path.
	// A previous bug returned (name, true) for ELOOP and the next clear
	// branch never fires (since the symlink keeps returning ELOOP).
	// Remove the symlink and write a file; capture/replay should commit.
	if err := os.Remove(bad); err != nil {
		t.Fatalf("remove bad symlink: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "recover.txt"),
		[]byte("after stat error\n"), 0o644); err != nil {
		t.Fatalf("write recover: %v", err)
	}
	wakeCh <- struct{}{}

	newHead := waitForCommit(t, f.dir, startHead, 5*time.Second)
	if newHead == startHead {
		t.Fatalf("HEAD did not advance; capture/replay wedged after stat error")
	}

	// Operation marker meta should never have been stamped (the fix path
	// treats stat errors as absent, so MetaKeyOperationInProgress stays
	// unset throughout).
	if _, ok, err := state.MetaGet(ctx, f.db, MetaKeyOperationInProgress); err != nil {
		t.Fatalf("MetaGet operation_in_progress: %v", err)
	} else if ok {
		t.Fatalf("MetaKeyOperationInProgress was set despite stat-error-as-absent semantics")
	}
}

// TestRun_FlushDrainExitsOnShutdownCh pins the non-blocking shutdownCh checks
// around each bounded flush batch. A caller using a non-
// cancelable ctx (context.Background) and signaling shutdown via the
// channel must observe the daemon exit promptly — not after the entire
// bounded drain (~hundreds of rows × per-row claim cost) elapses.
//
// Setup:
//   - Enqueue a large burst of flush_requests (1500 rows).
//   - Run the daemon under ctx == context.Background() so ctx.Err is never
//     set.
//   - Push a shutdown signal and assert Run returns within ~1s. Pre-fix
//     this could take up to ~7.5s on a slow host (DefaultFlushLimit=256
//     rows × ~30ms per claim cycle).
func TestRun_FlushDrainExitsOnShutdownCh(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)

	// Crucial: use a non-cancelable ctx so the only exit signal is
	// shutdownCh. Without the checks around the bounded statement, the
	// drain would have to finish before the run loop notices the signal.
	bgCtx := context.Background()

	for i := 0; i < 1500; i++ {
		if _, err := state.EnqueueFlushRequest(bgCtx, f.db, "wake", false, sql.NullString{}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	exited := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(exited)
		_ = Run(bgCtx, Options{
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

	// Wake the loop so it enters the drain immediately, then push
	// the shutdown signal.
	wakeCh <- struct{}{}
	// Tiny settle so the drain enters its inner loop before we push
	// shutdown — the assertion is "exits promptly while drain is in
	// flight", not "exits promptly when idle". 50ms is generous on a
	// race-instrumented Linux runner without being fragile.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	shutdownCh <- struct{}{}

	select {
	case <-exited:
		elapsed := time.Since(start)
		// 2s budget tolerates -race + a slow CI runner finishing the
		// in-flight bounded statement. Pre-fix worst-case was ~7.5s.
		if elapsed > 2*time.Second {
			t.Fatalf("Run took %v to exit on shutdownCh; want <=2s (drain not shutdown-aware)",
				elapsed)
		}
		t.Logf("graceful shutdown via shutdownCh in %v with non-cancelable ctx", elapsed)
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not exit within 3s after shutdownCh signal under non-cancelable ctx")
	}
	wg.Wait()
}

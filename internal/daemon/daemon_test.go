package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
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
	c := testutil.LegacyClient{
		SessionID: "test-session",
		Harness:   "test",
		WatchPID:  sql.NullInt64{Int64: int64(pid), Valid: true},
		WatchFP:   sql.NullString{String: FingerprintToken(fp), Valid: true},
	}
	if err := testutil.SeedLegacyClient(context.Background(), db, c); err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
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

func mustCommitPath(t *testing.T, repo, path, contents, message string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, path), []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repo},
		"add", "--", path); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repo},
		"commit", "-q", "-m", message); err != nil {
		t.Fatal(err)
	}
	head, err := git.RevParse(context.Background(), repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return head
}

// openTestDB returns a fresh per-test state DB.
func openTestDB(t *testing.T) *state.DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cloneDaemonTestState(t, ctx)
}

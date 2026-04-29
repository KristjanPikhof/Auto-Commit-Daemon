package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// startResult is the JSON payload returned by `acd start --json`.
type startResult struct {
	Started     bool   `json:"started"`
	Duplicate   bool   `json:"duplicate"`
	DaemonPID   int    `json:"daemon_pid,omitempty"`
	Repo        string `json:"repo"`
	RepoHash    string `json:"repo_hash"`
	SessionID   string `json:"session_id"`
	Harness     string `json:"harness"`
	ClientCount int    `json:"client_count"`
}

// spawnDaemon is the injection point used by tests to avoid actually fork-
// exec'ing a real `acd daemon run` subprocess. Production callers leave it
// at the package default which exec's os.Args[0]; tests override it with a
// stub that simulates a healthy daemon.
//
// Returns the spawned PID (or 0 if the spawn was a no-op stub).
var spawnDaemon = defaultSpawnDaemon

// defaultSpawnDaemon fork-execs a detached `acd daemon run --repo <abs>`
// process. Stdin/stdout/stderr point to /dev/null so the parent can exit
// cleanly without holding the child's pipes; the daemon configures its own
// rotating slog logger inside Run.
func defaultSpawnDaemon(ctx context.Context, repoAbs string) (int, error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, fmt.Errorf("resolve executable: %w", err)
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return 0, fmt.Errorf("open /dev/null: %w", err)
	}
	defer devNull.Close()

	cmd := exec.Command(exe, "daemon", "run", "--repo", repoAbs)
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	// Detach: new session so SIGINT to the parent shell does not also
	// reach the daemon, and so the controlling terminal is released.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start daemon: %w", err)
	}
	// Release the child immediately — we do not Wait on it; the kernel
	// reaps via the new session leader.
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	return pid, nil
}

func newStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Register a session and ensure a daemon is running for this repo",
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			jsonOut, _ := c.Flags().GetBool("json")
			sessionID, _ := c.Flags().GetString("session-id")
			harness, _ := c.Flags().GetString("harness")
			watchPID, _ := c.Flags().GetInt("watch-pid")
			return runStart(c.Context(), c.OutOrStdout(), repoFlag, sessionID, harness, watchPID, jsonOut)
		},
	}
	cmd.Flags().String("session-id", "", "Universal session identifier (UUID, required)")
	cmd.Flags().String("harness", "", "Harness identifier (claude-code|codex|opencode|pi|shell|other)")
	cmd.Flags().Int("watch-pid", 0, "Optional fast-path PID for liveness probe (0 to disable)")
	return cmd
}

func runStart(ctx context.Context, out io.Writer, repoFlag, sessionID, harness string, watchPID int, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sessionID == "" {
		return errors.New("acd start: --session-id is required")
	}
	if harness == "" {
		harness = "other"
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	if !fileExists(repo) {
		return fmt.Errorf("acd start: repo %s does not exist", repo)
	}
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd start: resolve git dir: %w", err)
	}
	if err := ensureAttachedHEAD(ctx, repo); err != nil {
		return err
	}

	// Brief control.lock for the daemon_clients read-modify-write window.
	if err := os.MkdirAll(filepath.Join(gitDir, "acd"), 0o700); err != nil {
		return fmt.Errorf("acd start: mkdir state dir: %w", err)
	}
	clock, err := daemon.AcquireControlLock(gitDir)
	if err != nil {
		return fmt.Errorf("acd start: acquire control.lock: %w", err)
	}
	defer func() { _ = clock.Release() }()

	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd start: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	// watch_pid defaults to PPID when running from a shell hook; pass 0
	// to opt out (per §7.2 flag semantics).
	if watchPID == 0 {
		watchPID = os.Getppid()
	}
	var watchPIDNull sql.NullInt64
	var watchFPNull sql.NullString
	if watchPID > 0 {
		watchPIDNull = sql.NullInt64{Int64: int64(watchPID), Valid: true}
		if fp, ferr := identity.Capture(watchPID); ferr == nil && !fp.Empty() {
			watchFPNull = sql.NullString{String: fingerprintToken(fp), Valid: true}
		}
	}

	// Detect whether this session_id row already exists — the duplicate
	// flag in the response distinguishes "first registration" from
	// "refresh".
	existing, _ := state.ListClients(ctx, db)
	duplicate := false
	for _, c := range existing {
		if c.SessionID == sessionID {
			duplicate = true
			break
		}
	}

	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: sessionID,
		Harness:   harness,
		WatchPID:  watchPIDNull,
		WatchFP:   watchFPNull,
	}); err != nil {
		return fmt.Errorf("acd start: register client: %w", err)
	}

	// Detect daemon liveness: PID alive AND heartbeat fresh.
	st, _, err := state.LoadDaemonState(ctx, db)
	if err != nil {
		return fmt.Errorf("acd start: load daemon state: %w", err)
	}
	daemonPID := 0
	daemonAlive := false
	if st.PID > 0 && identity.Alive(st.PID) {
		hbAge := time.Since(time.Unix(int64(st.HeartbeatTS), 0))
		if hbAge < clientTTL() && st.Mode != "stopped" {
			daemonAlive = true
			daemonPID = st.PID
		}
	}

	started := false
	if !daemonAlive {
		// Spawn detached. Drop the control lock first — the daemon will
		// itself acquire daemon.lock and may briefly need control.lock
		// during its boot if it sweeps clients.
		if err := clock.Release(); err != nil {
			return fmt.Errorf("acd start: release control.lock pre-spawn: %w", err)
		}
		pid, err := spawnDaemon(ctx, repo)
		if err != nil {
			return fmt.Errorf("acd start: spawn daemon: %w", err)
		}
		started = true
		// Poll daemon_state.pid for up to ~3s. Tests inject a stub
		// spawnDaemon that stamps the row synchronously, so the loop
		// usually exits on the first iteration.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			st, _, _ = state.LoadDaemonState(ctx, db)
			if st.PID > 0 && st.Mode != "stopped" {
				daemonPID = st.PID
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if daemonPID == 0 {
			daemonPID = pid // fall back to the spawned PID
		}
	}

	// Update central registry — atomic via WithLock.
	repoHash, err := paths.RepoHash(repo)
	if err != nil {
		return fmt.Errorf("acd start: repo hash: %w", err)
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd start: resolve paths: %w", err)
	}
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(repo, repoHash, dbPath, harness, time.Now().Unix())
		return nil
	}); err != nil {
		return fmt.Errorf("acd start: update registry: %w", err)
	}

	clients, _ := state.CountClients(ctx, db)

	res := startResult{
		Started:     started,
		Duplicate:   duplicate,
		DaemonPID:   daemonPID,
		Repo:        repo,
		RepoHash:    repoHash,
		SessionID:   sessionID,
		Harness:     harness,
		ClientCount: clients,
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if started {
		fmt.Fprintf(out, "acd start: spawned daemon pid=%d for %s (session %s, harness %s)\n",
			daemonPID, repo, sessionID, harness)
	} else if duplicate {
		fmt.Fprintf(out, "acd start: refreshed session %s (daemon already running, pid %d)\n",
			sessionID, daemonPID)
	} else {
		fmt.Fprintf(out, "acd start: registered session %s (daemon already running, pid %d)\n",
			sessionID, daemonPID)
	}
	return nil
}

// resolveGitDir resolves the .git directory for a repo. Falls back to
// <repo>/.git when the git binary fails (common in synthetic test repos).
func resolveGitDir(ctx context.Context, repo string) (string, error) {
	resolved, err := git.AbsoluteGitDir(ctx, repo)
	if err == nil {
		return resolved, nil
	}
	fallback := filepath.Join(repo, ".git")
	if fileExists(fallback) {
		return fallback, nil
	}
	return "", err
}

func ensureAttachedHEAD(ctx context.Context, repo string) error {
	branchRef, err := git.RunBranchRef(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd start: resolve HEAD branch: %w", err)
	}
	if branchRef == "" {
		return errors.New("acd start: detached HEAD is not supported; checkout a branch before starting")
	}
	return nil
}

// fingerprintToken renders a Fingerprint into the canonical persisted form
// shared with the daemon refcount layer (lstart||argv-hash). We re-use the
// daemon-side helper rather than duplicating the format.
func fingerprintToken(fp identity.Fingerprint) string {
	return daemon.FingerprintToken(fp)
}

package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	Skipped     bool   `json:"skipped,omitempty"`
	SkipReason  string `json:"skipped_reason,omitempty"`
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

const defaultDaemonSpawnPollTimeout = 3 * time.Second

var daemonSpawnPollTimeout = defaultDaemonSpawnPollTimeout
var daemonSpawnPollInterval = 50 * time.Millisecond
var afterDaemonSpawnPollDeadline func(context.Context, *state.DB)
var startControlLockTimeout = 5 * time.Second
var startControlLockRetryInterval = 10 * time.Millisecond

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
		Long: `Register a client session for the resolved repo and start the daemon if it is not already running.

Without flags, acd start uses the current working directory as the repo and creates a manual session. Harness integrations normally pass --session-id, --harness, and --watch-pid so acd can keep a refcount and clean up when the harness process exits.

Use acd status to inspect the daemon and acd stop to stop the current repo daemon or deregister a harness session.`,
		Example: `  acd start
  acd start --repo /path/to/repo
  acd start --session-id "$ACD_SESSION_ID" --harness codex --watch-pid "$PPID"
  acd start --json`,
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			jsonOut, _ := c.Flags().GetBool("json")
			sessionID, _ := c.Flags().GetString("session-id")
			harness, _ := c.Flags().GetString("harness")
			watchPID, _ := c.Flags().GetInt("watch-pid")
			return runStart(c.Context(), c.OutOrStdout(), repoFlag, sessionID, harness, watchPID, jsonOut)
		},
	}
	cmd.Flags().String("session-id", "", "Universal session identifier (UUID; optional for manual starts)")
	cmd.Flags().String("harness", "", "Harness identifier (claude-code|codex|opencode|pi|shell|other)")
	cmd.Flags().Int("watch-pid", 0, "Optional fast-path PID for liveness probe (0 to disable)")
	return cmd
}

func runStart(ctx context.Context, out io.Writer, repoFlag, sessionID, harness string, watchPID int, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sessionID == "" && harness != "" {
		return errors.New("acd start: --session-id is required when --harness is set")
	}
	caller := startAutodiscoveryCaller(harness)
	if harness == "" {
		harness = "other"
	}
	if sessionID != "" {
		if res, ok := tryRegistryBackedShortCircuitStart(ctx, repoFlag, sessionID, harness); ok {
			_ = touchClientHotPath(ctx, res.gitDir, sessionID)
			if jsonOut {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(res.startResult)
			}
			fmt.Fprintf(out, "acd start: refreshed session %s (daemon already running, pid %d)\n",
				sessionID, res.startResult.DaemonPID)
			return nil
		}
	}
	policy, err := evaluateRepoAutodiscoveryPolicy(ctx, "start", repoFlag, caller)
	if err != nil {
		return err
	}
	wt := policy.Worktree
	repo := wt.Root
	gitDir := wt.GitDir
	repoHash, err := paths.RepoHash(repo)
	if err != nil {
		return fmt.Errorf("acd start: repo hash: %w", err)
	}
	restoreRestartEnvironment, err := applyRestartEnvironment(repo)
	if err != nil {
		return fmt.Errorf("acd start: load saved restart settings: %w", err)
	}
	defer restoreRestartEnvironment()
	if sessionID == "" {
		sessionID = humanStartSessionID(repoHash)
	}
	if !policy.allowsImplicitState() {
		if isManualAutodiscoveryCaller(caller) {
			if policy.Disabled {
				return repoDisabledError("start", policy)
			}
			return repoInitRequiredError("start", policy)
		}
		res := startResult{
			Started:    false,
			Duplicate:  false,
			Skipped:    true,
			SkipReason: policy.skipReason(),
			Repo:       repo,
			RepoHash:   repoHash,
			SessionID:  sessionID,
			Harness:    harness,
		}
		if jsonOut {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(res)
		}
		if policy.Disabled {
			fmt.Fprintf(out, "acd start: skipped for %s (%s; run `acd repo enable --repo %s` to allow ACD to manage it)\n",
				repo, policy.skipReason(), repo)
		} else {
			fmt.Fprintf(out, "acd start: skipped for %s (%s; run `acd repo init --repo %s` to register explicitly)\n",
				repo, policy.skipReason(), repo)
		}
		return nil
	}
	if err := ensureAttachedHEAD(ctx, repo); err != nil {
		return err
	}

	/* perf-lane: registry-read short-circuit
	 *
	 * Hot-path optimization for active hooks (PreToolUse / PostToolUse /
	 * UserPromptSubmit) that fire `acd start` on every tool invocation.
	 * When the same session_id has already been registered for this repo
	 * and the cached daemon heartbeat is fresh, return success without
	 * acquiring control.lock, opening SQLite, or rewriting registry.json.
	 *
	 * The cache file at <gitDir>/acd/start-cache.json is written by the
	 * full registration path below; missing / stale / mismatched cache
	 * forces the cold path (which is unchanged).
	 *
	 * Coordination: this short-circuit MUST run before any lock
	 * acquisition so concurrent active hooks from the same session never
	 * serialize on the daemon's control.lock. The adapter-lane PPID
	 * probe lives mid-runStart (near refcount.RegisterClient) and only
	 * applies on the cold path.
	 */
	if ok, cachedPID, cachedClients, _ := tryShortCircuitStart(ctx, gitDir, repoHash, sessionID, harness, repo); ok {
		// Refresh daemon_clients.last_seen_ts so the daemon's refcount
		// sweeper does not evict this session after clientTTL minutes
		// of all-hot-path activity. We deliberately keep this on the
		// hot path: it is a single UPDATE on a primary-key row (no
		// flock, no migrations, no central registry rewrite) and
		// completes well under our 50ms hot-path budget. Failure is
		// non-fatal — the worst case is the next sweeper tick evicts
		// the row and the next active hook re-registers via the cold
		// path. The cache short-circuit decision itself is unchanged.
		_ = touchClientHotPath(ctx, gitDir, sessionID)

		res := startResult{
			Started:   false,
			Duplicate: true,
			DaemonPID: cachedPID,
			Repo:      repo,
			RepoHash:  repoHash,
			SessionID: sessionID,
			Harness:   harness,
			// ClientCount comes from the cache snapshot. May lag the
			// SQLite truth by one tick under concurrent registrations;
			// hook consumers do not depend on a strictly-fresh value.
			ClientCount: cachedClients,
		}
		if jsonOut {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(res)
		}
		fmt.Fprintf(out, "acd start: refreshed session %s (daemon already running, pid %d)\n",
			sessionID, cachedPID)
		return nil
	}

	// Brief control.lock for the daemon_clients read-modify-write window.
	if err := os.MkdirAll(filepath.Join(gitDir, "acd"), 0o700); err != nil {
		return fmt.Errorf("acd start: mkdir state dir: %w", err)
	}
	clock, err := acquireStartControlLock(ctx, gitDir)
	if err != nil {
		return fmt.Errorf("acd start: acquire control.lock: %w", err)
	}
	defer func() { _ = clock.Release() }()
	if repoDisabledAfterControlLock(policy) {
		res := startResult{
			Started:    false,
			Duplicate:  false,
			Skipped:    true,
			SkipReason: repoAutodiscoverySkipRepoDisabled,
			Repo:       repo,
			RepoHash:   repoHash,
			SessionID:  sessionID,
			Harness:    harness,
		}
		if jsonOut {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(res)
		}
		fmt.Fprintf(out, "acd start: skipped for %s (%s; run `acd repo enable --repo %s` to allow ACD to manage it)\n",
			repo, repoAutodiscoverySkipRepoDisabled, repo)
		return nil
	}

	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd start: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	/* adapter-lane: PPID liveness probe.
	 *
	 * Probe the resolved watch_pid with kill(pid, 0) BEFORE consulting
	 * identity.AliveContext — kill(0) is the lowest-overhead syscall-level
	 * liveness check and lets us emit a focused diagnostic for the common
	 * "harness wraps `bash -c` through a transient parent" failure mode,
	 * where $PPID at hook-fire time names a shell process that has already
	 * exited by the time `acd start` runs. ESRCH here is informational —
	 * we log a single warning naming the pid (so users can grep the daemon
	 * log to confirm the wrapper-exit hypothesis) and continue without
	 * recording a watch_pid; the refcount sweeper will fall back to the
	 * TTL gate. Other kill(0) errors (EPERM, etc.) imply the pid is alive
	 * but owned by another user and are passed through to AliveContext.
	 */
	var watchPIDNull sql.NullInt64
	var watchFPNull sql.NullString
	if watchPID > 0 {
		if perr := syscall.Kill(watchPID, 0); errors.Is(perr, syscall.ESRCH) {
			slog.Default().Warn(
				"acd start: watch_pid is not alive at registration; harness may be wrapping the hook through a transient parent (e.g. `bash -c`) whose PID has already exited; continuing without a fast-path liveness PID",
				"pid", watchPID,
				"session_id", sessionID,
				"harness", harness,
			)
		} else if identity.AliveContext(ctx, watchPID) {
			watchPIDNull = sql.NullInt64{Int64: int64(watchPID), Valid: true}
			if fp, ferr := identity.CaptureContext(ctx, watchPID); ferr == nil && !fp.Empty() {
				watchFPNull = sql.NullString{String: fingerprintToken(fp), Valid: true}
			}
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
	registeredClients, _ := state.CountClients(ctx, db)

	// Detect daemon liveness: PID alive AND heartbeat fresh.
	st, _, err := state.LoadDaemonState(ctx, db)
	if err != nil {
		return fmt.Errorf("acd start: load daemon state: %w", err)
	}
	daemonPID := 0
	daemonAlive := false
	if st.PID > 0 && identity.AliveContext(ctx, st.PID) {
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
		pollTimeout := daemonSpawnPollTimeout
		if registeredClients > 1 && pollTimeout == defaultDaemonSpawnPollTimeout {
			pollTimeout = 5 * time.Second
		}
		deadline := time.Now().Add(pollTimeout)
		for time.Now().Before(deadline) {
			st, _, _ = state.LoadDaemonState(ctx, db)
			if st.PID > 0 && st.Mode != "stopped" {
				daemonPID = st.PID
				break
			}
			time.Sleep(daemonSpawnPollInterval)
		}
		if daemonPID == 0 {
			if afterDaemonSpawnPollDeadline != nil {
				afterDaemonSpawnPollDeadline(ctx, db)
			}
			st, _, _ = state.LoadDaemonState(ctx, db)
			if st.PID > 0 && st.Mode != "stopped" {
				daemonPID = st.PID
			}
		}
		if daemonPID == 0 {
			daemonPID = pid // fall back to the spawned PID
		}
		if pid > 0 && daemonPID > 0 && daemonPID != pid {
			started = false
		}
	}

	// Update central registry — atomic via WithLock.
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

	// perf-lane: persist the per-repo start-cache so subsequent active
	// hooks under the same session_id can short-circuit at the top of
	// runStart without re-acquiring control.lock or re-opening SQLite.
	// Failure to write is non-fatal — the next call simply takes the
	// cold path.
	//
	// Schema v2 also stamps the daemon's process-identity fingerprint
	// (lstart + argv hash). The short-circuit reader re-captures the
	// fingerprint and requires equality before granting hot-path
	// success. This defends against PID reuse: a kill(0) succeeds for
	// any process that inherits the recycled pid, but ps will report
	// a different start-time + argv vector.
	clients, _ := state.CountClients(ctx, db)
	if daemonPID > 0 {
		var startTS, argvHash string
		// Indirected through captureDaemonFingerprint so unit tests that
		// stub the short-circuit reader's fingerprint stub also pin the
		// cold-path writer's stamp; production callers leave it at the
		// identity.CaptureContext default.
		if fp, ferr := captureDaemonFingerprint(ctx, daemonPID); ferr == nil && !fp.Empty() {
			startTS = fp.StartTime
			argvHash = fp.ArgvHash
		}
		_ = writeStartCache(gitDir, startCache{
			Version:        startCacheVersion,
			RepoHash:       repoHash,
			SessionID:      sessionID,
			Harness:        harness,
			DaemonPID:      daemonPID,
			WatchPID:       watchPID,
			ClientCount:    clients,
			UpdatedAt:      time.Now().Unix(),
			DaemonStartTS:  startTS,
			DaemonArgvHash: argvHash,
		})
	}

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

func humanStartSessionID(repoHash string) string {
	return "human:" + repoHash
}

func acquireStartControlLock(ctx context.Context, gitDir string) (*daemon.ControlLock, error) {
	deadline := time.Now().Add(startControlLockTimeout)
	for {
		clock, err := daemon.AcquireControlLock(gitDir)
		if err == nil {
			return clock, nil
		}
		if !errors.Is(err, daemon.ErrControlLockHeld) || time.Now().After(deadline) {
			return nil, err
		}
		timer := time.NewTimer(startControlLockRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
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

// touchClientHotPath bumps daemon_clients.last_seen_ts for the supplied
// session without touching any other field. It opens the per-repo SQLite
// file briefly, runs a single UPDATE keyed on session_id, then closes —
// no flock, no migrations, no registry rewrite. Used by the short-
// circuit branch of runStart so a session that lives entirely on the
// hot path never exceeds the refcount sweeper's clientTTL window.
//
// All errors are returned to the caller (which logs and continues): a
// failed touch never blocks the active hook from progressing — the next
// sweeper tick may evict the row, but the next runStart will simply
// fall through to the cold path and re-register via state.RegisterClient.
//
// Indirected through a package var so unit tests can stub the helper to
// observe call sites without a real SQLite open.
var touchClientHotPath = defaultTouchClientHotPath

func defaultTouchClientHotPath(ctx context.Context, gitDir, sessionID string) error {
	if sessionID == "" {
		return errors.New("touchClientHotPath: empty session_id")
	}
	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("touchClientHotPath: open db: %w", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := state.TouchClient(ctx, db, sessionID, float64(time.Now().Unix())); err != nil {
		return fmt.Errorf("touchClientHotPath: touch: %w", err)
	}
	return nil
}

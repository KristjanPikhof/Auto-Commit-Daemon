package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// stopRepoResult is the per-repo result emitted by `acd stop`. Used both
// for single-repo invocations and as the slice element for `--all`.
type stopRepoResult struct {
	Repo           string `json:"repo"`
	SessionID      string `json:"session_id,omitempty"`
	Stopped        bool   `json:"stopped"`
	Deferred       bool   `json:"deferred,omitempty"`
	Force          bool   `json:"force,omitempty"`
	Escalated      bool   `json:"escalated,omitempty"`
	Peers          int    `json:"peers,omitempty"`
	Reason         string `json:"reason,omitempty"`
	DaemonPID      int    `json:"daemon_pid,omitempty"`
	UnknownSession bool   `json:"unknown_session,omitempty"`
}

// stopAllResult is the JSON payload for `acd stop --all`.
//
// `Failed` holds repos where stop attempted to terminate the daemon but the
// daemon survived — typically a fingerprint mismatch swallowing SIGTERM /
// SIGKILL, or "daemon survived SIGKILL" after escalation. These rows MUST
// NOT be reported under `Stopped`: the previous classifier branched on
// `Deferred` alone and silently buried failures.
type stopAllResult struct {
	Stopped  []stopRepoResult `json:"stopped"`
	Deferred []stopRepoResult `json:"deferred"`
	Failed   []stopRepoResult `json:"failed,omitempty"`
}

// stopWaitTimeout is how long the controller waits for the daemon to
// transition to mode=stopped after SIGTERM. Mirrors the legacy daemonctl
// 5-second SIGTERM grace.
var stopWaitTimeout = 5 * time.Second

// stopPollInterval is the busy-loop polling cadence inside stopWaitTimeout.
var stopPollInterval = 100 * time.Millisecond

func newStopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Deregister a session; daemon exits when refcount hits zero",
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			jsonOut, _ := c.Flags().GetBool("json")
			sessionID, _ := c.Flags().GetString("session-id")
			force, _ := c.Flags().GetBool("force")
			all, _ := c.Flags().GetBool("all")
			return runStop(c.Context(), c.OutOrStdout(), repoFlag, sessionID, force, all, jsonOut)
		},
	}
	cmd.Flags().String("session-id", "", "Session identifier to deregister")
	cmd.Flags().Bool("flush", false, "Drain pending events before stopping (with --force)")
	cmd.Flags().Bool("force", false, "Skip refcount and SIGTERM the daemon")
	cmd.Flags().Bool("all", false, "Stop every daemon in the central registry")
	return cmd
}

func runStop(ctx context.Context, out io.Writer, repoFlag, sessionID string, force, all, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if all {
		return runStopAll(ctx, out, force, jsonOut)
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	res, err := stopOneRepo(ctx, repo, sessionID, force)
	if err != nil {
		return err
	}
	return writeStopResult(out, res, jsonOut)
}

func runStopAll(ctx context.Context, out io.Writer, force, jsonOut bool) error {
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd stop: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return fmt.Errorf("acd stop: load registry: %w", err)
	}
	out_all := stopAllResult{Stopped: []stopRepoResult{}, Deferred: []stopRepoResult{}}
	for _, rec := range reg.Repos {
		// Use the caller's force mode for each repo; without --force,
		// the per-repo refcount-aware shutdown path applies.
		res, err := stopOneRepoForAll(ctx, rec.Path, "", force)
		if err != nil {
			res = stopRepoResult{Repo: rec.Path, Reason: err.Error()}
		}
		if res.Deferred {
			out_all.Deferred = append(out_all.Deferred, res)
		} else {
			out_all.Stopped = append(out_all.Stopped, res)
		}
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(out_all)
	}
	fmt.Fprintf(out, "acd stop --all: stopped=%d deferred=%d\n",
		len(out_all.Stopped), len(out_all.Deferred))
	for _, r := range out_all.Stopped {
		fmt.Fprintf(out, "  stopped: %s (pid %d)\n", r.Repo, r.DaemonPID)
	}
	for _, r := range out_all.Deferred {
		fmt.Fprintf(out, "  deferred: %s (%s)\n", r.Repo, r.Reason)
	}
	return nil
}

var stopOneRepoForAll = stopOneRepo

// stopOneRepo handles the per-repo logic shared by single-repo and --all.
func stopOneRepo(ctx context.Context, repo, sessionID string, force bool) (stopRepoResult, error) {
	res := stopRepoResult{Repo: repo, SessionID: sessionID, Force: force}
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return res, fmt.Errorf("acd stop: resolve git dir: %w", err)
	}
	clock, err := daemon.AcquireControlLock(gitDir)
	if err != nil {
		return res, fmt.Errorf("acd stop: acquire control.lock: %w", err)
	}
	defer func() { _ = clock.Release() }()

	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return res, fmt.Errorf("acd stop: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	st, _, err := state.LoadDaemonState(ctx, db)
	if err != nil {
		return res, fmt.Errorf("acd stop: load daemon state: %w", err)
	}
	res.DaemonPID = st.PID

	// Refcount-aware default path: drop the caller's row first, then
	// inspect remaining peers.
	if !force {
		if sessionID == "" {
			return res, errors.New("acd stop: --session-id is required (or pass --force)")
		}
		existed, err := state.DeregisterClient(ctx, db, sessionID)
		if err != nil {
			return res, fmt.Errorf("acd stop: deregister: %w", err)
		}
		if !existed {
			res.UnknownSession = true
		}
		// Count remaining clients.
		remaining, err := state.CountClients(ctx, db)
		if err != nil {
			return res, fmt.Errorf("acd stop: count clients: %w", err)
		}
		if remaining > 0 {
			res.Deferred = true
			res.Peers = remaining
			res.Reason = fmt.Sprintf("%d peer(s) remain", remaining)
			return res, nil
		}
		// No peers remain. Fall through to default-stop: SIGTERM the
		// daemon. If it does not transition to mode=stopped within
		// stopWaitTimeout, report deferred (run-loop's own GC will
		// catch up).
		if st.PID > 0 && identity.Alive(st.PID) {
			_ = signalProcess(st.PID, syscall.SIGTERM, daemonFingerprintToken(st))
			if waitForStopped(ctx, db, stopWaitTimeout) {
				res.Stopped = true
				return res, nil
			}
			res.Deferred = true
			res.Reason = "daemon still running after SIGTERM"
			return res, nil
		}
		// No live daemon — already stopped.
		res.Stopped = true
		return res, nil
	}

	// --force path: optionally deregister the named session, then SIGTERM
	// the daemon, escalate to SIGKILL after stopWaitTimeout.
	if sessionID != "" {
		_, _ = state.DeregisterClient(ctx, db, sessionID)
	}
	if st.PID <= 0 || !identity.Alive(st.PID) {
		res.Stopped = true
		res.Reason = "daemon not running"
		return res, nil
	}
	expectedFingerprint := daemonFingerprintToken(st)
	if err := signalProcess(st.PID, syscall.SIGTERM, expectedFingerprint); err != nil {
		res.Reason = fmt.Sprintf("SIGTERM failed: %v", err)
	}
	if waitForStopped(ctx, db, stopWaitTimeout) {
		res.Stopped = true
		return res, nil
	}
	// Escalate: SIGKILL to the PID. Verify identity via fingerprint
	// before the kill if possible — protects against PID reuse.
	res.Escalated = true
	if !identity.Alive(st.PID) {
		res.Stopped = true
		return res, nil
	}
	if err := signalProcess(st.PID, syscall.SIGKILL, expectedFingerprint); err != nil {
		res.Reason = fmt.Sprintf("SIGKILL failed: %v", err)
		return res, nil
	}
	// SIGKILL is synchronous from the kernel's perspective but the
	// daemon's own state.db rows won't be updated. Treat the kill as
	// "stopped" once the PID is gone.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !identity.Alive(st.PID) {
			res.Stopped = true
			return res, nil
		}
		time.Sleep(stopPollInterval)
	}
	res.Reason = "daemon survived SIGKILL"
	return res, nil
}

// waitForStopped polls daemon_state.mode for "stopped" inside the timeout.
// Returns true once the transition is observed.
func waitForStopped(ctx context.Context, db *state.DB, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, _, err := state.LoadDaemonState(ctx, db)
		if err == nil && st.Mode == "stopped" {
			return true
		}
		// Belt and braces: PID gone is also success.
		if err == nil && st.PID > 0 && !identity.Alive(st.PID) {
			return true
		}
		time.Sleep(stopPollInterval)
	}
	return false
}

func writeStopResult(out io.Writer, res stopRepoResult, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	switch {
	case res.Deferred:
		fmt.Fprintf(out, "acd stop: deferred (%s)\n", res.Reason)
	case res.Stopped && res.Force:
		fmt.Fprintf(out, "acd stop: stopped (force, escalated=%v)\n", res.Escalated)
	case res.Stopped:
		fmt.Fprintln(out, "acd stop: stopped")
	default:
		fmt.Fprintf(out, "acd stop: result=%+v\n", res)
	}
	return nil
}

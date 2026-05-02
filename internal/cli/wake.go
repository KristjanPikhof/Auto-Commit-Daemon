package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// wakeResult is the JSON payload returned by `acd wake --json`.
type wakeResult struct {
	OK         bool   `json:"ok"`
	DaemonPID  int    `json:"daemon_pid,omitempty"`
	SentSignal bool   `json:"sent_signal"`
	Repo       string `json:"repo"`
	SessionID  string `json:"session_id"`
}

// signalProcess is the injection point used by tests to verify that wake
// would have sent SIGUSR1 without involving real OS signals.
var captureProcessFingerprint = identity.Capture
var killProcess = syscall.Kill

var signalProcess = func(pid int, sig syscall.Signal, expectedFingerprint string) error {
	if pid <= 0 {
		return errors.New("invalid pid")
	}
	if expectedFingerprint != "" {
		fp, err := captureProcessFingerprint(pid)
		if err == nil && daemon.FingerprintToken(fp) != expectedFingerprint {
			return fmt.Errorf("verify process identity for pid %d: fingerprint mismatch", pid)
		}
	}
	return killProcess(pid, sig)
}

func newWakeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wake",
		Short: "Heartbeat refresh + nudge daemon",
		Long: `Refresh one session heartbeat for the resolved repo and request an immediate daemon wake.

The default repo is the current working directory. Harness integrations call wake with --session-id after edits so the daemon can notice work promptly. If the session is absent, wake lazily registers it with harness "other" before signaling the daemon.

Use acd touch when you only need a heartbeat refresh without signaling.`,
		Example: `  acd wake --session-id "$ACD_SESSION_ID"
  acd wake --repo /path/to/repo --session-id "$ACD_SESSION_ID"
  acd wake --session-id "$ACD_SESSION_ID" --json
  acd touch --session-id "$ACD_SESSION_ID"`,
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			jsonOut, _ := c.Flags().GetBool("json")
			sessionID, _ := c.Flags().GetString("session-id")
			return runWake(c.Context(), c.OutOrStdout(), repoFlag, sessionID, jsonOut)
		},
	}
	cmd.Flags().String("session-id", "", "Session identifier (required)")
	return cmd
}

func runWake(ctx context.Context, out io.Writer, repoFlag, sessionID string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sessionID == "" {
		return errors.New("acd wake: --session-id is required")
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd wake: resolve git dir: %w", err)
	}

	clock, err := daemon.AcquireControlLock(gitDir)
	if err != nil {
		return fmt.Errorf("acd wake: acquire control.lock: %w", err)
	}
	defer func() { _ = clock.Release() }()

	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd wake: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Lazy-register if absent (§7.3 step 2). RegisterClient is upsert-
	// shaped and bumps last_seen_ts on every call, which is exactly what
	// wake should do.
	now := nowSecondsFloat()
	ok, err := state.TouchClient(ctx, db, sessionID, now)
	if err != nil {
		return fmt.Errorf("acd wake: touch client: %w", err)
	}
	if !ok {
		// Lazy-register with default harness "other" — adapters call
		// `acd start` first in normal flows; this is just the safety net.
		if err := state.RegisterClient(ctx, db, state.Client{
			SessionID:    sessionID,
			Harness:      "other",
			WatchPID:     sql.NullInt64{},
			WatchFP:      sql.NullString{},
			RegisteredTS: now,
			LastSeenTS:   now,
		}); err != nil {
			return fmt.Errorf("acd wake: lazy register: %w", err)
		}
	}

	if _, err := state.EnqueueFlushRequest(ctx, db, "wake", true,
		sql.NullString{String: sessionID, Valid: true}); err != nil {
		return fmt.Errorf("acd wake: enqueue flush request: %w", err)
	}

	// Send SIGUSR1 if the daemon is alive.
	st, _, err := state.LoadDaemonState(ctx, db)
	if err != nil {
		return fmt.Errorf("acd wake: load daemon state: %w", err)
	}
	sent := false
	pid := 0
	if st.PID > 0 && identity.Alive(st.PID) {
		pid = st.PID
		if err := signalProcess(st.PID, syscall.SIGUSR1, daemonFingerprintToken(st)); err == nil {
			sent = true
		}
	}

	res := wakeResult{
		OK:         true,
		DaemonPID:  pid,
		SentSignal: sent,
		Repo:       repo,
		SessionID:  sessionID,
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if sent {
		fmt.Fprintf(out, "acd wake: nudged daemon pid=%d (session %s)\n", pid, sessionID)
	} else {
		fmt.Fprintf(out, "acd wake: refreshed session %s (daemon not running)\n", sessionID)
	}
	return nil
}

func daemonFingerprintToken(st state.DaemonState) string {
	if !st.DaemonFingerprint.Valid {
		return ""
	}
	return st.DaemonFingerprint.String
}

func nowSecondsFloat() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// silence unused
var _ = os.Getpid

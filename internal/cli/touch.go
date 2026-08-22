package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// touchResult is the JSON payload returned by `acd touch --json`.
type touchResult struct {
	OK            bool    `json:"ok"`
	LastSeenTS    float64 `json:"last_seen_ts"`
	Skipped       bool    `json:"skipped,omitempty"`
	SkippedReason string  `json:"skipped_reason,omitempty"`
	Repo          string  `json:"repo"`
	SessionID     string  `json:"session_id"`
	SoftBoundary  bool    `json:"soft_boundary,omitempty"`
	BoundaryEpoch int64   `json:"boundary_epoch,omitempty"`
	DaemonPID     int     `json:"daemon_pid,omitempty"`
	SentSignal    bool    `json:"sent_signal,omitempty"`
}

func newTouchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "touch",
		Short: "Refresh heartbeat; optionally mark a soft boundary",
		Long: `Refresh one session heartbeat without publishing pending work.

With --soft-boundary, record a durable semantic evaluation boundary and nudge
the daemon. Soft boundaries never bypass atomicity, verification, pause,
Git-operation, branch-generation, conflict, or other replay safety gates.`,
		Example: `  acd touch --session-id "$ACD_SESSION_ID"
  acd touch --soft-boundary --session-id "$ACD_SESSION_ID"
  acd touch --soft-boundary --session-id "$ACD_SESSION_ID" --json`,
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			jsonOut, _ := c.Flags().GetBool("json")
			sessionID, _ := c.Flags().GetString("session-id")
			softBoundary, _ := c.Flags().GetBool("soft-boundary")
			return runTouchWithBoundary(c.Context(), c.OutOrStdout(), repoFlag,
				sessionID, jsonOut, softBoundary)
		},
	}
	cmd.Flags().String("session-id", "", "Session identifier (required)")
	cmd.Flags().Bool("soft-boundary", false, "Record a semantic evaluation boundary and nudge the daemon")
	return cmd
}

func runTouch(ctx context.Context, out io.Writer, repoFlag, sessionID string, jsonOut bool) error {
	return runTouchWithBoundary(ctx, out, repoFlag, sessionID, jsonOut, false)
}

func runTouchWithBoundary(
	ctx context.Context,
	out io.Writer,
	repoFlag string,
	sessionID string,
	jsonOut bool,
	softBoundary bool,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sessionID == "" {
		return errors.New("acd touch: --session-id is required")
	}
	policy, err := evaluateRepoAutodiscoveryPolicy(ctx, "touch", repoFlag, hookAutodiscoveryCaller())
	if err != nil {
		return err
	}
	repo := policy.Worktree.Root
	gitDir := policy.Worktree.GitDir
	if !policy.allowsImplicitState() {
		res := touchResult{
			OK:            true,
			Skipped:       true,
			SkippedReason: policy.skipReason(),
			Repo:          repo,
			SessionID:     sessionID,
			SoftBoundary:  softBoundary,
		}
		if jsonOut {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(res)
		}
		if policy.Disabled {
			fmt.Fprintf(out, "acd touch: skipped for %s (%s; run `acd on --repo %s` to enable protection)\n",
				repo, policy.skipReason(), repo)
		} else {
			fmt.Fprintf(out, "acd touch: skipped for %s (%s; run `acd on --repo %s` to enable protection)\n",
				repo, policy.skipReason(), repo)
		}
		return nil
	}
	clock, err := daemon.AcquireControlLock(gitDir)
	if err != nil {
		if errors.Is(err, daemon.ErrControlLockHeld) {
			// Best-effort heartbeat: another control caller is in flight and
			// will refresh state. Skip cleanly rather than surfacing a hook
			// error to the harness.
			res := touchResult{
				OK:            true,
				Skipped:       true,
				SkippedReason: "control_lock_held",
				Repo:          repo,
				SessionID:     sessionID,
				SoftBoundary:  softBoundary,
			}
			if jsonOut {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(res)
			}
			fmt.Fprintf(out, "acd touch: skipped (control.lock held; another control caller in flight)\n")
			return nil
		}
		return fmt.Errorf("acd touch: acquire control.lock: %w", err)
	}
	defer func() { _ = clock.Release() }()
	if repoDisabledAfterControlLock(policy) {
		res := touchResult{
			OK:            true,
			Skipped:       true,
			SkippedReason: repoAutodiscoverySkipRepoDisabled,
			Repo:          repo,
			SessionID:     sessionID,
			SoftBoundary:  softBoundary,
		}
		if jsonOut {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(res)
		}
		fmt.Fprintf(out, "acd touch: skipped for %s (%s; run `acd on --repo %s` to enable protection)\n",
			repo, repoAutodiscoverySkipRepoDisabled, repo)
		return nil
	}

	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd touch: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	now := nowSecondsFloat()
	ok, err := state.TouchClient(ctx, db, sessionID, now)
	if err != nil {
		return fmt.Errorf("acd touch: touch client: %w", err)
	}
	if !ok {
		// Lazy-register: matches `acd wake` behaviour but without any
		// flush_request or signal — pure heartbeat.
		if err := state.RegisterClient(ctx, db, state.Client{
			SessionID:    sessionID,
			Harness:      "other",
			WatchPID:     sql.NullInt64{},
			WatchFP:      sql.NullString{},
			RegisteredTS: now,
			LastSeenTS:   now,
		}); err != nil {
			return fmt.Errorf("acd touch: lazy register: %w", err)
		}
	}

	res := touchResult{
		OK:           true,
		LastSeenTS:   now,
		Repo:         repo,
		SessionID:    sessionID,
		SoftBoundary: softBoundary,
	}
	if softBoundary {
		st, _, err := state.LoadDaemonState(ctx, db)
		if err != nil {
			return fmt.Errorf("acd touch: load daemon state for soft boundary: %w", err)
		}
		boundary, err := state.AppendIntentActivityBoundary(ctx, db,
			newIntentActivityBoundary(
				state.IntentBoundarySoft, "touch_soft_boundary"))
		if err != nil {
			return fmt.Errorf("acd touch: record soft boundary: %w", err)
		}
		res.BoundaryEpoch = boundary.Epoch
		if st.PID > 0 && identity.Alive(st.PID) {
			res.DaemonPID = st.PID
			if err := signalProcess(st.PID, syscall.SIGUSR1,
				daemonFingerprintToken(st)); err == nil {
				res.SentSignal = true
			}
		}
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if softBoundary {
		if res.SentSignal {
			fmt.Fprintf(out, "acd touch: recorded soft boundary %d and nudged daemon pid=%d (session %s)\n",
				res.BoundaryEpoch, res.DaemonPID, sessionID)
			return nil
		}
		fmt.Fprintf(out, "acd touch: recorded soft boundary %d (daemon not running; session %s)\n",
			res.BoundaryEpoch, sessionID)
		return nil
	}
	fmt.Fprintf(out, "acd touch: refreshed session %s\n", sessionID)
	return nil
}

func newIntentActivityBoundary(
	kind string,
	source string,
) state.IntentActivityBoundary {
	return state.IntentActivityBoundary{
		Kind:   kind,
		Source: source,
	}
}

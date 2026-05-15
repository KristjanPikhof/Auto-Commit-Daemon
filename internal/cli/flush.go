// flush.go implements `acd flush` — explicit drain entrypoint for harness
// idle/stop hooks.
//
// Why this exists: the legacy hook flow was Stop -> `acd touch`, which only
// refreshed last_seen_ts. Pending intent windows still had to wait for
// IntentMaxPendingAge (default 5m) before the daemon would publish a
// commit. Sessions that ended after fewer than IntentMinPending edits would
// commonly leave work uncommitted on disk for the full age trigger.
//
// `acd flush --logical` rewires that to: refresh the heartbeat, enqueue a
// flush_request labeled "flush_logical", and signal the daemon. The daemon's
// existing flush-drain path already sets IntentBypassBatchWait=true on any
// non-empty drain, which causes the planner to evaluate the visible window
// without waiting for IntentMinPending or IntentMaxPendingAge — exactly the
// "treat as age-trigger-now" semantics the harness Stop/idle hooks need.
//
// Without --logical the command behaves like `acd touch`: heartbeat refresh
// only, no signal, no flush enqueue. This keeps the flag explicit so a
// future migration that drops the touch alias does not accidentally change
// caller semantics.
//
// Refusals mirror `acd commit-all`: detached HEAD, in-progress git operation
// (rebase/merge/cherry-pick/bisect), and manual pause marker all block the
// signal+enqueue path. A live daemon is REQUIRED (unlike commit-all which
// requires no daemon) — the daemon is the entity that actually drains; an
// absent daemon makes flush a noop, which we still report as ok=true so the
// hook does not surface a spurious failure to the harness.
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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// flushResult is the JSON payload returned by `acd flush --json`.
//
// LastSeenTS uses `omitempty` so the field is absent from the JSON
// payload when no fresh heartbeat was written on this call (the most
// common case is the control-lock-held skip branch where we never
// actually touched daemon_clients). Encoding 0 there would render as the
// 1970 epoch and mislead any downstream parser computing freshness from
// the value. Callers that need to distinguish "absent" from "zero
// timestamp on disk" must inspect the field with json.RawMessage rather
// than the float zero.
type flushResult struct {
	OK               bool    `json:"ok"`
	LastSeenTS       float64 `json:"last_seen_ts,omitempty"`
	Logical          bool    `json:"logical,omitempty"`
	FlushRequestID   int64   `json:"flush_request_id,omitempty"`
	DaemonPID        int     `json:"daemon_pid,omitempty"`
	SentSignal       bool    `json:"sent_signal,omitempty"`
	Skipped          bool    `json:"skipped,omitempty"`
	SkippedReason    string  `json:"skipped_reason,omitempty"`
	RefusedReason    string  `json:"refused_reason,omitempty"`
	Repo             string  `json:"repo"`
	SessionID        string  `json:"session_id"`
	BypassMinPending bool    `json:"bypass_min_pending,omitempty"`
}

func newFlushCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "flush",
		Short: "Refresh heartbeat; with --logical, drain pending captures now",
		Long: `Refresh the session heartbeat for the resolved repo.

With --logical, also enqueue a labeled flush request and nudge the daemon
so the next replay pass evaluates the visible pending window without
waiting for the IntentMinPending count or the IntentMaxPendingAge timer.
This is the "I am about to stop talking to you" signal harness Stop / idle
hooks send so partial work commits promptly instead of sitting on disk for
the full 5-minute age trigger.

Without --logical the command behaves like acd touch: heartbeat refresh
only, no signal, no flush enqueue.

Refuses to flush on detached HEAD, while a git operation is in progress
(rebase/merge/cherry-pick/bisect), or while a manual pause marker is
present. The heartbeat refresh always runs.`,
		Example: `  acd flush --session-id "$ACD_SESSION_ID" --logical
  acd flush --repo /path/to/repo --session-id "$ACD_SESSION_ID" --logical --json
  acd flush --session-id "$ACD_SESSION_ID"`,
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			jsonOut, _ := c.Flags().GetBool("json")
			sessionID, _ := c.Flags().GetString("session-id")
			logical, _ := c.Flags().GetBool("logical")
			return runFlush(c.Context(), c.OutOrStdout(), repoFlag, sessionID, logical, jsonOut)
		},
	}
	cmd.Flags().String("session-id", "", "Session identifier (required)")
	cmd.Flags().Bool("logical", false, "Enqueue a labeled flush request and nudge the daemon to drain immediately")
	return cmd
}

func runFlush(ctx context.Context, out io.Writer, repoFlag, sessionID string, logical, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sessionID == "" {
		return errors.New("acd flush: --session-id is required")
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd flush: resolve git dir: %w", err)
	}

	clock, err := daemon.AcquireControlLock(gitDir)
	if err != nil {
		if errors.Is(err, daemon.ErrControlLockHeld) {
			// Best-effort heartbeat: another control caller is in flight and
			// will refresh state. Skip cleanly rather than surfacing a hook
			// error to the harness. The in-flight caller's flush enqueue
			// (if any) covers our drain semantics.
			res := flushResult{
				OK:            true,
				Logical:       logical,
				Skipped:       true,
				SkippedReason: "control_lock_held",
				Repo:          repo,
				SessionID:     sessionID,
			}
			return renderFlush(out, res, jsonOut, "acd flush: skipped (control.lock held; another control caller in flight)")
		}
		return fmt.Errorf("acd flush: acquire control.lock: %w", err)
	}
	defer func() { _ = clock.Release() }()

	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd flush: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	now := nowSecondsFloat()
	ok, err := state.TouchClient(ctx, db, sessionID, now)
	if err != nil {
		return fmt.Errorf("acd flush: touch client: %w", err)
	}
	if !ok {
		// Lazy-register: matches `acd wake` behaviour. Pure heartbeat for
		// callers that only want to register the session before flushing.
		if err := state.RegisterClient(ctx, db, state.Client{
			SessionID:    sessionID,
			Harness:      "other",
			WatchPID:     sql.NullInt64{},
			WatchFP:      sql.NullString{},
			RegisteredTS: now,
			LastSeenTS:   now,
		}); err != nil {
			return fmt.Errorf("acd flush: lazy register: %w", err)
		}
	}

	res := flushResult{
		OK:         true,
		LastSeenTS: now,
		Logical:    logical,
		Repo:       repo,
		SessionID:  sessionID,
	}

	if !logical {
		// Heartbeat-only mode mirrors acd touch.
		return renderFlush(out, res, jsonOut, fmt.Sprintf("acd flush: refreshed session %s", sessionID))
	}

	// Logical-flush refusals mirror acd commit-all. Each refusal returns
	// ok=true with refused_reason set so the hook does not surface a
	// spurious nonzero exit to the harness — the daemon will reconcile on
	// its next pass when the operator clears the underlying state.
	branchRef, brErr := git.RunBranchRef(ctx, repo)
	if brErr != nil {
		return fmt.Errorf("acd flush: resolve HEAD branch: %w", brErr)
	}
	if branchRef == "" {
		res.RefusedReason = "detached_head"
		return renderFlush(out, res, jsonOut, fmt.Sprintf("acd flush: refused (detached HEAD); heartbeat refreshed for session %s", sessionID))
	}
	if name, active := daemon.GitOperationInProgress(gitDir); active {
		res.RefusedReason = "git_op_in_progress:" + name
		return renderFlush(out, res, jsonOut, fmt.Sprintf("acd flush: refused (git operation %q in progress); heartbeat refreshed for session %s", name, sessionID))
	}
	if _, present, perr := pausepkg.Read(gitDir); perr != nil {
		return fmt.Errorf("acd flush: read pause marker: %w", perr)
	} else if present {
		res.RefusedReason = "manual_pause"
		return renderFlush(out, res, jsonOut, fmt.Sprintf("acd flush: refused (manual pause marker at %s); heartbeat refreshed for session %s", pausepkg.Path(gitDir), sessionID))
	}

	// Enqueue the labeled flush request. The daemon's drain path treats any
	// non-empty drain as age-trigger-now (IntentBypassBatchWait=true), which
	// is the bypass semantic the task requires.
	frID, err := state.EnqueueFlushRequest(ctx, db, "flush_logical", true,
		sql.NullString{String: sessionID, Valid: true})
	if err != nil {
		return fmt.Errorf("acd flush: enqueue flush request: %w", err)
	}
	res.FlushRequestID = frID
	res.BypassMinPending = true

	// Signal the daemon if alive so it picks the request up on the next
	// tick instead of waiting for the poll interval. SIGUSR1 mirrors wake.
	st, _, err := state.LoadDaemonState(ctx, db)
	if err != nil {
		return fmt.Errorf("acd flush: load daemon state: %w", err)
	}
	if st.PID > 0 && identity.Alive(st.PID) {
		res.DaemonPID = st.PID
		if err := signalProcess(st.PID, syscall.SIGUSR1, daemonFingerprintToken(st)); err == nil {
			res.SentSignal = true
		}
	}

	if res.SentSignal {
		return renderFlush(out, res, jsonOut, fmt.Sprintf("acd flush: nudged daemon pid=%d for logical flush (session %s, request %d)", res.DaemonPID, sessionID, frID))
	}
	return renderFlush(out, res, jsonOut, fmt.Sprintf("acd flush: enqueued logical flush %d (daemon not running; will drain on next start)", frID))
}

// renderFlush emits the JSON payload or the human-readable line. Returning
// the result of the encoder/print so callers can surface IO errors.
func renderFlush(out io.Writer, res flushResult, jsonOut bool, line string) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	_, err := fmt.Fprintln(out, line)
	return err
}

package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
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
}

func newTouchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "touch",
		Short: "Heartbeat refresh only (no signal)",
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			jsonOut, _ := c.Flags().GetBool("json")
			sessionID, _ := c.Flags().GetString("session-id")
			return runTouch(c.Context(), c.OutOrStdout(), repoFlag, sessionID, jsonOut)
		},
	}
	cmd.Flags().String("session-id", "", "Session identifier (required)")
	return cmd
}

func runTouch(ctx context.Context, out io.Writer, repoFlag, sessionID string, jsonOut bool) error {
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
		}
		if jsonOut {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(res)
		}
		if policy.Disabled {
			fmt.Fprintf(out, "acd touch: skipped for %s (%s; run `acd repo enable --repo %s` to allow ACD to manage it)\n",
				repo, policy.skipReason(), repo)
		} else {
			fmt.Fprintf(out, "acd touch: skipped for %s (%s; run `acd repo init --repo %s` to register explicitly)\n",
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
		OK:         true,
		LastSeenTS: now,
		Repo:       repo,
		SessionID:  sessionID,
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	fmt.Fprintf(out, "acd touch: refreshed session %s\n", sessionID)
	return nil
}

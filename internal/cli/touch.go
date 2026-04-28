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
	OK         bool    `json:"ok"`
	LastSeenTS float64 `json:"last_seen_ts"`
	Repo       string  `json:"repo"`
	SessionID  string  `json:"session_id"`
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
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd touch: resolve git dir: %w", err)
	}
	clock, err := daemon.AcquireControlLock(gitDir)
	if err != nil {
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

package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"

	_ "modernc.org/sqlite"
)

// gcStaleAge is the threshold beyond which a dead-daemon entry is pruned
// per §7.11. 30 days matches the spec.
const gcStaleAge = 30 * 24 * time.Hour

// gcDrop is one entry that gc removed (or would remove). Reason is one of
// "repo-missing" | "state-db-missing" | "daemon-dead-30d".
type gcDrop struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// gcReport is the §7.11 JSON shape: dropped[] + kept count.
type gcReport struct {
	Dropped []gcDrop `json:"dropped"`
	Kept    int      `json:"kept"`
}

func newGCCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "gc",
		Short: "Prune central registry of dead/missing repo entries",
		Long: `Prune central registry entries for repos that are missing, have no state DB, or have had a dead daemon for at least 30 days.

This command does not edit repo state.db files or captured events. Use acd list first to see registered repos, and --json when scripting cleanup.`,
		Example: `  acd gc
  acd gc --json
  acd list`,
		RunE: func(cmd *cobra.Command, args []string) error {
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runGC(cmd.Context(), cmd.OutOrStdout(), jsonOut)
		},
	}
}

func runGC(ctx context.Context, out io.Writer, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd gc: resolve paths: %w", err)
	}

	var report gcReport
	now := time.Now()
	err = central.WithLock(roots, func(reg *central.Registry) error {
		kept := make([]central.RepoRecord, 0, len(reg.Repos))
		for _, rec := range reg.Repos {
			if reason, drop := gcReason(ctx, rec, now); drop {
				report.Dropped = append(report.Dropped, gcDrop{Path: rec.Path, Reason: reason})
				continue
			}
			kept = append(kept, rec)
		}
		reg.Repos = kept
		report.Kept = len(kept)
		return nil
	})
	if err != nil {
		return fmt.Errorf("acd gc: %w", err)
	}

	if jsonOut {
		// Ensure non-nil slice so JSON renders [] not null.
		if report.Dropped == nil {
			report.Dropped = []gcDrop{}
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	if len(report.Dropped) == 0 {
		fmt.Fprintf(out, "acd gc: nothing to prune (kept %d repos)\n", report.Kept)
		return nil
	}
	fmt.Fprintf(out, "acd gc: pruned %d entries, kept %d\n", len(report.Dropped), report.Kept)
	for _, d := range report.Dropped {
		fmt.Fprintf(out, "  - %s  (%s)\n", d.Path, d.Reason)
	}
	return nil
}

// gcReason inspects a repo record and returns (reason, true) when it is a
// pruning candidate. Order: repo dir > state.db > dead daemon.
func gcReason(ctx context.Context, rec central.RepoRecord, now time.Time) (string, bool) {
	if !fileExists(rec.Path) {
		return "repo-missing", true
	}
	if !fileExists(rec.StateDB) {
		return "state-db-missing", true
	}
	// Dead daemon AND last_heartbeat older than 30 days. We open the state
	// DB read-only and pull pid + heartbeat_ts; any error is non-fatal —
	// we keep the entry on the conservative side.
	pid, hb, err := readDaemonPidAndHeartbeat(ctx, rec.StateDB)
	if err != nil {
		return "", false
	}
	if pid > 0 && identity.Alive(pid) {
		return "", false
	}
	if hb == 0 {
		// Never had a heartbeat — only prune if registry's last_seen is
		// also stale > 30d, otherwise this could be a brand-new repo.
		if rec.LastSeenTS > 0 && now.Sub(time.Unix(rec.LastSeenTS, 0)) > gcStaleAge {
			return "daemon-dead-30d", true
		}
		return "", false
	}
	if now.Sub(time.Unix(int64(hb), 0)) > gcStaleAge {
		return "daemon-dead-30d", true
	}
	return "", false
}

// readDaemonPidAndHeartbeat reads the singleton daemon_state row from a
// read-only DSN, never running migrations or touching the file.
func readDaemonPidAndHeartbeat(ctx context.Context, dbPath string) (pid int, heartbeatTS float64, err error) {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	dsn := "file:" + dbPath + "?" + q.Encode()
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return 0, 0, fmt.Errorf("open: %w", err)
	}
	defer conn.Close()
	if err := conn.PingContext(ctx); err != nil {
		return 0, 0, fmt.Errorf("ping: %w", err)
	}
	row := conn.QueryRowContext(ctx,
		`SELECT pid, heartbeat_ts FROM daemon_state WHERE id = 1`)
	if err := row.Scan(&pid, &heartbeatTS); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("scan: %w", err)
	}
	return pid, heartbeatTS, nil
}

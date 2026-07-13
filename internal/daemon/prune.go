// prune.go drops stale terminal capture_events rows so the per-repo state DB
// does not grow without bound. Terminal failure rows are retained while they
// still form an active replay barrier for later pending events.
//
// Default retention is 7 days; override via env ACD_EVENT_RETENTION_DAYS.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// EnvEventRetentionDays is the env knob for capture_events retention.
const EnvEventRetentionDays = "ACD_EVENT_RETENTION_DAYS"

// DefaultEventRetention is the default retention window for published
// capture_events (7 days).
const DefaultEventRetention = 7 * 24 * time.Hour

// resolveEventRetention consults EnvEventRetentionDays + opt + default.
// opt > 0 wins over the env; env wins over the default.
func resolveEventRetention(opt time.Duration) time.Duration {
	if opt > 0 {
		return opt
	}
	if env := os.Getenv(EnvEventRetentionDays); env != "" {
		if days, err := strconv.ParseFloat(env, 64); err == nil && days > 0 {
			return time.Duration(days * float64(24*time.Hour))
		}
	}
	return DefaultEventRetention
}

// PruneCaptureEvents drops terminal capture_events older than retention.
// Returns the number of rows removed.
func PruneCaptureEvents(ctx context.Context, repoDir string, db *state.DB, now time.Time, retention time.Duration) (int, error) {
	if repoDir == "" {
		return 0, fmt.Errorf("daemon: PruneCaptureEvents: empty repoDir")
	}
	if db == nil {
		return 0, fmt.Errorf("daemon: PruneCaptureEvents: nil db")
	}
	r := resolveEventRetention(retention)
	cutoff := float64(now.Add(-r).UnixNano()) / 1e9
	published, err := state.PrunePublishedEventsBefore(ctx, db, cutoff)
	if err != nil {
		return 0, err
	}
	protected, err := pruneVerifiedRecoverySnapshotEvents(ctx, repoDir, db, cutoff)
	if err != nil {
		return published, err
	}
	terminal, err := state.PruneTerminalEventsBefore(ctx, db, cutoff)
	if err != nil {
		return published + protected, err
	}
	return published + protected + terminal, nil
}

func pruneVerifiedRecoverySnapshotEvents(ctx context.Context, repoDir string, db *state.DB, cutoff float64) (int, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT DISTINCT snapshot.id, snapshot.commit_oid, snapshot.recovery_ref
FROM recovery_snapshots snapshot
JOIN recovery_snapshot_events member ON member.snapshot_id = snapshot.id
JOIN capture_events event ON event.seq = member.event_seq
WHERE event.state IN ('published', 'recovered') AND event.captured_ts < ?
ORDER BY snapshot.id`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("daemon: list protected prune candidates: %w", err)
	}
	type candidate struct {
		id        int64
		commitOID string
		ref       string
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.id, &item.commitOID, &item.ref); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("daemon: scan protected prune candidate: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("daemon: iterate protected prune candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("daemon: close protected prune candidates: %w", err)
	}

	pruned := 0
	for _, item := range candidates {
		actual, err := git.RevParse(ctx, repoDir, item.ref)
		if errors.Is(err, git.ErrRefNotFound) || (err == nil && actual != item.commitOID) {
			continue
		}
		if err != nil {
			return pruned, fmt.Errorf("daemon: verify protected snapshot %d ref %s: %w", item.id, item.ref, err)
		}
		var n int
		err = git.WithLockedRecoveryRef(ctx, repoDir, item.ref, item.commitOID, func() error {
			var pruneErr error
			n, pruneErr = state.PruneRecoverySnapshotEventsBefore(ctx, db, item.id, cutoff)
			return pruneErr
		})
		if err != nil {
			return pruned, fmt.Errorf("daemon: prune protected snapshot %d under ref %s: %w", item.id, item.ref, err)
		}
		pruned += n
	}
	return pruned, nil
}

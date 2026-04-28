// prune.go drops stale capture_events rows so the per-repo state DB does not
// grow without bound. Pruned rows are restricted to terminal-success
// ('published') so operators can still inspect failures.
//
// Default retention is 7 days; override via env ACD_EVENT_RETENTION_DAYS.
package daemon

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

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

// PruneCaptureEvents drops 'published' capture_events older than retention.
// Returns the number of rows removed.
func PruneCaptureEvents(ctx context.Context, db *state.DB, now time.Time, retention time.Duration) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("daemon: PruneCaptureEvents: nil db")
	}
	r := resolveEventRetention(retention)
	cutoff := float64(now.Add(-r).UnixNano()) / 1e9
	return state.PrunePublishedEventsBefore(ctx, db, cutoff)
}

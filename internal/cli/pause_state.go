package cli

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"time"

	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
)

const replayPausedUntilMetaKey = "replay.paused_until"

type pauseInfo struct {
	Source           string `json:"source"`
	Reason           string `json:"reason,omitempty"`
	SetAt            string `json:"set_at,omitempty"`
	ExpiresAt        string `json:"expires_at,omitempty"`
	RemainingSeconds int64  `json:"remaining_seconds,omitempty"`
}

func pauseInfoForRepo(ctx context.Context, conn *sql.DB, stateDBPath string, now time.Time) (*pauseInfo, error) {
	gitDir := gitDirFromStateDB(stateDBPath)
	marker, ok, err := pausepkg.Read(gitDir)
	switch {
	case errors.Is(err, pausepkg.ErrMalformed):
		// Match daemon behavior: malformed manual markers fail open. Do not
		// surface as a pause; fall through to the rewind-grace probe.
	case err != nil:
		return nil, err
	case ok:
		// A marker on disk is operator intent. Surface it even if the TTL
		// has expired so status/list never silently hide a stuck marker.
		info, perr := pauseInfoFromMarker(marker, now)
		if perr != nil {
			return nil, perr
		}
		if info != nil {
			return info, nil
		}
	}

	raw, ok, err := metaLookup(ctx, conn, replayPausedUntilMetaKey)
	if err != nil || !ok || strings.TrimSpace(raw) == "" {
		return nil, err
	}
	until, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil || !until.After(now.UTC()) {
		return nil, nil
	}
	return &pauseInfo{
		Source:           "rewind_grace",
		Reason:           "rewind grace",
		ExpiresAt:        until.UTC().Format(time.RFC3339),
		RemainingSeconds: int64(until.Sub(now.UTC()).Seconds()),
	}, nil
}

// pauseInfoFromMarker projects a manual pause marker into pauseInfo. An
// active TTL produces Source="manual"; an expired TTL produces
// Source="manual_expired" so operators can see the stuck-marker state. A
// malformed RFC3339 expires_at is propagated to the caller (no silent drop).
func pauseInfoFromMarker(marker pausepkg.Marker, now time.Time) (*pauseInfo, error) {
	info := &pauseInfo{
		Source: "manual",
		Reason: marker.Reason,
		SetAt:  marker.SetAt,
	}
	if marker.ExpiresAt == nil || strings.TrimSpace(*marker.ExpiresAt) == "" {
		return info, nil
	}
	expiresAt, err := time.Parse(time.RFC3339, strings.TrimSpace(*marker.ExpiresAt))
	if err != nil {
		return nil, err
	}
	info.ExpiresAt = expiresAt.UTC().Format(time.RFC3339)
	if !expiresAt.After(now.UTC()) {
		info.Source = "manual_expired"
		info.RemainingSeconds = 0
		return info, nil
	}
	info.RemainingSeconds = int64(expiresAt.Sub(now.UTC()).Seconds())
	return info, nil
}

func pauseStatusNote(info *pauseInfo) string {
	if info == nil {
		return ""
	}
	switch info.Source {
	case "manual":
		return "manual"
	case "manual_expired":
		return "manual pause expired (marker still on disk; run acd resume --yes to remove)"
	case "rewind_grace":
		if info.ExpiresAt != "" {
			return "rewind grace, expires in " + formatDurationCompact(time.Duration(info.RemainingSeconds)*time.Second)
		}
		return "rewind grace"
	}
	return strings.ReplaceAll(info.Source, "_", " ")
}

// gitDirFromStateDB returns the gitDir that owns a per-repo state.db. It
// pins the on-disk layout `<gitDir>/acd/state.db`. Any change to that layout
// must update this helper and the table-driven test that asserts it.
func gitDirFromStateDB(stateDBPath string) string {
	return filepath.Dir(filepath.Dir(stateDBPath))
}

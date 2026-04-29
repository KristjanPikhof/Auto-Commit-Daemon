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
	gitDir := filepath.Dir(filepath.Dir(stateDBPath))
	if marker, ok, err := pausepkg.Read(gitDir); errors.Is(err, pausepkg.ErrMalformed) {
		// Match daemon behavior: malformed manual markers fail open.
	} else if err != nil {
		return nil, err
	} else if ok {
		info, err := pauseInfoFromMarker(marker, now)
		if err == nil && info != nil {
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
	if !expiresAt.After(now.UTC()) {
		return nil, nil
	}
	info.ExpiresAt = expiresAt.UTC().Format(time.RFC3339)
	info.RemainingSeconds = int64(expiresAt.Sub(now.UTC()).Seconds())
	return info, nil
}

func pauseStatusNote(info *pauseInfo) string {
	if info == nil {
		return ""
	}
	if info.Source == "manual" {
		return "manual"
	}
	if info.ExpiresAt != "" {
		return "rewind grace, expires in " + formatDurationCompact(time.Duration(info.RemainingSeconds)*time.Second)
	}
	return strings.ReplaceAll(info.Source, "_", " ")
}

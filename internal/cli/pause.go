package cli

import (
	"errors"
	"log"

	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
)

// PauseMarker is the durable gitDir/acd/paused file format.
type PauseMarker = pausepkg.Marker

// ReadMarker reads gitDir/acd/paused. It returns ok=false when the marker is
// absent or malformed, matching the daemon's best-effort pause-marker handling.
func ReadMarker(gitDir string) (PauseMarker, bool, error) {
	marker, ok, err := pausepkg.Read(gitDir)
	if errors.Is(err, pausepkg.ErrMalformed) {
		log.Printf("acd pause: ignoring malformed pause marker %s: %v", pauseMarkerPath(gitDir), err)
		return PauseMarker{}, false, nil
	}
	if err != nil {
		return PauseMarker{}, false, err
	}
	return marker, ok, nil
}

func pauseMarkerPath(gitDir string) string {
	return pausepkg.Path(gitDir)
}

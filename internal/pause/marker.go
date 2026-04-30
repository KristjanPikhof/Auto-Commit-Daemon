package pause

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrMalformed reports a pause marker that exists but is not valid JSON.
var ErrMalformed = errors.New("pause: malformed marker")

// ErrNonRegularTarget reports that the destination path exists but is not a
// regular file. The marker writer never follows symlinks or clobbers
// directories/devices/sockets/named pipes.
var ErrNonRegularTarget = errors.New("pause: marker target is not a regular file: refusing overwrite")

// Marker is the durable gitDir/acd/paused file format.
type Marker struct {
	Reason    string  `json:"reason"`
	SetAt     string  `json:"set_at"`
	SetBy     string  `json:"set_by"`
	ExpiresAt *string `json:"expires_at"`
}

func Path(gitDir string) string {
	return filepath.Join(gitDir, "acd", "paused")
}

func Read(gitDir string) (Marker, bool, error) {
	path := Path(gitDir)
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Marker{}, false, nil
	}
	if err != nil {
		return Marker{}, false, err
	}
	var marker Marker
	if err := json.Unmarshal(body, &marker); err != nil {
		return Marker{}, false, fmt.Errorf("%w: %s: %v", ErrMalformed, path, err)
	}
	return marker, true, nil
}

// Write durably writes the pause marker at path using a temp+rename atomic
// publish. It refuses to follow symlinks at the destination and refuses to
// overwrite any non-regular file that already exists at the destination.
//
// Returned bool is true when an existing regular pause-marker file was
// replaced; it is false when the marker did not previously exist. The bool is
// only meaningful when err == nil.
func Write(path string, marker Marker, overwrite bool) (bool, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, err
	}
	body, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return false, err
	}
	body = append(body, '\n')

	// Lstat-before so we can: (a) reject non-regular destinations, including
	// symlinks; (b) honor !overwrite without race-prone O_EXCL on the final
	// path; (c) report whether we actually replaced an existing marker.
	overwrote := false
	if info, err := os.Lstat(path); err == nil {
		mode := info.Mode()
		if mode&os.ModeSymlink != 0 || !mode.IsRegular() {
			return false, fmt.Errorf("%w: %s", ErrNonRegularTarget, path)
		}
		if !overwrite {
			return false, fmt.Errorf("pause: marker already exists at %s: %w", path, os.ErrExist)
		}
		overwrote = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	tmp, err := openExclusiveTemp(dir)
	if err != nil {
		return false, err
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		// Best-effort: remove the orphaned temp file. Ignore errors; the
		// caller already has a real error to report.
		_ = os.Remove(tmpPath)
	}

	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		cleanup()
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return false, err
	}

	// POSIX rename is atomic on the same filesystem and replaces an existing
	// regular destination in a single step. It does not follow a symlink at
	// the destination — a pre-existing symlink at path was already rejected
	// by the Lstat-before check above.
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return false, err
	}
	return overwrote, nil
}

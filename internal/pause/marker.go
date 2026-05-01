package pause

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// ErrMalformed reports a pause marker that exists but is not valid JSON.
var ErrMalformed = errors.New("pause: malformed marker")

// ErrNonRegularTarget reports that the destination path exists but is not a
// regular file. The marker writer never follows symlinks or clobbers
// directories/devices/sockets/named pipes.
var ErrNonRegularTarget = errors.New("pause: marker target is not a regular file: refusing overwrite")

// ErrNonRegularSource reports that the on-disk marker exists but is not a
// regular file (e.g. a FIFO, socket, device, directory, or symlink). The
// reader refuses to consume such a path because it could block forever or
// follow an attacker-controlled redirection.
var ErrNonRegularSource = errors.New("pause: marker source is not a regular file: refusing to read")

// maxMarkerSize caps the bytes Read will consume before bailing out. The
// marker is a tiny JSON object (well under 1KB in practice); 64KiB is a
// generous, attacker-resistant ceiling.
const maxMarkerSize = 64 * 1024

// Marker is the durable gitDir/acd/paused file format.
type Marker struct {
	Reason    string  `json:"reason"`
	SetAt     string  `json:"set_at"`
	SetBy     string  `json:"set_by"`
	ExpiresAt *string `json:"expires_at"`
	// Version is a forward-compatibility hint. Default 0 means "v0 schema",
	// which is the only schema the codebase reads or writes today. Reserved
	// for a future migration; no behavior change is gated on it yet.
	Version int `json:"version,omitempty"`
}

func Path(gitDir string) string {
	return filepath.Join(gitDir, "acd", "paused")
}

// Read loads the durable pause marker. It refuses to follow a symlink at the
// destination (O_NOFOLLOW), refuses to read non-regular files (FIFO, socket,
// device, directory), and caps the read at maxMarkerSize so a malicious
// large/streaming file cannot exhaust memory.
func Read(gitDir string) (Marker, bool, error) {
	path := Path(gitDir)
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Marker{}, false, nil
		}
		// O_NOFOLLOW on a symlink yields ELOOP on linux/darwin; surface as
		// a non-regular-source error so callers can distinguish it from a
		// transient I/O failure.
		if errors.Is(err, unix.ELOOP) {
			return Marker{}, false, fmt.Errorf("%w: %s: %v", ErrNonRegularSource, path, err)
		}
		return Marker{}, false, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return Marker{}, false, err
	}
	if !info.Mode().IsRegular() {
		return Marker{}, false, fmt.Errorf("%w: %s", ErrNonRegularSource, path)
	}

	body, err := io.ReadAll(io.LimitReader(f, maxMarkerSize+1))
	if err != nil {
		return Marker{}, false, err
	}
	if len(body) > maxMarkerSize {
		return Marker{}, false, fmt.Errorf("%w: %s: marker exceeds %d bytes", ErrMalformed, path, maxMarkerSize)
	}

	var marker Marker
	if err := json.Unmarshal(body, &marker); err != nil {
		return Marker{}, false, fmt.Errorf("%w: %s: %v", ErrMalformed, path, err)
	}
	return marker, true, nil
}

// Write durably publishes the pause marker at path.
//
//   - When overwrite=false the publish is race-free: we open the destination
//     with O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW so two concurrent writers cannot
//     both succeed. The losing writer sees os.ErrExist.
//   - When overwrite=true we keep the temp+rename atomic-publish path so the
//     file content is never observed half-written, and we fsync the parent
//     directory after the rename so the rename survives a crash.
//
// In both modes we refuse to follow a pre-existing symlink at the destination
// and refuse to clobber any non-regular file that already exists there.
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

	if !overwrite {
		return writeExclusive(path, body)
	}
	return writeOverwrite(dir, path, body)
}

// writeExclusive publishes the marker via a single O_CREAT|O_EXCL|O_NOFOLLOW
// open. This is the race-free path used when overwrite=false: two concurrent
// callers cannot both win the create, and the loser sees os.ErrExist.
func writeExclusive(path string, body []byte) (bool, error) {
	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY | unix.O_NOFOLLOW
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		// If a non-regular destination exists, surface the dedicated error
		// so callers can distinguish "operator already paused" (ErrExist)
		// from "something weird is sitting at the marker path".
		if errors.Is(err, unix.ELOOP) {
			return false, fmt.Errorf("%w: %s: %v", ErrNonRegularTarget, path, err)
		}
		if errors.Is(err, os.ErrExist) {
			// Differentiate non-regular existing entries (dir, fifo,
			// socket, device) from a regular existing marker. The regular
			// case is the operator-already-paused signal we want callers
			// to see as os.ErrExist.
			if info, statErr := os.Lstat(path); statErr == nil {
				mode := info.Mode()
				if mode&os.ModeSymlink != 0 || !mode.IsRegular() {
					return false, fmt.Errorf("%w: %s", ErrNonRegularTarget, path)
				}
			}
			return false, fmt.Errorf("pause: marker already exists at %s: %w", path, os.ErrExist)
		}
		return false, err
	}
	if _, werr := f.Write(body); werr != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return false, werr
	}
	if serr := f.Sync(); serr != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return false, serr
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(path)
		return false, cerr
	}
	if err := fsyncDir(filepath.Dir(path)); err != nil {
		return false, err
	}
	return false, nil
}

// writeOverwrite publishes the marker via temp+rename so readers always
// observe a fully-formed JSON document. The destination must be a regular
// file (or absent); symlinks/devices/dirs are rejected.
func writeOverwrite(dir, path string, body []byte) (bool, error) {
	overwrote := false
	if info, err := os.Lstat(path); err == nil {
		mode := info.Mode()
		if mode&os.ModeSymlink != 0 || !mode.IsRegular() {
			return false, fmt.Errorf("%w: %s", ErrNonRegularTarget, path)
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
	// fsync the parent directory so the rename itself is durable. On
	// filesystems that don't support directory fsync (tmpfs in some CI
	// configs) fsyncDir tolerates EINVAL/EROFS internally.
	if err := fsyncDir(dir); err != nil {
		return overwrote, err
	}
	return overwrote, nil
}

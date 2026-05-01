//go:build darwin || linux

package pause

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// openExclusiveTemp creates a fresh paused.tmp.<pid>.<n> file in dir with
// O_NOFOLLOW so a malicious symlink at the temp path cannot redirect the
// write. The caller is responsible for closing and removing the file.
func openExclusiveTemp(dir string) (*os.File, error) {
	pid := os.Getpid()
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL | unix.O_NOFOLLOW
	for n := 0; n < 1024; n++ {
		name := filepath.Join(dir, fmt.Sprintf("paused.tmp.%d.%d", pid, n))
		f, err := os.OpenFile(name, flags, 0o600)
		if err == nil {
			return f, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("pause: unable to create temp marker in %s after 1024 attempts", dir)
}

// fsyncDir flushes the parent directory's metadata so the most recent
// rename/create is durable across a crash. Tmpfs and a handful of FUSE
// filesystems return EINVAL or EROFS for fsync on a directory; treat those
// as non-fatal so CI runs on tmpfs do not regress.
func fsyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EROFS) {
			return nil
		}
		return err
	}
	return nil
}

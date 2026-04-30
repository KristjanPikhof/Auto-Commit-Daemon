//go:build !darwin && !linux

package pause

import (
	"fmt"
	"os"
	"path/filepath"
)

// openExclusiveTemp creates a fresh paused.tmp.<pid>.<n> file in dir using
// O_EXCL. Platforms without a portable O_NOFOLLOW (notably Windows) get the
// best-effort O_EXCL guarantee; ACD does not officially support those
// platforms today, so this is a safe fallback rather than a security claim.
func openExclusiveTemp(dir string) (*os.File, error) {
	pid := os.Getpid()
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
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

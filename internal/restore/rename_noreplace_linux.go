//go:build linux

package restore

import "golang.org/x/sys/unix"

func renameatNoReplace(oldDir int, oldPath string, newDir int, newPath string) error {
	return unix.Renameat2(oldDir, oldPath, newDir, newPath, unix.RENAME_NOREPLACE)
}

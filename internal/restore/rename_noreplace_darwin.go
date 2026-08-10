//go:build darwin

package restore

import "golang.org/x/sys/unix"

func renameatNoReplace(oldDir int, oldPath string, newDir int, newPath string) error {
	return unix.RenameatxNp(oldDir, oldPath, newDir, newPath, unix.RENAME_EXCL)
}

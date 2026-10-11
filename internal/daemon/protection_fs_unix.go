//go:build darwin || linux

package daemon

import (
	"errors"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// protectionTree anchors reads beneath the repository, rejecting symlinks in
// every ancestor. Frontier entries hold paths, never a descriptor per directory.
type protectionTree struct{ root int }

func openProtectionTree(root string) (*protectionTree, error) {
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return &protectionTree{root: fd}, nil
}

func (t *protectionTree) Close() error { return unix.Close(t.root) }

func (t *protectionTree) openDir(relative string) (int, error) {
	// Opening "." gives each enumeration its own offset; Dup would share it.
	current, err := unix.Openat(t.root, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil || relative == "" || relative == "." {
		return current, err
	}
	for _, name := range strings.Split(relative, "/") {
		if name == "" || name == "." || name == ".." {
			unix.Close(current)
			return -1, errors.New("protection: invalid relative directory")
		}
		next, err := unix.Openat(current, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(current)
		if err != nil {
			return -1, err
		}
		current = next
	}
	return current, nil
}

func (t *protectionTree) openParent(relative string) (int, string, error) {
	fd, err := t.openDir(path.Dir(relative))
	return fd, path.Base(relative), err
}

func (t *protectionTree) readDir(relative string) ([]os.FileInfo, error) {
	fd, err := t.openDir(relative)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), relative)
	// Check a nested repository boundary only after its directory survived
	// privacy and batched Gitignore pruning, using the same anchored descriptor.
	if relative != "" {
		var stat unix.Stat_t
		err := unix.Fstatat(fd, ".git", &stat, unix.AT_SYMLINK_NOFOLLOW)
		if err == nil {
			return nil, file.Close()
		}
		if !errors.Is(err, unix.ENOENT) {
			return nil, errors.Join(err, file.Close())
		}
	}
	// Readdir returns metadata from fstatat on this directory descriptor.
	// ReadDir's DirEntry.Info can instead resolve a later absolute path.
	children, readErr := file.Readdir(-1)
	return children, errors.Join(readErr, file.Close())
}

func (t *protectionTree) openFile(relative string) (*os.File, error) {
	parent, name, err := t.openParent(relative)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parent)
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), relative), nil
}

func (t *protectionTree) readlink(relative string) (string, error) {
	parent, name, err := t.openParent(relative)
	if err != nil {
		return "", err
	}
	defer unix.Close(parent)
	for size := 256; size <= 1<<20; size *= 2 {
		buffer := make([]byte, size)
		n, err := unix.Readlinkat(parent, name, buffer)
		if err != nil {
			return "", err
		}
		if n < len(buffer) {
			return string(buffer[:n]), nil
		}
	}
	return "", errors.New("protection: symlink target exceeds 1 MiB")
}

func (t *protectionTree) matches(relative string, info os.FileInfo) bool {
	parent, name, err := t.openParent(relative)
	if err != nil {
		return false
	}
	defer unix.Close(parent)
	var stat unix.Stat_t
	if unix.Fstatat(parent, name, &stat, unix.AT_SYMLINK_NOFOLLOW) != nil {
		return false
	}
	before, ok := info.Sys().(*syscall.Stat_t)
	return ok && before.Ino == stat.Ino && before.Dev == stat.Dev &&
		uint32(before.Mode) == uint32(stat.Mode) && info.Size() == stat.Size &&
		info.ModTime().Equal(time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec))
}

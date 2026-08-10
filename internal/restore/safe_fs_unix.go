//go:build darwin || linux

package restore

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// safeTree resolves every path component relative to an already-open root
// directory. Ancestor symlinks are rejected by O_NOFOLLOW, closing the race
// between restore preview and filesystem mutation.
type safeTree struct {
	root int
}

type safePathState struct {
	Exists    bool
	Mode      fs.FileMode
	Directory bool
	Symlink   bool
	Body      []byte
}

type safePathClaim struct {
	parent    int
	original  string
	temporary string
	state     safePathState
	exists    bool
}

func openSafeTree(root string) (*safeTree, error) {
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("restore: open safe root %s: %w", root, err)
	}
	return &safeTree{root: fd}, nil
}

func (t *safeTree) Close() error {
	if t == nil || t.root < 0 {
		return nil
	}
	err := unix.Close(t.root)
	t.root = -1
	return err
}

func (t *safeTree) openDir(relative string, create bool, mode fs.FileMode) (int, error) {
	current, err := unix.Dup(t.root)
	if err != nil {
		return -1, err
	}
	if relative == "" || relative == "." {
		return current, nil
	}
	clean, err := cleanRelativePath(relative)
	if err != nil {
		unix.Close(current)
		return -1, err
	}
	for _, component := range strings.Split(clean, "/") {
		next, openErr := unix.Openat(current, component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(openErr, unix.ENOENT) && create {
			if mkdirErr := unix.Mkdirat(current, component, uint32(mode.Perm())); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				unix.Close(current)
				return -1, mkdirErr
			}
			if syncErr := unix.Fsync(current); syncErr != nil {
				unix.Close(current)
				return -1, syncErr
			}
			next, openErr = unix.Openat(current, component,
				unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		unix.Close(current)
		if openErr != nil {
			return -1, fmt.Errorf("restore: open directory component %s: %w", component, openErr)
		}
		current = next
	}
	return current, nil
}

func (t *safeTree) openParent(relative string, create bool) (int, string, error) {
	clean, err := cleanRelativePath(relative)
	if err != nil {
		return -1, "", err
	}
	parent, name := filepath.ToSlash(filepath.Dir(clean)), filepath.Base(clean)
	fd, err := t.openDir(parent, create, 0o755)
	return fd, name, err
}

func (t *safeTree) read(relative string) (safePathState, error) {
	parent, name, err := t.openParent(relative, false)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return safePathState{}, nil
		}
		return safePathState{}, err
	}
	defer unix.Close(parent)
	return readPathAt(parent, name)
}

func readPathAt(parent int, name string) (safePathState, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(parent, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return safePathState{}, nil
		}
		return safePathState{}, err
	}
	state := safePathState{Exists: true, Mode: modeFromStat(uint32(stat.Mode))}
	state.Directory = state.Mode.IsDir()
	state.Symlink = state.Mode&fs.ModeSymlink != 0
	if state.Directory {
		return state, nil
	}
	if state.Symlink {
		body, readErr := readlinkAt(parent, name)
		state.Body = body
		return state, readErr
	}
	if !state.Mode.IsRegular() {
		return safePathState{}, fmt.Errorf("restore: unsupported file type at %s", name)
	}
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return safePathState{}, err
	}
	file := os.NewFile(uintptr(fd), name)
	state.Body, err = io.ReadAll(file)
	return state, errors.Join(err, file.Close())
}

func (t *safeTree) claim(relative string) (*safePathClaim, error) {
	parent, name, err := t.openParent(relative, false)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return &safePathClaim{parent: -1}, nil
		}
		return nil, err
	}
	temporary, err := safeTemporaryName()
	if err != nil {
		unix.Close(parent)
		return nil, err
	}
	claim := &safePathClaim{parent: parent, original: name, temporary: temporary}
	if err := renameatNoReplace(parent, name, parent, temporary); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return claim, nil
		}
		_ = claim.close()
		return nil, err
	}
	claim.exists = true
	claim.state, err = readPathAt(parent, temporary)
	if err != nil {
		return nil, errors.Join(err, claim.restore())
	}
	return claim, nil
}

func (c *safePathClaim) restore() error {
	if c == nil || !c.exists {
		return c.close()
	}
	err := renameatNoReplace(c.parent, c.temporary, c.parent, c.original)
	if err == nil {
		err = unix.Fsync(c.parent)
	}
	return errors.Join(err, c.close())
}

func (c *safePathClaim) discard() error {
	if c == nil || !c.exists {
		return c.close()
	}
	flags := 0
	if c.state.Directory {
		flags = unix.AT_REMOVEDIR
	}
	if err := unix.Unlinkat(c.parent, c.temporary, flags); err != nil {
		return errors.Join(err, c.restore())
	}
	err := unix.Fsync(c.parent)
	return errors.Join(err, c.close())
}

func (c *safePathClaim) close() error {
	if c == nil || c.parent < 0 {
		return nil
	}
	err := unix.Close(c.parent)
	c.parent = -1
	return err
}

func (t *safeTree) remove(relative string) error {
	parent, name, err := t.openParent(relative, false)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return err
	}
	defer unix.Close(parent)
	var stat unix.Stat_t
	if err := unix.Fstatat(parent, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return err
	}
	flags := 0
	if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
		flags = unix.AT_REMOVEDIR
	}
	if err := unix.Unlinkat(parent, name, flags); err != nil {
		return err
	}
	return unix.Fsync(parent)
}

func (t *safeTree) ensureDir(relative string, mode fs.FileMode) error {
	fd, err := t.openDir(relative, true, mode)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Fsync(fd)
}

func (t *safeTree) writeFile(relative string, body []byte, mode fs.FileMode) error {
	parent, name, err := t.openParent(relative, true)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	temporary, err := safeTemporaryName()
	if err != nil {
		return err
	}
	fd, err := unix.Openat(parent, temporary,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(mode.Perm()))
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), temporary)
	cleanup := true
	defer func() {
		if cleanup {
			_ = unix.Unlinkat(parent, temporary, 0)
		}
	}()
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := installTemporary(parent, temporary, name); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func (t *safeTree) writeSymlink(relative, target string) error {
	parent, name, err := t.openParent(relative, true)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	temporary, err := safeTemporaryName()
	if err != nil {
		return err
	}
	if err := unix.Symlinkat(target, parent, temporary); err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = unix.Unlinkat(parent, temporary, 0)
		}
	}()
	if err := installTemporary(parent, temporary, name); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func installTemporary(parent int, temporary, name string) error {
	if err := renameatNoReplace(parent, temporary, parent, name); err != nil {
		return err
	}
	if afterNoReplaceRenameForTest != nil {
		if err := afterNoReplaceRenameForTest(); err != nil {
			return err
		}
	}
	if err := unix.Fsync(parent); err != nil {
		return err
	}
	return nil
}

var afterNoReplaceRenameForTest func() error

func cleanRelativePath(relative string) (string, error) {
	clean := filepath.ToSlash(filepath.Clean(relative))
	if relative == "" || filepath.IsAbs(relative) || clean != filepath.ToSlash(relative) || clean == "." ||
		strings.HasPrefix(clean, "../") || strings.ContainsRune(relative, 0) {
		return "", fmt.Errorf("restore: unsafe path %q", relative)
	}
	return clean, nil
}

func modeFromStat(mode uint32) fs.FileMode {
	result := fs.FileMode(mode & 0o777)
	switch mode & unix.S_IFMT {
	case unix.S_IFDIR:
		result |= fs.ModeDir
	case unix.S_IFLNK:
		result |= fs.ModeSymlink
	case unix.S_IFIFO:
		result |= fs.ModeNamedPipe
	case unix.S_IFSOCK:
		result |= fs.ModeSocket
	case unix.S_IFCHR:
		result |= fs.ModeDevice | fs.ModeCharDevice
	case unix.S_IFBLK:
		result |= fs.ModeDevice
	}
	return result
}

func readlinkAt(parent int, name string) ([]byte, error) {
	size := 256
	for size <= 1<<20 {
		buffer := make([]byte, size)
		n, err := unix.Readlinkat(parent, name, buffer)
		if err != nil {
			return nil, err
		}
		if n < len(buffer) {
			return buffer[:n], nil
		}
		size *= 2
	}
	return nil, errors.New("restore: symlink target exceeds 1 MiB")
}

func safeTemporaryName() (string, error) {
	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return ".acd-restore-" + hex.EncodeToString(random), nil
}

func isNonDirectoryPathError(err error) bool {
	return errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP)
}

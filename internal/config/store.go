package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"golang.org/x/sys/unix"
)

// Store safely reads and updates the XDG operator configuration.
type Store struct{ Roots paths.Roots }

// ErrStaleGeneration means a caller attempted to update a document that has
// changed since its snapshot was read.
var ErrStaleGeneration = errors.New("acd config: stale generation")

func NewStore(roots paths.Roots) *Store { return &Store{Roots: roots} }

// Load reads config.json without creating it.
func (s *Store) Load() (*Document, error) {
	path := s.Roots.ConfigPath()
	body, err := readRegularNoFollow(path)
	if errors.Is(err, os.ErrNotExist) {
		return NewDocument(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("acd config: read %s: %w", path, err)
	}
	doc, err := ParseDocument(body)
	if err != nil {
		return nil, fmt.Errorf("acd config: read %s: %w", path, err)
	}
	return doc, nil
}

// Update serializes the complete read-modify-write transaction with a
// dedicated advisory lock. Returning an error from fn leaves disk unchanged.
func (s *Store) Update(fn func(*Document) error) error {
	return s.update(nil, fn)
}

// UpdateExpected performs a compare-and-swap update against the saved
// generation. It lets interactive settings sessions fail visibly instead of
// overwriting a newer concurrent edit.
func (s *Store) UpdateExpected(expected uint64, fn func(*Document) error) error {
	return s.update(&expected, fn)
}

func (s *Store) update(expected *uint64, fn func(*Document) error) error {
	if fn == nil {
		return errors.New("acd config: nil update")
	}
	dir := filepath.Dir(s.Roots.ConfigPath())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("acd config: mkdir: %w", err)
	}
	if info, err := os.Lstat(dir); err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("acd config: config directory is not a real directory: %s", dir)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("acd config: chmod directory: %w", err)
	}
	lock, err := openLock(s.Roots.ConfigLockPath())
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("acd config: lock: %w", err)
	}
	defer flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck
	doc, err := s.Load()
	if err != nil {
		return err
	}
	if expected != nil && doc.Generation != *expected {
		return fmt.Errorf("%w: expected %d, found %d", ErrStaleGeneration, *expected, doc.Generation)
	}
	if err := fn(doc); err != nil {
		return err
	}
	doc.Generation++
	if err := ValidateDocument(doc); err != nil {
		return err
	}
	return writeDocument(s.Roots.ConfigPath(), doc)
}

func openLock(path string) (*os.File, error) {
	dir, err := openParentNoFollow(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	fd, err := unix.Openat(int(dir.Fd()), filepath.Base(path), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	// Darwin can transiently report ENOENT when concurrent creators race on the
	// same O_CREAT|O_NOFOLLOW directory entry. Retrying against the same anchored
	// directory descriptor preserves the no-follow guarantee.
	if errors.Is(err, unix.ENOENT) {
		fd, err = unix.Openat(int(dir.Fd()), filepath.Base(path), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	}
	if err != nil {
		return nil, fmt.Errorf("acd config: open lock: %w", err)
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("acd config: lock target is not a regular file: %s", path)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return nil, fmt.Errorf("acd config: chmod lock: %w", err)
	}
	return f, nil
}

func writeDocument(path string, doc *Document) error {
	body, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("acd config: marshal: %w", err)
	}
	body = append(body, '\n')
	dir, err := openParentNoFollow(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := validateRegularAt(dir, filepath.Base(path), path); err != nil {
		return err
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("acd config: temp name: %w", err)
	}
	tmpName := ".config-" + hex.EncodeToString(nonce[:]) + ".tmp"
	fd, err := unix.Openat(int(dir.Fd()), tmpName, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("acd config: create temp: %w", err)
	}
	tmp := os.NewFile(uintptr(fd), tmpName)
	defer unix.Unlinkat(int(dir.Fd()), tmpName, 0) //nolint:errcheck
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("acd config: chmod temp: %w", err)
	}
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return fmt.Errorf("acd config: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("acd config: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("acd config: close temp: %w", err)
	}
	if err := unix.Renameat(int(dir.Fd()), tmpName, int(dir.Fd()), filepath.Base(path)); err != nil {
		return fmt.Errorf("acd config: rename: %w", err)
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("acd config: fsync parent: %w", err)
	}
	return nil
}

func openParentNoFollow(path string) (*os.File, error) {
	dirPath := filepath.Dir(path)
	fd, err := unix.Open(dirPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("acd config: open parent: %w", err)
	}
	return os.NewFile(uintptr(fd), dirPath), nil
}

func validateRegularAt(dir *os.File, name, display string) error {
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acd config: target is not a regular file: %s", display)
	}
	f := os.NewFile(uintptr(fd), display)
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("acd config: target is not a regular file: %s", display)
	}
	return nil
}

func readRegularNoFollow(path string) ([]byte, error) {
	dir, err := openParentNoFollow(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	defer dir.Close()
	fd, err := unix.Openat(int(dir.Fd()), filepath.Base(path), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("acd config: target is not a regular file: %s", path)
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("acd config: target is not a regular file: %s", path)
	}
	body, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("acd config: read %s: %w", path, err)
	}
	return body, nil
}

func flock(fd int, how int) error {
	for {
		err := syscall.Flock(fd, how)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

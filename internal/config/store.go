package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
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
	if err := validateRegularTarget(path); err != nil {
		return nil, err
	}
	body, err := os.ReadFile(path)
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
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("acd config: lock target is not a regular file: %s", path)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("acd config: inspect lock: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("acd config: open lock: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return nil, fmt.Errorf("acd config: chmod lock: %w", err)
	}
	return f, nil
}

func validateRegularTarget(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acd config: inspect %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("acd config: target is not a regular file: %s", path)
	}
	return nil
}

func writeDocument(path string, doc *Document) error {
	if err := validateRegularTarget(path); err != nil {
		return err
	}
	body, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("acd config: marshal: %w", err)
	}
	body = append(body, '\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".config-*.tmp")
	if err != nil {
		return fmt.Errorf("acd config: create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
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
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("acd config: rename: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("acd config: open parent: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("acd config: fsync parent: %w", err)
	}
	return nil
}

func flock(fd int, how int) error {
	for {
		err := syscall.Flock(fd, how)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

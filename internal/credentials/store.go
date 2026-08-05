// Package credentials manages the optional protected OpenAI-compatible API
// key file. It deliberately exposes no JSON-marshallable type containing the
// secret.
package credentials

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

const (
	SchemaVersion = 1
	fileName      = "credentials.json"
	maxFileBytes  = 64 * 1024
)

var ErrNotFound = errors.New("acd credentials: not configured")

type Source string

const (
	SourceNone        Source = "none"
	SourceEnvironment Source = "environment"
	SourceFile        Source = "protected_file"
)

type Status struct {
	Path             string
	ProtectedFileSet bool
}

type fileDocument struct {
	Version            int    `json:"version"`
	OpenAICompatAPIKey string `json:"openai_compat_api_key"`
}

type Store struct {
	dir  string
	path string
}

func NewStore(roots paths.Roots) Store {
	return Store{dir: roots.Config, path: filepath.Join(roots.Config, fileName)}
}

func (s Store) Path() string { return s.path }

func (s Store) Status() (Status, error) {
	_, err := s.read()
	if errors.Is(err, ErrNotFound) {
		return Status{Path: s.path}, nil
	}
	if err != nil {
		return Status{}, err
	}
	return Status{Path: s.path, ProtectedFileSet: true}, nil
}

func (s Store) Read() (string, error) {
	return s.read()
}

func (s Store) read() (string, error) {
	if err := validateDirectory(s.dir, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrNotFound
		}
		return "", err
	}
	info, err := os.Lstat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("acd credentials: inspect protected file: %w", err)
	}
	if err := validateFileInfo(info); err != nil {
		return "", err
	}
	file, err := os.Open(s.path)
	if err != nil {
		return "", fmt.Errorf("acd credentials: open protected file: %w", err)
	}
	defer file.Close()
	document, err := decode(io.LimitReader(file, maxFileBytes+1))
	if err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("acd credentials: close protected file: %w", err)
	}
	return document.OpenAICompatAPIKey, nil
}

func (s Store) Set(secret string) error {
	secret = strings.TrimSpace(secret)
	if !validSecret(secret) {
		return errors.New("acd credentials: API key is empty or contains unsupported control characters")
	}
	if err := ensureDirectory(s.dir); err != nil {
		return err
	}
	if _, err := s.read(); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	file, err := os.CreateTemp(s.dir, ".credentials-*.tmp")
	if err != nil {
		return fmt.Errorf("acd credentials: create temporary file: %w", err)
	}
	tempPath := file.Name()
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(tempPath)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("acd credentials: protect temporary file: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(fileDocument{
		Version: SchemaVersion, OpenAICompatAPIKey: secret,
	}); err != nil {
		return fmt.Errorf("acd credentials: encode protected file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("acd credentials: sync temporary file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("acd credentials: close temporary file: %w", err)
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("acd credentials: install protected file: %w", err)
	}
	keep = true
	if err := syncDirectory(s.dir); err != nil {
		return err
	}
	return nil
}

func (s Store) Remove() (bool, error) {
	if _, err := s.read(); errors.Is(err, ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if err := os.Remove(s.path); err != nil {
		return false, fmt.Errorf("acd credentials: remove protected file: %w", err)
	}
	if err := syncDirectory(s.dir); err != nil {
		return false, err
	}
	return true, nil
}

// Resolve gives ACD_AI_API_KEY priority without reading the protected file.
// The returned key is for provider construction only and must never be logged
// or serialized.
func Resolve(store Store, lookupEnv func(string) (string, bool)) (string, Source, error) {
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}
	if value, ok := lookupEnv("ACD_AI_API_KEY"); ok {
		value = strings.TrimSpace(value)
		if validSecret(value) {
			return value, SourceEnvironment, nil
		}
	}
	value, err := store.Read()
	if errors.Is(err, ErrNotFound) {
		return "", SourceNone, nil
	}
	if err != nil {
		return "", SourceNone, err
	}
	return value, SourceFile, nil
}

func decode(reader io.Reader) (fileDocument, error) {
	buffered := bufio.NewReader(reader)
	decoder := json.NewDecoder(buffered)
	decoder.DisallowUnknownFields()
	var document fileDocument
	if err := decoder.Decode(&document); err != nil {
		return fileDocument{}, errors.New("acd credentials: protected file is malformed")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fileDocument{}, errors.New("acd credentials: protected file must contain exactly one JSON value")
	}
	if document.Version != SchemaVersion {
		return fileDocument{}, errors.New("acd credentials: protected file uses an unsupported version")
	}
	document.OpenAICompatAPIKey = strings.TrimSpace(document.OpenAICompatAPIKey)
	if !validSecret(document.OpenAICompatAPIKey) {
		return fileDocument{}, errors.New("acd credentials: protected file does not contain a valid API key")
	}
	return document, nil
}

func ensureDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("acd credentials: create protected directory: %w", err)
	}
	return validateDirectory(path, true)
}

func validateDirectory(path string, required bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		if !required && errors.Is(err, os.ErrNotExist) {
			return os.ErrNotExist
		}
		return fmt.Errorf("acd credentials: inspect protected directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("acd credentials: protected directory must be a real directory")
	}
	if info.Mode().Perm() != 0o700 {
		return errors.New("acd credentials: protected directory permissions must be 0700")
	}
	if !ownedByCurrentUser(info) {
		return errors.New("acd credentials: protected directory has the wrong owner")
	}
	return nil
}

func validateFileInfo(info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("acd credentials: protected file must be a regular file")
	}
	if info.Mode().Perm() != 0o600 {
		return errors.New("acd credentials: protected file permissions must be 0600")
	}
	if !ownedByCurrentUser(info) {
		return errors.New("acd credentials: protected file has the wrong owner")
	}
	if info.Size() > maxFileBytes {
		return errors.New("acd credentials: protected file is too large")
	}
	return nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Getuid())
}

func validSecret(value string) bool {
	if value == "" || len(value) > maxFileBytes/2 {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("acd credentials: open protected directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("acd credentials: sync protected directory: %w", err)
	}
	return nil
}

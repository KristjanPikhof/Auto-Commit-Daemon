package pause

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrMalformed reports a pause marker that exists but is not valid JSON.
var ErrMalformed = errors.New("pause: malformed marker")

// Marker is the durable gitDir/acd/paused file format.
type Marker struct {
	Reason    string  `json:"reason"`
	SetAt     string  `json:"set_at"`
	SetBy     string  `json:"set_by"`
	ExpiresAt *string `json:"expires_at"`
}

func Path(gitDir string) string {
	return filepath.Join(gitDir, "acd", "paused")
}

func Read(gitDir string) (Marker, bool, error) {
	path := Path(gitDir)
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Marker{}, false, nil
	}
	if err != nil {
		return Marker{}, false, err
	}
	var marker Marker
	if err := json.Unmarshal(body, &marker); err != nil {
		return Marker{}, false, fmt.Errorf("%w: %s: %v", ErrMalformed, path, err)
	}
	return marker, true, nil
}

func Write(path string, marker Marker, overwrite bool) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	body, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')

	flags := os.O_WRONLY | os.O_CREATE
	if overwrite {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := f.Chmod(0o600); err != nil {
		return err
	}
	_, err = f.Write(body)
	return err
}

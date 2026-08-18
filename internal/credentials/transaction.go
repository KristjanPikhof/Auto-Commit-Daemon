package credentials

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	replacementMarker   = ".credentials-setup-transaction.json"
	replacementRollback = ".credentials-setup-rollback.json"
)

type replacementRecord struct {
	Version     int  `json:"version"`
	HadPrevious bool `json:"had_previous"`
}

// Replacement keeps the prior credential inside the protected credential
// directory until setup has completed. It contains no secret value in memory
// or in the setup backup tree.
type Replacement struct {
	store       Store
	hadPrevious bool
	finished    bool
}

// BeginReplacement installs a new credential and leaves enough protected,
// durable state to restore the prior credential after a crash.
func (s Store) BeginReplacement(secret string) (*Replacement, error) {
	if err := ensureDirectory(s.dir); err != nil {
		return nil, err
	}
	if err := s.RecoverPendingReplacement(); err != nil {
		return nil, err
	}
	status, err := s.Status()
	if err != nil {
		return nil, err
	}
	record := replacementRecord{Version: 1, HadPrevious: status.ProtectedFileSet}
	body, _ := json.Marshal(record)
	if err := writeProtected(filepath.Join(s.dir, replacementMarker), append(body, '\n')); err != nil {
		return nil, err
	}
	rollbackPath := filepath.Join(s.dir, replacementRollback)
	if status.ProtectedFileSet {
		if err := os.Rename(s.path, rollbackPath); err != nil {
			_ = os.Remove(filepath.Join(s.dir, replacementMarker))
			return nil, fmt.Errorf("acd credentials: preserve prior credential: %w", err)
		}
		if err := syncDirectory(s.dir); err != nil {
			return nil, err
		}
	}
	if err := s.Set(secret); err != nil {
		tx := &Replacement{store: s, hadPrevious: status.ProtectedFileSet}
		return nil, errors.Join(err, tx.Rollback())
	}
	return &Replacement{store: s, hadPrevious: status.ProtectedFileSet}, nil
}

// RecoverPendingReplacement rolls back a credential replacement that did not
// reach Commit. It is safe to call before every setup attempt.
func (s Store) RecoverPendingReplacement() error {
	markerPath := filepath.Join(s.dir, replacementMarker)
	body, err := os.ReadFile(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acd credentials: read setup transaction: %w", err)
	}
	var record replacementRecord
	if err := json.Unmarshal(body, &record); err != nil || record.Version != 1 {
		return errors.New("acd credentials: setup transaction is malformed")
	}
	rollbackPath := filepath.Join(s.dir, replacementRollback)
	if record.HadPrevious {
		if _, err := os.Lstat(rollbackPath); err == nil {
			if removeErr := os.Remove(s.path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return fmt.Errorf("acd credentials: remove interrupted replacement: %w", removeErr)
			}
			if err := os.Rename(rollbackPath, s.path); err != nil {
				return fmt.Errorf("acd credentials: restore prior credential: %w", err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("acd credentials: inspect setup rollback: %w", err)
		}
	} else if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("acd credentials: remove interrupted credential: %w", err)
	}
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("acd credentials: finish setup recovery: %w", err)
	}
	return syncDirectory(s.dir)
}

func (r *Replacement) Commit() error {
	if r == nil || r.finished {
		return nil
	}
	if err := os.Remove(filepath.Join(r.store.dir, replacementRollback)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("acd credentials: remove setup rollback: %w", err)
	}
	if err := os.Remove(filepath.Join(r.store.dir, replacementMarker)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("acd credentials: finish setup transaction: %w", err)
	}
	if err := syncDirectory(r.store.dir); err != nil {
		return err
	}
	r.finished = true
	return nil
}

func (r *Replacement) Rollback() error {
	if r == nil || r.finished {
		return nil
	}
	r.finished = true
	return r.store.RecoverPendingReplacement()
}

func writeProtected(path string, body []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".credentials-transaction-*.tmp")
	if err != nil {
		return fmt.Errorf("acd credentials: create setup transaction: %w", err)
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(body); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

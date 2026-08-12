package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

// QuickCheck validates an existing database without creating or migrating it.
func QuickCheck(ctx context.Context, dbPath string) error {
	q := url.Values{}
	q.Set("mode", "ro")
	q.Add("_pragma", "query_only(ON)")
	conn, err := sql.Open(driverName, "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return err
	}
	defer conn.Close()
	var result string
	if err := conn.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("state: quick_check=%s", result)
	}
	return nil
}

// BackupDatabase makes a WAL-consistent verified backup with VACUUM INTO.
// The destination must not exist, preventing accidental overwrite.
func BackupDatabase(ctx context.Context, dbPath, destination string) error {
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("state: backup target already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	conn, err := sql.Open(driverName, "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `VACUUM INTO ?`, destination); err != nil {
		conn.Close()
		return fmt.Errorf("state: VACUUM INTO: %w", err)
	}
	if err := conn.Close(); err != nil {
		return err
	}
	if err := os.Chmod(destination, 0o600); err != nil {
		return err
	}
	return QuickCheck(ctx, destination)
}

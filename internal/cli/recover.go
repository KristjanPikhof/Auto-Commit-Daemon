package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func recoverRepoRecord(repo string) (central.RepoRecord, error) {
	rec, _, abs, err := lookupRegisteredRepo("recover", repo)
	if err != nil {
		return central.RepoRecord{}, err
	}
	if !fileExists(rec.StateDB) {
		return central.RepoRecord{}, fmt.Errorf("acd recover: state.db missing for repo %s", abs)
	}
	return rec, nil
}

func refuseRecoverWhenDaemonAliveSQL(ctx context.Context, conn *sql.DB) error {
	var pid int
	var mode string
	err := conn.QueryRowContext(ctx, `SELECT pid, mode FROM daemon_state WHERE id = 1`).Scan(&pid, &mode)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acd recover: load daemon state\nDetails: %w", err)
	}
	return refuseRecoverWhenDaemonPIDAlive(ctx, pid, mode)
}

func refuseRecoverWhenDaemonAlive(ctx context.Context, db *state.DB) error {
	st, ok, err := state.LoadDaemonState(ctx, db)
	if err != nil {
		return fmt.Errorf("acd recover: load daemon state\nDetails: %w", err)
	}
	if !ok || st.PID <= 0 {
		return nil
	}
	return refuseRecoverWhenDaemonPIDAlive(ctx, st.PID, st.Mode)
}

func refuseRecoverWhenDaemonPIDAlive(ctx context.Context, pid int, mode string) error {
	if pid <= 0 {
		return nil
	}
	switch mode {
	case "running", "starting", "draining":
		if identity.AliveContext(ctx, pid) {
			return fmt.Errorf("acd recover: refusing while daemon pid %d is alive in mode %s", pid, mode)
		}
	}
	return nil
}

func checkParentDirWritable(path string) error {
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	tmp, err := os.CreateTemp(dir, ".acd-write-check-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Remove(name)
}

func backupStateDB(ctx context.Context, conn *sql.DB, stateDB string) (string, error) {
	if conn == nil {
		return "", errors.New("nil SQLite connection")
	}
	stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	backup := stateDB + ".bak-" + stamp
	if _, err := conn.ExecContext(ctx, `VACUUM INTO ?`, backup); err != nil {
		return "", fmt.Errorf("VACUUM INTO: %w", err)
	}
	removeOnError := func(err error) (string, error) {
		_ = os.Remove(backup)
		return "", err
	}
	if err := os.Chmod(backup, 0o600); err != nil {
		return removeOnError(fmt.Errorf("chmod backup\nDetails: %w", err))
	}
	verify, err := openStateDBReadOnly(ctx, backup)
	if err != nil {
		return removeOnError(fmt.Errorf("open backup for verification\nDetails: %w", err))
	}
	var integrity string
	checkErr := verify.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&integrity)
	closeErr := verify.Close()
	if checkErr != nil {
		return removeOnError(fmt.Errorf("verify backup integrity\nDetails: %w", checkErr))
	}
	if closeErr != nil {
		return removeOnError(fmt.Errorf("close verified backup\nDetails: %w", closeErr))
	}
	if !strings.EqualFold(strings.TrimSpace(integrity), "ok") {
		return removeOnError(fmt.Errorf("verify backup integrity: %s", integrity))
	}
	if _, err := os.Stat(backup); err != nil {
		return removeOnError(fmt.Errorf("stat verified backup\nDetails: %w", err))
	}
	return backup, nil
}

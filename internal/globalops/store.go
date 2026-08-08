// Package globalops owns the user-level immutable operation journal used by
// setup, upgrade, uninstall, configuration, registry, and service mutations.
package globalops

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
}

type Operation struct {
	ID         string
	Kind       string
	Phase      string
	PlanDigest string
	Error      string
	CreatedTS  float64
	UpdatedTS  float64
}

type Step struct {
	OperationID string
	Sequence    int
	Kind        string
	Target      string
	BeforeHash  string
	AfterHash   string
	BackupPath  string
	Phase       string
}

const ddl = `
CREATE TABLE IF NOT EXISTS operations(
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, phase TEXT NOT NULL,
 plan_digest TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
 created_ts REAL NOT NULL, updated_ts REAL NOT NULL, completed_ts REAL
);
CREATE TABLE IF NOT EXISTS operation_steps(
 operation_id TEXT NOT NULL, ord INTEGER NOT NULL, kind TEXT NOT NULL,
 target TEXT NOT NULL, before_hash TEXT NOT NULL DEFAULT '',
 after_hash TEXT NOT NULL DEFAULT '', backup_path TEXT NOT NULL DEFAULT '',
 phase TEXT NOT NULL, completed_ts REAL,
 PRIMARY KEY(operation_id, ord),
 FOREIGN KEY(operation_id) REFERENCES operations(id)
);
PRAGMA user_version=1;`

func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("globalops: empty path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "busy_timeout(5000)")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		db.Close()
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Prepare(ctx context.Context, operation Operation, steps []Step) error {
	if s == nil || operation.ID == "" || operation.Kind == "" || operation.PlanDigest == "" {
		return errors.New("globalops: incomplete operation")
	}
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations(id,kind,phase,plan_digest,error,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?)`,
		operation.ID, operation.Kind, operation.Phase, operation.PlanDigest, "", now, now); err != nil {
		return err
	}
	for i, step := range steps {
		ord := step.Sequence
		if ord == 0 {
			ord = i + 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO operation_steps(operation_id,ord,kind,target,before_hash,after_hash,backup_path,phase) VALUES(?,?,?,?,?,?,?,?)`,
			operation.ID, ord, step.Kind, step.Target, step.BeforeHash, step.AfterHash, step.BackupPath, step.Phase); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Advance(ctx context.Context, id, phase, sanitizedError string, complete bool) error {
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	var result sql.Result
	var err error
	if complete {
		result, err = s.db.ExecContext(ctx, `UPDATE operations SET phase=?,error=?,updated_ts=?,completed_ts=? WHERE id=?`, phase, sanitizedError, now, now, id)
	} else {
		result, err = s.db.ExecContext(ctx, `UPDATE operations SET phase=?,error=?,updated_ts=? WHERE id=?`, phase, sanitizedError, now, id)
	}
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) CompleteStep(ctx context.Context, operationID string, sequence int, phase string) error {
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	result, err := s.db.ExecContext(ctx, `UPDATE operation_steps SET phase=?,completed_ts=? WHERE operation_id=? AND ord=?`, phase, now, operationID, sequence)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("globalops: step not found")
	}
	return nil
}

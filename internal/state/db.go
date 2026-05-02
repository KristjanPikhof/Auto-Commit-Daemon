package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	// modernc.org/sqlite is the pure-Go driver mandated by D16 (zero cgo).
	// Do not switch to mattn/go-sqlite3 — that pulls cgo and breaks the
	// CGO_ENABLED=0 cross-compile target.
	_ "modernc.org/sqlite"
)

// driverName is registered by the modernc.org/sqlite blank import above.
const driverName = "sqlite"

// DBFileName is the per-repo SQLite filename inside .git/acd/.
const DBFileName = "state.db"

// DB wraps the per-repo SQLite handles plus a small amount of derived metadata.
//
// All exported helpers in this package take *DB and a context.Context, so the
// daemon can cancel long writes when the worktree is shutting down.
//
// SQLite WAL permits many readers but still serializes writes. Keeping writes
// on a single-connection handle avoids a local pool of writer-capable
// connections queueing behind busy_timeout under multi-client load, while a
// separate small read pool lets status/list-style queries proceed alongside
// the daemon's hot write paths.
type DB struct {
	conn     *sql.DB // single-connection write handle; kept for package compatibility.
	readConn *sql.DB
	path     string

	// initOnce guards the schema-bootstrap path so callers can safely call
	// Open repeatedly with the same file (e.g. tests reopening to verify
	// idempotence) without re-running migrate logic in parallel.
	initOnce sync.Once
	initErr  error
}

// Path returns the absolute path to the underlying state.db file.
func (d *DB) Path() string { return d.path }

// SQL returns the write handle. Exposed so state-adjacent packages can compose
// fixture and daemon queries without re-piping every helper through DB.
// External callers should prefer package helpers for normal reads/writes.
func (d *DB) SQL() *sql.DB { return d.conn }

func (d *DB) readSQL() *sql.DB { return d.readConn }

// Close releases the underlying database handle. Safe to call multiple times;
// the second call returns the original close error.
func (d *DB) Close() error {
	if d == nil {
		return nil
	}
	var err error
	if d.conn != nil {
		err = errors.Join(err, d.conn.Close())
	}
	if d.readConn != nil {
		err = errors.Join(err, d.readConn.Close())
	}
	return err
}

// AcdDirFromGitDir returns the canonical ACD state directory for a given .git
// directory. Path layout reference: CLAUDE.md ("State lives inside .git/").
func AcdDirFromGitDir(gitDir string) string {
	return filepath.Join(gitDir, "acd")
}

// DBPathFromGitDir returns the canonical state.db path for a given .git dir.
func DBPathFromGitDir(gitDir string) string {
	return filepath.Join(AcdDirFromGitDir(gitDir), DBFileName)
}

// Open opens (or creates) the per-repo state.db at dbPath.
//
// dbPath should usually come from DBPathFromGitDir. Open will:
//
//  1. mkdir -p the parent directory (typically <repo>/.git/acd/) with 0o700.
//  2. Open the SQLite database with WAL, NORMAL sync, foreign keys, and a
//     5-second busy timeout (§6.1 + §8.1 concurrency expectations).
//  3. Apply DDL from schema.go inside a transaction, idempotent.
//  4. Stamp PRAGMA user_version = SchemaVersion on first initialization.
//
// Re-opening an existing current-version database is read-only for schema
// purposes so status/daemon contenders do not take an avoidable SQLite write
// lock.
func Open(ctx context.Context, dbPath string) (*DB, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("state: empty dbPath")
	}

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, fmt.Errorf("state: mkdir parent: %w", err)
	}

	// Build the DSN with the PRAGMAs that must be applied on every connection
	// the driver opens. modernc.org/sqlite supports the _pragma= URL option
	// (repeatable) which it issues immediately after opening each underlying
	// connection — important because *sql.DB pools connections, and PRAGMA
	// state is per-connection (not per-database) for journal_mode/sync.
	dsn := buildDSN(dbPath)

	writeConn, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("state: sql.Open write: %w", err)
	}

	writeConn.SetMaxOpenConns(1)
	writeConn.SetMaxIdleConns(1)

	if err := writeConn.PingContext(ctx); err != nil {
		_ = writeConn.Close()
		return nil, fmt.Errorf("state: ping write: %w", err)
	}

	readConn, err := sql.Open(driverName, dsn)
	if err != nil {
		_ = writeConn.Close()
		return nil, fmt.Errorf("state: sql.Open read: %w", err)
	}
	readConn.SetMaxOpenConns(4)
	readConn.SetMaxIdleConns(4)

	if err := readConn.PingContext(ctx); err != nil {
		_ = readConn.Close()
		_ = writeConn.Close()
		return nil, fmt.Errorf("state: ping read: %w", err)
	}

	d := &DB{conn: writeConn, readConn: readConn, path: dbPath}

	if err := d.bootstrapWithRetry(ctx); err != nil {
		_ = d.Close()
		return nil, err
	}

	return d, nil
}

// buildDSN composes the modernc.org/sqlite DSN with the PRAGMAs required by
// §6.1. Each PRAGMA is URL-encoded as a separate _pragma= query parameter so
// the driver issues them on every fresh connection in the pool.
func buildDSN(dbPath string) string {
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "busy_timeout(5000)")
	return "file:" + dbPath + "?" + q.Encode()
}

// bootstrap applies the DDL and stamps user_version on first open. Subsequent
// calls first check user_version and return without taking a write lock when
// the database is already current.
func (d *DB) bootstrap(ctx context.Context) error {
	d.initOnce.Do(func() {
		d.initErr = d.runBootstrap(ctx)
	})
	return d.initErr
}

func (d *DB) bootstrapWithRetry(ctx context.Context) error {
	// runBootstrap is idempotent: it early-returns when user_version already
	// equals SchemaVersion, and applies DDL inside a single transaction
	// otherwise. Call it directly across attempts rather than resetting the
	// initOnce sync.Once value — mutating a sync.Once after use is undefined
	// behavior under the race detector. initOnce is reserved for the
	// no-retry bootstrap entrypoint.
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		err := d.runBootstrap(ctx)
		if err == nil || !isSQLiteLocked(err) {
			return err
		}
		lastErr = err
		timer := time.NewTimer(time.Duration(attempt+1) * 25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (d *DB) runBootstrap(ctx context.Context) error {
	cur, err := d.UserVersion(ctx)
	if err != nil {
		return err
	}
	if cur > SchemaVersion {
		return fmt.Errorf("state: db user_version=%d is newer than this binary's SchemaVersion=%d", cur, SchemaVersion)
	}
	if cur == SchemaVersion {
		return nil
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op if Commit succeeded

	if _, err := tx.ExecContext(ctx, schemaDDL); err != nil {
		return fmt.Errorf("state: apply schema: %w", err)
	}

	// Set the schema version. PRAGMA user_version is per-database (stored in
	// the file header), not per-connection — safe to set once.
	pragma := fmt.Sprintf("PRAGMA user_version = %d", SchemaVersion)
	if _, err := tx.ExecContext(ctx, pragma); err != nil {
		return fmt.Errorf("state: stamp user_version: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit bootstrap: %w", err)
	}
	return nil
}

func isSQLiteLocked(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "SQLITE_BUSY")
}

// UserVersion reads the SQLite PRAGMA user_version from the open database.
// Useful for tests + migration logic in migrate.go.
func (d *DB) UserVersion(ctx context.Context) (int, error) {
	var v int
	if err := d.readSQL().QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return 0, fmt.Errorf("state: read user_version: %w", err)
	}
	return v, nil
}

// PragmaString reads a string-valued PRAGMA. Used by tests to confirm that
// WAL + busy_timeout are actually live on the connection pool.
func (d *DB) PragmaString(ctx context.Context, name string) (string, error) {
	var v string
	q := "PRAGMA " + name
	if err := d.readSQL().QueryRowContext(ctx, q).Scan(&v); err != nil {
		return "", fmt.Errorf("state: read pragma %s: %w", name, err)
	}
	return v, nil
}

// PragmaInt reads an int-valued PRAGMA.
func (d *DB) PragmaInt(ctx context.Context, name string) (int64, error) {
	var v int64
	q := "PRAGMA " + name
	if err := d.readSQL().QueryRowContext(ctx, q).Scan(&v); err != nil {
		return 0, fmt.Errorf("state: read pragma %s: %w", name, err)
	}
	return v, nil
}

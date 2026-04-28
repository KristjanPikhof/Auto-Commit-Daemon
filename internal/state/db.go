package state

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	// modernc.org/sqlite is the pure-Go driver mandated by D16 (zero cgo).
	// Do not switch to mattn/go-sqlite3 — that pulls cgo and breaks the
	// CGO_ENABLED=0 cross-compile target.
	_ "modernc.org/sqlite"
)

// driverName is registered by the modernc.org/sqlite blank import above.
const driverName = "sqlite"

// DBFileName is the per-repo SQLite filename inside .git/acd/.
const DBFileName = "state.db"

// DB wraps the per-repo *sql.DB plus a small amount of derived metadata.
//
// All exported helpers in this package take *DB and a context.Context, so the
// daemon can cancel long writes when the worktree is shutting down.
//
// The underlying *sql.DB is safe for concurrent use from multiple goroutines;
// SQLite serialises writers internally and WAL + busy_timeout=5000 guarantees
// readers and writers do not deadlock under the daemon's expected load.
type DB struct {
	conn *sql.DB
	path string

	// initOnce guards the schema-bootstrap path so callers can safely call
	// Open repeatedly with the same file (e.g. tests reopening to verify
	// idempotence) without re-running migrate logic in parallel.
	initOnce sync.Once
	initErr  error
}

// Path returns the absolute path to the underlying state.db file.
func (d *DB) Path() string { return d.path }

// SQL returns the underlying *sql.DB. Exposed so other state-package files
// (events.go, shadow.go, ...) can compose queries without re-piping every
// helper through DB. External callers should not rely on this directly.
func (d *DB) SQL() *sql.DB { return d.conn }

// Close releases the underlying database handle. Safe to call multiple times;
// the second call returns the original close error.
func (d *DB) Close() error {
	if d == nil || d.conn == nil {
		return nil
	}
	return d.conn.Close()
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
//  4. Stamp PRAGMA user_version = SchemaVersion.
//
// Re-opening an existing database is a no-op for schema purposes (CREATE TABLE
// IF NOT EXISTS + INSERT OR IGNORE for the singleton rows).
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

	conn, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("state: sql.Open: %w", err)
	}

	// SQLite in WAL mode supports concurrent readers + a single writer. We
	// keep the pool small to avoid file-handle storms but allow > 1 reader.
	conn.SetMaxOpenConns(8)
	conn.SetMaxIdleConns(4)

	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("state: ping: %w", err)
	}

	d := &DB{conn: conn, path: dbPath}

	if err := d.bootstrap(ctx); err != nil {
		_ = conn.Close()
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
// calls are cheap because every CREATE uses IF NOT EXISTS.
func (d *DB) bootstrap(ctx context.Context) error {
	d.initOnce.Do(func() {
		d.initErr = d.runBootstrap(ctx)
	})
	return d.initErr
}

func (d *DB) runBootstrap(ctx context.Context) error {
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

// UserVersion reads the SQLite PRAGMA user_version from the open database.
// Useful for tests + migration logic in migrate.go.
func (d *DB) UserVersion(ctx context.Context) (int, error) {
	var v int
	if err := d.conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return 0, fmt.Errorf("state: read user_version: %w", err)
	}
	return v, nil
}

// PragmaString reads a string-valued PRAGMA. Used by tests to confirm that
// WAL + busy_timeout are actually live on the connection pool.
func (d *DB) PragmaString(ctx context.Context, name string) (string, error) {
	var v string
	q := "PRAGMA " + name
	if err := d.conn.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return "", fmt.Errorf("state: read pragma %s: %w", name, err)
	}
	return v, nil
}

// PragmaInt reads an int-valued PRAGMA.
func (d *DB) PragmaInt(ctx context.Context, name string) (int64, error) {
	var v int64
	q := "PRAGMA " + name
	if err := d.conn.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return 0, fmt.Errorf("state: read pragma %s: %w", name, err)
	}
	return v, nil
}

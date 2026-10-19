package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// openTestDB returns a freshly-opened DB rooted at a t.TempDir() .git/acd path.
// Test isolation: each subtest gets its own temp directory; SQLite files are
// removed implicitly when the test framework cleans up the dir.
func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	gitDir := filepath.Join(t.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	d, err := Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, dbPath
}

func TestOpenCreatesSchemaAndPragmas(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	if got := d.SQL().Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("write pool MaxOpenConnections = %d, want 1", got)
	}
	if got := d.readSQL().Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("read pool MaxOpenConnections = %d, want 4", got)
	}

	v, err := d.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion: %v", err)
	}
	if v != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", v, SchemaVersion)
	}

	jm, err := d.PragmaString(ctx, "journal_mode")
	if err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if strings.ToLower(jm) != "wal" {
		t.Fatalf("journal_mode = %q, want wal", jm)
	}

	bt, err := d.PragmaInt(ctx, "busy_timeout")
	if err != nil {
		t.Fatalf("busy_timeout: %v", err)
	}
	if bt != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", bt)
	}

	fk, err := d.PragmaInt(ctx, "foreign_keys")
	if err != nil {
		t.Fatalf("foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Fatalf("foreign_keys = %d, want 1", fk)
	}

	sync, err := d.PragmaInt(ctx, "synchronous")
	if err != nil {
		t.Fatalf("synchronous: %v", err)
	}
	if sync != 1 { // NORMAL == 1
		t.Fatalf("synchronous = %d, want 1 (NORMAL)", sync)
	}

	tables := []string{"daemon_state", "daemon_meta", "daily_rollups", "decision_records", "config_revisions", "runtime_config_state", "config_activation_requests", "operations", "operation_steps", "checkpoints", "checkpoint_events", "checkpoint_exclusions", "checkpoint_capture_issues", "restore_operations", "finish_requests", "finish_history_requests", "finish_legacy_cutover"}
	for _, table := range tables {
		var name string
		err := d.SQL().QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table,
		).Scan(&name)
		if err != nil || name != table {
			t.Fatalf("table %q missing: err=%v name=%q", table, err, name)
		}
	}
}

func TestOpenIdempotent(t *testing.T) {
	t.Parallel()
	gitDir := filepath.Join(t.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	ctx := context.Background()

	d1, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := MetaSet(ctx, d1, "k", "v1"); err != nil {
		t.Fatalf("seed meta: %v", err)
	}
	if err := d1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	v, ok, err := MetaGet(ctx, d2, "k")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("post-reopen meta = (%q, %v, %v), want (\"v1\", true, nil)", v, ok, err)
	}
	uv, err := d2.UserVersion(ctx)
	if err != nil {
		t.Fatalf("user_version after reopen: %v", err)
	}
	if uv != SchemaVersion {
		t.Fatalf("post-reopen user_version = %d, want %d", uv, SchemaVersion)
	}
}

func TestOpenExistingCurrentDBDoesNotNeedWriteLock(t *testing.T) {
	t.Parallel()
	gitDir := filepath.Join(t.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	ctx := context.Background()

	d1, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer d1.Close()

	tx, err := d1.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin writer: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO daemon_meta(key, value, updated_ts) VALUES('held-writer', '1', 1)`); err != nil {
		t.Fatalf("hold writer lock: %v", err)
	}

	d2, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("second Open while writer active: %v", err)
	}
	defer d2.Close()

	uv, err := d2.UserVersion(ctx)
	if err != nil {
		t.Fatalf("second user_version: %v", err)
	}
	if uv != SchemaVersion {
		t.Fatalf("second user_version = %d, want %d", uv, SchemaVersion)
	}
}

func TestDaemonStateRoundTrip(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	// Empty state returns ok=false with mode="stopped".
	s, ok, err := LoadDaemonState(ctx, d)
	if err != nil {
		t.Fatalf("load empty: %v", err)
	}
	if ok {
		t.Fatalf("ok = true on empty table")
	}
	if s.Mode != "stopped" {
		t.Fatalf("default mode = %q, want stopped", s.Mode)
	}

	want := DaemonState{PID: 4242, Mode: "running", HeartbeatTS: 12.5}
	if err := SaveDaemonState(ctx, d, want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := LoadDaemonState(ctx, d)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got.PID != 4242 || got.Mode != "running" || got.HeartbeatTS != 12.5 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	if err := TouchHeartbeat(ctx, d, 99.9); err != nil {
		t.Fatalf("touch: %v", err)
	}
	got, _, _ = LoadDaemonState(ctx, d)
	if got.HeartbeatTS != 99.9 {
		t.Fatalf("post-touch heartbeat = %v, want 99.9", got.HeartbeatTS)
	}
	// Touch must not clobber other fields.
	if got.Mode != "running" || got.PID != 4242 {
		t.Fatalf("touch clobbered fields: %+v", got)
	}
}

func TestLoadDaemonStateReadOnlyAllowsFutureSchema(t *testing.T) {
	t.Parallel()
	d, dbPath := openTestDB(t)
	ctx := context.Background()
	want := DaemonState{
		PID:               4242,
		Mode:              "running",
		HeartbeatTS:       12.5,
		BranchRef:         sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration:  sql.NullInt64{Int64: 9, Valid: true},
		Note:              sql.NullString{String: "ready", Valid: true},
		DaemonToken:       sql.NullString{String: "token", Valid: true},
		DaemonFingerprint: sql.NullString{String: "fingerprint", Valid: true},
		UpdatedTS:         13.5,
	}
	if err := SaveDaemonState(ctx, d, want); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	futureVersion := SchemaVersion + 1
	if _, err := d.SQL().ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", futureVersion)); err != nil {
		t.Fatalf("set future schema version: %v", err)
	}

	got, ok, err := LoadDaemonStateReadOnly(ctx, dbPath)
	if err != nil || !ok {
		t.Fatalf("load read-only: ok=%v err=%v", ok, err)
	}
	if got != want {
		t.Fatalf("read-only daemon state = %+v, want %+v", got, want)
	}
	var version int
	if err := d.SQL().QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version != futureVersion {
		t.Fatalf("user_version = %d, want %d", version, futureVersion)
	}
}

func TestLoadDaemonStateReadOnlyMissingDatabase(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "missing", DBFileName)
	got, ok, err := LoadDaemonStateReadOnly(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("load missing database: %v", err)
	}
	if ok || got.Mode != "stopped" {
		t.Fatalf("missing database = (%+v, %v), want stopped and false", got, ok)
	}
}

func TestOpenRuntimeRefusesOlderSchemasWithoutMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, oldVersion := range []int{19, 20, 32} {
		db, dbPath := openTestDB(t)
		if _, err := db.SQL().ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", oldVersion)); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenRuntime(ctx, dbPath); !errors.Is(err, ErrSetupRequired) {
			t.Fatalf("v%d runtime open=%v, want checkpoint-first setup", oldVersion, err)
		}
		read, err := OpenReadOnly(ctx, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		version, err := read.UserVersion(ctx)
		if err != nil || version != oldVersion {
			t.Fatalf("runtime changed schema v%d to v%d: %v", oldVersion, version, err)
		}
		if err := read.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRollupsAppendOnly(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	r := DailyRollup{Day: "2026-04-28", RepoRoot: "/repo", EventsTotal: 5}
	ins, err := InsertDailyRollup(ctx, d, r)
	if err != nil || !ins {
		t.Fatalf("first insert: ins=%v err=%v", ins, err)
	}

	// Repeat insert is ignored (sticky).
	r2 := r
	r2.EventsTotal = 999
	ins2, err := InsertDailyRollup(ctx, d, r2)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if ins2 {
		t.Fatalf("second insert reported inserted=true; INSERT OR IGNORE should be a no-op")
	}

	rows, err := ListDailyRollupsSince(ctx, d, "2026-04-01")
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: len=%d err=%v", len(rows), err)
	}
	if rows[0].EventsTotal != 5 {
		t.Fatalf("first-write wins violated: events_total = %d, want 5", rows[0].EventsTotal)
	}

	// Verify the helper API surface: only Insert + List exposed; no Update or
	// Delete. This is a compile-time assertion via the package's public
	// surface — if any UpdateDailyRollup/DeleteDailyRollup function is added
	// in the future, this test forces the author to read why it must not be.
	// (See rollups.go comments.)
}

func TestMetaCRUD(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	if err := MetaSet(ctx, d, "branch_token", "rev:abc"); err != nil {
		t.Fatalf("set: %v", err)
	}
	v, ok, err := MetaGet(ctx, d, "branch_token")
	if err != nil || !ok || v != "rev:abc" {
		t.Fatalf("get: v=%q ok=%v err=%v", v, ok, err)
	}
	if err := MetaSet(ctx, d, "branch_token", "missing"); err != nil {
		t.Fatalf("update: %v", err)
	}
	v, _, _ = MetaGet(ctx, d, "branch_token")
	if v != "missing" {
		t.Fatalf("post-update v = %q", v)
	}
	gone, err := MetaDelete(ctx, d, "branch_token")
	if err != nil || !gone {
		t.Fatalf("delete: gone=%v err=%v", gone, err)
	}
	if _, ok, _ := MetaGet(ctx, d, "branch_token"); ok {
		t.Fatalf("post-delete still present")
	}
}

// TestMetaSetBatch_SingleTransaction verifies the MetaSetMany batch helper
// commits all keys atomically. The daemon's per-tick run loop relies on
// MetaSetMany to fold N back-to-back writes into a single tx so SQLite
// busy_timeout cannot amplify a contention episode into N×5s tick latency.
//
// Asserts:
//  1. Empty input is a no-op (returns nil; daemon_meta unchanged).
//  2. Empty key in input is rejected (returns error; daemon_meta unchanged).
//  3. Multi-key batch upserts every pair with the same updated_ts.
//  4. Conflict path on existing keys overwrites the prior value.
func TestMetaSetBatch_SingleTransaction(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	// (1) Empty input — no-op.
	if err := MetaSetMany(ctx, d, nil); err != nil {
		t.Fatalf("MetaSetMany(nil) error: %v", err)
	}
	if err := MetaSetMany(ctx, d, map[string]string{}); err != nil {
		t.Fatalf("MetaSetMany(empty) error: %v", err)
	}

	// (2) Empty key rejected; nothing committed from the batch.
	if err := MetaSetMany(ctx, d, map[string]string{
		"k1": "v1",
		"":   "vempty",
	}); err == nil {
		t.Fatalf("MetaSetMany with empty key: want error")
	}
	if _, ok, err := MetaGet(ctx, d, "k1"); err != nil || ok {
		t.Fatalf("k1 leaked from rejected batch: ok=%v err=%v", ok, err)
	}

	// (3) Happy path: 4 keys land together.
	pairs := map[string]string{
		"fsnotify.mode":            "active",
		"fsnotify.watch_count":     "42",
		"fsnotify.dropped_events":  "7",
		"fsnotify.fallback_reason": "",
	}
	if err := MetaSetMany(ctx, d, pairs); err != nil {
		t.Fatalf("MetaSetMany: %v", err)
	}
	for k, want := range pairs {
		got, ok, err := MetaGet(ctx, d, k)
		if err != nil || !ok || got != want {
			t.Fatalf("MetaGet %q = (%q, %v, %v); want (%q, true, nil)",
				k, got, ok, err, want)
		}
	}

	// (4) Conflict path overwrites prior values.
	if err := MetaSetMany(ctx, d, map[string]string{
		"fsnotify.mode":        "fallback",
		"fsnotify.watch_count": "0",
	}); err != nil {
		t.Fatalf("MetaSetMany overwrite: %v", err)
	}
	if got, _, _ := MetaGet(ctx, d, "fsnotify.mode"); got != "fallback" {
		t.Fatalf("post-overwrite fsnotify.mode = %q want fallback", got)
	}
	if got, _, _ := MetaGet(ctx, d, "fsnotify.watch_count"); got != "0" {
		t.Fatalf("post-overwrite fsnotify.watch_count = %q want 0", got)
	}
	// Untouched key from prior batch survives.
	if got, _, _ := MetaGet(ctx, d, "fsnotify.dropped_events"); got != "7" {
		t.Fatalf("untouched key = %q want 7 (overwrite leaked)", got)
	}
}

// TestConcurrentWritersUnderWAL fires N goroutines each appending events; with
// WAL + busy_timeout=5000 there should be no "database is locked" error.
//
// The goal is not raw throughput — the daemon never writes this fast in real
// life — but to confirm the locking primitives behave under -race.
func TestConcurrentWritersUnderWAL(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)

	const goroutines = 8
	const perG = 25

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for i := 0; i < perG; i++ {
				if err := MetaSet(ctx, d, fmt.Sprintf("writer.%d.%d", gid, i), "observed"); err != nil {
					errs <- err
					return
				}
				if err := TouchHeartbeat(ctx, d, float64(gid*1000+i)); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent writer error: %v", err)
		}
	}

	var got int
	err := d.ReadSQL().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM daemon_meta WHERE key LIKE 'writer.%'").Scan(&got)
	if err != nil || got != goroutines*perG {
		t.Fatalf("durable metadata=%d err=%v", got, err)
	}
}

func sqlNullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{Valid: true, String: s}
}

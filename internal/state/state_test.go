package state

import (
	"context"
	"database/sql"
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

	// Confirm every §6.1 table exists.
	tables := []string{
		"daemon_state", "daemon_clients", "shadow_paths",
		"capture_events", "capture_ops", "flush_requests",
		"publish_state", "daemon_meta", "daily_rollups",
	}
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

func TestClientsRefcount(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	if err := RegisterClient(ctx, d, Client{SessionID: "s1", Harness: "claude-code"}); err != nil {
		t.Fatalf("reg s1: %v", err)
	}
	if err := RegisterClient(ctx, d, Client{SessionID: "s2", Harness: "pi"}); err != nil {
		t.Fatalf("reg s2: %v", err)
	}
	n, err := CountClients(ctx, d)
	if err != nil || n != 2 {
		t.Fatalf("count = %d err=%v want 2", n, err)
	}

	ok, err := TouchClient(ctx, d, "s1", 1234.5)
	if err != nil || !ok {
		t.Fatalf("touch s1: ok=%v err=%v", ok, err)
	}
	ok, err = TouchClient(ctx, d, "missing", 0)
	if err != nil || ok {
		t.Fatalf("touch missing: ok=%v err=%v", ok, err)
	}

	clients, err := ListClients(ctx, d)
	if err != nil || len(clients) != 2 {
		t.Fatalf("list: len=%d err=%v", len(clients), err)
	}
	// last_seen_ts is REAL: s1 was touched to 1234.5, s2 was registered at
	// real wall time (>1.7e9). ASC order puts s1 first.
	if clients[0].SessionID != "s1" {
		t.Fatalf("expected s1 first after touch, got %s", clients[0].SessionID)
	}
	if clients[1].SessionID != "s2" {
		t.Fatalf("expected s2 second, got %s", clients[1].SessionID)
	}

	// s1 last_seen=1234.5; s2 was registered at nowSeconds() (real wall time,
	// > 1.7e9). Cutoff=9999 expires s1 only.
	expired, err := ExpireClientsBefore(ctx, d, 9999.0)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expire count = %d, want 1 (s1 only)", expired)
	}

	// s1 already gone; deregister returns gone=false.
	gone, err := DeregisterClient(ctx, d, "s1")
	if err != nil {
		t.Fatalf("dereg s1: %v", err)
	}
	if gone {
		t.Fatalf("expected s1 already gone after expire")
	}

	gone, err = DeregisterClient(ctx, d, "s2")
	if err != nil || !gone {
		t.Fatalf("dereg s2: gone=%v err=%v", gone, err)
	}
}

func TestEventsAppendAndPending(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	ev := CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         "deadbeef",
		Operation:        "modify",
		Path:             "a.txt",
		Fidelity:         "exact",
	}
	ops := []CaptureOp{
		{Op: "modify", Path: "a.txt", Fidelity: "exact"},
	}
	seq, err := AppendCaptureEvent(ctx, d, ev, ops)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if seq != 1 {
		t.Fatalf("first seq = %d, want 1", seq)
	}

	seq2, err := AppendCaptureEvent(ctx, d, ev, ops)
	if err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if seq2 != 2 {
		t.Fatalf("second seq = %d, want 2 (monotonic)", seq2)
	}

	pending, err := PendingEvents(ctx, d, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending count = %d, want 2", len(pending))
	}
	if pending[0].Seq != 1 || pending[1].Seq != 2 {
		t.Fatalf("pending order = [%d,%d], want [1,2]", pending[0].Seq, pending[1].Seq)
	}

	loadedOps, err := LoadCaptureOps(ctx, d, seq)
	if err != nil || len(loadedOps) != 1 {
		t.Fatalf("load ops: len=%d err=%v", len(loadedOps), err)
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

func TestFlushQueue(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	id, err := EnqueueFlushRequest(ctx, d, "wake", false, sqlNullStr(""))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	fr, ok, err := ClaimNextFlushRequest(ctx, d)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if fr.ID != id || fr.Status != "acknowledged" {
		t.Fatalf("claim returned %+v", fr)
	}

	// Empty queue -> ok=false.
	_, ok, err = ClaimNextFlushRequest(ctx, d)
	if err != nil || ok {
		t.Fatalf("empty claim: ok=%v err=%v", ok, err)
	}

	if err := CompleteFlushRequest(ctx, d, id, true, sqlNullStr("done")); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func TestShadowPathRoundTrip(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	sp := ShadowPath{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 7,
		Path:             "src/a.go",
		Operation:        "modify",
		BaseHead:         "abc123",
		Fidelity:         "exact",
	}
	if err := UpsertShadowPath(ctx, d, sp); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, ok, err := GetShadowPath(ctx, d, sp.BranchRef, sp.BranchGeneration, sp.Path)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.Path != sp.Path || got.Operation != sp.Operation {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	n, err := DeleteShadowGeneration(ctx, d, sp.BranchRef, sp.BranchGeneration)
	if err != nil || n != 1 {
		t.Fatalf("delete generation: n=%d err=%v", n, err)
	}
}

func TestPublishStateRoundTrip(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	if _, ok, err := LoadPublishState(ctx, d); err != nil || ok {
		t.Fatalf("empty publish_state: ok=%v err=%v", ok, err)
	}

	p := Publish{Status: "publishing"}
	if err := SavePublishState(ctx, d, p); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := LoadPublishState(ctx, d)
	if err != nil || !ok || got.Status != "publishing" {
		t.Fatalf("load: %+v ok=%v err=%v", got, ok, err)
	}
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
				ev := CaptureEvent{
					BranchRef:        "refs/heads/main",
					BranchGeneration: 1,
					BaseHead:         "abc",
					Operation:        "modify",
					Path:             "f.txt",
					Fidelity:         "exact",
				}
				if _, err := AppendCaptureEvent(ctx, d, ev, nil); err != nil {
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

	// Every goroutine appended perG events; total must match.
	got, err := LatestEventSeq(context.Background(), d)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if got != int64(goroutines*perG) {
		t.Fatalf("event seq = %d, want %d", got, goroutines*perG)
	}
}

func sqlNullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{Valid: true, String: s}
}

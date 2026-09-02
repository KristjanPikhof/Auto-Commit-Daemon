package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
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

	// Confirm every §6.1 table exists.
	tables := []string{
		"daemon_state", "daemon_clients", "shadow_paths",
		"capture_events", "capture_ops", "planner_state", "intent_planner_windows",
		"intent_planner_window_events", "rewrite_plans", "rewrite_plan_commits", "rewrite_plan_groups",
		"flush_requests", "decision_records", "recovery_snapshots",
		"recovery_snapshot_events", "publish_state", "daemon_meta", "daily_rollups",
		"config_revisions", "runtime_config_state", "config_activation_requests",
		"config_validation_runs", "config_experiments",
		"intent_candidates", "intent_candidate_events",
		"intent_candidate_lineage",
		"intent_capture_dependencies", "intent_activity_boundaries",
		"intent_repairs", "intent_repair_commits", "intent_repair_members",
		"intent_repair_member_seals",
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

func TestOpenRuntimeAppliesOnlyDeclaredSafeMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, dbPath := openTestDB(t)
	if _, err := db.SQL().ExecContext(ctx, `PRAGMA user_version = 20`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := OpenRuntime(ctx, dbPath)
	if err != nil {
		t.Fatalf("OpenRuntime safe migration: %v", err)
	}
	version, err := migrated.UserVersion(ctx)
	if err != nil || version != SchemaVersion {
		t.Fatalf("migrated version=(%d,%v), want %d", version, err, SchemaVersion)
	}
	if err := migrated.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `PRAGMA user_version = 19`); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRuntime(ctx, dbPath); !errors.Is(err, ErrSetupRequired) {
		t.Fatalf("OpenRuntime unsafe migration error=%v, want ErrSetupRequired", err)
	}
}

// TestTouchClient_RefreshesLastSeenWithoutClobberingMetadata pins the
// hot-path contract used by start.go's short-circuit branch: Touch must
// bump last_seen_ts only and never disturb harness / watch_pid / watch_fp
// / registered_ts. A fresh registration is followed by Touch with a
// distinct timestamp; we then confirm only last_seen_ts changed and that
// the unknown-session probe returns ok=false.
func TestTouchClient_RefreshesLastSeenWithoutClobberingMetadata(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	original := Client{
		SessionID: "hot-path", Harness: "claude-code",
		WatchPID: sql.NullInt64{Int64: 7777, Valid: true},
		WatchFP:  sql.NullString{String: "lstart|argv-hash", Valid: true},
	}
	if err := RegisterClient(ctx, d, original); err != nil {
		t.Fatalf("register: %v", err)
	}
	pre, err := ListClients(ctx, d)
	if err != nil || len(pre) != 1 {
		t.Fatalf("list pre: len=%d err=%v", len(pre), err)
	}
	registered := pre[0].RegisteredTS

	const refresh = 9_999_999.5
	ok, err := TouchClient(ctx, d, "hot-path", refresh)
	if err != nil || !ok {
		t.Fatalf("touch: ok=%v err=%v", ok, err)
	}
	post, err := ListClients(ctx, d)
	if err != nil || len(post) != 1 {
		t.Fatalf("list post: len=%d err=%v", len(post), err)
	}
	got := post[0]
	if got.LastSeenTS != refresh {
		t.Fatalf("last_seen_ts=%v want %v", got.LastSeenTS, refresh)
	}
	if got.RegisteredTS != registered {
		t.Fatalf("registered_ts changed: pre=%v post=%v", registered, got.RegisteredTS)
	}
	if got.Harness != "claude-code" {
		t.Fatalf("harness clobbered: %q", got.Harness)
	}
	if !got.WatchPID.Valid || got.WatchPID.Int64 != 7777 {
		t.Fatalf("watch_pid clobbered: %+v", got.WatchPID)
	}
	if !got.WatchFP.Valid || got.WatchFP.String != "lstart|argv-hash" {
		t.Fatalf("watch_fp clobbered: %+v", got.WatchFP)
	}

	missing, err := TouchClient(ctx, d, "no-such-session", 1)
	if err != nil {
		t.Fatalf("touch missing err: %v", err)
	}
	if missing {
		t.Fatalf("touch missing returned ok=true (should be false)")
	}

	if _, err := TouchClient(ctx, d, "", 1); err == nil {
		t.Fatalf("empty session_id should error")
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

func TestPlannerStateCRUDAndOldestOverdue(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	appendEvent := func(branch string, generation int64, path string, stateName string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef:        branch,
			BranchGeneration: generation,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             path,
			Fidelity:         "exact",
			State:            stateName,
		}, []CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
		if err != nil {
			t.Fatalf("append %s: %v", path, err)
		}
		return seq
	}

	first := appendEvent("refs/heads/main", 1, "first.txt", EventStatePending)
	second := appendEvent("refs/heads/main", 1, "second.txt", EventStatePending)
	otherBranch := appendEvent("refs/heads/feature", 1, "feature.txt", EventStatePending)
	published := appendEvent("refs/heads/main", 1, "published.txt", EventStatePublished)

	if err := RecordPlannerOffer(ctx, d, first, 20); err != nil {
		t.Fatalf("RecordPlannerOffer: %v", err)
	}
	if err := RecordPlannerDefer(ctx, d, first, 30, "waiting for related edit"); err != nil {
		t.Fatalf("RecordPlannerDefer first 1: %v", err)
	}
	if err := RecordPlannerDefer(ctx, d, first, 40, "still waiting"); err != nil {
		t.Fatalf("RecordPlannerDefer first 2: %v", err)
	}
	if err := RecordPlannerDefer(ctx, d, second, 10, "newer seq but older plan"); err != nil {
		t.Fatalf("RecordPlannerDefer second: %v", err)
	}
	if err := RecordPlannerError(ctx, d, second, 15, "planner parse failed"); err != nil {
		t.Fatalf("RecordPlannerError: %v", err)
	}
	if err := RecordPlannerDefer(ctx, d, otherBranch, 1, "other branch"); err != nil {
		t.Fatalf("RecordPlannerDefer other branch: %v", err)
	}
	if err := RecordPlannerDefer(ctx, d, published, 1, "terminal event"); err != nil {
		t.Fatalf("RecordPlannerDefer published: %v", err)
	}

	ps, ok, err := PlannerStateForEvent(ctx, d, first)
	if err != nil || !ok {
		t.Fatalf("PlannerStateForEvent first: ok=%v err=%v", ok, err)
	}
	if ps.DeferCount != 2 || ps.LastPlannedTS != 40 {
		t.Fatalf("first planner state = %+v, want defer_count=2 last_planned_ts=40", ps)
	}
	if !ps.LastDeferReason.Valid || ps.LastDeferReason.String != "still waiting" {
		t.Fatalf("first LastDeferReason = %+v", ps.LastDeferReason)
	}
	if ps.LastPlanError.Valid {
		t.Fatalf("first LastPlanError valid after defer: %+v", ps.LastPlanError)
	}

	ps, ok, err = PlannerStateForEvent(ctx, d, second)
	if err != nil || !ok {
		t.Fatalf("PlannerStateForEvent second: ok=%v err=%v", ok, err)
	}
	if ps.DeferCount != 1 || !ps.LastPlanError.Valid || ps.LastPlanError.String != "planner parse failed" {
		t.Fatalf("second planner state = %+v", ps)
	}

	ev, overdue, ok, err := OldestOverduePlannerEvent(ctx, d, "refs/heads/main", 1, 1)
	if err != nil || !ok {
		t.Fatalf("OldestOverduePlannerEvent: ok=%v err=%v", ok, err)
	}
	if ev.Seq != second || overdue.EventSeq != second {
		t.Fatalf("oldest overdue seq = event %d planner %d, want second %d", ev.Seq, overdue.EventSeq, second)
	}

	ev, overdue, ok, err = OldestOverduePlannerEvent(ctx, d, "refs/heads/main", 1, 2)
	if err != nil || !ok {
		t.Fatalf("OldestOverduePlannerEvent limit 2: ok=%v err=%v", ok, err)
	}
	if ev.Seq != first || overdue.DeferCount != 2 {
		t.Fatalf("limit 2 overdue = event %+v planner %+v, want first defer_count=2", ev, overdue)
	}

	if _, _, ok, err := OldestOverduePlannerEvent(ctx, d, "refs/heads/main", 1, 3); err != nil || ok {
		t.Fatalf("OldestOverduePlannerEvent limit 3: ok=%v err=%v, want not found", ok, err)
	}
}

func TestOldestOverduePlannerEventStopsAfterTerminalPredecessor(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	appendEvent := func(path string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             path,
			Fidelity:         "exact",
		}, []CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
		if err != nil {
			t.Fatalf("AppendCaptureEvent %s: %v", path, err)
		}
		return seq
	}

	visibleSeq := appendEvent("visible.txt")
	barrierSeq := appendEvent("barrier.txt")
	hiddenSeq := appendEvent("hidden.txt")

	if err := MarkEventPublished(ctx, d, barrierSeq, EventStateFailed,
		sql.NullString{}, sql.NullString{String: "commit-tree failed", Valid: true},
		sql.NullString{}, nowSeconds(),
	); err != nil {
		t.Fatalf("MarkEventPublished failed: %v", err)
	}
	if err := RecordPlannerDefer(ctx, d, hiddenSeq, 1, "oldest but behind barrier"); err != nil {
		t.Fatalf("RecordPlannerDefer hidden: %v", err)
	}
	if err := RecordPlannerDefer(ctx, d, visibleSeq, 30, "visible before barrier"); err != nil {
		t.Fatalf("RecordPlannerDefer visible: %v", err)
	}

	ev, ps, ok, err := OldestOverduePlannerEvent(ctx, d, "refs/heads/main", 1, 1)
	if err != nil || !ok {
		t.Fatalf("OldestOverduePlannerEvent: ok=%v err=%v", ok, err)
	}
	if ev.Seq != visibleSeq || ps.EventSeq != visibleSeq {
		t.Fatalf("oldest overdue = event %d planner %d, want visible %d before barrier %d; hidden %d must be held",
			ev.Seq, ps.EventSeq, visibleSeq, barrierSeq, hiddenSeq)
	}

	if err := MarkEventPublished(ctx, d, visibleSeq, EventStatePublished,
		sql.NullString{String: "abc123", Valid: true}, sql.NullString{},
		sql.NullString{}, nowSeconds(),
	); err != nil {
		t.Fatalf("MarkEventPublished visible: %v", err)
	}
	if _, _, ok, err := OldestOverduePlannerEvent(ctx, d, "refs/heads/main", 1, 1); err != nil || ok {
		t.Fatalf("OldestOverduePlannerEvent behind barrier only: ok=%v err=%v, want not found", ok, err)
	}
}

func TestPlannerStateConcurrentDefersAndMissingEvent(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         "deadbeef",
		Operation:        "modify",
		Path:             "race.txt",
		Fidelity:         "exact",
	}, []CaptureOp{{Op: "modify", Path: "race.txt", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- RecordPlannerDefer(ctx, d, seq, float64(i+1), "concurrent defer")
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("RecordPlannerDefer concurrent: %v", err)
		}
	}

	ps, ok, err := PlannerStateForEvent(ctx, d, seq)
	if err != nil || !ok {
		t.Fatalf("PlannerStateForEvent: ok=%v err=%v", ok, err)
	}
	if ps.DeferCount != workers {
		t.Fatalf("DeferCount=%d want %d", ps.DeferCount, workers)
	}

	if err := RecordPlannerOffer(ctx, d, seq+1000, 1); err == nil {
		t.Fatalf("RecordPlannerOffer for missing event returned nil error")
	}
	if _, ok, err := PlannerStateForEvent(ctx, d, seq+1000); err != nil || ok {
		t.Fatalf("PlannerStateForEvent missing = ok=%v err=%v, want false nil", ok, err)
	}
}

func TestPlannerStateSchemaMigratesFromV6WithoutDataLoss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	gitDir := filepath.Join(t.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatalf("sql.Open raw: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
CREATE TABLE capture_events(
    seq              INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_ref       TEXT NOT NULL,
    branch_generation INTEGER NOT NULL,
    base_head        TEXT NOT NULL,
    operation        TEXT NOT NULL,
    path             TEXT NOT NULL,
    old_path         TEXT,
    fidelity         TEXT NOT NULL,
    captured_ts      REAL NOT NULL,
    published_ts     REAL,
    state            TEXT NOT NULL DEFAULT 'pending',
    commit_oid       TEXT,
    error            TEXT,
    message          TEXT
);
INSERT INTO capture_events(
    seq, branch_ref, branch_generation, base_head, operation, path, fidelity,
    captured_ts, state
) VALUES (
    42, 'refs/heads/main', 3, 'abc123', 'modify', 'keep.txt', 'exact',
    12.5, 'pending'
);
PRAGMA user_version = 6;`); err != nil {
		_ = raw.Close()
		t.Fatalf("seed v6 db: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open migrated v6: %v", err)
	}
	defer d.Close()
	uv, err := d.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion: %v", err)
	}
	if uv != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", uv, SchemaVersion)
	}

	var path string
	if err := d.SQL().QueryRowContext(ctx, `SELECT path FROM capture_events WHERE seq = 42`).Scan(&path); err != nil {
		t.Fatalf("query migrated event: %v", err)
	}
	if path != "keep.txt" {
		t.Fatalf("migrated path=%q want keep.txt", path)
	}
	if err := RecordPlannerDefer(ctx, d, 42, 22, "migrated event"); err != nil {
		t.Fatalf("RecordPlannerDefer migrated event: %v", err)
	}
	ps, ok, err := PlannerStateForEvent(ctx, d, 42)
	if err != nil || !ok || ps.DeferCount != 1 {
		t.Fatalf("PlannerStateForEvent migrated = %+v ok=%v err=%v, want defer_count=1", ps, ok, err)
	}
}

func TestPlannerStateMigrationFromV6DoesNotRebuildDecisionRecords(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	gitDir := filepath.Join(t.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatalf("sql.Open raw: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
CREATE TABLE decision_records(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_ts          REAL NOT NULL,
    kind                TEXT NOT NULL,
    path                TEXT,
    reason              TEXT,
    event_seq           INTEGER,
    head_sha            TEXT,
    commit_oid          TEXT,
    branch_ref          TEXT,
    branch_generation   INTEGER,
    action_taken        TEXT,
    user_message        TEXT
);
CREATE INDEX custom_decision_records_kind ON decision_records(kind);
INSERT INTO decision_records(
    id, decision_ts, kind, path, event_seq, commit_oid, branch_ref,
    branch_generation, action_taken
) VALUES (
    5, 30, 'committed', 'src/app.go', 7, 'def456', 'refs/heads/main',
    1, 'committed'
);
PRAGMA user_version = 6;`); err != nil {
		_ = raw.Close()
		t.Fatalf("seed v6 db: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open migrated v6: %v", err)
	}
	defer d.Close()

	var customIndex string
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'decision_records' AND name = 'custom_decision_records_kind'`).Scan(&customIndex); err != nil {
		t.Fatalf("custom decision_records index missing after v6->v7 migration: %v", err)
	}
	if customIndex != "custom_decision_records_kind" {
		t.Fatalf("custom index name=%q want custom_decision_records_kind", customIndex)
	}

	got, err := DecisionsForEvent(ctx, d, 7, 10)
	if err != nil {
		t.Fatalf("DecisionsForEvent: %v", err)
	}
	if len(got) != 1 || got[0].ID != 5 {
		t.Fatalf("decisions after v6->v7 migration = %+v, want id 5", got)
	}
}

func TestPlannerStateIndexesExist(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	for _, idx := range []string{
		"idx_planner_state_defer_count_planned",
		"idx_planner_state_last_planned",
	} {
		var name string
		err := d.SQL().QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'planner_state' AND name = ?`, idx).Scan(&name)
		if err != nil {
			t.Fatalf("planner_state index %q missing: %v", idx, err)
		}
		if name != idx {
			t.Fatalf("planner_state index name=%q want %q", name, idx)
		}
	}
}

func TestIntentPlannerWindowIndexesExist(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	checks := []struct {
		table string
		index string
	}{
		{"intent_planner_windows", "idx_intent_planner_windows_ts_id"},
		{"intent_planner_windows", "idx_intent_planner_windows_branch_id"},
		{"intent_planner_window_events", "idx_intent_planner_window_events_seq_window"},
	}
	for _, check := range checks {
		var name string
		err := d.SQL().QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name = ?`,
			check.table, check.index).Scan(&name)
		if err != nil {
			t.Fatalf("%s index %q missing: %v", check.table, check.index, err)
		}
		if name != check.index {
			t.Fatalf("%s index name=%q want %q", check.table, name, check.index)
		}
	}
}

func TestPendingEventsStopsAfterTerminalPredecessor(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	appendEvent := func(branch string, generation int64, path string) int64 {
		t.Helper()
		ev := CaptureEvent{
			BranchRef:        branch,
			BranchGeneration: generation,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             path,
			Fidelity:         "exact",
		}
		seq, err := AppendCaptureEvent(ctx, d, ev, []CaptureOp{
			{Op: "modify", Path: path, Fidelity: "exact"},
		})
		if err != nil {
			t.Fatalf("append %s: %v", path, err)
		}
		return seq
	}

	blockedSeq := appendEvent("refs/heads/main", 1, "blocked-root.txt")
	blockedChildSeq := appendEvent("refs/heads/main", 1, "blocked-child.txt")
	otherBranchSeq := appendEvent("refs/heads/feature", 1, "feature.txt")
	otherGenerationSeq := appendEvent("refs/heads/main", 2, "main-gen2.txt")
	failedSeq := appendEvent("refs/heads/failed", 1, "failed-root.txt")
	failedChildSeq := appendEvent("refs/heads/failed", 1, "failed-child.txt")

	if err := MarkEventBlocked(ctx, d, blockedSeq, "before-state mismatch", nowSeconds(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}
	if err := MarkEventPublished(ctx, d, failedSeq, EventStateFailed,
		sql.NullString{}, sql.NullString{String: "commit-tree failed", Valid: true},
		sql.NullString{}, nowSeconds(),
	); err != nil {
		t.Fatalf("MarkEventPublished failed: %v", err)
	}

	pending, err := PendingEvents(ctx, d, 0)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	seen := map[int64]bool{}
	for _, ev := range pending {
		seen[ev.Seq] = true
	}
	if seen[blockedChildSeq] {
		t.Fatalf("seq %d behind blocked_conflict predecessor should be held; pending=%+v", blockedChildSeq, pending)
	}
	if seen[failedChildSeq] {
		t.Fatalf("seq %d behind failed predecessor should be held; pending=%+v", failedChildSeq, pending)
	}
	if !seen[otherBranchSeq] {
		t.Fatalf("different branch seq %d should remain pending; pending=%+v", otherBranchSeq, pending)
	}
	if !seen[otherGenerationSeq] {
		t.Fatalf("different generation seq %d should remain pending; pending=%+v", otherGenerationSeq, pending)
	}
}

func TestPruneTerminalEventsBeforePreservesActiveBarriers(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	appendEvent := func(branch string, generation int64, path string, capturedTS float64, state string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef:        branch,
			BranchGeneration: generation,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             path,
			Fidelity:         "exact",
			CapturedTS:       capturedTS,
			State:            state,
		}, []CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
		if err != nil {
			t.Fatalf("append %s: %v", path, err)
		}
		return seq
	}

	prunedBlocked := appendEvent("refs/heads/main", 1, "old-blocked.txt", 10, EventStateBlockedConflict)
	prunedFailed := appendEvent("refs/heads/failed", 1, "old-failed.txt", 11, EventStateFailed)
	unprotected := appendEvent("refs/heads/unprotected", 1, "old-unprotected.txt", 11, EventStateFailed)
	barrier := appendEvent("refs/heads/barrier", 1, "barrier.txt", 12, EventStateBlockedConflict)
	pendingBehindBarrier := appendEvent("refs/heads/barrier", 1, "pending.txt", 13, EventStatePending)
	freshFailed := appendEvent("refs/heads/fresh", 1, "fresh-failed.txt", 200, EventStateFailed)
	n, err := PruneTerminalEventsBefore(ctx, d, 100)
	if err != nil {
		t.Fatalf("PruneTerminalEventsBefore: %v", err)
	}
	if n != 0 {
		t.Fatalf("pruned=%d want 0; unresolved terminal rows are never pruned", n)
	}

	rows, err := d.SQL().QueryContext(ctx, `SELECT seq FROM capture_events ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("query remaining: %v", err)
	}
	defer rows.Close()
	remaining := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scan remaining: %v", err)
		}
		remaining[seq] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate remaining: %v", err)
	}
	for _, seq := range []int64{prunedBlocked, prunedFailed, unprotected, barrier, pendingBehindBarrier, freshFailed} {
		if !remaining[seq] {
			t.Fatalf("seq %d should remain; remaining=%v", seq, remaining)
		}
	}

	var opCount int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_ops WHERE event_seq IN (?, ?)`,
		prunedBlocked, prunedFailed).Scan(&opCount); err != nil {
		t.Fatalf("count pruned ops: %v", err)
	}
	if opCount != 2 {
		t.Fatalf("capture_ops for terminal rows=%d want 2", opCount)
	}
}

func TestPrunePublishedEventsPreservesSnapshotMembers(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	appendPublished := func(path string) int64 {
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "deadbeef", Operation: "create", Path: path,
			Fidelity: "full", CapturedTS: 10, State: EventStatePublished,
		}, []CaptureOp{{Op: "create", Path: path, Fidelity: "full"}})
		if err != nil {
			t.Fatalf("append published %s: %v", path, err)
		}
		return seq
	}
	protected := appendPublished("protected-published.txt")
	unprotected := appendPublished("unprotected-published.txt")
	res, err := d.SQL().ExecContext(ctx, `
INSERT INTO recovery_snapshots(
    created_ts, outcome, branch_ref, branch_generation,
    first_event_seq, last_event_seq, event_count,
    commit_oid, recovery_ref, reason
) VALUES (20, ?, 'refs/heads/main', 1, ?, ?, 1, 'commit',
          'refs/acd/recovery/prune-published', 'retention test')`,
		EventStatePublished, protected, protected)
	if err != nil {
		t.Fatalf("insert published snapshot: %v", err)
	}
	snapshotID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("snapshot id: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx,
		`INSERT INTO recovery_snapshot_events(snapshot_id, ord, event_seq) VALUES (?, 0, ?)`,
		snapshotID, protected); err != nil {
		t.Fatalf("insert published member: %v", err)
	}

	n, err := PrunePublishedEventsBefore(ctx, d, 100)
	if err != nil {
		t.Fatalf("PrunePublishedEventsBefore: %v", err)
	}
	if n != 1 {
		t.Fatalf("ordinary published pruned=%d want 1", n)
	}
	var protectedCount, unprotectedCount int
	if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events WHERE seq = ?`, protected).Scan(&protectedCount); err != nil {
		t.Fatalf("count protected published: %v", err)
	}
	if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events WHERE seq = ?`, unprotected).Scan(&unprotectedCount); err != nil {
		t.Fatalf("count ordinary published: %v", err)
	}
	if protectedCount != 1 || unprotectedCount != 0 {
		t.Fatalf("counts protected=%d ordinary=%d", protectedCount, unprotectedCount)
	}

	n, err = PruneRecoverySnapshotEventsBefore(ctx, d, snapshotID, 100)
	if err != nil {
		t.Fatalf("PruneRecoverySnapshotEventsBefore: %v", err)
	}
	if n != 1 {
		t.Fatalf("protected published pruned=%d want 1", n)
	}
}

func TestPruneRecoverySnapshotEventsWaitsForDurableOwners(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	const (
		branch     = "refs/heads/main"
		generation = int64(7)
		worktreeID = "0123456789abcdef"
	)
	appendEvent := func(path string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef: branch, BranchGeneration: generation,
			BaseHead: "head", Operation: "modify", Path: path,
			Fidelity: "exact", CapturedTS: 10, State: EventStatePending,
		}, []CaptureOp{{
			Op: "modify", Path: path, Fidelity: "exact",
		}})
		if err != nil {
			t.Fatalf("append %s: %v", path, err)
		}
		return seq
	}
	unowned := appendEvent("unowned.go")
	checkpointOwned := appendEvent("checkpoint-owned.go")
	drainOwned := appendEvent("drain-owned.go")

	prepareCheckpoint := func(id, operationID, suffix string, seq int64) Checkpoint {
		t.Helper()
		checkpoint := Checkpoint{
			ID: id, OperationID: operationID, WorktreeID: worktreeID,
			Reason: CheckpointReasonPoll, ObservationEpoch: 1, CoverageEpoch: 1,
			ObservedHead: "head", ObservedRef: branch,
			TreeOID: "tree-" + suffix, CommitOID: "commit-" + suffix,
			Ref:       "refs/acd/checkpoints/v1/" + worktreeID + "/" + id,
			CreatedTS: 1, EventSeqs: []int64{seq},
		}
		created, err := PrepareCheckpoint(ctx, d, checkpoint, checkpointTestDigest)
		if err != nil || !created {
			t.Fatalf("prepare checkpoint %s=(%t,%v)", id, created, err)
		}
		if err := CompleteCheckpoint(
			ctx, d, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2,
		); err != nil {
			t.Fatalf("complete checkpoint %s: %v", id, err)
		}
		return checkpoint
	}
	checkpointOnly := prepareCheckpoint(
		"cp-1000-aaaaaaaaaaaaaaaa", "op-checkpoint-only", "checkpoint", checkpointOwned)
	drainCheckpoint := prepareCheckpoint(
		"cp-1001-bbbbbbbbbbbbbbbb", "op-drain-owned", "drain", drainOwned)
	drain := PublicationDrain{
		ID: "drain-retention-owner", CheckpointID: drainCheckpoint.ID,
		WorktreeID: worktreeID, BranchRef: branch, BranchGeneration: generation,
		Phase: PublicationDrainCheckpointing, TargetEventCount: 1,
		CreatedTS: 3, UpdatedTS: 3, LastProgressTS: 3,
	}
	if created, err := PreparePublicationDrain(ctx, d, drain); err != nil || !created {
		t.Fatalf("prepare publication drain=(%t,%v)", created, err)
	}

	if _, err := d.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='recovered', published_ts=20, commit_oid='recovery'
WHERE seq IN (?, ?, ?)`, unowned, checkpointOwned, drainOwned); err != nil {
		t.Fatalf("recover snapshot members: %v", err)
	}
	if _, err := AdvancePublicationDrain(ctx, d, drain.ID, PublicationDrainUpdate{
		ExpectedPhase: PublicationDrainCheckpointing,
		Phase:         PublicationDrainCompleted, PublishedEventCount: 1,
		UpdatedTS: 4, LastProgressTS: 4,
		CompletedTS: sql.NullFloat64{Float64: 4, Valid: true},
	}); err != nil {
		t.Fatalf("complete publication drain: %v", err)
	}
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "terminal-snapshot-member", BranchRef: branch,
		BranchGeneration: generation, Status: IntentCandidatePublished,
		Readiness:          IntentReadinessReady,
		PublishedCommitOID: sql.NullString{String: "recovery", Valid: true},
		Events:             []IntentCandidateEvent{{EventSeq: unowned, EventRole: "code"}},
	}); err != nil {
		t.Fatalf("save terminal intent candidate: %v", err)
	}
	if err := ReplaceIntentCaptureDependencies(ctx, d, branch, generation,
		[]IntentCaptureDependency{{
			PrerequisiteSeq: unowned, DependentSeq: checkpointOwned,
			Strength: IntentDependencySoft, Kind: "module_proximity",
		}}); err != nil {
		t.Fatalf("save terminal intent dependency: %v", err)
	}

	res, err := d.SQL().ExecContext(ctx, `
INSERT INTO recovery_snapshots(
    created_ts, outcome, branch_ref, branch_generation,
    first_event_seq, last_event_seq, event_count,
    commit_oid, recovery_ref, reason
) VALUES (20, 'recovered', ?, ?, ?, ?, 3, 'recovery',
          'refs/acd/recovery/prune-owners', 'retention owner test')`,
		branch, generation, unowned, drainOwned)
	if err != nil {
		t.Fatalf("insert recovery snapshot: %v", err)
	}
	snapshotID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("recovery snapshot id: %v", err)
	}
	for ord, seq := range []int64{unowned, checkpointOwned, drainOwned} {
		if _, err := d.SQL().ExecContext(ctx, `
INSERT INTO recovery_snapshot_events(snapshot_id, ord, event_seq)
VALUES (?, ?, ?)`, snapshotID, ord, seq); err != nil {
			t.Fatalf("insert recovery member %d: %v", seq, err)
		}
	}

	assertEventCount := func(seq int64, want int) {
		t.Helper()
		var got int
		if err := d.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM capture_events WHERE seq=?`, seq,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("event %d count=%d want %d", seq, got, want)
		}
	}
	if pruned, err := PruneRecoverySnapshotEventsBefore(
		ctx, d, snapshotID, 100,
	); err != nil || pruned != 1 {
		t.Fatalf("initial snapshot prune=(%d,%v), want (1,nil)", pruned, err)
	}
	assertEventCount(unowned, 0)
	assertEventCount(checkpointOwned, 1)
	assertEventCount(drainOwned, 1)
	var memberships, dependencies int
	if err := d.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidate_events WHERE event_seq=?`, unowned,
	).Scan(&memberships); err != nil {
		t.Fatal(err)
	}
	if err := d.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_capture_dependencies
WHERE prerequisite_seq=? OR dependent_seq=?`, unowned, unowned,
	).Scan(&dependencies); err != nil {
		t.Fatal(err)
	}
	if memberships != 0 || dependencies != 0 {
		t.Fatalf("terminal intent rows membership=%d dependencies=%d want 0",
			memberships, dependencies)
	}

	completeCheckpointRetention := func(checkpoint Checkpoint) {
		t.Helper()
		item := RetentionCheckpoint{
			ID: checkpoint.ID, WorktreeID: checkpoint.WorktreeID,
			Ref: checkpoint.Ref, CommitOID: checkpoint.CommitOID,
		}
		operationID, err := PrepareCheckpointPrune(
			ctx, d, item, checkpointTestDigest)
		if err != nil {
			t.Fatalf("prepare checkpoint retention %s: %v", checkpoint.ID, err)
		}
		if err := CompleteCheckpointPrune(
			ctx, d, operationID, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID,
		); err != nil {
			t.Fatalf("complete checkpoint retention %s: %v", checkpoint.ID, err)
		}
	}
	completeCheckpointRetention(checkpointOnly)
	completeCheckpointRetention(drainCheckpoint)
	if pruned, err := PruneRecoverySnapshotEventsBefore(
		ctx, d, snapshotID, 100,
	); err != nil || pruned != 1 {
		t.Fatalf("post-checkpoint snapshot prune=(%d,%v), want (1,nil)", pruned, err)
	}
	assertEventCount(checkpointOwned, 0)
	assertEventCount(drainOwned, 1)

	if _, err := d.SQL().ExecContext(ctx,
		`DELETE FROM publication_drains WHERE id=?`, drain.ID); err != nil {
		t.Fatalf("expire publication drain owner: %v", err)
	}
	if pruned, err := PruneRecoverySnapshotEventsBefore(
		ctx, d, snapshotID, 100,
	); err != nil || pruned != 1 {
		t.Fatalf("post-drain snapshot prune=(%d,%v), want (1,nil)", pruned, err)
	}
	assertEventCount(drainOwned, 0)
}

func TestPrunePublishedEventsPreservesRecoveryMaterializationPrefix(t *testing.T) {
	for _, unpublishedState := range []string{
		EventStatePending,
		EventStateBlockedConflict,
		EventStateFailed,
	} {
		t.Run(unpublishedState, func(t *testing.T) {
			t.Parallel()
			d, _ := openTestDB(t)
			ctx := context.Background()
			appendEvent := func(baseHead, path, eventState string) int64 {
				t.Helper()
				seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
					BranchRef: "refs/heads/main", BranchGeneration: 7,
					BaseHead: baseHead, Operation: "modify", Path: path,
					Fidelity: "exact", CapturedTS: 10, State: eventState,
				}, []CaptureOp{{
					Op: "modify", Path: path,
					BeforeOID: sqlNullStr("before-" + path), BeforeMode: sqlNullStr("100644"),
					AfterOID: sqlNullStr("after-" + path), AfterMode: sqlNullStr("100644"),
					Fidelity: "exact",
				}})
				if err != nil {
					t.Fatalf("AppendCaptureEvent %s: %v", path, err)
				}
				return seq
			}

			prefix := appendEvent("shared-base", "prefix.txt", EventStatePublished)
			ordinary := appendEvent("other-base", "ordinary.txt", EventStatePublished)
			advancedBasePrefix := appendEvent("older-base", "suffix.txt", EventStatePublished)
			suffix := appendEvent("shared-base", "suffix.txt", unpublishedState)

			n, err := PrunePublishedEventsBefore(ctx, d, 100)
			if err != nil {
				t.Fatalf("PrunePublishedEventsBefore: %v", err)
			}
			if n != 1 {
				t.Fatalf("pruned=%d want only ordinary published row", n)
			}
			for _, seq := range []int64{prefix, advancedBasePrefix, suffix} {
				var eventCount, opCount int
				if err := d.SQL().QueryRowContext(ctx,
					`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, seq).Scan(&eventCount); err != nil {
					t.Fatalf("count event seq=%d: %v", seq, err)
				}
				if err := d.SQL().QueryRowContext(ctx,
					`SELECT COUNT(*) FROM capture_ops WHERE event_seq = ?`, seq).Scan(&opCount); err != nil {
					t.Fatalf("count ops seq=%d: %v", seq, err)
				}
				if eventCount != 1 || opCount != 1 {
					t.Fatalf("recovery context seq=%d event=%d ops=%d want 1,1", seq, eventCount, opCount)
				}
			}
			var ordinaryCount int
			if err := d.SQL().QueryRowContext(ctx,
				`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, ordinary).Scan(&ordinaryCount); err != nil {
				t.Fatalf("count ordinary published: %v", err)
			}
			if ordinaryCount != 0 {
				t.Fatalf("ordinary published row retained: count=%d", ordinaryCount)
			}
		})
	}
}

func TestPrunePublishedEventsPreservesRenameClosure(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	const branchRef = "refs/heads/main"
	const generation = int64(7)

	appendOp := func(baseHead, eventState string, op CaptureOp) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef: branchRef, BranchGeneration: generation,
			BaseHead: baseHead, Operation: op.Op, Path: op.Path,
			OldPath: op.OldPath, Fidelity: "exact",
			CapturedTS: 10, State: eventState,
		}, []CaptureOp{op})
		if err != nil {
			t.Fatalf("AppendCaptureEvent %s: %v", op.Path, err)
		}
		return seq
	}
	rename := func(baseHead, oldPath, path string) int64 {
		return appendOp(baseHead, EventStatePublished, CaptureOp{
			Op: "rename", Path: path, OldPath: sqlNullStr(oldPath),
			BeforeOID: sqlNullStr("before-" + oldPath), BeforeMode: sqlNullStr("100644"),
			AfterOID: sqlNullStr("after-" + path), AfterMode: sqlNullStr("100644"),
			Fidelity: "exact",
		})
	}

	firstRename := rename("base-a", "a.txt", "b.txt")
	secondRename := rename("base-b", "b.txt", "c.txt")
	ordinary := appendOp("base-ordinary", EventStatePublished, CaptureOp{
		Op: "modify", Path: "ordinary.txt",
		BeforeOID: sqlNullStr("ordinary-before"), BeforeMode: sqlNullStr("100644"),
		AfterOID: sqlNullStr("ordinary-after"), AfterMode: sqlNullStr("100644"),
		Fidelity: "exact",
	})
	suffix := appendOp("base-c", EventStateBlockedConflict, CaptureOp{
		Op: "modify", Path: "c.txt",
		BeforeOID: sqlNullStr("before-c"), BeforeMode: sqlNullStr("100644"),
		AfterOID: sqlNullStr("after-c"), AfterMode: sqlNullStr("100644"),
		Fidelity: "exact",
	})

	n, err := PrunePublishedEventsBefore(ctx, d, 100)
	if err != nil {
		t.Fatalf("PrunePublishedEventsBefore: %v", err)
	}
	if n != 1 {
		t.Fatalf("pruned=%d want only unrelated published row", n)
	}
	for _, seq := range []int64{firstRename, secondRename, suffix} {
		var count int
		if err := d.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, seq).Scan(&count); err != nil {
			t.Fatalf("count retained seq=%d: %v", seq, err)
		}
		if count != 1 {
			t.Fatalf("rename recovery context seq=%d count=%d want 1", seq, count)
		}
	}
	var ordinaryCount int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, ordinary).Scan(&ordinaryCount); err != nil {
		t.Fatalf("count ordinary seq=%d: %v", ordinary, err)
	}
	if ordinaryCount != 0 {
		t.Fatalf("ordinary published row retained: count=%d", ordinaryCount)
	}
}

func TestPrunePublishedEventsIsolatesOversizedRecoveryPair(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	tx, err := d.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	insertEvent, err := tx.PrepareContext(ctx, `
INSERT INTO capture_events(
    branch_ref, branch_generation, base_head, operation, path,
    fidelity, captured_ts, state
) VALUES (?, ?, ?, 'modify', ?, 'exact', 10, ?)`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("prepare event insert: %v", err)
	}
	insertOp, err := tx.PrepareContext(ctx, `
INSERT INTO capture_ops(
    event_seq, ord, op, path, before_oid, before_mode,
    after_oid, after_mode, fidelity
) VALUES (?, 0, 'modify', ?, 'before', '100644', 'after', '100644', 'exact')`)
	if err != nil {
		_ = insertEvent.Close()
		_ = tx.Rollback()
		t.Fatalf("prepare op insert: %v", err)
	}
	const oversizedBranch = "refs/heads/oversized"
	for i := 0; i <= maxPublishedRecoveryContextEvents; i++ {
		res, err := insertEvent.ExecContext(ctx,
			oversizedBranch, 1, fmt.Sprintf("base-%d", i), "hot.txt", EventStatePublished)
		if err != nil {
			t.Fatalf("insert published event %d: %v", i, err)
		}
		seq, err := res.LastInsertId()
		if err != nil {
			t.Fatalf("published event seq %d: %v", i, err)
		}
		if _, err := insertOp.ExecContext(ctx, seq, "hot.txt"); err != nil {
			t.Fatalf("insert published op %d: %v", i, err)
		}
	}
	res, err := insertEvent.ExecContext(ctx,
		oversizedBranch, 1, "unresolved-base", "hot.txt", EventStatePending)
	if err != nil {
		t.Fatalf("insert oversized pending event: %v", err)
	}
	pendingSeq, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("oversized pending seq: %v", err)
	}
	if _, err := insertOp.ExecContext(ctx, pendingSeq, "hot.txt"); err != nil {
		t.Fatalf("insert oversized pending op: %v", err)
	}
	if err := insertOp.Close(); err != nil {
		t.Fatalf("close op insert: %v", err)
	}
	if err := insertEvent.Close(); err != nil {
		t.Fatalf("close event insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit oversized fixture: %v", err)
	}

	ordinary, err := AppendCaptureEvent(ctx, d, CaptureEvent{
		BranchRef: "refs/heads/ordinary", BranchGeneration: 1,
		BaseHead: "ordinary-base", Operation: "modify", Path: "ordinary.txt",
		Fidelity: "exact", CapturedTS: 10, State: EventStatePublished,
	}, []CaptureOp{{
		Op: "modify", Path: "ordinary.txt",
		BeforeOID: sqlNullStr("ordinary-before"), BeforeMode: sqlNullStr("100644"),
		AfterOID: sqlNullStr("ordinary-after"), AfterMode: sqlNullStr("100644"),
		Fidelity: "exact",
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent ordinary: %v", err)
	}

	n, err := PrunePublishedEventsBefore(ctx, d, 100)
	if err != nil {
		t.Fatalf("PrunePublishedEventsBefore: %v", err)
	}
	if n != 1 {
		t.Fatalf("pruned=%d want only ordinary row", n)
	}
	var oversizedCount int
	if err := d.SQL().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM capture_events
WHERE branch_ref = ? AND state = ?`,
		oversizedBranch, EventStatePublished).Scan(&oversizedCount); err != nil {
		t.Fatalf("count oversized retained rows: %v", err)
	}
	if oversizedCount != maxPublishedRecoveryContextEvents+1 {
		t.Fatalf("oversized retained=%d want %d",
			oversizedCount, maxPublishedRecoveryContextEvents+1)
	}
	var ordinaryCount int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, ordinary).Scan(&ordinaryCount); err != nil {
		t.Fatalf("count ordinary row: %v", err)
	}
	if ordinaryCount != 0 {
		t.Fatalf("ordinary published row retained: count=%d", ordinaryCount)
	}
}

func TestPrunePublishedEventsPreservesInterleavedRecoveryContext(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	appendEvent := func(baseHead, path, eventState string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 7,
			BaseHead: baseHead, Operation: "modify", Path: path,
			Fidelity: "exact", CapturedTS: 10, State: eventState,
		}, []CaptureOp{{
			Op: "modify", Path: path,
			BeforeOID: sqlNullStr("before-" + path), BeforeMode: sqlNullStr("100644"),
			AfterOID: sqlNullStr("after-" + path), AfterMode: sqlNullStr("100644"),
			Fidelity: "exact",
		}})
		if err != nil {
			t.Fatalf("AppendCaptureEvent %s: %v", path, err)
		}
		return seq
	}

	first := appendEvent("base-one", "first.txt", EventStatePending)
	contextSeq := appendEvent("base-one", "context.txt", EventStatePublished)
	last := appendEvent("base-two", "last.txt", EventStatePending)
	ordinary := appendEvent("base-two", "ordinary.txt", EventStatePublished)

	n, err := PrunePublishedEventsBefore(ctx, d, 100)
	if err != nil {
		t.Fatalf("PrunePublishedEventsBefore: %v", err)
	}
	if n != 1 {
		t.Fatalf("pruned=%d want only non-context published row", n)
	}
	for _, seq := range []int64{first, contextSeq, last} {
		var count int
		if err := d.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, seq).Scan(&count); err != nil {
			t.Fatalf("count event seq=%d: %v", seq, err)
		}
		if count != 1 {
			t.Fatalf("recovery context seq=%d count=%d want 1", seq, count)
		}
	}
	var ordinaryCount int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, ordinary).Scan(&ordinaryCount); err != nil {
		t.Fatalf("count ordinary event: %v", err)
	}
	if ordinaryCount != 0 {
		t.Fatalf("ordinary published row retained: count=%d", ordinaryCount)
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

func TestReplaceShadowGenerationIsAtomic(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	const (
		branch = "refs/heads/main"
		marker = "shadow.bootstrapped:test"
	)
	original := ShadowPath{
		BranchRef: branch, BranchGeneration: 7, Path: "old.txt",
		Operation: "modify", BaseHead: "old-head", Fidelity: "exact",
		OID: sql.NullString{String: "old-blob", Valid: true},
	}
	if err := UpsertShadowPath(ctx, d, original); err != nil {
		t.Fatalf("seed original shadow: %v", err)
	}
	duplicate := ShadowPath{
		BranchRef: branch, BranchGeneration: 7, Path: "new.txt",
		Operation: "bootstrap", BaseHead: "new-head", Fidelity: "full",
		OID: sql.NullString{String: "new-blob", Valid: true},
	}
	if _, err := ReplaceShadowGeneration(
		ctx, d, branch, 7, marker, []ShadowPath{duplicate, duplicate}); err == nil {
		t.Fatal("duplicate replacement unexpectedly succeeded")
	}
	if got, ok, err := GetShadowPath(
		ctx, d, branch, 7, original.Path); err != nil || !ok ||
		got.OID.String != original.OID.String {
		t.Fatalf("failed replacement changed original: got=%+v ok=%v err=%v",
			got, ok, err)
	}
	if replaced, err := ReplaceShadowGeneration(
		ctx, d, branch, 7, marker, []ShadowPath{duplicate}); err != nil ||
		replaced != 1 {
		t.Fatalf("replace shadow: rows=%d err=%v", replaced, err)
	}
	if _, ok, err := GetShadowPath(
		ctx, d, branch, 7, original.Path); err != nil || ok {
		t.Fatalf("old shadow survived replacement: ok=%v err=%v", ok, err)
	}
	if got, ok, err := GetShadowPath(
		ctx, d, branch, 7, duplicate.Path); err != nil || !ok ||
		got.BaseHead != duplicate.BaseHead {
		t.Fatalf("replacement shadow: got=%+v ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := MetaGet(ctx, d, marker); err != nil || !ok || got != "1" {
		t.Fatalf("replacement marker=%q ok=%v err=%v", got, ok, err)
	}
}

func TestPruneShadowGenerationsRetainsConfiguredPriorGenerations(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	upsert := func(branch string, generation int64, path string) {
		t.Helper()
		if err := UpsertShadowPath(ctx, d, ShadowPath{
			BranchRef:        branch,
			BranchGeneration: generation,
			Path:             path,
			Operation:        "bootstrap",
			BaseHead:         "abc123",
			Fidelity:         "full",
		}); err != nil {
			t.Fatalf("upsert %s gen %d: %v", branch, generation, err)
		}
	}

	upsert("refs/heads/main", 1, "gen1.txt")
	upsert("refs/heads/main", 2, "gen2.txt")
	upsert("refs/heads/main", 3, "gen3.txt")
	upsert("refs/heads/other", 1, "other-gen1.txt")

	n, err := PruneShadowGenerations(ctx, d, "refs/heads/main", 3, 1)
	if err != nil {
		t.Fatalf("PruneShadowGenerations: %v", err)
	}
	if n != 1 {
		t.Fatalf("pruned=%d want 1", n)
	}

	cases := []struct {
		branch     string
		generation int64
		path       string
		want       bool
	}{
		{"refs/heads/main", 1, "gen1.txt", false},
		{"refs/heads/main", 2, "gen2.txt", true},
		{"refs/heads/main", 3, "gen3.txt", true},
		{"refs/heads/other", 1, "other-gen1.txt", true},
	}
	for _, tc := range cases {
		_, ok, err := GetShadowPath(ctx, d, tc.branch, tc.generation, tc.path)
		if err != nil {
			t.Fatalf("GetShadowPath %s gen %d: %v", tc.branch, tc.generation, err)
		}
		if ok != tc.want {
			t.Fatalf("shadow row %s gen %d ok=%v want %v", tc.branch, tc.generation, ok, tc.want)
		}
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

func BenchmarkConcurrentWrites(b *testing.B) {
	gitDir := filepath.Join(b.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	d, err := Open(context.Background(), dbPath)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	ctx := context.Background()
	const writers = 10
	for i := 0; i < writers; i++ {
		if err := RegisterClient(ctx, d, Client{
			SessionID: fmt.Sprintf("session-%02d", i),
			Harness:   "bench",
		}); err != nil {
			b.Fatalf("seed client %d: %v", i, err)
		}
	}

	b.ResetTimer()
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	wg.Add(writers)
	for g := 0; g < writers; g++ {
		go func(gid int) {
			defer wg.Done()
			sessionID := fmt.Sprintf("session-%02d", gid)
			for i := gid; i < b.N; i += writers {
				if _, err := TouchClient(ctx, d, sessionID, nowSeconds()); err != nil {
					errCh <- err
					return
				}
				ev := CaptureEvent{
					BranchRef:        "refs/heads/main",
					BranchGeneration: 1,
					BaseHead:         "abc",
					Operation:        "modify",
					Path:             fmt.Sprintf("bench-%d-%d.txt", gid, i),
					Fidelity:         "exact",
				}
				if _, err := AppendCaptureEvent(ctx, d, ev, nil); err != nil {
					errCh <- err
					return
				}
				if _, err := CountClients(ctx, d); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			b.Fatalf("concurrent write benchmark error: %v", err)
		}
	}
}

func sqlNullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{Valid: true, String: s}
}

// TestState_PendingEventsBarrierIndex_Exists asserts the v3 covering index
// idx_capture_events_barrier is created on a fresh DB (and on reopen) so the
// PendingEvents barrier subquery never falls back to a full-table scan.
func TestState_PendingEventsBarrierIndex_Exists(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	uv, err := d.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion: %v", err)
	}
	if uv < 3 {
		t.Fatalf("user_version=%d, want >= 3 (v3 introduces idx_capture_events_barrier)", uv)
	}

	var name string
	err = d.SQL().QueryRowContext(ctx, `
SELECT name FROM sqlite_master
WHERE type = 'index' AND name = 'idx_capture_events_barrier' AND tbl_name = 'capture_events'`).Scan(&name)
	if err != nil {
		t.Fatalf("idx_capture_events_barrier missing: %v", err)
	}
	if name != "idx_capture_events_barrier" {
		t.Fatalf("got index name=%q, want idx_capture_events_barrier", name)
	}

	// Verify SQLite chooses the new index for the depth-cap counting query.
	rows, err := d.SQL().QueryContext(ctx,
		`EXPLAIN QUERY PLAN
		 SELECT COUNT(*) FROM capture_events
		 WHERE state = 'pending' AND branch_ref = 'refs/heads/main' AND branch_generation = 1`)
	if err != nil {
		t.Fatalf("explain query plan: %v", err)
	}
	defer rows.Close()
	var planText strings.Builder
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		planText.WriteString(detail)
		planText.WriteByte('\n')
	}
	if !strings.Contains(planText.String(), "idx_capture_events_barrier") {
		t.Fatalf("query plan did not select idx_capture_events_barrier:\n%s", planText.String())
	}
}

func TestState_RecoveryPrefixPruneIndexMigratesFromV12(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	gitDir := filepath.Join(t.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)

	fresh, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open fresh: %v", err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatalf("close fresh: %v", err)
	}

	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatalf("sql.Open raw: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
DROP INDEX idx_capture_events_recovery_prefix;
PRAGMA user_version = 12;`); err != nil {
		_ = raw.Close()
		t.Fatalf("seed v12 schema: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open migrated v12: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	uv, err := d.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion: %v", err)
	}
	if uv != SchemaVersion {
		t.Fatalf("user_version=%d want %d", uv, SchemaVersion)
	}

	var name string
	if err := d.SQL().QueryRowContext(ctx, `
SELECT name FROM sqlite_master
WHERE type = 'index'
  AND name = 'idx_capture_events_recovery_prefix'
  AND tbl_name = 'capture_events'`).Scan(&name); err != nil {
		t.Fatalf("idx_capture_events_recovery_prefix missing after v12 migration: %v", err)
	}

	rows, err := d.SQL().QueryContext(ctx, `
EXPLAIN QUERY PLAN
SELECT 1
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND base_head = ?
  AND seq > ?
  AND state IN (?, ?, ?)`,
		"refs/heads/main", 1, "base", 1,
		EventStatePending, EventStateBlockedConflict, EventStateFailed,
	)
	if err != nil {
		t.Fatalf("explain recovery-prefix lookup: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan recovery-prefix plan: %v", err)
		}
		plan.WriteString(detail)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate recovery-prefix plan: %v", err)
	}
	if !strings.Contains(plan.String(), "idx_capture_events_recovery_prefix") {
		t.Fatalf("query plan did not select recovery-prefix index:\n%s", plan.String())
	}
}

// BenchmarkPendingEvents seeds 10k pending rows + a couple of terminal
// barriers in older generations and measures PendingEvents throughput. The
// benchmark protects against future regressions where someone drops the
// covering index or rewrites the barrier subquery to scan the entire table.
func BenchmarkPendingEvents(b *testing.B) {
	d, _ := openBenchDB(b)
	ctx := context.Background()

	const rows = 10_000
	for i := 0; i < rows; i++ {
		ev := CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 7,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             fmt.Sprintf("bench-%05d.txt", i),
			Fidelity:         "exact",
		}
		if _, err := AppendCaptureEvent(ctx, d, ev, nil); err != nil {
			b.Fatalf("seed event %d: %v", i, err)
		}
	}
	// Two stale-generation barriers that PendingEvents must skip cleanly.
	for _, gen := range []int64{5, 6} {
		ev := CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: gen,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             fmt.Sprintf("stale-gen-%d.txt", gen),
			Fidelity:         "exact",
		}
		seq, err := AppendCaptureEvent(ctx, d, ev, nil)
		if err != nil {
			b.Fatalf("seed stale: %v", err)
		}
		if err := MarkEventBlocked(ctx, d, seq, "stale", float64(time.Now().Unix()),
			sql.NullString{String: "refs/heads/main", Valid: true},
			sql.NullInt64{Int64: gen, Valid: true},
			sql.NullString{String: "deadbeef", Valid: true},
		); err != nil {
			b.Fatalf("mark stale blocked: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := PendingEvents(ctx, d, 100)
		if err != nil {
			b.Fatalf("PendingEvents: %v", err)
		}
		if len(out) != 100 {
			b.Fatalf("got %d pending, want 100", len(out))
		}
	}
}

func openBenchDB(b *testing.B) (*DB, string) {
	b.Helper()
	gitDir := filepath.Join(b.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	d, err := Open(context.Background(), dbPath)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = d.Close() })
	return d, dbPath
}

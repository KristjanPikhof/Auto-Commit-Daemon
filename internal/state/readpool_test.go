package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestPendingEvents_DoesNotBlockHeartbeat asserts that PendingEvents reads
// flow through the multi-connection read pool, so a long-running write
// transaction holding the serialized writer connection cannot starve them.
//
// Before the read-pool routing fix, PendingEvents queried d.conn (writer
// pool, MaxOpenConns=1). A replay drain holding a write tx would block the
// heartbeat loop's PendingEvents call indefinitely.
func TestPendingEvents_DoesNotBlockHeartbeat(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	// Seed one pending event so PendingEvents has something to scan.
	if _, err := AppendCaptureEvent(ctx, d, CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: "deadbeef",
		Operation: "create", Path: "f.txt", Fidelity: "exact",
	}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Hold the writer connection inside a long transaction. SQLite WAL
	// permits readers to keep working; the writer pool MaxOpenConns=1 means
	// any QueryContext routed through d.conn would block here.
	tx, err := d.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO daemon_meta(key, value, updated_ts) VALUES (?, ?, ?)`,
		"hold", "1", nowSeconds()); err != nil {
		t.Fatalf("hold write: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := PendingEvents(readCtx, d, 10)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PendingEvents under writer-hold: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PendingEvents blocked behind held writer tx — read pool not in use")
	}

	// Same probe for CountEventsByState and LatestEventSeq.
	if _, err := CountEventsByState(ctx, d, EventStatePending); err != nil {
		t.Fatalf("CountEventsByState: %v", err)
	}
	if _, err := LatestEventSeq(ctx, d); err != nil {
		t.Fatalf("LatestEventSeq: %v", err)
	}
	if _, err := LoadCaptureOps(ctx, d, 1); err != nil {
		t.Fatalf("LoadCaptureOps: %v", err)
	}
}

// TestSchemaMigrate_AddsFlushRequestsIndex confirms the v4 schema bump
// installs the (status, id) index that keeps ClaimNextFlushRequest off a
// full-table scan after long uptime.
func TestSchemaMigrate_AddsFlushRequestsIndex(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	if SchemaVersion < 4 {
		t.Fatalf("SchemaVersion = %d, want >= 4", SchemaVersion)
	}
	v, err := d.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion: %v", err)
	}
	if v != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", v, SchemaVersion)
	}

	var name string
	err = d.ReadSQL().QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='index' AND name=?`,
		"idx_flush_requests_status_id",
	).Scan(&name)
	if err != nil {
		t.Fatalf("idx_flush_requests_status_id missing: %v", err)
	}
	if name != "idx_flush_requests_status_id" {
		t.Fatalf("index name = %q", name)
	}

	// Confirm the index is actually planned for ClaimNextFlushRequest's
	// access pattern via EXPLAIN QUERY PLAN.
	rows, err := d.ReadSQL().QueryContext(ctx,
		`EXPLAIN QUERY PLAN SELECT id FROM flush_requests WHERE status='pending' ORDER BY id ASC LIMIT 1`,
	)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	saw := false
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		if containsAny(detail, "idx_flush_requests_status_id") {
			saw = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan iter: %v", err)
	}
	if !saw {
		t.Fatalf("query plan did not reference idx_flush_requests_status_id")
	}
}

// TestAppendShadowBatch_AtomicCommit verifies that AppendShadowBatch is
// transactional — a context cancel mid-batch leaves shadow_paths empty —
// and that a successful batch persists every row.
func TestAppendShadowBatch_AtomicCommit(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	// 1. Cancellation path: cancel before calling so the BeginTx sees a
	//    cancelled ctx and bails. shadow_paths must remain empty.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	rows := []ShadowPath{
		{BranchRef: "refs/heads/main", BranchGeneration: 1, Path: "a.txt", Operation: "create", BaseHead: "abc", Fidelity: "exact"},
		{BranchRef: "refs/heads/main", BranchGeneration: 1, Path: "b.txt", Operation: "create", BaseHead: "abc", Fidelity: "exact"},
	}
	err := AppendShadowBatch(cancelled, d, rows)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("AppendShadowBatch w/ cancelled ctx: err=%v, want context.Canceled", err)
	}
	var n int
	if err := d.ReadSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("shadow_paths after cancel = %d, want 0", n)
	}

	// 2. Required-field validation also leaves the table empty.
	bad := []ShadowPath{
		{BranchRef: "refs/heads/main", BranchGeneration: 1, Path: "ok.txt", Operation: "create", BaseHead: "abc", Fidelity: "exact"},
		{BranchRef: "", BranchGeneration: 1, Path: "missing.txt", Operation: "create", BaseHead: "abc", Fidelity: "exact"},
	}
	if err := AppendShadowBatch(ctx, d, bad); err == nil {
		t.Fatalf("AppendShadowBatch w/ empty BranchRef: want error")
	}
	if err := d.ReadSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("shadow_paths after validation error = %d, want 0 (rolled back)", n)
	}

	// 3. Happy path: every row is present and primary-key uniqueness held.
	good := []ShadowPath{
		{BranchRef: "refs/heads/main", BranchGeneration: 2, Path: "x.txt", Operation: "create", BaseHead: "abc", Fidelity: "exact",
			Mode: sql.NullString{String: "100644", Valid: true},
			OID:  sql.NullString{String: "0123456789abcdef0123456789abcdef01234567", Valid: true}},
		{BranchRef: "refs/heads/main", BranchGeneration: 2, Path: "y.txt", Operation: "create", BaseHead: "abc", Fidelity: "exact"},
		{BranchRef: "refs/heads/main", BranchGeneration: 2, Path: "z.txt", Operation: "create", BaseHead: "abc", Fidelity: "exact"},
	}
	if err := AppendShadowBatch(ctx, d, good); err != nil {
		t.Fatalf("AppendShadowBatch happy: %v", err)
	}
	if err := d.ReadSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths WHERE branch_generation = 2`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != len(good) {
		t.Fatalf("shadow_paths after happy batch = %d, want %d", n, len(good))
	}

	// 4. Empty batch is a no-op.
	if err := AppendShadowBatch(ctx, d, nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}

	// 5. Re-running the same batch upserts (no PK collision error).
	if err := AppendShadowBatch(ctx, d, good); err != nil {
		t.Fatalf("re-apply batch: %v", err)
	}
}

func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if n != "" && len(s) >= len(n) {
			for i := 0; i+len(n) <= len(s); i++ {
				if s[i:i+len(n)] == n {
					return true
				}
			}
		}
	}
	return false
}

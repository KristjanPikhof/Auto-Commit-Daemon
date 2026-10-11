package state

import (
	"context"

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
	d, _ := openLegacyTestDB(t)
	ctx := context.Background()

	// Seed one pending event so PendingEvents has something to scan.
	if _, err := fixtureAppendCaptureEvent(ctx, d, CaptureEvent{
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

}

// TestSchemaMigrate_AddsFlushRequestsIndex confirms the v4 schema bump
// installs the (status, id) index that keeps ClaimNextFlushRequest off a
// full-table scan after long uptime.
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

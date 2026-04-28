package central

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// openTestPerRepoDB stands up a fresh per-repo state.db in t.TempDir().
func openTestPerRepoDB(t *testing.T) *state.DB {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	db, err := state.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedPerRepoRollup writes a single per-repo daily_rollups row.
func seedPerRepoRollup(t *testing.T, db *state.DB, day, repoPath string, events int64) {
	t.Helper()
	if _, err := state.InsertDailyRollup(context.Background(), db, state.DailyRollup{
		Day:          day,
		RepoRoot:     repoPath,
		EventsTotal:  events,
		CommitsTotal: events,
		FilesChanged: 1,
	}); err != nil {
		t.Fatalf("InsertDailyRollup: %v", err)
	}
}

// TestPushRollupsToCentral_HappyPath: three per-repo rows -> three rows in
// stats.db keyed by repo_hash. A second push is a no-op.
func TestPushRollupsToCentral_HappyPath(t *testing.T) {
	stats := openTestStats(t)
	per := openTestPerRepoDB(t)
	ctx := context.Background()

	repoPath := "/tmp/example-repo"
	repoHash := "h_example1"

	seedPerRepoRollup(t, per, "2026-04-01", repoPath, 5)
	seedPerRepoRollup(t, per, "2026-04-02", repoPath, 7)
	seedPerRepoRollup(t, per, "2026-04-03", repoPath, 3)

	pushed, err := PushRollupsToCentral(ctx, per, stats, repoHash, repoPath)
	if err != nil {
		t.Fatalf("PushRollupsToCentral: %v", err)
	}
	if pushed != 3 {
		t.Fatalf("pushed=%d want 3", pushed)
	}

	rows, err := stats.ListRollupsSince(ctx, 0)
	if err != nil {
		t.Fatalf("ListRollupsSince: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3", len(rows))
	}
	for _, r := range rows {
		if r.RepoHash != repoHash {
			t.Fatalf("row %+v repo_hash mismatch", r)
		}
	}

	// Second push: no new rows.
	pushed2, err := PushRollupsToCentral(ctx, per, stats, repoHash, repoPath)
	if err != nil {
		t.Fatalf("PushRollupsToCentral #2: %v", err)
	}
	if pushed2 != 0 {
		t.Fatalf("second push pushed=%d want 0", pushed2)
	}

	through, present, err := state.MetaGet(ctx, per, metaRollupCentralPushedThrough)
	if err != nil || !present {
		t.Fatalf("pushed_through: present=%v err=%v", present, err)
	}
	if through != "2026-04-03" {
		t.Fatalf("pushed_through=%q want 2026-04-03", through)
	}
}

// TestPushRollupsToCentral_TwoRepos: pushing from two distinct repos
// keeps both keys present and never collides on (day, repo_hash).
func TestPushRollupsToCentral_TwoRepos(t *testing.T) {
	stats := openTestStats(t)
	ctx := context.Background()

	perA := openTestPerRepoDB(t)
	perB := openTestPerRepoDB(t)
	seedPerRepoRollup(t, perA, "2026-04-01", "/repo/a", 10)
	seedPerRepoRollup(t, perA, "2026-04-02", "/repo/a", 20)
	seedPerRepoRollup(t, perB, "2026-04-01", "/repo/b", 30)
	seedPerRepoRollup(t, perB, "2026-04-02", "/repo/b", 40)

	if _, err := PushRollupsToCentral(ctx, perA, stats, "hashA", "/repo/a"); err != nil {
		t.Fatalf("push A: %v", err)
	}
	if _, err := PushRollupsToCentral(ctx, perB, stats, "hashB", "/repo/b"); err != nil {
		t.Fatalf("push B: %v", err)
	}

	rows, err := stats.ListRollupsSince(ctx, 0)
	if err != nil {
		t.Fatalf("ListRollupsSince: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("rows=%d want 4", len(rows))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		key := r.Day + "|" + r.RepoHash
		if seen[key] {
			t.Fatalf("duplicate (day, repo_hash) key %q", key)
		}
		seen[key] = true
	}
}

// TestPushRollupsToCentral_IncrementalCursor: a first push of one day,
// then a fresh per-repo row arrives, second push only inserts the new
// day.
func TestPushRollupsToCentral_IncrementalCursor(t *testing.T) {
	stats := openTestStats(t)
	per := openTestPerRepoDB(t)
	ctx := context.Background()

	seedPerRepoRollup(t, per, "2026-04-01", "/repo", 1)
	if _, err := PushRollupsToCentral(ctx, per, stats, "h", "/repo"); err != nil {
		t.Fatalf("first push: %v", err)
	}
	// Add day 2 after first push completed.
	seedPerRepoRollup(t, per, "2026-04-02", "/repo", 2)
	pushed, err := PushRollupsToCentral(ctx, per, stats, "h", "/repo")
	if err != nil {
		t.Fatalf("second push: %v", err)
	}
	if pushed != 1 {
		t.Fatalf("second push pushed=%d want 1", pushed)
	}
}

// TestPushRollupsToCentral_NilGuard exercises the validation surface so
// callers don't get cryptic SQL errors when they forget required args.
func TestPushRollupsToCentral_NilGuard(t *testing.T) {
	per := openTestPerRepoDB(t)
	stats := openTestStats(t)
	ctx := context.Background()
	if _, err := PushRollupsToCentral(ctx, nil, stats, "h", "/r"); err == nil {
		t.Fatalf("nil per-repo: want error")
	}
	if _, err := PushRollupsToCentral(ctx, per, nil, "h", "/r"); err == nil {
		t.Fatalf("nil stats: want error")
	}
	if _, err := PushRollupsToCentral(ctx, per, stats, "", "/r"); err == nil {
		t.Fatalf("empty hash: want error")
	}
}

package central

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// TestPushRollupsToCentral_IdempotentAfterCrashMidAggregation simulates a
// crash mid-push: a first call succeeds up to day K, then we forcibly
// rewind the cursor + delete central rows past K to mimic a partial-write
// followed by a process death where the cursor never advanced. A second
// call must republish the missing rows exactly once and leave the cursor
// at the latest day with no duplicates anywhere.
//
// This pins the spec's "Cursor advances only past committed rows"
// invariant (§6.3 + §8.10) the hard way, by exercising the recovery path.
func TestPushRollupsToCentral_IdempotentAfterCrashMidAggregation(t *testing.T) {
	stats := openTestStats(t)
	per := openTestPerRepoDB(t)
	ctx := context.Background()
	const repoHash = "h-crashy"
	const repoPath = "/tmp/crashy-repo"

	// Seed 10 days into the per-repo rollups table.
	const N = 10
	for i := 0; i < N; i++ {
		day := fmt.Sprintf("2026-04-%02d", i+1)
		seedPerRepoRollup(t, per, day, repoPath, int64(i+1))
	}

	// First push lands every row.
	pushed, err := PushRollupsToCentral(ctx, per, stats, repoHash, repoPath)
	if err != nil {
		t.Fatalf("initial push: %v", err)
	}
	if pushed != N {
		t.Fatalf("first push pushed=%d want %d", pushed, N)
	}

	// Capture the post-push cursor for sanity.
	through, present, err := state.MetaGet(ctx, per, metaRollupCentralPushedThrough)
	if err != nil || !present {
		t.Fatalf("first cursor: present=%v err=%v", present, err)
	}
	if through != "2026-04-10" {
		t.Fatalf("first cursor=%q want 2026-04-10", through)
	}

	// Simulate a crash mid-aggregation: rewind the cursor to day 5 (so
	// days 6..10 are recoverable from the per-repo table) and physically
	// delete days 7..10 from the central stats.db. This models the case
	// where the per-repo cursor advanced PARTWAY but a subsequent insert
	// failed AND a stats-side row was rolled back.
	//
	// Idempotency contract: re-running PushRollupsToCentral must
	//   (a) re-insert days 7..10 in the central stats.db,
	//   (b) leave days 1..6 untouched (they were already committed), and
	//   (c) advance the cursor back to 2026-04-10.
	if err := state.MetaSet(ctx, per, metaRollupCentralPushedThrough, "2026-04-05"); err != nil {
		t.Fatalf("rewind cursor: %v", err)
	}
	if _, err := stats.SQL().ExecContext(ctx,
		`DELETE FROM daily_rollups WHERE repo_hash = ? AND day > ?`,
		repoHash, "2026-04-06",
	); err != nil {
		t.Fatalf("delete partial rows: %v", err)
	}

	// Rerun.
	pushed2, err := PushRollupsToCentral(ctx, per, stats, repoHash, repoPath)
	if err != nil {
		t.Fatalf("recovery push: %v", err)
	}
	// We expect 4 rows to be re-inserted: days 7, 8, 9, 10. (Days 6 was
	// not deleted, so the INSERT OR IGNORE returns 0 for it — pushed
	// counter only counts new rows.)
	if pushed2 != 4 {
		t.Fatalf("recovery push pushed=%d want 4", pushed2)
	}

	// Final state: 10 rows in central, cursor at last day, no duplicates.
	rows, err := stats.ListRollupsSince(ctx, 0)
	if err != nil {
		t.Fatalf("list rollups: %v", err)
	}
	if len(rows) != N {
		t.Fatalf("rows=%d want %d", len(rows), N)
	}
	seen := make(map[string]int, N)
	for _, r := range rows {
		if r.RepoHash != repoHash {
			t.Fatalf("foreign repo_hash: %+v", r)
		}
		seen[r.Day]++
	}
	for day, count := range seen {
		if count != 1 {
			t.Fatalf("day %s appears %d times", day, count)
		}
	}

	through2, _, err := state.MetaGet(ctx, per, metaRollupCentralPushedThrough)
	if err != nil {
		t.Fatalf("final cursor: %v", err)
	}
	if through2 != "2026-04-10" {
		t.Fatalf("final cursor=%q want 2026-04-10", through2)
	}

	// A third push with no new per-repo rows is a no-op.
	pushed3, err := PushRollupsToCentral(ctx, per, stats, repoHash, repoPath)
	if err != nil {
		t.Fatalf("third push: %v", err)
	}
	if pushed3 != 0 {
		t.Fatalf("third push pushed=%d want 0", pushed3)
	}
}

// TestPushRollupsToCentral_ConcurrentRepos pushes from two repos at the
// same time. INSERT-OR-IGNORE on (day, repo_hash) means the two streams
// never collide, but the cursor + per-repo state must advance independently.
func TestPushRollupsToCentral_ConcurrentRepos(t *testing.T) {
	stats := openTestStats(t)
	ctx := context.Background()

	perA := openTestPerRepoDB(t)
	perB := openTestPerRepoDB(t)
	const days = 5
	for i := 0; i < days; i++ {
		day := fmt.Sprintf("2026-05-%02d", i+1)
		seedPerRepoRollup(t, perA, day, "/repo/a", int64(10+i))
		seedPerRepoRollup(t, perB, day, "/repo/b", int64(20+i))
	}

	errs := make(chan error, 2)
	go func() {
		_, err := PushRollupsToCentral(ctx, perA, stats, "ha", "/repo/a")
		errs <- err
	}()
	go func() {
		_, err := PushRollupsToCentral(ctx, perB, stats, "hb", "/repo/b")
		errs <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent push: %v", err)
		}
	}

	rows, err := stats.ListRollupsSince(ctx, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 2*days {
		t.Fatalf("rows=%d want %d", len(rows), 2*days)
	}
}

// TestGCAgainstSyntheticMissingRepoEntries — build a registry with 10
// entries; half point to non-existent paths. The §7.11 GC predicate
// (gcReason in cmd/cli) is the production source of truth, but this test
// stays in package central by exercising a pure registry-level dropMissing
// helper that mirrors the predicate's "repo dir missing" branch.
//
// First run drops exactly the 5 phantom entries; second run is a no-op.
func TestGCAgainstSyntheticMissingRepoEntries(t *testing.T) {
	roots := rootsForTest(t)
	tmp := t.TempDir()

	// Build 10 entries: even indices live in tmp/repo-i, odd indices
	// point at a path that never exists.
	if err := WithLock(roots, func(reg *Registry) error {
		for i := 0; i < 10; i++ {
			var path string
			if i%2 == 0 {
				path = filepath.Join(tmp, fmt.Sprintf("real-%d", i))
				if err := os.MkdirAll(path, 0o700); err != nil {
					return err
				}
			} else {
				path = filepath.Join(tmp, fmt.Sprintf("phantom-%d", i))
			}
			reg.UpsertRepo(path, fmt.Sprintf("h%02d", i),
				path+"/.git/acd/state.db", "shell", int64(4000+i))
		}
		return nil
	}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	// First GC pass.
	dropped, kept, err := dropMissingRepoEntries(roots)
	if err != nil {
		t.Fatalf("first dropMissing: %v", err)
	}
	if dropped != 5 {
		t.Fatalf("first dropped=%d want 5", dropped)
	}
	if kept != 5 {
		t.Fatalf("first kept=%d want 5", kept)
	}

	// Second GC pass: every survivor still exists, so it must be a no-op.
	dropped2, kept2, err := dropMissingRepoEntries(roots)
	if err != nil {
		t.Fatalf("second dropMissing: %v", err)
	}
	if dropped2 != 0 {
		t.Fatalf("second dropped=%d want 0 (idempotent)", dropped2)
	}
	if kept2 != 5 {
		t.Fatalf("second kept=%d want 5", kept2)
	}

	// Verify each surviving record's path actually exists on disk.
	final, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, rec := range final.Repos {
		if _, err := os.Stat(rec.Path); err != nil {
			t.Fatalf("survivor %q does not exist: %v", rec.Path, err)
		}
	}
}

// dropMissingRepoEntries is a test-side helper that mirrors the production
// `acd gc` "repo-missing" branch but stays in this package so the
// concurrency test does not need a CLI dependency. Returns (dropped,
// kept, err).
//
// The production GC has additional reasons (state-db-missing, daemon-dead-
// 30d) that depend on per-repo state.db inspection. Those branches are
// covered by internal/cli/gc_test.go; this helper isolates the pure
// registry-level pruning so the central package is tested in isolation.
func dropMissingRepoEntries(roots paths.Roots) (int, int, error) {
	var dropped, kept int
	err := WithLock(roots, func(reg *Registry) error {
		survivors := make([]RepoRecord, 0, len(reg.Repos))
		for _, rec := range reg.Repos {
			if _, err := os.Stat(rec.Path); err != nil {
				dropped++
				continue
			}
			survivors = append(survivors, rec)
		}
		reg.Repos = survivors
		kept = len(survivors)
		return nil
	})
	return dropped, kept, err
}

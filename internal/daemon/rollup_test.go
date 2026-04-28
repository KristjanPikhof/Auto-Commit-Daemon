package daemon

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// insertEventOn injects a single capture_events row with both
// captured_ts and published_ts pinned to the given UTC moment so the
// rollup aggregator attributes it deterministically.
func insertEventOn(t *testing.T, db *state.DB, when time.Time, path, evState, commit string) int64 {
	t.Helper()
	ctx := context.Background()
	ev := state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         "deadbeef",
		Operation:        "create",
		Path:             path,
		Fidelity:         "full",
		CapturedTS:       float64(when.Unix()),
		PublishedTS:      sql.NullFloat64{Float64: float64(when.Unix()), Valid: true},
		State:            evState,
	}
	if commit != "" {
		ev.CommitOID = sql.NullString{String: commit, Valid: true}
	}
	seq, err := state.AppendCaptureEvent(ctx, db, ev, []state.CaptureOp{{
		Op: "create", Path: path, Fidelity: "full",
		AfterMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:  sql.NullString{String: "abcd", Valid: true},
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	return seq
}

// rollupHarness sets up a fresh repo + DB and a "now" pinned to a
// post-event UTC date so the aggregator sees a stable yesterday cursor.
type rollupHarness struct {
	db   *state.DB
	repo string
}

func newRollupHarness(t *testing.T) *rollupHarness {
	t.Helper()
	f := newDaemonFixture(t)
	return &rollupHarness{db: f.db, repo: f.dir}
}

// TestRunDailyRollup_ThreeDayFixture covers the happy path: three days of
// events become three rows in daily_rollups; a second call is a no-op.
func TestRunDailyRollup_ThreeDayFixture(t *testing.T) {
	h := newRollupHarness(t)
	ctx := context.Background()

	// Days: 2026-04-01, 2026-04-02, 2026-04-03 — pin events at noon UTC
	// so day attribution can't slide on tz quirks.
	d1 := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	d3 := d2.Add(24 * time.Hour)
	insertEventOn(t, h.db, d1, "a.txt", "published", "c1")
	insertEventOn(t, h.db, d1, "b.txt", "published", "c2")
	insertEventOn(t, h.db, d2, "c.txt", "published", "c3")
	insertEventOn(t, h.db, d2, "c.txt", "failed", "")
	insertEventOn(t, h.db, d3, "d.txt", "published", "c4")

	// "now" = 2026-04-04 00:30 UTC so yesterday = 2026-04-03 (all three
	// days complete).
	now := func() time.Time { return time.Date(2026, 4, 4, 0, 30, 0, 0, time.UTC) }

	n, err := RunDailyRollup(ctx, h.db, RunDailyRollupOpts{RepoPath: h.repo, Now: now})
	if err != nil {
		t.Fatalf("RunDailyRollup: %v", err)
	}
	if n != 3 {
		t.Fatalf("rolled days=%d want 3", n)
	}

	rows, err := state.ListDailyRollupsSince(ctx, h.db, "2026-04-01")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3 (%+v)", len(rows), rows)
	}
	// Sanity-check counters on day 2 (2 events, 1 published, 1 failed).
	for _, r := range rows {
		if r.Day != "2026-04-02" {
			continue
		}
		if r.EventsTotal != 2 {
			t.Fatalf("day2 events=%d want 2", r.EventsTotal)
		}
		if r.CommitsTotal != 1 {
			t.Fatalf("day2 commits=%d want 1", r.CommitsTotal)
		}
		if r.ErrorsTotal != 1 {
			t.Fatalf("day2 errors=%d want 1", r.ErrorsTotal)
		}
		if r.FilesChanged != 1 {
			t.Fatalf("day2 files=%d want 1", r.FilesChanged)
		}
	}

	// Second call must be idempotent.
	n2, err := RunDailyRollup(ctx, h.db, RunDailyRollupOpts{RepoPath: h.repo, Now: now})
	if err != nil {
		t.Fatalf("RunDailyRollup #2: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second pass rolled %d want 0", n2)
	}

	last, present, err := state.MetaGet(ctx, h.db, metaRollupLastDay)
	if err != nil || !present {
		t.Fatalf("MetaGet rollup.last_day: present=%v err=%v", present, err)
	}
	if last != "2026-04-03" {
		t.Fatalf("rollup.last_day=%q want 2026-04-03", last)
	}
}

// TestRunDailyRollup_CrashMidDay simulates the daemon crashing after only
// two of three days are present, then receiving day 3 events later. The
// second call must roll exactly day 3 (not duplicate days 1/2).
func TestRunDailyRollup_CrashMidDay(t *testing.T) {
	h := newRollupHarness(t)
	ctx := context.Background()

	d1 := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	d3 := d2.Add(24 * time.Hour)
	insertEventOn(t, h.db, d1, "a.txt", "published", "c1")
	insertEventOn(t, h.db, d2, "b.txt", "published", "c2")

	// First "now" still inside day 3 — yesterday = day 2. So only days
	// 1+2 land.
	now1 := func() time.Time { return time.Date(2026, 4, 3, 12, 0, 0, 0, time.UTC) }
	n, err := RunDailyRollup(ctx, h.db, RunDailyRollupOpts{RepoPath: h.repo, Now: now1})
	if err != nil {
		t.Fatalf("first roll: %v", err)
	}
	if n != 2 {
		t.Fatalf("first roll n=%d want 2", n)
	}
	last, _, _ := state.MetaGet(ctx, h.db, metaRollupLastDay)
	if last != "2026-04-02" {
		t.Fatalf("after first roll last_day=%q want 2026-04-02", last)
	}

	// Now day 3 events arrive and a later "now" advances past day 3.
	insertEventOn(t, h.db, d3, "c.txt", "published", "c3")
	now2 := func() time.Time { return time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC) }
	n2, err := RunDailyRollup(ctx, h.db, RunDailyRollupOpts{RepoPath: h.repo, Now: now2})
	if err != nil {
		t.Fatalf("second roll: %v", err)
	}
	if n2 != 1 {
		t.Fatalf("second roll n=%d want 1", n2)
	}

	rows, err := state.ListDailyRollupsSince(ctx, h.db, "2026-04-01")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("final rows=%d want 3", len(rows))
	}
}

// TestRunDailyRollup_PreInsertedDayPreserved verifies INSERT OR IGNORE
// semantics: a synthetic pre-existing row for day 1 is *not* overwritten
// by the aggregator; days 2/3 are added beside it.
func TestRunDailyRollup_PreInsertedDayPreserved(t *testing.T) {
	h := newRollupHarness(t)
	ctx := context.Background()

	d1 := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	d3 := d2.Add(24 * time.Hour)

	// Pre-seed a synthetic day-1 row with sentinel counters. The
	// aggregator should see (day=2026-04-01, repo_root=h.repo) already
	// present and leave it alone.
	if _, err := state.InsertDailyRollup(ctx, h.db, state.DailyRollup{
		Day:          "2026-04-01",
		RepoRoot:     h.repo,
		EventsTotal:  999,
		CommitsTotal: 999,
		FilesChanged: 999,
		BytesChanged: 999,
		ErrorsTotal:  999,
		SessionsSeen: 999,
	}); err != nil {
		t.Fatalf("seed day 1: %v", err)
	}

	insertEventOn(t, h.db, d1, "a.txt", "published", "c1")
	insertEventOn(t, h.db, d2, "b.txt", "published", "c2")
	insertEventOn(t, h.db, d3, "c.txt", "published", "c3")

	now := func() time.Time { return time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC) }
	n, err := RunDailyRollup(ctx, h.db, RunDailyRollupOpts{RepoPath: h.repo, Now: now})
	if err != nil {
		t.Fatalf("RunDailyRollup: %v", err)
	}
	// Two new rows (days 2+3); day 1 ignored by INSERT OR IGNORE.
	if n != 2 {
		t.Fatalf("rolled=%d want 2", n)
	}

	rows, err := state.ListDailyRollupsSince(ctx, h.db, "2026-04-01")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3", len(rows))
	}
	for _, r := range rows {
		if r.Day == "2026-04-01" {
			if r.EventsTotal != 999 {
				t.Fatalf("day1 events=%d want 999 (sentinel preserved)", r.EventsTotal)
			}
		}
	}
}

// TestRunDailyRollup_NoEvents is a smoke test: empty capture_events => no
// rows + no error.
func TestRunDailyRollup_NoEvents(t *testing.T) {
	h := newRollupHarness(t)
	ctx := context.Background()
	now := func() time.Time { return time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC) }
	n, err := RunDailyRollup(ctx, h.db, RunDailyRollupOpts{RepoPath: h.repo, Now: now})
	if err != nil {
		t.Fatalf("RunDailyRollup: %v", err)
	}
	if n != 0 {
		t.Fatalf("n=%d want 0", n)
	}
}

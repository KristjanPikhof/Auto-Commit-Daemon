package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
)

func TestStats_ParseSinceUnits(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"24h", 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"30d", 30 * 24 * time.Hour},
		{"1y", 365 * 24 * time.Hour},
		{"90m", 90 * time.Minute},
	}
	for _, c := range cases {
		got, err := parseSince(c.in)
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: got %s, want %s", c.in, got, c.want)
		}
	}
	if _, err := parseSince("7days"); err == nil {
		t.Error("'7days' should be rejected")
	}
}

func TestStats_HumanRendersBars(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	db, err := central.Open(ctx, roots)
	if err != nil {
		t.Fatalf("open stats.db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	now := time.Now()
	day := now.Format("2006-01-02")
	rows := []central.DailyRollup{
		{Day: day, RepoHash: "h1", RepoPath: "/tmp/repo-A",
			EventsTotal: 100, CommitsTotal: 132, FilesChanged: 50,
			BytesChanged: 1024 * 1024, AggregatedAt: float64(now.Unix())},
		{Day: day, RepoHash: "h2", RepoPath: "/tmp/repo-B",
			EventsTotal: 50, CommitsTotal: 78, FilesChanged: 20,
			BytesChanged: 512 * 1024, AggregatedAt: float64(now.Unix())},
	}
	for _, r := range rows {
		if _, err := db.InsertRollup(ctx, r); err != nil {
			t.Fatalf("insert rollup: %v", err)
		}
	}
	_ = db.Close()

	var out bytes.Buffer
	if err := runStats(ctx, &out, "7d", false); err != nil {
		t.Fatalf("runStats: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Period: last 7d",
		"Across all repos:",
		"Commits",
		"Per repo:",
		"/tmp/repo-A",
		"/tmp/repo-B",
		"#",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

func TestStats_JSONShape(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	db, err := central.Open(ctx, roots)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	now := time.Now()
	if _, err := db.InsertRollup(ctx, central.DailyRollup{
		Day: now.Format("2006-01-02"), RepoHash: "h1", RepoPath: "/tmp/repo-A",
		EventsTotal: 5, CommitsTotal: 3, AggregatedAt: float64(now.Unix()),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	_ = db.Close()

	var out bytes.Buffer
	if err := runStats(ctx, &out, "24h", true); err != nil {
		t.Fatalf("runStats json: %v", err)
	}
	var rep statsReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if rep.Since == "" || rep.Until == "" {
		t.Fatal("since/until missing")
	}
	if rep.Totals.Commits != 3 {
		t.Fatalf("totals.commits = %d, want 3", rep.Totals.Commits)
	}
	if len(rep.ByRepo) != 1 || rep.ByRepo[0].RepoPath != "/tmp/repo-A" {
		t.Fatalf("by_repo wrong: %+v", rep.ByRepo)
	}
	if len(rep.ByDay) != 1 {
		t.Fatalf("by_day wrong: %+v", rep.ByDay)
	}
}

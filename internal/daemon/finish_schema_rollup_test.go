package daemon

import (
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishSchemaFreshRollupDoesNotWrite(t *testing.T) {
	ctx := t.Context()
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	legacy, err := state.LegacyTableExists(ctx, db.ReadSQL(), "capture_events")
	if err != nil || legacy {
		t.Fatalf("fresh database has legacy captures: %t, %v", legacy, err)
	}
	var before, after int64
	if err := db.SQL().QueryRowContext(ctx, "SELECT total_changes()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		written, err := RunDailyRollup(ctx, db, RunDailyRollupOpts{RepoPath: t.TempDir()})
		if err != nil || written != 0 {
			t.Fatalf("fresh rollup wrote %d rows: %v", written, err)
		}
	}
	if err := db.SQL().QueryRowContext(ctx, "SELECT total_changes()").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("fresh rollup mutated state: changes %d -> %d", before, after)
	}
}

package state

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenPreservesURICharactersInDatabasePath(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"repo?name", "repo#name", "repo%23name", "relative?name"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name, "state.db")
			if name == "relative?name" {
				cwd, err := os.Getwd()
				if err != nil {
					t.Fatal(err)
				}
				path, err = filepath.Rel(cwd, path)
				if err != nil {
					t.Fatal(err)
				}
			}
			db, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("database not created at requested path: %v", err)
			}
			fixture := finishFixture(t, db, 1)
			if err := MetaSet(ctx, db, "path-sentinel", name); err != nil {
				t.Fatal(err)
			}
			if version, err := ReadUserVersion(ctx, path); err != nil || version != SchemaVersion {
				t.Fatalf("version=%d err=%v", version, err)
			}
			readonly, err := OpenReadOnly(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			got, ok, err := MetaGet(ctx, readonly, "path-sentinel")
			readonly.Close()
			if err != nil || !ok || got != name {
				t.Fatalf("readonly sentinel=%q ok=%t err=%v", got, ok, err)
			}
			if _, _, err := LoadDaemonStateReadOnly(ctx, path); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadUnresolvedCapturePairs(ctx, path); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadLegacyWorkSummary(ctx, path); err != nil {
				t.Fatal(err)
			}
			if projection, err := ReadCheckpointProjection(ctx, path, 10); err != nil || projection.Completed != 1 {
				t.Fatalf("projection=%+v err=%v", projection, err)
			}
			if checkpoint, err := ResolveCheckpoint(ctx, path, fixture.Target.CheckpointID); err != nil || checkpoint.ID != fixture.Target.CheckpointID {
				t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
			}
			backup := filepath.Join(filepath.Dir(path), "backup?#%.db")
			if err := BackupDatabase(ctx, path, backup); err != nil {
				t.Fatal(err)
			}
			if version, err := ReadUserVersion(ctx, backup); err != nil || version != SchemaVersion {
				t.Fatalf("backup version=%d err=%v", version, err)
			}
		})
	}
}

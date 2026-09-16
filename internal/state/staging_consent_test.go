package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicationDrainIndexConsentMigrationAndReadOnlyCompatibility(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	cp := seedPublicationDrainCheckpoint(t, db, []string{"approved.txt"})
	drain := PublicationDrain{ID: "approved", CheckpointID: cp.ID,
		WorktreeID: cp.WorktreeID, BranchRef: cp.ObservedRef, BranchGeneration: 7,
		Phase: PublicationDrainCheckpointing, TargetEventCount: 1,
		StagedConsent: true, ExpectedIndexDigest: strings.Repeat("a", 64),
		CreatedTS: 10, UpdatedTS: 10, LastProgressTS: 10}
	if _, err := PreparePublicationDrain(ctx, db, drain); err != nil {
		t.Fatal(err)
	}
	changed := drain
	changed.ExpectedIndexDigest = strings.Repeat("b", 64)
	if _, err := PreparePublicationDrain(ctx, db, changed); !errors.Is(err, ErrPublicationDrainIdentity) {
		t.Fatalf("changed consent accepted: %v", err)
	}
	// Recreate the previous schema with populated checkpoint and drain rows.
	for _, statement := range []string{"ALTER TABLE publication_drains DROP COLUMN expected_index_digest", "PRAGMA user_version=26"} {
		if _, err := db.SQL().ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := ReadPublicationDrainProjection(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || projection.SchemaVersion != 26 || projection.Latest == nil || projection.Latest.ExpectedIndexDigest != "" {
		t.Fatalf("read-only projection changed old state: %+v", projection)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	version, err := db.UserVersion(ctx)
	if err != nil || version != SchemaVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
	loaded, err := PublicationDrainByID(ctx, db, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.StagedConsent || loaded.StagedConsumed || loaded.ExpectedIndexDigest != "" || loaded.CheckpointID != cp.ID || len(loaded.EventSeqs) != 1 {
		t.Fatalf("migration invented consent or lost protected membership: %+v", loaded)
	}
}

package daemon

import (
	"context"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestExternalRepairMappingReaderSupportsLegacyWithoutMigration(t *testing.T) {
	t.Parallel()
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	result, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, f.plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.db.SQL().ExecContext(ctx, `DROP VIEW intent_repair_commit_mappings; PRAGMA user_version=29`); err != nil {
		t.Fatal(err)
	}
	reader, err := state.OpenReadOnly(ctx, f.repo.db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	evidence := newExternalRepairEvidence(reader, f.repo.dir, f.cctx.BranchRef, f.cctx.BranchGeneration)
	mappings, err := evidence.commitMappingsFrom(ctx, f.plan.ExpectedHead)
	if err != nil || len(mappings) != 1 || mappings[0].newOID != result.NewHead {
		t.Fatalf("legacy mappings=%+v err=%v", mappings, err)
	}
	if _, err := evidence.loadRepair(ctx, f.plan.ID); err != nil {
		t.Fatalf("legacy reader proof failed: %v", err)
	}
	version, err := state.ReadUserVersion(ctx, f.repo.db.Path())
	if err != nil || version != 29 {
		t.Fatalf("read-only recovery migrated schema: version=%d err=%v", version, err)
	}
}

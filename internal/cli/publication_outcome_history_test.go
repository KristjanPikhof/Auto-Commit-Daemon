package cli

import (
	"context"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func TestFinishStatusHistoryBoundsLargeArchiveProjection(t *testing.T) {
	ctx := context.Background()
	repo, dbPath, db := makeRegisteredGitRepoStateDB(t)
	if err := testutil.InitLegacy(ctx, db); err != nil {
		t.Fatal(err)
	}
	// Large legacy archives must remain bounded, read-only, and retain exact
	// checkpoint membership and recovery meaning in the current history route.
	const historySize = 4096
	for _, query := range []string{
		`WITH RECURSIVE numbers(n) AS (
    VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<4096
)
INSERT INTO operations(id,kind,worktree_id,phase,status,created_ts,updated_ts)
SELECT 'op-'||n,'checkpoint','0123456789abcdef','completed','completed',n,n
FROM numbers`,
		`INSERT INTO checkpoints(
    id,seq,operation_id,worktree_id,reason,observation_epoch,coverage_epoch,
    tree_oid,commit_oid,checkpoint_ref,phase,created_ts,completed_ts
)
SELECT 'cp-'||created_ts,created_ts,id,worktree_id,'poll',created_ts,created_ts,
       'tree','commit','refs/acd/checkpoints/'||created_ts,
       CASE WHEN created_ts%4=0 THEN 'prepared' ELSE 'completed' END,
       created_ts,CASE WHEN created_ts%4=0 THEN NULL ELSE created_ts END
FROM operations`,
		`INSERT INTO capture_events(
    seq,branch_ref,branch_generation,base_head,operation,path,fidelity,
    captured_ts,state,commit_oid
)
SELECT seq,
       CASE WHEN seq%16 IN (1,3) THEN 'refs/heads/other' ELSE 'refs/heads/main' END,
       CASE WHEN seq%16 IN (5,7) THEN 8 ELSE 7 END,
       'head','modify','file-'||seq,'exact',seq,
       CASE seq%4 WHEN 1 THEN 'published' WHEN 2 THEN 'recovered' ELSE 'pending' END,
       CASE WHEN seq%4=1 THEN 'published' ELSE NULL END
FROM checkpoints`,
		`INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq)
SELECT id,0,seq FROM checkpoints`,
		`INSERT INTO capture_events(
    branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state,commit_oid
) VALUES('refs/heads/main',7,'head','modify','uncheckpointed','exact',5000,'published','orphan')`,
	} {
		if _, err := db.SQL().ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := fileDigest(t, dbPath)
	entries, err := loadCheckpointHistory(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 100 || entries[0].Sequence != historySize || entries[99].Sequence != historySize-99 {
		t.Fatalf("unbounded or unordered history: %d entries", len(entries))
	}
	for _, entry := range entries {
		if entry.EventCount != 1 {
			t.Fatalf("lost archived membership: %+v", entry)
		}
		if entry.Sequence%4 == 2 && (entry.Outcome != "recovered" || entry.RecoveredEvents != 1 || entry.PublicationOutcome.BranchCommitted == nil || *entry.PublicationOutcome.BranchCommitted) {
			t.Fatalf("recovery became branch commitment: %+v", entry)
		}
	}
	if fileDigest(t, dbPath) != before {
		t.Fatal("publication projection wrote or migrated state")
	}
}

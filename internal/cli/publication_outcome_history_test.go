package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestPublicationOutcomeLargeHistoryBuildsCompletedMembershipOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// The real dashboard hit a quadratic plan with thousands of checkpoints
	// and captures. Include unrelated branches/generations, unfinished
	// checkpoints, and an uncheckpointed capture so the cheaper plan must
	// preserve the publication proof rather than just count history rows.
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
	if err := state.MetaSet(ctx, db, daemon.MetaKeyProtectionClassificationPending, "true"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := fileDigest(t, dbPath)
	readOnly, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = readOnly.Close() })
	// Assert the engine's actual work shape, not an elapsed-time threshold.
	// A correlated subquery was what repeatedly scanned completed history
	// until every dashboard read exhausted its fixed request budget.
	rows, err := readOnly.ReadSQL().QueryContext(ctx,
		"EXPLAIN QUERY PLAN "+publicationOutcomeSelectSQL,
		"refs/heads/main", 7, "refs/heads/main", 7)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if strings.Contains(strings.ToUpper(detail), "CORRELATED") {
			_ = rows.Close()
			t.Fatalf("publication projection repeats work for every capture: %s", detail)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	outcome, err := readPublicationOutcomeForPair(ctx, readOnly.ReadSQL(), false,
		"", "refs/heads/main", 7, false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.BranchChanges != historySize/8 || outcome.RecoveredChanges != historySize/4 ||
		outcome.WaitingChanges != historySize/8 || !outcome.PendingClassification || outcome.BranchCommitted != nil {
		t.Fatalf("completed membership lost exact branch/protection semantics: %+v", outcome)
	}
	if fileDigest(t, dbPath) != before {
		t.Fatal("publication projection wrote or migrated state")
	}
}

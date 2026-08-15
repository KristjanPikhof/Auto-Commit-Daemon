//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const publicationSelfHealingDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestPublicationSelfHealingStagedRestartAndFrozenTarget(t *testing.T) {
	requireSQLite(t)
	ctx := context.Background()
	repo := tempRepo(t)
	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	tracked := filepath.Join(repo, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGitOK(t, repo, "add", "tracked.txt")
	if err := os.WriteFile(tracked, []byte("staged and unstaged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	untracked := filepath.Join(repo, "untracked.txt")
	if err := os.WriteFile(untracked, []byte("untracked\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	wantTracked, _ := os.ReadFile(tracked)
	wantUntracked, _ := os.ReadFile(untracked)

	dbPath := state.DBPathFromGitDir(filepath.Join(repo, ".git"))
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var targetSeqs []int64
	for i, path := range []string{"tracked.txt", "untracked.txt"} {
		result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,?,'modify',?,'exact',?,'pending')`,
			head, path, i+1)
		if err != nil {
			t.Fatal(err)
		}
		seq, _ := result.LastInsertId()
		targetSeqs = append(targetSeqs, seq)
	}
	checkpoint := state.Checkpoint{
		ID:               "cp-1786488000000-0123456789abcdef",
		OperationID:      "op-publication-self-healing",
		WorktreeID:       "0123456789abcdef",
		Reason:           state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: head,
		ObservedRef: "refs/heads/main", TreeOID: "tree", CommitOID: "commit",
		Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786488000000-0123456789abcdef",
		CreatedTS: 1, EventSeqs: targetSeqs,
	}
	if created, err := state.PrepareCheckpoint(
		ctx, db, checkpoint, publicationSelfHealingDigest); err != nil || !created {
		t.Fatalf("prepare checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	drain := state.PublicationDrain{
		ID: "drain-publication-self-healing", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: state.PublicationDrainCheckpointing,
		TargetEventCount: 2, StagedConsent: true,
		CreatedTS: 3, UpdatedTS: 3, LastProgressTS: 3,
		EventSeqs: targetSeqs,
	}
	if created, err := state.PreparePublicationDrain(ctx, db, drain); err != nil || !created {
		t.Fatalf("prepare drain=(%t,%v)", created, err)
	}
	resumed, err := daemon.ResumePublicationDrainCheckpointing(
		ctx, repo, db, drain, time.Unix(4, 0).UTC())
	if err != nil || resumed.Phase != state.PublicationDrainSemantic ||
		!resumed.StagedConsumed {
		t.Fatalf("resume=(%+v,%v)", resumed, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo},
		"diff", "--cached", "--quiet"); err != nil {
		t.Fatalf("index not reset: %v", err)
	}
	if got, _ := os.ReadFile(tracked); string(got) != string(wantTracked) {
		t.Fatalf("tracked worktree changed: %q", got)
	}
	if got, _ := os.ReadFile(untracked); string(got) != string(wantUntracked) {
		t.Fatalf("untracked worktree changed: %q", got)
	}

	later, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,?,'modify','later.txt','exact',5,'pending')`, head)
	if err != nil {
		t.Fatal(err)
	}
	laterSeq, _ := later.LastInsertId()
	normalized, err := daemon.UpdatePublicationDrainAfterReplay(
		ctx, db, resumed, daemon.ReplaySummary{}, nil, time.Unix(5, 0).UTC())
	if err != nil || normalized.Phase != state.PublicationDrainNormalizing {
		t.Fatalf("normalize=(%+v,%v)", normalized, err)
	}
	fallback, err := daemon.ResumePublicationDrainNormalization(
		ctx, db, normalized, time.Unix(6, 0).UTC())
	if err != nil || fallback.Phase != state.PublicationDrainEventFallback ||
		fallback.EventFallbackCount != 0 || fallback.FallbackMode != "semantic_replan" {
		t.Fatalf("fallback=(%+v,%v)", fallback, err)
	}
	unlock, err := daemon.UpdatePublicationDrainAfterReplay(
		ctx, db, fallback, daemon.ReplaySummary{
			Disposition: daemon.ReplayDispositionRecoverableStall,
		}, nil, time.Unix(7, 0).UTC())
	if err != nil || unlock.FallbackMode != "local_unlock" ||
		unlock.EventFallbackCount != 1 {
		t.Fatalf("unlock=(%+v,%v)", unlock, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='commit-1',published_ts=8
WHERE seq=?`, targetSeqs[0]); err != nil {
		t.Fatal(err)
	}
	replanned, err := daemon.UpdatePublicationDrainAfterReplay(
		ctx, db, unlock, daemon.ReplaySummary{
			Published: 1, RecoveryMode: "local_unlock",
		}, nil, time.Unix(8, 0).UTC())
	if err != nil || replanned.FallbackMode != "semantic_replan" ||
		replanned.PublishedEventCount != 1 {
		t.Fatalf("replanned=(%+v,%v)", replanned, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='commit-2',published_ts=9
WHERE seq=?`, targetSeqs[1]); err != nil {
		t.Fatal(err)
	}
	completed, err := daemon.UpdatePublicationDrainAfterReplay(
		ctx, db, replanned, daemon.ReplaySummary{Published: 1}, nil,
		time.Unix(9, 0).UTC())
	if err != nil || completed.Phase != state.PublicationDrainCompleted ||
		completed.PublishedEventCount != 2 {
		t.Fatalf("completed=(%+v,%v)", completed, err)
	}
	var laterState string
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, laterSeq).Scan(&laterState); err != nil || laterState != state.EventStatePending {
		t.Fatalf("later event=(%s,%v)", laterState, err)
	}
}

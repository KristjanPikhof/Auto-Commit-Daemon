package state

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

const completedBranchTransitionTestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestCompletedBranchTransitionChainRepairOnly(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()
	const (
		branch     = "refs/heads/main"
		generation = int64(7)
		source     = "old-head"
		target     = "rewritten-head"
	)

	completeIntentRepairTransition(
		t, db, "repair-only", branch, generation, source, target, 1)

	chain, ok, err := CompletedBranchTransitionChain(
		ctx, db, branch, generation, source, target)
	if err != nil || !ok {
		t.Fatalf("CompletedBranchTransitionChain=(%+v,%t,%v)", chain, ok, err)
	}
	if len(chain) != 1 ||
		chain[0].Kind != CompletedBranchTransitionIntentRepair ||
		chain[0].ID != "repair-only" ||
		chain[0].SourceHead != source ||
		chain[0].TargetHead != target ||
		len(chain[0].EventSeqs) != 0 ||
		len(chain[0].CommitMappings) != 1 ||
		chain[0].CommitMappings[0].OldOID != source ||
		!chain[0].CommitMappings[0].NewOID.Valid ||
		chain[0].CommitMappings[0].NewOID.String != target {
		t.Fatalf("repair chain=%+v", chain)
	}
}

func TestCompletedBranchTransitionChainAcceptsNoncontiguousRepartition(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()
	const (
		branch     = "refs/heads/main"
		generation = int64(11)
		source     = "old-a2"
		target     = "new-b"
		repairID   = "repair-repartitioned"
	)
	repair := IntentRepair{
		ID: repairID, BranchRef: branch, BranchGeneration: generation,
		Status: IntentRepairPrepared, ExpectedHead: source,
		PlanDigest: completedBranchTransitionTestDigest,
		OldHead:    sql.NullString{String: source, Valid: true},
		Commits: []IntentRepairCommit{
			{CandidateID: sql.NullString{String: "candidate-a", Valid: true}, OldOID: "old-a1"},
			{CandidateID: sql.NullString{String: "candidate-a", Valid: true}, OldOID: source},
			{CandidateID: sql.NullString{String: "candidate-b", Valid: true}, OldOID: "old-b1"},
		},
	}
	if err := SaveIntentRepair(ctx, db, repair); err != nil {
		t.Fatal(err)
	}
	mappings := []IntentRepairCommit{
		{CandidateID: sql.NullString{String: "candidate-a", Valid: true}, OldOID: "old-a1", NewOID: sql.NullString{String: "new-a", Valid: true}},
		{CandidateID: sql.NullString{String: "candidate-a", Valid: true}, OldOID: source, NewOID: sql.NullString{String: "new-a", Valid: true}},
		{CandidateID: sql.NullString{String: "candidate-b", Valid: true}, OldOID: "old-b1", NewOID: sql.NullString{String: target, Valid: true}},
	}
	applied, err := TransitionIntentRepair(ctx, db, repairID, IntentRepairTransition{
		ExpectedStatus: IntentRepairPrepared,
		Status:         IntentRepairGitApplied,
		BackupRef: sql.NullString{
			String: "refs/acd/intent-repair/fixture/repartitioned/backup", Valid: true,
		},
		OldHead:      sql.NullString{String: source, Valid: true},
		NewHead:      sql.NullString{String: target, Valid: true},
		Commits:      mappings,
		TransitionTS: 2,
	})
	if err != nil || !applied {
		t.Fatalf("mark Git-applied=(%t, %v)", applied, err)
	}
	completed, err := TransitionIntentRepair(ctx, db, repairID, IntentRepairTransition{
		ExpectedStatus: IntentRepairGitApplied,
		Status:         IntentRepairCompleted,
		TransitionTS:   3,
	})
	if err != nil || !completed {
		t.Fatalf("complete repair=(%t, %v)", completed, err)
	}

	chain, ok, err := CompletedBranchTransitionChain(
		ctx, db, branch, generation, source, target)
	if err != nil || !ok || len(chain) != 1 {
		t.Fatalf("repartitioned chain=(%+v, %t, %v)", chain, ok, err)
	}
}

func TestCompletedBranchTransitionChainMixedSelfPublicationAndRepair(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()
	const (
		branch     = "refs/heads/main"
		generation = int64(8)
		source     = "source-head"
		middle     = "published-head"
		target     = "repaired-head"
	)

	eventSeq := completeSelfPublicationTransition(
		t, db, "publication-mixed", branch, generation, source, middle, 1)
	completeIntentRepairTransition(
		t, db, "repair-mixed", branch, generation, middle, target, 4)

	chain, ok, err := CompletedBranchTransitionChain(
		ctx, db, branch, generation, source, target)
	if err != nil || !ok {
		t.Fatalf("CompletedBranchTransitionChain=(%+v,%t,%v)", chain, ok, err)
	}
	if len(chain) != 2 {
		t.Fatalf("mixed chain length=%d want 2: %+v", len(chain), chain)
	}
	if chain[0].Kind != CompletedBranchTransitionSelfPublication ||
		chain[0].ID != "publication-mixed" ||
		chain[0].SourceHead != source || chain[0].TargetHead != middle ||
		len(chain[0].EventSeqs) != 1 || chain[0].EventSeqs[0] != eventSeq ||
		len(chain[0].CommitMappings) != 0 {
		t.Fatalf("self-publication transition=%+v", chain[0])
	}
	if chain[1].Kind != CompletedBranchTransitionIntentRepair ||
		chain[1].ID != "repair-mixed" ||
		chain[1].SourceHead != middle || chain[1].TargetHead != target ||
		len(chain[1].EventSeqs) != 0 ||
		len(chain[1].CommitMappings) != 1 ||
		chain[1].CommitMappings[0].OldOID != middle ||
		!chain[1].CommitMappings[0].NewOID.Valid ||
		chain[1].CommitMappings[0].NewOID.String != target {
		t.Fatalf("intent-repair transition=%+v", chain[1])
	}
}

func TestCompletedBranchTransitionChainMissingExternal(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()

	chain, ok, err := CompletedBranchTransitionChain(
		ctx, db, "refs/heads/main", 9, "known-head", "external-head")
	if err != nil || ok || chain != nil {
		t.Fatalf("missing external chain=(%+v,%t,%v), want (nil,false,nil)",
			chain, ok, err)
	}
}

func TestCompletedBranchTransitionChainRejectsAmbiguousOutgoing(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()
	const (
		branch     = "refs/heads/main"
		generation = int64(10)
		source     = "shared-source"
	)

	completeSelfPublicationTransition(
		t, db, "publication-fork", branch, generation,
		source, "publication-target", 1)
	completeIntentRepairTransition(
		t, db, "repair-fork", branch, generation,
		source, "repair-target", 4)

	chain, ok, err := CompletedBranchTransitionChain(
		ctx, db, branch, generation, source, "repair-target")
	if err == nil || !strings.Contains(err.Error(), "ambiguous") || ok || chain != nil {
		t.Fatalf("ambiguous chain=(%+v,%t,%v)", chain, ok, err)
	}
}

func TestCompletedBranchTransitionOwnsCheckpointTargetUsesRepairMembers(t *testing.T) {
	for _, test := range []struct {
		name          string
		legacy        bool
		checkpointTS  float64
		includeMember bool
		want          bool
	}{
		{name: "post-checkpoint member included", checkpointTS: 1, includeMember: true, want: true},
		{name: "post-checkpoint member outside target", checkpointTS: 1},
		{name: "post-checkpoint legacy repair", legacy: true, checkpointTS: 1},
		{name: "pre-checkpoint legacy repair", legacy: true, checkpointTS: 10, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, _ := openTestDB(t)
			ctx := context.Background()
			const (
				branch     = "refs/heads/main"
				generation = int64(12)
				source     = "checkpoint-source"
				target     = "checkpoint-target"
			)
			var memberSeq int64
			if test.legacy {
				completeIntentRepairTransition(
					t, db, "checkpoint-legacy", branch, generation,
					source, target, 2)
			} else {
				memberSeq = completeIntentRepairTransitionWithPendingMember(
					t, db, "checkpoint-members", branch, generation,
					source, target, 2)
			}
			var targetSeqs []int64
			if test.includeMember {
				targetSeqs = []int64{memberSeq}
			}
			owned, err := CompletedBranchTransitionOwnsCheckpointTarget(
				ctx, db, branch, generation, source, target,
				test.checkpointTS, targetSeqs)
			if err != nil || owned != test.want {
				t.Fatalf("owned=(%t, %v) want (%t, nil)", owned, err, test.want)
			}
		})
	}
}

func completeIntentRepairTransitionWithPendingMember(
	t *testing.T,
	db *DB,
	id string,
	branch string,
	generation int64,
	source string,
	target string,
	createdTS float64,
) int64 {
	t.Helper()
	ctx := context.Background()
	seq, err := AppendCaptureEvent(ctx, db, CaptureEvent{
		BranchRef: branch, BranchGeneration: generation,
		BaseHead: source, Operation: "create", Path: id + ".go",
		Fidelity: "full", State: EventStatePending,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidateID := "candidate-" + id
	if err := SaveIntentCandidate(ctx, db, IntentCandidate{
		ID: candidateID, BranchRef: branch, BranchGeneration: generation,
		Status: IntentCandidateReady, Readiness: IntentReadinessReady,
		Events: []IntentCandidateEvent{{EventSeq: seq, EventRole: "code"}},
	}); err != nil {
		t.Fatal(err)
	}
	members, err := SnapshotIntentRepairMembers(
		ctx, db, id, branch, generation, []string{candidateID})
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveIntentRepair(ctx, db, IntentRepair{
		ID: id, BranchRef: branch, BranchGeneration: generation,
		Status: IntentRepairPrepared, ExpectedHead: source,
		PlanDigest: completedBranchTransitionTestDigest,
		OldHead:    sql.NullString{String: source, Valid: true},
		CreatedTS:  createdTS, UpdatedTS: createdTS,
		Commits: []IntentRepairCommit{{
			CandidateID: sql.NullString{String: candidateID, Valid: true},
			OldOID:      source,
		}},
		Members: members,
	}); err != nil {
		t.Fatal(err)
	}
	mapping := []IntentRepairCommit{{
		CandidateID: sql.NullString{String: candidateID, Valid: true},
		OldOID:      source,
		NewOID:      sql.NullString{String: target, Valid: true},
	}}
	applied, err := TransitionIntentRepair(ctx, db, id, IntentRepairTransition{
		ExpectedStatus: IntentRepairPrepared,
		Status:         IntentRepairGitApplied,
		BackupRef: sql.NullString{
			String: "refs/acd/intent-repair/fixture/" + id + "/backup",
			Valid:  true,
		},
		OldHead:      sql.NullString{String: source, Valid: true},
		NewHead:      sql.NullString{String: target, Valid: true},
		Commits:      mapping,
		TransitionTS: createdTS + 1,
	})
	if err != nil || !applied {
		t.Fatalf("mark Git-applied=(%t, %v)", applied, err)
	}
	completed, err := TransitionIntentRepair(ctx, db, id, IntentRepairTransition{
		ExpectedStatus: IntentRepairGitApplied,
		Status:         IntentRepairCompleted,
		TransitionTS:   createdTS + 2,
	})
	if err != nil || !completed {
		t.Fatalf("complete repair=(%t, %v)", completed, err)
	}
	return seq
}

func completeIntentRepairTransition(
	t *testing.T,
	db *DB,
	id string,
	branch string,
	generation int64,
	source string,
	target string,
	createdTS float64,
) {
	t.Helper()
	ctx := context.Background()
	repair := IntentRepair{
		ID: id, BranchRef: branch, BranchGeneration: generation,
		Status: IntentRepairPrepared, ExpectedHead: source,
		PlanDigest: completedBranchTransitionTestDigest,
		OldHead:    sql.NullString{String: source, Valid: true},
		CreatedTS:  createdTS, UpdatedTS: createdTS,
		Commits: []IntentRepairCommit{{
			CandidateID: sql.NullString{String: "candidate-" + id, Valid: true},
			OldOID:      source,
		}},
	}
	if err := SaveIntentRepair(ctx, db, repair); err != nil {
		t.Fatalf("SaveIntentRepair %s: %v", id, err)
	}
	mapping := []IntentRepairCommit{{
		CandidateID: sql.NullString{String: "candidate-" + id, Valid: true},
		OldOID:      source,
		NewOID:      sql.NullString{String: target, Valid: true},
	}}
	applied, err := TransitionIntentRepair(ctx, db, id, IntentRepairTransition{
		ExpectedStatus: IntentRepairPrepared,
		Status:         IntentRepairGitApplied,
		BackupRef: sql.NullString{
			String: "refs/acd/intent-repair/fixture/" + id + "/backup",
			Valid:  true,
		},
		OldHead:      sql.NullString{String: source, Valid: true},
		NewHead:      sql.NullString{String: target, Valid: true},
		Commits:      mapping,
		TransitionTS: createdTS + 1,
	})
	if err != nil || !applied {
		t.Fatalf("TransitionIntentRepair git_applied %s=(%t,%v)",
			id, applied, err)
	}
	completed, err := TransitionIntentRepair(ctx, db, id, IntentRepairTransition{
		ExpectedStatus: IntentRepairGitApplied,
		Status:         IntentRepairCompleted,
		TransitionTS:   createdTS + 2,
	})
	if err != nil || !completed {
		t.Fatalf("TransitionIntentRepair completed %s=(%t,%v)",
			id, completed, err)
	}
	stored, ok, err := IntentRepairByID(ctx, db, id)
	if err != nil || !ok || stored.Status != IntentRepairCompleted ||
		len(stored.Commits) != 1 || !stored.Commits[0].NewOID.Valid ||
		stored.Commits[0].NewOID.String != target {
		t.Fatalf("completed repair %s=(%+v,%t,%v)", id, stored, ok, err)
	}
}

func completeSelfPublicationTransition(
	t *testing.T,
	db *DB,
	id string,
	branch string,
	generation int64,
	source string,
	target string,
	createdTS float64,
) int64 {
	t.Helper()
	ctx := context.Background()
	seq := appendSelfPublicationEvent(t, db, branch, generation, id+".go")
	publication := SelfPublication{
		ID: id, BranchRef: branch, BranchGeneration: generation,
		SourceHead: source, TargetCommitOID: target,
		TargetTreeOID: "tree-" + id, CreatedTS: createdTS,
		Completion: SelfPublicationCompletion{
			PublishedTS: createdTS + 2,
		},
		Members: []SelfPublicationMember{{EventSeq: seq}},
	}
	created, err := PrepareSelfPublication(ctx, db, publication)
	if err != nil || !created {
		t.Fatalf("PrepareSelfPublication %s=(%t,%v)", id, created, err)
	}
	applied, err := MarkSelfPublicationGitApplied(
		ctx, db, publication, createdTS+1)
	if err != nil || !applied {
		t.Fatalf("MarkSelfPublicationGitApplied %s=(%t,%v)",
			id, applied, err)
	}
	completed, err := CompleteSelfPublication(
		ctx, db, publication, publication.Completion)
	if err != nil || !completed {
		t.Fatalf("CompleteSelfPublication %s=(%t,%v)", id, completed, err)
	}
	return seq
}

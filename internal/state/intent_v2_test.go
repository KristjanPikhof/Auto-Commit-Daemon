package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const testIntentRepairPlanDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestIntentV2CandidateRoundTripAndReassignment(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	first := appendIntentV2Event(t, d, "refs/heads/main", 3, "a.go")
	second := appendIntentV2Event(t, d, "refs/heads/main", 3, "a_test.go")

	candidate := IntentCandidate{
		ID: "candidate-a", BranchRef: "refs/heads/main", BranchGeneration: 3,
		Status: IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Purpose: "add state behavior", MissingCompanions: "matching test",
		AtomicityStatus:  sql.NullString{String: "valid", Valid: true},
		AtomicitySummary: "one source and test component",
		PresetID:         sql.NullString{String: "intent.balanced", Valid: true},
		PresetVersion:    sql.NullInt64{Int64: 2, Valid: true},
		VerificationStatus: sql.NullString{
			String: "passed", Valid: true,
		},
		VerificationOutput: strings.Repeat("x", IntentVerificationOutputMaxBytes+100),
		Events: []IntentCandidateEvent{
			{EventSeq: first, EventRole: "implementation"},
			{EventSeq: second, EventRole: "test"},
		},
	}
	if err := SaveIntentCandidate(ctx, d, candidate); err != nil {
		t.Fatalf("SaveIntentCandidate: %v", err)
	}
	got, ok, err := IntentCandidateByID(ctx, d, candidate.ID)
	if err != nil || !ok {
		t.Fatalf("IntentCandidateByID: ok=%v err=%v", ok, err)
	}
	if got.Purpose != candidate.Purpose || len(got.Events) != 2 ||
		got.Events[0].EventSeq != first || got.Events[1].EventSeq != second {
		t.Fatalf("candidate round trip = %+v", got)
	}
	if len(got.VerificationOutput) != IntentVerificationOutputMaxBytes {
		t.Fatalf("verification output bytes=%d want=%d",
			len(got.VerificationOutput), IntentVerificationOutputMaxBytes)
	}

	other := IntentCandidate{
		ID: "candidate-b", BranchRef: "refs/heads/main", BranchGeneration: 3,
		Status: IntentCandidateReady, Readiness: IntentReadinessReady,
		Purpose: "separate test change",
		Events:  []IntentCandidateEvent{{EventSeq: second, EventRole: "test"}},
	}
	if err := SaveIntentCandidate(ctx, d, other); err != nil {
		t.Fatalf("save reassigned candidate: %v", err)
	}
	got, _, err = IntentCandidateByID(ctx, d, candidate.ID)
	if err != nil || len(got.Events) != 1 || got.Events[0].EventSeq != first {
		t.Fatalf("active events after reassignment=%+v err=%v", got.Events, err)
	}
	var state string
	if err := d.SQL().QueryRowContext(ctx, `
SELECT membership_state FROM intent_candidate_events
WHERE candidate_id=? AND event_seq=?`, candidate.ID, second).Scan(&state); err != nil {
		t.Fatalf("load superseded membership: %v", err)
	}
	if state != IntentMembershipSuperseded {
		t.Fatalf("membership state=%q want superseded", state)
	}
}

func TestMarkIntentCandidateVerificationPendingPreservesSemanticState(
	t *testing.T,
) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendIntentV2Event(
		t, d, "refs/heads/main", 3, "resource_wait.go")
	candidate := IntentCandidate{
		ID: "candidate-resource-wait", BranchRef: "refs/heads/main",
		BranchGeneration: 3, Status: IntentCandidateWaiting,
		Readiness: IntentReadinessWait, Purpose: "preserve semantic grouping",
		AtomicityStatus:  sql.NullString{String: "failed", Valid: true},
		AtomicitySummary: "verification workspace setup failed",
		VerificationStatus: sql.NullString{
			String: "needs_attention", Valid: true,
		},
		VerificationOutput: "No space left on device",
		VerificationTS:     sql.NullFloat64{Float64: 9, Valid: true},
		Events: []IntentCandidateEvent{{
			EventSeq: seq, EventRole: "implementation",
		}},
	}
	if err := SaveIntentCandidate(ctx, d, candidate); err != nil {
		t.Fatal(err)
	}
	if changed, err := MarkIntentCandidateVerificationPending(
		ctx, d, candidate.ID, candidate.BranchRef, 4,
		"verification resources unavailable", 10,
	); err != nil || changed {
		t.Fatalf("wrong-pair pending update=(%t,%v)", changed, err)
	}
	changed, err := MarkIntentCandidateVerificationPending(
		ctx, d, candidate.ID, candidate.BranchRef,
		candidate.BranchGeneration,
		"verification resources unavailable", 11,
	)
	if err != nil || !changed {
		t.Fatalf("pending update=(%t,%v)", changed, err)
	}
	got, ok, err := IntentCandidateByID(ctx, d, candidate.ID)
	if err != nil || !ok {
		t.Fatalf("load pending candidate=(%+v,%t,%v)", got, ok, err)
	}
	if got.Status != candidate.Status || got.Readiness != candidate.Readiness ||
		got.Purpose != candidate.Purpose || len(got.Events) != 1 ||
		got.Events[0].EventSeq != seq ||
		!got.AtomicityStatus.Valid || got.AtomicityStatus.String != "pending" ||
		got.AtomicitySummary != "verification resources unavailable" ||
		!got.VerificationStatus.Valid ||
		got.VerificationStatus.String != "pending" ||
		got.VerificationOutput != "" || got.VerificationTS.Valid ||
		got.UpdatedTS != 11 {
		t.Fatalf("pending candidate=%+v", got)
	}
}

func TestIntentCandidateMergePreservesMembershipAndLineage(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	const (
		branch     = "refs/heads/main"
		generation = int64(17)
	)
	targetSeq := appendIntentV2Event(t, d, branch, generation, "target.go")
	firstSourceSeq := appendIntentV2Event(t, d, branch, generation, "first.go")
	secondSourceSeq := appendIntentV2Event(t, d, branch, generation, "second.go")
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "target", BranchRef: branch, BranchGeneration: generation,
		Status: IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Purpose: "initial target",
		Events: []IntentCandidateEvent{{
			EventSeq: targetSeq, EventRole: "implementation",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	for _, source := range []struct {
		id, oid, pathRole string
		seq               int64
	}{
		{id: "source-a", oid: "commit-a", pathRole: "test", seq: firstSourceSeq},
		{id: "source-b", oid: "commit-b", pathRole: "migration", seq: secondSourceSeq},
	} {
		if err := SaveIntentCandidate(ctx, d, IntentCandidate{
			ID: source.id, BranchRef: branch, BranchGeneration: generation,
			Status: IntentCandidateSoftPublished, Readiness: IntentReadinessReady,
			PublishedCommitOID: sql.NullString{String: source.oid, Valid: true},
			Events: []IntentCandidateEvent{{
				EventSeq: source.seq, EventRole: source.pathRole,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	request := IntentCandidateMergeRequest{
		Target: IntentCandidate{
			ID: "target", BranchRef: branch, BranchGeneration: generation,
			Status: IntentCandidateWaiting, Readiness: IntentReadinessWait,
			Purpose: "canonical hard dependency component",
			Events: []IntentCandidateEvent{{
				EventSeq: targetSeq, EventRole: "implementation",
			}},
		},
		SourceCandidateIDs: []string{"source-b", "source-a", "source-a"},
		Reason:             "hard_dependency_continuation",
		MergedTS:           123,
	}
	result, err := MergeIntentCandidates(ctx, d, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Candidate.ID != "target" ||
		result.Candidate.Purpose != "canonical hard dependency component" ||
		len(result.Candidate.Events) != 3 ||
		len(result.Lineage) != 2 {
		t.Fatalf("merge result=%+v", result)
	}
	wantSeqs := []int64{targetSeq, firstSourceSeq, secondSourceSeq}
	for i, want := range wantSeqs {
		if result.Candidate.Events[i].EventSeq != want ||
			result.Candidate.Events[i].Ord != i ||
			result.Candidate.Events[i].MembershipState != IntentMembershipActive {
			t.Fatalf("canonical events=%+v", result.Candidate.Events)
		}
	}
	for i, want := range []struct {
		id, oid string
	}{
		{id: "source-a", oid: "commit-a"},
		{id: "source-b", oid: "commit-b"},
	} {
		got := result.Lineage[i]
		if got.SourceCandidateID != want.id ||
			got.TargetCandidateID != "target" ||
			got.SourceStatus != IntentCandidateSoftPublished ||
			!got.SourcePublishedCommitOID.Valid ||
			got.SourcePublishedCommitOID.String != want.oid ||
			got.Reason != request.Reason || got.CreatedTS != 123 {
			t.Fatalf("lineage[%d]=%+v", i, got)
		}
		source, ok, err := IntentCandidateByID(ctx, d, want.id)
		if err != nil || !ok || source.Status != IntentCandidateSuperseded ||
			len(source.Events) != 0 ||
			source.PublishedCommitOID.String != want.oid {
			t.Fatalf("source %s=%+v ok=%v err=%v", want.id, source, ok, err)
		}
		history, err := IntentCandidateEventHistory(ctx, d, want.id)
		if err != nil || len(history) != 1 ||
			history[0].MembershipState != IntentMembershipSuperseded {
			t.Fatalf("source history %s=%+v err=%v", want.id, history, err)
		}
	}
	byTarget, err := IntentCandidateLineageForTarget(
		ctx, d, branch, generation, "target", 0)
	if err != nil || len(byTarget) != 2 {
		t.Fatalf("lineage by target=%+v err=%v", byTarget, err)
	}
	bySource, ok, err := IntentCandidateLineageBySource(
		ctx, d, branch, generation, "source-a")
	if err != nil || !ok || bySource.TargetCandidateID != "target" {
		t.Fatalf("lineage by source=%+v ok=%v err=%v", bySource, ok, err)
	}

	// The same durable request is a no-op: it neither duplicates lineage nor
	// rewrites the original timestamp.
	repeated, err := MergeIntentCandidates(ctx, d, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(repeated.Lineage) != 2 ||
		repeated.Candidate.UpdatedTS != 123 {
		t.Fatalf("idempotent result=%+v", repeated)
	}
	var count int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM intent_candidate_lineage`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("lineage rows=%d want=2", count)
	}

	// Reusing the same lineage is idempotent only for lineage insertion. A
	// later exact-pair capture and revised target metadata must still advance
	// the canonical candidate transactionally.
	lateSeq := appendIntentV2Event(t, d, branch, generation, "late-test.go")
	advanced := request
	advanced.Target.Purpose = "canonical component with late companion"
	advanced.Target.Events = append(
		append([]IntentCandidateEvent(nil), request.Target.Events...),
		IntentCandidateEvent{
			EventSeq: lateSeq, EventRole: "test",
		},
	)
	advanced.MergedTS = 124
	lateResult, err := MergeIntentCandidates(ctx, d, advanced)
	if err != nil {
		t.Fatal(err)
	}
	if lateResult.Candidate.Purpose != advanced.Target.Purpose ||
		lateResult.Candidate.UpdatedTS != 124 ||
		len(lateResult.Candidate.Events) != 4 ||
		lateResult.Candidate.Events[3].EventSeq != lateSeq ||
		lateResult.Candidate.Events[3].MembershipState !=
			IntentMembershipActive {
		t.Fatalf("advanced merge result=%+v", lateResult)
	}
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM intent_candidate_lineage`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("advanced lineage rows=%d want=2", count)
	}
	var lateOwner string
	if err := d.SQL().QueryRowContext(ctx, `
SELECT candidate_id FROM intent_candidate_events
WHERE event_seq=? AND membership_state='active'`, lateSeq).Scan(&lateOwner); err != nil {
		t.Fatal(err)
	}
	if lateOwner != "target" {
		t.Fatalf("late event owner=%q want target", lateOwner)
	}
}

func TestIntentCandidateMergeFailsClosedWithoutDroppingMembership(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	targetSeq := appendIntentV2Event(
		t, d, "refs/heads/main", 18, "target.go")
	sourceSeq := appendIntentV2Event(
		t, d, "refs/heads/main", 18, "source.go")
	foreignSeq := appendIntentV2Event(
		t, d, "refs/heads/main", 18, "foreign.go")
	for _, candidate := range []IntentCandidate{
		{
			ID: "target", BranchRef: "refs/heads/main", BranchGeneration: 18,
			Status: IntentCandidateWaiting, Readiness: IntentReadinessWait,
			Events: []IntentCandidateEvent{{
				EventSeq: targetSeq, EventRole: "code",
			}},
		},
		{
			ID: "source", BranchRef: "refs/heads/main", BranchGeneration: 18,
			Status: IntentCandidateReady, Readiness: IntentReadinessReady,
			Events: []IntentCandidateEvent{{
				EventSeq: sourceSeq, EventRole: "test",
			}},
		},
		{
			ID: "foreign", BranchRef: "refs/heads/main", BranchGeneration: 18,
			Status: IntentCandidateWaiting, Readiness: IntentReadinessWait,
			Events: []IntentCandidateEvent{{
				EventSeq: foreignSeq, EventRole: "docs",
			}},
		},
	} {
		if err := SaveIntentCandidate(ctx, d, candidate); err != nil {
			t.Fatal(err)
		}
	}
	_, err := MergeIntentCandidates(ctx, d, IntentCandidateMergeRequest{
		Target: IntentCandidate{
			ID: "target", BranchRef: "refs/heads/main", BranchGeneration: 18,
			Status: IntentCandidateReady, Readiness: IntentReadinessReady,
			Events: []IntentCandidateEvent{
				{EventSeq: targetSeq, EventRole: "code"},
				{EventSeq: foreignSeq, EventRole: "docs"},
			},
		},
		SourceCandidateIDs: []string{"source"},
		Reason:             "hard_dependency_continuation",
	})
	if err == nil || !strings.Contains(err.Error(), "unlisted candidate") {
		t.Fatalf("merge error=%v", err)
	}
	for id, seq := range map[string]int64{
		"target": targetSeq, "source": sourceSeq, "foreign": foreignSeq,
	} {
		candidate, ok, loadErr := IntentCandidateByID(ctx, d, id)
		if loadErr != nil || !ok || len(candidate.Events) != 1 ||
			candidate.Events[0].EventSeq != seq ||
			candidate.Status == IntentCandidateSuperseded {
			t.Fatalf("rolled back %s=%+v ok=%v err=%v",
				id, candidate, ok, loadErr)
		}
	}
	var lineage int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM intent_candidate_lineage`).Scan(&lineage); err != nil {
		t.Fatal(err)
	}
	if lineage != 0 {
		t.Fatalf("lineage rows=%d want=0", lineage)
	}
}

func TestIntentV2CandidateSaveRollsBackOnWrongPair(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendIntentV2Event(t, d, "refs/heads/other", 1, "other.go")
	err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "wrong-pair", BranchRef: "refs/heads/main", BranchGeneration: 1,
		Status: IntentCandidateOpen, Readiness: IntentReadinessWait,
		Purpose: "must not persist",
		Events:  []IntentCandidateEvent{{EventSeq: seq, EventRole: "code"}},
	})
	if err == nil || !strings.Contains(err.Error(), "exact branch pair") {
		t.Fatalf("SaveIntentCandidate err=%v", err)
	}
	var count int
	if err := d.SQL().QueryRow(`SELECT COUNT(*) FROM intent_candidates`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("candidate count=%d want=0", count)
	}
}

func TestIntentV2CandidateAndDependencyCapsFailClosed(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendIntentV2Event(t, d, "refs/heads/main", 1, "a.go")

	events := make([]IntentCandidateEvent, IntentCandidateMaxCaptures+1)
	for i := range events {
		events[i] = IntentCandidateEvent{EventSeq: int64(i + 1), EventRole: "code"}
	}
	err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "too-large", BranchRef: "refs/heads/main", BranchGeneration: 1,
		Status: IntentCandidateOpen, Readiness: IntentReadinessWait,
		Events: events,
	})
	if err == nil || !strings.Contains(err.Error(), "1..256") {
		t.Fatalf("candidate cap err=%v", err)
	}

	baseline := []IntentCaptureDependency{{
		PrerequisiteSeq: seq, DependentSeq: seq + 1,
		Strength: IntentDependencyHard, Kind: "same_path",
	}}
	// The dependent does not exist, so seed it and establish one valid graph.
	dependent := appendIntentV2Event(t, d, "refs/heads/main", 1, "b.go")
	baseline[0].DependentSeq = dependent
	if err := ReplaceIntentCaptureDependencies(ctx, d, "refs/heads/main", 1, baseline); err != nil {
		t.Fatalf("seed dependencies: %v", err)
	}
	over := make([]IntentCaptureDependency, IntentDependencyMaxPerPair+1)
	err = ReplaceIntentCaptureDependencies(ctx, d, "refs/heads/main", 1, over)
	if err == nil || !strings.Contains(err.Error(), "cap 4096") {
		t.Fatalf("dependency cap err=%v", err)
	}
	got, err := IntentCaptureDependenciesForPair(ctx, d, "refs/heads/main", 1)
	if err != nil || len(got) != 1 || got[0].Kind != "same_path" {
		t.Fatalf("prior dependency graph=%+v err=%v", got, err)
	}
}

func TestIntentV2OpenCandidateAndRepairCapsFailClosed(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendIntentV2Event(t, d, "refs/heads/main", 4, "cap.go")
	for i := 0; i < IntentCandidateMaxOpenPerPair; i++ {
		memberSeq := appendIntentV2Event(t, d, "refs/heads/main", 4,
			fmt.Sprintf("existing-%03d.go", i))
		if _, err := d.SQL().ExecContext(ctx, `
INSERT INTO intent_candidates(
    id, branch_ref, branch_generation, status, created_ts, updated_ts
) VALUES (?, 'refs/heads/main', 4, 'open', 1, 1)`,
			fmt.Sprintf("existing-%03d", i)); err != nil {
			t.Fatalf("seed open candidate %d: %v", i, err)
		}
		if _, err := d.SQL().ExecContext(ctx, `
INSERT INTO intent_candidate_events(
    candidate_id, ord, event_seq, event_role, membership_state
) VALUES (?, 0, ?, 'code', 'active')`,
			fmt.Sprintf("existing-%03d", i), memberSeq); err != nil {
			t.Fatalf("seed open membership %d: %v", i, err)
		}
	}
	err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "over-open-cap", BranchRef: "refs/heads/main", BranchGeneration: 4,
		Status: IntentCandidateOpen, Readiness: IntentReadinessWait,
		Events: []IntentCandidateEvent{{EventSeq: seq, EventRole: "code"}},
	})
	if err == nil || !strings.Contains(err.Error(), "cap 128") {
		t.Fatalf("open candidate cap err=%v", err)
	}

	commits := make([]IntentRepairCommit, IntentRepairMaxCommits+1)
	for i := range commits {
		commits[i].OldOID = fmt.Sprintf("old-%d", i)
	}
	err = SaveIntentRepair(ctx, d, IntentRepair{
		ID: "over-repair-cap", BranchRef: "refs/heads/main",
		BranchGeneration: 4, ExpectedHead: "head",
		PlanDigest: testIntentRepairPlanDigest,
		Commits:    commits,
	})
	if err == nil || !strings.Contains(err.Error(), "1..5") {
		t.Fatalf("repair cap err=%v", err)
	}
	var repairs int
	if err := d.SQL().QueryRow(`SELECT COUNT(*) FROM intent_repairs`).Scan(&repairs); err != nil {
		t.Fatal(err)
	}
	if repairs != 0 {
		t.Fatalf("repair count=%d want=0", repairs)
	}
}

func TestIntentV2TerminalAndEmptyCandidatesDoNotConsumeOpenCap(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	first := appendIntentV2Event(t, d, "refs/heads/main", 5, "first.go")
	second := appendIntentV2Event(t, d, "refs/heads/main", 5, "second.go")
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "old", BranchRef: "refs/heads/main", BranchGeneration: 5,
		Status: IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Events: []IntentCandidateEvent{
			{EventSeq: first, EventRole: "code"},
			{EventSeq: second, EventRole: "test"},
		},
	}); err != nil {
		t.Fatalf("save old candidate: %v", err)
	}
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "replacement", BranchRef: "refs/heads/main", BranchGeneration: 5,
		Status: IntentCandidateReady, Readiness: IntentReadinessReady,
		Events: []IntentCandidateEvent{
			{EventSeq: first, EventRole: "code"},
			{EventSeq: second, EventRole: "test"},
		},
	}); err != nil {
		t.Fatalf("save replacement: %v", err)
	}
	old, ok, err := IntentCandidateByID(ctx, d, "old")
	if err != nil || !ok || old.Status != IntentCandidateSuperseded ||
		len(old.Events) != 0 {
		t.Fatalf("retired old candidate=%+v ok=%v err=%v", old, ok, err)
	}
	if _, err := d.SQL().ExecContext(ctx, `
UPDATE intent_candidates
SET status='published', published_commit_oid='commit'
WHERE id='replacement'`); err != nil {
		t.Fatal(err)
	}
	open, err := IntentCandidatesForPair(ctx, d, "refs/heads/main", 5, 0)
	if err != nil || len(open) != 0 {
		t.Fatalf("open candidates=%+v err=%v", open, err)
	}
}

func TestIntentV2FinalizeExpiredSoftPublication(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendIntentV2Event(t, d, "refs/heads/main", 6, "late.go")
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "soft", BranchRef: "refs/heads/main", BranchGeneration: 6,
		Status: IntentCandidateSoftPublished, Readiness: IntentReadinessReady,
		SoftPublicationDeadline: sql.NullFloat64{Float64: 10, Valid: true},
		Events:                  []IntentCandidateEvent{{EventSeq: seq, EventRole: "code"}},
	}); err != nil {
		t.Fatalf("save soft candidate: %v", err)
	}
	if n, err := FinalizeExpiredIntentCandidates(
		ctx, d, "refs/heads/main", 6, 11,
	); err != nil || n != 1 {
		t.Fatalf("finalize expired=(%d,%v)", n, err)
	}
	got, ok, err := IntentCandidateByID(ctx, d, "soft")
	if err != nil || !ok || got.Status != IntentCandidatePublished {
		t.Fatalf("finalized candidate=%+v ok=%v err=%v", got, ok, err)
	}
	open, err := IntentCandidatesForPair(ctx, d, "refs/heads/main", 6, 0)
	if err != nil || len(open) != 0 {
		t.Fatalf("open after expiry=%+v err=%v", open, err)
	}
}

func TestIntentV2PublishedPruneCleansTerminalGraphAndProtectsOpenMember(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	appendPublished := func(path string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 7,
			BaseHead: "base", Operation: "update", Path: path,
			Fidelity: "full", CapturedTS: 10, State: EventStatePublished,
		}, nil)
		if err != nil {
			t.Fatalf("append published %s: %v", path, err)
		}
		return seq
	}
	terminalSeq := appendPublished("terminal.go")
	protectedSeq := appendPublished("soft.go")
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "terminal", BranchRef: "refs/heads/main", BranchGeneration: 7,
		Status: IntentCandidatePublished, Readiness: IntentReadinessReady,
		PublishedCommitOID: sql.NullString{String: "old", Valid: true},
		Events:             []IntentCandidateEvent{{EventSeq: terminalSeq, EventRole: "code"}},
	}); err != nil {
		t.Fatalf("save terminal candidate: %v", err)
	}
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "repairable", BranchRef: "refs/heads/main", BranchGeneration: 7,
		Status: IntentCandidateSoftPublished, Readiness: IntentReadinessReady,
		SoftPublicationDeadline: sql.NullFloat64{Float64: 200, Valid: true},
		PublishedCommitOID:      sql.NullString{String: "head", Valid: true},
		Events:                  []IntentCandidateEvent{{EventSeq: protectedSeq, EventRole: "code"}},
	}); err != nil {
		t.Fatalf("save repairable candidate: %v", err)
	}
	if err := ReplaceIntentCaptureDependencies(ctx, d, "refs/heads/main", 7,
		[]IntentCaptureDependency{{
			PrerequisiteSeq: terminalSeq, DependentSeq: protectedSeq,
			Strength: IntentDependencySoft, Kind: "module_proximity",
		}}); err != nil {
		t.Fatalf("save dependency: %v", err)
	}
	if n, err := PrunePublishedEventsBefore(ctx, d, 100); err != nil || n != 1 {
		t.Fatalf("prune published=(%d,%v)", n, err)
	}
	var terminalExists, protectedExists int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq=?`, terminalSeq,
	).Scan(&terminalExists); err != nil {
		t.Fatal(err)
	}
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq=?`, protectedSeq,
	).Scan(&protectedExists); err != nil {
		t.Fatal(err)
	}
	if terminalExists != 0 || protectedExists != 1 {
		t.Fatalf("event counts terminal=%d protected=%d",
			terminalExists, protectedExists)
	}
	terminal, ok, err := IntentCandidateByID(ctx, d, "terminal")
	if err != nil || !ok || len(terminal.Events) != 0 {
		t.Fatalf("terminal candidate=%+v ok=%v err=%v", terminal, ok, err)
	}
	deps, err := IntentCaptureDependenciesForPair(
		ctx, d, "refs/heads/main", 7)
	if err != nil || len(deps) != 0 {
		t.Fatalf("dependencies after prune=%+v err=%v", deps, err)
	}
}

func TestIntentV2DependenciesBoundariesAndRepairRoundTrip(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	first := appendIntentV2Event(t, d, "refs/heads/main", 9, "model.go")
	second := appendIntentV2Event(t, d, "refs/heads/main", 9, "model_test.go")
	deps := []IntentCaptureDependency{
		{PrerequisiteSeq: first, DependentSeq: second, Strength: IntentDependencyHard, Kind: "create_before_modify", Evidence: "path-hash"},
		{PrerequisiteSeq: first, DependentSeq: second, Strength: IntentDependencySoft, Kind: "test_source", Evidence: "symbol-hash"},
	}
	if err := ReplaceIntentCaptureDependencies(ctx, d, "refs/heads/main", 9, deps); err != nil {
		t.Fatalf("ReplaceIntentCaptureDependencies: %v", err)
	}
	gotDeps, err := IntentCaptureDependenciesForPair(ctx, d, "refs/heads/main", 9)
	if err != nil || len(gotDeps) != 2 {
		t.Fatalf("dependencies=%+v err=%v", gotDeps, err)
	}

	boundary, err := AppendIntentActivityBoundary(ctx, d, IntentActivityBoundary{
		Kind: IntentBoundarySoft, Source: "codex_stop",
		BranchRef:        sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 9, Valid: true},
	})
	if err != nil || boundary.Epoch != 1 {
		t.Fatalf("boundary=%+v err=%v", boundary, err)
	}
	if _, err := AppendIntentActivityBoundary(ctx, d, IntentActivityBoundary{
		Kind: IntentBoundaryHard, Source: "logical_flush",
	}); err != nil {
		t.Fatalf("append second boundary: %v", err)
	}
	pending, err := PendingIntentActivityBoundaries(ctx, d, 0, 10)
	if err != nil || len(pending) != 2 || pending[1].Epoch != 2 {
		t.Fatalf("pending boundaries=%+v err=%v", pending, err)
	}
	if n, err := ConsumeIntentActivityBoundaries(ctx, d, 1, 10); err != nil || n != 1 {
		t.Fatalf("consume boundaries=(%d,%v)", n, err)
	}
	for _, candidate := range []IntentCandidate{
		{
			ID: "candidate-a", BranchRef: "refs/heads/main",
			BranchGeneration: 9, Status: IntentCandidateReady,
			Readiness: IntentReadinessReady,
			Events:    []IntentCandidateEvent{{EventSeq: first, EventRole: "code"}},
		},
		{
			ID: "candidate-b", BranchRef: "refs/heads/main",
			BranchGeneration: 9, Status: IntentCandidateReady,
			Readiness: IntentReadinessReady,
			Events:    []IntentCandidateEvent{{EventSeq: second, EventRole: "test"}},
		},
	} {
		if err := SaveIntentCandidate(ctx, d, candidate); err != nil {
			t.Fatalf("SaveIntentCandidate %s: %v", candidate.ID, err)
		}
	}
	members, err := SnapshotIntentRepairMembers(ctx, d, "repair-1",
		"refs/heads/main", 9, []string{"candidate-a", "candidate-b"})
	if err != nil {
		t.Fatalf("SnapshotIntentRepairMembers: %v", err)
	}

	repair := IntentRepair{
		ID: "repair-1", BranchRef: "refs/heads/main", BranchGeneration: 9,
		Status: IntentRepairPrepared, ExpectedHead: "head-2",
		PlanDigest: testIntentRepairPlanDigest,
		Commits: []IntentRepairCommit{
			{OldOID: "head-1", CandidateID: sql.NullString{String: "candidate-a", Valid: true}},
			{OldOID: "head-2", CandidateID: sql.NullString{String: "candidate-b", Valid: true}},
		},
		Members: members,
	}
	if err := SaveIntentRepair(ctx, d, repair); err != nil {
		t.Fatalf("SaveIntentRepair: %v", err)
	}
	applied, err := TransitionIntentRepair(ctx, d, repair.ID, IntentRepairTransition{
		ExpectedStatus: IntentRepairPrepared, Status: IntentRepairGitApplied,
		BackupRef: sql.NullString{String: "refs/acd/intent-repair/backup", Valid: true},
		OldHead:   sql.NullString{String: "head-2", Valid: true},
		NewHead:   sql.NullString{String: "new-2", Valid: true},
		Commits: []IntentRepairCommit{
			{OldOID: "head-1", CandidateID: sql.NullString{String: "candidate-a", Valid: true}, NewOID: sql.NullString{String: "new-1", Valid: true}},
			{OldOID: "head-2", CandidateID: sql.NullString{String: "candidate-b", Valid: true}, NewOID: sql.NullString{String: "new-2", Valid: true}},
		},
	})
	if err != nil || !applied {
		t.Fatalf("git-applied transition=(%v,%v)", applied, err)
	}
	completed, err := TransitionIntentRepair(ctx, d, repair.ID, IntentRepairTransition{
		ExpectedStatus: IntentRepairGitApplied, Status: IntentRepairCompleted,
	})
	if err != nil || !completed {
		t.Fatalf("completed transition=(%v,%v)", completed, err)
	}
	gotRepair, ok, err := IntentRepairByID(ctx, d, repair.ID)
	if err != nil || !ok || gotRepair.Status != IntentRepairCompleted ||
		len(gotRepair.Commits) != 2 || gotRepair.Commits[1].NewOID.String != "new-2" {
		t.Fatalf("repair=%+v ok=%v err=%v", gotRepair, ok, err)
	}
}

func TestIntentRepairMembershipSnapshotRoundTripAndImmutability(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	published, err := AppendCaptureEvent(ctx, d, CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 12,
		BaseHead: "base", Operation: "create", Path: "published.go",
		Fidelity: "full", State: EventStatePublished,
		CommitOID: sql.NullString{String: "old-commit", Valid: true},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pending := appendIntentV2Event(t, d, "refs/heads/main", 12, "pending.go")
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "repair-member-candidate", BranchRef: "refs/heads/main",
		BranchGeneration: 12, Status: IntentCandidateReady,
		Readiness: IntentReadinessReady,
		Events: []IntentCandidateEvent{
			{EventSeq: published, EventRole: "code"},
			{EventSeq: pending, EventRole: "test"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	members, err := SnapshotIntentRepairMembers(
		ctx, d, "repair-members", "refs/heads/main", 12,
		[]string{"repair-member-candidate"})
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0].EventSeq != published ||
		members[0].PriorState != EventStatePublished ||
		members[1].EventSeq != pending ||
		members[1].PriorState != EventStatePending {
		t.Fatalf("members=%+v", members)
	}
	repair := IntentRepair{
		ID: "repair-members", BranchRef: "refs/heads/main",
		BranchGeneration: 12, ExpectedHead: "old-commit",
		PlanDigest: testIntentRepairPlanDigest,
		Commits: []IntentRepairCommit{{
			CandidateID: sql.NullString{
				String: "repair-member-candidate", Valid: true,
			},
			OldOID: "old-commit",
		}},
		Members: members,
	}
	if err := SaveIntentRepair(ctx, d, repair); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := IntentRepairByID(ctx, d, repair.ID)
	if err != nil || !ok || len(stored.Members) != 2 ||
		stored.Members[1] != members[1] {
		t.Fatalf("stored=%+v ok=%v err=%v", stored, ok, err)
	}
	if _, err := d.SQL().ExecContext(ctx, `
UPDATE intent_repair_members SET prior_state='published'
WHERE repair_id=? AND ord=1`, repair.ID); err == nil ||
		!strings.Contains(err.Error(), "membership is immutable") {
		t.Fatalf("mutable repair membership error=%v", err)
	}
	applied, err := TransitionIntentRepair(ctx, d, repair.ID,
		IntentRepairTransition{
			ExpectedStatus: IntentRepairPrepared,
			Status:         IntentRepairGitApplied,
			Commits: []IntentRepairCommit{{
				CandidateID: sql.NullString{
					String: "repair-member-candidate", Valid: true,
				},
				OldOID: "old-commit",
				NewOID: sql.NullString{String: "new-commit", Valid: true},
			}},
		})
	if err != nil || !applied {
		t.Fatalf("git-applied transition=(%v,%v)", applied, err)
	}
	recoverable, err := RecoverableIntentRepairs(ctx, d, 10)
	if err != nil || len(recoverable) != 1 ||
		len(recoverable[0].Members) != len(members) {
		t.Fatalf("recoverable=%+v err=%v", recoverable, err)
	}
	if _, err := d.SQL().ExecContext(ctx, `
INSERT INTO intent_repair_members(
    repair_id,ord,candidate_id,event_seq,prior_state
	) VALUES (?,2,'repair-member-candidate',999,'pending')`, repair.ID); err == nil ||
		!strings.Contains(err.Error(), "membership is not open") {
		t.Fatalf("late repair membership error=%v", err)
	}
	if _, err := d.SQL().ExecContext(ctx,
		`DELETE FROM intent_repair_members WHERE repair_id=? AND ord=0`,
		repair.ID); err == nil ||
		!strings.Contains(err.Error(), "membership is immutable") {
		t.Fatalf("deleted repair membership error=%v", err)
	}
}

func TestSaveIntentRepairRejectsMembershipDrift(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	published, err := AppendCaptureEvent(ctx, d, CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 13,
		BaseHead: "base", Operation: "create", Path: "before.go",
		Fidelity: "full", State: EventStatePublished,
		CommitOID: sql.NullString{String: "old-commit", Valid: true},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate := IntentCandidate{
		ID: "drifting-repair-candidate", BranchRef: "refs/heads/main",
		BranchGeneration: 13, Status: IntentCandidateReady,
		Readiness: IntentReadinessReady,
		Events:    []IntentCandidateEvent{{EventSeq: published, EventRole: "code"}},
	}
	if err := SaveIntentCandidate(ctx, d, candidate); err != nil {
		t.Fatal(err)
	}
	members, err := SnapshotIntentRepairMembers(
		ctx, d, "repair-drift", candidate.BranchRef,
		candidate.BranchGeneration, []string{candidate.ID})
	if err != nil {
		t.Fatal(err)
	}
	late := appendIntentV2Event(t, d, candidate.BranchRef,
		candidate.BranchGeneration, "after.go")
	candidate.Events = append(candidate.Events,
		IntentCandidateEvent{EventSeq: late, EventRole: "test"})
	if err := SaveIntentCandidate(ctx, d, candidate); err != nil {
		t.Fatal(err)
	}
	err = SaveIntentRepair(ctx, d, IntentRepair{
		ID: "repair-drift", BranchRef: candidate.BranchRef,
		BranchGeneration: candidate.BranchGeneration,
		ExpectedHead:     "old-commit",
		PlanDigest:       testIntentRepairPlanDigest,
		Commits: []IntentRepairCommit{{
			CandidateID: sql.NullString{String: candidate.ID, Valid: true},
			OldOID:      "old-commit",
		}},
		Members: members,
	})
	if err == nil || !strings.Contains(err.Error(), "incomplete membership") {
		t.Fatalf("membership drift error=%v", err)
	}
	if _, ok, loadErr := IntentRepairByID(ctx, d, "repair-drift"); loadErr != nil || ok {
		t.Fatalf("rolled-back repair ok=%v err=%v", ok, loadErr)
	}
}

func TestIntentActivityBoundaryRetriesConcurrentEpochAllocation(t *testing.T) {
	d1, dbPath := openTestDB(t)
	d2, err := Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d2.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		db := d1
		if i%2 == 1 {
			db = d2
		}
		go func(index int) {
			defer wg.Done()
			<-start
			_, err := AppendIntentActivityBoundary(
				ctx, db, IntentActivityBoundary{
					Kind:   IntentBoundaryHard,
					Source: fmt.Sprintf("concurrent-%d", index),
				})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent boundary: %v", err)
		}
	}
	pending, err := PendingIntentActivityBoundaries(ctx, d1, 0, 25)
	if err != nil || len(pending) != 20 {
		t.Fatalf("boundaries=%d err=%v", len(pending), err)
	}
}

func TestIntentV2SchemaV14MigrationAndIdempotentReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`
CREATE TABLE daemon_meta(
 key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_ts REAL NOT NULL);
CREATE TABLE daily_rollups(
 day TEXT NOT NULL, repo_root TEXT NOT NULL, events_total INTEGER NOT NULL DEFAULT 0,
 commits_total INTEGER NOT NULL DEFAULT 0, files_changed INTEGER NOT NULL DEFAULT 0,
 bytes_changed INTEGER NOT NULL DEFAULT 0, errors_total INTEGER NOT NULL DEFAULT 0,
 sessions_seen INTEGER NOT NULL DEFAULT 0, daemon_uptime_seconds INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(day, repo_root));
INSERT INTO daemon_meta(key,value,updated_ts) VALUES('v14.keep','yes',1);
INSERT INTO daily_rollups(day,repo_root,events_total) VALUES('2026-07-26','/repo',17);
PRAGMA user_version=14;`); err != nil {
		t.Fatalf("seed v14: %v", err)
	}
	_ = raw.Close()

	d, err := Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("Open migrated v14: %v", err)
	}
	if version, _ := d.UserVersion(context.Background()); version != SchemaVersion {
		t.Fatalf("user_version=%d want=%d", version, SchemaVersion)
	}
	var keep string
	if err := d.SQL().QueryRow(`SELECT value FROM daemon_meta WHERE key='v14.keep'`).Scan(&keep); err != nil || keep != "yes" {
		t.Fatalf("preserved meta=%q err=%v", keep, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("idempotent reopen: %v", err)
	}
	defer reopened.Close()
	for _, table := range intentV2TableNames() {
		var count int
		if err := reopened.SQL().QueryRow(`
SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("table %s count=%d err=%v", table, count, err)
		}
	}
}

func TestIntentRepairMembershipV24RuntimeMigrationSealsLegacyRows(t *testing.T) {
	t.Parallel()
	d, dbPath := openTestDB(t)
	ctx := context.Background()
	legacyStatuses := map[string]string{
		"legacy-prepared":    IntentRepairPrepared,
		"legacy-git-applied": IntentRepairGitApplied,
		"legacy-completed":   IntentRepairCompleted,
	}
	for id, status := range legacyStatuses {
		if err := SaveIntentRepair(ctx, d, IntentRepair{
			ID: id, BranchRef: "refs/heads/main", BranchGeneration: 3,
			ExpectedHead: "old-" + id, PlanDigest: testIntentRepairPlanDigest,
			MembershipMode: IntentRepairMembershipNone,
			Commits: []IntentRepairCommit{{
				CandidateID: sql.NullString{String: "candidate-" + id, Valid: true},
				OldOID:      "old-" + id,
			}},
		}); err != nil {
			t.Fatal(err)
		}
		if status == IntentRepairPrepared {
			continue
		}
		completedTS := any(nil)
		if status == IntentRepairCompleted {
			completedTS = 3.0
		}
		if _, err := d.SQL().ExecContext(ctx, `
UPDATE intent_repairs
SET status=?, backup_ref=?, old_head=expected_head, new_head=?,
    git_applied_ts=2, completed_ts=?
WHERE id=?;
UPDATE intent_repair_commits SET new_oid=? WHERE repair_id=?`,
			status, "refs/acd/intent-repair/legacy/"+id+"/backup",
			"new-"+id, completedTS, id, "new-"+id, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `
DROP TABLE intent_repair_members;
DROP TABLE intent_repair_member_seals;
PRAGMA user_version=23;`); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := OpenRuntime(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	for id, status := range legacyStatuses {
		stored, ok, err := IntentRepairByID(ctx, migrated, id)
		if err != nil || !ok || stored.Status != status ||
			stored.MembershipMode != IntentRepairMembershipLegacy ||
			len(stored.Members) != 0 {
			t.Fatalf("legacy repair %s=%+v ok=%v err=%v",
				id, stored, ok, err)
		}
	}
	for _, table := range []string{
		"intent_repair_members", "intent_repair_member_seals",
	} {
		var found string
		if err := migrated.SQL().QueryRowContext(ctx, `
SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).
			Scan(&found); err != nil || found != table {
			t.Fatalf("migrated table %s=%q err=%v", table, found, err)
		}
	}
}

func TestLoadIntentV2StateReadOnlyPreV15DoesNotMigrate(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE marker(value TEXT); INSERT INTO marker VALUES('keep'); PRAGMA user_version=14;`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()

	projection, err := LoadIntentV2StateReadOnly(context.Background(), dbPath)
	if err != nil || projection.Available || projection.SchemaVersion != 14 {
		t.Fatalf("projection=%+v err=%v", projection, err)
	}
	check, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var version, v2Tables int
	if err := check.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := check.QueryRow(`
SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'intent_%'`,
	).Scan(&v2Tables); err != nil {
		t.Fatal(err)
	}
	if version != 14 || v2Tables != 0 {
		t.Fatalf("read-only projection mutated db: version=%d tables=%d", version, v2Tables)
	}
}

func TestLoadIntentV2StateReadOnlyCurrentProjection(t *testing.T) {
	t.Parallel()
	d, dbPath := openTestDB(t)
	ctx := context.Background()
	seq := appendIntentV2Event(t, d, "refs/heads/main", 1, "projection.go")
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "projection-candidate", BranchRef: "refs/heads/main",
		BranchGeneration: 1, Status: IntentCandidateBlocked,
		Readiness: IntentReadinessWait,
		VerificationStatus: sql.NullString{
			String: "failed", Valid: true,
		},
		Events: []IntentCandidateEvent{{EventSeq: seq, EventRole: "code"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendIntentActivityBoundary(ctx, d, IntentActivityBoundary{
		Kind: IntentBoundaryHard, Source: "logical_flush",
	}); err != nil {
		t.Fatal(err)
	}
	if err := SaveIntentRepair(ctx, d, IntentRepair{
		ID: "projection-repair", BranchRef: "refs/heads/main",
		BranchGeneration: 1, ExpectedHead: "head",
		PlanDigest: testIntentRepairPlanDigest,
		Commits:    []IntentRepairCommit{{OldOID: "head"}},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := LoadIntentV2StateReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Available || got.SchemaVersion != SchemaVersion ||
		!got.CandidateLineageAvailable || got.CandidateLineageRecords != 0 ||
		got.OpenCandidates != 1 || got.VerificationAttention != 1 ||
		got.RecoverableRepairs != 1 || got.LastBoundaryEpoch != 1 {
		t.Fatalf("projection=%+v", got)
	}
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "projection-replacement", BranchRef: "refs/heads/main",
		BranchGeneration: 1, Status: IntentCandidateWaiting,
		Readiness: IntentReadinessWait,
		VerificationStatus: sql.NullString{
			String: "passed", Valid: true,
		},
		Events: []IntentCandidateEvent{{EventSeq: seq, EventRole: "code"}},
	}); err != nil {
		t.Fatal(err)
	}
	got, err = LoadIntentV2StateReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if got.OpenCandidates != 1 || got.VerificationAttention != 0 {
		t.Fatalf("superseded failure projection=%+v", got)
	}
}

func TestIntentV2SchemaV16ReadOnlyProjectionAndMigration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()
	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	seq := appendIntentV2Event(
		t, d, "refs/heads/main", 16, "preserved.go")
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID: "preserved-candidate", BranchRef: "refs/heads/main",
		BranchGeneration: 16, Status: IntentCandidateWaiting,
		Readiness: IntentReadinessWait,
		Events: []IntentCandidateEvent{{
			EventSeq: seq, EventRole: "implementation",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`
DROP TABLE intent_candidate_lineage;
PRAGMA user_version=16;`); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	projection, err := LoadIntentV2StateReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !projection.Available || projection.SchemaVersion != 16 ||
		projection.CandidateLineageAvailable ||
		projection.CandidateLineageRecords != 0 ||
		projection.OpenCandidates != 1 {
		t.Fatalf("v16 projection=%+v", projection)
	}
	check, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	var version, lineageTables int
	if err := check.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		_ = check.Close()
		t.Fatal(err)
	}
	if err := check.QueryRow(`
SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name='intent_candidate_lineage'`,
	).Scan(&lineageTables); err != nil {
		_ = check.Close()
		t.Fatal(err)
	}
	if err := check.Close(); err != nil {
		t.Fatal(err)
	}
	if version != 16 || lineageTables != 0 {
		t.Fatalf("read-only v16 projection mutated db: version=%d lineage_tables=%d",
			version, lineageTables)
	}

	migrated, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := migrated.UserVersion(ctx); err != nil ||
		got != SchemaVersion {
		_ = migrated.Close()
		t.Fatalf("migrated version=%d err=%v", got, err)
	}
	candidate, ok, err := IntentCandidateByID(
		ctx, migrated, "preserved-candidate")
	if err != nil || !ok || len(candidate.Events) != 1 ||
		candidate.Events[0].EventSeq != seq {
		_ = migrated.Close()
		t.Fatalf("preserved candidate=%+v ok=%v err=%v",
			candidate, ok, err)
	}
	if err := migrated.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	projection, err = LoadIntentV2StateReadOnly(ctx, dbPath)
	if err != nil || !projection.CandidateLineageAvailable ||
		projection.SchemaVersion != SchemaVersion {
		t.Fatalf("reopened projection=%+v err=%v", projection, err)
	}
}

func TestIntentV2SchemaHasNoRawDiffColumns(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	for _, table := range intentV2TableNames() {
		rows, err := d.SQL().QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
		if err != nil {
			t.Fatalf("table_info %s: %v", table, err)
		}
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			lower := strings.ToLower(column)
			if lower == "diff" || lower == "raw_diff" || lower == "prompt" ||
				lower == "raw_response" || lower == "source_content" {
				rows.Close()
				t.Fatalf("privacy-sensitive column %s.%s", table, column)
			}
		}
		if err := rows.Close(); err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
	}
	for _, index := range []string{
		"idx_intent_candidates_pair_status_updated",
		"idx_intent_candidates_id_pair",
		"idx_intent_candidate_events_candidate_state_ord",
		"idx_intent_candidate_events_active_seq",
		"idx_intent_candidate_lineage_target_created",
		"idx_intent_capture_dependencies_pair_from",
		"idx_intent_capture_dependencies_pair_to",
		"idx_intent_activity_boundaries_consumed_epoch",
		"idx_intent_repairs_pair_status_updated",
		"idx_intent_repair_commits_old_oid",
		"idx_intent_repair_members_candidate",
		"idx_intent_repair_members_event",
	} {
		var found string
		if err := d.SQL().QueryRow(`
SELECT name FROM sqlite_master WHERE type='index' AND name=?`, index).Scan(&found); err != nil {
			t.Fatalf("index %s: %v", index, err)
		}
	}
}

func appendIntentV2Event(t *testing.T, d *DB, branch string, generation int64, path string) int64 {
	t.Helper()
	seq, err := AppendCaptureEvent(context.Background(), d, CaptureEvent{
		BranchRef: branch, BranchGeneration: generation, BaseHead: "base",
		Operation: "update", Path: path, Fidelity: "full",
	}, nil)
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	return seq
}

func intentV2TableNames() []string {
	return []string{
		"intent_candidates",
		"intent_candidate_events",
		"intent_candidate_lineage",
		"intent_capture_dependencies",
		"intent_activity_boundaries",
		"intent_repairs",
		"intent_repair_commits",
		"intent_repair_members",
		"intent_repair_member_seals",
	}
}

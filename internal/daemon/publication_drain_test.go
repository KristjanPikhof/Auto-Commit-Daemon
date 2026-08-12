package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const publicationDrainTestDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestPublicationDrainFrozenTargetOrdersHardDependenciesAndExcludesLaterEdits(
	t *testing.T,
) {
	ctx := context.Background()
	db, events, drain := openPublicationDrainTestState(t, 3, 2)
	drain.Phase = state.PublicationDrainEventFallback
	if err := state.ReplaceIntentCaptureDependencies(ctx, db,
		"refs/heads/main", 7, []state.IntentCaptureDependency{{
			BranchRef: "refs/heads/main", BranchGeneration: 7,
			PrerequisiteSeq: events[1].Seq, DependentSeq: events[0].Seq,
			Strength: string(ai.IntentDependencyHard), Kind: "cross_path",
		}}); err != nil {
		t.Fatal(err)
	}
	pending := []state.CaptureEvent{events[2], events[0], events[1]}
	filtered, err := publicationDrainPendingEvents(ctx, db, CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 7,
	}, drain, pending)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int64, 0, len(filtered))
	for _, event := range filtered {
		got = append(got, event.Seq)
	}
	want := []int64{events[1].Seq, events[0].Seq}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("frozen dependency order=%v want=%v", got, want)
	}
}

func TestPublicationDrainFinalFallbackKeepsHardComponentAtomic(t *testing.T) {
	planner := publicationDrainAtomicFallbackPlanner{}
	plan, err := planner.PlanIntentV2(context.Background(), ai.IntentPlanRequestV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		OfferedCaptures: []ai.OfferedCapture{
			{Seq: 1, Path: "service.go", Op: "modify"},
			{Seq: 2, Path: "service_test.go", Op: "modify"},
			{Seq: 3, Path: "README.md", Op: "modify"},
		},
		Dependencies: []ai.IntentCaptureDependency{{
			FromSeq: 1, ToSeq: 2, Strength: ai.IntentDependencyHard,
			Kind: "test_source",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Candidates) != 2 {
		t.Fatalf("candidates=%+v, want two dependency components", plan.Candidates)
	}
	var linked []int64
	for _, candidate := range plan.Candidates {
		if slices.Contains(candidate.SelectedSeqs, int64(1)) ||
			slices.Contains(candidate.SelectedSeqs, int64(2)) {
			linked = candidate.SelectedSeqs
		}
	}
	if !reflect.DeepEqual(linked, []int64{1, 2}) {
		t.Fatalf("hard dependency component=%v, want [1 2]", linked)
	}
}

func TestPublicationDrainFinalFallbackExpandsAcrossIntentWindow(t *testing.T) {
	ctx := context.Background()
	db, events, _ := openPublicationDrainTestState(t, 11, 11)
	for index := range events {
		events[index].Path = "same.go"
	}
	window, err := publicationDrainAtomicFallbackWindow(ctx, db, CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 7,
	}, events)
	if err != nil {
		t.Fatal(err)
	}
	if len(window) != 11 {
		t.Fatalf("atomic fallback window=%d, want complete 11-event component", len(window))
	}
}

func TestPublicationDrainFinalFallbackRefusesOversizedHardComponent(t *testing.T) {
	ctx := context.Background()
	db, events, _ := openPublicationDrainTestState(
		t, ai.IntentCandidateCaptureCap+1, ai.IntentCandidateCaptureCap+1)
	for index := range events {
		events[index].Path = "same.go"
	}
	_, err := publicationDrainAtomicFallbackWindow(ctx, db, CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 7,
	}, events)
	if err == nil || !strings.Contains(err.Error(), "exceeds 256 captures") {
		t.Fatalf("oversized component error=%v", err)
	}
}

func TestPublicationDrainRestartEscalatesOnceAndCompletesIdempotently(
	t *testing.T,
) {
	ctx := context.Background()
	db, events, drain := openPublicationDrainTestState(t, 2, 2)
	update := PublicationDrainUpdateFrom(drain, 11, 10)
	update.Phase = state.PublicationDrainSemantic
	if _, err := state.AdvancePublicationDrain(ctx, db, drain.ID, update); err != nil {
		t.Fatal(err)
	}
	loaded, err := state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := UpdatePublicationDrainAfterReplay(
		ctx, db, loaded, ReplaySummary{PlannerFailure: "invalid dependency graph"}, nil,
		time.Unix(12, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Phase != state.PublicationDrainNormalizing ||
		normalized.SemanticRebuildAttempts != 1 ||
		normalized.LastError != "invalid dependency graph" {
		t.Fatalf("normalized=%+v", normalized)
	}

	// Reload through the startup lookup to model a worker replacement.
	restarted, err := ActivePublicationDrainForPair(
		ctx, db, "refs/heads/main", 7)
	if err != nil || restarted == nil {
		t.Fatalf("restart lookup=(%+v,%v)", restarted, err)
	}
	fallback, err := ResumePublicationDrainNormalization(
		ctx, db, *restarted, time.Unix(13, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if fallback.Phase != state.PublicationDrainEventFallback ||
		fallback.SemanticRebuildAttempts != 1 ||
		fallback.EventFallbackCount != 1 {
		t.Fatalf("fallback=%+v", fallback)
	}

	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='commit-1',published_ts=14
WHERE seq=?`, events[0].Seq); err != nil {
		t.Fatal(err)
	}
	progress, err := UpdatePublicationDrainAfterReplay(
		ctx, db, fallback, ReplaySummary{Published: 1}, nil,
		time.Unix(14, 0).UTC())
	if err != nil || progress.PublishedEventCount != 1 ||
		progress.Phase != state.PublicationDrainEventFallback {
		t.Fatalf("progress=(%+v,%v)", progress, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='commit-2',published_ts=15
WHERE seq=?`, events[1].Seq); err != nil {
		t.Fatal(err)
	}
	completed, err := UpdatePublicationDrainAfterReplay(
		ctx, db, progress, ReplaySummary{Published: 1}, nil,
		time.Unix(15, 0).UTC())
	if err != nil || completed.Phase != state.PublicationDrainCompleted ||
		completed.PublishedEventCount != 2 || completed.CommitCount != 2 {
		t.Fatalf("completed=(%+v,%v)", completed, err)
	}
	if active, err := ActivePublicationDrainForPair(
		ctx, db, "refs/heads/main", 7); err != nil || active != nil {
		t.Fatalf("active after completion=(%+v,%v)", active, err)
	}
}

func TestPublicationDrainTerminalBarrierNeedsAction(t *testing.T) {
	ctx := context.Background()
	db, events, drain := openPublicationDrainTestState(t, 1, 1)
	update := PublicationDrainUpdateFrom(drain, 11, 10)
	update.Phase = state.PublicationDrainEventFallback
	update.EventFallbackCount = 1
	update.FallbackMode = "atomic_dependency_components"
	drain, err := state.AdvancePublicationDrain(ctx, db, drain.ID, update)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='failed',error='missing object' WHERE seq=?`,
		events[0].Seq); err != nil {
		t.Fatal(err)
	}
	blocked, err := UpdatePublicationDrainAfterReplay(ctx, db, drain,
		ReplaySummary{Failed: 1}, nil, time.Unix(12, 0).UTC())
	if err != nil || blocked.Phase != state.PublicationDrainNeedsAction {
		t.Fatalf("blocked=(%+v,%v)", blocked, err)
	}
}

func TestPublicationDrainRecoveredTargetIsResolved(t *testing.T) {
	ctx := context.Background()
	db, events, drain := openPublicationDrainTestState(t, 1, 1)
	update := PublicationDrainUpdateFrom(drain, 11, 10)
	update.Phase = state.PublicationDrainSemantic
	drain, err := state.AdvancePublicationDrain(ctx, db, drain.ID, update)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='recovered',commit_oid='archive-proof' WHERE seq=?`,
		events[0].Seq); err != nil {
		t.Fatal(err)
	}
	completed, err := UpdatePublicationDrainAfterReplay(
		ctx, db, drain, ReplaySummary{}, nil, time.Unix(12, 0).UTC())
	if err != nil || completed.Phase != state.PublicationDrainCompleted ||
		completed.PublishedEventCount != 1 {
		t.Fatalf("completed=(%+v,%v), want completed resolved=1", completed, err)
	}
}

func TestPublicationDrainAcceptsLongJournalProvenHeadAdvance(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	commit := func(body, message string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(repo, "owned.txt"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "owned.txt"); err != nil {
			t.Fatal(err)
		}
		if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
			"-c", "user.name=ACD Test", "-c", "user.email=acd@test.invalid",
			"commit", "-m", message); err != nil {
			t.Fatal(err)
		}
		out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "rev-parse", "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	base := commit("base\n", "base")
	db, events, drain := openPublicationDrainTestState(t, 70, 1)
	previous := base
	for i := 0; i < 65; i++ {
		target := commit(strings.Repeat("owned\n", i+1), "owned")
		treeOutput, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
			"rev-parse", target+"^{tree}")
		if err != nil {
			t.Fatal(err)
		}
		publication := state.SelfPublication{
			ID:        fmt.Sprintf("sp-owned-%d", i),
			BranchRef: drain.BranchRef, BranchGeneration: drain.BranchGeneration,
			SourceHead: previous, TargetCommitOID: target,
			TargetTreeOID: strings.TrimSpace(string(treeOutput)),
			Members:       []state.SelfPublicationMember{{EventSeq: events[i].Seq}},
			Completion: state.SelfPublicationCompletion{
				PublishedTS: float64(i + 11),
			},
		}
		if created, err := state.PrepareSelfPublication(
			ctx, db, publication); err != nil || !created {
			t.Fatalf("PrepareSelfPublication %d=(%t,%v)", i, created, err)
		}
		if changed, err := state.MarkSelfPublicationGitApplied(
			ctx, db, publication, float64(i+11)); err != nil || !changed {
			t.Fatalf("MarkSelfPublicationGitApplied %d=(%t,%v)", i, changed, err)
		}
		if completed, err := state.CompleteSelfPublication(
			ctx, db, publication, publication.Completion); err != nil || !completed {
			t.Fatalf("CompleteSelfPublication %d=(%t,%v)", i, completed, err)
		}
		previous = target
	}
	if safe, err := publicationDrainOwnsHeadAdvance(
		ctx, repo, db, drain, base, previous); err != nil || !safe {
		t.Fatalf("owned advance=(%t,%v), want true,nil", safe, err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		`UPDATE checkpoints SET observed_head=? WHERE id=?`,
		base, drain.CheckpointID); err != nil {
		t.Fatal(err)
	}
	blockedUpdate := PublicationDrainUpdateFrom(drain, 100, drain.LastProgressTS)
	blockedUpdate.Phase = state.PublicationDrainNeedsAction
	blockedUpdate.LastError = fmt.Sprintf(
		publicationDrainHeadChangedPrefix+" observed=%s current=%s", base, previous)
	blocked, err := state.AdvancePublicationDrain(ctx, db, drain.ID, blockedUpdate)
	if err != nil {
		t.Fatal(err)
	}
	restartable, err := RestartablePublicationDrainForPair(
		ctx, db, drain.BranchRef, drain.BranchGeneration)
	if err != nil || restartable == nil || restartable.ID != drain.ID {
		t.Fatalf("restartable=(%+v,%v), want %s", restartable, err, drain.ID)
	}
	resumed, err := ResumePublicationDrainCheckpointing(
		ctx, repo, db, blocked, time.Unix(101, 0))
	if err != nil || resumed.Phase != state.PublicationDrainSemantic {
		t.Fatalf("resumed=(phase=%q err=%v), want semantic,nil", resumed.Phase, err)
	}
	externalHead := commit("external\n", "external")
	if safe, err := publicationDrainOwnsHeadAdvance(
		ctx, repo, db, drain, base, externalHead); err != nil || safe {
		t.Fatalf("external advance=(%t,%v), want false,nil", safe, err)
	}
}

func openPublicationDrainTestState(
	t *testing.T,
	eventCount int,
	targetCount int,
) (*state.DB, []state.CaptureEvent, state.PublicationDrain) {
	t.Helper()
	ctx := context.Background()
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var events []state.CaptureEvent
	checkpoint := state.Checkpoint{
		ID:               "cp-1786487000000-0123456789abcdef",
		OperationID:      "op-publication-drain-daemon-test",
		WorktreeID:       "0123456789abcdef",
		Reason:           state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: "head",
		ObservedRef: "refs/heads/main", TreeOID: "tree", CommitOID: "commit",
		Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786487000000-0123456789abcdef",
		CreatedTS: 1,
	}
	for i := 0; i < eventCount; i++ {
		result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,'head','modify',?,'exact',?,'pending')`,
			string(rune('a'+i))+".go", i+1)
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, state.CaptureEvent{
			Seq: seq, BranchRef: "refs/heads/main", BranchGeneration: 7,
			BaseHead: "head", Operation: "modify",
			Path: string(rune('a'+i)) + ".go", Fidelity: "exact",
			State: state.EventStatePending,
		})
		if i < targetCount {
			checkpoint.EventSeqs = append(checkpoint.EventSeqs, seq)
		}
	}
	if created, err := state.PrepareCheckpoint(
		ctx, db, checkpoint, publicationDrainTestDigest); err != nil || !created {
		t.Fatalf("prepare checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	drain := state.PublicationDrain{
		ID: "drain-daemon-test", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: state.PublicationDrainCheckpointing,
		TargetEventCount: int64(targetCount), CreatedTS: 10,
		UpdatedTS: 10, LastProgressTS: 10,
	}
	if created, err := state.PreparePublicationDrain(
		ctx, db, drain); err != nil || !created {
		t.Fatalf("prepare drain=(%t,%v)", created, err)
	}
	drain.EventSeqs = append([]int64(nil), checkpoint.EventSeqs...)
	return db, events, drain
}

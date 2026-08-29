package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
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
	filtered, err = publicationDrainAtomicFallbackWindow(ctx, db, CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 7,
	}, filtered)
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

func TestPublicationDrainFinalFallbackIgnoresPublishedDependencyCapacity(t *testing.T) {
	ctx := context.Background()
	db, events, _ := openPublicationDrainTestState(t, 4, 4)
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published', published_ts=50, commit_oid='old-commit'
WHERE seq IN (?, ?)`, events[0].Seq, events[1].Seq); err != nil {
		t.Fatal(err)
	}
	stale := make([]state.IntentCaptureDependency, 0,
		state.IntentDependencyMaxPerPair)
	for i := 0; i < state.IntentDependencyMaxPerPair; i++ {
		stale = append(stale, state.IntentCaptureDependency{
			BranchRef: "refs/heads/main", BranchGeneration: 7,
			PrerequisiteSeq: events[0].Seq, DependentSeq: events[1].Seq,
			Strength: string(ai.IntentDependencySoft),
			Kind:     fmt.Sprintf("stale_%04d", i),
		})
	}
	if err := state.ReplaceIntentCaptureDependencies(
		ctx, db, "refs/heads/main", 7, stale); err != nil {
		t.Fatal(err)
	}
	events[2].Path = "active.go"
	events[3].Path = "active.go"
	window, err := publicationDrainAtomicFallbackWindow(ctx, db, CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 7,
	}, events[2:])
	if err != nil {
		t.Fatal(err)
	}
	if len(window) != 2 {
		t.Fatalf("atomic fallback window=%d, want active hard component", len(window))
	}
}

func TestPublicationDrainLocalUnlockSelectsSmallestHardComponent(t *testing.T) {
	ctx := context.Background()
	db, events, _ := openPublicationDrainTestState(t, 3, 3)
	if err := state.ReplaceIntentCaptureDependencies(ctx, db,
		"refs/heads/main", 7, []state.IntentCaptureDependency{{
			BranchRef: "refs/heads/main", BranchGeneration: 7,
			PrerequisiteSeq: events[0].Seq, DependentSeq: events[1].Seq,
			Strength: string(ai.IntentDependencyHard), Kind: "test_source",
		}}); err != nil {
		t.Fatal(err)
	}
	window, err := publicationDrainAtomicFallbackWindow(ctx, db, CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 7,
	}, events)
	if err != nil {
		t.Fatal(err)
	}
	if len(window) != 1 || window[0].Seq != events[2].Seq {
		t.Fatalf("unlock=%+v, want smallest singleton %d", window, events[2].Seq)
	}
}

func TestIntentForwardRecoveryPrefixFollowsSemanticTopology(t *testing.T) {
	events := []state.CaptureEvent{
		{Seq: 10}, {Seq: 20}, {Seq: 30},
	}
	plan := ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{CandidateID: "prerequisite", SelectedSeqs: []int64{30}},
			{
				CandidateID: "dependent", SelectedSeqs: []int64{10},
				DependsOnCandidates: []string{"prerequisite"},
			},
			{
				CandidateID: "final", SelectedSeqs: []int64{20},
				DependsOnCandidates: []string{"dependent"},
			},
		},
	}
	for _, test := range []struct {
		cursor int
		want   []int64
	}{
		{cursor: 1, want: []int64{30}},
		{cursor: 2, want: []int64{10, 30}},
		{cursor: 4, want: []int64{10, 20, 30}},
	} {
		prefix, err := selectIntentForwardRecoveryPrefix(
			plan, events, test.cursor)
		if err != nil {
			t.Fatalf("cursor %d: %v", test.cursor, err)
		}
		got := make([]int64, 0, len(prefix.Events))
		for _, event := range prefix.Events {
			got = append(got, event.Seq)
		}
		if !reflect.DeepEqual(got, test.want) ||
			prefix.CandidateCount != min(test.cursor, 3) ||
			prefix.RemainingGroups != 3 {
			t.Fatalf("cursor %d prefix=(%v,%d/%d) want %v",
				test.cursor, got, prefix.CandidateCount,
				prefix.RemainingGroups, test.want)
		}
	}
}

func TestResolvedIntentForwardRecoveryPlanUsesStoredMembership(t *testing.T) {
	for _, test := range []struct {
		name        string
		candidates  func([]state.CaptureEvent) []ai.IntentCandidateAssignment
		wantValid   bool
		wantPartial bool
	}{
		{
			name: "resolved target member predates plan",
			candidates: func(events []state.CaptureEvent) []ai.IntentCandidateAssignment {
				return []ai.IntentCandidateAssignment{
					semanticPlanTestCandidate("remaining-a", events[1].Seq),
					semanticPlanTestCandidate("remaining-b", events[2].Seq),
				}
			},
			wantValid: true,
		},
		{
			name: "stored candidate partially resolved",
			candidates: func(events []state.CaptureEvent) []ai.IntentCandidateAssignment {
				return []ai.IntentCandidateAssignment{
					semanticPlanTestCandidate(
						"partial", events[0].Seq, events[1].Seq),
					semanticPlanTestCandidate("remaining", events[2].Seq),
				}
			},
			wantPartial: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db, events, _ := openPublicationDrainTestState(t, 3, 3)
			if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published',published_ts=1,commit_oid='already-published'
WHERE seq=?`, events[0].Seq); err != nil {
				t.Fatal(err)
			}
			fingerprint := "stored-membership-" + strings.ReplaceAll(
				test.name, " ", "-")
			plan := ai.IntentPlanV2{
				ProtocolVersion: ai.IntentPlannerProtocolV2,
				Candidates:      test.candidates(events),
			}
			run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{
				Fingerprint: fingerprint, BranchRef: "refs/heads/main",
				BranchGeneration: 7, AttemptLimit: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := storeResolvedIntentPlanRun(&run, plan, nil); err != nil {
				t.Fatal(err)
			}
			run.Completed = true
			if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
				t.Fatal(err)
			}
			loaded, valid, partial, err := resolvedIntentForwardRecoveryPlan(
				ctx, db, state.IntentForwardRecovery{
					BranchRef: "refs/heads/main", BranchGeneration: 7,
					PlanFingerprint: fingerprint,
					TargetEventSeqs: []int64{
						events[0].Seq, events[1].Seq, events[2].Seq,
					},
				})
			if err != nil {
				t.Fatal(err)
			}
			if valid != test.wantValid || partial != test.wantPartial {
				t.Fatalf("loaded=(%+v valid=%t partial=%t)",
					loaded, valid, partial)
			}
		})
	}
}

func semanticPlanTestCandidate(
	id string,
	seqs ...int64,
) ai.IntentCandidateAssignment {
	return ai.IntentCandidateAssignment{
		CandidateID: id, SelectedSeqs: seqs,
		Purpose:   "preserve the provider semantic group",
		Readiness: ai.IntentCandidateReady,
		Subject:   "Preserve semantic group",
		GroupingReason: "the provider selected this exact reviewable " +
			"membership",
	}
}

func TestConfigureIntentSalvageHonorsProviderProbeWindow(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	health := &IntentPlannerHealth{
		state:   IntentPlannerCircuitOpen,
		retryAt: now.Add(time.Minute),
		now:     func() time.Time { return now },
	}
	cfg := intentReplayConfig{}
	configureIntentSalvage(&cfg, health,
		publicationFallbackSemanticReplan, []int64{1, 2})
	if !cfg.atomicFallback || cfg.semanticSalvage || !cfg.bypassBatchWait ||
		cfg.window != state.IntentCandidateMaxCaptures {
		t.Fatalf("open circuit config=%+v, want local unlock", cfg)
	}

	health.retryAt = now
	cfg = intentReplayConfig{}
	configureIntentSalvage(&cfg, health,
		publicationFallbackLocalUnlock, []int64{1, 2})
	if cfg.atomicFallback || !cfg.semanticSalvage || !cfg.bypassBatchWait ||
		cfg.window != 2 ||
		!reflect.DeepEqual(cfg.targetEventSeqs, []int64{1, 2}) {
		t.Fatalf("half-open config=%+v, want semantic replan", cfg)
	}
}

func TestConfigureAtomicIntentFallbackPreservesSemanticProvider(t *testing.T) {
	planner := &recoveringPublicationDrainPlanner{}
	health := &IntentPlannerHealth{}
	cfg := intentReplayConfig{
		planner: planner, health: health,
		plannerProvider: planner.Name(), plannerModel: "semantic-model",
		commitFormat: ai.CommitFormatImperative,
	}

	configureAtomicIntentFallback(&cfg)

	wrapped, ok := cfg.planner.(publicationDrainAtomicFallbackPlanner)
	if !ok {
		t.Fatalf("planner=%T, want atomic fallback wrapper", cfg.planner)
	}
	if wrapped.messagePlanner != planner || !wrapped.requireSemanticMessage {
		t.Fatalf("wrapper=%+v, want configured semantic planner", wrapped)
	}
	if cfg.plannerProvider != planner.Name() ||
		cfg.plannerModel != "semantic-model" || cfg.health != health {
		t.Fatalf("provider identity or health changed: %+v", cfg)
	}
}

func TestConfigureAtomicIntentFallbackAllowsExplicitDeterministicMessages(
	t *testing.T,
) {
	cfg := intentReplayConfig{
		planner: ai.DeterministicProvider{}, plannerProvider: "deterministic",
	}
	configureAtomicIntentFallback(&cfg)
	planner := cfg.planner.(publicationDrainAtomicFallbackPlanner)
	if planner.requireSemanticMessage {
		t.Fatal("explicit deterministic provider unexpectedly requires rewrite")
	}
	plan, err := planner.PlanIntentV2(context.Background(), ai.IntentPlanRequestV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		OfferedCaptures: []ai.OfferedCapture{{
			Seq: 1, Path: "replay.go", Op: "modify",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Candidates) != 1 ||
		plan.Candidates[0].Subject != "Update replay.go" {
		t.Fatalf("plan=%+v", plan)
	}
}

func TestConfigureIntentForwardRecoveryPreservesPathQuiescence(t *testing.T) {
	cfg := intentReplayConfig{pathQuiescence: 30 * time.Second}
	configureIntentForwardRecovery(&cfg, state.IntentForwardRecovery{
		Stage: publicationFallbackSemanticReplan, TargetEventSeqs: []int64{1, 2},
	}, nil)
	if cfg.pathQuiescence != 30*time.Second || !cfg.semanticSalvage ||
		cfg.atomicFallback {
		t.Fatalf("forward recovery config=%+v", cfg)
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

func TestPublicationDrainAutomaticallyRecoversSupersededCandidateIDCollision(
	t *testing.T,
) {
	ctx := context.Background()
	db, events, drain := openPublicationDrainTestState(t, 1, 1)
	candidate := state.IntentCandidate{
		ID: "terminal-collision", BranchRef: drain.BranchRef,
		BranchGeneration: drain.BranchGeneration,
		Status:           state.IntentCandidateReady, Readiness: state.IntentReadinessReady,
		Events: []state.IntentCandidateEvent{{
			EventSeq: events[0].Seq, EventRole: "change",
		}},
	}
	if err := state.SaveIntentCandidate(ctx, db, candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE intent_candidate_events SET membership_state='superseded'
WHERE candidate_id=?`, candidate.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		`UPDATE intent_candidates SET status='superseded' WHERE id=?`,
		candidate.ID); err != nil {
		t.Fatal(err)
	}
	semantic := PublicationDrainUpdateFrom(drain, 11, 10)
	semantic.Phase = state.PublicationDrainSemantic
	loaded, err := state.AdvancePublicationDrain(ctx, db, drain.ID, semantic)
	if err != nil {
		t.Fatal(err)
	}
	blocked := PublicationDrainUpdateFrom(loaded, 12, 10)
	blocked.Phase = state.PublicationDrainNeedsAction
	blocked.LastError = supersededCandidateDrainErrorPrefix + candidate.ID +
		supersededCandidateDrainErrorSuffix
	if _, err := state.AdvancePublicationDrain(ctx, db, drain.ID, blocked); err != nil {
		t.Fatal(err)
	}
	recovered, err := RecoverSupersededCandidatePublicationDrain(
		ctx, db, drain.BranchRef, drain.BranchGeneration, time.Unix(13, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if recovered == nil || recovered.Phase != state.PublicationDrainCheckpointing ||
		recovered.LastError != "" {
		t.Fatalf("recovered drain=%+v", recovered)
	}
}

func TestPublicationDrainLocalUnlockReturnsToIntentPlanner(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"first.txt":  "first\n",
		"second.txt": "second\n",
	}
	for path, contents := range want {
		if err := os.WriteFile(filepath.Join(f.dir, path),
			[]byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	drain := state.PublicationDrain{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Phase: state.PublicationDrainEventFallback, TargetEventCount: 2,
		FallbackMode: publicationFallbackLocalUnlock,
	}
	for _, event := range pending {
		drain.EventSeqs = append(drain.EventSeqs, event.Seq)
	}
	planner := &recoveringPublicationDrainPlanner{}
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
		PublicationDrain: &drain,
	}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || first.Published != 1 || planner.calls != 0 ||
		planner.rewriteCalls != 1 {
		t.Fatalf("first fallback=%+v planner_calls=%d rewrite_calls=%d err=%v",
			first, planner.calls, planner.rewriteCalls, err)
	}
	if len(planner.rewriteRequests) != 1 ||
		len(planner.rewriteRequests[0].LockedPlan.SelectedSeqs) != 1 {
		t.Fatalf("locked rewrite requests=%+v", planner.rewriteRequests)
	}
	if subject := strings.TrimSpace(mustGitOutput(
		t, f.dir, "show", "-s", "--format=%s", "HEAD",
	)); subject != "Publish safe dependency group" {
		t.Fatalf("local unlock subject=%q", subject)
	}
	f.cctx.BaseHead = first.BaseHead
	drain.FallbackMode = publicationFallbackSemanticReplan
	semanticPlanner := &recoveringPublicationDrainPlanner{}
	opts.IntentPlanner = semanticPlanner
	second, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || second.Published != 1 || semanticPlanner.calls != 1 {
		t.Fatalf("semantic replan=%+v provider_calls=%d err=%v",
			second, semanticPlanner.calls, err)
	}
	for path, contents := range want {
		got, err := os.ReadFile(filepath.Join(f.dir, path))
		if err != nil || string(got) != contents {
			t.Fatalf("worktree %s=%q err=%v want=%q", path, got, err, contents)
		}
	}
	remaining, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("remaining=%+v err=%v", remaining, err)
	}
}

func TestPublicationDrainLocalUnlockWaitsForSemanticMessage(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "replay.go"),
		[]byte("package replay\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	drain := state.PublicationDrain{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Phase: state.PublicationDrainEventFallback, TargetEventCount: 1,
		FallbackMode: publicationFallbackLocalUnlock,
		EventSeqs:    []int64{pending[0].Seq},
	}
	planner := &recoveringPublicationDrainPlanner{
		rewriteErr: errors.New("semantic provider unavailable"),
	}
	retryLimit := 0
	before := f.cctx.BaseHead
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentRetryLimit: &retryLimit, IntentBypassBatchWait: true,
		IntentWindow: 10, PublicationDrain: &drain,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Published != 0 || !sum.Skipped ||
		sum.SkippedReason != "intent_v2_waiting_message_rewrite" ||
		sum.Disposition != ReplayDispositionTransientWait || !sum.HasMore {
		t.Fatalf("summary=%+v, want retryable semantic-message wait", sum)
	}
	if planner.calls != 0 || planner.rewriteCalls == 0 {
		t.Fatalf("planner_calls=%d rewrite_calls=%d",
			planner.calls, planner.rewriteCalls)
	}
	after, err := gitpkg.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("HEAD=%s want unchanged %s", after, before)
	}
	remaining, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 1 ||
		remaining[0].Seq != pending[0].Seq {
		t.Fatalf("remaining=%+v err=%v", remaining, err)
	}
}

func TestPublicationDrainSemanticMessageWaitIsTransient(t *testing.T) {
	drain := state.PublicationDrain{Phase: state.PublicationDrainSemantic}
	evaluation := IntentCandidateEvaluationResult{
		Fallback: "waiting_message_rewrite",
	}
	if !publicationDrainMessageRewriteWait(
		ReplayOpts{PublicationDrain: &drain}, intentReplayConfig{}, evaluation) {
		t.Fatal("semantic drain message wait was not retryable")
	}
	if publicationDrainMessageRewriteWait(
		ReplayOpts{}, intentReplayConfig{}, evaluation) {
		t.Fatal("ordinary candidate message failure became an unbounded wait")
	}
}

func TestPublicationDrainSemanticExcludesCandidateBeyondFrozenTarget(
	t *testing.T,
) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	captureOnePendingFile(t, ctx, f, "target.txt", "target\n")
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("target pending=%+v err=%v", pending, err)
	}
	target := pending[0]
	captureOnePendingFile(t, ctx, f, "later.txt", "later\n")
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("all pending=%+v err=%v", pending, err)
	}
	later := pending[1]

	const mixedCandidateID = "candidate-spanning-frozen-target"
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: mixedCandidateID, BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		Status:           state.IntentCandidateReady,
		Purpose:          "candidate includes a later edit",
		Readiness:        state.IntentReadinessReady,
		Events: []state.IntentCandidateEvent{
			{EventSeq: target.Seq, EventRole: "code"},
			{EventSeq: later.Seq, EventRole: "code"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	planner := &recoveringPublicationDrainPlanner{}
	drain := state.PublicationDrain{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Phase: state.PublicationDrainSemantic, TargetEventCount: 1,
		EventSeqs: []int64{target.Seq},
	}
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
		IntentRepairEnabled: true, PublicationDrain: &drain,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Published != 1 || planner.calls != 1 {
		t.Fatalf("summary=%+v planner_calls=%d, want one target publication",
			sum, planner.calls)
	}
	if len(planner.requests) != 1 {
		t.Fatalf("planner requests=%d, want one", len(planner.requests))
	}
	for _, candidate := range planner.requests[0].Candidates {
		if candidate.CandidateID == mixedCandidateID {
			t.Fatalf("planner reused candidate beyond frozen target: %+v", candidate)
		}
	}
	remaining, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 1 || remaining[0].Seq != later.Seq {
		t.Fatalf("remaining=%+v err=%v, want only later event %d",
			remaining, err, later.Seq)
	}
	if oid, err := gitpkg.LsTreeBlobOID(
		ctx, f.dir, "HEAD", "target.txt"); err != nil || oid == "" {
		t.Fatalf("target at HEAD=%q err=%v", oid, err)
	}
	if oid, err := gitpkg.LsTreeBlobOID(
		ctx, f.dir, "HEAD", "later.txt"); err != nil || oid != "" {
		t.Fatalf("later at HEAD=%q err=%v, want absent", oid, err)
	}
}

func TestPublicationDrainLocalUnlockRetiresOnlyOverlappingCandidates(t *testing.T) {
	ctx := context.Background()
	db, events, drain := openPublicationDrainTestState(t, 2, 2)
	for index, id := range []string{"overlap", "unrelated"} {
		candidate := state.IntentCandidate{
			ID: id, BranchRef: drain.BranchRef,
			BranchGeneration: drain.BranchGeneration,
			Status:           state.IntentCandidateReady, Readiness: state.IntentReadinessReady,
			Events: []state.IntentCandidateEvent{{
				EventSeq: events[index].Seq, EventRole: "change",
			}},
		}
		if err := state.SaveIntentCandidate(ctx, db, candidate); err != nil {
			t.Fatal(err)
		}
	}
	if err := retireIntentCandidatesForFallbackEvents(ctx, db, CaptureContext{
		BranchRef: drain.BranchRef, BranchGeneration: drain.BranchGeneration,
	}, []state.CaptureEvent{events[0]}); err != nil {
		t.Fatal(err)
	}
	overlap, _, err := state.IntentCandidateByID(ctx, db, "overlap")
	if err != nil || overlap.Status != state.IntentCandidateSuperseded {
		t.Fatalf("overlap=(%+v,%v)", overlap, err)
	}
	unrelated, _, err := state.IntentCandidateByID(ctx, db, "unrelated")
	if err != nil || unrelated.Status != state.IntentCandidateReady {
		t.Fatalf("unrelated=(%+v,%v)", unrelated, err)
	}
}

type forbiddenPublicationDrainPlanner struct {
	calls int
}

type recoveringPublicationDrainPlanner struct {
	calls           int
	rewriteCalls    int
	requests        []ai.IntentPlanRequestV2
	rewriteRequests []ai.IntentMessageRewriteRequest
	rewriteErr      error
}

func (*recoveringPublicationDrainPlanner) Name() string {
	return "recovering-publication-drain-provider"
}

func (p *recoveringPublicationDrainPlanner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	p.calls++
	return ai.IntentPlan{}, errors.New("expected Intent v2 planning")
}

func (p *recoveringPublicationDrainPlanner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	p.requests = append(p.requests, req)
	selected := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		selected = append(selected, capture.Seq)
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "semantic-after-unlock", SelectedSeqs: selected,
			Purpose: "finish the remaining intent", Readiness: ai.IntentCandidateReady,
			Subject:        "Finish remaining intent",
			GroupingReason: "the remaining changes share one purpose",
		}},
	}, nil
}

func (p *recoveringPublicationDrainPlanner) RewriteIntentMessage(
	_ context.Context,
	req ai.IntentMessageRewriteRequest,
) (ai.Result, error) {
	p.rewriteCalls++
	p.rewriteRequests = append(p.rewriteRequests, req)
	if p.rewriteErr != nil {
		return ai.Result{}, p.rewriteErr
	}
	return ai.Result{
		Subject: "Publish safe dependency group",
		Source:  p.Name(),
	}, nil
}

func (*forbiddenPublicationDrainPlanner) Name() string {
	return "forbidden-publication-drain-provider"
}

func (p *forbiddenPublicationDrainPlanner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	p.calls++
	return ai.IntentPlan{}, errors.New("provider must not run during fallback")
}

func (p *forbiddenPublicationDrainPlanner) PlanIntentV2(
	context.Context,
	ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	return ai.IntentPlanV2{}, errors.New("provider must not run during fallback")
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
		fallback.EventFallbackCount != 0 ||
		fallback.FallbackMode != publicationFallbackSemanticReplan {
		t.Fatalf("fallback=%+v", fallback)
	}
	unlock, err := UpdatePublicationDrainAfterReplay(
		ctx, db, fallback, ReplaySummary{
			Disposition:       ReplayDispositionRecoverableStall,
			DispositionReason: "provider plan remained invalid",
		}, nil, time.Unix(13, 0).UTC())
	if err != nil || unlock.FallbackMode != publicationFallbackLocalUnlock ||
		unlock.EventFallbackCount != 1 {
		t.Fatalf("unlock=(%+v,%v)", unlock, err)
	}

	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='commit-1',published_ts=14
WHERE seq=?`, events[0].Seq); err != nil {
		t.Fatal(err)
	}
	progress, err := UpdatePublicationDrainAfterReplay(
		ctx, db, unlock, ReplaySummary{Published: 1}, nil,
		time.Unix(14, 0).UTC())
	if err != nil || progress.PublishedEventCount != 1 ||
		progress.Phase != state.PublicationDrainEventFallback ||
		progress.FallbackMode != publicationFallbackSemanticReplan {
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

func TestPublicationDrainOpenCircuitKeepsLocalUnlockMode(t *testing.T) {
	ctx := context.Background()
	db, events, drain := openPublicationDrainTestState(t, 2, 2)
	update := PublicationDrainUpdateFrom(drain, 11, 10)
	update.Phase = state.PublicationDrainEventFallback
	update.FallbackMode = publicationFallbackLocalUnlock
	drain, err := state.AdvancePublicationDrain(ctx, db, drain.ID, update)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='unlock',published_ts=12
WHERE seq=?`, events[0].Seq); err != nil {
		t.Fatal(err)
	}
	continued, err := UpdatePublicationDrainAfterReplay(
		ctx, db, drain, ReplaySummary{
			Published: 1, RecoveryMode: publicationFallbackLocalUnlock,
			PlannerCircuitOpen: true,
		}, nil, time.Unix(12, 0).UTC())
	if err != nil || continued.FallbackMode != publicationFallbackLocalUnlock ||
		continued.EventFallbackCount != drain.EventFallbackCount {
		t.Fatalf("continued local unlock=(%+v,%v)", continued, err)
	}
}

func TestPublicationDrainUnknownRuntimeContractNeedsAction(t *testing.T) {
	ctx := context.Background()
	db, _, drain := openPublicationDrainTestState(t, 1, 1)
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE publication_drains
SET commit_strategy='',commit_format='',config_revision_id=0,
    provider='',provider_model='',provider_fingerprint=''
WHERE id=?`, drain.ID); err != nil {
		t.Fatal(err)
	}
	drain, err := state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &RuntimeBundle{
		Provider:       &runtimeTestProvider{name: "deterministic"},
		CommitStrategy: ai.CommitStrategyEvent,
		CommitFormat:   ai.CommitFormatImperative,
	}
	reason := publicationDrainRuntimeBlock(drain, bundle)
	if reason != "publication_drain_runtime_contract_unavailable" {
		t.Fatalf("runtime block=%q", reason)
	}
	failed, err := failPublicationDrainRuntimeContract(
		ctx, db, drain, reason, time.Unix(12, 0).UTC())
	if err != nil || failed.Phase != state.PublicationDrainNeedsAction ||
		failed.LastError != reason {
		t.Fatalf("failed drain=%+v err=%v", failed, err)
	}
	barrier, err := PublicationDrainBarrierForPair(
		ctx, db, drain.BranchRef, drain.BranchGeneration)
	if err != nil || barrier == nil || barrier.ID != drain.ID ||
		barrier.Phase != state.PublicationDrainNeedsAction {
		t.Fatalf("needs-action replay barrier=%+v err=%v", barrier, err)
	}
	if active, err := ActivePublicationDrainForPair(
		ctx, db, drain.BranchRef, drain.BranchGeneration); err != nil || active != nil {
		t.Fatalf("active lookup=%+v err=%v", active, err)
	}
	second := drain
	second.ID = "second-drain-daemon-test"
	second.Phase = state.PublicationDrainCheckpointing
	second.LastError = ""
	second.UpdatedTS = 13
	if created, err := state.PreparePublicationDrain(ctx, db, second); created || !errors.Is(err, state.ErrPublicationDrainBarrier) {
		t.Fatalf("second drain across needs-action barrier=(%t,%v)", created, err)
	}
}

func TestPublicationDrainConvergingRuntimeMismatchStaysActive(t *testing.T) {
	ctx := context.Background()
	db, _, drain := openPublicationDrainTestState(t, 1, 1)
	drain, err := state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	reason := publicationDrainRuntimeBlock(drain, &RuntimeBundle{
		RevisionID:     99,
		Provider:       &runtimeTestProvider{name: "deterministic"},
		CommitStrategy: ai.CommitStrategyEvent,
		CommitFormat:   ai.CommitFormatImperative,
	})
	if reason != "publication_drain_runtime_revision_mismatch" {
		t.Fatalf("runtime block=%q", reason)
	}
	if _, err := failPublicationDrainRuntimeContract(
		ctx, db, drain, reason, time.Unix(12, 0).UTC()); err == nil {
		t.Fatal("converging mismatch unexpectedly terminalized")
	}
	loaded, err := state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil || loaded.Phase != state.PublicationDrainCheckpointing {
		t.Fatalf("active drain=%+v err=%v", loaded, err)
	}
}

func TestPublicationDrainEnvironmentRuntimeChangeNeedsAction(t *testing.T) {
	ctx := context.Background()
	db, _, drain := openPublicationDrainTestState(t, 1, 1)
	drain, err := state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	reason := publicationDrainRuntimeBlock(drain, &RuntimeBundle{
		Provider:       &runtimeTestProvider{name: "openai-compat"},
		CommitStrategy: ai.CommitStrategyIntent,
		CommitFormat:   ai.CommitFormatImperative,
		HealthIdentity: IntentPlannerProviderIdentity{Provider: "openai-compat"},
	})
	terminalReason := publicationDrainTerminalRuntimeReason(drain, reason)
	if terminalReason != "publication_drain_environment_runtime_changed" {
		t.Fatalf("terminal runtime reason=%q block=%q", terminalReason, reason)
	}
	failed, err := failPublicationDrainRuntimeContract(
		ctx, db, drain, terminalReason, time.Unix(12, 0).UTC())
	if err != nil || failed.Phase != state.PublicationDrainNeedsAction ||
		failed.LastError != terminalReason {
		t.Fatalf("failed environment drain=%+v err=%v", failed, err)
	}
}

func TestPublicationDrainNoProgressEscalatesWithMorePending(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, _, drain := openPublicationDrainTestState(t, 152, 152)
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
		ctx, db, loaded, ReplaySummary{
			HasMore:           true,
			Skipped:           true,
			SkippedReason:     "intent_v2_forward_recovery_repair_horizon_expired",
			Disposition:       ReplayDispositionRecoverableStall,
			DispositionReason: "repair_horizon_expired",
		}, nil, time.Unix(12, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Phase != state.PublicationDrainNormalizing ||
		normalized.SemanticRebuildAttempts != 1 ||
		normalized.FallbackMode != "deterministic_semantic" ||
		normalized.LastError != "repair_horizon_expired" {
		t.Fatalf("normalized=%+v", normalized)
	}
}

func TestReplayDispositionSeparatesTransientWaitsFromStalls(t *testing.T) {
	for _, tc := range []struct {
		name string
		sum  ReplaySummary
		want ReplayDisposition
	}{
		{name: "batch wait", sum: ReplaySummary{
			Skipped: true, SkippedReason: "skipped_due_intent_batch_wait",
		}, want: ReplayDispositionTransientWait},
		{name: "settle window", sum: ReplaySummary{
			Skipped: true, SkippedReason: "skipped_due_intent_settle_window",
		}, want: ReplayDispositionTransientWait},
		{name: "expired repair", sum: ReplaySummary{
			Skipped:       true,
			SkippedReason: "intent_v2_forward_recovery_repair_horizon_expired",
		}, want: ReplayDispositionRecoverableStall},
		{name: "published", sum: ReplaySummary{
			Published: 1,
		}, want: ReplayDispositionProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classifyReplayDisposition(&tc.sum, nil)
			if tc.sum.Disposition != tc.want {
				t.Fatalf("disposition=%s want=%s summary=%+v",
					tc.sum.Disposition, tc.want, tc.sum)
			}
		})
	}
}

func TestPublicationDrainFallbackNoProgressNeedsAttention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, _, drain := openPublicationDrainTestState(t, 2, 2)
	update := PublicationDrainUpdateFrom(drain, 11, 10)
	update.Phase = state.PublicationDrainEventFallback
	update.EventFallbackCount = 1
	if _, err := state.AdvancePublicationDrain(ctx, db, drain.ID, update); err != nil {
		t.Fatal(err)
	}
	loaded, err := state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := UpdatePublicationDrainAfterReplay(
		ctx, db, loaded, ReplaySummary{
			Disposition:       ReplayDispositionRecoverableStall,
			DispositionReason: "atomic dependency component made no progress",
		}, nil, time.Unix(12, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Phase != state.PublicationDrainNeedsAction ||
		blocked.LastError != "atomic dependency component made no progress" {
		t.Fatalf("blocked=%+v", blocked)
	}
}

func TestPublicationDrainPreflightFailureUsesLocalRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, _, drain := openPublicationDrainTestState(t, 2, 2)
	planRun, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{
		Fingerprint: "sha256:blocked-preflight",
		BranchRef:   drain.BranchRef, BranchGeneration: drain.BranchGeneration,
		AttemptLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	update := PublicationDrainUpdateFrom(drain, 11, 10)
	update.Phase = state.PublicationDrainSemantic
	drain, err = state.AdvancePublicationDrain(ctx, db, drain.ID, update)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := UpdatePublicationDrainAfterReplay(
		ctx, db, drain, ReplaySummary{}, &IntentPlanPreflightError{
			Failure: "open candidate cap exceeded",
		}, time.Unix(12, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Phase != state.PublicationDrainNormalizing ||
		normalized.SemanticRebuildAttempts != 1 ||
		normalized.LastError == "" {
		t.Fatalf("normalized=%+v", normalized)
	}
	var attemptCount int
	err = db.ReadSQL().QueryRowContext(ctx, `
SELECT attempt_count FROM intent_plan_runs WHERE fingerprint=?`,
		planRun.Fingerprint).Scan(&attemptCount)
	if err != nil || attemptCount != 0 {
		t.Fatalf("replay recovery consumed provider attempt: attempts=%d err=%v",
			attemptCount, err)
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
	db, events, drain := openPublicationDrainTestState(t, 70, 65)
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
		ctx, db, drain, base, previous, 1); err != nil || !safe {
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
		ctx, db, drain, base, externalHead, 1); err != nil || safe {
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

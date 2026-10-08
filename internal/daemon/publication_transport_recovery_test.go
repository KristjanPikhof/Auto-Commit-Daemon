package daemon

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type publicationOutagePlanner struct{ intentCandidatePlannerStub }

func (p *publicationOutagePlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, p.err
}

func TestPublicationDrainOversizedFallbackAfter502RemainsRetryable(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 13; i++ {
		writePublicationFile(t, f, fmt.Sprintf("file-%02d.go", i), fmt.Sprintf("package fixture\n// version %d\n", i))
	}
	capture := capturePublicationFiles(t, f)
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 13 {
		t.Fatalf("pending=%d err=%v", len(pending), err)
	}
	var edges []state.IntentCaptureDependency
	var seqs []int64
	for i, event := range pending {
		seqs = append(seqs, event.Seq)
		if i > 0 {
			edges = append(edges, state.IntentCaptureDependency{
				BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
				PrerequisiteSeq: pending[i-1].Seq, DependentSeq: event.Seq,
				Strength: string(ai.IntentDependencyHard), Kind: "object_reference",
			})
		}
	}
	if err := state.ReplaceIntentCaptureDependencies(ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, edges); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ts := float64(now.UnixNano()) / 1e9
	drain := state.PublicationDrain{
		ID: "drain-502", CheckpointID: capture.CheckpointID, WorktreeID: checkpoint.WorktreeID(f.dir),
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Phase: state.PublicationDrainSemantic, TargetEventCount: 13, EventSeqs: seqs,
		CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts,
	}
	if created, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil || !created {
		t.Fatalf("prepare=%t err=%v", created, err)
	}
	writePublicationFile(t, f, "later.md", "Later work stays outside the target.\n")
	capturePublicationFiles(t, f)
	planner := &publicationOutagePlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{err: &ai.ProviderHTTPError{StatusCode: 502, Detail: "Bad Gateway"}}}
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	summary, replayErr := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentHealth: health, IntentPreset: config.PresetBalanced,
		IntentPlannerProvider: "openai-compat", IntentWindow: 20,
		IntentMinPending: 1, IntentBypassBatchWait: true, PublicationDrain: &drain,
	})
	if !isIntentPlannerCircuitWait(replayErr) || summary.Disposition != ReplayDispositionTransientWait || summary.Published != 0 {
		t.Fatalf("outage became terminal: summary=%+v err=%v", summary, replayErr)
	}
	waiting, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, summary, replayErr, time.Now().UTC())
	if err != nil || waiting.Phase != state.PublicationDrainSemantic || waiting.LastError != "" || !reflect.DeepEqual(waiting.EventSeqs, seqs) {
		t.Fatalf("waiting=%+v err=%v", waiting, err)
	}
	remaining, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 14 {
		t.Fatalf("protected captures=%d err=%v", len(remaining), err)
	}
	if head, err := gitpkg.RevParse(ctx, f.dir, "HEAD"); err != nil || head != f.cctx.BaseHead {
		t.Fatalf("outage moved HEAD=%s err=%v", head, err)
	}
}

func TestRecoverTransportWaitPublicationDrainRequiresMatchingProof(t *testing.T) {
	for _, scenario := range []string{"recover", "different-provider", "different-error", "validation", "cooldown", "external-head", "terminal-event", "typed-safety-error"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			repo := t.TempDir()
			initPublicationDrainTestRepo(t, ctx, repo)
			head := commitSingleFile(t, ctx, repo, "", "owned.txt", "base\n", "base")
			if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", "refs/heads/main", head); err != nil {
				t.Fatal(err)
			}
			db, events, drain := openPublicationDrainTestState(t, 2, 1)
			if _, err := db.SQL().ExecContext(ctx, `UPDATE checkpoints SET observed_head=? WHERE id=?`, head, drain.CheckpointID); err != nil {
				t.Fatal(err)
			}
			const fingerprint = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			const failure = "openai-compat: http 502: Bad Gateway"
			if _, err := db.SQL().ExecContext(ctx, `UPDATE publication_drains SET phase='needs_action',last_error=?,commit_strategy='intent',provider='openai-compat',provider_fingerprint=? WHERE id=?`, failure, fingerprint, drain.ID); err != nil {
				t.Fatal(err)
			}
			drain, err := state.PublicationDrainByID(ctx, db, drain.ID)
			if err != nil {
				t.Fatal(err)
			}
			health := intentPlannerHealthRecord{Version: intentPlannerHealthVersion, IntentPlannerHealthSnapshot: IntentPlannerHealthSnapshot{
				State: IntentPlannerCircuitOpen, ProviderFingerprint: fingerprint,
				LastFailureClass: IntentPlannerFailureTransport, LastError: failure,
				LastFailureTS: 11, NextProbeTS: 12, ConsecutiveFailures: 1,
			}}
			switch scenario {
			case "different-provider":
				health.ProviderFingerprint = publicationDrainTestDigest
			case "different-error":
				health.LastError = "another error"
			case "validation":
				health.LastFailureClass = IntentPlannerFailureValidation
			case "cooldown":
				health.NextProbeTS = 100
			case "external-head":
				external := commitSingleFile(t, ctx, repo, head, "other.txt", "external\n", "external")
				if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", "refs/heads/main", external); err != nil {
					t.Fatal(err)
				}
			case "terminal-event":
				if _, err := db.SQL().ExecContext(ctx, `UPDATE capture_events SET state='failed' WHERE seq=?`, events[0].Seq); err != nil {
					t.Fatal(err)
				}
			case "typed-safety-error":
				drain.ReasonCode = "missing_objects"
			}
			if err := state.MetaSetJSON(ctx, db, MetaKeyIntentPlannerHealth, health); err != nil {
				t.Fatal(err)
			}
			resumed, err := RecoverTransportWaitPublicationDrain(ctx, repo, db, drain, time.Unix(20, 0))
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "recover" {
				if resumed != nil {
					t.Fatalf("unsafe recovery=%+v", resumed)
				}
				return
			}
			if resumed == nil || resumed.Phase != state.PublicationDrainSemantic || resumed.LastError != "" || !reflect.DeepEqual(resumed.EventSeqs, drain.EventSeqs) {
				t.Fatalf("resumed=%+v", resumed)
			}
			again, err := RecoverTransportWaitPublicationDrain(ctx, repo, db, *resumed, time.Unix(21, 0))
			if err != nil || again != nil {
				t.Fatalf("recovery repeated: %+v %v", again, err)
			}
			if headAfter, err := gitpkg.RevParse(ctx, repo, "HEAD"); err != nil || headAfter != head {
				t.Fatalf("recovery moved HEAD=%s err=%v", headAfter, err)
			}
		})
	}
}

package daemon

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type reconnectingIntentPlanner struct {
	calls       int
	unavailable bool
}

func (*reconnectingIntentPlanner) Name() string { return "reconnecting-provider" }
func (p *reconnectingIntentPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, fmt.Errorf("v2 required")
}
func (p *reconnectingIntentPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	if p.unavailable {
		return ai.IntentPlanV2{}, &ai.ProviderHTTPError{StatusCode: 502, Detail: "Bad Gateway"}
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, capture := range req.OfferedCaptures {
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID:  fmt.Sprintf("document-offline-capture-%d", capture.Seq),
			SelectedSeqs: []int64{capture.Seq}, Readiness: ai.IntentCandidateReady,
			Purpose:        "document continued capture during a provider outage",
			Subject:        "Document continued offline capture",
			Body:           "- Explain how saved work survives a temporary provider outage",
			GroupingReason: "this document independently explains offline capture",
		})
	}
	return plan, nil
}

func TestIntentProviderOutageRetriesAcrossRestartAndPreservesLaterCaptures(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "offline.md", "Capture continues while the semantic provider is unavailable.\n")
	protected := capturePublicationFiles(t, f)
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("target captures=%v err=%v", pending, err)
	}
	now := time.Now().UTC()
	ts := float64(now.UnixNano()) / 1e9
	drain := state.PublicationDrain{
		ID: "outage-retry", CheckpointID: protected.CheckpointID,
		WorktreeID: checkpoint.WorktreeID(f.dir), BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration, Phase: state.PublicationDrainSemantic,
		TargetEventCount: 1, EventSeqs: []int64{pending[0].Seq},
		CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts,
	}
	if created, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil || !created {
		t.Fatalf("prepare=%t err=%v", created, err)
	}
	planner := &reconnectingIntentPlanner{unavailable: true}
	identity := IntentPlannerProviderIdentity{Provider: planner.Name()}
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{
		Provider: identity, Now: func() time.Time { return now },
	})
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentHealth: health, IntentPreset: config.PresetBalanced,
		IntentPlannerProvider: planner.Name(), IntentWindow: 20, IntentMinPending: 1,
		IntentBypassBatchWait: true, PublicationDrain: &drain,
	}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	assertIntentProviderWait(t, first, err)
	run, found, err := state.IntentPlanRunByFingerprint(ctx, f.db, first.PlanFingerprint)
	if err != nil || !found || run.Completed || run.AttemptCount != 0 || run.ProviderDeadlineTS != 0 || run.ProgressState.String != "waiting_for_ai" {
		t.Fatalf("outage planning run=%+v found=%t err=%v", run, found, err)
	}
	if planner.calls != 1 {
		t.Fatalf("initial calls=%d", planner.calls)
	}

	writePublicationFile(t, f, "later.md", "Later work is protected outside the frozen publication target.\n")
	laterProtection := capturePublicationFiles(t, f)
	if !laterProtection.Protected || laterProtection.CheckpointID == protected.CheckpointID {
		t.Fatalf("later work was not independently protected: %+v", laterProtection)
	}
	for attempt, delay := range []time.Duration{5 * time.Minute, 10 * time.Minute, time.Hour, time.Hour} {
		before := health.Snapshot()
		if before.NextProbeTS != intentPlannerHealthTimestamp(now.Add(delay)) {
			t.Fatalf("attempt %d next probe=%+v want delay %s", attempt, before, delay)
		}
		// Restart the state handle and provider circuit throughout the outage.
		// No file edit or changed planning fingerprint should be needed to retry.
		dbPath := f.db.Path()
		if err := f.db.Close(); err != nil {
			t.Fatal(err)
		}
		f.db, err = state.Open(ctx, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.db.Close() })
		health = NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{
			Provider: identity, Now: func() time.Time { return now },
		})
		opts.IntentHealth = health
		if after := health.Snapshot(); after.State != IntentPlannerCircuitOpen || after.NextProbeTS != before.NextProbeTS || after.BackoffLevel != before.BackoffLevel {
			t.Fatalf("restart reset cooldown: before=%+v after=%+v", before, after)
		}
		waiting, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
		assertIntentProviderWait(t, waiting, err)
		if planner.calls != attempt+1 {
			t.Fatalf("cooldown called provider: calls=%d attempt=%d", planner.calls, attempt)
		}
		now = now.Add(delay)
		if attempt == 3 {
			planner.unavailable = false
			// Model an old worker dying after recording the transport circuit,
			// before suspending its elapsed provider deadline.
			if _, err := f.db.SQL().ExecContext(ctx,
				`UPDATE intent_plan_runs SET provider_deadline_ts=? WHERE fingerprint=?`,
				float64(time.Now().Add(-time.Hour).Unix()), first.PlanFingerprint); err != nil {
				t.Fatal(err)
			}
		}
		summary, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
		if attempt < 3 {
			assertIntentProviderWait(t, summary, err)
			if summary.PlanFingerprint != first.PlanFingerprint {
				t.Fatalf("outage changed planning fingerprint: %s != %s", summary.PlanFingerprint, first.PlanFingerprint)
			}
		} else {
			if err != nil || summary.Published != 1 || summary.Failed != 0 {
				t.Fatalf("reconnection failed to publish: %+v err=%v", summary, err)
			}
			f.cctx.BaseHead = summary.BaseHead
		}
		if planner.calls != attempt+2 {
			t.Fatalf("due retries=%d want %d", planner.calls, attempt+2)
		}
		drain, err = UpdatePublicationDrainAfterReplay(ctx, f.db, drain, summary, err, now)
		if err != nil || (attempt < 3 && drain.Phase != state.PublicationDrainSemantic) {
			t.Fatalf("drain stopped retrying: %+v err=%v", drain, err)
		}
	}
	if drain.Phase != state.PublicationDrainCompleted || health.Snapshot().State != IntentPlannerCircuitClosed {
		t.Fatalf("recovery did not finish: drain=%+v health=%+v", drain, health.Snapshot())
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "later.md" {
		t.Fatalf("frozen target consumed later work: pending=%v err=%v", pending, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "show", "HEAD:later.md"); err == nil {
		t.Fatal("later work was published inside the frozen target")
	}
	if headMessage, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "log", "-1", "--format=%s"); err != nil || strings.TrimSpace(string(headMessage)) != "Document continued offline capture" {
		t.Fatalf("provider message=%q err=%v", headMessage, err)
	}
	opts.PublicationDrain = nil
	later, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || later.Published != 1 || later.Failed != 0 {
		t.Fatalf("later work did not resume: %+v err=%v", later, err)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after reconnection=%v err=%v", pending, err)
	}
}

func assertIntentProviderWait(t *testing.T, summary ReplaySummary, err error) {
	t.Helper()
	if err != nil && !isIntentPlannerCircuitWait(err) {
		t.Fatalf("outage returned terminal error: %v", err)
	}
	if summary.Disposition != ReplayDispositionTransientWait || summary.Published != 0 || summary.Failed != 0 || summary.Conflicts != 0 {
		t.Fatalf("outage was not a provider wait: %+v err=%v", summary, err)
	}
}

package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const publishedTransportPath = "tests/helpers/mock-worker-transport.ts"
const publishedWorkerTestPath = "tests/runtime/worker-manager.test.ts"

func publishedTypeScriptRegressionFixture(t *testing.T) (*captureFixture, IntentCandidateEvaluation) {
	t.Helper()
	ctx := context.Background()
	f := newCaptureFixture(t)
	for _, dir := range []string{"tests/helpers", "tests/runtime"} {
		if err := os.MkdirAll(filepath.Join(f.dir, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	body := "export class MockWorkerTransport {\n" + strings.Repeat(" // unchanged transport behavior\n", 20) + " prompt() { return \"started\"; }\n}\n"
	f.cctx.BaseHead = mustCommitPath(t, f.dir, publishedTransportPath, body, "Add a worker transport fixture")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, publishedWorkerTestPath, "import { MockWorkerTransport } from \"../helpers/mock-worker-transport.js\";\n"+strings.Repeat("// unchanged regression context\n", 20)+"if (new MockWorkerTransport().prompt() !== \"queued\") throw new Error(\"incorrect disposition\");\n")
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("test unprotected: %+v", captured)
	}
	events, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	pub, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent})
	if err != nil || pub.Published != 1 {
		t.Fatalf("test did not publish: %+v err=%v", pub, err)
	}
	f.cctx.BaseHead = pub.BaseHead
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{ID: "published-worker-regression", BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, Status: state.IntentCandidatePublished, Readiness: state.IntentReadinessReady, Purpose: "verify queued worker prompt dispositions", PublishedCommitOID: sql.NullString{String: pub.BaseHead, Valid: true}, Events: []state.IntentCandidateEvent{{EventSeq: events[0].Seq, EventRole: "test"}}}); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, publishedTransportPath, strings.Replace(body, "started", "queued", 1))
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("fixture unprotected: %+v", captured)
	}
	events, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	ops, err := state.LoadCaptureOps(ctx, f.db, events[0].Seq)
	if err != nil {
		t.Fatal(err)
	}
	return f, IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, IncludeDiffs: true, LatestCommit: &ai.CommitSummary{OID: pub.BaseHead[:8]}, Now: time.Now().UTC(), Captures: []IntentCandidateCapture{{Event: events[0], Ops: ops}}}
}

type publishedTypeScriptRegressionPlanner struct{ semanticRetryReplayPlanner }

func (p *publishedTypeScriptRegressionPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	p.req = req
	found := false
	for _, candidate := range req.Candidates {
		for _, evidence := range candidate.CapturedEvidence {
			found = found || candidate.Status == state.IntentCandidatePublished && evidence.Path == publishedWorkerTestPath && strings.Contains(evidence.CapturedDiff, "import { MockWorkerTransport }")
		}
	}
	a := ai.IntentCandidateAssignment{CandidateID: "queued-worker-disposition", Purpose: "model queued worker prompt dispositions", Readiness: ai.IntentCandidateWait, MissingCompanions: []string{"worker dispatch regression is not available"}, GroupingReason: "the exact recorded regression must support the queued disposition"}
	for _, capture := range req.OfferedCaptures {
		a.SelectedSeqs = append(a.SelectedSeqs, capture.Seq)
		found = found && strings.Contains(capture.CapturedDiff, " export class MockWorkerTransport")
	}
	if found {
		a.Readiness = ai.IntentCandidateReady
		a.MissingCompanions = nil
		a.Subject = "Model queued worker prompt dispositions"
		a.Body = "- Complete the transport fixture used by the published dispatch test"
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{a}}, nil
}

func TestIntentPublishedTypeScriptRegressionCompletesLateFixture(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, input := publishedTypeScriptRegressionFixture(t)
	seq := input.Captures[0].Event.Seq
	var checkpointID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT checkpoint_id FROM checkpoint_events WHERE event_seq=?", seq).Scan(&checkpointID); err != nil {
		t.Fatal(err)
	}
	ts := intentPlannerHealthTimestamp(time.Now())
	drain := state.PublicationDrain{ID: "late-ts-fixture", CheckpointID: checkpointID, WorktreeID: checkpoint.WorktreeID(f.dir), BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration, CommitStrategy: "intent", CommitFormat: "imperative", Provider: "intent-v2-test", ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic, TargetEventCount: 1, EventSeqs: []int64{seq}, CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "later.md", "# Later work stays protected\n")
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("later work unprotected: %+v", captured)
	}
	planner := &publishedTypeScriptRegressionPlanner{}
	pub, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner, IntentPreset: config.PresetBalanced, IntentIncludeDiffs: true, IntentBypassBatchWait: true, IntentVerificationMode: "structural", PublicationDrain: &drain})
	if err != nil || pub.Published != 1 || pub.Failed != 0 || planner.calls != 1 {
		t.Fatalf("published test did not release TS fixture: %+v calls=%d err=%v", pub, planner.calls, err)
	}
	if len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != seq {
		t.Fatalf("published test gained selection authority: %+v", planner.req.OfferedCaptures)
	}
	progress, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, pub, nil, time.Now())
	if err != nil || progress.Phase != state.PublicationDrainCompleted || !reflect.DeepEqual(progress.EventSeqs, []int64{seq}) {
		t.Fatalf("frozen progress=%+v err=%v", progress, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "later.md" {
		t.Fatalf("later work escaped: %+v err=%v", pending, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:later.md"); err == nil {
		t.Fatal("later work entered frozen commit")
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("visible required action=%t err=%v", attention, err)
	}
	if got := strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-1", "--format=%s")); got != "Model queued worker prompt dispositions" {
		t.Fatalf("nonpurposeful publication: %q", got)
	}
}

func TestIntentPublishedTypeScriptRegressionRequiresCurrentModuleResolution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, input := publishedTypeScriptRegressionFixture(t)
	head := mustCommitPath(t, f.dir, "tests/helpers/mock-worker-transport.js", "export class MockWorkerTransport {}\n", "Add a JavaScript transport")
	input.LatestCommit = &ai.CommitSummary{OID: strings.TrimSpace(head)}
	candidates, err := loadPublishedIntentFormerCompanions(ctx, f.db, &input, nil)
	if err != nil || len(candidates) != 0 || len(input.publishedContext) != 0 {
		t.Fatalf("old .js absence admitted a different current module: %+v err=%v", candidates, err)
	}
}

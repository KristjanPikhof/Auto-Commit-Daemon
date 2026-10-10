package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func swiftMaintenanceReplayOpts(f *captureFixture, planner ai.IntentPlanner) ReplayOpts {
	return ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPreset: config.PresetBalanced,
		IntentPlanner: planner, IntentIncludeDiffs: true, IntentBypassBatchWait: true,
		IntentSettleWindow: -1, IntentVerificationMode: "structural",
	}
}

func captureSwiftMaintenanceFixture(t *testing.T, f *captureFixture) {
	t.Helper()
	ctx := context.Background()
	epoch, err := BeginProtectionObservation(ctx, f.db)
	if err != nil {
		t.Fatal(err)
	}
	store := checkpointpkg.Store{DB: f.db}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher, GitDir: f.gitDir,
		CheckpointStore: &store, ObservationEpoch: epoch,
	}); err != nil {
		t.Fatal(err)
	}
}

func newSwiftMaintenanceFixture(t *testing.T) (*captureFixture, map[string]string) {
	t.Helper()
	f := newCaptureFixture(t)
	ctx := context.Background()
	bodies := map[string]string{
		"Assistant iOS/App/Assistant_iOSApp.swift":                                               "struct AppBootstrap {\n    func restart() {\n        \n        print(\"ready\")\n    }\n}\n",
		"Assistant iOS/App/iOSAppDelegate.swift":                                                 "final class AppDelegate {}\n\n",
		"Assistant iOS/Features/VoiceKeyboard/Services/VoiceKeyboardURLHandoffCoordinator.swift": "final class KeyboardHandoff {}\n\n",
		"Assistant/Core/Features/AI/Components/AIConsentDisclosureContent.swift":                 "struct ConsentDisclosure {}\n\n",
		"Assistant/Core/Features/AI/Components/ProviderCapabilityCompactStatusView.swift":        "struct CapabilityStatus {}\n\n",
	}
	paths := []string{"add", "--"}
	for name, body := range bodies {
		file := filepath.Join(f.dir, name)
		if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, name)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, paths...); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "Separate shared and native app ownership"); err != nil {
		t.Fatal(err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	f.cctx.BaseHead = head
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for name, body := range bodies {
		after := strings.ReplaceAll(body, "        \n", "\n")
		after = strings.TrimRight(after, "\n") + "\n"
		bodies[name] = after
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte(after), 0644); err != nil {
			t.Fatal(err)
		}
	}
	captureSwiftMaintenanceFixture(t, f)
	return f, bodies
}

func TestReplaySwiftBlankLineMaintenancePublishesOneGoal(t *testing.T) {
	t.Parallel()
	f, bodies := newSwiftMaintenanceFixture(t)
	ctx := context.Background()
	planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{err: errors.New("whitespace must not need a provider")}}
	// Recreate the existing per-file waits, rather than exercising only a fresh
	// window. The local goal must safely merge their original memberships.
	initialPending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(initialPending) != 5 {
		t.Fatalf("initial captures=%+v err=%v", initialPending, err)
	}
	for _, event := range initialPending {
		if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
			ID: fmt.Sprintf("waiting-whitespace-%d", event.Seq), BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
			Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait,
			Purpose:           "no meaningful semantic goal is evidenced by this whitespace-only diff",
			MissingCompanions: "a substantive change or useful maintenance goal is not yet evidenced",
			Events:            []state.IntentCandidateEvent{{EventSeq: event.Seq, EventRole: "code"}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	staged := filepath.Join(f.dir, "user-staged.txt")
	if err := os.WriteFile(staged, []byte("preserve user staging\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "--", "user-staged.txt"); err != nil {
		t.Fatal(err)
	}
	stagingBefore, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "ls-files", "--stage", "--", "user-staged.txt")
	if err != nil {
		t.Fatal(err)
	}
	before := revListCount(t, ctx, f.dir, "HEAD")
	summary, err := Replay(ctx, f.dir, f.db, f.cctx, swiftMaintenanceReplayOpts(f, planner))
	if err != nil || summary.Published != 5 || summary.Failed != 0 || planner.calls != 0 || revListCount(t, ctx, f.dir, "HEAD") != before+1 {
		t.Fatalf("maintenance=%+v calls=%d err=%v", summary, planner.calls, err)
	}
	message, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "log", "-1", "--format=%B")
	if err != nil || !strings.HasPrefix(string(message), "Normalize Swift source blank lines\n\n- ") {
		t.Fatalf("maintenance message=%q err=%v", message, err)
	}
	for name, body := range bodies {
		published, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "show", "HEAD:"+name)
		if err != nil || string(published) != body {
			t.Fatalf("wrong immutable post-image for %s: %q %v", name, published, err)
		}
		live, err := os.ReadFile(filepath.Join(f.dir, name))
		if err != nil || string(live) != body {
			t.Fatalf("live bytes changed for %s: %q %v", name, live, err)
		}
	}
	stagingAfter, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "ls-files", "--stage", "--", "user-staged.txt")
	if err != nil || string(stagingAfter) != string(stagingBefore) {
		t.Fatalf("user staging changed: %q -> %q %v", stagingBefore, stagingAfter, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:user-staged.txt"); err == nil {
		t.Fatal("unoffered user staging entered the maintenance goal")
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("maintenance remained pending: %+v %v", pending, err)
	}
}

func TestReplaySwiftBlankLineMaintenanceKeepsLaterCapturePending(t *testing.T) {
	t.Parallel()
	f, _ := newSwiftMaintenanceFixture(t)
	ctx := context.Background()
	planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{err: errors.New("maintenance is locally proved")}}
	opts := swiftMaintenanceReplayOpts(f, planner)
	opts.IntentVerificationMode = "full"
	verified := 0
	opts.IntentCandidateVerify = func(_ context.Context, assignment ai.IntentCandidateAssignment, captures []IntentCandidateCapture) (IntentCandidateVerification, error) {
		verified++
		if len(captures) != 5 || assignment.Subject != "Normalize Swift source blank lines" {
			t.Fatalf("wrong frozen maintenance target: %+v captures=%d", assignment, len(captures))
		}
		if err := os.WriteFile(filepath.Join(f.dir, "LaterValue.swift"), []byte("struct LaterValue { let enabled = true }\n"), 0644); err != nil {
			t.Fatal(err)
		}
		captureSwiftMaintenanceFixture(t, f)
		return IntentCandidateVerification{Status: "passed"}, nil
	}
	summary, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || summary.Published != 5 || verified != 1 || planner.calls != 0 {
		t.Fatalf("maintenance verification=%+v verified=%d calls=%d err=%v", summary, verified, planner.calls, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "LaterValue.swift" {
		t.Fatalf("later work entered frozen target: %+v %v", pending, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:LaterValue.swift"); err == nil {
		t.Fatal("later capture entered maintenance commit")
	}
	id, ok, err := state.MetaGet(ctx, f.db, MetaKeyProtectionCheckpointID)
	if err != nil || !ok {
		t.Fatalf("later checkpoint missing: %q %v", id, err)
	}
	checkpoint, err := state.ResolveCheckpoint(ctx, f.db.Path(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", checkpoint.CommitOID+":LaterValue.swift"); err != nil {
		t.Fatalf("later work lacks completed protection: %v", err)
	}
}

func TestReplaySwiftBlankLineMaintenanceRegroupsDueSingletonReviews(t *testing.T) {
	t.Parallel()
	for _, dueReviews := range []bool{true, false} {
		t.Run(fmt.Sprintf("due-reviews-%t", dueReviews), func(t *testing.T) {
			testReplaySwiftMaintenanceScheduledWindow(t, dueReviews)
		})
	}
}

func testReplaySwiftMaintenanceScheduledWindow(t *testing.T, dueReviews bool) {
	t.Helper()
	f, _ := newSwiftMaintenanceFixture(t)
	ctx := context.Background()
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 5 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	now := time.Now().UTC()
	for i, event := range pending {
		if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
			ID: fmt.Sprintf("old-whitespace-%d", event.Seq), BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
			Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait,
			Purpose:           "no meaningful semantic goal is evidenced by this whitespace-only diff",
			MissingCompanions: "a substantive change or useful maintenance goal is not yet evidenced",
			Events:            []state.IntentCandidateEvent{{EventSeq: event.Seq, EventRole: "code"}},
		}); err != nil {
			t.Fatal(err)
		}
		for count := 0; count < 3; count++ {
			if err := state.RecordPlannerDefer(ctx, f.db, event.Seq, intentPlannerHealthTimestamp(now.Add(-time.Hour)), "waiting for a useful goal"); err != nil {
				t.Fatal(err)
			}
		}
		if !dueReviews {
			continue
		}
		fingerprint := fmt.Sprintf("sha256:%064x", i+1)
		run, err := state.EnsureIntentPlanRun(ctx, f.db, state.IntentPlanRun{
			Fingerprint: fingerprint, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, AttemptLimit: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		run.Completed, run.AttemptCount = true, 1
		run.UnresolvedSeqs = []int64{event.Seq}
		run.ProgressState = sql.NullString{String: "waiting_semantic_retry", Valid: true}
		run.ResolutionMode = run.ProgressState
		if err := state.UpdateIntentPlanRun(ctx, f.db, run); err != nil {
			t.Fatal(err)
		}
		if err := state.MetaSetJSON(ctx, f.db, intentSemanticRetryKey(fingerprint), IntentSemanticRetrySnapshot{
			Version: 1, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
			EvidenceFingerprint: fingerprint, PlanFingerprint: fingerprint, ReviewCount: 1,
			ScheduledAtTS: intentPlannerHealthTimestamp(now.Add(-10 * time.Minute)), RetryAtTS: intentPlannerHealthTimestamp(now.Add(-time.Minute)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	oldWindow, forced, reason, err := selectIntentWindow(ctx, f.db, pending, intentReplayConfig{
		candidateMode: true, window: 64, bypassBatchWait: true, deferLimit: 3,
	})
	if err != nil || forced == dueReviews || reason != "" || len(oldWindow) != 1 {
		t.Fatalf("fixture did not recreate due singleton selection: window=%+v forced=%t reason=%s err=%v", oldWindow, forced, reason, err)
	}
	planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{err: errors.New("the normalization is locally proved")}}
	opts := swiftMaintenanceReplayOpts(f, planner)
	opts.IntentDeferLimit = 3
	before := revListCount(t, ctx, f.dir, "HEAD")
	summary, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || summary.Published != 5 || summary.Failed != 0 || planner.calls != 0 || revListCount(t, ctx, f.dir, "HEAD") != before+1 {
		t.Fatalf("old singleton reviews became commit boundaries: summary=%+v calls=%d err=%v", summary, planner.calls, err)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("normalization remains pending: %+v %v", pending, err)
	}
}

func TestIntentSwiftMaintenanceWindowPreservesSelectionFences(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"frozen target", "mixed initial window", "same-path successor"} {
		t.Run(scenario, func(t *testing.T) {
			f, _ := newSwiftMaintenanceFixture(t)
			ctx := context.Background()
			pending, err := state.PendingEvents(ctx, f.db, 0)
			if err != nil || len(pending) != 5 {
				t.Fatalf("pending=%+v err=%v", pending, err)
			}
			window := []state.CaptureEvent{pending[0]}
			cfg := intentReplayConfig{candidateMode: true, window: 64}
			switch scenario {
			case "frozen target":
				cfg.targetEventSeqs = []int64{window[0].Seq}
			case "mixed initial window":
				if err := os.WriteFile(filepath.Join(f.dir, "guide.md"), []byte("# Independent verification guide\n"), 0644); err != nil {
					t.Fatal(err)
				}
				captureSwiftMaintenanceFixture(t, f)
				pending, err = state.PendingEvents(ctx, f.db, 0)
				if err != nil || len(pending) != 6 {
					t.Fatalf("mixed pending=%+v err=%v", pending, err)
				}
				window = append(window, pending[len(pending)-1])
			case "same-path successor":
				if err := os.WriteFile(filepath.Join(f.dir, pending[0].Path), []byte("struct LaterBehavior { let enabled = false }\n"), 0644); err != nil {
					t.Fatal(err)
				}
				captureSwiftMaintenanceFixture(t, f)
				pending, err = state.PendingEvents(ctx, f.db, 0)
				if err != nil || len(pending) != 6 {
					t.Fatalf("successor pending=%+v err=%v", pending, err)
				}
			}
			expanded, err := expandIntentSwiftMaintenanceWindow(ctx, f.dir, f.db, pending, window, cfg, time.Now())
			if err != nil || len(expanded) != len(window) {
				t.Fatalf("%s was bypassed: expanded=%+v initial=%+v err=%v", scenario, expanded, window, err)
			}
			for i, event := range window {
				if expanded[i].Seq != event.Seq {
					t.Fatalf("%s changed the fairness anchor: expanded=%+v initial=%+v", scenario, expanded, window)
				}
			}
		})
	}
}

func TestReplaySwiftBlankLineMaintenanceDoesNotClassifySubstantiveChanges(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"multiline literal", "let title = \"\"\"\nhello\n    \nworld\n\"\"\"\n", "let title = \"\"\"\nhello\n\nworld\n\"\"\"\n"},
		{"behavior", "struct FeatureValue { let enabled = true }\n\n", "struct FeatureValue { let enabled = false }\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newCaptureFixture(t)
			ctx := context.Background()
			seedTrackedFileCommit(t, ctx, f, "FeatureValue.swift", tc.before)
			if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
				t.Fatal(err)
			}
			seq := captureSamePathEdit(t, ctx, f, "FeatureValue.swift", tc.after)
			// A tempting live whitespace-only edit must not replace the captured
			// string/behavior change as the proof's source of truth.
			if err := os.WriteFile(filepath.Join(f.dir, "FeatureValue.swift"), []byte(strings.TrimRight(tc.before, "\n")+"\n"), 0644); err != nil {
				t.Fatal(err)
			}
			planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{plan: ai.IntentPlanV2{
				ProtocolVersion: ai.IntentPlannerProtocolV2,
				Candidates: []ai.IntentCandidateAssignment{{
					CandidateID: "feature-value", SelectedSeqs: []int64{seq}, Readiness: ai.IntentCandidateReady,
					Purpose: "update the captured Swift value", Subject: "Update captured Swift value",
					Body: "- Preserve the exact requested literal or behavior change", GroupingReason: "the captured value change is independently complete",
				}},
			}}}
			summary, err := Replay(ctx, f.dir, f.db, f.cctx, swiftMaintenanceReplayOpts(f, planner))
			if err != nil || summary.Published != 1 || planner.calls == 0 {
				t.Fatalf("ordinary provider path=%+v calls=%d err=%v", summary, planner.calls, err)
			}
			published, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "show", "HEAD:FeatureValue.swift")
			if err != nil || string(published) != tc.after {
				t.Fatalf("live whitespace replaced captured behavior: %q %v", published, err)
			}
		})
	}
}

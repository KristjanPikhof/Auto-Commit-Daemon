package daemon

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentPreservedGroupsRespectExistingOwnership(t *testing.T) {
	t.Parallel()
	for _, sameOwner := range []bool{false, true} {
		name := "raw_replacement"
		if sameOwner {
			name = "normalized_continuation"
		}
		t.Run(name, func(t *testing.T) {
			req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
			owner := ai.IntentCandidateSummary{CandidateID: "intent-d63be9797ef23e5d0f3a1c2c", Status: "blocked"}
			for seq := int64(1); seq <= 24; seq++ {
				req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: seq})
				if seq <= 22 {
					owner.SelectedSeqs = append(owner.SelectedSeqs, seq)
				}
			}
			req.Candidates = []ai.IntentCandidateSummary{owner}
			id := "intent-unknown-dependency-recovery-20261009"
			if sameOwner {
				id = owner.CandidateID
			}
			plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{
				{CandidateID: id, SelectedSeqs: append([]int64(nil), owner.SelectedSeqs...), Readiness: ai.IntentCandidateReady},
				{CandidateID: "independent-goal", SelectedSeqs: []int64{23}, Readiness: ai.IntentCandidateReady},
				{CandidateID: "rejected-goal", SelectedSeqs: []int64{24}, DependsOnCandidates: []string{"rejected-goal"}},
			}}
			preserved, partial, ok := preserveIntentPlanGroups(req, plan, nil)
			if !ok {
				t.Fatal("independent goal lost its partial-replan boundary")
			}
			if err := ai.ValidateIntentPlanRequestV2(partial); err != nil {
				t.Fatalf("partial request invented duplicate ownership: %v", err)
			}
			if len(partial.Candidates) != 2 {
				t.Fatalf("durable owner acquired a duplicate descriptor: %+v", partial.Candidates)
			}
			if sameOwner {
				if len(preserved) != 2 || !reflect.DeepEqual(offeredIntentSeqs(partial), []int64{24}) ||
					!reflect.DeepEqual(partial.Candidates[0].SelectedSeqs, owner.SelectedSeqs) {
					t.Fatalf("normalized continuation was not retained exactly: kept=%+v partial=%+v", preserved, partial)
				}
			} else if len(preserved) != 1 || preserved[0].CandidateID != "independent-goal" ||
				!reflect.DeepEqual(partial.Candidates[0], owner) || len(partial.OfferedCaptures) != 23 {
				t.Fatalf("raw provider ID displaced durable ownership: kept=%+v partial=%+v", preserved, partial)
			}
		})
	}
}

type overlappingPreservedGoalPlanner struct {
	owner string
	calls int
	reqs  []ai.IntentPlanRequestV2
}

func (p *overlappingPreservedGoalPlanner) Name() string { return "overlapping-preserved-goal-test" }

func (p *overlappingPreservedGoalPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("native v2 required")
}

func (p *overlappingPreservedGoalPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	p.reqs = append(p.reqs, req)
	var source []int64
	var usage, release int64
	for _, capture := range req.OfferedCaptures {
		switch capture.Path {
		case "source.go", "source_test.go":
			source = append(source, capture.Seq)
		case "usage.md":
			usage = capture.Seq
		case "release.md":
			release = capture.Seq
		}
	}
	sourceID := "intent-unknown-dependency-recovery-20261009"
	if p.calls > 1 {
		sourceID = p.owner
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: sourceID, SelectedSeqs: source, Purpose: "return the new source value", Readiness: ai.IntentCandidateReady,
		Subject: "Return the new source value", Body: "- Keep the implementation and its regression together",
		GroupingReason: "value implementation and its matching assertion",
	}, {
		CandidateID: "release-guide", SelectedSeqs: []int64{release}, Purpose: "document release checks", Readiness: ai.IntentCandidateReady,
		Subject: "Document release checks", GroupingReason: "independent release checklist",
	}}}
	if p.calls == 1 {
		plan.Candidates[1].DependsOnCandidates = []string{"release-guide"}
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID: "usage-guide", SelectedSeqs: []int64{usage}, Purpose: "document keyboard shortcuts", Readiness: ai.IntentCandidateReady,
			Subject: "Document keyboard shortcuts", GroupingReason: "independent keyboard shortcut guide",
		})
	}
	return plan, nil
}

func TestReplayIntentPartialReplanKeepsOneDurableOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	seedTrackedFileCommit(t, ctx, f, "source.go", "package source\nfunc Value() int { return 1 }\n")
	seedTrackedFileCommit(t, ctx, f, "source_test.go", "package source\nimport \"testing\"\nfunc TestValue(t *testing.T) { if Value() != 1 { t.Fatal(Value()) } }\n")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string]string{
		"source.go":      "package source\nfunc Value() int { return 2 }\n",
		"source_test.go": "package source\nimport \"testing\"\nfunc TestValue(t *testing.T) { if Value() != 2 { t.Fatal(Value()) } }\n",
		"usage.md":       "# Keyboard shortcuts\nUse the command menu to discover shortcuts.\n",
		"release.md":     "# Release checks\nCheck the release build before distribution.\n",
	} {
		writePublicationFile(t, f, path, body)
	}
	capturePublicationFiles(t, f)
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 4 {
		t.Fatalf("captures=%+v err=%v", pending, err)
	}
	owner := state.IntentCandidate{ID: "intent-d63be9797ef23e5d0f3a1c2c", BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration, Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait,
		Purpose: "retain the value change until its goal is reviewed", AtomicityStatus: sql.NullString{String: "pending", Valid: true}}
	for _, event := range pending {
		if event.Path == "source.go" || event.Path == "source_test.go" {
			owner.Events = append(owner.Events, state.IntentCandidateEvent{EventSeq: event.Seq, EventRole: "implementation"})
		}
	}
	if err := state.SaveIntentCandidate(ctx, f.db, owner); err != nil {
		t.Fatal(err)
	}
	planner := &overlappingPreservedGoalPlanner{owner: owner.ID}
	retryLimit := 2
	opts := ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPreset: config.PresetBalanced, IntentBypassBatchWait: true, IntentWindow: 10,
		IntentRetryLimit: &retryLimit, IntentVerificationMode: "structural", IntentIncludeDiffs: true}
	before := revListCount(t, ctx, f.dir, "HEAD")
	result, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || result.Published != 4 || result.Failed != 0 || planner.calls != 2 ||
		revListCount(t, ctx, f.dir, "HEAD") != before+3 {
		t.Fatalf("raw rejected ID blocked partial retry: result=%+v calls=%d err=%v", result, planner.calls, err)
	}
	if len(planner.reqs[1].OfferedCaptures) != 3 || len(planner.reqs[1].Candidates) != 2 {
		t.Fatalf("independent goal was not retained: %+v", planner.reqs[1])
	}
	if err := ai.ValidateIntentPlanRequestV2(planner.reqs[1]); err != nil {
		t.Fatalf("retry request retained duplicate ownership: %v", err)
	}
	var sourceCommit, testCommit string
	for path, target := range map[string]*string{"source.go": &sourceCommit, "source_test.go": &testCommit} {
		if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT commit_oid FROM capture_events WHERE path=? AND state='published'", path).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if sourceCommit == "" || sourceCommit != testCommit {
		t.Fatalf("implementation and test were split: source=%s test=%s", sourceCommit, testCommit)
	}
	var rawOwner, blocked int
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM intent_candidates WHERE id=?", "intent-unknown-dependency-recovery-20261009").Scan(&rawOwner); err != nil {
		t.Fatal(err)
	}
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM intent_plan_runs WHERE progress_state='preflight_blocked'").Scan(&blocked); err != nil || blocked != 0 || rawOwner != 0 {
		t.Fatalf("derived duplicate became durable: raw=%d blocked=%d err=%v", rawOwner, blocked, err)
	}
}

package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentLateScriptCompanionRetainsRecordedWaitingCaller(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, source, reference, target, contents string }{
		{"configuration", "scripts/manifest.py", "from pathlib import Path\nsettings = Path(__file__).with_name(\"timings.json\").read_text()\n", "scripts/timings.json", "{\"_parallel_tests\": []}\n"},
		{"helper", "scripts/runner.sh", "python3 scripts/timing_helper.py\n", "scripts/timing_helper.py", "print(2)\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newCaptureFixture(t)
			before := tc.reference + strings.Repeat("# unchanged context\n", 20) + "workers=1\n"
			after := tc.reference + strings.Repeat("# unchanged context\n", 20) + "workers=2\n"
			beforeOID, err := git.HashObjectStdin(ctx, f.dir, []byte(before))
			if err != nil {
				t.Fatal(err)
			}
			afterOID, err := git.HashObjectStdin(ctx, f.dir, []byte(after))
			if err != nil {
				t.Fatal(err)
			}
			caller := appendIntentCandidateCapture(t, f.db, tc.source, "modify", beforeOID, afterOID)
			candidate := state.IntentCandidate{ID: "bounded-feedback", BranchRef: caller.Event.BranchRef,
				BranchGeneration: caller.Event.BranchGeneration, Status: state.IntentCandidateWaiting,
				Readiness: state.IntentReadinessWait, Purpose: "balance complete regression feedback",
				Events: []state.IntentCandidateEvent{{EventSeq: caller.Event.Seq, EventRole: "code"}}}
			if err := state.SaveIntentCandidate(ctx, f.db, candidate); err != nil {
				t.Fatal(err)
			}
			targetOID, err := git.HashObjectStdin(ctx, f.dir, []byte(tc.contents))
			if err != nil {
				t.Fatal(err)
			}
			target := appendIntentCandidateCapture(t, f.db, tc.target, "create", "", targetOID)
			// Later live bytes neither authorize nor erase the recorded caller.
			if err := os.MkdirAll(filepath.Join(f.dir, "scripts"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.dir, tc.source), []byte("echo unrelated.py\n"), 0600); err != nil {
				t.Fatal(err)
			}
			input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: caller.Event.BranchRef,
				BranchGeneration: caller.Event.BranchGeneration, Captures: []IntentCandidateCapture{target}, IncludeDiffs: true, Now: time.Now()}
			evidence, err := loadFocusedIntentGoalEvidence(ctx, input, []state.IntentCandidate{candidate}, []IntentCandidateCapture{caller, target})
			if err != nil || !strings.Contains(evidence[0].CapturedDiff, filepath.Base(tc.target)) || !strings.Contains(evidence[0].CapturedDiff, "+workers=2") {
				t.Fatalf("late companion lost its waiting recorded caller: %+v err=%v", evidence, err)
			}
			req, err := buildIntentCandidateRequest(input, []state.IntentCandidate{candidate}, nil, nil, evidence)
			if err != nil {
				t.Fatal(err)
			}
			plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
				CandidateID: candidate.ID, SelectedSeqs: []int64{target.Event.Seq},
				Purpose: "balance complete regression feedback", Readiness: ai.IntentCandidateReady,
				Subject:        "Balance complete regression feedback",
				Body:           "- Keep the runner and its recorded timing inputs together",
				GroupingReason: "the waiting recorded caller names the late timing input",
			}}}
			if err := ValidateIntentGoalPlan(req, plan); err != nil {
				t.Fatalf("complete late companion could not continue its captured goal: %v", err)
			}
		})
	}
}

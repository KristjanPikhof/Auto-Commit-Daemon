package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type typeScriptHistoryGoalPlanner struct {
	req ai.IntentPlanRequestV2
}

func (*typeScriptHistoryGoalPlanner) Name() string { return "recorded-typescript-history" }
func (p *typeScriptHistoryGoalPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.req = req
	var seqs []int64
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "prompt-dispositions", SelectedSeqs: seqs, Readiness: ai.IntentCandidateReady,
		Purpose: "handle prompt disposition outcomes with the mock regression and documented API companions",
		Subject: "Handle prompt disposition outcomes", Body: "- Keep mock behavior and its recorded regression and API guides complete",
		GroupingReason: "exact recorded imports and declared API references connect these companions",
	}}}, nil
}

func TestIntentHistoryRebuildsTypeScriptReferencesForCompletePathChains(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	const helperPath = "src/runtime/test-helpers.ts"
	const testPath = "tests/runtime/worker-manager.test.ts"
	const imported = "import { MockWorkerHandle, MockWorkerTransport, waitForMicrotasks } from \"../../src/runtime/test-helpers.js\";"
	var paths []string
	writeVersion := func(disposition string) {
		t.Helper()
		bodies := map[string]string{
			helperPath: "export class MockWorkerTransport {\n" + strings.Repeat(" // unchanged transport behavior\n", 16) +
				" prompt() { return \"" + disposition + "\"; }\n parse(input: string) { return input.split(/\\r?\\n/); }\n}\nexport class MockWorkerHandle {}\nexport function waitForMicrotasks() {}\n",
			testPath: imported + "\n" + strings.Repeat("// unchanged regression context\n", 16) +
				"const actual = new MockWorkerTransport().prompt();\nassert.equal(actual, \"" + disposition + "\");\n",
		}
		// Two real revisions across 130 paths exceed the exact-unit offer cap.
		// Every guide references the same actual declared public API.
		for i := 0; i < ai.IntentCandidateCaptureCap/2; i++ {
			p := fmt.Sprintf("docs/prompt-fixture-%03d.md", i)
			bodies[p] = fmt.Sprintf("# Prompt fixture %d\nUse `MockWorkerTransport` from `src/runtime/test-helpers.ts` for %s prompt outcomes.\n", i, disposition)
		}
		paths = paths[:0]
		for p, body := range bodies {
			if err := os.MkdirAll(filepath.Dir(filepath.Join(f.dir, p)), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.dir, p), []byte(body), 0644); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, p)
		}
	}
	commitVersion := func(disposition string) string {
		t.Helper()
		writeVersion(disposition)
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, append([]string{"add", "--"}, paths...)...); err != nil {
			t.Fatal(err)
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "Handle prompt dispositions"); err != nil {
			t.Fatal(err)
		}
		head, err := git.RevParse(ctx, f.dir, "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		return head
	}
	commitVersion("started")
	first := commitVersion("queued")
	last := commitVersion("handled")
	chain := []string{first, last}

	// Later source edits and an untracked .js competitor cannot change the
	// meaning or resolution of an immutable original TypeScript postimage.
	live := []byte("// unrelated later user edit\n")
	if err := os.WriteFile(filepath.Join(f.dir, testPath), live, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "--", testPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "src/runtime/test-helpers.js"), []byte("export class UnrelatedLiveTransport {}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(f.gitDir, "index")
	indexBefore, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	statusBefore := mustGitOutput(t, f.dir, "status", "--porcelain")
	planner := &typeScriptHistoryGoalPlanner{}
	plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, chain, planner, ai.CommitFormatImperative, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Units) != 2*len(paths) || len(plan.Units) <= ai.IntentCandidateCaptureCap || len(plan.Goals) != 1 || len(plan.Goals[0].Units) != len(plan.Units) {
		t.Fatalf("complete historical path chains lost ownership: units=%d paths=%d goals=%+v", len(plan.Units), len(paths), plan.Goals)
	}
	if len(planner.req.OfferedCaptures) != len(paths) {
		t.Fatalf("large history was not offered as complete path chains: %d", len(planner.req.OfferedCaptures))
	}
	var helper, consumer bool
	for _, capture := range planner.req.OfferedCaptures {
		if capture.Path == helperPath {
			helper = strings.Contains(capture.CapturedDiff, " export class MockWorkerTransport\n") && !strings.Contains(capture.CapturedDiff, "UnrelatedLiveTransport")
		}
		if capture.Path == testPath {
			consumer = strings.Contains(capture.CapturedDiff, " "+imported+"\n")
		}
	}
	if !helper || !consumer {
		t.Fatalf("recorded TS evidence did not survive history clipping: helper=%t consumer=%t", helper, consumer)
	}
	// Unchanged imports and declarations are outside the recorded hunks.
	// The shared gate must not substitute proximity for those witnesses.
	unproved := planner.req
	unproved.OfferedCaptures = append([]ai.OfferedCapture(nil), planner.req.OfferedCaptures...)
	for i, capture := range unproved.OfferedCaptures {
		if capture.Path != helperPath && capture.Path != testPath {
			continue
		}
		raw, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "diff", "--no-ext-diff", "--unified=3", plan.BaseTree, last, "--", capture.Path)
		if err != nil {
			t.Fatal(err)
		}
		unproved.OfferedCaptures[i].CapturedDiff = string(raw)
	}
	proposed, err := (&typeScriptHistoryGoalPlanner{}).PlanIntentV2(ctx, planner.req)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateIntentGoalPlan(unproved, proposed); err == nil {
		t.Fatal("raw hunks without the recorded TS references invented a complete goal")
	}
	plan.TargetBranchRef = "refs/heads/prompt-disposition-goal"
	plan, err = state.PrepareIntentHistoryPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var saved state.IntentHistoryPlan
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	replacements, err := ValidateIntentHistoryPlan(ctx, f.dir, saved)
	if err != nil {
		t.Fatal(err)
	}
	result, err := git.ApplyIntentHistoryReconstruction(ctx, f.dir, git.IntentHistoryReconstructionOptions{
		SourceBranchRef: saved.SourceBranchRef, TargetBranchRef: saved.TargetBranchRef, ExpectedHead: saved.ExpectedHead,
		OldChain: saved.SourceChain, Replacements: replacements, PlanID: saved.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if source, err := git.RevParse(ctx, f.dir, saved.SourceBranchRef); err != nil || source != last {
		t.Fatalf("source history changed: %s, %v", source, err)
	}
	if tree, err := git.RevParse(ctx, f.dir, result.NewHead+"^{tree}"); err != nil || tree != saved.Goals[0].TreeOID {
		t.Fatalf("goal changed the final original tree: %s, %v", tree, err)
	}
	before, err := git.RevParse(ctx, f.dir, first+"^")
	parent, parentErr := git.RevParse(ctx, f.dir, result.NewHead+"^")
	if err != nil || parentErr != nil || parent != before {
		t.Fatalf("history did not become one complete goal: parent=%s base=%s errors=%v/%v", parent, before, err, parentErr)
	}
	indexAfter, err := os.ReadFile(indexPath)
	if err != nil || !bytes.Equal(indexBefore, indexAfter) || mustGitOutput(t, f.dir, "status", "--porcelain") != statusBefore {
		t.Fatalf("history changed staging or later work: %v", err)
	}
	if contents, err := os.ReadFile(filepath.Join(f.dir, testPath)); err != nil || !bytes.Equal(contents, live) {
		t.Fatalf("history changed later source bytes: %q, %v", contents, err)
	}
}

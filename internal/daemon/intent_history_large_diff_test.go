package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

type largeGeneratedHistoryPlanner struct {
	calls   int
	request ai.IntentPlanRequestV2
}

func (*largeGeneratedHistoryPlanner) Name() string { return "generated-history-evidence" }

func (p *largeGeneratedHistoryPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	p.request = req
	var seqs []int64
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "measured-test-timings", SelectedSeqs: seqs,
		Purpose: "Record measured regression durations for bounded test scheduling", Readiness: ai.IntentCandidateReady,
		Subject:        "Record measured regression test timings",
		Body:           "- Keep shard balancing grounded in observed regression durations",
		GroupingReason: "The generated timing measurements complete one scheduling data update",
	}}}, nil
}

func TestIntentHistoryLargeGeneratedDiffPreservesFinalTree(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	path := "scripts/dev/test-timings.json"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(f.dir, path)), 0o755); err != nil {
		t.Fatal(err)
	}
	timings := func(seconds int) string {
		var body strings.Builder
		body.WriteString("{\n  \"version\": 1,\n  \"tests\": {\n")
		for i := 0; i < 5000; i++ {
			separator := ","
			if i == 4999 {
				separator = ""
			}
			fmt.Fprintf(&body, "    \"TestMeasuredDuration%05d\": %d.001%s\n", i, seconds, separator)
		}
		body.WriteString("  }\n}\n")
		return body.String()
	}
	before := mustCommitPath(t, f.dir, path, timings(1), "Record initial regression timings")
	contents := timings(2)
	if !json.Valid([]byte(contents)) {
		t.Fatal("generated-data fixture is not valid JSON")
	}
	head := mustCommitPath(t, f.dir, path, contents, "Refresh measured regression durations")
	args := []string{"diff", "--no-ext-diff", "--unified=3", before, head, "--", path}
	if _, err := git.RunWithLimit(ctx, git.RunOpts{Dir: f.dir}, 256<<10, args...); !errors.Is(err, git.ErrStdoutOverflow) {
		t.Fatalf("fixture did not reproduce the old raw history limit: %v", err)
	}
	raw, err := git.RunWithLimit(ctx, git.RunOpts{Dir: f.dir}, intentHistoryRawDiffCap, args...)
	if err != nil || len(raw) <= 256<<10 || len(raw) > intentHistoryRawDiffCap {
		t.Fatalf("raw generated diff bytes=%d err=%v", len(raw), err)
	}
	planner := &largeGeneratedHistoryPlanner{}
	plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, []string{head}, planner, ai.CommitFormatImperative, true)
	if err != nil {
		t.Fatal(err)
	}
	if planner.calls != 1 || len(planner.request.OfferedCaptures) != 1 {
		t.Fatalf("generated-data planning calls=%d captures=%d", planner.calls, len(planner.request.OfferedCaptures))
	}
	capture := planner.request.OfferedCaptures[0]
	if len(capture.CapturedDiff) > ai.IntentStageDiffCap || len(capture.CapturedDiff) > ai.HistoryRewriteTotalDiffCap ||
		!capture.CapturedDiffTruncated || !strings.Contains(capture.CapturedDiff, "\n... <truncated> ...\n") {
		t.Fatalf("provider evidence lost its clipping bounds/provenance: bytes=%d truncated=%t", len(capture.CapturedDiff), capture.CapturedDiffTruncated)
	}
	replacements, err := ValidateIntentHistoryPlan(ctx, f.dir, plan)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := git.RevParse(ctx, f.dir, head+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	if len(replacements) != 1 || replacements[0].TreeOID != expected || len(plan.Units) != 1 {
		t.Fatalf("bounded evidence changed the exact recorded goal tree: %+v expected=%s units=%d", replacements, expected, len(plan.Units))
	}
	if current, err := git.RevParse(ctx, f.dir, "HEAD"); err != nil || current != head {
		t.Fatalf("history preview moved source HEAD=%s want=%s err=%v", current, head, err)
	}
}

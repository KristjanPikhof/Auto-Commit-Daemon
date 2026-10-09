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
)

func TestIntentReviewKeepsCompleteLargeRecordedImplementation(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	const middle = "if !protected { return false }"
	padding := strings.Repeat(" _ = \"recorded publication context\"\n", 550)
	contents := "package app\nfunc PublishGoal(protected bool) bool {\n" + padding + " " + middle + "\n" + padding + " return true\n}\n"
	if len(contents) <= 32000 || len(contents) >= ai.IntentStageDiffCap {
		t.Fatal("fixture must exceed the old allowance and fit complete review")
	}
	after, err := git.HashObjectStdin(ctx, f.dir, []byte(contents))
	if err != nil {
		t.Fatal(err)
	}
	capture := appendIntentCandidateCapture(t, f.db, "publication.go", "create", "", after)
	// Evidence must come from the protected version, even while editing continues.
	if err := os.WriteFile(filepath.Join(f.dir, "publication.go"), []byte("package app\n// later live work\n"), 0600); err != nil {
		t.Fatal(err)
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: capture.Event.BranchRef, BranchGeneration: capture.Event.BranchGeneration, Captures: []IntentCandidateCapture{capture}, IncludeDiffs: true, Now: time.Now()}
	captures, err := loadFocusedIntentGoalEvidence(ctx, input, nil, input.Captures)
	if err != nil {
		t.Fatal(err)
	}
	if err := attachIntentFileMetadata(ctx, f.dir, captures); err != nil {
		t.Fatal(err)
	}
	req, err := buildIntentCandidateRequest(input, nil, nil, nil, captures)
	if err != nil {
		t.Fatal(err)
	}
	got := req.OfferedCaptures[0]
	if !strings.Contains(got.CapturedDiff, middle) || strings.Contains(got.CapturedDiff, "<truncated>") || strings.Contains(got.CapturedDiff, "later live work") || got.CapturedDiffTruncated {
		t.Fatal("completed recorded implementation was clipped or replaced by live work")
	}
	if got.FileMetadata == nil || got.FileMetadata.DiffOmittedReason != "" {
		t.Fatalf("complete implementation reported missing evidence: %+v", got.FileMetadata)
	}
	if len(got.CapturedDiff) > ai.IntentStageDiffCap || len(got.CapturedDiff) > ai.IntentGoalEvidenceTotalDiffCap {
		t.Fatal("review evidence exceeded its limits")
	}
	wire, err := ai.BuildIntentPlanV2UserPrompt(req)
	if err != nil || !strings.Contains(wire, middle) {
		t.Fatalf("provider request lost the implementation: %v", err)
	}
	input.IncludeDiffs = false
	private, err := buildIntentCandidateRequest(input, nil, nil, nil, captures)
	if err != nil || private.OfferedCaptures[0].CapturedDiff != "" {
		t.Fatalf("larger evidence bypassed diff permission: %v", err)
	}
}

func TestIntentLargerReviewStillBoundsAggregateEvidence(t *testing.T) {
	t.Parallel()
	var raw []string
	preferred := make(map[int]bool)
	for i := 0; i < 8; i++ {
		raw = append(raw, strings.Repeat("+recorded implementation line\n", 1500))
		preferred[i] = true
	}
	allocated := allocateIntentEvidenceDiffsPrioritized(raw, ai.IntentGoalEvidenceTotalDiffCap, preferred)
	total, clipped := 0, false
	for _, diff := range allocated {
		total += len(diff)
		clipped = clipped || strings.Contains(diff, "<truncated>")
		if len(diff) > ai.IntentStageDiffCap {
			t.Fatal("per-capture allowance exceeded")
		}
	}
	if total > ai.IntentGoalEvidenceTotalDiffCap || !clipped {
		t.Fatalf("aggregate bound not enforced: bytes=%d clipped=%t", total, clipped)
	}
}

func TestIntentReviewReportsClippedRecordedImplementation(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	contents := "package app\nfunc PublishGoal() bool {\n" + strings.Repeat(" _ = \"recorded publication context\"\n", 2100) + " return true\n}\n"
	after, err := git.HashObjectStdin(ctx, f.dir, []byte(contents))
	if err != nil {
		t.Fatal(err)
	}
	capture := appendIntentCandidateCapture(t, f.db, "publication.go", "create", "", after)
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: capture.Event.BranchRef, BranchGeneration: capture.Event.BranchGeneration, Captures: []IntentCandidateCapture{capture}, IncludeDiffs: true, Now: time.Now()}
	captures, err := loadFocusedIntentGoalEvidence(ctx, input, nil, input.Captures)
	if err != nil {
		t.Fatal(err)
	}
	if err := attachIntentFileMetadata(ctx, f.dir, captures); err != nil {
		t.Fatal(err)
	}
	req, err := buildIntentCandidateRequest(input, nil, nil, nil, captures)
	if err != nil {
		t.Fatal(err)
	}
	got := req.OfferedCaptures[0]
	if !strings.Contains(got.CapturedDiff, "<truncated>") || got.FileMetadata == nil || got.FileMetadata.DiffOmittedReason != "truncated" || len(got.CapturedDiff) > ai.IntentStageDiffCap {
		t.Fatalf("clipped recorded evidence was reported as complete: %+v", got.FileMetadata)
	}
}

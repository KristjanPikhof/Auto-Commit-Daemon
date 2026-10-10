package daemon

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentHistoryRebuildsQualifiedCLIProofFromRecordedVersions(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	for _, dir := range []string{"cli", "docs"} {
		if err := os.MkdirAll(filepath.Join(f.dir, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	seedTrackedFileCommit(t, ctx, f, "cli/root.go", intentCobraFixtureRoot)
	seedTrackedFileCommit(t, ctx, f, "cli/fix.go", strings.Replace(intentCobraFixtureFix, "return force }", "return false }", 1))
	seedTrackedFileCommit(t, ctx, f, "cli/facade.go", strings.Replace(intentCobraFixtureFacade, "return buildFixPlan(force) }", "return false }", 1))
	changes := []struct{ path, contents string }{
		{"docs/commands.md", "# Recovery\nUse `acd support recover --force --yes` to preserve pending work.\n"},
		{"cli/fix.go", intentCobraFixtureFix},
		{"cli/fix_pending_capture_recovery_test.go", "package cli\nfunc TestPendingRecovery() { if !buildFixPlan(true) { panic(fixActionReconcileUnpublishedChain) } }\n"},
		{"cli/facade.go", intentCobraFixtureFacade},
		{"cli/product_recovery_changed_test.go", "package cli\nfunc TestRecoveryChanged() { if !runProductFix(true) { panic(fixActionReconcileUnpublishedChain) } }\n"},
	}
	var chain []string
	for _, change := range changes {
		chain = append(chain, mustCommitPath(t, f.dir, change.path, change.contents, "Update source"))
	}
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "pending-recovery", SelectedSeqs: []int64{1, 2, 3, 4, 5}, Readiness: ai.IntentCandidateReady,
		Purpose: "preserve unresolved pending capture chains during forced recovery", Subject: "Preserve pending capture chains",
		Body: "- Keep forced recovery and its documented result complete", GroupingReason: "The registered command and both regression callers complete one recovery goal",
	}}}}
	plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, chain, planner, ai.CommitFormatImperative, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Goals) != 1 || len(plan.Goals[0].Units) != len(changes) {
		t.Fatalf("history omitted a recovery companion: %+v", plan.Goals)
	}
	plan.TargetBranchRef = "refs/heads/recovery-goal"
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
	// A later uncommitted edit cannot supply or revoke command authority.
	if err := os.WriteFile(filepath.Join(f.dir, "cli/root.go"), []byte("package cli\n"), 0644); err != nil {
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
	if tree, err := git.RevParse(ctx, f.dir, result.NewHead+"^{tree}"); err != nil || tree != saved.Goals[0].TreeOID {
		t.Fatalf("reconstructed recovery changed the final tree: %s, %v", tree, err)
	}
	if contents, err := os.ReadFile(filepath.Join(f.dir, "cli/root.go")); err != nil || string(contents) != "package cli\n" {
		t.Fatalf("history overwrote later work: %q, %v", contents, err)
	}
}

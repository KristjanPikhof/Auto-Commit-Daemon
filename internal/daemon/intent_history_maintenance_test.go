package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type maintenanceHistoryPlanner struct{}

func (maintenanceHistoryPlanner) Name() string { return "proved-maintenance-history" }
func (maintenanceHistoryPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	if plan, ok := swiftBlankLineMaintenancePlan(req); ok {
		return plan, nil
	}
	return ai.IntentPlanV2{}, errors.New("recorded history does not prove one maintenance goal")
}

func TestIntentHistoryRegroupsSwiftMaintenanceWithoutChangingLiveWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, bodies := newSwiftMaintenanceFixture(t)
	var paths, chain []string
	for path := range bodies {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		after := strings.ReplaceAll(bodies[path], "        \n", "\n")
		after = strings.TrimRight(after, "\n") + "\n"
		chain = append(chain, mustCommitPath(t, f.dir, path, after, "Normalize Swift source blank lines"))
	}
	live := []byte("struct LaterUserChange {}\n")
	if err := os.WriteFile(filepath.Join(f.dir, paths[0]), live, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "--", paths[0]); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(f.gitDir, "index")
	indexBefore, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, chain, maintenanceHistoryPlanner{}, ai.CommitFormatImperative, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Goals) != 1 || len(plan.Goals[0].Units) != 5 {
		t.Fatalf("per-file history was not regrouped into one proved goal: %+v", plan.Goals)
	}
	plan.TargetBranchRef = "refs/heads/maintenance-goal"
	plan, err = state.PrepareIntentHistoryPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	// A saved plan must reconstruct its proof from Git, rather than trust
	// transient metadata supplied by a planner or JSON.
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
		SourceBranchRef: saved.SourceBranchRef, TargetBranchRef: saved.TargetBranchRef,
		ExpectedHead: saved.ExpectedHead, OldChain: saved.SourceChain,
		Replacements: replacements, PlanID: saved.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if source, err := git.RevParse(ctx, f.dir, saved.SourceBranchRef); err != nil || source != chain[len(chain)-1] {
		t.Fatalf("source branch changed: %s err=%v", source, err)
	}
	if tree, err := git.RevParse(ctx, f.dir, result.NewHead+"^{tree}"); err != nil || tree != saved.Goals[0].TreeOID {
		t.Fatalf("reconstructed maintenance tree changed: %s err=%v", tree, err)
	}
	parent, err := git.RevParse(ctx, f.dir, result.NewHead+"^")
	base, baseErr := git.RevParse(ctx, f.dir, chain[0]+"^")
	if err != nil || baseErr != nil || parent != base {
		t.Fatalf("maintenance was not one commit above the original base: %s != %s, errors=%v/%v", parent, base, err, baseErr)
	}
	indexAfter, err := os.ReadFile(indexPath)
	if err != nil || !bytes.Equal(indexBefore, indexAfter) {
		t.Fatalf("reconstruction changed user staging: %v", err)
	}
	contents, err := os.ReadFile(filepath.Join(f.dir, paths[0]))
	if err != nil || !bytes.Equal(contents, live) {
		t.Fatalf("reconstruction changed later live work: %q err=%v", contents, err)
	}
}

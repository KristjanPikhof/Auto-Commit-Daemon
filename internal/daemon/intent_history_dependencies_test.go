package daemon

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishHistoryRewriteCorrectsCrossAuthorGoalBeforeSaving(t *testing.T) {
	f, _ := finishWorkerFixture(t)
	first := mustCommitPath(t, f.dir, "policy.go", "package policy\nfunc Window()int{return 64}\n", "Bound capture planning windows")
	finishWorkerGit(t, f.dir, "config", "user.name", "Other Author")
	finishWorkerGit(t, f.dir, "config", "user.email", "other@example.com")
	second := mustCommitPath(t, f.dir, "policy.go", "package policy\nfunc Window()int{return 64}\nfunc Retry()int{return 300}\n", "Schedule provider outage retries")
	transport := &finishTestTransport{exchange: func(_ context.Context, r ai.FinishPlanRequest) ([]byte, error) {
		goals := []finish.Goal{{Purpose: "Bound capture planning windows", Boundary: "Keep window limits independently reviewable", Message: "Bound capture planning windows", ChangeIDs: []string{r.Changes[0].ID}}, {Purpose: "Schedule provider outage retries", Boundary: "Keep retry policy independently reviewable", Message: "Schedule provider outage retries", ChangeIDs: []string{r.Changes[1].ID}}}
		if r.Changes[0].Author == r.Changes[1].Author {
			t.Fatal("author boundary missing")
		}
		if r.Correction == nil {
			goals = goals[:1]
			goals[0].ChangeIDs = append(goals[0].ChangeIDs, r.Changes[1].ID)
		} else if !strings.Contains(r.Correction.ValidationError, "author") {
			t.Fatal("correction missing exact author failure")
		}
		raw, _ := json.Marshal(map[string]any{"goals": goals})
		return raw, nil
	}}
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{first, second}, nil, transport, FinishHistoryOptions{Mode: "intent", Style: "imperative", Deadline: time.Now().Add(time.Minute)})
	if err != nil || transport.calls.Load() != 2 || len(plan.Goals) != 2 {
		t.Fatalf("correction %+v %v", plan, err)
	}
	if plan.Goals[0].AuthorOID != first || plan.Goals[1].AuthorOID != second {
		t.Fatal("author ownership changed")
	}
}

func TestFinishHistoryRewriteSavedPrerequisitesPreserveSuccessiveGoalTrees(t *testing.T) {
	f, w := finishWorkerFixture(t)
	first := mustCommitPath(t, f.dir, "policy.go", "package policy\nfunc Window()int{return 64}\n", "Bound capture planning windows")
	second := mustCommitPath(t, f.dir, "policy.go", "package policy\nfunc Window()int{return 64}\nfunc Retry()int{return 300}\n", "Schedule provider outage retries")
	transport := &finishTestTransport{exchange: func(_ context.Context, r ai.FinishPlanRequest) ([]byte, error) {
		raw, _ := json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: "Bound capture planning windows", Boundary: "Keep window limits independently reviewable", Message: "Bound capture planning windows", ChangeIDs: []string{r.Changes[0].ID}}, {Purpose: "Schedule provider outage retries", Boundary: "Add retry policy after its prerequisite", Message: "Schedule provider outage retries", ChangeIDs: []string{r.Changes[1].ID}}}})
		return raw, nil
	}}
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{first, second}, nil, transport, FinishHistoryOptions{Mode: "intent", Style: "imperative", Deadline: time.Now().Add(time.Minute), VerificationConfigDigest: w.Config.ConfigDigest})
	if err != nil {
		t.Fatal(err)
	}
	plan.TargetBranchRef = "refs/heads/successive-goals"
	plan, err = state.SaveIntentHistoryPlan(t.Context(), f.db, plan)
	if err != nil {
		t.Fatal(err)
	}
	saved, ok, err := state.LoadIntentHistoryPlan(t.Context(), f.db, plan.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	for index, oid := range []string{first, second} {
		tree, err := git.RevParse(t.Context(), f.dir, oid+"^{tree}")
		if err != nil || saved.Goals[index].TreeOID != tree {
			t.Fatal("successive recorded tree lost")
		}
	}
	r := historyStartTest(t, w, saved, false)
	r, err = w.ExecuteFinishHistoryRequest(t.Context(), saved, r)
	if err != nil {
		t.Fatal(err)
	}
	var proof state.FinishHistoryProof
	_ = json.Unmarshal(r.Proof, &proof)
	if len(proof.Commits) != 2 || proof.Commits[0].TreeOID != plan.Goals[0].TreeOID || proof.Commits[1].TreeOID != plan.Goals[1].TreeOID || proof.Commits[1].ParentOID != proof.Commits[0].OID {
		t.Fatal("prerequisite order/tree changed")
	}
	if transport.calls.Load() != 1 {
		t.Fatal("saved apply called AI")
	}
}

package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func historyStartTest(t *testing.T, w *FinishWorker, plan state.IntentHistoryPlan, dry bool) state.FinishHistoryRequest {
	t.Helper()
	index, err := git.IndexContentDigest(t.Context(), w.RepoPath)
	if err != nil {
		t.Fatal(err)
	}
	wt, err := git.ResolveWorktree(t.Context(), w.RepoPath)
	if err != nil {
		t.Fatal(err)
	}
	r, created, err := state.StartFinishHistoryRequest(t.Context(), w.DB, state.FinishHistoryRequest{PlanID: plan.ID, WorktreeID: checkpoint.WorktreeID(wt.Root), ConfigDigest: w.Config.ConfigDigest, IndexDigest: index, DryRun: dry, DeadlineTS: finishTimestamp(time.Now().Add(time.Minute))})
	if err != nil || !created {
		t.Fatalf("start created=%t: %v", created, err)
	}
	return r
}

func TestFinishHistoryRewriteLegacyQueueNeverPublishes(t *testing.T) {
	f, _ := finishWorkerFixture(t)
	head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	if err := state.MetaSetJSON(t.Context(), f.db, "intent.history.request", map[string]string{"plan_id": "legacy-plan", "status": "pending"}); err != nil {
		t.Fatal(err)
	}
	before, _, err := state.MetaGet(t.Context(), f.db, "intent.history.request")
	if err != nil {
		t.Fatal(err)
	}
	_, stop := finishProtectionRun(t, f, nil)
	stop()
	after, _, err := state.MetaGet(t.Context(), f.db, "intent.history.request")
	if err != nil || before != after || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != head {
		t.Fatalf("legacy evidence or branch changed: %v", err)
	}
}

func TestFinishHistoryRewriteLocalPreservesEmptyNeutralAuthorsAndStyles(t *testing.T) {
	for _, style := range []string{finish.StyleImperative, finish.StyleConventional} {
		t.Run(style, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			w.Config.Style = style
			message := func(subject, reason string) string {
				if style == finish.StyleConventional {
					subject = "chore: " + strings.ToLower(subject[:1]) + subject[1:]
				}
				return subject + "\n\n- " + reason
			}
			a := mustCommitPath(t, f.dir, "goal.txt", "a\n", message("Add recorded goal", "Keep the initial approved value available for rollback."))
			finishWorkerGit(t, f.dir, "config", "user.name", "Second Author")
			finishWorkerGit(t, f.dir, "config", "user.email", "second@example.com")
			finishWorkerGit(t, f.dir, "commit", "--allow-empty", "-qm", message("Preserve empty checkpoint", "Mark an authored boundary without changing the selected tree."))
			empty := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
			b := mustCommitPath(t, f.dir, "goal.txt", "b\n", message("Refine recorded goal", "Try the alternate value before deciding what to retain."))
			neutral := mustCommitPath(t, f.dir, "goal.txt", "a\n", message("Restore recorded goal", "Keep the approved value after trying the alternative."))
			plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{a, empty, b, neutral}, nil, nil, FinishHistoryOptions{Mode: "local", Style: style, VerificationConfigDigest: w.Config.ConfigDigest})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Goals) != 4 || plan.PlanningCalls != 0 {
				t.Fatalf("membership %+v", plan)
			}
			r := historyStartTest(t, w, plan, false)
			index := r.IndexDigest
			finishWrite(t, f.dir, "goal.txt", "later live edit\n")
			r, err = w.ExecuteFinishHistoryRequest(t.Context(), plan, r)
			if err != nil || r.Status != "completed" {
				t.Fatalf("execute %s: %v", r.Status, err)
			}
			var proof state.FinishHistoryProof
			if err := json.Unmarshal(r.Proof, &proof); err != nil {
				t.Fatal(err)
			}
			if len(proof.Commits) != 4 || len(proof.Mappings) != 4 {
				t.Fatalf("proof %+v", proof)
			}
			for i, old := range []string{a, empty, b, neutral} {
				if got := finishWorkerGit(t, f.dir, "show", "-s", "--format=%an%x00%ae%x00%aI", proof.Commits[i].OID); got != finishWorkerGit(t, f.dir, "show", "-s", "--format=%an%x00%ae%x00%aI", old) {
					t.Fatal("author/date changed")
				}
			}
			if data, _ := os.ReadFile(filepath.Join(f.dir, "goal.txt")); string(data) != "later live edit\n" {
				t.Fatal("live worktree overwritten")
			}
			if after, _ := git.IndexContentDigest(t.Context(), f.dir); after != index {
				t.Fatal("index changed")
			}
			if got := finishWorkerGit(t, f.dir, "rev-parse", proof.BackupRef); got != neutral {
				t.Fatal("source backup lost")
			}
			var mappings int
			if err := f.db.ReadSQL().QueryRowContext(t.Context(), "SELECT count(*) FROM finish_history_mappings").Scan(&mappings); err != nil || mappings != 4 {
				t.Fatalf("immutable mappings %d %v", mappings, err)
			}
		})
	}
}

func TestFinishHistoryRewriteIntentInterleavedPurposeAndSavedProof(t *testing.T) {
	f, w := finishWorkerFixture(t)
	a := mustCommitPath(t, f.dir, "a.txt", "alpha\n", "Add alpha")
	b := mustCommitPath(t, f.dir, "b.txt", "beta\n", "Add beta")
	correction := mustCommitPath(t, f.dir, "a.txt", "correct alpha\n", "Correct alpha")
	suffix := mustCommitPath(t, f.dir, "later.txt", "unchanged descendant\n", "Keep later descendant")
	transport := &finishTestTransport{exchange: func(_ context.Context, r ai.FinishPlanRequest) ([]byte, error) {
		ids := map[string][]string{}
		for _, c := range r.Changes {
			ids[c.Path] = append(ids[c.Path], c.ID)
		}
		raw, _ := json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: "Complete alpha behavior", Boundary: "Keep alpha and its correction together", Reason: "Keep the alpha outcome and its correction together.", Message: "Complete alpha behavior\n\n- Keep the alpha outcome and its correction together.", ChangeIDs: ids["a.txt"]}, {Purpose: "Complete beta behavior", Boundary: "Keep the independent beta outcome reviewable", Reason: "Preserve the independent beta outcome.", Message: "Complete beta behavior\n\n- Preserve the independent beta outcome.", ChangeIDs: ids["b.txt"]}}})
		return raw, nil
	}}
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{a, b, correction}, []string{suffix}, transport, FinishHistoryOptions{Mode: "intent", DiffEgress: true, Style: "imperative", Deadline: time.Now().Add(time.Minute), VerificationConfigDigest: w.Config.ConfigDigest})
	if err != nil {
		t.Fatal(err)
	}
	if transport.calls.Load() != 1 || len(plan.Goals) != 2 || len(plan.Goals[0].Units) != 2 {
		t.Fatalf("plan %+v", plan)
	}
	for _, mutate := range []func(*state.IntentHistoryPlan){func(p *state.IntentHistoryPlan) { p.Goals[0].TreeOID = p.BaseTree }, func(p *state.IntentHistoryPlan) { p.Units[0].After.OID = p.Units[1].After.OID }, func(p *state.IntentHistoryPlan) { p.Goals[0].AuthorOID = b }} {
		raw, _ := json.Marshal(plan)
		var edited state.IntentHistoryPlan
		_ = json.Unmarshal(raw, &edited)
		mutate(&edited)
		if err := ValidateIntentHistoryPlan(t.Context(), f.dir, edited); err == nil {
			t.Fatal("fabricated saved plan accepted")
		}
	}
	r := historyStartTest(t, w, plan, true)
	r, err = w.ExecuteFinishHistoryRequest(t.Context(), plan, r)
	if err != nil {
		t.Fatal(err)
	}
	if head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD"); head != suffix {
		t.Fatal("preview moved source")
	}
	r = historyStartTest(t, w, plan, false)
	r, err = w.ExecuteFinishHistoryRequest(t.Context(), plan, r)
	if err != nil {
		t.Fatal(err)
	}
	var proof state.FinishHistoryProof
	_ = json.Unmarshal(r.Proof, &proof)
	if len(proof.Commits) != 3 || transport.calls.Load() != 1 {
		t.Fatalf("saved apply replanned %+v", proof)
	}
	// Exact post-CAS recovery records completion without resolving providers or current configuration.
	recovered := r
	recovered.Status = "prepared"
	if err := state.UpdateFinishHistoryRequest(t.Context(), f.db, recovered, "completed"); err != nil {
		t.Fatal(err)
	}
	w.Config = FinishWorkerConfig{}
	recovered, err = w.ExecuteFinishHistoryRequest(t.Context(), state.IntentHistoryPlan{}, recovered)
	if err != nil || recovered.Status != "completed" {
		t.Fatalf("recover %s %v", recovered.Status, err)
	}
}

func TestFinishHistorySplitsLargeMixedCommit(t *testing.T) {
	f, w := finishSizeFixture(t)
	finishWorkerGit(t, f.dir, "add", "routes.txt", "errors.txt")
	finishWorkerGit(t, f.dir, "commit", "-m", "Add combined protocol tables")
	head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	finalTree := finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}")
	w.Config.Mode, w.Config.Provider.DiffEgress = "intent", true
	transport := &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		goals := finishSizeGoals(request, request.Partition != nil)
		if request.Partition == nil {
			goals[0].LargeChangeReason = "The complete protocol reference tables accompany the same client-facing interface change."
			goals[0].Message += "\n- " + goals[0].LargeChangeReason
		}
		return json.Marshal(map[string]any{"goals": goals})
	}}
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{head}, nil, transport,
		FinishHistoryOptions{Mode: "intent", Style: "imperative", DiffEgress: true, Deadline: time.Now().Add(time.Minute), VerificationConfigDigest: w.Config.ConfigDigest})
	if err != nil || len(plan.Goals) != 2 || plan.Version != state.IntentHistoryPlanVersion {
		t.Fatalf("history retained the mixed boundary: goals=%d err=%v", len(plan.Goals), err)
	}
	raw, err := json.Marshal(plan)
	if err != nil || json.Unmarshal(raw, &plan) != nil || plan.SharedPlan.Base != nil || plan.SharedPlan.Steps[0].Tree != nil {
		t.Fatal("saved history repeated full trees or lost its compact memberships", err)
	}
	withoutReview := plan
	withoutReview.Version = 2
	if err := ValidateIntentHistoryPlan(t.Context(), f.dir, withoutReview); err == nil {
		t.Fatal("compact plan was accepted under older full-tree rules")
	}
	evidence, _, _, err := finishHistoryEvidence(t.Context(), f.dir, []string{head})
	if err != nil {
		t.Fatal(err)
	}
	var goals []finish.Goal
	for _, step := range plan.SharedPlan.Steps {
		goals = append(goals, step.Goal)
	}
	full, err := finish.Build(evidence, goals)
	if err != nil {
		t.Fatal(err)
	}
	legacy := plan
	legacy.Version, legacy.SharedPlan = 2, &full
	if err := ValidateIntentHistoryPlan(t.Context(), f.dir, legacy); err != nil {
		t.Fatal("older full-tree history plan lost compatibility", err)
	}
	request := historyStartTest(t, w, plan, false)
	completed, err := w.ExecuteFinishHistoryRequest(t.Context(), plan, request)
	if err != nil || completed.Status != "completed" {
		t.Fatalf("history publication failed: %s %v", completed.Status, err)
	}
	if tree := finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}"); tree != finalTree {
		t.Fatal("history split changed final files")
	}
}

func TestFinishHistoryCompactPlanFitsWorkerMessage(t *testing.T) {
	// Repeated full repository trees must not limit useful commit boundaries.
	large := finish.Tree{}
	for i := 0; i < 5000; i++ {
		large[strconv.Itoa(i)+strings.Repeat("a", 100)] = finish.Entry{Mode: "100644", OID: strings.Repeat("a", 40)}
	}
	plan := finish.Plan{Base: large, Target: large}
	for i := 0; i < 20; i++ {
		plan.Steps = append(plan.Steps, finish.Step{Tree: large, Goal: finish.Goal{Purpose: "Complete one independent change", ChangeIDs: []string{strconv.Itoa(i)}}})
	}
	raw, err := json.Marshal(compactFinishHistoryPlan(plan))
	if err != nil || len(raw) >= 1<<20 || len(compactFinishHistoryPlan(plan).Steps) != len(plan.Steps) {
		t.Fatalf("compact plan cannot cross worker RPC: bytes=%d err=%v", len(raw), err)
	}
}

func TestFinishHistoryRewritePreservesNewerMerge(t *testing.T) {
	f, w := finishSizeFixture(t)
	base := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	finishWorkerGit(t, f.dir, "add", "routes.txt", "errors.txt")
	finishWorkerGit(t, f.dir, "commit", "-m", "Add combined reference tables")
	selected := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	finishWorkerGit(t, f.dir, "switch", "-c", "feature", base)
	side := mustCommitPath(t, f.dir, "feature.txt", "independent merged feature\n", "Add independent feature")
	finishWorkerGit(t, f.dir, "switch", "main")
	finishWorkerGit(t, f.dir, "merge", "--no-ff", "-m", "Merge independent feature", "feature")
	merge := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	tree := finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}")
	if err := git.ValidateIntentHistoryChain(t.Context(), f.dir, []string{base}, []string{selected, merge}); err == nil || !strings.Contains(err.Error(), "retains selected commits") {
		t.Fatal("rewrite accepted selected history retained through the side branch", err)
	}
	selection, err := git.ResolveRewriteSelection(t.Context(), f.dir, git.RewriteSelectionOptions{GitRange: base + ".." + selected})
	if err != nil || len(selection.Selected) != 1 || len(selection.RecreateUnchanged) != 1 {
		t.Fatalf("cannot select work beneath newer merge: %+v %v", selection, err)
	}
	transport := &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		return json.Marshal(map[string]any{"goals": finishSizeGoals(request, request.Partition != nil)})
	}}
	w.Config.Mode, w.Config.Provider.DiffEgress = "intent", true
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{selected}, []string{merge}, transport,
		FinishHistoryOptions{Mode: "intent", Style: "imperative", DiffEgress: true, Deadline: time.Now().Add(time.Minute), VerificationConfigDigest: w.Config.ConfigDigest})
	if err != nil {
		t.Fatal(err)
	}
	r := historyStartTest(t, w, plan, false)
	completed, err := w.ExecuteFinishHistoryRequest(t.Context(), plan, r)
	if err != nil || completed.Status != "completed" {
		t.Fatalf("rewrite below newer merge failed: %s %v", completed.Status, err)
	}
	parents := strings.Fields(finishWorkerGit(t, f.dir, "show", "-s", "--format=%P", "HEAD"))
	if len(parents) != 2 || parents[1] != side || parents[0] == selected ||
		finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}") != tree ||
		finishWorkerGit(t, f.dir, "show", "-s", "--format=%B", "HEAD") != "Merge independent feature" ||
		finishWorkerGit(t, f.dir, "rev-parse", "feature") != side {
		t.Fatal("replayed merge changed its side history, message or final files")
	}
	var proof state.FinishHistoryProof
	if json.Unmarshal(completed.Proof, &proof) != nil || proof.Version != 2 {
		t.Fatal("merge-aware publication proof was not saved")
	}
	proof.Version = 1
	if err := git.ValidateFinishHistoryProof(t.Context(), f.dir, proof); err == nil {
		t.Fatal("older linear proof accepted merge parents")
	}
}

func TestFinishHistoryRewriteVerificationFailureAndFrozenIndex(t *testing.T) {
	for _, scenario := range []string{"verification_failure", "later_staging", "expired_deadline"} {
		t.Run(scenario, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			a := mustCommitPath(t, f.dir, "goal.txt", "recorded\n", "Preserve recorded goal\n\n- Keep the authored value available for rollback.")
			if scenario == "verification_failure" {
				w.Config.VerificationMode = "fast"
				command, err := verification.NewApprovedCommand(f.dir, w.Config.ConfigDigest, verification.ModeFast, "exit 7", time.Second)
				if err != nil {
					t.Fatal(err)
				}
				w.Config.Command = command
			}
			plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{a}, nil, nil, FinishHistoryOptions{Mode: "local", Style: "imperative", VerificationConfigDigest: w.Config.ConfigDigest})
			if err != nil {
				t.Fatal(err)
			}
			r := historyStartTest(t, w, plan, false)
			if scenario == "later_staging" {
				finishWrite(t, f.dir, "goal.txt", "later staged\n")
				finishWorkerGit(t, f.dir, "add", "goal.txt")
			}
			if scenario == "expired_deadline" {
				r.DeadlineTS = 1
			}
			result, err := w.ExecuteFinishHistoryRequest(t.Context(), plan, r)
			if err == nil || result.Status != "failed" {
				t.Fatalf("unsafe publication %s %v", result.Status, err)
			}
			if head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD"); head != a {
				t.Fatal("failed verification moved branch")
			}
			if _, err := git.RevParse(t.Context(), f.dir, "refs/acd/rewrite-backups/"+plan.ID); err == nil {
				t.Fatal("failed verification made backup publication")
			}
		})
	}
}

func TestFinishHistoryRewriteMessagesOnlyCorrectionPreservesMembership(t *testing.T) {
	f, _ := finishWorkerFixture(t)
	a := mustCommitPath(t, f.dir, "a.txt", "alpha\n", "Add alpha")
	finishWorkerGit(t, f.dir, "commit", "--allow-empty", "-qm", "Preserve empty checkpoint")
	empty := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	transport := &finishTestTransport{exchange: func(_ context.Context, r ai.FinishPlanRequest) ([]byte, error) {
		if r.Correction == nil {
			return []byte(`{"goals":[]}`), nil
		}
		var goals []finish.Goal
		for _, c := range r.HistoryCommits {
			goals = append(goals, finish.Goal{Purpose: "Preserve recorded outcome", Boundary: "Keep original membership", Reason: "Keep the original authored boundary.", Message: "Preserve recorded outcome\n\n- Keep the original authored boundary.", ChangeIDs: []string{c.ID}})
		}
		raw, _ := json.Marshal(map[string]any{"goals": goals})
		return raw, nil
	}}
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{a, empty}, nil, transport, FinishHistoryOptions{Mode: "intent", DiffEgress: true, Style: "imperative", MessagesOnly: true, Deadline: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Goals) != 2 || len(plan.Goals[1].Units) != 0 || transport.calls.Load() != 2 {
		t.Fatalf("membership %+v", plan)
	}
	transport.exchange = func(context.Context, ai.FinishPlanRequest) ([]byte, error) { return nil, errors.New("offline") }
	_, err = PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{a, empty}, nil, transport, FinishHistoryOptions{Mode: "intent", DiffEgress: true, Style: "imperative", MessagesOnly: true, Deadline: time.Now().Add(time.Minute)})
	if err == nil || transport.calls.Load() != 3 {
		t.Fatal("provider failure retried or fell back")
	}
}

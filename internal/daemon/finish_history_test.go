package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
			a := mustCommitPath(t, f.dir, "goal.txt", "a\n", "Add recorded goal")
			finishWorkerGit(t, f.dir, "config", "user.name", "Second Author")
			finishWorkerGit(t, f.dir, "config", "user.email", "second@example.com")
			finishWorkerGit(t, f.dir, "commit", "--allow-empty", "-qm", "Preserve empty checkpoint")
			empty := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
			b := mustCommitPath(t, f.dir, "goal.txt", "b\n", "Refine recorded goal")
			neutral := mustCommitPath(t, f.dir, "goal.txt", "a\n", "Restore recorded goal")
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
		raw, _ := json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: "Complete alpha behavior", Boundary: "Keep alpha and its correction together", Message: "Complete alpha behavior\n\n- Keep the alpha outcome and its correction together.", ChangeIDs: ids["a.txt"]}, {Purpose: "Complete beta behavior", Boundary: "Keep the independent beta outcome reviewable", Message: "Complete beta behavior\n\n- Preserve the independent beta outcome.", ChangeIDs: ids["b.txt"]}}})
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

func TestFinishHistoryRewriteVerificationFailureAndFrozenIndex(t *testing.T) {
	for _, scenario := range []string{"verification_failure", "later_staging", "expired_deadline"} {
		t.Run(scenario, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			a := mustCommitPath(t, f.dir, "goal.txt", "recorded\n", "Preserve recorded goal")
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
			goals = append(goals, finish.Goal{Purpose: "Preserve recorded outcome", Boundary: "Keep original membership", Message: "Preserve recorded outcome\n\n- Keep the original authored boundary.", ChangeIDs: []string{c.ID}})
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

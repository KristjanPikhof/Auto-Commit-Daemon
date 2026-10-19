package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func finishCaptureVersion(t *testing.T, w *FinishWorker) state.Checkpoint {
	t.Helper()
	wt, cfg, _, err := w.resolved(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cp, err := w.protect(t.Context(), wt, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	return cp
}

func TestFinishVersionReadsOnlyChangedPaths(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "selected[1].txt", "recorded version\n")
	finishWrite(t, f.dir, "unrelated.txt", "unrelated content\n")
	cp := finishCaptureVersion(t, w)
	proof := state.FinishCheckpoint{ID: cp.ID, Ref: cp.Ref, CommitOID: cp.CommitOID, TreeOID: cp.TreeOID}
	err := withFinishVersion(t.Context(), f.dir, proof, []string{"selected[1].txt", "missing.txt"}, func(_ context.Context, tree finish.Tree) error {
		if len(tree) != 1 || tree["selected[1].txt"].OID == "" {
			t.Fatalf("checkpoint read included unrelated paths or lost a literal filename: %+v", tree)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func finishDecisionFixture(t *testing.T) (*captureFixture, *FinishWorker, state.Checkpoint) {
	t.Helper()
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "auth.sh", "authenticate() { printf password; }\n")
	finishWrite(t, f.dir, "verify.sh", ". ./auth.sh\ntest \"$(authenticate local)\" = password\n")
	finishWorkerGit(t, f.dir, "add", "auth.sh", "verify.sh")
	finishWorkerGit(t, f.dir, "commit", "-m", "Seed password authentication")
	w.Config.Mode = "intent"
	w.Config.Provider.CommitStrategy = ai.CommitStrategyIntent
	w.Config.Provider.DiffEgress = true
	w.Config.Provider.Timeout = time.Minute
	finishWrite(t, f.dir, "auth.sh", "local_auth() { printf password; }\nauthenticate() { local_auth; }\n")
	version := finishCaptureVersion(t, w)
	finishWrite(t, f.dir, "auth.sh", "local_auth() { printf password; }\nauthenticate() { case \"$1\" in google) printf google;; *) local_auth;; esac; }\n")
	finishWrite(t, f.dir, "verify.sh", ". ./auth.sh\ntest \"$(authenticate local)\" = password\ntest \"$(authenticate google)\" = google\n")
	return f, w, version
}

func finishDecisionPlan(t *testing.T, request ai.FinishPlanRequest) []byte {
	t.Helper()
	goals := []finish.Goal{
		{Purpose: "Extract password authentication before adding another provider", Boundary: "Review the behavior-preserving preparation independently", Message: "Extract password authentication\n\n- Keep password behavior while preparing a second provider."},
		{Purpose: "Add Google authentication using the prepared dispatch", Boundary: "Review the new provider and its direct regression test", Message: "Add Google authentication\n\n- Dispatch Google requests while retaining password support."},
	}
	for _, change := range request.Changes {
		index := 1
		if change.Path == "auth.sh" && strings.HasPrefix(change.ID, "checkpoint-") {
			index = 0
		}
		goals[index].ChangeIDs = append(goals[index].ChangeIDs, change.ID)
	}
	if len(goals[0].ChangeIDs) != 1 || len(goals[1].ChangeIDs) != 2 {
		t.Errorf("lost same-file decisions: %+v", request.Changes)
	}
	raw, _ := json.Marshal(map[string]any{"goals": goals})
	return raw
}

func TestFinishWorkerPublishesCheckedSameFileDecisions(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(map[bool]string{false: "working steps", true: "broken intermediate"}[broken], func(t *testing.T) {
			f, w, _ := finishDecisionFixture(t)
			command := "sh verify.sh"
			if broken {
				// This command requires the final feature. It must fail on the
				// prerequisite, even though it passes on the final worktree.
				command = ". ./auth.sh; test \"$(authenticate google)\" = google"
			}
			approved, err := verification.NewApprovedCommand(f.dir, "approved-v1", verification.ModeFast, command, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			w.Config.VerificationMode, w.Config.Command = "fast", approved
			w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
				if len(request.Diffs) != 3 || strings.Contains(request.Diffs[0].Text, "+authenticate() { case") {
					t.Error("preparation diff absorbed the later feature")
				}
				return finishDecisionPlan(t, request), nil
			}}
			r := startFinishWorker(t, w, FinishOptions{Purpose: "Prepare password authentication, then add Google"})
			if len(r.Target.Checkpoints) != 1 {
				t.Fatalf("recorded versions: %+v", r.Target.Checkpoints)
			}
			finishWrite(t, f.dir, "auth.sh", "later live work\n")
			completed, err := w.Execute(t.Context(), r.ID)
			if broken {
				if err == nil || completed.State != state.FinishFailed || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
					t.Fatalf("broken step published: %+v %v", completed, err)
				}
				return
			}
			if err != nil || completed.State != state.FinishCompleted {
				t.Fatalf("same-file publication: %+v %v", completed, err)
			}
			commits := strings.Fields(finishWorkerGit(t, f.dir, "rev-list", "--reverse", r.Target.ExpectedHead+"..HEAD"))
			if len(commits) != 2 {
				t.Fatalf("want two useful steps: %v", commits)
			}
			if body := finishWorkerGit(t, f.dir, "show", commits[0]+":auth.sh"); strings.Contains(body, "google") || !strings.Contains(body, "local_auth") {
				t.Fatalf("invalid preparation: %s", body)
			}
			if finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}") != r.Target.TargetTree {
				t.Fatal("published chain changed the frozen target")
			}
			for _, oid := range commits {
				if message := finishWorkerGit(t, f.dir, "show", "-s", "--format=%B", oid); !strings.Contains(message, "\n\n- ") {
					t.Fatalf("Git lost the decision reason: %s", message)
				}
			}
			var plan FinishSavedPlan
			if err := json.Unmarshal(completed.Plan, &plan); err != nil {
				t.Fatal(err)
			}
			for _, step := range plan.Steps {
				if !step.Check.Executed || step.Check.Result.ExitCode != 0 {
					t.Fatalf("intermediate check not executed: %+v", step.Check)
				}
			}
			if body, _ := os.ReadFile(filepath.Join(f.dir, "auth.sh")); string(body) != "later live work\n" {
				t.Fatal("publication replaced later work")
			}
		})
	}
}

func TestFinishWorkerRetriesPinnedVersionsAfterRestartAndPrune(t *testing.T) {
	f, w, source := finishDecisionFixture(t)
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		return finishDecisionPlan(t, request), nil
	}}
	r := startFinishWorker(t, w, FinishOptions{Purpose: "Prepare password authentication, then add Google"})
	if _, err := w.RecoverPrepared(t.Context()); err != nil {
		t.Fatal(err)
	}
	finishWorkerGit(t, f.dir, "update-ref", "-d", source.Ref, source.CommitOID)
	if _, err := f.db.SQL().Exec("UPDATE checkpoints SET retained=0 WHERE id=?", source.ID); err != nil {
		t.Fatal(err)
	}
	finishWrite(t, f.dir, "auth.sh", "later saved work\n")
	finishCaptureVersion(t, w)
	retry, created, err := w.Start(t.Context(), FinishOptions{RetryID: r.ID})
	if err != nil || !created || !reflect.DeepEqual(retry.Target, r.Target) {
		t.Fatalf("retry changed recorded versions: %+v %v", retry, err)
	}
	pins, err := state.FinishObjectPins(t.Context(), f.db, r.Target.WorktreeID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, pin := range pins {
		found = found || pin.Ref == r.Target.Checkpoints[0].Ref
	}
	if !found {
		t.Fatal("retention lost the finish version pin")
	}
	if _, err := w.Execute(t.Context(), retry.ID); err != nil {
		t.Fatal(err)
	}
	if body := finishWorkerGit(t, f.dir, "show", "HEAD:auth.sh"); strings.Contains(body, "later") {
		t.Fatal("retry absorbed a later checkpoint")
	}
}

func TestFinishWorkerRejectsMovedVersionProof(t *testing.T) {
	f, w, _ := finishDecisionFixture(t)
	r := startFinishWorker(t, w, FinishOptions{Purpose: "Prepare password authentication, then add Google"})
	finishWorkerGit(t, f.dir, "update-ref", r.Target.Checkpoints[0].Ref, r.Target.ExpectedHead)
	provider := &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		return finishDecisionPlan(t, request), nil
	}}
	w.Transport = provider
	failed, err := w.Execute(t.Context(), r.ID)
	if err == nil || failed.State != state.FinishFailed || provider.calls.Load() != 0 || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
		t.Fatalf("moved proof accepted: %+v %v", failed, err)
	}
}

func TestFinishWorkerKeepsStagedAndExactMessageBatchesOutOfVersions(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact message", true: "staged selection"}[staged], func(t *testing.T) {
			f, w, _ := finishDecisionFixture(t)
			options := FinishOptions{Purpose: "Complete Google authentication"}
			if staged {
				finishWorkerGit(t, f.dir, "add", "auth.sh", "verify.sh")
				options.IncludeStaged = true
			} else {
				options.Message = "Add Google authentication"
			}
			r := startFinishWorker(t, w, options)
			if len(r.Target.Checkpoints) != 0 {
				t.Fatal("historical versions changed staged consent or an exact batch")
			}
			wt, _, _, _ := w.resolved(t.Context())
			evidence, _, err := w.evidence(t.Context(), wt, r.Target)
			if err != nil || len(evidence.Changes) != 2 {
				t.Fatalf("complete final transitions lost: %+v %v", evidence, err)
			}
		})
	}
}

func TestFinishWorkerRequiresDiffsEvenWithCompletionContext(t *testing.T) {
	for _, options := range []FinishOptions{{Purpose: "Publish the completed protected goal"}, {DryRun: true, Purpose: "Preview the completed protected goal"}, {Message: "Preserve completed behavior"}} {
		t.Run(fmt.Sprintf("preview_%t_message_%t", options.DryRun, options.Message != ""), func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			finishWrite(t, f.dir, "goal.txt", "protected\n")
			w.Config.Mode = "intent"
			w.Config.Provider.CommitStrategy = ai.CommitStrategyIntent
			provider := &finishTestTransport{exchange: func(context.Context, ai.FinishPlanRequest) ([]byte, error) {
				return nil, errors.New("must not call provider")
			}}
			w.Transport = provider
			r := startFinishWorker(t, w, options)
			failed, err := w.Execute(t.Context(), r.ID)
			if err == nil || failed.ReasonCode != "provider_diffs_required" || failed.State != state.FinishFailed || provider.calls.Load() != 0 {
				t.Fatalf("request without diffs guessed intent: %+v %v", failed, err)
			}
			if _, err := state.ResolveCheckpoint(t.Context(), f.db.Path(), r.Target.CheckpointID); err != nil {
				t.Fatal("context failure lost protected work")
			}
			if finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
				t.Fatal("insufficient context moved the branch")
			}
		})
	}
}

package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

type finishTestTransport struct {
	calls    atomic.Int32
	exchange func(context.Context, ai.FinishPlanRequest) ([]byte, error)
}

func (*finishTestTransport) Name() string { return "finish-test" }
func (*finishTestTransport) EncodeFinishRequest(r ai.FinishPlanRequest) ([]byte, error) {
	return json.Marshal(r)
}
func (p *finishTestTransport) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	p.calls.Add(1)
	var request ai.FinishPlanRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, err
	}
	return p.exchange(ctx, request)
}

func finishWorkerFixture(t *testing.T) (*captureFixture, *FinishWorker) {
	t.Helper()
	f := newCaptureFixture(t)
	w := &FinishWorker{RepoPath: f.dir, DB: f.db, ProtectionGate: &sync.Mutex{}, EvaluationInterval: time.Hour, Config: FinishWorkerConfig{Mode: "local", Style: "imperative", ConfigDigest: "approved-v1", AuthorName: "Finish Author", AuthorEmail: "finish@example.com", VerificationMode: "none", VerificationTimeout: 10 * time.Second, Capture: CaptureOpts{IgnoreChecker: f.ig, SensitiveMatcher: f.matcher}}}
	return f, w
}
func finishWrite(t *testing.T, repo, path, body string) {
	t.Helper()
	full := filepath.Join(repo, path)
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
func finishWorkerGit(t *testing.T, repo string, args ...string) string {
	t.Helper()
	out, err := git.Run(context.Background(), git.RunOpts{Dir: repo}, args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}
func startFinishWorker(t *testing.T, w *FinishWorker, options FinishOptions) state.FinishRequest {
	t.Helper()
	r, created, err := w.Start(context.Background(), options)
	if err != nil || !created {
		t.Fatalf("start %+v created=%t: %v", r, created, err)
	}
	return r
}
func completeFinishTestPlan(request ai.FinishPlanRequest) []byte {
	ids := make([]string, 0, len(request.Changes))
	for _, change := range request.Changes {
		ids = append(ids, change.ID)
	}
	raw, _ := json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: "Preserve completed changes", Message: "Preserve completed changes", ChangeIDs: ids}}})
	return raw
}

func TestFinishWorkerLocalFreezePreviewAndPublication(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish", true: "preview"}[dryRun], func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			finishWrite(t, f.dir, "goal.txt", "frozen\n")
			head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
			index, _ := git.IndexContentDigest(context.Background(), f.dir)
			r := startFinishWorker(t, w, FinishOptions{DryRun: dryRun, Purpose: "completed goal"})
			finishWrite(t, f.dir, "goal.txt", "later\n")
			reconnected, created, err := w.Start(context.Background(), FinishOptions{DryRun: dryRun, Purpose: "completed goal"})
			if err != nil || created || reconnected.ID != r.ID || reconnected.Target.CheckpointID != r.Target.CheckpointID {
				t.Fatalf("reconnect %+v created=%t: %v", reconnected, created, err)
			}
			if _, _, err := w.Start(context.Background(), FinishOptions{DryRun: dryRun, Purpose: "different"}); !errors.Is(err, state.ErrFinishBusy) {
				t.Fatalf("different caller: %v", err)
			}
			completed, err := w.Execute(context.Background(), r.ID)
			if err != nil || completed.State != state.FinishCompleted {
				t.Fatalf("execute %+v: %v", completed, err)
			}
			var result FinishResult
			if err := json.Unmarshal(completed.Result, &result); err != nil {
				t.Fatal(err)
			}
			if result.Published == dryRun || result.DryRun != dryRun {
				t.Fatalf("outcome %+v", result)
			}
			var plan FinishSavedPlan
			if err := json.Unmarshal(completed.Plan, &plan); err != nil {
				t.Fatal(err)
			}
			if len(plan.Steps) != 1 || plan.Steps[0].Check.Executed || plan.Steps[0].Check.Result.Executed || plan.Steps[0].Check.Result.Mode != verification.ModeStructural {
				t.Fatalf("invented check %+v", plan)
			}
			if body, _ := os.ReadFile(filepath.Join(f.dir, "goal.txt")); string(body) != "later\n" {
				t.Fatal("later bytes replaced")
			}
			if dryRun {
				if current := finishWorkerGit(t, f.dir, "rev-parse", "HEAD"); current != head {
					t.Fatal("preview moved branch")
				}
				if current, _ := git.IndexContentDigest(context.Background(), f.dir); current != index {
					t.Fatal("preview changed index")
				}
			}
			if body := finishWorkerGit(t, f.dir, "show", r.Target.TargetTree+":goal.txt"); body != "frozen" {
				t.Fatalf("target absorbed later bytes %q", body)
			}
			fresh := startFinishWorker(t, w, FinishOptions{DryRun: true})
			if fresh.Target.CheckpointID == r.Target.CheckpointID {
				t.Fatal("new finish reused source checkpoint")
			}
		})
	}
}

func TestFinishWorkerStagedOverlayAndExcludedBase(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, ".env", "protected branch secret\n")
	finishWrite(t, f.dir, "goal.txt", "baseline\n")
	finishWorkerGit(t, f.dir, "add", "-f", ".env", "goal.txt")
	finishWorkerGit(t, f.dir, "commit", "-m", "Seed tracked inputs")
	finishWrite(t, f.dir, ".env", "later secret\n")
	finishWrite(t, f.dir, "goal.txt", "approved staging\n")
	finishWorkerGit(t, f.dir, "add", "goal.txt")
	finishWrite(t, f.dir, "goal.txt", "unstaged later\n")
	if _, _, err := w.Start(context.Background(), FinishOptions{}); !errors.Is(err, git.ErrFinishStaged) {
		t.Fatalf("default staged consent: %v", err)
	}
	r := startFinishWorker(t, w, FinishOptions{IncludeStaged: true})
	if r.Target.TargetTree == r.Target.CheckpointTree || r.Target.TargetRef == "" {
		t.Fatal("overlay lacks distinct private target pin")
	}
	if body := finishWorkerGit(t, f.dir, "show", r.Target.CheckpointTree+":goal.txt"); body != "unstaged later" {
		t.Fatal("source checkpoint overwritten")
	}
	if body := finishWorkerGit(t, f.dir, "show", r.Target.TargetTree+":goal.txt"); body != "approved staging" {
		t.Fatal("staged bytes replaced with worktree")
	}
	if body := finishWorkerGit(t, f.dir, "show", r.Target.TargetTree+":.env"); body != "protected branch secret" {
		t.Fatal("excluded tracked bytes deleted or exposed")
	}
	completed, err := w.Execute(context.Background(), r.ID)
	if err != nil || completed.State != state.FinishCompleted {
		t.Fatalf("overlay %+v: %v", completed, err)
	}
	if body, _ := os.ReadFile(filepath.Join(f.dir, "goal.txt")); string(body) != "unstaged later\n" {
		t.Fatal("unstaged bytes lost")
	}
	if body, _ := os.ReadFile(filepath.Join(f.dir, ".env")); string(body) != "later secret\n" {
		t.Fatal("sensitive live bytes changed")
	}
	finishWorkerGit(t, f.dir, "add", "-f", ".env")
	if _, _, err := w.Start(context.Background(), FinishOptions{IncludeStaged: true}); err == nil || !strings.Contains(err.Error(), "staged_scope_excluded") {
		t.Fatalf("excluded staged consent bypass: %v", err)
	}
}

func TestFinishWorkerProtectsDuringProviderAndReconnects(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "goal.txt", "frozen\n")
	entered, release := make(chan struct{}), make(chan struct{})
	wake := make(chan struct{}, 1)
	w.Wake = wake
	w.OperationGate = &sync.RWMutex{}
	p := &finishTestTransport{exchange: func(ctx context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		close(entered)
		select {
		case <-release:
			return completeFinishTestPlan(request), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	w.Transport = p
	w.Config.Mode = "intent"
	w.Config.Provider.CommitStrategy = ai.CommitStrategyIntent
	w.Config.Provider.Timeout = 10 * time.Second
	r := startFinishWorker(t, w, FinishOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		w.OperationGate.RLock()
		defer w.OperationGate.RUnlock()
		_, err := w.Execute(ctx, r.ID)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("provider not called")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("provider job did not join")
		}
	})
	finishWrite(t, f.dir, "later.txt", "protected while waiting\n")
	wake <- struct{}{}
	var protected string
	waitFor(t, 3*time.Second, "checkpoint during provider", func() bool {
		id, ok, _ := state.MetaGet(context.Background(), f.db, MetaKeyProtectionCheckpointID)
		if !ok || id == r.Target.CheckpointID {
			return false
		}
		cp, err := state.ResolveCheckpoint(context.Background(), f.db.Path(), id)
		if err != nil {
			return false
		}
		_, err = git.Run(context.Background(), git.RunOpts{Dir: f.dir}, "cat-file", "-e", cp.TreeOID+":later.txt")
		if err == nil {
			protected = id
		}
		return err == nil
	})
	if protected == "" {
		t.Fatal("later protection absent")
	}
	attached, err := w.Execute(context.Background(), r.ID)
	if err != nil || attached.ID != r.ID || attached.State != state.FinishRunning {
		t.Fatalf("detached client attachment %+v: %v", attached, err)
	}
	if p.calls.Load() != 1 {
		t.Fatal("duplicate provider request")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("request did not complete")
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:later.txt"); err == nil {
		t.Fatal("later protected bytes entered frozen publication")
	}
}

func TestFinishWorkerBoundedFailureAndExplicitRetry(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "goal.txt", "frozen\n")
	w.Config.Mode = "intent"
	w.Config.Provider.CommitStrategy = ai.CommitStrategyIntent
	p := &finishTestTransport{exchange: func(context.Context, ai.FinishPlanRequest) ([]byte, error) { return []byte(`{"goals":[]}`), nil }}
	w.Transport = p
	r := startFinishWorker(t, w, FinishOptions{})
	failed, err := w.Execute(context.Background(), r.ID)
	if err == nil || failed.State != state.FinishFailed || failed.PlanningCalls != 2 || p.calls.Load() != 2 {
		t.Fatalf("bounded failure %+v calls=%d: %v", failed, p.calls.Load(), err)
	}
	var disclosed FinishSavedPlan
	if err := json.Unmarshal(failed.Plan, &disclosed); err != nil || len(disclosed.Disclosure.RequestBytes) != 2 {
		t.Fatalf("failed request egress lost %+v: %v", disclosed, err)
	}
	finishWrite(t, f.dir, "goal.txt", "later\n")
	p.exchange = func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		return completeFinishTestPlan(request), nil
	}
	retry, created, err := w.Start(context.Background(), FinishOptions{RetryID: r.ID, DryRun: true})
	if err != nil || !created || retry.Attempt != 2 || retry.Target != r.Target {
		t.Fatalf("retry %+v created=%t: %v", retry, created, err)
	}
	completed, err := w.Execute(context.Background(), retry.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.Target.TargetTree != r.Target.TargetTree {
		t.Fatalf("retry preview %+v: %v", completed, err)
	}
	if finishWorkerGit(t, f.dir, "show", r.Target.TargetTree+":goal.txt") != "frozen" {
		t.Fatal("retry absorbed new work")
	}
}

func TestFinishWorkerCommandChecksAndRestart(t *testing.T) {
	for _, scenario := range []string{"passed", "failed", "running_restart", "prepared_restart"} {
		t.Run(scenario, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			finishWrite(t, f.dir, "goal.txt", "approved\n")
			r := startFinishWorker(t, w, FinishOptions{})
			if scenario == "running_restart" {
				recovery := &FinishWorker{RepoPath: f.dir, DB: f.db}
				if _, err := recovery.RecoverPrepared(context.Background()); err != nil {
					t.Fatal(err)
				}
				saved, _ := state.LoadFinishRequest(context.Background(), f.db, r.ID)
				if saved.State != state.FinishFailed {
					t.Fatal("restart resumed provider/planning")
				}
				return
			}
			if scenario == "prepared_restart" {
				wt, err := git.ResolveWorktree(context.Background(), f.dir)
				if err != nil {
					t.Fatal(err)
				}
				p, err := git.BuildFinishChain(context.Background(), f.dir, git.FinishBuildOptions{RequestID: r.ID, Attempt: r.Attempt, WorktreeID: r.Target.WorktreeID, BranchRef: r.Target.BranchRef, ExpectedHead: r.Target.ExpectedHead, TargetTree: r.Target.TargetTree, IndexDigest: r.Target.IndexDigest, ObjectRef: git.FinishRefPrefix + r.ID + "/chain", Commits: []git.FinishCommit{{TreeOID: r.Target.TargetTree, Message: "Complete protected goal"}}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := state.PrepareFinishPublication(context.Background(), f.db, r.ID, r.Attempt, p, json.RawMessage(`{"steps":1}`)); err != nil {
					t.Fatal(err)
				}
				if _, err := git.ApplyFinishPublication(context.Background(), wt.Root, p, false); err != nil {
					t.Fatal(err)
				}
				recovery := &FinishWorker{RepoPath: f.dir, DB: f.db}
				saved, err := recovery.RecoverPrepared(context.Background())
				if err != nil || saved.State != state.FinishCompleted {
					t.Fatalf("startup recovery %+v: %v", saved, err)
				}
				return
			}
			// Configure the approved command before freezing the actual checked request.
			if _, err := state.InterruptFinishRequests(context.Background(), f.db, r.Target.WorktreeID); err != nil {
				t.Fatal(err)
			}
			command := "test \"$(cat goal.txt)\" = approved"
			if scenario == "failed" {
				command = "exit 7"
			}
			approved, err := verification.NewApprovedCommand(f.dir, "approved-v2", verification.ModeFast, command, 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			w.Config.VerificationMode = "fast"
			w.Config.Command = approved
			r = startFinishWorker(t, w, FinishOptions{})
			completed, err := w.Execute(context.Background(), r.ID)
			if scenario == "failed" {
				if err == nil || completed.State != state.FinishFailed {
					t.Fatalf("check failure %+v: %v", completed, err)
				}
				if head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD"); head != r.Target.ExpectedHead {
					t.Fatal("failed candidate published")
				}
			} else {
				if err != nil || completed.State != state.FinishCompleted {
					t.Fatalf("check passed %+v: %v", completed, err)
				}
			}
			var plan FinishSavedPlan
			if err := json.Unmarshal(completed.Plan, &plan); err != nil {
				t.Fatal(err)
			}
			if len(plan.Steps) != 1 || !plan.Steps[0].Check.Executed || plan.Steps[0].Check.Result.CommandDigest != approved.Identity().Digest || plan.Steps[0].Check.Result.EnvironmentDigest == "" {
				t.Fatalf("check provenance %+v", plan)
			}
		})
	}
}

func TestFinishWorkerExactProviderEvidence(t *testing.T) {
	f, w := finishWorkerFixture(t)
	for path, body := range map[string]string{"*.txt": "wildcard file\n", "other.txt": "other file\n", ":(exclude)config.go": "exclude named file\n", "binary.dat": strings.Repeat("x", 9000) + "\x00tail", "unicode.txt": strings.Repeat("界", 4000)} {
		finishWrite(t, f.dir, path, body)
	}
	r := startFinishWorker(t, w, FinishOptions{DryRun: true})
	wt, cfg, _, err := w.resolved(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	evidence, base, err := w.evidence(context.Background(), wt, r.Target, cfg)
	if err != nil {
		t.Fatal(err)
	}
	diffs, err := finishProviderDiffs(context.Background(), f.dir, base, r.Target.TargetTree, evidence.Changes, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, diff := range diffs {
		if diff.ChangeID == "binary.dat" {
			if !diff.Binary || diff.Text != "" || diff.AfterSize != 9005 {
				t.Fatalf("binary bytes sent %+v", diff)
			}
			continue
		}
		if diff.ChangeID == "unicode.txt" && (diff.Binary || !strings.Contains(diff.Text, "界")) {
			t.Fatal("truncated UTF8 prefix hid valid textual evidence")
		}
		if diff.ChangeID == "*.txt" && strings.Contains(diff.Text, "other file") {
			t.Fatal("wildcard diff included another path")
		}
		if diff.ChangeID == ":(exclude)config.go" && (!strings.Contains(diff.Text, "exclude named file") || strings.Contains(diff.Text, "other file")) {
			t.Fatal("exclude filename interpreted as pathspec")
		}
	}
	if len(diffs) != 5 {
		t.Fatalf("evidence missing %d", len(diffs))
	}
}

func TestFinishWorkerRetainsRenameAtomicBoundary(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "old.txt", "rename content\n")
	finishWorkerGit(t, f.dir, "add", "old.txt")
	finishWorkerGit(t, f.dir, "commit", "-m", "Seed rename")
	if err := os.Rename(filepath.Join(f.dir, "old.txt"), filepath.Join(f.dir, "new.txt")); err != nil {
		t.Fatal(err)
	}
	r := startFinishWorker(t, w, FinishOptions{DryRun: true})
	wt, cfg, _, _ := w.resolved(context.Background())
	evidence, _, err := w.evidence(context.Background(), wt, r.Target, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Changes) != 2 || evidence.Changes[0].AtomicGroup == "" || evidence.Changes[0].AtomicGroup != evidence.Changes[1].AtomicGroup {
		t.Fatalf("rename split evidence %+v", evidence.Changes)
	}
	if _, err := finish.Build(evidence, []finish.Goal{{Purpose: "Delete source", Message: "Delete source", ChangeIDs: []string{evidence.Changes[0].ID}}, {Purpose: "Create target", Boundary: "Independent completed goal", Message: "Create target", ChangeIDs: []string{evidence.Changes[1].ID}}}); err == nil {
		t.Fatal("rename allowed split publication")
	}
}

func TestFinishWorkerCredentialRefreshRetriesSameTarget(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "goal.txt", "frozen\n")
	calls := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("Authorization") != "Bearer refreshed-token" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		plan := string(completeFinishTestPlan(ai.FinishPlanRequest{Changes: []finish.Change{{ID: "goal.txt"}}}))
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"type": "function", "function": map[string]string{"name": "finish_plan", "arguments": plan}}}}}}})
	}))
	defer server.Close()
	w.Config.Mode = "intent"
	w.Config.Provider = ai.ProviderConfig{Mode: "openai-compat", BaseURL: server.URL, Model: "test-model", Timeout: 3 * time.Second, CommitStrategy: ai.CommitStrategyIntent, CommitFormat: ai.CommitFormatImperative}
	w.Config.Approved = []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials, ai.ConfirmationInsecureEndpointCredentials}
	r := startFinishWorker(t, w, FinishOptions{DryRun: true})
	failed, err := w.Execute(context.Background(), r.ID)
	if err == nil || failed.State != state.FinishFailed || calls.Load() != 0 {
		t.Fatalf("missing credential %+v calls=%d: %v", failed, calls.Load(), err)
	}
	w.Config.Provider.APIKey = "refreshed-token"
	retry, created, err := w.Start(context.Background(), FinishOptions{RetryID: r.ID, DryRun: true})
	if err != nil || !created || retry.Target != r.Target {
		t.Fatalf("credential retry %+v: %v", retry, err)
	}
	completed, err := w.Execute(context.Background(), retry.ID)
	if err != nil || completed.State != state.FinishCompleted || calls.Load() != 1 {
		t.Fatalf("credential refreshed %+v calls=%d: %v", completed, calls.Load(), err)
	}
}

func TestFinishWorkerPlanningDeadlineAndInputLimit(t *testing.T) {
	for _, scenario := range []string{"expired", "oversized"} {
		t.Run(scenario, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			body := "goal\n"
			if scenario == "oversized" {
				body = strings.Repeat("x", ai.IntentStageDiffCap+1024)
			}
			finishWrite(t, f.dir, "goal.txt", body)
			p := &finishTestTransport{exchange: func(_ context.Context, r ai.FinishPlanRequest) ([]byte, error) { return completeFinishTestPlan(r), nil }}
			w.Transport = p
			w.Config.Mode = "intent"
			w.Config.Provider.CommitStrategy = ai.CommitStrategyIntent
			w.Config.Provider.Timeout = time.Second
			if scenario == "expired" {
				w.Config.Provider.Timeout = 100 * time.Millisecond
			} else {
				w.Config.Provider.DiffEgress = true
			}
			r := startFinishWorker(t, w, FinishOptions{})
			if scenario == "expired" {
				ctx, cancel := context.WithDeadline(context.Background(), finishDeadline(r))
				<-ctx.Done()
				cancel()
			}
			failed, err := w.Execute(context.Background(), r.ID)
			if err == nil || failed.State != state.FinishFailed || p.calls.Load() != 0 {
				t.Fatalf("bounded preparation %+v calls=%d: %v", failed, p.calls.Load(), err)
			}
			if scenario == "expired" && failed.ReasonCode != "provider_deadline" {
				t.Fatalf("deadline reason %s", failed.ReasonCode)
			}
			if scenario == "oversized" && (failed.ReasonCode != "provider_input_too_large" || !strings.Contains(failed.Reason, "local finish") || !strings.Contains(failed.Reason, "bytes")) {
				t.Fatalf("input guidance %s %s", failed.ReasonCode, failed.Reason)
			}
			if head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD"); head != r.Target.ExpectedHead {
				t.Fatal("failed preparation published")
			}
		})
	}
}

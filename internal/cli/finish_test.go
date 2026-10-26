package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func finishCLIFixture(t *testing.T) (controlRepoLookup, *workerRuntime, *repositoryWorkerHandler) {
	t.Helper()
	roots := withIsolatedHome(t)
	// macOS Unix sockets have a short path limit; keep only the isolated socket
	// root short, without sharing HOME, repository, or runtime state.
	socketRoot, err := os.MkdirTemp("/tmp", "acd-finish-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("XDG_STATE_HOME", socketRoot)
	roots, err = paths.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	repo, _, db := makeRepoStateDB(t)
	commitStartRepoSeed(t, repo)
	registerProtectedControlRepo(t, roots, repo)
	lookup, err := loadControlRepo(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &workerRuntime{record: lookup.Record, worktree: lookup.Worktree, db: db, roots: roots, workerCtx: ctx, gate: &sync.RWMutex{}, protectionGate: &sync.Mutex{}}
	handler := &repositoryWorkerHandler{repositoryID: lookup.Record.RepositoryID, runtimes: map[string]*workerRuntime{lookup.Record.WorktreeID: runtime}}
	t.Cleanup(func() { cancel(); runtime.finishMu.Lock(); runtime.finishMu.Unlock(); runtime.finishJobs.Wait() })
	return lookup, runtime, handler
}

func TestFinishCLIPreviewExplainsEvidenceAndDecisionBoundary(t *testing.T) {
	lookup, _, _ := finishCLIFixture(t)
	plan, _ := json.Marshal(daemon.FinishSavedPlan{Disclosure: ai.FinishDisclosure{MetadataIncluded: true}, Steps: []daemon.FinishPlanStep{{Goal: finish.Goal{
		Message:  "Preserve selected models\n\n- Preference edits should not reset the selected model.",
		Boundary: "Review model selection separately from Fast requests",
	}}}})
	r := state.FinishRequest{ID: "finish-preview", State: state.FinishCompleted, DryRun: true, Target: state.FinishTarget{Mode: "intent"}, Plan: plan, Result: json.RawMessage(`{"dry_run":true}`)}
	var out bytes.Buffer
	if err := renderFinish(&out, lookup, r, false, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"code diffs were not shared", "Preference edits should not reset", "Boundary: Review model selection separately"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("preview omitted %q:\n%s", want, out.String())
		}
	}
}

func TestFinishCLIHelpDistinguishesContextFromExactBatch(t *testing.T) {
	var out bytes.Buffer
	cmd := newRootCmd()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"finish", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"what changed and why", "disables automatic splitting", "Create one batch commit with this exact message"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("help omitted %q:\n%s", want, out.String())
		}
	}
}

func TestFinishCLIContextFailureNeedsANewRequest(t *testing.T) {
	lookup, _, _ := finishCLIFixture(t)
	r := state.FinishRequest{ID: "finish-context", State: state.FinishFailed, ReasonCode: "provider_context_required", Reason: "Supply what changed and why", Target: state.FinishTarget{CheckpointID: "saved"}}
	var out bytes.Buffer
	err := renderFinish(&out, lookup, r, true, "")
	var response productEnvelope
	if decodeErr := json.Unmarshal(out.Bytes(), &response); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if err == nil || response.Error == nil || response.Error.Retryable || response.NextAction == nil || !strings.Contains(*response.NextAction, "what changed and why") || strings.Contains(out.String(), "--retry") {
		t.Fatalf("unchanged retry suggested for missing context: %s %v", out.String(), err)
	}
}

func TestFinishCLIDiffApprovalFailureNeedsANewRequest(t *testing.T) {
	lookup, _, _ := finishCLIFixture(t)
	r := state.FinishRequest{ID: "finish-diffs", DryRun: true, State: state.FinishFailed, ReasonCode: "provider_diffs_required", Reason: ai.ErrIntentDiffsRequired.Error(), Target: state.FinishTarget{CheckpointID: "saved"}}
	var out bytes.Buffer
	err := renderFinish(&out, lookup, r, true, "")
	var response productEnvelope
	if decodeErr := json.Unmarshal(out.Bytes(), &response); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if err == nil || response.Error == nil || response.Error.Code != "provider_diffs_required" || response.Error.Retryable || response.NextAction == nil || !strings.Contains(*response.NextAction, "acd config") || strings.Contains(out.String(), "--retry") {
		t.Fatalf("unchanged retry suggested for missing code sharing: %s %v", out.String(), err)
	}
}

func finishCLIExecute(t *testing.T, ctx context.Context, repo string, args ...string) (finishCLIResult, string, error) {
	t.Helper()
	var out, progress bytes.Buffer
	root := newRootCmd()
	root.SetOut(&out)
	root.SetErr(&progress)
	root.SetArgs(append(args, "--repo", repo, "--json"))
	err := root.ExecuteContext(ctx)
	var envelope struct {
		Data finishCLIResult `json:"data"`
	}
	if out.Len() > 0 {
		if decodeErr := json.Unmarshal(out.Bytes(), &envelope); decodeErr != nil {
			t.Fatalf("one JSON envelope: %v\n%s", decodeErr, out.String())
		}
	}
	return envelope.Data, progress.String(), err
}

func TestFinishCLIPublishPreviewNoopAndStagedConsent(t *testing.T) {
	for _, scenario := range []string{"publish", "preview", "noop", "staged-refusal", "include-staged", "compatibility"} {
		t.Run(scenario, func(t *testing.T) {
			lookup, _, handler := finishCLIFixture(t)
			serveIntentHistoryWorker(t, lookup, handler)
			head, _ := git.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
			if scenario != "noop" {
				if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "goal.txt"), []byte("goal\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			args := []string{"finish", "completed goal"}
			if scenario == "preview" {
				args = append(args, "--dry-run")
			}
			if strings.Contains(scenario, "staged") || scenario == "compatibility" {
				if _, err := git.Run(t.Context(), git.RunOpts{Dir: lookup.Worktree.Root}, "add", "goal.txt"); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "include-staged" {
				args = append(args, "--include-staged")
			}
			if scenario == "compatibility" {
				args = []string{"commit-all", "--yes"}
			}
			index, _ := git.IndexContentDigest(t.Context(), lookup.Worktree.Root)
			result, progress, err := finishCLIExecute(t, t.Context(), lookup.Worktree.Root, args...)
			if scenario == "staged-refusal" {
				if ExitCode(err) != ExitActionRequired {
					t.Fatalf("staging refusal: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("finish: %v", err)
			}
			if !result.Protected || result.State != state.FinishCompleted {
				t.Fatalf("result %+v", result)
			}
			current, _ := git.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
			if scenario == "preview" || scenario == "noop" {
				if result.Published || current != head {
					t.Fatalf("preview/noop moved branch %+v", result)
				}
				if after, _ := git.IndexContentDigest(t.Context(), lookup.Worktree.Root); after != index {
					t.Fatal("preview changed index")
				}
				if scenario == "noop" && !result.NoChanges {
					t.Fatal("noop not reported")
				}
			} else if !result.Published || result.CommitsCreated != 1 || len(result.CommitOIDs) != 1 || result.CommitOIDs[0] != current {
				t.Fatalf("actual OIDs %+v", result)
			}
			if scenario == "compatibility" && (!result.StagedConsumed || !strings.Contains(progress, "deprecated")) {
				t.Fatalf("alias: %+v %s", result, progress)
			}
		})
	}
}

type finishDisconnectHandler struct {
	inner  supervisor.WorkerHandler
	cancel context.CancelFunc
}

func (h finishDisconnectHandler) HandleWorkerRequest(ctx context.Context, request supervisor.Request) (any, *supervisor.ProtocolError) {
	result, err := h.inner.HandleWorkerRequest(ctx, request)
	if request.Method == "finish_start" {
		h.cancel()
	}
	return result, err
}

func TestFinishCLIClientDisconnectAndRetry(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "goal.txt"), []byte("saved\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// A client ends immediately after freeze; the detached worker must finish.
	clientCtx, cancel := context.WithCancel(context.Background())
	serveIntentHistoryWorker(t, lookup, finishDisconnectHandler{handler, cancel})
	_, _, _ = finishCLIExecute(t, clientCtx, lookup.Worktree.Root, "finish")
	joined := make(chan struct{})
	go func() {
		runtime.finishJobs.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(30 * time.Second):
		t.Fatal("detached finish worker did not join within its test budget")
	}
	r, err := state.LatestFinishRequest(t.Context(), runtime.db, lookup.Record.WorktreeID)
	if err != nil || r.State != state.FinishCompleted {
		t.Fatalf("worker did not survive disconnect: %+v %v", r, err)
	}
	if len(r.Result) == 0 {
		t.Fatal("missing durable terminal result")
	}
	// Successful publication cannot be replayed through retry.
	_, _, err = finishCLIExecute(t, t.Context(), lookup.Worktree.Root, "finish", "--retry", r.ID)
	if ExitCode(err) != ExitActionRequired {
		t.Fatalf("replayed completed publication: %v", err)
	}
}

func TestFinishCLIRetryFrozenFailureAndReadOnlyStatus(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "goal.txt"), []byte("frozen\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolveFinishWorkerConfig(t.Context(), lookup.Roots, runtime)
	if err != nil {
		t.Fatal(err)
	}
	w := daemon.FinishWorker{RepoPath: lookup.Worktree.Root, RepositoryID: lookup.Record.RepositoryID, DB: runtime.db, Config: cfg, ProtectionGate: runtime.protectionGate}
	r, _, err := w.Start(t.Context(), daemon.FinishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, err = state.UpdateFinishRequest(t.Context(), runtime.db, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishFailed, ReasonCode: "provider_unavailable", Reason: "temporary failure"})
	if err != nil {
		t.Fatal(err)
	}
	before, _, _ := state.MetaGet(t.Context(), runtime.db, daemon.MetaKeyProtectionObservationEpoch)
	raw, _ := json.Marshal(map[string]string{"request_id": r.ID})
	if _, failure := handler.HandleWorkerRequest(t.Context(), supervisor.Request{Method: "finish_status", RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID, Params: raw}); failure != nil {
		t.Fatal(failure)
	}
	after, _, _ := state.MetaGet(t.Context(), runtime.db, daemon.MetaKeyProtectionObservationEpoch)
	if before != after {
		t.Fatal("status mutated observation")
	}
	if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "goal.txt"), []byte("later\n"), 0600); err != nil {
		t.Fatal(err)
	}
	serveIntentHistoryWorker(t, lookup, handler)
	result, _, err := finishCLIExecute(t, t.Context(), lookup.Worktree.Root, "finish", "--retry", r.ID)
	if err != nil || result.Attempt != 2 || result.Target.TargetTree != r.Target.TargetTree || !result.Published {
		t.Fatalf("retry %+v: %v", result, err)
	}
	body, err := git.Run(t.Context(), git.RunOpts{Dir: lookup.Worktree.Root}, "show", "HEAD:goal.txt")
	if err != nil || string(body) != "frozen\n" {
		t.Fatalf("retry target %q: %v", body, err)
	}
}

func TestFinishCLIFlagsAndProtectionRemedy(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	_, _, err := finishCLIExecute(t, t.Context(), repo, "finish")
	if ExitCode(err) != ExitActionRequired || !strings.Contains(err.Error(), "acd on") {
		t.Fatalf("remedy: %v", err)
	}
	for _, args := range [][]string{{"finish", "--retry", "id", "--message", ""}, {"finish", "--retry", "id", "--include-staged=false"}, {"commit-all"}} {
		_, _, err := finishCLIExecute(t, t.Context(), repo, args...)
		if ExitCode(err) != ExitInvalid {
			t.Fatalf("flags %v: %v", args, err)
		}
	}
}

func TestFinishCLIReconnectIgnoresChangedCredentials(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	finishConfigRevision(t, runtime.db, map[string]any{config.FieldCommitStrategy: "intent", config.FieldProvider: "openai-compat", config.FieldDiffEgress: true, "confirmations": []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress}}, sql.NullInt64{}, true)
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "test-frozen-key")
	if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "goal.txt"), []byte("saved\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolveFinishWorkerConfig(t.Context(), lookup.Roots, runtime)
	if err != nil {
		t.Fatal(err)
	}
	w := daemon.FinishWorker{RepoPath: lookup.Worktree.Root, RepositoryID: lookup.Record.RepositoryID, DB: runtime.db, Config: cfg, ProtectionGate: runtime.protectionGate}
	r, _, err := w.Start(t.Context(), daemon.FinishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(ai.EnvAPIKey, "")
	store := credentials.NewStore(lookup.Roots)
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.Path(), []byte("invalid credential document"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveFinishWorkerConfig(t.Context(), lookup.Roots, runtime); err == nil {
		t.Fatal("fixture must reject new credential resolution")
	}
	for _, params := range []finishStartParams{{}, {RetryID: r.ID}} {
		raw, _ := json.Marshal(params)
		got, failure := handler.HandleWorkerRequest(t.Context(), supervisor.Request{Method: "finish_start", RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID, Params: raw})
		if failure != nil {
			t.Fatal(failure)
		}
		reconnected := got.(state.FinishRequest)
		if reconnected.ID != r.ID || reconnected.Attempt != r.Attempt {
			t.Fatalf("changed attempt %+v", reconnected)
		}
	}
}

func TestFinishCLIUnavailableOutputAndExit(t *testing.T) {
	lookup, runtime, _ := finishCLIFixture(t)
	if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "goal.txt"), []byte("saved\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolveFinishWorkerConfig(t.Context(), lookup.Roots, runtime)
	if err != nil {
		t.Fatal(err)
	}
	w := daemon.FinishWorker{RepoPath: lookup.Worktree.Root, RepositoryID: lookup.Record.RepositoryID, DB: runtime.db, Config: cfg}
	r, _, err := w.Start(t.Context(), daemon.FinishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"provider_deadline", "provider_failure", "status-unavailable"} {
		attempt := r
		waitCode := ""
		if code == "status-unavailable" {
			waitCode = "finish_status_unavailable"
		} else {
			attempt.State = state.FinishFailed
			attempt.ReasonCode = code
			attempt.Reason = "temporary provider failure"
		}
		var out bytes.Buffer
		err := renderFinish(&out, lookup, attempt, true, waitCode)
		if ExitCode(err) != ExitUnavailable || !ErrorRendered(err) {
			t.Fatalf("%s exit: %v", code, err)
		}
		if strings.Contains(out.String(), "worker continues") || !strings.Contains(out.String(), r.ID) {
			t.Fatalf("false liveness or missing request: %s", out.String())
		}
	}
}

func TestFinishCLIInputLimitNeedsNewRequest(t *testing.T) {
	lookup, _, _ := finishCLIFixture(t)
	r := state.FinishRequest{ID: "finish-large", State: state.FinishFailed, ReasonCode: "provider_input_too_large", Reason: "Request exceeds ai.max_input_bytes"}
	var out bytes.Buffer
	if err := renderFinish(&out, lookup, r, false, ""); err == nil {
		t.Fatal("expected unsuccessful finish")
	}
	if strings.Contains(out.String(), "--retry") || !strings.Contains(out.String(), "Review ai.max_input_bytes") {
		t.Fatalf("misleading size guidance: %s", out.String())
	}
}

func TestFinishCLIReportsEvidenceReviewProgress(t *testing.T) {
	lookup, runtime, _ := finishCLIFixture(t)
	worker := &daemon.FinishWorker{RepoPath: lookup.Worktree.Root, DB: runtime.db, Config: daemon.FinishWorkerConfig{Mode: "local", Style: "imperative", ConfigDigest: "test-config"}}
	r, _, err := worker.Start(t.Context(), daemon.FinishOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ConsumeFinishEvidenceCall(t.Context(), runtime.db, r.ID, r.Attempt); err != nil {
		t.Fatal(err)
	}
	report, err := readFinishStatus(t.Context(), runtime.db.ReadSQL(), r.Target.WorktreeID)
	if err != nil || report == nil || report.EvidenceCalls != 1 || report.Stage != "reviewing_evidence" {
		t.Fatalf("status/list review progress %+v %v", report, err)
	}
	if label := finishStageLabel(report.Stage); !strings.Contains(label, "Reviewing saved changes") {
		t.Fatalf("hidden review phase: %s", label)
	}
}

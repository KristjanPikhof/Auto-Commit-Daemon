package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type historyGoalCLIPlanner struct{}

func (historyGoalCLIPlanner) Name() string { return "history-goal-test" }
func (historyGoalCLIPlanner) Generate(context.Context, ai.CommitContext) (ai.Result, error) {
	return ai.Result{}, nil
}
func (historyGoalCLIPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	var seqs []int64
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{CandidateID: "recovery-guide", SelectedSeqs: seqs, Purpose: "explain checkpoint recovery", GroupingReason: "Keep the recovery instructions complete for readers", Readiness: ai.IntentCandidateReady, Subject: "Document checkpoint recovery", Body: "- Explain how protected work can resume after an interruption"}}}, nil
}

func TestIntentHistoryCLIPlanPreviewPreservesSourceAndReadsWithoutMutation(t *testing.T) {
	withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	writeRewriteTestFile(t, repo, "recovery.md", "# Checkpoint recovery\nResume protected work after interruption.\n")
	for _, args := range [][]string{{"add", "recovery.md"}, {"commit", "-q", "-m", "Update recovery.md"}} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, args...); err != nil {
			t.Fatal(err)
		}
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	selection, err := git.ResolveRewriteSelection(ctx, repo, git.RewriteSelectionOptions{Last: 1})
	if err != nil {
		t.Fatal(err)
	}
	statePath, _ := rewriteStateDBPath(ctx, repo)
	initialized, err := state.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	initialized.Close()
	var out bytes.Buffer
	err = generateIntentHistoryPlan(ctx, &out, repo, selection, rewriteCommitsOptions{newBranch: "semantic-recovery", planOnly: true}, historyGoalCLIPlanner{}, ai.ProviderConfig{CommitFormat: ai.CommitFormatImperative, DiffEgress: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	var plan state.IntentHistoryPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("JSON output polluted: %v %s", err, out.String())
	}
	if plan.TargetBranchRef != "refs/heads/semantic-recovery" || len(plan.Goals) != 1 {
		t.Fatalf("plan=%+v", plan)
	}
	path, _ := rewriteStateDBPath(ctx, repo)
	db, err := state.OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var before string
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT value FROM daemon_meta WHERE key=?", "intent.history.plan."+plan.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	db.Close()
	out.Reset()
	if err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{showPlan: plan.ID}, true); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{applyPlan: plan.ID, dryRun: true}, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Preview passed") {
		t.Fatal(out.String())
	}
	db, err = state.OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var after string
	_ = db.ReadSQL().QueryRowContext(ctx, "SELECT value FROM daemon_meta WHERE key=?", "intent.history.plan."+plan.ID).Scan(&after)
	if before != after {
		t.Fatal("preview mutated saved plan")
	}
	if _, ok, _ := state.LoadIntentHistoryRequest(ctx, db); ok {
		t.Fatal("preview enqueued publication")
	}
	if got, _ := git.RevParse(ctx, repo, "HEAD"); got != head {
		t.Fatal("source branch moved")
	}
	if _, err := git.RevParse(ctx, repo, plan.TargetBranchRef); err == nil {
		t.Fatal("preview created target branch")
	}
	if err := applyIntentHistoryPlan(ctx, &out, repo, plan, false); err == nil || !strings.Contains(err.Error(), "active worker") {
		t.Fatalf("inactive apply bypassed approved verifier: %v", err)
	}
	file := filepath.Join(t.TempDir(), "plan.json")
	raw, _ := json.Marshal(plan)
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if loaded, ok, err := readIntentHistoryPlanRef(ctx, repo, file); err != nil || !ok || loaded.ID != plan.ID {
		t.Fatalf("file=%+v ok=%v err=%v", loaded, ok, err)
	}
}

func TestIntentHistorySharedProgressKeepsCaptureAndRequestSeparate(t *testing.T) {
	ctx := context.Background()
	_, _, db := makeRepoStateDB(t)
	now := time.Now()
	if err := state.SaveIntentHistoryRequest(ctx, db, state.IntentHistoryRequest{PlanID: "reviewed-goals", Status: "running"}); err != nil {
		t.Fatal(err)
	}
	report := statusReport{Daemon: "running", PID: os.Getpid(), Protected: true, CheckpointProtectionAvailable: true, IntentStrategy: intentStrategyReport{Strategy: "intent", PlannerHealth: &daemon.IntentPlannerHealthSnapshot{State: daemon.IntentPlannerCircuitOpen, NextProbeTS: float64(now.Add(time.Minute).Unix())}}}
	progress, err := buildPublicationProgressReport(ctx, db.ReadSQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Phase != "history_reconstruction" || progress.HistoryPlanID != "reviewed-goals" || progress.QueuePending != 0 {
		t.Fatalf("progress=%+v", progress)
	}
	if label := productListPhase(productListEntry{PublicationProgress: progress}); label != "history-reconstruct" {
		t.Fatal(label)
	}
	report.Paused = true
	progress, err = buildPublicationProgressReport(ctx, db.ReadSQL(), report, now)
	if err != nil || progress.Phase != "paused" {
		t.Fatalf("manual pause hidden: %+v %v", progress, err)
	}
}

func TestIntentHistoryPreviewAndUnsupportedWorkerDoNotMigrateState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo := rewriteSelectionTestRepo(t)
	writeRewriteTestFile(t, repo, "recovery.md", "# Checkpoint recovery\nResume protected work after interruption.\n")
	for _, args := range [][]string{{"add", "recovery.md"}, {"commit", "-q", "-m", "Update recovery.md"}} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, args...); err != nil {
			t.Fatal(err)
		}
	}
	dbPath, _ := rewriteStateDBPath(ctx, repo)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: 12345, Mode: "running", HeartbeatTS: nowFloat()}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, "PRAGMA user_version=29"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	selection, err := git.ResolveRewriteSelection(ctx, repo, git.RewriteSelectionOptions{Last: 1})
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "goals.json")
	var out bytes.Buffer
	if err := generateIntentHistoryPlan(ctx, &out, repo, selection, rewriteCommitsOptions{newBranch: "goal-preview", planOut: file, planOnly: true}, historyGoalCLIPlanner{}, ai.ProviderConfig{CommitFormat: ai.CommitFormatImperative, DiffEgress: true}, false); err != nil {
		t.Fatal(err)
	}
	plan, ok, err := readIntentHistoryPlanRef(ctx, repo, file)
	if err != nil || !ok {
		t.Fatal(err)
	}
	out.Reset()
	if err := applyIntentHistoryPlan(ctx, &out, repo, plan, false); err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("old worker accepted request: %v", err)
	}
	if version, err := state.ReadUserVersion(ctx, dbPath); err != nil || version != 29 {
		t.Fatalf("authoring migrated old worker database: %d %v", version, err)
	}
	readOnly, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if _, ok, _ := state.LoadIntentHistoryRequest(ctx, readOnly); ok {
		t.Fatal("unsupported worker received a request")
	}
	if _, err := git.RevParse(ctx, repo, plan.TargetBranchRef); err == nil {
		t.Fatal("unsupported worker created a branch")
	}
}

func TestIntentHistoryApplyUsesCanonicalWorkerSocketAndWake(t *testing.T) {
	repo, db, plan, lookup := intentHistoryApplyFixture(t)
	lock, err := daemon.AcquireDaemonLock(lookup.Worktree.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	woke := make(chan string, 1)
	handler := &repositoryWorkerHandler{
		repositoryID: lookup.Record.RepositoryID,
		runtimes: map[string]*workerRuntime{lookup.Record.WorktreeID: {
			record: lookup.Record, worktree: lookup.Worktree, db: db,
		}},
		wake: func(id string) { woke <- id },
	}
	serveIntentHistoryWorker(t, lookup, handler)
	var out bytes.Buffer
	if err := applyIntentHistoryPlan(context.Background(), &out, repo, plan, false); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-woke:
		if id != lookup.Record.WorktreeID {
			t.Fatalf("woke another worktree: %s", id)
		}
	default:
		t.Fatal("saved request did not wake the canonical worker")
	}
	request, ok, err := state.LoadIntentHistoryRequest(context.Background(), db)
	if err != nil || !ok || request.Status != "pending" || request.PlanID != plan.ID {
		t.Fatalf("request=%+v ok=%v err=%v", request, ok, err)
	}
	if !strings.Contains(out.String(), "Queued for the active worker") {
		t.Fatal(out.String())
	}
	if head, _ := git.RevParse(context.Background(), repo, "HEAD"); head != plan.ExpectedHead {
		t.Fatal("queueing changed the source branch")
	}
	if _, err := git.RevParse(context.Background(), repo, plan.TargetBranchRef); err == nil {
		t.Fatal("queueing bypassed worker verification")
	}
}

func TestIntentHistoryApplyRejectsUnprovedWorkerWithoutWrites(t *testing.T) {
	for _, scenario := range []string{"absent_socket", "different_pid", "different_repository", "not_ready", "older_schema"} {
		t.Run(scenario, func(t *testing.T) {
			repo, db, plan, lookup := intentHistoryApplyFixture(t)
			wantVersion := state.SchemaVersion
			if scenario == "older_schema" {
				wantVersion = 29
				if _, err := db.SQL().ExecContext(context.Background(), "PRAGMA user_version=29"); err != nil {
					t.Fatal(err)
				}
			}
			if scenario != "absent_socket" && scenario != "older_schema" {
				ready := supervisor.WorkerReadiness{RepositoryID: lookup.Record.RepositoryID, PID: os.Getpid(), Ready: true}
				switch scenario {
				case "different_pid":
					ready.PID++
				case "different_repository":
					ready.RepositoryID = "ffffffffffffffff"
				case "not_ready":
					ready.Ready = false
				}
				serveIntentHistoryWorker(t, lookup, historyReadinessFixture{ready: ready})
			}
			var out bytes.Buffer
			if err := applyIntentHistoryPlan(context.Background(), &out, repo, plan, false); err == nil || !strings.Contains(err.Error(), "worker") {
				t.Fatalf("unproved owner accepted: %v", err)
			}
			if _, ok, err := state.LoadIntentHistoryRequest(context.Background(), db); err != nil || ok {
				t.Fatalf("unproved owner received request: ok=%v err=%v", ok, err)
			}
			if _, ok, err := state.LoadIntentHistoryPlan(context.Background(), db, plan.ID); err != nil || ok {
				t.Fatalf("unproved owner saved plan: ok=%v err=%v", ok, err)
			}
			if version, err := db.UserVersion(context.Background()); err != nil || version != wantVersion {
				t.Fatalf("schema changed: %d %v", version, err)
			}
		})
	}
}

func TestIntentHistoryCompletedApplyProvesExistingTarget(t *testing.T) {
	for _, scenario := range []string{"intact", "deleted", "drifted", "backup_deleted"} {
		t.Run(scenario, func(t *testing.T) {
			repo, db, plan, _ := intentHistoryApplyFixture(t)
			ctx := context.Background()
			replacements, err := daemon.ValidateIntentHistoryPlan(ctx, repo, plan)
			if err != nil {
				t.Fatal(err)
			}
			result, err := git.ApplyIntentHistoryReconstruction(ctx, repo, git.IntentHistoryReconstructionOptions{
				SourceBranchRef: plan.SourceBranchRef, TargetBranchRef: plan.TargetBranchRef, ExpectedHead: plan.ExpectedHead,
				OldChain: plan.SourceChain, Replacements: replacements, PlanID: plan.ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			request := state.IntentHistoryRequest{PlanID: plan.ID, Status: "completed", NewHead: result.NewHead, BackupRef: result.BackupRef}
			if err := state.SaveIntentHistoryRequest(ctx, db, request); err != nil {
				t.Fatal(err)
			}
			request, _, err = state.LoadIntentHistoryRequest(ctx, db)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "deleted":
				_, err = git.Run(ctx, git.RunOpts{Dir: repo}, "update-ref", "-d", plan.TargetBranchRef)
			case "drifted":
				_, err = git.Run(ctx, git.RunOpts{Dir: repo}, "update-ref", plan.TargetBranchRef, plan.ExpectedHead)
			case "backup_deleted":
				_, err = git.Run(ctx, git.RunOpts{Dir: repo}, "update-ref", "-d", result.BackupRef)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, dryRun := range []bool{true, false} {
				var out bytes.Buffer
				err := applyIntentHistoryPlan(ctx, &out, repo, plan, dryRun)
				if scenario == "intact" {
					if err != nil || !strings.Contains(out.String(), "Already completed") || strings.Contains(out.String(), "Queued") {
						t.Fatalf("completed result=%q err=%v", out.String(), err)
					}
				} else if err == nil || !strings.Contains(err.Error(), "generate a new plan") {
					t.Fatalf("changed completed target accepted: %v", err)
				}
			}
			after, _, err := state.LoadIntentHistoryRequest(ctx, db)
			if err != nil || after != request {
				t.Fatalf("completion evidence changed: %+v %v", after, err)
			}
			if scenario == "deleted" {
				if _, err := git.RevParse(ctx, repo, plan.TargetBranchRef); err == nil {
					t.Fatal("replay silently recreated the deleted branch")
				}
			}
		})
	}
}

func intentHistoryApplyFixture(t *testing.T) (string, *state.DB, state.IntentHistoryPlan, controlRepoLookup) {
	t.Helper()
	withIsolatedHome(t)
	shortState, err := os.MkdirTemp("/tmp", "acd-history-cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortState) })
	t.Setenv("XDG_STATE_HOME", shortState)
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	writeRewriteTestFile(t, repo, "recovery.md", "# Checkpoint recovery\nResume protected work after interruption.\n")
	for _, args := range [][]string{{"add", "recovery.md"}, {"commit", "-q", "-m", "Update recovery.md"}} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, args...); err != nil {
			t.Fatal(err)
		}
	}
	wt, err := git.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	dbPath := state.DBPathFromGitDir(wt.GitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		upsertActivatedRepoFixture(reg, wt.Root, wt.CommonDir, dbPath, "codex", time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	worker := state.DaemonState{PID: os.Getpid(), Mode: "running", DaemonFingerprint: sql.NullString{String: "history-owner-fixture", Valid: true}}
	if err := state.SaveDaemonState(ctx, db, worker); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetJSON(ctx, db, state.MetaKeyIntentHistoryWorker, state.IntentHistoryWorker{PID: worker.PID, Fingerprint: worker.DaemonFingerprint.String, Protocol: state.IntentHistoryPlanVersion}); err != nil {
		t.Fatal(err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := daemon.PlanIntentHistory(ctx, repo, "refs/heads/main", []string{head}, historyGoalCLIPlanner{}, ai.CommitFormatImperative, true)
	if err != nil {
		t.Fatal(err)
	}
	plan.TargetBranchRef = "refs/heads/verified-recovery"
	plan, err = state.PrepareIntentHistoryPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	return repo, db, plan, lookup
}

type historyReadinessFixture struct{ ready supervisor.WorkerReadiness }

func (h historyReadinessFixture) HandleWorkerRequest(_ context.Context, request supervisor.Request) (any, *supervisor.ProtocolError) {
	if request.Method != "status" {
		return nil, &supervisor.ProtocolError{Code: "unexpected_mutation", Message: "owner proof must precede mutation"}
	}
	return h.ready, nil
}

func serveIntentHistoryWorker(t *testing.T, lookup controlRepoLookup, handler supervisor.WorkerHandler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- supervisor.ServeWorker(ctx, supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID), handler)
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker socket: %v", err)
		}
	})
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID)); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("worker socket did not start")
		}
		time.Sleep(time.Millisecond)
	}
}

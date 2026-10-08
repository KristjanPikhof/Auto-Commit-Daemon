package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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

//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentHistoryCLIReconstructsPausedGoalsAndProtectsLaterOutageCapture(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo, env := tempRepo(t), withIsolatedHome(t)
	rewriteIntegrationCommit(t, repo, "recovery.go", "package recovery\n\nfunc RecoveryCheckpoint(retained string) (string, bool) { return retained, retained != \"\" }\n", "Update recovery.go")
	head := rewriteIntegrationCommit(t, repo, "recovery_test.go", "package recovery\n\nimport \"testing\"\n\nfunc TestRecoveryCheckpoint(t *testing.T) {\n if _, ok := RecoveryCheckpoint(\"\"); ok { t.Fatal(\"missing checkpoint cannot protect work\") }\n if got, ok := RecoveryCheckpoint(\"retained\"); !ok || got != \"retained\" { t.Fatal(\"retained checkpoint must survive\") }\n}\n", "Update recovery_test.go")
	originalTree := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD^{tree}"))
	var online atomic.Bool
	var hits atomic.Int32
	online.Store(true)
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !online.Load() {
			http.Error(w, "provider temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		req := decodeIntentChatRequest(t, r)
		writeNativeIntentCandidatesResponse(t, w, "history-recovery", []map[string]any{nativeReadyIntentCandidate(
			"retained-checkpoint", offeredIntentSeqs(t, req), "Validate retained recovery checkpoints",
			"- Require a retained checkpoint before resuming protected work",
			"Keep RecoveryCheckpoint and its direct tests together as one complete recovery goal")})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	extra := activateIntentV2Runtime(t, repo,
		"ACD_INTENT_MIN_PENDING=1", "ACD_INTENT_SETTLE_WINDOW=0", "ACD_INTENT_MAX_PENDING_AGE=1h",
		"ACD_AI_PROVIDER=openai-compat", "ACD_AI_BASE_URL="+server.URL, "ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-6-luna", trustEnv)
	fullEnv := envWith(env, append(extra, "ACD_COMMIT_STRATEGY=intent")...)
	t.Cleanup(func() { stopSessionForce(t, fullEnv, repo) })
	startSession(t, ctx, env, repo, "history-paused-outage", "shell", extra...)
	command := func(args ...string) ExecResult {
		result := runAcd(t, ctx, fullEnv, append([]string{"--repo", repo}, args...)...)
		if result.ExitCode != 0 {
			t.Fatalf("acd %v failed: %s %s", args, result.Stdout, result.Stderr)
		}
		return result
	}
	command("pause", "--reason", "review goal reconstruction")
	file := filepath.Join(t.TempDir(), "goals.json")
	command("history", "rewrite", "--last", "2", "--new-branch", "verified-recovery", "--plan-out", file, "--plan-only")
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var plan state.IntentHistoryPlan
	if err := json.Unmarshal(raw, &plan); err != nil || len(plan.Goals) != 1 || len(plan.SourceChain) != 2 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	command("history", "rewrite", "--apply", file, "--dry-run")
	if result := command("history", "rewrite", "--apply", file, "--yes"); !strings.Contains(result.Stdout, "Queued for the active worker") {
		t.Fatal(result.Stdout)
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	db, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request, ok, err := state.LoadIntentHistoryRequest(ctx, db)
	if err != nil || !ok || request.PlanID != plan.ID || request.Status != "pending" {
		t.Fatalf("paused queue=%+v ok=%v err=%v", request, ok, err)
	}
	online.Store(false)
	const laterPath, laterBody = "outage-recovery.md", "# Provider outage recovery\nKeep later work protected until the provider reconnects.\n"
	writeFile(t, filepath.Join(repo, laterPath), laterBody)
	runGitOK(t, repo, "add", laterPath)
	indexBefore := runGitOK(t, repo, "write-tree")
	worktreeBefore := runGitOK(t, repo, "status", "--porcelain=v1")
	wakeSession(t, ctx, fullEnv, repo, "history-paused-outage")
	waitFor(t, "later work checkpointed while history is paused", 8*time.Second, func() bool {
		ref := sqliteScalar(t, dbPath, "SELECT checkpoint_ref FROM checkpoints WHERE phase='completed' AND retained=1 ORDER BY seq DESC LIMIT 1")
		body, err := runGit(repo, "show", ref+":"+laterPath)
		return err == nil && body == laterBody
	})
	if _, err := runGit(repo, "rev-parse", plan.TargetBranchRef); err == nil || hits.Load() != 1 {
		t.Fatal("paused request published or contacted the offline provider")
	}
	command("resume", "--yes")
	waitFor(t, "worker completed exact history goal", 15*time.Second, func() bool {
		var ok bool
		var err error
		request, ok, err = state.LoadIntentHistoryRequest(ctx, db)
		return err == nil && ok && request.PlanID == plan.ID && request.Status == "completed"
	})
	assertProviderWaitPreservesCheckpoint(t, repo, laterPath, laterBody, head)
	assertOutageStatusAndList(t, ctx, fullEnv, repo, 1)
	if target := strings.TrimSpace(runGitOK(t, repo, "rev-parse", plan.TargetBranchRef)); target != request.NewHead || target == head {
		t.Fatalf("completed target=%s request=%+v original=%s", target, request, head)
	}
	if tree := strings.TrimSpace(runGitOK(t, repo, "rev-parse", plan.TargetBranchRef+"^{tree}")); tree != originalTree {
		t.Fatal("reconstruction changed original goal contents")
	}
	base := strings.TrimSpace(runGitOK(t, repo, "rev-parse", plan.SourceChain[0]+"^"))
	if count := strings.TrimSpace(runGitOK(t, repo, "rev-list", "--count", base+".."+plan.TargetBranchRef)); count != "1" {
		t.Fatalf("two related saves were not reconstructed into one goal: %s", count)
	}
	if subject := strings.TrimSpace(runGitOK(t, repo, "show", "-s", "--format=%s", plan.TargetBranchRef)); subject != "Validate retained recovery checkpoints" {
		t.Fatalf("reconstructed goal has an unhelpful message: %s", subject)
	}
	if _, err := runGit(repo, "show", plan.TargetBranchRef+":"+laterPath); err == nil {
		t.Fatal("frozen history target consumed the later capture")
	}
	if runGitOK(t, repo, "write-tree") != indexBefore || runGitOK(t, repo, "status", "--porcelain=v1") != worktreeBefore {
		t.Fatal("history reconstruction changed live staging or files")
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", request.BackupRef)); got != head || hits.Load() != 2 {
		t.Fatalf("backup=%s provider calls=%d", got, hits.Load())
	}
}

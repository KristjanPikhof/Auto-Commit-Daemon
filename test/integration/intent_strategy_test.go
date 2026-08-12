//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

func TestIntentStrategy_RejectsDisconnectedNativeGroup(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		if writeIntentMessageRewriteResponse(t, w, req) {
			return
		}
		hits.Add(1)
		seqs := offeredIntentSeqs(t, req)
		if len(seqs) < 2 {
			http.Error(w, "need at least two offered captures", http.StatusBadRequest)
			return
		}
		plan := map[string]any{
			"selected_seqs":    seqs,
			"deferred_seqs":    []int64{},
			"subject":          "Intent grouped files",
			"body":             "Publish both captures together.",
			"grouping_reason":  "same integration test intent",
			"deferred_reasons": []map[string]any{},
		}
		writeIntentPlanResponse(t, w, "call_1", plan)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	startSession(t, ctx, env, repo, "intent-group", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	assertIntentV2RuntimeActive(t, repo)

	paused := runAcd(t, ctx, envWith(env, extra...), "pause", "--repo", repo, "--reason", "batch intent test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	writeFile(t, filepath.Join(repo, "intent-one.txt"), "one\n")
	writeFile(t, filepath.Join(repo, "intent-two.txt"), "two\n")

	startCount := commitCount(t, repo)
	resumed := runAcd(t, ctx, envWith(env, extra...), "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	flushed := runAcd(t, ctx, envWith(env, extra...), "flush", "--repo", repo, "--session-id", "intent-group", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s", flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "intent-one.txt", "published", 10*time.Second)
	flushed = runAcd(t, ctx, envWith(env, extra...), "flush", "--repo", repo,
		"--session-id", "intent-group", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("second acd flush exit=%d\nstdout=%s\nstderr=%s",
			flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}
	waitForEventState(t, dbPath, "intent-two.txt", "published", 10*time.Second)

	oidOne := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'intent-one.txt' ORDER BY seq DESC LIMIT 1")
	oidTwo := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'intent-two.txt' ORDER BY seq DESC LIMIT 1")
	if oidOne == "" || oidTwo == "" || oidOne == oidTwo {
		t.Fatalf("disconnected commit oids one=%q two=%q", oidOne, oidTwo)
	}
	if got := commitCount(t, repo); got != startCount+2 {
		t.Fatalf("commit count=%d want %d (two atomic intent commits)",
			got, startCount+2)
	}
	if hits.Load() == 0 {
		t.Fatal("mock planner was not called")
	}
}

func TestIntentStrategy_RapidFiveCapturesOfferedThenSeparated(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var hits atomic.Int32
	var firstOffered atomic.Value
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		if writeIntentMessageRewriteResponse(t, w, req) {
			return
		}
		hits.Add(1)
		seqs := offeredIntentSeqs(t, req)
		if firstOffered.Load() == nil {
			copied := append([]int64(nil), seqs...)
			firstOffered.Store(copied)
		}
		if len(seqs) != 5 {
			http.Error(w, "expected exactly five offered captures", http.StatusBadRequest)
			return
		}
		plan := map[string]any{
			"selected_seqs":    seqs,
			"deferred_seqs":    []int64{},
			"subject":          "Group rapid five",
			"body":             "Publish all rapid captures together.",
			"grouping_reason":  "settle window collected the rapid burst",
			"deferred_reasons": []map[string]any{},
		}
		writeIntentPlanResponse(t, w, "call_rapid_five", plan)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=2",
		"ACD_INTENT_SETTLE_WINDOW=1s",
		"ACD_INTENT_MAX_PENDING_AGE=30s",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	sessionID := "intent-rapid-five"
	startSession(t, ctx, env, repo, sessionID, "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	files := []string{
		"rapid/one.go",
		"rapid/two.go",
		"rapid/three.go",
		"rapid/four.go",
		"rapid/five.go",
	}
	startCount := commitCount(t, repo)
	for _, name := range files {
		writeFile(t, filepath.Join(repo, name), "rapid content for "+name+"\n")
	}
	wake := runAcd(t, ctx, fullEnv, "wake", "--repo", repo, "--session-id", sessionID, "--json")
	if wake.ExitCode != 0 {
		t.Fatalf("acd wake exit=%d\nstdout=%s\nstderr=%s", wake.ExitCode, wake.Stdout, wake.Stderr)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	for attempt := 0; attempt < len(files)+1; attempt++ {
		allPublished := true
		for _, name := range files {
			if sqliteScalar(t, dbPath,
				"SELECT state FROM capture_events WHERE path='"+name+
					"' ORDER BY seq DESC LIMIT 1") != "published" {
				allPublished = false
				break
			}
		}
		if allPublished {
			break
		}
		flushed := runAcd(t, ctx, fullEnv, "flush", "--repo", repo,
			"--session-id", sessionID, "--logical", "--json")
		if flushed.ExitCode != 0 {
			t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s",
				flushed.ExitCode, flushed.Stdout, flushed.Stderr)
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, name := range files {
		waitForEventState(t, dbPath, name, "published", 25*time.Second)
	}
	if hits.Load() < 1 {
		t.Fatal("planner was not called")
	}
	seqs, ok := firstOffered.Load().([]int64)
	if !ok || len(seqs) != 5 {
		t.Fatalf("first offered seqs=%v want 5 seqs", firstOffered.Load())
	}
	if got := commitCount(t, repo); got != startCount+len(files) {
		t.Fatalf("commit count=%d want %d (five independent commits)",
			got, startCount+len(files))
	}
	if got := sqliteScalar(t, dbPath, `
SELECT COUNT(*) FROM intent_candidates
WHERE planner_protocol='v2' AND status IN ('published','soft_published')`); got != "5" {
		t.Fatalf("native v2 rapid candidates=%s want 5", got)
	}
}

func TestIntentV2MigrationMissingPrerequisitesKeepsCapturePending(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_API_KEY=",
		"ACD_AI_DIFF_EGRESS=0",
	}
	t.Cleanup(func() { stopSessionForce(t, envWith(env, extra...), repo) })
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	sessionID := "intent-v2-missing-prerequisites"
	startSession(t, ctx, env, repo, sessionID, "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	headBefore := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "captured-while-blocked.txt"),
		"capture remains durable\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, sessionID)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "captured-while-blocked.txt", "pending",
		10*time.Second)
	waitFor(t, "checkpoint protection completes while publication is blocked", 10*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			`SELECT value FROM daemon_meta WHERE key='protection.complete'`) == "true" &&
			sqliteScalar(t, dbPath, `
SELECT COUNT(*)
FROM checkpoint_events ce
JOIN checkpoints cp ON cp.id=ce.checkpoint_id
JOIN capture_events e ON e.seq=ce.event_seq
WHERE e.path='captured-while-blocked.txt' AND cp.phase='completed'`) != "0"
	})
	if headAfter := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); headAfter != headBefore {
		t.Fatalf("blocked Intent v2 replay advanced HEAD: before=%s after=%s",
			headBefore, headAfter)
	}
	status := runAcd(t, ctx, envWith(env, extra...),
		"status", "--repo", repo, "--json")
	if (status.ExitCode != 0 && status.ExitCode != 3) ||
		!strings.Contains(status.Stdout, `"operational_state": "needs_attention"`) ||
		!strings.Contains(status.Stdout, `"published": false`) {
		t.Fatalf("status did not report blocked publication truthfully:\n%s\n%s",
			status.Stdout, status.Stderr)
	}
	if got := sqliteScalar(t, dbPath, `
SELECT COUNT(*) FROM intent_candidates`); got != "0" {
		t.Fatalf("blocked migration invoked candidate planning: candidates=%s",
			got)
	}
}

type intentChatRequest struct {
	Messages []struct {
		Content string `json:"content"`
	} `json:"messages"`
	ToolChoice struct {
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	} `json:"tool_choice"`
}

type intentPlanPromptPayload struct {
	OfferedCaptures []struct {
		Seq int64 `json:"seq"`
	} `json:"offered_captures"`
}

func decodeIntentChatRequest(t *testing.T, r *http.Request) intentChatRequest {
	t.Helper()
	defer r.Body.Close()
	var req intentChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Fatalf("decode OpenAI request: %v", err)
	}
	return req
}

// writeIntentMessageRewriteResponse handles the locked message-only second
// round trip used when an otherwise accepted intent plan has a low-quality
// commit message. Returning only commit_message output mirrors the provider
// protocol without changing the plan's accepted grouping.
func writeIntentMessageRewriteResponse(t *testing.T, w http.ResponseWriter, req intentChatRequest) bool {
	t.Helper()
	if req.ToolChoice.Function.Name != "commit_message" {
		return false
	}
	args, err := json.Marshal(map[string]string{
		"subject": "Apply selected intent change",
		"body":    "- Preserve the accepted capture grouping.",
	})
	if err != nil {
		t.Fatalf("marshal intent message rewrite: %v", err)
	}
	body, err := json.Marshal(map[string]any{
		"id":     "chatcmpl-intent-message-rewrite",
		"object": "chat.completion",
		"model":  "gpt-5.4-mini",
		"choices": []map[string]any{{
			"index": 0,
			"message": map[string]any{
				"role":    "assistant",
				"content": "",
				"tool_calls": []map[string]any{{
					"id":   "call_intent_message_rewrite",
					"type": "function",
					"function": map[string]any{
						"name":      "commit_message",
						"arguments": string(args),
					},
				}},
			},
			"finish_reason": "tool_calls",
		}},
	})
	if err != nil {
		t.Fatalf("marshal intent message rewrite response: %v", err)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
	return true
}

func offeredIntentSeqs(t *testing.T, req intentChatRequest) []int64 {
	t.Helper()
	captures := offeredIntentCaptures(t, req)
	seqs := make([]int64, 0, len(captures))
	for _, capture := range captures {
		seqs = append(seqs, capture.Seq)
	}
	return seqs
}

func TestIntentStrategy_InvalidEnvFallsBackToEventDefaults(t *testing.T) {
	t.Setenv("ACD_COMMIT_STRATEGY", "bogus")
	t.Setenv("ACD_INTENT_WINDOW", "0")
	t.Setenv("ACD_INTENT_RECENT_COMMITS", "bogus")
	t.Setenv("ACD_INTENT_DEFER_LIMIT", "-1")
	cfg := ai.LoadProviderConfigFromEnv()
	if cfg.CommitStrategy != ai.CommitStrategyEvent ||
		cfg.IntentWindow != ai.DefaultIntentWindow ||
		cfg.IntentRecentCommits != ai.DefaultIntentRecentCommits ||
		cfg.IntentDeferLimit != ai.DefaultIntentDeferLimit {
		t.Fatalf("fallback config = strategy=%s window=%d recent=%d defer=%d",
			cfg.CommitStrategy, cfg.IntentWindow, cfg.IntentRecentCommits, cfg.IntentDeferLimit)
	}
}

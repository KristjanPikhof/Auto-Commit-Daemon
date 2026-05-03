//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntentStrategy_OpenAIPlannerGroupsTwoCaptures(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
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
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":      "chatcmpl-intent",
			"object":  "chat.completion",
			"model":   "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_1",
						"type": "function",
						"function": map[string]any{
							"name":      "capture_intent_plan",
							"arguments": string(args),
						},
					}},
				},
				"finish_reason": "tool_calls",
			}},
		}
		body, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
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
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "intent-group", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

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
	wakeSession(t, ctx, envWith(env, extra...), repo, "intent-group")

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "intent-one.txt", "published", 10*time.Second)
	waitForEventState(t, dbPath, "intent-two.txt", "published", 10*time.Second)

	oidOne := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'intent-one.txt' ORDER BY seq DESC LIMIT 1")
	oidTwo := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'intent-two.txt' ORDER BY seq DESC LIMIT 1")
	if oidOne == "" || oidOne != oidTwo {
		t.Fatalf("grouped commit oids one=%q two=%q", oidOne, oidTwo)
	}
	if got := commitCount(t, repo); got != startCount+1 {
		t.Fatalf("commit count=%d want %d (one grouped intent commit)", got, startCount+1)
	}
	if subj := headSubject(t, repo); subj != "Intent grouped files" {
		t.Fatalf("subject=%q want grouped planner subject", subj)
	}
	if hits.Load() == 0 {
		t.Fatal("mock planner was not called")
	}
}

type intentChatRequest struct {
	Messages []struct {
		Content string `json:"content"`
	} `json:"messages"`
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

func offeredIntentSeqs(t *testing.T, req intentChatRequest) []int64 {
	t.Helper()
	for _, msg := range req.Messages {
		const marker = "Plan the next commit intent for these offered captures:\n"
		if !strings.HasPrefix(msg.Content, marker) {
			continue
		}
		var payload intentPlanPromptPayload
		if err := json.Unmarshal([]byte(strings.TrimPrefix(msg.Content, marker)), &payload); err != nil {
			t.Fatalf("decode intent prompt payload: %v", err)
		}
		seqs := make([]int64, 0, len(payload.OfferedCaptures))
		for _, capture := range payload.OfferedCaptures {
			seqs = append(seqs, capture.Seq)
		}
		return seqs
	}
	t.Fatalf("intent planner user prompt not found in request: %+v", req)
	return nil
}

func TestIntentStrategy_InvalidEnvFallsBackToEventDefaults(t *testing.T) {
	t.Setenv("ACD_COMMIT_STRATEGY", "bogus")
	t.Setenv("ACD_INTENT_WINDOW", "0")
	t.Setenv("ACD_INTENT_RECENT_COMMITS", "bogus")
	t.Setenv("ACD_INTENT_DEFER_LIMIT", "-1")
	cfg := aiProviderConfigForIntegrationTest()
	if got := fmt.Sprint(cfg.CommitStrategy, cfg.IntentWindow, cfg.IntentRecentCommits, cfg.IntentDeferLimit); got != "event1052" {
		t.Fatalf("fallback config = %s, want event1052", got)
	}
}

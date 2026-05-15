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
)

// TestIntentStrategy_OpenAIPlannerNormalizesSpuriousDeferredReason is the
// integration arm of the [Tests] task. It boots the daemon under
// ACD_COMMIT_STRATEGY=intent against a mock openai-compat server that returns
// a planner response with a deferred_reasons entry referencing a selected
// seq (the same shape as ai/testdata/intent_planner/bad_deferred_reason.json).
//
// Before this fix the daemon recorded an intent_planner_error decision and
// fell back to one-item deterministic commits; the two captures landed in
// separate commits. After provider-side normalization the spurious entry is
// dropped, the cleaned plan validates, both captures publish under the same
// commit_oid, and the decision_records table carries no intent_planner_error
// rows for the affected window.
func TestIntentStrategy_OpenAIPlannerNormalizesSpuriousDeferredReason(t *testing.T) {
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
		// Select all offered seqs but emit a deferred_reasons entry whose
		// seq is one of the selected captures. A pre-fix daemon would have
		// rejected this plan; a post-fix daemon drops the spurious entry
		// and proceeds with the grouped commit.
		plan := map[string]any{
			"selected_seqs":   seqs,
			"deferred_seqs":   []int64{},
			"subject":         "Intent grouped files",
			"body":            "Publish both captures together.",
			"grouping_reason": "shared normalization test intent",
			"deferred_reasons": []map[string]any{{
				"seq":    seqs[0],
				"reason": "spurious entry referencing selected seq",
			}},
		}
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-bad-deferred-reason",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_intent_plan_bad",
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
	startSession(t, ctx, env, repo, "intent-normalize", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	paused := runAcd(t, ctx, envWith(env, extra...), "pause", "--repo", repo, "--reason", "batch normalization test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	writeFile(t, filepath.Join(repo, "norm-one.txt"), "one\n")
	writeFile(t, filepath.Join(repo, "norm-two.txt"), "two\n")

	startCount := commitCount(t, repo)
	resumed := runAcd(t, ctx, envWith(env, extra...), "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	wakeSession(t, ctx, envWith(env, extra...), repo, "intent-normalize")

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "norm-one.txt", "published", 10*time.Second)
	waitForEventState(t, dbPath, "norm-two.txt", "published", 10*time.Second)

	oidOne := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'norm-one.txt' ORDER BY seq DESC LIMIT 1")
	oidTwo := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'norm-two.txt' ORDER BY seq DESC LIMIT 1")
	if oidOne == "" || oidOne != oidTwo {
		t.Fatalf("grouped commit oids one=%q two=%q (expected normalization to keep them grouped)", oidOne, oidTwo)
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
	plannerErrors := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM decision_records WHERE kind = 'intent_planner_error'")
	if plannerErrors != "0" {
		t.Fatalf("intent_planner_error decisions=%s want 0 (provider normalization should suppress this)", plannerErrors)
	}
}

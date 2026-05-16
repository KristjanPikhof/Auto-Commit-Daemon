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
)

// TestIntentStrategy_OpenAIPlannerNormalizesSelectedDeferredOverlap is the
// integration arm of the [Tests] task. It boots the daemon under
// ACD_COMMIT_STRATEGY=intent against a mock openai-compat server that returns
// a planner response with a seq in both selected_seqs and deferred_seqs plus a
// deferred_reasons entry referencing that selected seq.
//
// Before this fix the daemon recorded an intent_planner_error decision and
// fell back to one-item deterministic commits; the two captures landed in
// separate commits. After provider-side normalization the overlap and spurious
// reason are dropped, the cleaned plan validates, both captures publish under
// the same commit_oid, and the decision_records table carries no
// intent_planner_error rows for the affected window.
func TestIntentStrategy_OpenAIPlannerNormalizesSelectedDeferredOverlap(t *testing.T) {
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
		// Select all offered seqs but also defer one selected capture and
		// attach a deferred_reasons entry to it. A pre-fix daemon would
		// have rejected this plan; a post-fix daemon treats selected as
		// authoritative, drops the overlap and reason, then proceeds with
		// the grouped commit.
		plan := map[string]any{
			"selected_seqs":   seqs,
			"deferred_seqs":   []int64{seqs[0]},
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
		"ACD_INTENT_MIN_PENDING=2",
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
	flushed := runAcd(t, ctx, envWith(env, extra...), "flush", "--repo", repo, "--session-id", "intent-normalize", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s", flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}

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

func TestIntentStrategy_PlannerRejectsLogCapturesValidationFailure(t *testing.T) {
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
			"selected_seqs":    []int64{999999},
			"deferred_seqs":    seqs,
			"subject":          "Invalid planner output",
			"body":             "This response must be rejected.",
			"grouping_reason":  "intentional validation failure",
			"deferred_reasons": buildDeferredReasons(seqs),
		}
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-rejects-log",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_intent_plan_reject",
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
		"ACD_INTENT_MIN_PENDING=2",
		"ACD_INTENT_MAX_PENDING_AGE=1h",
		"ACD_INTENT_RETRY_ON_INVALID=0",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "intent-rejects-log", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	paused := runAcd(t, ctx, envWith(env, extra...), "pause", "--repo", repo, "--reason", "rejects log test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	writeFile(t, filepath.Join(repo, "reject-one.txt"), "one\n")
	writeFile(t, filepath.Join(repo, "reject-two.txt"), "two\n")
	rejectsPath := filepath.Join(repo, ".git", "acd", "planner-rejects.jsonl")
	if err := os.MkdirAll(filepath.Dir(rejectsPath), 0o755); err != nil {
		t.Fatalf("mkdir rejects dir: %v", err)
	}
	seed := strings.Repeat("x", 5*1024*1024+1)
	if err := os.WriteFile(rejectsPath, []byte(seed), 0o644); err != nil {
		t.Fatalf("seed planner rejects log: %v", err)
	}
	resumed := runAcd(t, ctx, envWith(env, extra...), "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	flushed := runAcd(t, ctx, envWith(env, extra...), "flush", "--repo", repo, "--session-id", "intent-rejects-log", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s", flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitFor(t, "intent_planner_error decisions", 10*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM decision_records WHERE kind = 'intent_planner_error'") == "2"
	})
	waitFor(t, "planner rejects log", 10*time.Second, func() bool {
		_, err := os.Stat(rejectsPath)
		return err == nil
	})
	rotatedInfo, err := os.Stat(rejectsPath + ".1")
	if err != nil {
		t.Fatalf("stat rotated planner rejects log: %v", err)
	}
	if rotatedInfo.Size() <= 5*1024*1024 {
		t.Fatalf("rotated planner rejects log size=%d want >5MiB", rotatedInfo.Size())
	}
	rawRejects, err := os.ReadFile(rejectsPath)
	if err != nil {
		t.Fatalf("read planner rejects log: %v", err)
	}
	if !strings.Contains(string(rawRejects), `"provider":"openai-compat"`) ||
		!strings.Contains(string(rawRejects), `selected seq 999999 outside offered window`) {
		t.Fatalf("planner rejects log missing expected record:\n%s", rawRejects)
	}
	if hits.Load() == 0 {
		t.Fatal("mock planner was not called")
	}
}

func TestIntentStrategy_SingletonShortCircuitUsesMessageProvider(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var messageHits atomic.Int32
	var plannerHits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		for _, msg := range req.Messages {
			if strings.HasPrefix(msg.Content, "Plan the next commit intent for these offered captures:\n") {
				plannerHits.Add(1)
				http.Error(w, "singleton path must not call intent planner", http.StatusInternalServerError)
				return
			}
		}
		messageHits.Add(1)
		args, err := json.Marshal(map[string]string{
			"subject": "Singleton openai subject",
			"body":    "Generated by the per-event message provider.",
		})
		if err != nil {
			t.Fatalf("marshal singleton message args: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-singleton-message",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_commit_message_singleton",
						"type": "function",
						"function": map[string]any{
							"name":      "commit_message",
							"arguments": string(args),
						},
					}},
				},
				"finish_reason": "tool_calls",
			}},
		}
		body, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("marshal singleton response: %v", err)
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
		"ACD_INTENT_MIN_PENDING=1",
		"ACD_INTENT_MAX_PENDING_AGE=1h",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "intent-singleton-shortcircuit", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	headBefore := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "singleton.txt"), "one\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "intent-singleton-shortcircuit")
	waitHeadAdvances(t, repo, headBefore, 10*time.Second)

	if got := plannerHits.Load(); got != 0 {
		t.Fatalf("planner hits=%d want 0 for singleton short-circuit", got)
	}
	if got := messageHits.Load(); got != 1 {
		t.Fatalf("message hits=%d want 1", got)
	}
	if subj := headSubject(t, repo); subj != "Singleton openai subject" {
		t.Fatalf("subject=%q want per-event provider subject", subj)
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	reason := sqliteScalar(t, dbPath, "SELECT reason FROM decision_records WHERE kind='committed' ORDER BY event_seq DESC LIMIT 1")
	if !strings.Contains(reason, "singleton fast path") {
		t.Fatalf("decision reason=%q want singleton fast path", reason)
	}
}

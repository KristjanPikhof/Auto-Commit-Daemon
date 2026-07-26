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

// TestIntentStrategy_OpenAIPlannerRejectsUnrepairedSelectedDeferredOverlap is
// the integration arm of the [Tests] task. It boots the daemon under
// ACD_COMMIT_STRATEGY=intent against a mock openai-compat server that returns
// a planner response with a seq in both selected_seqs and deferred_seqs plus a
// deferred_reasons entry referencing that selected seq.
//
// The overlap must stay invalid: the daemon records intent_planner_error
// decisions for the offered window, logs the rejected planner response, and
// uses deterministic fallback for one safe capture instead of publishing the
// contradictory grouped plan.
func TestIntentStrategy_OpenAIPlannerRejectsUnrepairedSelectedDeferredOverlap(t *testing.T) {
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
		// attach a deferred_reasons entry to it. The daemon must preserve
		// this contradiction through validation rather than cleaning it into
		// a publishable grouped plan.
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
		writeIntentPlanResponse(t, w, "call_intent_plan_bad", plan)
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
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
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
	waitFor(t, "intent_planner_error decisions", 10*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM decision_records WHERE kind = 'intent_planner_error'") == "2"
	})

	oidOne := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'norm-one.txt' ORDER BY seq DESC LIMIT 1")
	oidTwo := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'norm-two.txt' ORDER BY seq DESC LIMIT 1")
	if oidOne == "" {
		t.Fatalf("norm-one commit oid is empty")
	}
	if oidTwo != "" && oidOne == oidTwo {
		t.Fatalf("overlap plan published as grouped commit oid=%q for both captures", oidOne)
	}
	if got := commitCount(t, repo); got != startCount+1 {
		t.Fatalf("commit count=%d want %d (one deterministic fallback commit)", got, startCount+1)
	}
	if subj := headSubject(t, repo); subj == "Intent grouped files" {
		t.Fatalf("subject=%q shows contradictory planner plan was published", subj)
	}
	if hits.Load() == 0 {
		t.Fatal("mock planner was not called")
	}
	rejectsPath := filepath.Join(repo, ".git", "acd", "planner-rejects.jsonl")
	rawRejects, err := os.ReadFile(rejectsPath)
	if err != nil {
		t.Fatalf("read planner rejects log: %v", err)
	}
	if !strings.Contains(string(rawRejects), `"provider":"openai-compat"`) ||
		!strings.Contains(string(rawRejects), `capture_assigned_twice`) {
		t.Fatalf("planner rejects log missing overlap record:\n%s", rawRejects)
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
		writeIntentPlanResponse(t, w, "call_intent_plan_reject", plan)
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
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
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
		!strings.Contains(string(rawRejects), `capture_outside_window`) {
		t.Fatalf("planner rejects log missing expected record:\n%s", rawRejects)
	}
	if hits.Load() == 0 {
		t.Fatal("mock planner was not called")
	}
}

func TestIntentStrategy_SingletonTransportFailureOpensCircuit(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var plannerHits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		plannerHits.Add(1)
		http.Error(w, "planner temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=1",
		"ACD_INTENT_SETTLE_WINDOW=0",
		"ACD_INTENT_MAX_PENDING_AGE=1h",
		"ACD_INTENT_RETRY_ON_INVALID=0",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	startSession(t, ctx, env, repo, "intent-singleton-circuit", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	startCount := commitCount(t, repo)
	headBefore := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "singleton-one.txt"), "one\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "intent-singleton-circuit")
	waitHeadAdvances(t, repo, headBefore, 10*time.Second)
	waitForEventState(t, dbPath, "singleton-one.txt", "published", 10*time.Second)
	waitFor(t, "first planner error", 10*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM decision_records WHERE kind='intent_planner_error'") == "1"
	})
	if got := plannerHits.Load(); got != 1 {
		t.Fatalf("planner hits after first capture=%d want 1", got)
	}

	headAfterFirst := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "singleton-two.txt"), "two\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "intent-singleton-circuit")
	waitHeadAdvances(t, repo, headAfterFirst, 10*time.Second)
	waitForEventState(t, dbPath, "singleton-two.txt", "published", 10*time.Second)

	if got := plannerHits.Load(); got != 1 {
		t.Fatalf("planner hits after cooldown bypass=%d want 1", got)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM decision_records WHERE kind='intent_planner_error'"); got != "1" {
		t.Fatalf("planner error decisions=%s want only the originating provider failure", got)
	}
	waitFor(t, "persisted planner circuit bypass", 10*time.Second, func() bool {
		raw := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key='intent.planner.health'")
		var health struct {
			State       string `json:"state"`
			BypassCount uint64 `json:"bypass_count"`
		}
		return json.Unmarshal([]byte(raw), &health) == nil && health.State == "open" && health.BypassCount >= 1
	})
	if got := commitCount(t, repo); got != startCount+2 {
		t.Fatalf("commit count=%d want two deterministic fallback commits after initial HEAD", got)
	}
}

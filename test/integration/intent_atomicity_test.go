//go:build integration
// +build integration

package integration_test

// intent_atomicity_test.go — verification-lane Wave 3 acceptance tests for
// the b1/c1 outcome that landed earlier in this branch:
//
//   - four sequential modifies on the SAME path must collapse into exactly one
//     planner-grouped commit. The decision_records ledger must record one
//     row per original capture seq, all sharing the same commit_oid (so the
//     CLI's grouped_seqs derivation reports len 4).
//
//   - an A → B → A interleave on two distinct paths must NOT coalesce. The
//     planner sees three distinct offers; the daemon publishes at least two
//     commits even when the planner groups everything it sees.
//
// The companion daemon-package tests TestReplay_IntentPathCoalesce_* (see
// internal/daemon/replay_test.go) cover the planner-offer shape directly via
// the recordingIntentPlanner helper. This file drives the same paths through
// the real `acd` binary plus a mock openai-compat HTTP server so the wiring
// from capture → coalesce → planner request → grouped publish survives the
// CLI/daemon process boundary too.

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

// TestIntentAtomicity_FourSamePathEditsLandAsOneCommit drives four sequential
// modifies on the same tracked path through the real daemon under the intent
// commit strategy. The mock planner accepts whatever offered-seqs the daemon
// presents; the assertion is that the daemon's path-coalesce gate folds the
// four captures into a single offer (planner hit count == 1) and publishes
// them as a single commit covering all four seqs.
func TestIntentAtomicity_FourSamePathEditsLandAsOneCommit(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	// Seed the file under version control so subsequent writes capture as
	// modify ops (the coalesce gate folds same-path modify chains; create
	// followed by modify is intentionally NOT a coalesce target — see the
	// daemon-package coverage).
	target := filepath.Join(repo, "burst.txt")
	writeFile(t, target, "v0\n")
	gitCommitAll(t, repo, "seed burst.txt", "burst.txt")

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		seqs := offeredIntentSeqs(t, req)
		if len(seqs) == 0 {
			http.Error(w, "expected at least one offered capture", http.StatusBadRequest)
			return
		}
		plan := map[string]any{
			"selected_seqs":    seqs,
			"deferred_seqs":    []int64{},
			"subject":          "Coalesced burst",
			"body":             "Folded same-path edits into one commit.",
			"grouping_reason":  "single-path edit chain",
			"deferred_reasons": []map[string]any{},
		}
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-coalesce",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// IntentMinPending=4 keeps the planner gated until all four captures have
	// landed. IntentMaxPendingAge stays at the 5m default so age never trips
	// the trigger; only the count gate fires.
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=4",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "intent-atomic-coalesce", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	startCount := commitCount(t, repo)

	// Drive four sequential modifies. A wake after each write makes capture
	// observe each transition (v0→v1, v1→v2, …) so we end up with four
	// distinct capture_events rows for the same path. Without the wakes,
	// successive writes would collapse against the shadow as a single delta.
	for i, body := range []string{"v1\n", "v2\n", "v3\n", "v4\n"} {
		writeFile(t, target, body)
		wakeSession(t, ctx, fullEnv, repo, "intent-atomic-coalesce")
		// Each wake should land a new pending event for burst.txt before the
		// next write races the capture pass. Poll briefly so the test does
		// not depend on absolute wall-clock pacing.
		waitFor(t, "pending burst.txt event", 5*time.Second, func() bool {
			n := sqliteScalar(t, dbPath,
				"SELECT COUNT(*) FROM capture_events WHERE path='burst.txt' AND state='pending'")
			return n == intToString(i+1)
		})
	}

	// Trip the planner: the count gate is now satisfied (4 >= IntentMinPending).
	// One more wake walks replay through the coalesce path → single offer →
	// grouped publish.
	wakeSession(t, ctx, fullEnv, repo, "intent-atomic-coalesce")
	waitForEventState(t, dbPath, "burst.txt", "published", 15*time.Second)

	// Exactly one new commit on top of the seed.
	if got, want := commitCount(t, repo), startCount+1; got != want {
		t.Fatalf("commit count=%d want %d (four edits must collapse to one commit)", got, want)
	}

	// All four capture rows must share the same commit_oid.
	oid := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='burst.txt' AND state='published' ORDER BY seq ASC LIMIT 1")
	if oid == "" {
		t.Fatalf("expected non-empty commit_oid for first published row")
	}
	distinct := sqliteScalar(t, dbPath,
		"SELECT COUNT(DISTINCT commit_oid) FROM capture_events WHERE path='burst.txt' AND state='published'")
	if distinct != "1" {
		t.Fatalf("distinct commit_oid for burst.txt published rows=%s want 1 (all four seqs share one commit)", distinct)
	}
	rows := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_events WHERE path='burst.txt' AND state='published' AND commit_oid="+sqliteLiteral(oid))
	if rows != "4" {
		t.Fatalf("published rows for burst.txt under commit_oid=%s = %s want 4", oid, rows)
	}

	// decision_records must carry one row per original seq for the same commit
	// (this is what the CLI's events command reads when deriving grouped_seqs).
	committed := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE commit_oid="+sqliteLiteral(oid)+" AND kind='committed'")
	if committed != "4" {
		t.Fatalf("committed decision rows for grouped commit=%s = %s want 4 (one per original seq)", oid, committed)
	}

	if subj := headSubject(t, repo); subj != "Coalesced burst" {
		t.Fatalf("HEAD subject=%q want %q (planner subject must land for grouped commit)", subj, "Coalesced burst")
	}

	if hits.Load() != 1 {
		t.Fatalf("planner hits=%d want 1 (path coalesce must produce a single planner offer)", hits.Load())
	}

	// No planner errors must accompany this clean grouped publish.
	plannerErrors := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE kind='intent_planner_error'")
	if plannerErrors != "0" {
		t.Fatalf("intent_planner_error decisions=%s want 0 for clean grouped publish", plannerErrors)
	}
}

// TestIntentAtomicity_InterleavedABADoesNotCoalesce drives an A→B→A interleave
// on two distinct paths. The path-coalesce gate folds runs of same-path
// modifies; an unrelated path between two A captures must keep the boundaries
// intact, so the planner sees three distinct offers and the daemon emits at
// least two commits even when the mock planner accepts each offered window
// in full.
func TestIntentAtomicity_InterleavedABADoesNotCoalesce(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	// Seed both files so subsequent writes capture as modify ops (matching
	// the same-path coalesce contract).
	pathA := filepath.Join(repo, "alpha.txt")
	pathB := filepath.Join(repo, "beta.txt")
	writeFile(t, pathA, "a0\n")
	writeFile(t, pathB, "b0\n")
	gitCommitAll(t, repo, "seed alpha+beta")

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		seqs := offeredIntentSeqs(t, req)
		if len(seqs) == 0 {
			http.Error(w, "expected at least one offered capture", http.StatusBadRequest)
			return
		}
		// Aggressively select every offered capture so the planner is NOT
		// the reason commits split — only the daemon's no-coalesce-across-
		// boundaries contract can be the reason.
		plan := map[string]any{
			"selected_seqs":    seqs,
			"deferred_seqs":    []int64{},
			"subject":          "ABA grouped",
			"body":             "Planner accepts every offer.",
			"grouping_reason":  "ABA negative test",
			"deferred_reasons": []map[string]any{},
		}
		args, _ := json.Marshal(plan)
		resp := map[string]any{
			"id":     "chatcmpl-aba",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_aba",
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
		body, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=3",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "intent-atomic-aba", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	startCount := commitCount(t, repo)

	// A → B → A interleave. Wake between writes so each modify becomes its
	// own capture row (otherwise A1 and A2 collapse against the shadow).
	writeFile(t, pathA, "a1\n")
	wakeSession(t, ctx, fullEnv, repo, "intent-atomic-aba")
	waitFor(t, "pending alpha event", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM capture_events WHERE path='alpha.txt' AND state='pending'") == "1"
	})

	writeFile(t, pathB, "b1\n")
	wakeSession(t, ctx, fullEnv, repo, "intent-atomic-aba")
	waitFor(t, "pending beta event", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM capture_events WHERE path='beta.txt' AND state='pending'") == "1"
	})

	writeFile(t, pathA, "a2\n")
	wakeSession(t, ctx, fullEnv, repo, "intent-atomic-aba")
	waitFor(t, "pending alpha event", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM capture_events WHERE path='alpha.txt' AND state='pending'") == "2"
	})

	// Trip the planner. Three distinct offered captures (A1, B1, A2) must
	// produce at least two commits because the A→B→A boundary breaks the
	// path-coalesce gate; coalescing across B would change the worktree
	// state in a way that can't be folded into a single tree-write.
	wakeSession(t, ctx, fullEnv, repo, "intent-atomic-aba")
	waitForEventState(t, dbPath, "alpha.txt", "published", 15*time.Second)
	waitForEventState(t, dbPath, "beta.txt", "published", 15*time.Second)

	if got := commitCount(t, repo); got < startCount+2 {
		t.Fatalf("commit count=%d want >= %d (A→B→A must yield at least two commits, no full coalesce)",
			got, startCount+2)
	}
	// And no planner errors on the clean ABA path either.
	plannerErrors := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE kind='intent_planner_error'")
	if plannerErrors != "0" {
		t.Fatalf("intent_planner_error decisions=%s want 0", plannerErrors)
	}
	if hits.Load() == 0 {
		t.Fatal("mock planner was not called")
	}
}

// intToString stringifies an int without pulling in strconv at every call site
// in the helpers above. The values here are tiny test counters.
func intToString(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

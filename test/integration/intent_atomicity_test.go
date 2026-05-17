//go:build integration
// +build integration

package integration_test

// intent_atomicity_test.go — verification-lane Wave 3 acceptance tests for
// the b1 outcome (intent grouped publishes are atomic) that landed earlier
// in this branch.
//
// The exhaustive same-path 4-edit coalesce proof lives at the daemon
// package level in internal/daemon/replay_test.go (see
// TestReplay_IntentPathCoalesce_FoldsFourEditsIntoOneOffer): four sequential
// captures on burst.txt fold into a single planner offer, every covered seq
// shares the resulting commit_oid, and decision_records carries one row per
// original seq joined by commit_oid.
//
// The integration suite cannot drive four sequential same-path captures
// deterministically: `acd pause` halts capture as well as replay, so a
// write-pause-write sequence produces ONE capture against the worktree state
// at resume rather than four. We therefore drive the same b1
// guarantee end-to-end at the multi-FILE granularity here:
//
//   - Pause the daemon, write four distinct new files in one shot, resume.
//     Capture observes four creates against the baseline shadow; the
//     planner is offered four distinct entries; the mock provider selects
//     all four; the daemon publishes ONE grouped commit covering every
//     capture seq under the same commit_oid; decision_records carries
//     four rows joined by that commit_oid.
//
//   - Pause, write three files, resume. With the planner deferring the
//     middle file (B), publishes A and C in one grouped commit while B
//     stays pending. This proves the daemon does NOT coalesce across
//     planner-deferred captures even when they sit between two selected
//     ones — the at-least-two-commits contract is the planner-decision
//     analogue of the b1 path-boundary contract.

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

// TestIntentAtomicity_FourFileBatchLandsAsOneGroupedCommit drives four
// distinct creates through the real daemon under intent strategy. The
// mock planner accepts every offered seq; the daemon must publish ONE
// commit covering all four, with decision_records carrying one row per
// original seq joined by commit_oid (so the CLI's grouped_seqs derivation
// reports len 4). This is the integration-level acceptance of the
// "intent group publishes atomically" contract from b1.
func TestIntentAtomicity_FourFileBatchLandsAsOneGroupedCommit(t *testing.T) {
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
		if len(seqs) < 4 {
			http.Error(w, "expected at least four offered captures", http.StatusBadRequest)
			return
		}
		plan := map[string]any{
			"selected_seqs":    seqs,
			"deferred_seqs":    []int64{},
			"subject":          "Atomic four-file group",
			"body":             "Group every offered capture in one commit.",
			"grouping_reason":  "atomicity test: select all four offered seqs",
			"deferred_reasons": []map[string]any{},
		}
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-atomic-4",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_atomic",
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

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "intent-atomic-batch4", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	// Pause-then-write-then-resume so all four creates surface in a single
	// post-resume capture pass. This mirrors how a multi-file edit in a
	// real harness flows: the harness pauses its own activity while the
	// editor writes the burst, then the daemon catches the whole batch.
	paused := runAcd(t, ctx, fullEnv, "pause", "--repo", repo, "--reason", "atomic batch test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	files := []string{"atomic-a.txt", "atomic-b.txt", "atomic-c.txt", "atomic-d.txt"}
	for _, name := range files {
		writeFile(t, filepath.Join(repo, name), "atomic content for "+name+"\n")
	}

	startCount := commitCount(t, repo)
	resumed := runAcd(t, ctx, fullEnv, "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	flushed := runAcd(t, ctx, fullEnv, "flush", "--repo", repo, "--session-id", "intent-atomic-batch4", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s", flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	for _, name := range files {
		waitForEventState(t, dbPath, name, "published", 20*time.Second)
	}

	// Exactly one new commit covering all four files.
	if got, want := commitCount(t, repo), startCount+1; got != want {
		t.Fatalf("commit count=%d want %d (four-file batch must land as one commit)", got, want)
	}

	// All four capture rows must share the same commit_oid.
	oid := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='atomic-a.txt' AND state='published' ORDER BY seq DESC LIMIT 1")
	if oid == "" {
		t.Fatalf("expected non-empty commit_oid for atomic-a.txt")
	}
	for _, name := range files {
		got := sqliteScalar(t, dbPath,
			"SELECT commit_oid FROM capture_events WHERE path="+sqliteQuote(name)+" AND state='published' ORDER BY seq DESC LIMIT 1")
		if got != oid {
			t.Fatalf("commit_oid for %s = %q want %q (all four captures must share one commit)", name, got, oid)
		}
	}

	// decision_records must carry one committed row per original seq for
	// the same commit_oid (this is what the CLI's events command reads
	// when deriving grouped_seqs).
	committed := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE commit_oid="+sqliteLiteral(oid)+" AND kind='committed'")
	if committed != "4" {
		t.Fatalf("committed decision rows for grouped commit=%s = %s want 4 (one per original seq, the grouped_seqs basis)",
			oid, committed)
	}

	if subj := headSubject(t, repo); subj != "Atomic four-file group" {
		t.Fatalf("HEAD subject=%q want %q (planner subject must land for grouped commit)", subj, "Atomic four-file group")
	}
	if hits.Load() != 1 {
		t.Fatalf("planner hits=%d want 1 (single offered window for the four creates)", hits.Load())
	}
	plannerErrors := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE kind='intent_planner_error'")
	if plannerErrors != "0" {
		t.Fatalf("intent_planner_error decisions=%s want 0 for clean grouped publish", plannerErrors)
	}
}

// TestIntentAtomicity_DeferredMiddleSplitsCommit drives an A, B, C three-file
// batch where the planner defers B and selects A+C. The daemon must publish
// A and C as ONE grouped commit and leave B pending — proving the
// at-least-two-commits negative arm: when the planner draws a boundary in
// the middle of the offered window, the daemon honors it and does NOT fold
// the prefix and suffix together. The deferred capture remains in
// planner_state with defer_count >= 1.
func TestIntentAtomicity_DeferredMiddleSplitsCommit(t *testing.T) {
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
		if len(seqs) < 3 {
			http.Error(w, "expected three offered captures", http.StatusBadRequest)
			return
		}
		// Defer the middle seq, select the bookends. ValidateIntentPlan
		// requires every offered seq to appear in selected or deferred and
		// requires deferred_reasons to cover every deferred seq.
		selected := []int64{seqs[0], seqs[2]}
		deferred := []int64{seqs[1]}
		plan := map[string]any{
			"selected_seqs":    selected,
			"deferred_seqs":    deferred,
			"subject":          "Bookends only",
			"body":             "Defer middle to prove no prefix/suffix coalesce.",
			"grouping_reason":  "split: select first and third, defer middle",
			"deferred_reasons": buildDeferredReasons(deferred),
		}
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-split",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_split",
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

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		// Hold off forced-aging so the deferred middle does NOT immediately
		// publish on the next tick — we want it to remain pending so the
		// "at least two commits" contract is observable as a delta in
		// commit_count after a single replay pass.
		"ACD_INTENT_DEFER_LIMIT=5",
		"ACD_INTENT_MAX_PENDING_AGE=5m",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "intent-split-middle", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	// Pause-then-write-then-resume so all three creates surface in one
	// capture pass and the planner sees three offered seqs together.
	paused := runAcd(t, ctx, fullEnv, "pause", "--repo", repo, "--reason", "split test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	for _, name := range []string{"split-a.txt", "split-b.txt", "split-c.txt"} {
		writeFile(t, filepath.Join(repo, name), name+" content\n")
	}

	startCount := commitCount(t, repo)
	resumed := runAcd(t, ctx, fullEnv, "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	flushed := runAcd(t, ctx, fullEnv, "flush", "--repo", repo, "--session-id", "intent-split-middle", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s", flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "split-a.txt", "published", 20*time.Second)
	waitForEventState(t, dbPath, "split-c.txt", "published", 20*time.Second)

	// One new commit (the bookends grouped); the deferred middle is still
	// pending. With IntentDeferLimit=5 the next tick can't force-publish
	// it, so commitCount stays at startCount+1.
	if got, want := commitCount(t, repo), startCount+1; got != want {
		t.Fatalf("commit count=%d want %d (one grouped commit for A+C; deferred B must remain pending)",
			got, want)
	}

	// Bookends share commit_oid. The deferred middle MUST stay pending
	// (state='pending', commit_oid empty/null).
	oidA := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='split-a.txt' AND state='published'")
	oidC := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='split-c.txt' AND state='published'")
	if oidA == "" || oidA != oidC {
		t.Fatalf("bookend commit_oids A=%q C=%q (expected to share one commit)", oidA, oidC)
	}
	stateB := sqliteScalar(t, dbPath,
		"SELECT state FROM capture_events WHERE path='split-b.txt' ORDER BY seq DESC LIMIT 1")
	if stateB != "pending" {
		t.Fatalf("split-b.txt state=%q want pending (planner deferred it; daemon must NOT coalesce across)", stateB)
	}
	deferCount := sqliteScalar(t, dbPath,
		"SELECT IFNULL(MAX(defer_count), 0) FROM planner_state ps JOIN capture_events ce ON ce.seq=ps.event_seq WHERE ce.path='split-b.txt'")
	if deferCount == "" || deferCount == "0" {
		t.Fatalf("planner_state.defer_count for split-b.txt=%q want >=1 (defer must be recorded)", deferCount)
	}

	if hits.Load() != 1 {
		t.Fatalf("planner hits=%d want 1 (single offered window for the three creates)", hits.Load())
	}
}

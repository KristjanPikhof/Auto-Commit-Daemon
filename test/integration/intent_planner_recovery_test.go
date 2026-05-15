//go:build integration
// +build integration

package integration_test

// intent_planner_recovery_test.go — verification-lane Wave 3 acceptance
// tests for the a1/c1 recovery paths that landed earlier on this branch:
//
//   - Composed retry (a1): when the openai-compat provider returns a
//     malformed plan on the first call, the composed planner re-prompts the
//     same provider with a RetryCorrection appended. A valid plan on the
//     second attempt must NOT surface an intent_planner_error decision —
//     the composed wrapper absorbs the validation error in-process. The
//     daemon-side proof lives in internal/ai/intent_planner_test.go; this
//     file checks the same path through the real `acd` binary.
//
//   - Forced-aging singleton (c1): when a single capture has been deferred
//     enough times to trip ACD_INTENT_DEFER_LIMIT, replay must skip the
//     provider entirely (planIntentSingletonFastPath) and use the
//     diff-aware subject fallback. The mock planner counts HTTP hits; the
//     test asserts only the deferral call lands and the resulting commit
//     subject is the Go function name (NOT the generic "Update <basename>"
//     placeholder).
//
// Companion daemon-package coverage:
//   - internal/ai/intent_planner_test.go (composed retry + retry-correction)
//   - internal/daemon/replay_test.go TestReplay_IntentStrategyForcedAging…
//     SingletonSkipsProviderAndUsesDiffSubject (forced-singleton fast path)

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

// TestIntentPlannerRecovery_RetryAbsorbsValidationError simulates an
// openai-compat planner whose first response is structurally valid JSON but
// semantically invalid (deferred_reasons.seq references a selected seq —
// the same shape as ai/testdata/intent_planner/bad_deferred_reason.json
// before normalization). The composed retry loop must:
//
//  1. Detect the *IntentPlanValidationError on attempt 1.
//  2. Re-prompt the same provider with the validator message appended.
//  3. Accept the valid plan returned on attempt 2.
//  4. Publish the grouped commit WITHOUT recording an intent_planner_error
//     row (the retry suppresses the failure inside Compose).
//
// The provider-side normalization pass actually drops the spurious entry
// for the FIRST response shape today (see
// intent_planner_normalization_test.go), so to exercise the retry path
// proper this test returns a different first-attempt failure: an empty
// selected_seqs (validator code IntentPlanValidationEmptySelected) that
// normalization cannot heal. The second attempt then returns the full
// selection and the commit lands cleanly.
func TestIntentPlannerRecovery_RetryAbsorbsValidationError(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var hits atomic.Int32
	var sawRetryCorrection atomic.Bool
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := hits.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		seqs := offeredIntentSeqs(t, req)
		if len(seqs) < 2 {
			http.Error(w, "expected at least two offered captures", http.StatusBadRequest)
			return
		}

		// On retry the composed loop appends the validator message into the
		// user prompt as a "Correction:" block. Detect it so we can prove
		// the retry path actually fired.
		for _, msg := range req.Messages {
			if strings.Contains(msg.Content, "Correction:") {
				sawRetryCorrection.Store(true)
			}
		}

		var plan map[string]any
		if call == 1 {
			// First attempt: empty selected_seqs. Normalization cannot heal
			// this; ValidateIntentPlan returns
			// IntentPlanValidationError{Code: IntentPlanValidationEmptySelected}
			// and the composed loop will retry once with RetryCorrection set.
			plan = map[string]any{
				"selected_seqs":    []int64{},
				"deferred_seqs":    seqs,
				"subject":          "First-attempt placeholder",
				"body":             "Will be retried.",
				"grouping_reason":  "intentional first-attempt failure",
				"deferred_reasons": buildDeferredReasons(seqs),
			}
		} else {
			// Second attempt: clean grouped plan. The retry path must
			// accept this and the daemon must NOT log an
			// intent_planner_error decision.
			plan = map[string]any{
				"selected_seqs":    seqs,
				"deferred_seqs":    []int64{},
				"subject":          "Recovered after retry",
				"body":             "Composed retry absorbed the first failure.",
				"grouping_reason":  "second-attempt success",
				"deferred_reasons": []map[string]any{},
			}
		}
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-recovery",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_recovery",
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
	startSession(t, ctx, env, repo, "intent-recovery-retry", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	// Two captures with the daemon paused so they batch into one planner
	// offering — same pattern as intent_strategy_test.go's grouping check.
	paused := runAcd(t, ctx, fullEnv, "pause", "--repo", repo, "--reason", "retry recovery test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	writeFile(t, filepath.Join(repo, "recovery-one.txt"), "one\n")
	writeFile(t, filepath.Join(repo, "recovery-two.txt"), "two\n")

	startCount := commitCount(t, repo)
	resumed := runAcd(t, ctx, fullEnv, "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	wakeSession(t, ctx, fullEnv, repo, "intent-recovery-retry")

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "recovery-one.txt", "published", 15*time.Second)
	waitForEventState(t, dbPath, "recovery-two.txt", "published", 15*time.Second)

	// Same commit on both captures = grouped publish from the second
	// (valid) plan. If the retry had failed we would expect either two
	// separate deterministic commits OR an intent_planner_error decision.
	oidOne := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='recovery-one.txt' ORDER BY seq DESC LIMIT 1")
	oidTwo := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='recovery-two.txt' ORDER BY seq DESC LIMIT 1")
	if oidOne == "" || oidOne != oidTwo {
		t.Fatalf("grouped commit oids one=%q two=%q (expected retry to land both as one commit)", oidOne, oidTwo)
	}
	if got, want := commitCount(t, repo), startCount+1; got != want {
		t.Fatalf("commit count=%d want %d (retry must publish single grouped commit)", got, want)
	}
	if subj := headSubject(t, repo); subj != "Recovered after retry" {
		t.Fatalf("HEAD subject=%q want %q (second-attempt subject must land)", subj, "Recovered after retry")
	}
	if hits.Load() < 2 {
		t.Fatalf("planner hits=%d want >= 2 (retry must invoke provider twice)", hits.Load())
	}
	if !sawRetryCorrection.Load() {
		t.Fatal("retry attempt did not include a Correction: block in the user prompt")
	}
	plannerErrors := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE kind='intent_planner_error'")
	if plannerErrors != "0" {
		t.Fatalf("intent_planner_error decisions=%s want 0 (composed retry must absorb the first-attempt failure)", plannerErrors)
	}
}

// TestIntentPlannerRecovery_ForcedSingletonSkipsProvider drives the
// forced-aging singleton fast path through the real binary. With
// IntentDeferLimit=1, a single deferred capture trips the gate; the next
// replay tick must publish the capture WITHOUT another planner call. The
// commit subject is the diff-aware fallback (a Go function name extracted
// from the captured source) — proving the planner subject is NOT what
// landed (the subject in the planner's defer plan is "Defer placeholder").
func TestIntentPlannerRecovery_ForcedSingletonSkipsProvider(t *testing.T) {
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
		if len(seqs) == 0 {
			http.Error(w, "expected at least one offered capture", http.StatusBadRequest)
			return
		}
		// First (and only) call: defer everything offered. The replay loop
		// records the defer in planner_state, defer_count climbs to 1,
		// matches IntentDeferLimit=1, and the next replay tick takes the
		// forced-singleton fast path — no second provider call.
		plan := map[string]any{
			"selected_seqs":    []int64{},
			"deferred_seqs":    seqs,
			"subject":          "Defer placeholder",
			"body":             "Waiting for related edits.",
			"grouping_reason":  "first pass: defer to test forced-aging",
			"deferred_reasons": buildDeferredReasons(seqs),
		}
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-defer",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_defer",
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

	// IntentDeferLimit=1 keeps the test budget tight: one defer is enough
	// to flip the gate. IntentMinPending=1 lets a single capture reach the
	// planner; IntentMaxPendingAge=2s ensures the second replay tick
	// inside the 60s test budget treats the deferred capture as overdue.
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=1",
		"ACD_INTENT_MAX_PENDING_AGE=2s",
		"ACD_INTENT_DEFER_LIMIT=1",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "intent-recovery-singleton", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	// Real Go source so the diff-aware subject fallback extracts the
	// function name. This is what proves the planner subject is NOT the
	// one that landed (planner subject would have been "Defer placeholder"
	// from the only call we accept).
	body := "package overdue\n\nfunc HandleOverdueCapture() error {\n\treturn nil\n}\n"
	target := filepath.Join(repo, "overdue.go")
	writeFile(t, target, body)
	wakeSession(t, ctx, fullEnv, repo, "intent-recovery-singleton")

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	// Wait for the first planner call to land a defer. The defer is
	// recorded in planner_state; we poll for defer_count >= 1 on the
	// pending row before pushing the next tick.
	waitFor(t, "planner deferred overdue.go (defer_count>=1)", 15*time.Second, func() bool {
		got := sqliteScalar(t, dbPath,
			"SELECT IFNULL(MAX(defer_count), 0) FROM planner_state ps "+
				"JOIN capture_events ce ON ce.seq = ps.event_seq "+
				"WHERE ce.path='overdue.go'")
		return got != "" && got != "0"
	})
	// Capture how many hits the deferral pass cost — must be exactly 1.
	hitsAfterDefer := hits.Load()

	// Wait past IntentMaxPendingAge so the next replay tick treats the
	// deferred capture as forced-aging-ready, then drive the tick. The
	// planner must NOT be called again (planIntentSingletonFastPath gate).
	time.Sleep(2500 * time.Millisecond)
	wakeSession(t, ctx, fullEnv, repo, "intent-recovery-singleton")
	waitForEventState(t, dbPath, "overdue.go", "published", 15*time.Second)

	if got := hits.Load(); got != hitsAfterDefer {
		t.Fatalf("planner hits after forced-singleton tick=%d want %d (forced-aging singleton must skip provider)",
			got, hitsAfterDefer)
	}

	// Subject MUST be the diff-aware fallback ("Add HandleOverdueCapture"),
	// NOT the planner's own "Defer placeholder", and NOT the legacy
	// generic "Add overdue.go" / "Update overdue.go" strings.
	subj := headSubject(t, repo)
	if subj == "Defer placeholder" {
		t.Fatalf("HEAD subject=%q is the planner's defer placeholder; forced-singleton must use diff-aware fallback", subj)
	}
	if strings.HasPrefix(subj, "Add overdue.go") || strings.HasPrefix(subj, "Update overdue.go") {
		t.Fatalf("HEAD subject=%q is the generic basename fallback; expected the diff-aware Go-symbol fallback", subj)
	}
	if !strings.Contains(subj, "HandleOverdueCapture") {
		t.Fatalf("HEAD subject=%q must contain the extracted Go symbol HandleOverdueCapture", subj)
	}
}

// buildDeferredReasons emits the deferred_reasons array OpenAI-style for the
// given seqs. ValidateIntentPlan requires every deferred seq to have a
// matching deferred_reasons entry, so we generate one per seq.
func buildDeferredReasons(seqs []int64) []map[string]any {
	out := make([]map[string]any, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, map[string]any{
			"seq":    seq,
			"reason": "synthetic defer for recovery test",
		})
	}
	return out
}

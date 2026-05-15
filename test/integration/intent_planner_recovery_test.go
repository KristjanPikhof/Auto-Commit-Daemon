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

// TestIntentPlannerRecovery_RetryAbsorbsEmptySelectedError simulates an
// openai-compat planner whose first response trips the typed validator
// with IntentPlanValidationEmptySelected — selected_seqs comes back as
// an empty array which normalization cannot heal. The composed retry
// loop must:
//
//  1. Detect the *IntentPlanValidationError{Code:
//     IntentPlanValidationEmptySelected} on attempt 1.
//  2. Re-prompt the same provider with the validator message appended.
//  3. Accept the valid plan returned on attempt 2.
//  4. Publish the grouped commit WITHOUT recording an intent_planner_error
//     row (the retry suppresses the failure inside Compose).
//
// We deliberately do NOT exercise the bad_deferred_reason shape here:
// the openai-compat provider's NormalizeIntentPlanDeferredReasons pass
// drops spurious entries before ValidateIntentPlan ever runs (see
// intent_planner_normalization_test.go), so that path never reaches
// the typed-error retry surface. EmptySelected is a clean proxy for
// "real semantic failure that the retry path absorbs".
func TestIntentPlannerRecovery_RetryAbsorbsEmptySelectedError(t *testing.T) {
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
		seqs := offeredIntentSeqsLenient(t, req)
		if len(seqs) < 2 {
			http.Error(w, "expected at least two offered captures", http.StatusBadRequest)
			return
		}

		// On retry the composed loop appends the validator message into the
		// user prompt as a "Your previous capture_intent_plan tool call
		// failed validation" block. Detect that suffix so we can prove the
		// retry path actually fired.
		for _, msg := range req.Messages {
			if strings.Contains(msg.Content, "previous capture_intent_plan tool call failed validation") {
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
// forced-aging singleton fast path through the real binary. The flow:
//
//  1. Two captures offered together; the mock planner selects ONE and
//     defers the OTHER (selected/deferred coverage that passes
//     ValidateIntentPlan, which requires non-empty selected_seqs).
//  2. After publish, only the deferred capture survives as pending with
//     defer_count >= IntentDeferLimit (set to 1).
//  3. Next replay tick: pending=1, with defer_count >= limit. The
//     selectIntentWindow forced-aging branch fires AND len(items)==1, so
//     planIntentSingletonFastPath skips the provider entirely and lands
//     a diff-aware-subject commit.
//
// The mock provider counts HTTP hits; only the first (defer) call must
// reach it. The published commit subject must contain the extracted Go
// function symbol from the diff (NOT the planner's own subject from the
// first call, since the planner is never asked the second time).
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
		captures := offeredIntentCaptures(t, req)
		if len(captures) < 2 {
			http.Error(w, "expected at least two offered captures", http.StatusBadRequest)
			return
		}
		// Find overdue.go and defer it; select everything else. We MUST key
		// on path (not seq order) because capture enumeration is path-sorted
		// — a basename-alphabetical comparison would put overdue.go ahead of
		// warm.txt regardless of write order.
		var selected, deferred []int64
		for _, c := range captures {
			if c.Path == "overdue.go" {
				deferred = append(deferred, c.Seq)
			} else {
				selected = append(selected, c.Seq)
			}
		}
		if len(selected) == 0 || len(deferred) == 0 {
			http.Error(w, "expected at least one selected and one deferred capture", http.StatusBadRequest)
			return
		}
		plan := map[string]any{
			"selected_seqs":    selected,
			"deferred_seqs":    deferred,
			"subject":          "Planner: pick warm-up",
			"body":             "Defer the Go file to drive forced-aging.",
			"grouping_reason":  "first pass: select warm-up, defer the rest",
			"deferred_reasons": buildDeferredReasons(deferred),
		}
		args, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal intent plan: %v", err)
		}
		resp := map[string]any{
			"id":     "chatcmpl-singleton",
			"object": "chat.completion",
			"model":  "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_singleton",
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

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// IntentDeferLimit=1 keeps the test budget tight: one defer is enough
	// to flip the forced-aging gate. IntentMaxPendingAge stays small so
	// the second replay tick treats the deferred capture as overdue.
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=2",
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

	// Pause the daemon so we can drop both files atomically; the planner
	// must see them in the same offered window so the "select first,
	// defer rest" mock plan picks warm.txt and defers overdue.go.
	paused := runAcd(t, ctx, fullEnv, "pause", "--repo", repo, "--reason", "singleton recovery test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	// warm.txt seq is lower than overdue.go (alphabetical write order isn't
	// guaranteed; capture orders by event time). Write warm.txt FIRST so
	// it gets the smaller seq, then overdue.go so it's the one deferred.
	writeFile(t, filepath.Join(repo, "warm.txt"), "warm\n")
	writeFile(t, filepath.Join(repo, "overdue.go"),
		"package overdue\n\nfunc HandleOverdueCapture() error {\n\treturn nil\n}\n")

	resumed := runAcd(t, ctx, fullEnv, "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	wakeSession(t, ctx, fullEnv, repo, "intent-recovery-singleton")

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")

	// Wait for warm.txt to land (selected) and overdue.go to gather a
	// defer (defer_count >= 1).
	waitForEventState(t, dbPath, "warm.txt", "published", 15*time.Second)
	waitFor(t, "planner deferred overdue.go (defer_count>=1)", 15*time.Second, func() bool {
		got := sqliteScalar(t, dbPath,
			"SELECT IFNULL(MAX(defer_count), 0) FROM planner_state ps "+
				"JOIN capture_events ce ON ce.seq = ps.event_seq "+
				"WHERE ce.path='overdue.go'")
		return got != "" && got != "0"
	})
	// Capture how many hits the deferral pass cost. Must be at least 1
	// (the defer call); subsequent tick must add zero.
	hitsAfterDefer := hits.Load()
	if hitsAfterDefer == 0 {
		t.Fatal("planner was never called for the first (defer) pass")
	}

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

	// HEAD subject MUST be the diff-aware fallback ("Add HandleOverdueCapture"),
	// NOT the planner's "Planner: pick warm-up" subject from the first call,
	// and NOT the generic "Add overdue.go" / "Update overdue.go" basename
	// fallback. Reading HEAD subject only tells us about the LAST commit;
	// since overdue.go publishes after warm.txt, HEAD reflects overdue.go.
	subj := headSubject(t, repo)
	if subj == "Planner: pick warm-up" {
		t.Fatalf("HEAD subject=%q is the planner's first-pass subject; forced-singleton must use diff-aware fallback", subj)
	}
	if subj == "Add overdue.go" || subj == "Update overdue.go" {
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

// offeredIntentSeqsLenient is the retry-aware variant of offeredIntentSeqs.
// On a retry attempt the composed planner appends a free-text correction
// block AFTER the embedded JSON, which breaks json.Unmarshal of the entire
// payload. We use json.Decoder so we can stop reading at the end of the
// embedded object.
func offeredIntentSeqsLenient(t *testing.T, req intentChatRequest) []int64 {
	t.Helper()
	captures := offeredIntentCaptures(t, req)
	out := make([]int64, 0, len(captures))
	for _, c := range captures {
		out = append(out, c.Seq)
	}
	return out
}

// offeredIntentCapture is the {seq,path} subset of intentPlanPromptPayload
// the recovery test needs to address captures by path rather than by
// arbitrary capture order.
type offeredIntentCapture struct {
	Seq  int64
	Path string
}

// offeredIntentCaptures parses the planner user prompt and returns the
// list of offered captures with their paths. Lenient about trailing
// non-JSON content (e.g. composed retry's Correction: block).
func offeredIntentCaptures(t *testing.T, req intentChatRequest) []offeredIntentCapture {
	t.Helper()
	const marker = "Plan the next commit intent for these offered captures:\n"
	for _, msg := range req.Messages {
		if !strings.HasPrefix(msg.Content, marker) {
			continue
		}
		body := strings.TrimPrefix(msg.Content, marker)
		dec := json.NewDecoder(strings.NewReader(body))
		var payload struct {
			OfferedCaptures []struct {
				Seq  int64  `json:"seq"`
				Path string `json:"path"`
			} `json:"offered_captures"`
		}
		if err := dec.Decode(&payload); err != nil {
			t.Fatalf("decode intent prompt payload (captures): %v\nbody=%s", err, body)
		}
		out := make([]offeredIntentCapture, 0, len(payload.OfferedCaptures))
		for _, c := range payload.OfferedCaptures {
			out = append(out, offeredIntentCapture{Seq: c.Seq, Path: c.Path})
		}
		return out
	}
	t.Fatalf("intent planner user prompt not found in request: %+v", req)
	return nil
}

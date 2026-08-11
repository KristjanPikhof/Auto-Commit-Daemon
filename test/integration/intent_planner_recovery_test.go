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
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestIntentPlannerRecovery_RetryAbsorbsEligibleValidationError simulates an
// openai-compat planner whose first response trips the typed validator with
// an empty ready-candidate subject. That metadata-only failure is eligible
// for the single bounded remote correction. The composed retry
// loop must:
//
//  1. Detect the eligible ready_subject_empty validation finding on attempt 1.
//  2. Re-prompt the same provider with the validator message appended.
//  3. Accept the valid plan returned on attempt 2.
//  4. Publish the grouped commit WITHOUT recording an intent_planner_error
//     row (the retry suppresses the failure inside Compose).
func TestIntentPlannerRecovery_RetryAbsorbsEligibleValidationError(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var plannerHits atomic.Int32
	var rewriteHits atomic.Int32
	var sawRetryCorrection atomic.Bool
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		if req.ToolChoice.Function.Name == "commit_message" {
			rewriteHits.Add(1)
			writeIntentMessageRewriteResponse(t, w, req)
			return
		}
		call := plannerHits.Add(1)
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
			if strings.Contains(msg.Content,
				"previous candidate plan failed atomicity validation") {
				sawRetryCorrection.Store(true)
			}
		}

		var plan map[string]any
		if call != 1 {
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
		if call == 1 {
			writeNativeIntentCandidatesResponse(t, w, "call_recovery", []map[string]any{
				nativeReadyIntentCandidate("recovery-group", seqs, "", "- Correct the accepted candidate metadata.", "group related recovery changes"),
			})
		} else {
			writeIntentPlanResponse(t, w, "call_recovery", plan)
		}
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
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	startSession(t, ctx, env, repo, "intent-recovery-retry", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	// Two captures with the daemon paused so they batch into one planner
	// offering — same pattern as intent_strategy_test.go's grouping check.
	paused := runAcd(t, ctx, fullEnv, "pause", "--repo", repo, "--reason", "retry recovery test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	writeFile(t, filepath.Join(repo, "internal/recovery/one.go"),
		"package recovery\n\nfunc One() {}\n")
	writeFile(t, filepath.Join(repo, "internal/recovery/two.go"),
		"package recovery\n\nfunc Two() {}\n")

	startCount := commitCount(t, repo)
	resumed := runAcd(t, ctx, fullEnv, "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	flushed := runAcd(t, ctx, fullEnv, "flush", "--repo", repo, "--session-id", "intent-recovery-retry", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s", flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "internal/recovery/one.go", "published", 15*time.Second)
	waitForEventState(t, dbPath, "internal/recovery/two.go", "published", 15*time.Second)

	// Same commit on both captures = grouped publish from the second
	// (valid) plan. If the retry had failed we would expect either two
	// separate deterministic commits OR an intent_planner_error decision.
	oidOne := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='internal/recovery/one.go' ORDER BY seq DESC LIMIT 1")
	oidTwo := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='internal/recovery/two.go' ORDER BY seq DESC LIMIT 1")
	if oidOne == "" || oidOne != oidTwo {
		t.Fatalf("grouped commit oids one=%q two=%q (expected retry to land both as one commit)", oidOne, oidTwo)
	}
	if got, want := commitCount(t, repo), startCount+1; got != want {
		t.Fatalf("commit count=%d want %d (retry must publish single grouped commit)", got, want)
	}
	if subj := headSubject(t, repo); subj != "Recovered after retry" {
		t.Fatalf("HEAD subject=%q want corrected plan subject", subj)
	}
	if plannerHits.Load() != 2 {
		t.Fatalf("planner hits=%d want 2 (retry must invoke provider twice)", plannerHits.Load())
	}
	if rewriteHits.Load() != 0 {
		t.Fatalf("message rewrite hits=%d want 0 for acceptable corrected message", rewriteHits.Load())
	}
	if !sawRetryCorrection.Load() {
		t.Fatal("retry attempt did not include a Correction: block in the user prompt")
	}
	plannerErrors := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE kind='intent_planner_error'")
	if plannerErrors != "0" {
		t.Fatalf("intent_planner_error decisions=%s want 0 (composed retry must absorb the first-attempt failure)", plannerErrors)
	}
	if got := sqliteScalar(t, dbPath, `
SELECT COUNT(*) FROM intent_candidates
WHERE planner_protocol='v2' AND status='published'`); got != "1" {
		t.Fatalf("native v2 recovery candidates=%s want 1", got)
	}
}

// TestIntentPlannerRecovery_ForcedSingletonUsesProvider drives the
// forced-aging singleton provider path through the real binary. The flow:
//
//  1. Two captures offered together; the mock planner selects ONE and
//     defers the OTHER (selected/deferred coverage that passes
//     ValidateIntentPlan, which requires non-empty selected_seqs).
//  2. After publish, only the deferred capture survives as pending with
//     defer_count >= IntentDeferLimit (set to 1).
//  3. Next replay tick: pending=1, with defer_count >= limit. Because the
//     configured planner is not deterministic, replay asks the provider for
//     the overdue singleton so message-quality policy can run.
//
// The mock provider counts HTTP hits; the first call defers overdue.go, and
// the second call returns a semantic singleton message. The published commit
// subject must be the second provider subject, not the first-pass subject and
// not a generic basename fallback.
func TestIntentPlannerRecovery_ForcedSingletonUsesProvider(t *testing.T) {
	t.Parallel()
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
		if len(captures) == 0 {
			http.Error(w, "expected offered captures", http.StatusBadRequest)
			return
		}
		if len(captures) == 1 {
			plan := map[string]any{
				"selected_seqs":    []int64{captures[0].Seq},
				"deferred_seqs":    []int64{},
				"subject":          "Add overdue capture handler",
				"body":             "",
				"grouping_reason":  "forced-aging singleton: publish overdue capture",
				"deferred_reasons": []map[string]any{},
			}
			writeIntentPlanResponse(t, w, "call_singleton_forced", plan)
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
		writeIntentPlanResponse(t, w, "call_singleton_defer", plan)
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
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
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

	// Wait for warm.txt to land and verify overdue.go remains a durable
	// waiting candidate.
	waitForEventState(t, dbPath, "warm.txt", "published", 15*time.Second)
	waitFor(t, "planner retained overdue.go as waiting candidate", 15*time.Second, func() bool {
		got := sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM intent_candidates candidate "+
				"JOIN intent_candidate_events member ON member.candidate_id=candidate.id "+
				"JOIN capture_events capture ON capture.seq=member.event_seq "+
				"WHERE capture.path='overdue.go' AND candidate.status='waiting'")
		return got == "1"
	})
	// Capture how many hits the deferral pass cost. Must be at least 1
	// (the defer call); subsequent tick must add one forced-singleton call.
	hitsAfterDefer := hits.Load()
	if hitsAfterDefer == 0 {
		t.Fatal("planner was never called for the first (defer) pass")
	}

	// Wait past IntentMaxPendingAge so the next replay tick treats the
	// deferred capture as forced-aging-ready, then drive the tick. The
	// provider must be called again so message-quality policy can inspect
	// the singleton commit message.
	time.Sleep(2500 * time.Millisecond)
	wakeSession(t, ctx, fullEnv, repo, "intent-recovery-singleton")
	waitForEventState(t, dbPath, "overdue.go", "published", 15*time.Second)

	if got, want := hits.Load(), hitsAfterDefer+1; got != want {
		t.Fatalf("planner hits after forced-singleton tick=%d want %d (forced-aging singleton must use provider)",
			got, want)
	}

	// HEAD subject MUST be the second provider response, NOT the planner's
	// first-pass subject, and NOT the generic basename fallback. Reading HEAD
	// subject only tells us about the LAST commit; since overdue.go publishes
	// after warm.txt, HEAD reflects overdue.go.
	subj := headSubject(t, repo)
	if subj == "Planner: pick warm-up" {
		t.Fatalf("HEAD subject=%q is the planner's first-pass subject; forced-singleton must use second provider response", subj)
	}
	if subj == "Add overdue.go" || subj == "Update overdue.go" {
		t.Fatalf("HEAD subject=%q is the generic basename fallback; expected semantic provider subject", subj)
	}
	if subj != "Add overdue capture handler" {
		t.Fatalf("HEAD subject=%q want semantic provider subject", subj)
	}
	if got := sqliteScalar(t, dbPath, `
SELECT COUNT(*) FROM intent_candidates
WHERE planner_protocol='v2'`); got == "0" {
		t.Fatal("forced singleton did not retain native v2 candidate protocol")
	}
}

func writeIntentPlanResponse(t *testing.T, w http.ResponseWriter, callID string, plan map[string]any) {
	t.Helper()
	writeNativeIntentCandidatesResponse(
		t, w, callID, nativeIntentCandidatesFromLegacyPlan(plan))
}

func writeNativeIntentCandidatesResponse(
	t *testing.T,
	w http.ResponseWriter,
	callID string,
	candidates []map[string]any,
) {
	t.Helper()
	native := map[string]any{
		"protocol_version": "v2",
		"candidates":       candidates,
	}
	args, err := json.Marshal(native)
	if err != nil {
		t.Fatalf("marshal intent plan: %v", err)
	}
	resp := map[string]any{
		"id":     "chatcmpl-singleton",
		"object": "chat.completion",
		"model":  "gpt-5.4-mini",
		"choices": []map[string]any{{
			"index": 0,
			"message": map[string]any{
				"role":    "assistant",
				"content": "",
				"tool_calls": []map[string]any{{
					"id":   callID,
					"type": "function",
					"function": map[string]any{
						"name":      "capture_intent_plan_v2",
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
}

func nativeIntentCandidatesFromLegacyPlan(plan map[string]any) []map[string]any {
	var candidates []map[string]any
	if groups, ok := plan["commit_groups"].([]map[string]any); ok {
		for i, group := range groups {
			candidates = append(candidates, nativeReadyIntentCandidate(
				fmt.Sprintf("native-group-%d", i+1),
				group["selected_seqs"].([]int64),
				stringValue(group["subject"]), stringValue(group["body"]),
				stringValue(group["grouping_reason"])))
		}
	} else if selected, ok := plan["selected_seqs"].([]int64); ok &&
		len(selected) > 0 {
		candidates = append(candidates, nativeReadyIntentCandidate(
			"native-selected", selected,
			stringValue(plan["subject"]), stringValue(plan["body"]),
			stringValue(plan["grouping_reason"])))
	}
	reasons := map[int64]string{}
	if entries, ok := plan["deferred_reasons"].([]map[string]any); ok {
		for _, entry := range entries {
			seq, _ := entry["seq"].(int64)
			reasons[seq] = stringValue(entry["reason"])
		}
	}
	if deferred, ok := plan["deferred_seqs"].([]int64); ok {
		for _, seq := range deferred {
			reason := reasons[seq]
			if reason == "" {
				reason = "waiting for a required companion"
			}
			candidates = append(candidates, map[string]any{
				"candidate_id":  fmt.Sprintf("native-wait-%d", seq),
				"selected_seqs": []int64{seq},
				"purpose":       "retain deferred capture", "readiness": "wait",
				"missing_companions":    []string{reason},
				"depends_on_candidates": []string{},
				"subject":               "", "body": "",
				"grouping_reason": "planner deferred this capture",
			})
		}
	}
	return candidates
}

func nativeReadyIntentCandidate(
	id string,
	seqs []int64,
	subject string,
	body string,
	reason string,
) map[string]any {
	return map[string]any{
		"candidate_id": id, "selected_seqs": seqs,
		"purpose": reason, "readiness": "ready",
		"missing_companions":    []string{},
		"depends_on_candidates": []string{},
		"subject":               subject, "body": body, "grouping_reason": reason,
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
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
	markers := []string{
		"Plan durable semantic commit candidates for these offered captures:\n",
		"Plan the next commit intent for these offered captures:\n",
	}
	for _, msg := range req.Messages {
		body := ""
		for _, marker := range markers {
			if strings.HasPrefix(msg.Content, marker) {
				body = strings.TrimPrefix(msg.Content, marker)
				break
			}
		}
		if body == "" {
			continue
		}
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

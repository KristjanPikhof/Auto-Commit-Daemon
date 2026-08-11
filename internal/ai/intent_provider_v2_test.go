package ai

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

type rewritingIntentV2Provider struct {
	rewriteCalls int
	locked       IntentPlan
}

func (p *rewritingIntentV2Provider) Name() string { return "rewriting-v2" }

func (p *rewritingIntentV2Provider) Generate(context.Context, CommitContext) (Result, error) {
	return Result{}, errors.New("event generation is not used")
}

func (p *rewritingIntentV2Provider) PlanIntentV2(context.Context, IntentPlanRequestV2) (IntentPlanV2, error) {
	plan := sampleIntentPlanV2()
	plan.Candidates[0].Subject = "Update changes"
	return plan, nil
}

func (p *rewritingIntentV2Provider) RewriteIntentMessage(_ context.Context, req IntentMessageRewriteRequest) (Result, error) {
	p.rewriteCalls++
	p.locked = req.LockedPlan
	return Result{
		Subject: "Validate checkout behavior",
		Body:    "- Keep checkout validation focused",
		Source:  p.Name(),
	}, nil
}

func cannedIntentPlanV2ToolCall(plan IntentPlanV2) string {
	args, _ := json.Marshal(plan)
	response := map[string]any{
		"choices": []any{map[string]any{
			"message": map[string]any{
				"tool_calls": []any{map[string]any{
					"function": map[string]any{
						"name":      "capture_intent_plan_v2",
						"arguments": string(args),
					},
				}},
			},
		}},
	}
	out, _ := json.Marshal(response)
	return string(out)
}

func sampleIntentPlanV2Request(t *testing.T) IntentPlanRequestV2 {
	t.Helper()
	return mustIntentPlanRequestV2(t, []OfferedCapture{
		{Seq: 101, Path: "pkg/checkout.go", Op: "modify"},
		{Seq: 102, Path: "docs/checkout.md", Op: "modify"},
	}, nil)
}

func sampleIntentPlanV2() IntentPlanV2 {
	return IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates: []IntentCandidateAssignment{
			{
				CandidateID:    "checkout-code",
				SelectedSeqs:   []int64{101},
				Purpose:        "validate checkout behavior",
				Readiness:      IntentCandidateReady,
				Subject:        "Validate checkout behavior",
				Body:           "- Keep checkout validation focused",
				GroupingReason: "single implementation capture",
			},
			{
				CandidateID:    "checkout-docs",
				SelectedSeqs:   []int64{102},
				Purpose:        "document checkout behavior",
				Readiness:      IntentCandidateReady,
				Subject:        "Document checkout behavior",
				Body:           "- Explain the checkout contract",
				GroupingReason: "single documentation capture",
			},
		},
	}
}

func TestOpenAIPlanIntentV2UsesNativeBoundedTool(t *testing.T) {
	req := sampleIntentPlanV2Request(t)
	req.RetryCorrection = strings.Repeat("x", IntentAtomicityCorrectionCap+100)
	p, last, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedIntentPlanV2ToolCall(sampleIntentPlanV2())
	})

	plan, err := p.PlanIntentV2(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntentV2: %v", err)
	}
	if plan.ProtocolVersion != IntentPlannerProtocolV2 || len(plan.Candidates) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	var body struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(last.rawBody, &body); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if got := body.Tools[0].Function.Name; got != "capture_intent_plan_v2" {
		t.Fatalf("tool=%q", got)
	}
	if !strings.Contains(body.Messages[1].Content, "previous candidate plan failed") {
		t.Fatalf("correction prompt missing: %s", body.Messages[1].Content)
	}
	if strings.Count(body.Messages[1].Content, "x") > IntentAtomicityCorrectionCap+50 {
		t.Fatalf("correction prompt was not bounded")
	}
}

func TestOpenAIPlanIntentV2RedactsDiffAndTracesExactRequest(t *testing.T) {
	const secret = "sk-super-secret-provider-token"
	req, err := NewIntentPlanRequestV2(IntentPlanRequestV2Options{
		OfferedCaptures: []OfferedCapture{{
			Seq:          101,
			Path:         "pkg/checkout.go",
			Op:           "modify",
			CapturedDiff: "+api_key=" + secret + "\n",
		}},
		IncludeCapturedDiffs: true,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequestV2: %v", err)
	}
	plan := IntentPlanV2{
		ProtocolVersion: IntentPlannerProtocolV2,
		Candidates: []IntentCandidateAssignment{{
			CandidateID:    "checkout-code",
			SelectedSeqs:   []int64{101},
			Purpose:        "validate checkout behavior",
			Readiness:      IntentCandidateReady,
			Subject:        "Validate checkout behavior",
			Body:           "- Keep checkout validation focused",
			GroupingReason: "single implementation capture",
		}},
	}
	p, last, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedIntentPlanV2ToolCall(plan)
	})
	writer, records := newPromptTraceTestWriter(t)
	ctx := prompttrace.With(context.Background(), writer, prompttrace.Metadata{
		BranchRef:  "refs/heads/main",
		Generation: 4,
	})
	if _, err := p.PlanIntentV2(ctx, req); err != nil {
		t.Fatalf("PlanIntentV2: %v", err)
	}
	if strings.Contains(string(last.rawBody), secret) {
		t.Fatalf("request leaked raw secret: %s", last.rawBody)
	}
	record := promptTraceRecordByStage(t, records(), "request")
	if record.Strategy != "intent_v2" || string(record.Request) != string(last.rawBody) {
		t.Fatalf("trace strategy/request mismatch: strategy=%q", record.Strategy)
	}
	if !record.Transform.RedactionApplied || record.Transform.OutputBytes == 0 {
		t.Fatalf("transform=%+v", record.Transform)
	}
}

func TestComposedPlanIntentV2ReturnsAtomicityFailureWithoutFallback(t *testing.T) {
	req := sampleIntentPlanV2Request(t)
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		mega := IntentPlanV2{
			ProtocolVersion: IntentPlannerProtocolV2,
			Candidates: []IntentCandidateAssignment{{
				CandidateID:    "mega",
				SelectedSeqs:   []int64{101, 102},
				Purpose:        "combine all work",
				Readiness:      IntentCandidateReady,
				Subject:        "Combine checkout changes",
				GroupingReason: "captures happened together",
			}},
		}
		return 200, cannedIntentPlanV2ToolCall(mega)
	})
	composed := Compose(p, DeterministicProvider{})
	ctx, counter := WithIntentAttemptCounter(context.Background())
	_, err := composed.(IntentPlannerV2).PlanIntentV2(ctx, req)
	var typed *IntentPlanV2ValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("error=%T %v", err, err)
	}
	if counter.RetryCount() != 0 {
		t.Fatalf("provider layer performed a retry: %d", counter.RetryCount())
	}
}

func TestComposedPlanIntentV2ReturnsTransportFailure(t *testing.T) {
	req := sampleIntentPlanV2Request(t)
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 503, `{"error":{"message":"temporarily unavailable"}}`
	})
	composed := Compose(p, DeterministicProvider{})
	_, err := composed.(IntentPlannerV2).PlanIntentV2(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "http 503") {
		t.Fatalf("error=%v", err)
	}
}

func TestComposedPlanIntentV2LocksCandidateDuringMessageRewrite(t *testing.T) {
	req := sampleIntentPlanV2Request(t)
	primary := &rewritingIntentV2Provider{}
	composed := Compose(primary, DeterministicProvider{})
	plan, err := composed.(IntentPlannerV2).PlanIntentV2(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntentV2: %v", err)
	}
	if primary.rewriteCalls != 1 {
		t.Fatalf("rewrite calls=%d", primary.rewriteCalls)
	}
	if got := primary.locked.SelectedSeqs; len(got) != 1 || got[0] != 101 {
		t.Fatalf("locked selected seqs=%v", got)
	}
	if plan.Candidates[0].CandidateID != "checkout-code" ||
		plan.Candidates[0].GroupingReason != "single implementation capture" ||
		len(plan.Candidates[0].SelectedSeqs) != 1 ||
		plan.Candidates[0].SelectedSeqs[0] != 101 {
		t.Fatalf("rewrite changed grouping: %+v", plan.Candidates[0])
	}
	if plan.Candidates[0].Subject != "Validate checkout behavior" {
		t.Fatalf("subject=%q", plan.Candidates[0].Subject)
	}
}

func TestOpenAIPlanIntentV2RejectsMalformedResponseWithRedactedLog(t *testing.T) {
	req := sampleIntentPlanV2Request(t)
	dir := t.TempDir()
	writer := NewIntentRejectsWriter(dir, time.Now)
	ctx := WithIntentRejectsWriter(context.Background(), writer)
	t.Setenv("ACD_INTENT_REJECTS_RAW", "")
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, `{"choices":[{"message":{"tool_calls":[{"function":{"name":"capture_intent_plan_v2","arguments":"{\"protocol_version\":\"v2\",\"candidates\":[],\"secret\":\"do-not-store\"}"}}]}}]}`
	})
	_, err := p.PlanIntentV2(ctx, req)
	var typed *IntentPlanV2ValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("error=%T %v", err, err)
	}
	raw, readErr := os.ReadFile(writer.Path())
	if readErr != nil {
		t.Fatalf("read rejects: %v", readErr)
	}
	if strings.Contains(string(raw), "do-not-store") || !strings.Contains(string(raw), `"raw_response_redacted":true`) {
		t.Fatalf("reject log redaction failed: %s", raw)
	}
}

func TestOpenAIPlanIntentV2ConcurrentRejectWritersStayIsolated(t *testing.T) {
	t.Setenv("ACD_INTENT_REJECTS_RAW", "")
	req := sampleIntentPlanV2Request(t)
	type plannerRun struct {
		name   string
		writer *IntentRejectsWriter
		ctx    context.Context
		plan   IntentPlannerV2
		hash   string
	}
	runs := make([]plannerRun, 0, 2)
	for _, name := range []string{"main", "linked"} {
		writer := NewIntentRejectsWriter(filepath.Join(t.TempDir(), name, "acd"), time.Now)
		raw := fmt.Sprintf(`{"choices":[{"message":{"tool_calls":[{"function":{"name":"capture_intent_plan_v2","arguments":"{\"protocol_version\":\"v2\",\"candidates\":[],\"run\":\"%s\"}"}}]}}]}`, name)
		provider, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
			return 200, raw
		})
		sum := sha256.Sum256([]byte(raw))
		runs = append(runs, plannerRun{
			name:   name,
			writer: writer,
			ctx:    WithIntentRejectsWriter(context.Background(), writer),
			plan:   provider,
			hash:   fmt.Sprintf("%x", sum),
		})
	}

	var wg sync.WaitGroup
	for _, current := range runs {
		current := current
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := current.plan.PlanIntentV2(current.ctx, req); err == nil {
					t.Errorf("%s planner call %d unexpectedly succeeded", current.name, i)
				}
			}
		}()
	}
	wg.Wait()

	for _, current := range runs {
		body, err := os.ReadFile(current.writer.Path())
		if err != nil {
			t.Fatalf("read %s rejects: %v", current.name, err)
		}
		if got := strings.Count(string(body), "\n"); got != 20 {
			t.Fatalf("%s rows=%d want 20", current.name, got)
		}
		if !strings.Contains(string(body), current.hash) {
			t.Fatalf("%s log missing own response hash %s", current.name, current.hash)
		}
		otherHash := map[string]string{"main": runs[1].hash, "linked": runs[0].hash}[current.name]
		if strings.Contains(string(body), otherHash) {
			t.Fatalf("%s log contains cross-directory response hash %s", current.name, otherHash)
		}
	}
}

func TestSubprocessPlanIntentV2NegotiatesNativeEnvelope(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "native-v2", `
probes=0
while IFS= read -r line; do
  case "$line" in
    *'"version":2'*'"request_version":2'*'"request_type":"intent_plan_v2"'*'"planner_protocol":"v2"'*'"planner_request_v2"'*)
      printf '%s\n' '{"version":2,"planner_protocol":"v2","intent_plan_v2":{"protocol_version":"v2","candidates":[{"candidate_id":"checkout-code","selected_seqs":[101],"purpose":"validate checkout behavior","readiness":"ready","missing_companions":[],"depends_on_candidates":[],"subject":"Validate checkout behavior","body":"","grouping_reason":"single implementation capture"},{"candidate_id":"checkout-docs","selected_seqs":[102],"purpose":"document checkout behavior","readiness":"ready","missing_companions":[],"depends_on_candidates":[],"subject":"Document checkout behavior","body":"","grouping_reason":"single documentation capture"}]}}'
      ;;
    *'"version":1'*'"request_type":"intent_plan"'*'"planner_request"'*)
      probes=$((probes + 1))
      if [ "$probes" -ne 1 ]; then
        printf '%s\n' '{"version":1,"error":"capability probe was not cached"}'
      else
        printf '%s\n' '{"version":1,"capabilities":["intent_plan_v2"],"selected_seqs":[101],"deferred_seqs":[102],"subject":"Validate checkout behavior","body":"","grouping_reason":"capability negotiation response","deferred_reasons":[{"seq":102,"reason":"documentation remains separate"}]}'
      fi
      ;;
    *)
      printf '%s\n' '{"version":1,"error":"missing v2 compatibility envelope"}'
      ;;
  esac
done
`)
	p := NewSubprocessProvider("native-v2", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-native-v2", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	plan, err := PlanIntentV2WithCompatibility(context.Background(), p, sampleIntentPlanV2Request(t))
	if err != nil {
		t.Fatalf("PlanIntentV2WithCompatibility: %v", err)
	}
	if plan.ProtocolVersion != IntentPlannerProtocolV2 || len(plan.Candidates) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	if _, err := PlanIntentV2WithCompatibility(context.Background(), p, sampleIntentPlanV2Request(t)); err != nil {
		t.Fatalf("second PlanIntentV2WithCompatibility: %v", err)
	}
}

func TestSubprocessPlanIntentV2AdaptsLegacyResponseOnce(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "legacy-v1", `
while IFS= read -r line; do
  case "$line" in
    *'"request_version"'*|*'"planner_protocol"'*|*'"planner_request_v2"'*)
      printf '%s\n' '{"version":1,"error":"legacy plugin received v2-only fields"}'
      ;;
    *'"request_type":"intent_plan"'*'"planner_request"'*)
      printf '%s\n' '{"version":1,"selected_seqs":[101],"deferred_seqs":[102],"subject":"Validate checkout behavior","body":"","grouping_reason":"single implementation capture","deferred_reasons":[{"seq":102,"reason":"documentation remains separate"}]}'
      ;;
    *)
      printf '%s\n' '{"version":1,"error":"missing legacy planner request"}'
      ;;
  esac
done
`)
	p := NewSubprocessProvider("legacy-v1", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-legacy-v1", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	plan, err := PlanIntentV2WithCompatibility(context.Background(), p, sampleIntentPlanV2Request(t))
	if err != nil {
		t.Fatalf("PlanIntentV2WithCompatibility: %v", err)
	}
	if plan.ProtocolVersion != IntentPlannerProtocolV1Compat {
		t.Fatalf("protocol=%q", plan.ProtocolVersion)
	}
	if len(plan.Candidates) != 2 || plan.Candidates[1].Readiness != IntentCandidateWait {
		t.Fatalf("candidates=%+v", plan.Candidates)
	}
}

func TestSubprocessPlanIntentV2RejectsMalformedNativeResponse(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "bad-v2", `
while IFS= read -r line; do
  case "$line" in
    *'"request_type":"intent_plan_v2"'*)
      printf '%s\n' '{"version":2,"planner_protocol":"v2","intent_plan_v2":{"protocol_version":"v2","candidates":[],"unexpected":"field"}}'
      ;;
    *)
      printf '%s\n' '{"version":1,"capabilities":["intent_plan_v2"],"selected_seqs":[101],"deferred_seqs":[102],"subject":"Validate checkout behavior","body":"","grouping_reason":"capability negotiation response","deferred_reasons":[{"seq":102,"reason":"documentation remains separate"}]}'
      ;;
  esac
done
`)
	p := NewSubprocessProvider("bad-v2", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-bad-v2", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	_, err := p.PlanIntentV2(context.Background(), sampleIntentPlanV2Request(t))
	var typed *IntentPlanV2ValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("error=%T %v", err, err)
	}
}

func TestSubprocessPlanIntentV2RejectsInvalidNativeEnvelopeVersion(t *testing.T) {
	skipIfWindows(t)
	for _, version := range []int{0, 1, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			dir := t.TempDir()
			nativeResponse, err := json.Marshal(map[string]any{
				"version":          version,
				"planner_protocol": IntentPlannerProtocolV2,
				"intent_plan_v2":   sampleIntentPlanV2(),
			})
			if err != nil {
				t.Fatalf("marshal response: %v", err)
			}
			script := `
while IFS= read -r line; do
  case "$line" in
    *'"request_type":"intent_plan_v2"'*)
      printf '%s\n' '` + string(nativeResponse) + `'
      ;;
    *)
      printf '%s\n' '{"version":1,"capabilities":["intent_plan_v2"],"selected_seqs":[101],"deferred_seqs":[102],"subject":"Validate checkout behavior","body":"","grouping_reason":"capability negotiation response","deferred_reasons":[{"seq":102,"reason":"documentation remains separate"}]}'
      ;;
  esac
done
`
			bin := writePluginScript(t, dir, "bad-version", script)
			p := NewSubprocessProvider("bad-version", SubprocessOptions{
				LookPath: fixedLookPath("acd-provider-bad-version", bin),
				Timeout:  5 * time.Second,
				Stderr:   io.Discard,
			})
			t.Cleanup(func() { _ = p.Close() })

			_, err = p.PlanIntentV2(context.Background(), sampleIntentPlanV2Request(t))
			var typed *IntentPlanV2ValidationError
			if !errors.As(err, &typed) {
				t.Fatalf("error=%T %v", err, err)
			}
			if len(typed.Findings) != 1 || typed.Findings[0].Code != "response_version_invalid" {
				t.Fatalf("findings=%+v", typed.Findings)
			}
		})
	}
}

func TestSubprocessPlanIntentV2CancellationStopsSession(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "slow-v2", `
while IFS= read -r line; do
  sleep 10
done
`)
	p := NewSubprocessProvider("slow-v2", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-slow-v2", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := p.PlanIntentV2(ctx, sampleIntentPlanV2Request(t))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%T %v", err, err)
	}
	start := time.Now()
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Close after cancellation took %v", elapsed)
	}
}

func TestReadLineEnforcesSubprocessResponseByteCap(t *testing.T) {
	t.Run("at cap", func(t *testing.T) {
		line := strings.Repeat("a", subprocessResponseByteCap) + "\n"
		got, err := readLine(bufio.NewReader(strings.NewReader(line)), subprocessResponseByteCap)
		if err != nil {
			t.Fatalf("readLine: %v", err)
		}
		if len(got) != subprocessResponseByteCap {
			t.Fatalf("bytes=%d", len(got))
		}
	})
	t.Run("above cap", func(t *testing.T) {
		line := strings.Repeat("a", subprocessResponseByteCap+1) + "\n"
		got, err := readLine(bufio.NewReader(strings.NewReader(line)), subprocessResponseByteCap)
		var tooLarge *subprocessResponseTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("error=%T %v", err, err)
		}
		if len(got) != subprocessResponseByteCap {
			t.Fatalf("retained bytes=%d", len(got))
		}
	})
}

func TestSubprocessPlanIntentV2MalformedEnvelopeUsesRedactedRejectLog(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	writer := NewIntentRejectsWriter(filepath.Join(dir, "rejects"), time.Now)
	ctx := WithIntentRejectsWriter(context.Background(), writer)
	t.Setenv("ACD_INTENT_REJECTS_RAW", "")
	bin := writePluginScript(t, dir, "malformed-envelope", `
while IFS= read -r line; do
  case "$line" in
    *'"request_type":"intent_plan_v2"'*)
      printf '%s\n' '{"version":2,"planner_protocol":"v2","leak":"do-not-store"'
      ;;
    *)
      printf '%s\n' '{"version":1,"capabilities":["intent_plan_v2"],"selected_seqs":[101],"deferred_seqs":[102],"subject":"Validate checkout behavior","body":"","grouping_reason":"capability negotiation response","deferred_reasons":[{"seq":102,"reason":"documentation remains separate"}]}'
      ;;
  esac
done
`)
	p := NewSubprocessProvider("malformed-envelope", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-malformed-envelope", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	_, err := p.PlanIntentV2(ctx, sampleIntentPlanV2Request(t))
	var typed *IntentPlanV2ValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("error=%T %v", err, err)
	}
	raw, readErr := os.ReadFile(writer.Path())
	if readErr != nil {
		t.Fatalf("read rejects: %v", readErr)
	}
	text := string(raw)
	if strings.Contains(text, "do-not-store") ||
		!strings.Contains(text, `"raw_response_redacted":true`) ||
		!strings.Contains(text, `"raw_response_sha256"`) {
		t.Fatalf("reject log=%s", text)
	}
}

func TestSubprocessPlanIntentV2RejectsOversizeResponseAndCloses(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "oversize-v2", `
while IFS= read -r line; do
  case "$line" in
    *'"request_type":"intent_plan_v2"'*)
      printf '%s' '{"version":2,"planner_protocol":"v2","padding":"'
      head -c 1048577 /dev/zero | tr '\000' x
      printf '%s\n' '"}'
      ;;
    *)
      printf '%s\n' '{"version":1,"capabilities":["intent_plan_v2"],"selected_seqs":[101],"deferred_seqs":[102],"subject":"Validate checkout behavior","body":"","grouping_reason":"capability negotiation response","deferred_reasons":[{"seq":102,"reason":"documentation remains separate"}]}'
      ;;
  esac
done
`)
	p := NewSubprocessProvider("oversize-v2", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-oversize-v2", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	_, err := p.PlanIntentV2(context.Background(), sampleIntentPlanV2Request(t))
	var typed *IntentPlanV2ValidationError
	if !errors.As(err, &typed) || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error=%T %v", err, err)
	}
	start := time.Now()
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Close after oversize response took %v", elapsed)
	}
}

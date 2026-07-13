package ai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

// helper: spin up a mock OpenAI server. handler receives the parsed
// request body and returns (status, response). Captures the last request
// for inspection.
type capturedReq struct {
	method  string
	path    string
	auth    string
	rawBody []byte
}

func newOpenAIMock(t *testing.T, handler func(req capturedReq) (int, string)) (*OpenAIProvider, *capturedReq, *httptest.Server) {
	t.Helper()
	last := &capturedReq{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		last.method = r.Method
		last.path = r.URL.Path
		last.auth = r.Header.Get("Authorization")
		last.rawBody = body
		status, resp := handler(*last)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, resp)
	}))
	t.Cleanup(srv.Close)
	p := &OpenAIProvider{
		BaseURL: srv.URL,
		APIKey:  "test-key",
		Model:   "test-model",
		HTTP:    srv.Client(),
	}
	return p, last, srv
}

// canned tool-call response shaped like the OpenAI v1 API.
func cannedToolCall(subject, body string) string {
	args, _ := json.Marshal(map[string]string{"subject": subject, "body": body})
	resp := map[string]any{
		"choices": []any{
			map[string]any{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []any{
						map[string]any{
							"id":   "call_1",
							"type": "function",
							"function": map[string]any{
								"name":      "commit_message",
								"arguments": string(args),
							},
						},
					},
				},
			},
		},
	}
	out, _ := json.Marshal(resp)
	return string(out)
}

func cannedIntentPlanToolCall(plan IntentPlan) string {
	args, _ := json.Marshal(plan)
	resp := map[string]any{
		"choices": []any{
			map[string]any{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []any{
						map[string]any{
							"id":   "call_1",
							"type": "function",
							"function": map[string]any{
								"name":      "capture_intent_plan",
								"arguments": string(args),
							},
						},
					},
				},
			},
		},
	}
	out, _ := json.Marshal(resp)
	return string(out)
}

func TestOpenAI_HappyPath(t *testing.T) {
	p, last, _ := newOpenAIMock(t, func(req capturedReq) (int, string) {
		return 200, cannedToolCall("Update token expiry", "- refresh tokens now last 7 days")
	})
	r, err := p.Generate(context.Background(), CommitContext{
		Op: "modify", Path: "src/auth.go",
		DiffText: "diff --git a/src/auth.go b/src/auth.go\n@@\n-old\n+new\n",
		Branch:   "refs/heads/main",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if r.Subject != "Update token expiry" {
		t.Fatalf("subject=%q", r.Subject)
	}
	if !strings.HasPrefix(r.Body, "- ") {
		t.Fatalf("body=%q", r.Body)
	}
	if r.Source != "openai-compat" {
		t.Fatalf("source=%q", r.Source)
	}
	if last.method != http.MethodPost {
		t.Fatalf("method=%s", last.method)
	}
	if last.path != "/chat/completions" {
		t.Fatalf("path=%s", last.path)
	}
	if last.auth != "Bearer test-key" {
		t.Fatalf("auth=%q", last.auth)
	}
}

func TestOpenAI_ConventionalPromptAndSchema(t *testing.T) {
	p, last, _ := newOpenAIMock(t, func(req capturedReq) (int, string) {
		return 200, cannedToolCall("fix: validate AI messages", "")
	})
	p.Format = CommitFormatConventional
	if _, err := p.Generate(context.Background(), CommitContext{Op: "modify", Path: "internal/ai/openai_compat.go"}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	var sent struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Parameters struct {
					Properties map[string]struct {
						Description string `json:"description"`
					} `json:"properties"`
				} `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(last.rawBody, &sent); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if !strings.Contains(sent.Messages[0].Content, "<type>: <description>") {
		t.Fatalf("system prompt missing conventional format: %s", sent.Messages[0].Content)
	}
	if got := sent.Tools[0].Function.Parameters.Properties["subject"].Description; !strings.Contains(got, "Conventional Commit") {
		t.Fatalf("subject schema description=%q", got)
	}
}

func TestOpenAI_ConventionalWrongFormatFallsBack(t *testing.T) {
	p, _, _ := newOpenAIMock(t, func(req capturedReq) (int, string) {
		return 200, cannedToolCall("Update openai_compat.go", "")
	})
	p.Format = CommitFormatConventional
	prov := Compose(p, DeterministicProvider{CommitFormat: CommitFormatConventional})
	got, err := prov.Generate(context.Background(), CommitContext{Op: "modify", Path: "internal/ai/openai_compat.go"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if got.Source != "deterministic" {
		t.Fatalf("Source=%q want deterministic", got.Source)
	}
	if got.Subject != "chore: update openai_compat.go" {
		t.Fatalf("Subject=%q", got.Subject)
	}
}

func TestOpenAI_PromptTraceDisabledWritesNothing(t *testing.T) {
	t.Setenv(prompttrace.EnvTrace, "")
	gitDir := t.TempDir()
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedToolCall("Update auth", "")
	})
	ctx := prompttrace.With(context.Background(), prompttrace.FromEnv("/repo", gitDir), prompttrace.Metadata{
		Strategy:     string(CommitStrategyEvent),
		Seq:          7,
		BranchRef:    "refs/heads/main",
		Generation:   3,
		DiffIncluded: true,
		DiffCap:      DiffCap,
	})
	if _, err := p.Generate(ctx, CommitContext{
		Op:       "modify",
		Path:     "auth.go",
		DiffText: "diff --git a/auth.go b/auth.go\n@@\n-password=secret\n+password=secret2\n",
		Branch:   "refs/heads/main",
	}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, err := os.Stat(filepath.Join(gitDir, "acd", "prompt-trace")); !os.IsNotExist(err) {
		t.Fatalf("prompt trace dir exists or stat failed unexpectedly: %v", err)
	}
}

func TestOpenAI_PromptTraceRecordsExactEventRequest(t *testing.T) {
	p, last, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedToolCall("Update auth", "- rotate token validation")
	})
	writer, records := newPromptTraceTestWriter(t)
	ctx := prompttrace.With(context.Background(), writer, prompttrace.Metadata{
		Strategy:     string(CommitStrategyEvent),
		Seq:          42,
		BranchRef:    "refs/heads/main",
		Generation:   9,
		DiffIncluded: true,
		DiffCap:      DiffCap,
	})
	if _, err := p.Generate(ctx, CommitContext{
		Op:       "modify",
		Path:     "auth.go",
		DiffText: strings.Repeat("token=secret\n", 500),
		Branch:   "refs/heads/main",
	}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	got := promptTraceRecordByStage(t, records(), "request")
	if got.Strategy != string(CommitStrategyEvent) || got.Provider != "openai-compat" || got.Model != "test-model" {
		t.Fatalf("metadata strategy/provider/model = %q/%q/%q", got.Strategy, got.Provider, got.Model)
	}
	if got.Seq != 42 || got.BranchRef != "refs/heads/main" || got.Generation != 9 {
		t.Fatalf("event metadata = seq %d branch %q generation %d", got.Seq, got.BranchRef, got.Generation)
	}
	if string(got.Request) != string(last.rawBody) {
		t.Fatalf("trace request differs from sent body\ntrace=%s\nsent=%s", got.Request, last.rawBody)
	}
	if !strings.Contains(got.SystemMessage, "git commit message generator") {
		t.Fatalf("system message not recorded: %q", got.SystemMessage)
	}
	for _, want := range []string{
		"max 50 characters",
		"Line 3+: bullet list for why/context",
		"Do not mention filenames in line 1",
	} {
		if !strings.Contains(got.SystemMessage, want) {
			t.Fatalf("system message missing %q: %q", want, got.SystemMessage)
		}
	}
	if !strings.Contains(got.UserMessage, "Generate a commit message") {
		t.Fatalf("user message not recorded: %q", got.UserMessage)
	}
	if got.Transform.InputBytes == 0 || got.Transform.OutputBytes == 0 {
		t.Fatalf("transform metadata missing: %+v", got.Transform)
	}
}

// 5xx -> error so Compose can fall back.
func TestOpenAI_5xxErrors(t *testing.T) {
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 500, `{"error":{"message":"internal"}}`
	})
	_, err := p.Generate(context.Background(), CommitContext{Op: "modify", Path: "x"})
	if err == nil {
		t.Fatalf("expected error on 5xx")
	}
}

// Bad JSON in body -> error.
func TestOpenAI_BadJSON(t *testing.T) {
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, "{not-json"
	})
	_, err := p.Generate(context.Background(), CommitContext{Op: "modify", Path: "x"})
	if err == nil {
		t.Fatalf("expected error on bad json")
	}
}

// No tool call in the response -> error.
func TestOpenAI_NoToolCall(t *testing.T) {
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, `{"choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}`
	})
	_, err := p.Generate(context.Background(), CommitContext{Op: "modify", Path: "x"})
	if err == nil {
		t.Fatalf("expected error when no tool call")
	}
}

func TestOpenAI_RejectsWrongCommitMessageToolName(t *testing.T) {
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, `{"choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"not_commit_message","arguments":"{\"subject\":\"Update x\"}"}}]}}]}`
	})
	_, err := p.Generate(context.Background(), CommitContext{Op: "modify", Path: "x"})
	if err == nil || !strings.Contains(err.Error(), "unexpected tool") {
		t.Fatalf("Generate error = %v, want unexpected-tool refusal", err)
	}
}

// Compose(openai, deterministic): on openai 5xx the deterministic
// fallback fires and Source reflects "deterministic".
func TestOpenAI_ComposeFallback(t *testing.T) {
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 503, `{"error":{"message":"unavailable"}}`
	})
	prov := Compose(p, DeterministicProvider{})
	r, err := prov.Generate(context.Background(), CommitContext{Op: "modify", Path: "src/foo.go"})
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if r.Subject != "Update foo.go" {
		t.Fatalf("subject=%q (expected deterministic fallback)", r.Subject)
	}
	if r.Source != "deterministic" {
		t.Fatalf("source=%q want deterministic", r.Source)
	}
}

// Compose(openai, deterministic): on openai success Source reflects "openai-compat".
func TestOpenAI_ComposePrimaryWins(t *testing.T) {
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedToolCall("Refactor pipeline", "")
	})
	prov := Compose(p, DeterministicProvider{})
	r, err := prov.Generate(context.Background(), CommitContext{Op: "modify", Path: "src/foo.go"})
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if r.Subject != "Refactor pipeline" {
		t.Fatalf("subject=%q", r.Subject)
	}
	if r.Source != "openai-compat" {
		t.Fatalf("source=%q want openai-compat", r.Source)
	}
}

// Diff > DiffCap is truncated before being sent. Inspect the captured
// payload to confirm.
func TestOpenAI_TruncatesDiff(t *testing.T) {
	p, last, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedToolCall("Update foo", "")
	})
	huge := "diff --git a/x b/x\nindex 1..2\n--- a/x\n+++ b/x\n@@ -1 +1 @@\n" +
		strings.Repeat("+lots of stuff\n", 1000) // way over 4000 chars
	_, err := p.Generate(context.Background(), CommitContext{
		Op: "modify", Path: "x", DiffText: huge,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	// Decode the user message; assert the diff field is at most DiffCap.
	var sent struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(last.rawBody, &sent); err != nil {
		t.Fatalf("decode sent: %v", err)
	}
	var userContent string
	for _, m := range sent.Messages {
		if m.Role == "user" {
			userContent = m.Content
		}
	}
	if userContent == "" {
		t.Fatalf("no user message captured")
	}
	// Pull the JSON tail out of the user content.
	jsonStart := strings.Index(userContent, "{")
	if jsonStart < 0 {
		t.Fatalf("user content missing JSON: %q", userContent)
	}
	var inner struct {
		Diff string `json:"diff"`
	}
	if err := json.Unmarshal([]byte(userContent[jsonStart:]), &inner); err != nil {
		t.Fatalf("decode user payload: %v", err)
	}
	if len(inner.Diff) > DiffCap+64 { // small cushion for sentinel
		t.Fatalf("diff len=%d not capped near %d", len(inner.Diff), DiffCap)
	}
	if !strings.Contains(inner.Diff, "<truncated>") {
		t.Fatalf("diff did not include truncation sentinel: %q", inner.Diff[:120])
	}
}

func TestOpenAI_RedactsDiffBeforeSend(t *testing.T) {
	p, last, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedToolCall("Update secrets", "")
	})
	diff := "diff --git a/config/prod.yaml b/config/prod.yaml\n@@\n" +
		"+aws_access_key_id: AKIAIOSFODNN7EXAMPLE\n" +
		"+authorization: Bearer abcdefghij.klmnopqrst.uvwxyz123456\n"
	_, err := p.Generate(context.Background(), CommitContext{
		Op: "modify", Path: "config/prod.yaml", DiffText: diff,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	var sent struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(last.rawBody, &sent); err != nil {
		t.Fatalf("decode sent: %v", err)
	}
	var userContent string
	for _, m := range sent.Messages {
		if m.Role == "user" {
			userContent = m.Content
		}
	}
	jsonStart := strings.Index(userContent, "{")
	if jsonStart < 0 {
		t.Fatalf("user content missing JSON: %q", userContent)
	}
	var inner struct {
		Diff string `json:"diff"`
	}
	if err := json.Unmarshal([]byte(userContent[jsonStart:]), &inner); err != nil {
		t.Fatalf("decode user payload: %v", err)
	}
	if strings.Contains(inner.Diff, "AKIAIOSFODNN7EXAMPLE") ||
		strings.Contains(inner.Diff, "Bearer abcdefghij.klmnopqrst.uvwxyz123456") {
		t.Fatalf("diff leaked secret:\n%s", inner.Diff)
	}
	if !strings.Contains(inner.Diff, redactedSecret) {
		t.Fatalf("diff missing redaction marker:\n%s", inner.Diff)
	}
}

// Sanitization: tool-call subject with control chars / trailing period /
// long length is cleaned and capped.
func TestOpenAI_SanitizesResponse(t *testing.T) {
	noisy := "Update auth\x00 token expiry mechanism with new caching layer for sessions and bearer tokens."
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedToolCall(noisy, "- detail one")
	})
	r, err := p.Generate(context.Background(), CommitContext{Op: "modify", Path: "x"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if strings.ContainsAny(r.Subject, "\x00\x07") {
		t.Fatalf("subject still has control chars: %q", r.Subject)
	}
	if strings.HasSuffix(r.Subject, ".") {
		t.Fatalf("subject still has trailing period: %q", r.Subject)
	}
	if len([]rune(r.Subject)) > SubjectCap {
		t.Fatalf("subject not capped: len=%d %q", len([]rune(r.Subject)), r.Subject)
	}
}

// Missing API key surfaces as an error before the request fires.
func TestOpenAI_MissingKey(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer srv.Close()
	p := &OpenAIProvider{BaseURL: srv.URL, APIKey: "", HTTP: srv.Client()}
	_, err := p.Generate(context.Background(), CommitContext{Op: "modify", Path: "x"})
	if err == nil {
		t.Fatalf("expected missing-key error")
	}
	if calls.Load() != 0 {
		t.Fatalf("server was contacted with missing key: %d calls", calls.Load())
	}
}

// Compose with nil primary degenerates to fallback alone.
func TestCompose_NilPrimary(t *testing.T) {
	prov := Compose(nil, DeterministicProvider{})
	r, err := prov.Generate(context.Background(), CommitContext{Op: "modify", Path: "src/foo.go"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if r.Subject != "Update foo.go" {
		t.Fatalf("subject=%q", r.Subject)
	}
	if r.Source != "deterministic" {
		t.Fatalf("source=%q", r.Source)
	}
}

// Compose with nil fallback panics — programming error.
func TestCompose_NilFallbackPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic")
		}
	}()
	Compose(DeterministicProvider{}, nil)
}

// TestOpenAI_ForwardsCommitContext: the daemon-side wiring populates
// DiffText (built from captured blobs), RepoRoot, Branch, and MultiOp on
// CommitContext before calling Generate. This test asserts each of
// those fields lands in the JSON payload the mock OpenAI server sees,
// using a hand-rolled diff text that resembles what BuildOpsDiff
// produces from before/after blob OIDs.
func TestOpenAI_ForwardsCommitContext(t *testing.T) {
	p, last, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedToolCall("Update files", "")
	})

	// Diff shaped like what daemon.BuildOpsDiff emits for a multi-op event:
	// per-op `diff --git a/<path> b/<path>` headers, anchored hunks, etc.
	capturedDiff := strings.Join([]string{
		"diff --git a/src/a.go b/src/a.go",
		"--- a/src/a.go",
		"+++ b/src/a.go",
		"@@ -1 +1 @@",
		"-old A",
		"+new A",
		"diff --git a/src/b.go b/src/b.go",
		"new file mode 100644",
		"--- a/src/b.go",
		"+++ b/src/b.go",
		"@@ -0,0 +1 @@",
		"+fresh B",
		"",
	}, "\n")

	cc := CommitContext{
		Branch:   "refs/heads/main",
		RepoRoot: "/tmp/some-repo",
		DiffText: capturedDiff,
		MultiOp: []OpItem{
			{Path: "src/a.go", Op: "modify"},
			{Path: "src/b.go", Op: "create"},
		},
	}
	if _, err := p.Generate(context.Background(), cc); err != nil {
		t.Fatalf("Generate: %v", err)
	}

	var sent struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(last.rawBody, &sent); err != nil {
		t.Fatalf("decode sent: %v", err)
	}
	var userContent string
	for _, m := range sent.Messages {
		if m.Role == "user" {
			userContent = m.Content
		}
	}
	if userContent == "" {
		t.Fatalf("no user message captured")
	}
	jsonStart := strings.Index(userContent, "{")
	if jsonStart < 0 {
		t.Fatalf("user content missing JSON: %q", userContent)
	}
	var inner struct {
		Branch   string `json:"branch"`
		RepoRoot string `json:"repo_root"`
		Diff     string `json:"diff"`
		MultiOp  []struct {
			Path string `json:"path"`
			Op   string `json:"op"`
		} `json:"multi_op"`
	}
	if err := json.Unmarshal([]byte(userContent[jsonStart:]), &inner); err != nil {
		t.Fatalf("decode user payload: %v", err)
	}
	if inner.Branch != "refs/heads/main" {
		t.Fatalf("branch=%q", inner.Branch)
	}
	if inner.RepoRoot != "/tmp/some-repo" {
		t.Fatalf("repo_root=%q", inner.RepoRoot)
	}
	if !strings.Contains(inner.Diff, "diff --git a/src/a.go b/src/a.go") ||
		!strings.Contains(inner.Diff, "+new A") {
		t.Fatalf("diff missing first op section:\n%s", inner.Diff)
	}
	if !strings.Contains(inner.Diff, "diff --git a/src/b.go b/src/b.go") ||
		!strings.Contains(inner.Diff, "+fresh B") {
		t.Fatalf("diff missing second op section:\n%s", inner.Diff)
	}
	if len(inner.MultiOp) != 2 ||
		inner.MultiOp[0].Path != "src/a.go" ||
		inner.MultiOp[1].Path != "src/b.go" {
		t.Fatalf("multi_op=%+v", inner.MultiOp)
	}
}

func TestOpenAIIntentPlan_HappyPath(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	p, last, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedIntentPlanToolCall(IntentPlan{
			SelectedSeqs:   []int64{101},
			DeferredSeqs:   []int64{102},
			Subject:        "Update checkout flow.",
			Body:           "- apply checkout validation",
			GroupingReason: " \tsingle\x00 focused\ncheckout change\x7f ",
			DeferredReasons: []DeferredReason{{
				Seq:    102,
				Reason: " \rdocumentation\x1b should commit separately\t ",
			}},
		})
	})
	plan, err := p.PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if plan.Subject != "Update checkout flow" {
		t.Fatalf("subject=%q", plan.Subject)
	}
	if plan.Source != "openai-compat" {
		t.Fatalf("source=%q", plan.Source)
	}
	if plan.GroupingReason != "single focusedcheckout change" {
		t.Fatalf("grouping reason=%q", plan.GroupingReason)
	}
	if len(plan.DeferredReasons) != 1 || plan.DeferredReasons[0].Reason != "documentation should commit separately" {
		t.Fatalf("deferred reasons=%+v", plan.DeferredReasons)
	}

	var sent struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Name       string `json:"name"`
				Parameters struct {
					Required   []string `json:"required"`
					Properties map[string]struct {
						Description string `json:"description"`
					} `json:"properties"`
				} `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
		ToolChoice struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tool_choice"`
	}
	if err := json.Unmarshal(last.rawBody, &sent); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if len(sent.Tools) != 1 || sent.Tools[0].Function.Name != "capture_intent_plan" {
		t.Fatalf("tool=%+v", sent.Tools)
	}
	if sent.ToolChoice.Function.Name != "capture_intent_plan" {
		t.Fatalf("tool_choice=%+v", sent.ToolChoice)
	}
	required := strings.Join(sent.Tools[0].Function.Parameters.Required, ",")
	for _, field := range []string{"selected_seqs", "deferred_seqs", "subject", "body", "grouping_reason", "deferred_reasons"} {
		if !strings.Contains(required, field) {
			t.Fatalf("schema required fields %q missing %q", required, field)
		}
	}
	props := sent.Tools[0].Function.Parameters.Properties
	for field, want := range map[string]string{
		"subject":         "<= 50 chars",
		"body":            "Do not explain why selected captures fit together",
		"grouping_reason": "not part of the git commit message",
	} {
		if !strings.Contains(props[field].Description, want) {
			t.Fatalf("%s description missing %q: %q", field, want, props[field].Description)
		}
	}
	var systemContent string
	var userContent string
	for _, m := range sent.Messages {
		if m.Role == "system" {
			systemContent = m.Content
		}
		if m.Role == "user" {
			userContent = m.Content
		}
	}
	for _, want := range []string{
		"max 50 characters",
		"Keep grouping rationale in grouping_reason, not in body",
		"never write prose explaining why the selected captures fit together",
	} {
		if !strings.Contains(systemContent, want) {
			t.Fatalf("intent system message missing %q: %q", want, systemContent)
		}
	}
	if !strings.Contains(userContent, `"latest_commit"`) ||
		!strings.Contains(userContent, `"path_commit_context"`) ||
		!strings.Contains(userContent, `"offered_captures"`) {
		t.Fatalf("planner request missing context: %s", userContent)
	}
}

func TestOpenAIIntentPlan_ResponseShapeErrorsAreTypedValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response string
	}{
		{name: "malformed response", response: `{"choices":`},
		{name: "missing choices", response: `{"choices":[]}`},
		{name: "missing tool call", response: `{"choices":[{"message":{"content":"no tool"}}]}`},
		{name: "malformed arguments", response: `{"choices":[{"message":{"tool_calls":[{"function":{"name":"capture_intent_plan","arguments":"{"}}]}}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
				return 200, tc.response
			})
			_, err := p.PlanIntent(context.Background(), sampleIntentPlanRequest(t))
			var validationErr *IntentPlanValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("error=%T %v want *IntentPlanValidationError", err, err)
			}
			if validationErr.Code != IntentPlanValidationShape {
				t.Fatalf("validation code=%v want shape", validationErr.Code)
			}
		})
	}
}

func TestOpenAIIntentMessageRewrite_HappyPath(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	plan := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Update parsed",
		GroupingReason: "checkout service change is focused",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: "docs change is independent",
		}},
	}
	rewriteReq := NewIntentMessageRewriteRequest(req, plan, EvaluateIntentPlanMessageQuality(req, plan))
	p, last, _ := newOpenAIMock(t, func(req capturedReq) (int, string) {
		return 200, cannedToolCall("Tighten checkout validation", "")
	})

	got, err := p.RewriteIntentMessage(context.Background(), rewriteReq)
	if err != nil {
		t.Fatalf("RewriteIntentMessage: %v", err)
	}
	if got.Subject != "Tighten checkout validation" || got.Body != "" {
		t.Fatalf("rewrite result=%+v", got)
	}

	var sent struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
		ToolChoice struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tool_choice"`
	}
	if err := json.Unmarshal(last.rawBody, &sent); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if len(sent.Tools) != 1 || sent.Tools[0].Function.Name != "commit_message" {
		t.Fatalf("tools=%+v", sent.Tools)
	}
	if sent.ToolChoice.Function.Name != "commit_message" {
		t.Fatalf("tool_choice=%+v", sent.ToolChoice)
	}
	if len(sent.Messages) != 2 || !strings.Contains(sent.Messages[1].Content, "Do not change selected_seqs") {
		t.Fatalf("rewrite prompt missing lock instruction: %+v", sent.Messages)
	}
	if strings.Contains(string(last.rawBody), "capture_intent_plan") {
		t.Fatalf("rewrite request must not expose capture_intent_plan tool: %s", string(last.rawBody))
	}
}

func TestOpenAIIntentMessageRewrite_ValidationErrorsAreTyped(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response string
	}{
		{name: "malformed response", response: "{"},
		{name: "empty subject after sanitize", response: cannedToolCall("\x01", "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := sampleIntentPlanRequest(t)
			plan := IntentPlan{
				SelectedSeqs:   []int64{101},
				DeferredSeqs:   []int64{102},
				Subject:        "Update parsed",
				GroupingReason: "checkout service change is focused",
				DeferredReasons: []DeferredReason{{
					Seq:    102,
					Reason: "docs change is independent",
				}},
			}
			rewriteReq := NewIntentMessageRewriteRequest(req, plan, EvaluateIntentPlanMessageQuality(req, plan))
			p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
				return http.StatusOK, tc.response
			})

			_, err := p.RewriteIntentMessage(context.Background(), rewriteReq)
			var validationErr *IntentMessageRewriteValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("error=%T %v want IntentMessageRewriteValidationError", err, err)
			}
		})
	}
}

func TestOpenAIIntentMessageRewrite_PromptTraceUsesRewriteStrategy(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	plan := IntentPlan{
		SelectedSeqs:   []int64{101},
		DeferredSeqs:   []int64{102},
		Subject:        "Update parsed",
		GroupingReason: "checkout service change is focused",
		DeferredReasons: []DeferredReason{{
			Seq:    102,
			Reason: "docs change is independent",
		}},
	}
	rewriteReq := NewIntentMessageRewriteRequest(req, plan, EvaluateIntentPlanMessageQuality(req, plan))
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedToolCall("Tighten checkout validation", "")
	})
	writer, records := newPromptTraceTestWriter(t)
	ctx := prompttrace.With(context.Background(), writer, prompttrace.Metadata{
		Strategy:     string(CommitStrategyIntent),
		OfferedSeqs:  []int64{101, 102},
		BranchRef:    "refs/heads/main",
		Generation:   11,
		DiffIncluded: true,
		DiffCap:      DiffCap,
	})

	if _, err := p.RewriteIntentMessage(ctx, rewriteReq); err != nil {
		t.Fatalf("RewriteIntentMessage: %v", err)
	}
	got := promptTraceRecordByStage(t, records(), "request")
	if got.Strategy != "intent_message_rewrite" {
		t.Fatalf("strategy=%q want intent_message_rewrite", got.Strategy)
	}
	if strings.Join(int64sToStrings(got.OfferedSeqs), ",") != "101" {
		t.Fatalf("offered seqs=%v want selected seq only", got.OfferedSeqs)
	}
	if got.DiffCap != IntentStageDiffCap || !got.DiffIncluded {
		t.Fatalf("diff metadata cap=%d included=%v", got.DiffCap, got.DiffIncluded)
	}
}

func TestOpenAI_PromptTraceRecordsExactIntentRequest(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	p, last, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedIntentPlanToolCall(IntentPlan{
			SelectedSeqs:   []int64{101},
			DeferredSeqs:   []int64{102},
			Subject:        "Update checkout flow",
			Body:           "- apply checkout validation",
			GroupingReason: "single focused checkout change",
			DeferredReasons: []DeferredReason{{
				Seq:    102,
				Reason: "documentation should commit separately",
			}},
		})
	})
	writer, records := newPromptTraceTestWriter(t)
	ctx := prompttrace.With(context.Background(), writer, prompttrace.Metadata{
		Strategy:     string(CommitStrategyIntent),
		OfferedSeqs:  []int64{101, 102},
		BranchRef:    "refs/heads/main",
		Generation:   11,
		DiffIncluded: true,
		DiffCap:      DiffCap,
	})
	if _, err := p.PlanIntent(ctx, req); err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	got := promptTraceRecordByStage(t, records(), "request")
	if got.Strategy != string(CommitStrategyIntent) || got.Provider != "openai-compat" || got.Model != "test-model" {
		t.Fatalf("metadata strategy/provider/model = %q/%q/%q", got.Strategy, got.Provider, got.Model)
	}
	if strings.Join(int64sToStrings(got.OfferedSeqs), ",") != "101,102" {
		t.Fatalf("offered seqs=%v", got.OfferedSeqs)
	}
	if got.BranchRef != "refs/heads/main" || got.Generation != 11 {
		t.Fatalf("branch metadata = %q generation %d", got.BranchRef, got.Generation)
	}
	if !got.DiffIncluded {
		t.Fatal("DiffIncluded=false want true for captured diffs")
	}
	if got.Transform.InputBytes == 0 || got.Transform.RedactedBytes == 0 || got.Transform.OutputBytes == 0 {
		t.Fatalf("transform byte counts=%+v want captured diff metadata", got.Transform)
	}
	if !got.Transform.RedactionApplied {
		t.Fatalf("redaction_applied=false want true for captured secret diff: %+v", got.Transform)
	}
	if string(got.Request) != string(last.rawBody) {
		t.Fatalf("trace request differs from sent body\ntrace=%s\nsent=%s", got.Request, last.rawBody)
	}
	if !strings.Contains(got.SystemMessage, "intent planner") {
		t.Fatalf("system message not recorded: %q", got.SystemMessage)
	}
	if !strings.Contains(got.UserMessage, "Plan the next commit intent") {
		t.Fatalf("user message not recorded: %q", got.UserMessage)
	}
	schema, ok := got.ToolSchema.(map[string]any)
	if !ok || schema["type"] != "object" {
		t.Fatalf("tool schema not recorded: %#v", got.ToolSchema)
	}
}

func TestOpenAI_PromptTraceSanitizesProviderError(t *testing.T) {
	const secret = "prompt-trace-secret"
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 400, `{"error":{"token":"` + secret + `"}}`
	})
	writer, records := newPromptTraceTestWriter(t)
	ctx := prompttrace.With(context.Background(), writer, prompttrace.Metadata{Strategy: string(CommitStrategyIntent)})
	if _, err := p.PlanIntent(ctx, sampleIntentPlanRequest(t)); err == nil {
		t.Fatal("PlanIntent unexpectedly succeeded")
	}
	got := promptTraceRecordByStage(t, records(), "response")
	if got.Response == nil || strings.Contains(got.Response.Error, secret) || !strings.Contains(got.Response.Error, "[REDACTED]") {
		t.Fatalf("prompt response=%+v", got.Response)
	}
}

// TestOpenAIIntentPlan_NormalizesSpuriousDeferredReason exercises case (a)
// from the [Tests] task: when the planner emits a deferred_reasons entry whose
// seq is selected (not deferred), the openai-compat provider drops the
// spurious entry and returns a valid plan. The grouped commit goes ahead
// instead of collapsing to deterministic one-item fallback.
func TestOpenAIIntentPlan_NormalizesSpuriousDeferredReason(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedIntentPlanToolCall(IntentPlan{
			SelectedSeqs:   []int64{101},
			DeferredSeqs:   []int64{102},
			Subject:        "Tighten checkout flow",
			Body:           "- align validator",
			GroupingReason: "single focused checkout change",
			DeferredReasons: []DeferredReason{
				{Seq: 102, Reason: "documentation change is separate"},
				{Seq: 101, Reason: "spurious entry referencing selected seq"},
			},
		})
	})
	plan, err := p.PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if len(plan.DeferredReasons) != 1 || plan.DeferredReasons[0].Seq != 102 {
		t.Fatalf("normalized deferred reasons=%+v", plan.DeferredReasons)
	}
	if plan.SelectedSeqs[0] != 101 || plan.DeferredSeqs[0] != 102 {
		t.Fatalf("plan seqs not preserved: %+v", plan)
	}
	if plan.Source != "openai-compat" {
		t.Fatalf("source=%q", plan.Source)
	}
}

func TestOpenAIIntentPlan_RejectsSelectedDeferredOverlap(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedIntentPlanToolCall(IntentPlan{
			SelectedSeqs:   []int64{101},
			DeferredSeqs:   []int64{101, 102},
			Subject:        "Tighten checkout flow",
			Body:           "- align validator",
			GroupingReason: "single focused checkout change",
			DeferredReasons: []DeferredReason{
				{Seq: 101, Reason: "overlap emitted by planner"},
				{Seq: 102, Reason: "documentation change is separate"},
			},
		})
	})
	_, err := p.PlanIntent(context.Background(), req)
	if err == nil {
		t.Fatalf("expected selected/deferred overlap validation error")
	}
	var typed *IntentPlanValidationError
	if !errors.As(err, &typed) {
		t.Fatalf("error=%T %v want *IntentPlanValidationError", err, err)
	}
	if typed.Code != IntentPlanValidationSelectedDeferredOverlap || typed.Seq != 101 {
		t.Fatalf("typed error code=%v seq=%d want overlap seq 101", typed.Code, typed.Seq)
	}
}

// TestOpenAIIntentPlan_AllBadDeferredReasonsSynthesizesMarker exercises the
// coercion path: when every emitted deferred_reasons entry is spurious (here
// the only reason references the selected seq 101), the openai-compat
// provider drops the spurious entry and synthesizes a marker reason for the
// real deferred seq 102. The plan then validates and the grouped commit
// proceeds rather than collapsing to the deterministic fallback.
func TestOpenAIIntentPlan_AllBadDeferredReasonsSynthesizesMarker(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedIntentPlanToolCall(IntentPlan{
			SelectedSeqs:   []int64{101},
			DeferredSeqs:   []int64{102},
			Subject:        "Tighten checkout flow",
			GroupingReason: "single focused checkout change",
			DeferredReasons: []DeferredReason{
				{Seq: 101, Reason: "all reasons are spurious"},
			},
		})
	})
	planner := Compose(p, DeterministicProvider{})
	plan, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if len(plan.DeferredReasons) != 1 || plan.DeferredReasons[0].Seq != 102 {
		t.Fatalf("normalized deferred reasons=%+v", plan.DeferredReasons)
	}
	if plan.DeferredReasons[0].Reason != IntentPlanReasonMarker {
		t.Fatalf("synthesized reason=%q want %q", plan.DeferredReasons[0].Reason, IntentPlanReasonMarker)
	}
}

// TestOpenAIIntentPlan_FixtureNormalizesAndProceeds wires the
// bad_deferred_reason.json repro fixture through the full openai-compat
// PlanIntent path. The mock returns the recorded planner JSON; provider-side
// normalization drops the spurious deferred_reasons entry and the resulting
// plan validates without collapsing to deterministic.
func TestOpenAIIntentPlan_FixtureNormalizesAndProceeds(t *testing.T) {
	fx := loadBadDeferredReasonFixture(t)
	req := badDeferredReasonRequest(t, fx)
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, string(fx.OpenAIResponse)
	})
	plan, err := p.PlanIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("PlanIntent: %v", err)
	}
	if len(plan.SelectedSeqs) != 2 || plan.SelectedSeqs[0] != 5017 || plan.SelectedSeqs[1] != 5018 {
		t.Fatalf("selected=%v want [5017 5018]", plan.SelectedSeqs)
	}
	if len(plan.DeferredSeqs) != 1 || plan.DeferredSeqs[0] != 5019 {
		t.Fatalf("deferred=%v want [5019]", plan.DeferredSeqs)
	}
	if len(plan.DeferredReasons) != 1 || plan.DeferredReasons[0].Seq != 5019 {
		t.Fatalf("deferred reasons=%+v want one entry for seq 5019", plan.DeferredReasons)
	}
}

func TestOpenAIIntentPlan_InvalidPlanReturnsErrorWhenComposed(t *testing.T) {
	req := sampleIntentPlanRequest(t)
	p, _, _ := newOpenAIMock(t, func(capturedReq) (int, string) {
		return 200, cannedIntentPlanToolCall(IntentPlan{
			SelectedSeqs:   []int64{999},
			DeferredSeqs:   []int64{102},
			Subject:        "Invent bad plan",
			GroupingReason: "bad",
			DeferredReasons: []DeferredReason{{
				Seq:    102,
				Reason: "defer",
			}},
		})
	})
	planner := Compose(p, DeterministicProvider{})
	_, err := planner.(IntentPlanner).PlanIntent(context.Background(), req)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "selected seq 999 outside offered window") {
		t.Fatalf("error=%v", err)
	}
}

func newPromptTraceTestWriter(t *testing.T) (*prompttrace.Writer, func() []promptTraceJSONRecord) {
	t.Helper()
	dir := t.TempDir()
	writer, err := prompttrace.New(prompttrace.Options{
		Repo: "/repo",
		Dir:  dir,
		Now: func() time.Time {
			return time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("prompttrace.New: %v", err)
	}
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Fatalf("close prompt trace: %v", err)
		}
	})
	return writer, func() []promptTraceJSONRecord {
		t.Helper()
		if err := writer.Close(); err != nil {
			t.Fatalf("close prompt trace: %v", err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "2026-05-04.jsonl"))
		if err != nil {
			t.Fatalf("read prompt trace: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		out := make([]promptTraceJSONRecord, 0, len(lines))
		for _, line := range lines {
			var rec promptTraceJSONRecord
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("decode prompt trace %q: %v", line, err)
			}
			out = append(out, rec)
		}
		return out
	}
}

type promptTraceJSONRecord struct {
	Stage         string                        `json:"stage"`
	Strategy      string                        `json:"strategy"`
	Provider      string                        `json:"provider"`
	Model         string                        `json:"model"`
	Seq           int64                         `json:"seq"`
	OfferedSeqs   []int64                       `json:"offered_seqs"`
	BranchRef     string                        `json:"branch_ref"`
	Generation    int64                         `json:"generation"`
	DiffIncluded  bool                          `json:"diff_included"`
	DiffCap       int                           `json:"diff_cap"`
	Transform     prompttrace.TransformMetadata `json:"transform"`
	SystemMessage string                        `json:"system_message"`
	UserMessage   string                        `json:"user_message"`
	ToolSchema    any                           `json:"tool_schema"`
	Request       json.RawMessage               `json:"request"`
	Response      *prompttrace.Response         `json:"response"`
}

func promptTraceRecordByStage(t *testing.T, records []promptTraceJSONRecord, stage string) promptTraceJSONRecord {
	t.Helper()
	for _, rec := range records {
		if rec.Stage == stage {
			return rec
		}
	}
	t.Fatalf("no prompt trace record with stage %q in %+v", stage, records)
	return promptTraceJSONRecord{}
}

func int64sToStrings(in []int64) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, strconv.FormatInt(v, 10))
	}
	return out
}

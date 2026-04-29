package ai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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

// openai_compat.go — HTTP provider that speaks the OpenAI chat-completions
// dialect. We deliberately use a structured tool-call (rather than the
// json_schema response_format the legacy used) so any vaguely-conformant
// gateway (Azure OpenAI, vLLM, LiteLLM, OpenRouter, llama.cpp's HTTP
// shim) can satisfy the contract — tool/function calling has the widest
// support across the OpenAI-compatible ecosystem.
//
// The full request shape:
//
//	POST <BaseURL>/chat/completions
//	{
//	  "model": "<Model>",
//	  "messages": [
//	    {"role": "system", "content": <SystemPrompt>},
//	    {"role": "user",   "content": <Serialized CommitContext>}
//	  ],
//	  "temperature": 0.3,
//	  "tools": [
//	    {"type": "function", "function": {
//	       "name": "commit_message",
//	       "parameters": {
//	         "type": "object",
//	         "properties": {
//	           "subject": {"type": "string"},
//	           "body":    {"type": "string"}
//	         },
//	         "required": ["subject"],
//	         "additionalProperties": false
//	       }}}
//	  ],
//	  "tool_choice": {"type": "function", "function": {"name": "commit_message"}}
//	}
//
// Errors (network, 4xx/5xx, parse failure, no tool call returned, empty
// subject) all surface as ordinary `error`s so the caller — typically
// Compose(openai, deterministic) — can fall back without bespoke
// classification logic.
//
// SECURITY NOTES (subset of the legacy hardening that v1 still wants):
//   - Authorization header is set via Bearer; redirect-following is
//     disabled on the default HTTP client we construct so the bearer
//     token never leaks to a different host through a 3xx.
//   - SanitizeMessage is applied on every successful response; a model
//     that returns control characters or oversize subjects cannot
//     pollute the commit log.
package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

// DefaultOpenAIBaseURL is the canonical OpenAI v1 endpoint root.
const DefaultOpenAIBaseURL = "https://api.openai.com/v1"

// DefaultOpenAIModel matches the spec §10.4 default.
const DefaultOpenAIModel = "gpt-4o-mini"

// DefaultOpenAITimeout is the per-request HTTP timeout. The caller's ctx
// can shorten this; the field exists so a client without a deadline still
// gets bounded behaviour.
const DefaultOpenAITimeout = 15 * time.Second

// openAISystemPrompt is the steering text we prepend to every request.
// Kept short on purpose — verbose system prompts tend to make smaller
// models hallucinate boilerplate the sanitizer then has to strip.
const openAISystemPrompt = "You are a git commit message generator. " +
	"Always call the commit_message function. " +
	"Subject is imperative, concise, no trailing period. " +
	"Body (optional) is a bullet list describing what changed and why."

var openAICommitMessageParameters = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"subject": map[string]any{
			"type":        "string",
			"description": "Imperative subject line; <= 72 chars; no trailing period.",
		},
		"body": map[string]any{
			"type":        "string",
			"description": "Optional bullet body explaining what/why; may be empty.",
		},
	},
	"required":             []string{"subject"},
	"additionalProperties": false,
}

var openAIIntentPlanParameters = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"selected_seqs": map[string]any{
			"type":        "array",
			"description": "Non-empty seqs selected for the next commit.",
			"items":       map[string]any{"type": "integer"},
		},
		"deferred_seqs": map[string]any{
			"type":        "array",
			"description": "Every offered seq not selected for this commit.",
			"items":       map[string]any{"type": "integer"},
		},
		"subject": map[string]any{
			"type":        "string",
			"description": "Imperative subject line for the selected captures; <= 72 chars; no trailing period.",
		},
		"body": map[string]any{
			"type":        "string",
			"description": "Optional bullet body explaining what/why; may be empty.",
		},
		"grouping_reason": map[string]any{
			"type":        "string",
			"description": "Evidence-grounded reason these selected captures belong together.",
		},
		"deferred_reasons": map[string]any{
			"type":        "array",
			"description": "One reason for every deferred seq.",
			"items": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"seq":    map[string]any{"type": "integer"},
					"reason": map[string]any{"type": "string"},
				},
				"required":             []string{"seq", "reason"},
				"additionalProperties": false,
			},
		},
	},
	"required":             []string{"selected_seqs", "deferred_seqs", "subject", "body", "grouping_reason", "deferred_reasons"},
	"additionalProperties": false,
}

// OpenAIProvider is the OpenAI-compatible HTTP provider. Zero value is
// usable: Generate fills in the BaseURL/Model/HTTP/Now defaults on first
// call. Once initialized the provider is concurrency-safe (the http
// client is the only mutable shared state, and net/http.Client is safe
// for concurrent use).
type OpenAIProvider struct {
	BaseURL string       // chat-completions root; defaults to DefaultOpenAIBaseURL
	APIKey  string       // bearer token; required (empty -> error from Generate)
	Model   string       // defaults to DefaultOpenAIModel
	HTTP    *http.Client // defaults to a redirect-refusing client w/ DefaultOpenAITimeout
	Now     func() time.Time

	// DiffCap caps the unified-diff payload before send (default DiffCap).
	DiffCap int
}

// Name reports the canonical identifier; sources stamped on Result are
// useful for downstream telemetry.
func (*OpenAIProvider) Name() string { return "openai-compat" }

// Generate POSTs the chat-completion request and parses the tool-call.
func (p *OpenAIProvider) Generate(ctx context.Context, cc CommitContext) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(p.APIKey) == "" {
		return Result{}, errors.New("openai-compat: missing API key")
	}

	baseURL, err := normalizeOpenAIBaseURL(p.BaseURL, false)
	if err != nil {
		return Result{}, err
	}
	model := p.Model
	if model == "" {
		model = DefaultOpenAIModel
	}
	diffCap := p.DiffCap
	if diffCap == 0 {
		diffCap = DiffCap
	}
	httpClient := p.HTTP
	if httpClient == nil {
		httpClient = defaultOpenAIClient()
	}

	body, transform, err := buildOpenAIRequestWithTrace(model, cc, diffCap)
	if err != nil {
		return Result{}, fmt.Errorf("openai-compat: build request: %w", err)
	}
	p.recordPromptRequest(ctx, body, transform, prompttrace.Metadata{
		Strategy:     "event",
		DiffIncluded: cc.DiffText != "",
		DiffCap:      diffCap,
	})

	endpoint, err := url.JoinPath(baseURL, "chat", "completions")
	if err != nil {
		return Result{}, fmt.Errorf("openai-compat: build endpoint: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return Result{}, fmt.Errorf("openai-compat: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.APIKey)
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		p.recordPromptResponse(ctx, model, "event", prompttrace.Response{Error: err.Error()})
		return Result{}, fmt.Errorf("openai-compat: http: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MiB cap
	if err != nil {
		p.recordPromptResponse(ctx, model, "event", prompttrace.Response{StatusCode: resp.StatusCode, Error: err.Error()})
		return Result{}, fmt.Errorf("openai-compat: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("openai-compat: http %d: %s", resp.StatusCode, truncateForError(string(raw)))
		p.recordPromptResponse(ctx, model, "event", prompttrace.Response{StatusCode: resp.StatusCode, Error: err.Error()})
		return Result{}, err
	}

	subject, body2, err := parseToolCall(raw)
	if err != nil {
		p.recordPromptResponse(ctx, model, "event", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		return Result{}, err
	}
	composed := subject
	if strings.TrimSpace(body2) != "" {
		composed = subject + "\n\n" + body2
	}
	cleaned := SanitizeMessage(composed)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	subj := parts[0]
	var bodyOut string
	if len(parts) == 2 {
		bodyOut = parts[1]
	}
	if strings.TrimSpace(subj) == "" {
		err := errors.New("openai-compat: empty subject after sanitize")
		p.recordPromptResponse(ctx, model, "event", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		return Result{}, err
	}
	p.recordPromptResponse(ctx, model, "event", prompttrace.Response{StatusCode: resp.StatusCode, Subject: subj, Body: bodyOut})
	return Result{
		Subject: subj,
		Body:    bodyOut,
		Source:  "openai-compat",
	}, nil
}

// PlanIntent POSTs the planner request and parses the structured tool-call.
func (p *OpenAIProvider) PlanIntent(ctx context.Context, plannerReq IntentPlanRequest) (IntentPlan, error) {
	if err := ctx.Err(); err != nil {
		return IntentPlan{}, err
	}
	if strings.TrimSpace(p.APIKey) == "" {
		return IntentPlan{}, errors.New("openai-compat: missing API key")
	}

	baseURL, err := normalizeOpenAIBaseURL(p.BaseURL, false)
	if err != nil {
		return IntentPlan{}, err
	}
	model := p.Model
	if model == "" {
		model = DefaultOpenAIModel
	}
	httpClient := p.HTTP
	if httpClient == nil {
		httpClient = defaultOpenAIClient()
	}

	body, transform, err := buildOpenAIIntentPlanRequestWithTrace(model, plannerReq)
	if err != nil {
		return IntentPlan{}, fmt.Errorf("openai-compat: build intent plan request: %w", err)
	}
	p.recordPromptRequest(ctx, body, transform, prompttrace.Metadata{
		Strategy:     "intent",
		OfferedSeqs:  offeredSeqs(plannerReq),
		DiffIncluded: intentDiffIncluded(plannerReq),
		DiffCap:      DiffCap,
	})

	endpoint, err := url.JoinPath(baseURL, "chat", "completions")
	if err != nil {
		return IntentPlan{}, fmt.Errorf("openai-compat: build endpoint: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return IntentPlan{}, fmt.Errorf("openai-compat: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.APIKey)
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{Error: err.Error()})
		return IntentPlan{}, fmt.Errorf("openai-compat: http: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{StatusCode: resp.StatusCode, Error: err.Error()})
		return IntentPlan{}, fmt.Errorf("openai-compat: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("openai-compat: http %d: %s", resp.StatusCode, truncateForError(string(raw)))
		p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{StatusCode: resp.StatusCode, Error: err.Error()})
		return IntentPlan{}, err
	}

	plan, err := parseIntentPlanToolCall(raw)
	if err != nil {
		p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		return IntentPlan{}, err
	}
	if strings.TrimSpace(plan.Subject) == "" {
		err := errors.New("openai-compat: intent plan returned empty subject")
		p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		return IntentPlan{}, err
	}
	cleaned := SanitizeMessage(plan.Subject + "\n\n" + plan.Body)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	plan.Subject = parts[0]
	if len(parts) == 2 {
		plan.Body = parts[1]
	} else {
		plan.Body = ""
	}
	plan = NormalizeIntentPlanReasons(plan)
	plan, dropped, synthesized := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) > 0 || len(synthesized) > 0 {
		attrs := []any{
			slog.String("provider", p.Name()),
			slog.String("model", model),
		}
		if len(dropped) > 0 {
			attrs = append(attrs, slog.Any("dropped_seqs", dropped))
		}
		if len(synthesized) > 0 {
			attrs = append(attrs, slog.Any("synthesized_seqs", synthesized))
		}
		slog.Warn("intent planner: normalized deferred_reasons", attrs...)
	}
	plan.Source = p.Name()
	if err := ValidateIntentPlan(plannerReq, plan); err != nil {
		p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		return IntentPlan{}, err
	}
	p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{
		StatusCode:     resp.StatusCode,
		Subject:        plan.Subject,
		Body:           plan.Body,
		SelectedSeqs:   plan.SelectedSeqs,
		DeferredSeqs:   plan.DeferredSeqs,
		GroupingReason: plan.GroupingReason,
	})
	return plan, nil
}

func (p *OpenAIProvider) recordPromptRequest(ctx context.Context, body []byte, transform prompttrace.TransformMetadata, fallback prompttrace.Metadata) {
	logger, meta, ok := prompttrace.From(ctx)
	if !ok {
		return
	}
	if meta.Strategy == "" {
		meta.Strategy = fallback.Strategy
	}
	if len(meta.OfferedSeqs) == 0 {
		meta.OfferedSeqs = append([]int64(nil), fallback.OfferedSeqs...)
	}
	if meta.DiffCap == 0 {
		meta.DiffCap = fallback.DiffCap
	}
	meta.DiffIncluded = meta.DiffIncluded || fallback.DiffIncluded
	meta = promptTraceMetadata(meta, p.Name(), p.resolvedModel())
	rec, err := openAITraceRequest(body, transform)
	if err != nil {
		logger.Record(prompttrace.Record{
			Stage:        "request",
			Strategy:     meta.Strategy,
			Provider:     meta.Provider,
			Model:        meta.Model,
			Seq:          meta.Seq,
			OfferedSeqs:  meta.OfferedSeqs,
			BranchRef:    meta.BranchRef,
			Generation:   meta.Generation,
			DiffIncluded: meta.DiffIncluded,
			DiffCap:      meta.DiffCap,
			Error:        err.Error(),
		})
		return
	}
	rec.Strategy = meta.Strategy
	rec.Provider = meta.Provider
	rec.Model = meta.Model
	rec.Seq = meta.Seq
	rec.OfferedSeqs = append([]int64(nil), meta.OfferedSeqs...)
	rec.BranchRef = meta.BranchRef
	rec.Generation = meta.Generation
	rec.DiffIncluded = meta.DiffIncluded
	rec.DiffCap = meta.DiffCap
	logger.Record(rec)
}

func (p *OpenAIProvider) recordPromptResponse(ctx context.Context, model, strategy string, response prompttrace.Response) {
	logger, meta, ok := prompttrace.From(ctx)
	if !ok {
		return
	}
	if meta.Strategy == "" {
		meta.Strategy = strategy
	}
	if meta.Model == "" {
		meta.Model = model
	}
	meta = promptTraceMetadata(meta, p.Name(), meta.Model)
	logger.Record(prompttrace.Record{
		Stage:        "response",
		Strategy:     meta.Strategy,
		Provider:     meta.Provider,
		Model:        meta.Model,
		Seq:          meta.Seq,
		OfferedSeqs:  append([]int64(nil), meta.OfferedSeqs...),
		BranchRef:    meta.BranchRef,
		Generation:   meta.Generation,
		DiffIncluded: meta.DiffIncluded,
		DiffCap:      meta.DiffCap,
		Response:     &response,
	})
}

func (p *OpenAIProvider) resolvedModel() string {
	if p.Model != "" {
		return p.Model
	}
	return DefaultOpenAIModel
}

func normalizeOpenAIBaseURL(raw string, requireHTTPS bool) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(raw), "/")
	if base == "" {
		base = DefaultOpenAIBaseURL
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("openai-compat: invalid ACD_AI_BASE_URL: %w", err)
	}
	if !u.IsAbs() || u.Host == "" {
		return "", errors.New("openai-compat: ACD_AI_BASE_URL must be an absolute URL")
	}
	if requireHTTPS && u.Scheme != "https" {
		return "", fmt.Errorf("openai-compat: ACD_AI_BASE_URL must use https, got %q", u.Scheme)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", fmt.Errorf("openai-compat: unsupported ACD_AI_BASE_URL scheme %q", u.Scheme)
	}
	return u.String(), nil
}

// defaultOpenAIClient is a redirect-refusing http.Client with a sane
// default timeout. The 3xx-refusal is the v1 minimum hardening that
// keeps a hostile network from steering the bearer token to a logging
// host (full SSRF guard lives in the legacy implementation; we ship the
// most important piece here and revisit hostname guarding when wiring
// lands in the daemon).
func defaultOpenAIClient() *http.Client {
	return &http.Client{
		Timeout: DefaultOpenAITimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// truncateForError shortens an upstream error body before embedding it
// in our error chain — large 5xx HTML pages are not useful to a user.
func truncateForError(s string) string {
	const max = 200
	s = strings.TrimSpace(s)
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

// buildOpenAIRequest serializes the chat-completion payload. Keeping
// this in its own function makes the test-mode "capture the JSON the
// provider sent" assertion straightforward. DiffText is redacted before
// truncation so secrets near either end of a large diff cannot survive
// provider serialization.
func buildOpenAIRequest(model string, cc CommitContext, diffCap int) ([]byte, error) {
	body, _, err := buildOpenAIRequestWithTrace(model, cc, diffCap)
	return body, err
}

func buildOpenAIRequestWithTrace(model string, cc CommitContext, diffCap int) ([]byte, prompttrace.TransformMetadata, error) {
	redacted := RedactDiffSecrets(cc.DiffText)
	diff := Truncate(redacted, diffCap)
	transform := promptTransformMetadata(cc.DiffText, redacted, diff)

	type op struct {
		Path    string `json:"path"`
		Op      string `json:"op"`
		OldPath string `json:"old_path,omitempty"`
	}
	type userPayload struct {
		Path     string   `json:"path"`
		Op       string   `json:"op"`
		OldPath  string   `json:"old_path,omitempty"`
		Branch   string   `json:"branch,omitempty"`
		RepoRoot string   `json:"repo_root,omitempty"`
		Diff     string   `json:"diff,omitempty"`
		Commits  []string `json:"recent_commits,omitempty"`
		MultiOp  []op     `json:"multi_op,omitempty"`
	}

	up := userPayload{
		Path:     cc.Path,
		Op:       cc.Op,
		OldPath:  cc.OldPath,
		Branch:   cc.Branch,
		RepoRoot: cc.RepoRoot,
		Diff:     diff,
		Commits:  cc.Commits,
	}
	for _, item := range cc.MultiOp {
		up.MultiOp = append(up.MultiOp, op{Path: item.Path, Op: item.Op, OldPath: item.OldPath})
	}

	userJSON, err := json.Marshal(up)
	if err != nil {
		return nil, prompttrace.TransformMetadata{}, err
	}

	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type funcDecl struct {
		Name        string         `json:"name"`
		Description string         `json:"description,omitempty"`
		Parameters  map[string]any `json:"parameters"`
	}
	type tool struct {
		Type     string   `json:"type"`
		Function funcDecl `json:"function"`
	}
	type toolChoiceFn struct {
		Name string `json:"name"`
	}
	type toolChoice struct {
		Type     string       `json:"type"`
		Function toolChoiceFn `json:"function"`
	}
	type req struct {
		Model       string     `json:"model"`
		Messages    []message  `json:"messages"`
		Tools       []tool     `json:"tools"`
		ToolChoice  toolChoice `json:"tool_choice"`
		Temperature float64    `json:"temperature"`
	}

	body := req{
		Model: model,
		Messages: []message{
			{Role: "system", Content: openAISystemPrompt},
			{Role: "user", Content: "Generate a commit message for this change:\n" + string(userJSON)},
		},
		Tools: []tool{{
			Type: "function",
			Function: funcDecl{
				Name:        "commit_message",
				Description: "Emit a single commit message for the change described.",
				Parameters:  openAICommitMessageParameters,
			},
		}},
		ToolChoice: toolChoice{
			Type:     "function",
			Function: toolChoiceFn{Name: "commit_message"},
		},
		Temperature: 0.3,
	}
	raw, err := json.Marshal(body)
	return raw, transform, err
}

func buildOpenAIIntentPlanRequest(model string, plannerReq IntentPlanRequest) ([]byte, error) {
	body, _, err := buildOpenAIIntentPlanRequestWithTrace(model, plannerReq)
	return body, err
}

func buildOpenAIIntentPlanRequestWithTrace(model string, plannerReq IntentPlanRequest) ([]byte, prompttrace.TransformMetadata, error) {
	userPrompt, err := BuildIntentPlanUserPrompt(plannerReq)
	if err != nil {
		return nil, prompttrace.TransformMetadata{}, err
	}

	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type funcDecl struct {
		Name        string         `json:"name"`
		Description string         `json:"description,omitempty"`
		Parameters  map[string]any `json:"parameters"`
	}
	type tool struct {
		Type     string   `json:"type"`
		Function funcDecl `json:"function"`
	}
	type toolChoiceFn struct {
		Name string `json:"name"`
	}
	type toolChoice struct {
		Type     string       `json:"type"`
		Function toolChoiceFn `json:"function"`
	}
	type req struct {
		Model       string     `json:"model"`
		Messages    []message  `json:"messages"`
		Tools       []tool     `json:"tools"`
		ToolChoice  toolChoice `json:"tool_choice"`
		Temperature float64    `json:"temperature"`
	}

	body := req{
		Model: model,
		Messages: []message{
			{Role: "system", Content: IntentPlannerSystemPrompt()},
			{Role: "user", Content: userPrompt},
		},
		Tools: []tool{{
			Type: "function",
			Function: funcDecl{
				Name:        "capture_intent_plan",
				Description: "Select or defer every offered capture for the next commit.",
				Parameters:  openAIIntentPlanParameters,
			},
		}},
		ToolChoice: toolChoice{
			Type:     "function",
			Function: toolChoiceFn{Name: "capture_intent_plan"},
		},
		Temperature: 0.2,
	}
	raw, err := json.Marshal(body)
	return raw, plannerReq.CapturedDiffTransform, err
}

// parseToolCall extracts subject + body from a chat-completion response
// whose assistant message carries a single tool_call to commit_message.
// Tolerates the OpenAI-canonical shape and the (older / vLLM) shape that
// embeds the call as `function_call`. Returns an error when no call is
// present or arguments are malformed.
func parseToolCall(raw []byte) (subject string, body string, err error) {
	type funcArgs struct {
		Subject string `json:"subject"`
		Body    string `json:"body"`
	}
	type fcall struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	type toolCall struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function fcall  `json:"function"`
	}
	type message struct {
		Role         string     `json:"role"`
		Content      string     `json:"content"`
		ToolCalls    []toolCall `json:"tool_calls"`
		FunctionCall *fcall     `json:"function_call"`
	}
	type choice struct {
		Index   int     `json:"index"`
		Message message `json:"message"`
	}
	type respShape struct {
		Choices []choice `json:"choices"`
		Error   *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	var r respShape
	if err := json.Unmarshal(raw, &r); err != nil {
		return "", "", fmt.Errorf("openai-compat: parse response: %w", err)
	}
	if r.Error != nil && r.Error.Message != "" {
		return "", "", fmt.Errorf("openai-compat: api error: %s", r.Error.Message)
	}
	if len(r.Choices) == 0 {
		return "", "", errors.New("openai-compat: no choices in response")
	}
	msg := r.Choices[0].Message

	var args string
	switch {
	case len(msg.ToolCalls) > 0 && msg.ToolCalls[0].Function.Arguments != "":
		args = msg.ToolCalls[0].Function.Arguments
	case msg.FunctionCall != nil && msg.FunctionCall.Arguments != "":
		args = msg.FunctionCall.Arguments
	default:
		return "", "", errors.New("openai-compat: response carried no tool_call arguments")
	}

	var fa funcArgs
	if err := json.Unmarshal([]byte(args), &fa); err != nil {
		return "", "", fmt.Errorf("openai-compat: parse tool arguments: %w", err)
	}
	if strings.TrimSpace(fa.Subject) == "" {
		return "", "", errors.New("openai-compat: tool call returned empty subject")
	}
	return fa.Subject, fa.Body, nil
}

func parseIntentPlanToolCall(raw []byte) (IntentPlan, error) {
	type fcall struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	type toolCall struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function fcall  `json:"function"`
	}
	type message struct {
		Role         string     `json:"role"`
		Content      string     `json:"content"`
		ToolCalls    []toolCall `json:"tool_calls"`
		FunctionCall *fcall     `json:"function_call"`
	}
	type choice struct {
		Index   int     `json:"index"`
		Message message `json:"message"`
	}
	type respShape struct {
		Choices []choice `json:"choices"`
		Error   *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	var r respShape
	if err := json.Unmarshal(raw, &r); err != nil {
		return IntentPlan{}, fmt.Errorf("openai-compat: parse response: %w", err)
	}
	if r.Error != nil && r.Error.Message != "" {
		return IntentPlan{}, fmt.Errorf("openai-compat: api error: %s", r.Error.Message)
	}
	if len(r.Choices) == 0 {
		return IntentPlan{}, errors.New("openai-compat: no choices in response")
	}
	msg := r.Choices[0].Message

	var args string
	switch {
	case len(msg.ToolCalls) > 0 && msg.ToolCalls[0].Function.Arguments != "":
		if msg.ToolCalls[0].Function.Name != "capture_intent_plan" {
			return IntentPlan{}, fmt.Errorf("openai-compat: unexpected tool %q", msg.ToolCalls[0].Function.Name)
		}
		args = msg.ToolCalls[0].Function.Arguments
	case msg.FunctionCall != nil && msg.FunctionCall.Arguments != "":
		if msg.FunctionCall.Name != "capture_intent_plan" {
			return IntentPlan{}, fmt.Errorf("openai-compat: unexpected function %q", msg.FunctionCall.Name)
		}
		args = msg.FunctionCall.Arguments
	default:
		return IntentPlan{}, errors.New("openai-compat: response carried no intent-plan tool_call arguments")
	}

	var plan IntentPlan
	if err := json.Unmarshal([]byte(args), &plan); err != nil {
		return IntentPlan{}, fmt.Errorf("openai-compat: parse intent-plan tool arguments: %w", err)
	}
	return plan, nil
}

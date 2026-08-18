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
const DefaultOpenAIModel = "gpt-5.4-mini"

// DefaultOpenAITimeout is the per-request HTTP timeout. The caller's ctx
// can shorten this; the field exists so a client without a deadline still
// gets bounded behaviour.
const DefaultOpenAITimeout = 15 * time.Second

// openAISystemPrompt is the steering text we prepend to every request.
// Kept short on purpose — verbose system prompts tend to make smaller
// models hallucinate boilerplate the sanitizer then has to strip.
func openAISystemPrompt(format CommitFormat) string {
	return "You are a git commit message generator. " +
		"Always call the commit_message function. " +
		CommitMessageFormatInstructions(format)
}

func openAICommitMessageParameters(format CommitFormat) map[string]any {
	subjectDescription := "Line 1 only: imperative verb plus semantic change, <= 50 chars, no trailing period, avoid filenames unless the file itself changed."
	if effectiveCommitFormat(format) == CommitFormatConventional {
		subjectDescription = "Line 1 only: scope-less Conventional Commit '<type>: <description>', allowed type, <= 50 chars, no trailing period."
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"subject": map[string]any{
				"type":        "string",
				"description": subjectDescription,
			},
			"body": map[string]any{
				"type":        "string",
				"description": "Optional commit body bullets for why/context/impact. Each bullet starts with '- ', max 72 chars per line, continuation lines are indented and do not start with '- '. Do not restate the diff.",
			},
		},
		"required":             []string{"subject"},
		"additionalProperties": false,
	}
}

func openAIIntentPlanParameters(format CommitFormat) map[string]any {
	subjectDescription := "Final commit subject for selected captures: imperative verb plus semantic change, <= 50 chars, no trailing period, avoid filenames unless the file itself changed."
	if effectiveCommitFormat(format) == CommitFormatConventional {
		subjectDescription = "Final commit subject for selected captures: scope-less Conventional Commit '<type>: <description>', allowed type, <= 50 chars, no trailing period."
	}
	return map[string]any{
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
				"description": subjectDescription,
			},
			"body": map[string]any{
				"type":        "string",
				"description": "Optional final commit body bullets for why/context/impact. Do not explain why selected captures fit together; put that rationale only in grouping_reason.",
			},
			"grouping_reason": map[string]any{
				"type":        "string",
				"description": "Evidence-grounded rationale for why the selected captures belong together. This is not part of the git commit message.",
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
			"commit_groups": map[string]any{
				"type":        "array",
				"description": "Optional ordered commit partition for selected captures. Use when one offered window contains multiple independent commits.",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"selected_seqs": map[string]any{
							"type":        "array",
							"description": "Non-empty seqs selected for this commit group.",
							"items":       map[string]any{"type": "integer"},
						},
						"subject": map[string]any{
							"type":        "string",
							"description": subjectDescription,
						},
						"body": map[string]any{
							"type":        "string",
							"description": "Optional final commit body bullets for why/context/impact.",
						},
						"grouping_reason": map[string]any{
							"type":        "string",
							"description": "Evidence-grounded rationale for why this group is one commit.",
						},
					},
					"required":             []string{"selected_seqs", "subject", "body", "grouping_reason"},
					"additionalProperties": false,
				},
			},
		},
		"required":             []string{"selected_seqs", "deferred_seqs", "subject", "body", "grouping_reason", "deferred_reasons"},
		"additionalProperties": false,
	}
}

func openAIIntentPlanV2Parameters(format CommitFormat) map[string]any {
	subjectDescription := "Final commit subject: imperative verb plus semantic change, <= 50 chars, no trailing period."
	if effectiveCommitFormat(format) == CommitFormatConventional {
		subjectDescription = "Final commit subject: scope-less Conventional Commit '<type>: <description>', allowed type, <= 50 chars, no trailing period."
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"protocol_version": map[string]any{
				"type": "string",
				"enum": []string{IntentPlannerProtocolV2},
			},
			"candidates": map[string]any{
				"type":     "array",
				"minItems": 1,
				"maxItems": IntentOpenCandidateCap,
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"candidate_id": map[string]any{"type": "string", "maxLength": IntentCandidateIDCap},
						"selected_seqs": map[string]any{
							"type":     "array",
							"minItems": 1,
							"maxItems": IntentCandidateCaptureCap,
							"items":    map[string]any{"type": "integer"},
						},
						"purpose": map[string]any{"type": "string", "maxLength": IntentCandidatePurposeCap},
						"readiness": map[string]any{
							"type": "string",
							"enum": []string{string(IntentCandidateReady), string(IntentCandidateWait)},
						},
						"missing_companions": map[string]any{
							"type":     "array",
							"maxItems": IntentCandidateCaptureCap,
							"items":    map[string]any{"type": "string", "maxLength": IntentAtomicitySummaryCap},
						},
						"depends_on_candidates": map[string]any{
							"type":     "array",
							"maxItems": IntentOpenCandidateCap,
							"items":    map[string]any{"type": "string", "maxLength": IntentCandidateIDCap},
						},
						"subject": map[string]any{
							"type":        "string",
							"maxLength":   SubjectCap,
							"description": subjectDescription,
						},
						"body": map[string]any{
							"type":        "string",
							"description": "Optional commit body bullets for why/context/impact. Keep grouping rationale out of the body.",
						},
						"grouping_reason": map[string]any{"type": "string", "maxLength": IntentReasonCap},
					},
					"required": []string{
						"candidate_id", "selected_seqs", "purpose", "readiness",
						"missing_companions", "depends_on_candidates", "subject",
						"body", "grouping_reason",
					},
					"additionalProperties": false,
				},
			},
		},
		"required":             []string{"protocol_version", "candidates"},
		"additionalProperties": false,
	}
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

	// Format selects the commit-message subject contract. Empty preserves
	// the historical imperative default.
	Format CommitFormat
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

	if cc.CommitFormat == "" {
		cc.CommitFormat = p.Format
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
	if err := commitFormatValidationError("openai-compat", validateCommitMessageFormat(cc.CommitFormat, subj, bodyOut)); err != nil {
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
		// Intent stage uses IntentStageDiffCap so the planner sees enough
		// per-capture context to group multi-file changes; per-event
		// commit messages still cap at the legacy DiffCap.
		DiffCap: IntentStageDiffCap,
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
		cause := errors.New("openai-compat: intent plan returned empty subject")
		err := intentPlanShapeValidationError(cause.Error(), cause)
		p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		return IntentPlan{}, err
	}
	plan.Subject, plan.Body = sanitizeSubjectBody(plan.Subject, plan.Body)
	plan = NormalizeIntentPlanReasons(plan)
	plan, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) > 0 || len(synthesized) > 0 || len(overlapRemoved) > 0 {
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
		if len(overlapRemoved) > 0 {
			attrs = append(attrs, slog.Any("overlap_removed_seqs", overlapRemoved))
		}
		slog.Warn("intent planner: normalized deferred_reasons", attrs...)
	}
	plan.Source = p.Name()
	if err := ValidateIntentPlan(plannerReq, plan); err != nil {
		p.recordPromptResponse(ctx, model, "intent", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		LogRejectedIntentPlan(ctx, p.Name(), plannerReq, string(raw), err)
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

// PlanIntentV2 runs the native durable-candidate protocol. Correction retries
// are owned by the candidate engine; this method always serializes the exact
// bounded request it receives, including any RetryCorrection generated by the
// previous atomicity failure.
func (p *OpenAIProvider) PlanIntentV2(ctx context.Context, plannerReq IntentPlanRequestV2) (IntentPlanV2, error) {
	if err := ctx.Err(); err != nil {
		return IntentPlanV2{}, err
	}
	if strings.TrimSpace(p.APIKey) == "" {
		return IntentPlanV2{}, errors.New("openai-compat: missing API key")
	}
	if err := ValidateIntentPlanRequestV2(plannerReq); err != nil {
		return IntentPlanV2{}, err
	}

	baseURL, err := normalizeOpenAIBaseURL(p.BaseURL, false)
	if err != nil {
		return IntentPlanV2{}, err
	}
	model := p.Model
	if model == "" {
		model = DefaultOpenAIModel
	}
	httpClient := p.HTTP
	if httpClient == nil {
		httpClient = defaultOpenAIClient()
	}

	body, transform, err := buildOpenAIIntentPlanV2RequestWithTrace(model, plannerReq)
	if err != nil {
		return IntentPlanV2{}, fmt.Errorf("openai-compat: build intent v2 plan request: %w", err)
	}
	p.recordPromptRequest(ctx, body, transform, prompttrace.Metadata{
		Strategy:     "intent_v2",
		OfferedSeqs:  offeredSeqsV2(plannerReq),
		DiffIncluded: intentDiffIncludedV2(plannerReq),
		DiffCap:      IntentStageDiffCap,
	})

	endpoint, err := url.JoinPath(baseURL, "chat", "completions")
	if err != nil {
		return IntentPlanV2{}, fmt.Errorf("openai-compat: build endpoint: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return IntentPlanV2{}, fmt.Errorf("openai-compat: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.APIKey)
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		p.recordPromptResponse(ctx, model, "intent_v2", prompttrace.Response{Error: err.Error()})
		return IntentPlanV2{}, fmt.Errorf("openai-compat: http: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		p.recordPromptResponse(ctx, model, "intent_v2", prompttrace.Response{StatusCode: resp.StatusCode, Error: err.Error()})
		return IntentPlanV2{}, fmt.Errorf("openai-compat: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("openai-compat: http %d: %s", resp.StatusCode, truncateForError(string(raw)))
		p.recordPromptResponse(ctx, model, "intent_v2", prompttrace.Response{StatusCode: resp.StatusCode, Error: err.Error()})
		return IntentPlanV2{}, err
	}

	plan, err := parseIntentPlanV2ToolCall(raw, plannerReq)
	if err != nil {
		p.recordPromptResponse(ctx, model, "intent_v2", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		LogRejectedIntentPlanV2(ctx, p.Name(), plannerReq, string(raw), err)
		return IntentPlanV2{}, err
	}
	if err := validateNativeIntentPlanV2(plannerReq, plan); err != nil {
		p.recordPromptResponse(ctx, model, "intent_v2", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		LogRejectedIntentPlanV2(ctx, p.Name(), plannerReq, string(raw), err)
		return IntentPlanV2{}, err
	}
	for i := range plan.Candidates {
		plan.Candidates[i].Subject, plan.Candidates[i].Body = sanitizeSubjectBody(
			plan.Candidates[i].Subject,
			plan.Candidates[i].Body,
		)
	}
	if err := validateNativeIntentPlanV2(plannerReq, plan); err != nil {
		p.recordPromptResponse(ctx, model, "intent_v2", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: err.Error()})
		LogRejectedIntentPlanV2(ctx, p.Name(), plannerReq, string(raw), err)
		return IntentPlanV2{}, err
	}
	p.recordPromptResponse(ctx, model, "intent_v2", prompttrace.Response{
		StatusCode:   resp.StatusCode,
		SelectedSeqs: candidatePlanSeqs(plan),
	})
	return plan, nil
}

// RewriteIntentMessage POSTs a locked message-only rewrite request and parses
// the replacement subject/body. Grouping fields never come back from the tool.
func (p *OpenAIProvider) RewriteIntentMessage(ctx context.Context, rewriteReq IntentMessageRewriteRequest) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	ctx = withPromptTraceStrategy(ctx, "intent_message_rewrite", rewriteReq.LockedPlan.SelectedSeqs, intentDiffIncluded(rewriteReq.PlannerRequest), IntentStageDiffCap)
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
	httpClient := p.HTTP
	if httpClient == nil {
		httpClient = defaultOpenAIClient()
	}

	body, transform, err := buildOpenAIIntentMessageRewriteRequestWithTrace(model, rewriteReq)
	if err != nil {
		return Result{}, fmt.Errorf("openai-compat: build intent message rewrite request: %w", err)
	}
	p.recordPromptRequest(ctx, body, transform, prompttrace.Metadata{
		Strategy:     "intent_message_rewrite",
		OfferedSeqs:  append([]int64(nil), rewriteReq.LockedPlan.SelectedSeqs...),
		DiffIncluded: intentDiffIncluded(rewriteReq.PlannerRequest),
		DiffCap:      IntentStageDiffCap,
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
		p.recordPromptResponse(ctx, model, "intent_message_rewrite", prompttrace.Response{Error: err.Error()})
		return Result{}, fmt.Errorf("openai-compat: http: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		p.recordPromptResponse(ctx, model, "intent_message_rewrite", prompttrace.Response{StatusCode: resp.StatusCode, Error: err.Error()})
		return Result{}, fmt.Errorf("openai-compat: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("openai-compat: http %d: %s", resp.StatusCode, truncateForError(string(raw)))
		p.recordPromptResponse(ctx, model, "intent_message_rewrite", prompttrace.Response{StatusCode: resp.StatusCode, Error: err.Error()})
		return Result{}, err
	}

	subject, bodyOut, err := parseToolCall(raw)
	if err != nil {
		validationErr := &IntentMessageRewriteValidationError{Err: err}
		p.recordPromptResponse(ctx, model, "intent_message_rewrite", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: validationErr.Error()})
		return Result{}, validationErr
	}
	if intentMessageRewriteSubjectEmpty(subject) {
		validationErr := &IntentMessageRewriteValidationError{
			Err: errors.New("openai-compat: empty subject after intent message rewrite sanitize"),
		}
		p.recordPromptResponse(ctx, model, "intent_message_rewrite", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: validationErr.Error()})
		return Result{}, validationErr
	}
	cleaned := SanitizeMessage(subject + "\n\n" + bodyOut)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	result := Result{Subject: parts[0], Source: p.Name()}
	if len(parts) == 2 {
		result.Body = parts[1]
	}
	if strings.TrimSpace(result.Subject) == "" {
		validationErr := &IntentMessageRewriteValidationError{
			Err: errors.New("openai-compat: empty subject after intent message rewrite sanitize"),
		}
		p.recordPromptResponse(ctx, model, "intent_message_rewrite", prompttrace.Response{StatusCode: resp.StatusCode, ValidationError: validationErr.Error()})
		return Result{}, validationErr
	}
	p.recordPromptResponse(ctx, model, "intent_message_rewrite", prompttrace.Response{
		StatusCode: resp.StatusCode,
		Subject:    result.Subject,
		Body:       result.Body,
	})
	return result, nil
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
			Stage:            "request",
			Strategy:         meta.Strategy,
			Protocol:         meta.Protocol,
			Provider:         meta.Provider,
			Model:            meta.Model,
			Seq:              meta.Seq,
			OfferedSeqs:      meta.OfferedSeqs,
			BranchRef:        meta.BranchRef,
			Generation:       meta.Generation,
			DiffIncluded:     meta.DiffIncluded,
			DiffCap:          meta.DiffCap,
			ConfigRevisionID: meta.ConfigRevisionID,
			ConfigProfile:    meta.ConfigProfile,
			RetryCount:       meta.RetryCount,
			Error:            err.Error(),
		})
		return
	}
	rec.Strategy = meta.Strategy
	rec.Protocol = meta.Protocol
	rec.Provider = meta.Provider
	rec.Model = meta.Model
	rec.Seq = meta.Seq
	rec.OfferedSeqs = append([]int64(nil), meta.OfferedSeqs...)
	rec.BranchRef = meta.BranchRef
	rec.Generation = meta.Generation
	rec.DiffIncluded = meta.DiffIncluded
	rec.DiffCap = meta.DiffCap
	rec.ConfigRevisionID = meta.ConfigRevisionID
	rec.ConfigProfile = meta.ConfigProfile
	rec.RetryCount = meta.RetryCount
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
	response.Error = SanitizePlannerError(response.Error)
	response.ValidationError = SanitizePlannerError(response.ValidationError)
	meta = promptTraceMetadata(meta, p.Name(), meta.Model)
	logger.Record(prompttrace.Record{
		Stage:            "response",
		Strategy:         meta.Strategy,
		Protocol:         meta.Protocol,
		Provider:         meta.Provider,
		Model:            meta.Model,
		Seq:              meta.Seq,
		OfferedSeqs:      append([]int64(nil), meta.OfferedSeqs...),
		BranchRef:        meta.BranchRef,
		Generation:       meta.Generation,
		DiffIncluded:     meta.DiffIncluded,
		DiffCap:          meta.DiffCap,
		ConfigRevisionID: meta.ConfigRevisionID,
		ConfigProfile:    meta.ConfigProfile,
		RetryCount:       meta.RetryCount,
		Response:         &response,
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
	if u.User != nil {
		return "", errors.New("openai-compat: ACD_AI_BASE_URL must not contain embedded credentials")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("openai-compat: ACD_AI_BASE_URL must not contain a query or fragment")
	}
	if requireHTTPS && u.Scheme != "https" {
		return "", fmt.Errorf("openai-compat: ACD_AI_BASE_URL must use https, got %q", u.Scheme)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", fmt.Errorf("openai-compat: unsupported ACD_AI_BASE_URL scheme %q", u.Scheme)
	}
	return u.String(), nil
}

// NormalizeOpenAIBaseURL validates a user-authored endpoint and removes a
// trailing slash. HTTP remains available to callers that separately collect
// its required consent.
func NormalizeOpenAIBaseURL(raw string) (string, error) {
	return normalizeOpenAIBaseURL(raw, false)
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

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIFunctionDeclaration struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters"`
}

type openAITool struct {
	Type     string                    `json:"type"`
	Function openAIFunctionDeclaration `json:"function"`
}

type openAIFunctionChoice struct {
	Name string `json:"name"`
}

type openAIToolChoice struct {
	Type     string               `json:"type"`
	Function openAIFunctionChoice `json:"function"`
}

type openAIToolRequest struct {
	Model       string           `json:"model"`
	Messages    []openAIMessage  `json:"messages"`
	Tools       []openAITool     `json:"tools"`
	ToolChoice  openAIToolChoice `json:"tool_choice"`
	Temperature float64          `json:"temperature"`
}

type openAIToolRequestOptions struct {
	Model           string
	SystemPrompt    string
	UserPrompt      string
	ToolName        string
	ToolDescription string
	Parameters      map[string]any
	Temperature     float64
}

func marshalOpenAIToolRequest(opts openAIToolRequestOptions) ([]byte, error) {
	return json.Marshal(openAIToolRequest{
		Model: opts.Model,
		Messages: []openAIMessage{
			{Role: "system", Content: opts.SystemPrompt},
			{Role: "user", Content: opts.UserPrompt},
		},
		Tools: []openAITool{{
			Type: "function",
			Function: openAIFunctionDeclaration{
				Name:        opts.ToolName,
				Description: opts.ToolDescription,
				Parameters:  opts.Parameters,
			},
		}},
		ToolChoice: openAIToolChoice{
			Type:     "function",
			Function: openAIFunctionChoice{Name: opts.ToolName},
		},
		Temperature: opts.Temperature,
	})
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

	raw, err := marshalOpenAIToolRequest(openAIToolRequestOptions{
		Model:           model,
		SystemPrompt:    openAISystemPrompt(cc.CommitFormat),
		UserPrompt:      "Generate a commit message for this change:\n" + string(userJSON),
		ToolName:        "commit_message",
		ToolDescription: "Emit a single commit message for the change described.",
		Parameters:      openAICommitMessageParameters(cc.CommitFormat),
		Temperature:     0.3,
	})
	return raw, transform, err
}

func buildOpenAIIntentPlanRequest(model string, plannerReq IntentPlanRequest) ([]byte, error) {
	body, _, err := buildOpenAIIntentPlanRequestWithTrace(model, plannerReq)
	return body, err
}

func buildOpenAIIntentPlanRequestWithTrace(model string, plannerReq IntentPlanRequest) ([]byte, prompttrace.TransformMetadata, error) {
	plannerReq.CommitFormat = effectiveCommitFormat(plannerReq.CommitFormat)
	userPrompt, err := BuildIntentPlanUserPrompt(plannerReq)
	if err != nil {
		return nil, prompttrace.TransformMetadata{}, err
	}

	raw, err := marshalOpenAIToolRequest(openAIToolRequestOptions{
		Model:           model,
		SystemPrompt:    IntentPlannerSystemPrompt(plannerReq.CommitFormat),
		UserPrompt:      userPrompt,
		ToolName:        "capture_intent_plan",
		ToolDescription: "Select or defer every offered capture for the next commit.",
		Parameters:      openAIIntentPlanParameters(plannerReq.CommitFormat),
		Temperature:     0.2,
	})
	return raw, plannerReq.CapturedDiffTransform, err
}

func buildOpenAIIntentPlanV2Request(model string, plannerReq IntentPlanRequestV2) ([]byte, error) {
	body, _, err := buildOpenAIIntentPlanV2RequestWithTrace(model, plannerReq)
	return body, err
}

func buildOpenAIIntentPlanV2RequestWithTrace(model string, plannerReq IntentPlanRequestV2) ([]byte, prompttrace.TransformMetadata, error) {
	plannerReq.CommitFormat = effectiveCommitFormat(plannerReq.CommitFormat)
	userPrompt, err := BuildIntentPlanV2UserPrompt(plannerReq)
	if err != nil {
		return nil, prompttrace.TransformMetadata{}, err
	}

	raw, err := marshalOpenAIToolRequest(openAIToolRequestOptions{
		Model:           model,
		SystemPrompt:    IntentPlannerV2SystemPrompt(plannerReq.CommitFormat),
		UserPrompt:      userPrompt,
		ToolName:        "capture_intent_plan_v2",
		ToolDescription: "Assign every offered capture to a durable semantic candidate.",
		Parameters:      openAIIntentPlanV2Parameters(plannerReq.CommitFormat),
		Temperature:     0.2,
	})
	return raw, plannerReq.CapturedDiffTransform, err
}

func buildOpenAIIntentMessageRewriteRequest(model string, rewriteReq IntentMessageRewriteRequest) ([]byte, error) {
	body, _, err := buildOpenAIIntentMessageRewriteRequestWithTrace(model, rewriteReq)
	return body, err
}

// ProposeCommitRewrite POSTs a historical commit rewrite request and parses a
// replacement commit_message tool call.
func (p *OpenAIProvider) ProposeCommitRewrite(ctx context.Context, rewriteReq CommitRewriteRequest) (Result, error) {
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
	httpClient := p.HTTP
	if httpClient == nil {
		httpClient = defaultOpenAIClient()
	}
	body, err := buildOpenAICommitRewriteRequest(model, rewriteReq)
	if err != nil {
		return Result{}, fmt.Errorf("openai-compat: build commit rewrite request: %w", err)
	}
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
		return Result{}, fmt.Errorf("openai-compat: http: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Result{}, fmt.Errorf("openai-compat: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Result{}, fmt.Errorf("openai-compat: http %d: %s", resp.StatusCode, truncateForError(string(raw)))
	}
	subject, bodyOut, err := parseToolCall(raw)
	if err != nil {
		return Result{}, err
	}
	result, err := ValidateCommitRewriteProposal(rewriteReq, Result{Subject: subject, Body: bodyOut, Source: p.Name()})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func buildOpenAICommitRewriteRequest(model string, rewriteReq CommitRewriteRequest) ([]byte, error) {
	rewriteReq.CommitFormat = effectiveCommitFormat(rewriteReq.CommitFormat)
	userPrompt, err := BuildCommitRewriteUserPrompt(rewriteReq)
	if err != nil {
		return nil, err
	}
	return marshalOpenAIToolRequest(openAIToolRequestOptions{
		Model:           model,
		SystemPrompt:    "You rewrite existing git commit messages. Always call the commit_message function. " + CommitMessageFormatInstructions(rewriteReq.CommitFormat),
		UserPrompt:      userPrompt,
		ToolName:        "commit_message",
		ToolDescription: "Emit only the replacement commit message subject and body.",
		Parameters:      openAICommitMessageParameters(rewriteReq.CommitFormat),
		Temperature:     0.2,
	})
}

func buildOpenAIIntentMessageRewriteRequestWithTrace(model string, rewriteReq IntentMessageRewriteRequest) ([]byte, prompttrace.TransformMetadata, error) {
	rewriteReq.CommitFormat = firstCommitFormat(rewriteReq.CommitFormat, rewriteReq.PlannerRequest.CommitFormat)
	userPrompt, err := BuildIntentMessageRewriteUserPrompt(rewriteReq)
	if err != nil {
		return nil, prompttrace.TransformMetadata{}, err
	}

	raw, err := marshalOpenAIToolRequest(openAIToolRequestOptions{
		Model:           model,
		SystemPrompt:    "You rewrite git commit messages for already accepted intent plans. Always call the commit_message function. " + CommitMessageFormatInstructions(rewriteReq.CommitFormat),
		UserPrompt:      userPrompt,
		ToolName:        "commit_message",
		ToolDescription: "Emit only the replacement commit message subject and body.",
		Parameters:      openAICommitMessageParameters(rewriteReq.CommitFormat),
		Temperature:     0.2,
	})
	return raw, rewriteReq.PlannerRequest.CapturedDiffTransform, err
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
		if msg.ToolCalls[0].Function.Name != "commit_message" {
			return "", "", fmt.Errorf("openai-compat: unexpected tool %q", msg.ToolCalls[0].Function.Name)
		}
		args = msg.ToolCalls[0].Function.Arguments
	case msg.FunctionCall != nil && msg.FunctionCall.Arguments != "":
		if msg.FunctionCall.Name != "commit_message" {
			return "", "", fmt.Errorf("openai-compat: unexpected function %q", msg.FunctionCall.Name)
		}
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
		cause := fmt.Errorf("openai-compat: parse response: %w", err)
		return IntentPlan{}, intentPlanShapeValidationError(cause.Error(), cause)
	}
	if r.Error != nil && r.Error.Message != "" {
		return IntentPlan{}, fmt.Errorf("openai-compat: api error: %s", r.Error.Message)
	}
	if len(r.Choices) == 0 {
		cause := errors.New("openai-compat: no choices in response")
		return IntentPlan{}, intentPlanShapeValidationError(cause.Error(), cause)
	}
	msg := r.Choices[0].Message

	var args string
	switch {
	case len(msg.ToolCalls) > 0 && msg.ToolCalls[0].Function.Arguments != "":
		if msg.ToolCalls[0].Function.Name != "capture_intent_plan" {
			cause := fmt.Errorf("openai-compat: unexpected tool %q", msg.ToolCalls[0].Function.Name)
			return IntentPlan{}, intentPlanShapeValidationError(cause.Error(), cause)
		}
		args = msg.ToolCalls[0].Function.Arguments
	case msg.FunctionCall != nil && msg.FunctionCall.Arguments != "":
		if msg.FunctionCall.Name != "capture_intent_plan" {
			cause := fmt.Errorf("openai-compat: unexpected function %q", msg.FunctionCall.Name)
			return IntentPlan{}, intentPlanShapeValidationError(cause.Error(), cause)
		}
		args = msg.FunctionCall.Arguments
	default:
		cause := errors.New("openai-compat: response carried no intent-plan tool_call arguments")
		return IntentPlan{}, intentPlanShapeValidationError(cause.Error(), cause)
	}

	var plan IntentPlan
	if err := json.Unmarshal([]byte(args), &plan); err != nil {
		cause := fmt.Errorf("openai-compat: parse intent-plan tool arguments: %w", err)
		return IntentPlan{}, intentPlanShapeValidationError(cause.Error(), cause)
	}
	return plan, nil
}

func parseIntentPlanV2ToolCall(raw []byte, req IntentPlanRequestV2) (IntentPlanV2, error) {
	type fcall struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	type toolCall struct {
		Function fcall `json:"function"`
	}
	type message struct {
		ToolCalls    []toolCall `json:"tool_calls"`
		FunctionCall *fcall     `json:"function_call"`
	}
	type choice struct {
		Message message `json:"message"`
	}
	type respShape struct {
		Choices []choice `json:"choices"`
		Error   *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	var response respShape
	if err := json.Unmarshal(raw, &response); err != nil {
		return IntentPlanV2{}, v2ValidationError("", IntentAtomicityCohesion,
			"response_decode_failed", fmt.Sprintf("openai-compat: parse response: %v", err))
	}
	if response.Error != nil && response.Error.Message != "" {
		return IntentPlanV2{}, fmt.Errorf("openai-compat: api error: %s", response.Error.Message)
	}
	if len(response.Choices) == 0 {
		return IntentPlanV2{}, v2ValidationError("", IntentAtomicityCohesion,
			"response_choices_missing", "openai-compat: no choices in response")
	}
	msg := response.Choices[0].Message
	var args string
	switch {
	case len(msg.ToolCalls) > 0 && msg.ToolCalls[0].Function.Arguments != "":
		if msg.ToolCalls[0].Function.Name != "capture_intent_plan_v2" {
			return IntentPlanV2{}, v2ValidationError("", IntentAtomicityCohesion,
				"response_tool_invalid",
				fmt.Sprintf("openai-compat: unexpected tool %q", msg.ToolCalls[0].Function.Name))
		}
		args = msg.ToolCalls[0].Function.Arguments
	case msg.FunctionCall != nil && msg.FunctionCall.Arguments != "":
		if msg.FunctionCall.Name != "capture_intent_plan_v2" {
			return IntentPlanV2{}, v2ValidationError("", IntentAtomicityCohesion,
				"response_tool_invalid",
				fmt.Sprintf("openai-compat: unexpected function %q", msg.FunctionCall.Name))
		}
		args = msg.FunctionCall.Arguments
	default:
		return IntentPlanV2{}, v2ValidationError("", IntentAtomicityCohesion,
			"response_tool_missing", "openai-compat: response carried no v2 intent-plan tool_call arguments")
	}
	return DecodeIntentPlanV2([]byte(args), req)
}

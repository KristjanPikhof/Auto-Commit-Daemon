// OpenAI-compatible Finish exchanges use one structured tool call per request.
package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

// DefaultOpenAIBaseURL is the canonical OpenAI v1 endpoint root.
const DefaultOpenAIBaseURL = "https://api.openai.com/v1"

// DefaultOpenAIModel is the default model for OpenAI-compatible requests.
const DefaultOpenAIModel = config.DefaultOpenAIModel

// DefaultOpenAITimeout is the per-request HTTP timeout. The caller's ctx
// can shorten this; the field exists so a client without a deadline still
// gets bounded behaviour.
const DefaultOpenAITimeout = 15 * time.Second

// OpenAIProvider is the OpenAI-compatible HTTP provider. Zero value is
// usable: exchanges fill in the BaseURL/Model/HTTP defaults on first
// call. Once initialized the provider is concurrency-safe (the http
// client is the only mutable shared state, and net/http.Client is safe
// for concurrent use).
type OpenAIProvider struct {
	BaseURL string       // chat-completions root; defaults to DefaultOpenAIBaseURL
	APIKey  string       // bearer token; required (empty -> error from exchange)
	Model   string       // defaults to DefaultOpenAIModel
	HTTP    *http.Client // defaults to a redirect-refusing client w/ DefaultOpenAITimeout

}

// Name reports the configured transport identifier.
func (*OpenAIProvider) Name() string { return "openai-compat" }

func (p *OpenAIProvider) EncodeFinishRequest(request FinishPlanRequest) ([]byte, error) {
	data, err := json.Marshal(finishWireRequest(request))
	if err != nil {
		return nil, err
	}
	if request.Review != nil {
		return marshalOpenAIToolRequest(openAIToolRequestOptions{
			Model: p.resolvedModel(), SystemPrompt: finishPrompt(request), UserPrompt: string(data),
			ToolName: "finish_evidence", ToolDescription: "Review exact diff fragments for one later global commit plan",
			Parameters: map[string]any{"type": "object", "additionalProperties": false, "required": []string{"observations"},
				"properties": map[string]any{"observations": map[string]any{"type": "array", "items": map[string]any{
					"type": "object", "additionalProperties": false, "required": []string{"id", "summary"},
					"properties": map[string]any{"id": map[string]any{"type": "string"}, "summary": map[string]any{
						"type": "string", "pattern": fmt.Sprintf(`^[\s\S]{1,%d}$`, finishReviewSummaryRunes),
						"description": fmt.Sprintf("Factual summary of recorded behavior, dependencies, and corrections; use up to %d characters when needed.", finishReviewSummaryRunes),
					}},
				}}}},
		})
	}
	goal := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"purpose", "boundary", "message", "change_ids"},
		"properties": map[string]any{
			"purpose":    map[string]any{"type": "string"},
			"boundary":   map[string]any{"type": "string"},
			"message":    map[string]any{"type": "string"},
			"change_ids": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		},
	}
	return marshalOpenAIToolRequest(openAIToolRequestOptions{
		Model: p.resolvedModel(), SystemPrompt: finishPrompt(request), UserPrompt: string(data),
		ToolName: "finish_plan", ToolDescription: "Plan completed purposes, safe recorded changes, and grounded messages",
		Parameters: map[string]any{"type": "object", "additionalProperties": false, "required": []string{"goals"},
			"properties": map[string]any{"goals": map[string]any{"type": "array", "items": goal}}},
	})
}

func (p *OpenAIProvider) ExchangeFinishReviews(ctx context.Context, bodies [][]byte) ([][]byte, error) {
	return ExchangeFinishReviews(ctx, p, bodies)
}

// ExchangeFinish sends one HTTP request. Transport failures never retry or
// invoke another provider; malformed output is returned to the bounded planner.
func (p *OpenAIProvider) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	if strings.TrimSpace(p.APIKey) == "" {
		return nil, errors.New("openai-compat: missing API key")
	}
	baseURL, err := normalizeOpenAIBaseURL(p.BaseURL, false)
	if err != nil {
		return nil, err
	}
	endpoint, err := url.JoinPath(baseURL, "chat", "completions")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.APIKey)
	req.Header.Set("Accept", "application/json")
	client := p.HTTP
	if client == nil {
		client = defaultOpenAIClient()
	}
	// Do not let an injected client redirect credentials or turn one paid
	// exchange into several requests. Preserve its transport and timeout.
	single := *client
	single.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := single.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, subprocessResponseByteCap+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > subprocessResponseByteCap {
		return nil, errors.New("openai-compat: response exceeds 1 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &ProviderHTTPError{StatusCode: resp.StatusCode, Detail: finishHTTPErrorDetail(raw, p.APIKey)}
	}
	expectedTool := "finish_plan"
	var sent struct {
		ToolChoice openAIToolChoice `json:"tool_choice"`
	}
	if json.Unmarshal(body, &sent) == nil && sent.ToolChoice.Function.Name == "finish_evidence" {
		expectedTool = "finish_evidence"
	}
	arguments, err := parseFinishToolArgumentsFor(raw, expectedTool)
	if err != nil {
		return nil, &finishOutputError{Err: err, Raw: raw}
	}
	return arguments, nil
}

// Only use the standard error message, never echo an arbitrary response body.
func finishHTTPErrorDetail(raw []byte, apiKey string) string {
	var response struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &response) != nil || strings.TrimSpace(response.Error.Message) == "" {
		return "finish request rejected"
	}
	message := response.Error.Message
	if apiKey != "" {
		message = strings.ReplaceAll(message, apiKey, "[REDACTED]")
	}
	return SanitizePlannerError(finishRedactText(message))
}

func parseFinishToolArguments(raw []byte) ([]byte, error) {
	return parseFinishToolArgumentsFor(raw, "finish_plan")
}

func parseFinishToolArgumentsFor(raw []byte, expectedTool string) ([]byte, error) {
	var response struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("OpenAI response is not valid UTF-8")
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode OpenAI finish response: %w", err)
	}
	if len(response.Choices) != 1 || len(response.Choices[0].Message.ToolCalls) != 1 {
		return nil, errors.New("OpenAI finish response must contain exactly one choice and finish_plan tool call")
	}
	function := response.Choices[0].Message.ToolCalls[0].Function
	if function.Name != expectedTool || function.Arguments == "" {
		return nil, fmt.Errorf("OpenAI finish response requires %s arguments", expectedTool)
	}
	return []byte(function.Arguments), nil
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

// defaultOpenAIClient bounds requests and refuses redirects so credentials
// are not forwarded to another endpoint.
func defaultOpenAIClient() *http.Client {
	return &http.Client{
		Timeout: DefaultOpenAITimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIFunctionDeclaration struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Strict      bool           `json:"strict"`
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
				Strict:      true,
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

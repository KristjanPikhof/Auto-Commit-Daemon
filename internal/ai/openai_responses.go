package ai

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// Terra tool calls use Responses so planning retains its supported reasoning.
// Other compatible models retain their existing Chat Completions contract.
func marshalFinishToolRequest(opts openAIToolRequestOptions) ([]byte, error) {
	if opts.Model != "gpt-6.1-terra" {
		return marshalOpenAIToolRequest(opts)
	}
	effort := "high"
	if opts.ToolName == "finish_evidence" {
		effort = "medium"
	}
	return json.Marshal(struct {
		Model        string                   `json:"model"`
		Instructions string                   `json:"instructions"`
		Input        []openAIMessage          `json:"input"`
		Tools        []openAIResponseFunction `json:"tools"`
		ToolChoice   openAIResponseChoice     `json:"tool_choice"`
		Reasoning    map[string]string        `json:"reasoning"`
		Store        bool                     `json:"store"`
	}{
		Model: opts.Model, Instructions: opts.SystemPrompt,
		Input:      []openAIMessage{{Role: "user", Content: opts.UserPrompt}},
		Tools:      []openAIResponseFunction{{Type: "function", Name: opts.ToolName, Description: opts.ToolDescription, Parameters: opts.Parameters, Strict: true}},
		ToolChoice: openAIResponseChoice{Type: "function", Name: opts.ToolName},
		Reasoning:  map[string]string{"effort": effort}, Store: false,
	})
}

type openAIResponseFunction struct {
	Type        string         `json:"type"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
	Strict      bool           `json:"strict"`
}

type openAIResponseChoice struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

func parseFinishResponseArguments(raw []byte, expectedTool string) ([]byte, error) {
	var response struct {
		Status string `json:"status"`
		Output []struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"output"`
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("OpenAI response is not valid UTF-8")
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode OpenAI finish response: %w", err)
	}
	if response.Status != "completed" {
		return nil, errors.New("OpenAI finish response is not completed")
	}
	var arguments string
	for _, item := range response.Output {
		if item.Type == "reasoning" {
			continue
		}
		if item.Type != "function_call" || item.Name != expectedTool || item.Arguments == "" || arguments != "" {
			return nil, fmt.Errorf("OpenAI finish response requires exactly one %s function call", expectedTool)
		}
		arguments = item.Arguments
	}
	if arguments == "" {
		return nil, fmt.Errorf("OpenAI finish response requires %s arguments", expectedTool)
	}
	return []byte(arguments), nil
}

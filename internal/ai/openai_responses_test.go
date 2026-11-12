package ai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFinishTerraResponsesRetainReasoningAndEvidence(t *testing.T) {
	evidence, options, _ := finishSourceFixture()
	request, _, err := prepareFinishRequest(evidence, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, review := range []bool{false, true} {
		sent := request
		tool := "finish_plan"
		if review {
			sent = FinishPlanRequest{Style: request.Style, Review: &FinishReviewTask{Parts: []FinishReviewPart{{ID: "part-1", ChangeID: "camera.go", Text: request.Diffs[0].Text}}}}
			tool = "finish_evidence"
		}
		effort := "high"
		if review {
			effort = "medium"
		}
		calls := 0
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if r.URL.Path != "/responses" || r.Header.Get("Authorization") != "Bearer test-key" {
				t.Error("Responses changed endpoint ownership or credentials")
			}
			var wire struct {
				Model        string                   `json:"model"`
				Instructions string                   `json:"instructions"`
				Input        []openAIMessage          `json:"input"`
				Tools        []openAIResponseFunction `json:"tools"`
				ToolChoice   openAIResponseChoice     `json:"tool_choice"`
				Reasoning    map[string]string        `json:"reasoning"`
				Store        bool                     `json:"store"`
				Temperature  *float64                 `json:"temperature"`
			}
			if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
				t.Error(err)
				return
			}
			if wire.Model != "gpt-6.1-terra" || wire.Instructions != finishPrompt(sent) || wire.Reasoning["effort"] != effort || wire.Temperature != nil || wire.Store {
				t.Error("Terra lost its reasoning, prompt or privacy contract")
			}
			if len(wire.Tools) != 1 || !wire.Tools[0].Strict || wire.Tools[0].Name != tool || wire.ToolChoice.Name != tool || len(wire.Input) != 1 {
				t.Error("Responses lost strict tool selection")
				return
			}
			var decoded FinishPlanRequest
			if err := json.Unmarshal([]byte(wire.Input[0].Content), &decoded); err != nil {
				t.Error(err)
				return
			}
			if !review && (len(decoded.Diffs) != 1 || decoded.Diffs[0].Text != request.Diffs[0].Text || len(decoded.SourceEdits) != 0 || len(decoded.SelectionFiles) != 1) {
				t.Error("Responses lost exact source evidence or repeated coordinates")
			}
			if review && (decoded.Review == nil || len(decoded.Review.Parts) != 1 || decoded.Review.Parts[0].Text != request.Diffs[0].Text || len(decoded.Diffs) != 0) {
				t.Error("Responses changed the frozen review fragment")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed", "output": []any{
				map[string]any{"type": "reasoning", "summary": []any{}},
				map[string]any{"type": "function_call", "name": tool, "arguments": `{"result":"complete"}`},
			}})
		}))
		provider := &OpenAIProvider{BaseURL: server.URL, APIKey: "test-key", Model: "gpt-6.1-terra", HTTP: server.Client()}
		body, err := provider.EncodeFinishRequest(sent)
		if err != nil {
			t.Fatal(err)
		}
		arguments, err := provider.ExchangeFinish(t.Context(), body)
		server.Close()
		if err != nil || string(arguments) != `{"result":"complete"}` || calls != 1 {
			t.Fatalf("Responses exchange calls=%d output=%s error=%v", calls, arguments, err)
		}
	}
}

func TestFinishResponsesRejectIncompleteOrAmbiguousTools(t *testing.T) {
	for _, raw := range []string{
		`{"status":"incomplete","output":[{"type":"function_call","name":"finish_plan","arguments":"{}"}]}`,
		`{"status":"completed","output":[]}`,
		`{"status":"completed","output":[{"type":"function_call","name":"other","arguments":"{}"}]}`,
		`{"status":"completed","output":[{"type":"function_call","name":"finish_plan","arguments":"{}"},{"type":"function_call","name":"finish_plan","arguments":"{}"}]}`,
		`{"status":"completed","output":[{"type":"message","content":[]},{"type":"function_call","name":"finish_plan","arguments":"{}"}]}`,
		`{"choices":[{"message":{"tool_calls":[{"function":{"name":"finish_plan","arguments":"{}"}}]}}]}`,
		string([]byte{255}),
	} {
		if _, err := parseFinishResponseArguments([]byte(raw), "finish_plan"); err == nil {
			t.Fatalf("accepted incomplete or ambiguous response: %s", raw)
		}
	}
}

func TestFinishTerraResponsesDoesNotFallBackAfterRejection(t *testing.T) {
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/responses" {
			t.Error("provider silently changed API")
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"message":"Responses unsupported"}}`))
	}))
	defer server.Close()
	provider := &OpenAIProvider{BaseURL: server.URL, APIKey: "test-key", Model: "gpt-6.1-terra", HTTP: server.Client()}
	body, err := provider.EncodeFinishRequest(FinishPlanRequest{Style: "imperative"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = provider.ExchangeFinish(t.Context(), body); err == nil || !strings.Contains(err.Error(), "404") || calls != 1 {
		t.Fatalf("rejection made calls=%d error=%v", calls, err)
	}
}

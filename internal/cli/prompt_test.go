package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPromptNoTraceHumanAndJSONAreReadOnly(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	registerRepo(t, roots, repo, stateDB, "test")

	var human bytes.Buffer
	if err := runPrompt(context.Background(), &human, repo, true, 0, false); err != nil {
		t.Fatalf("runPrompt human: %v", err)
	}
	if got := human.String(); !strings.Contains(got, "No prompt traces found") ||
		!strings.Contains(got, "ACD_AI_PROMPT_TRACE=1") {
		t.Fatalf("human no trace output missing guidance:\n%s", got)
	}
	traceDir := filepath.Join(gitDirFromStateDB(stateDB), "acd", "prompt-trace")
	if _, err := os.Stat(traceDir); !os.IsNotExist(err) {
		t.Fatalf("trace dir stat err=%v, want not exist", err)
	}

	var raw bytes.Buffer
	if err := runPrompt(context.Background(), &raw, repo, true, 0, true); err != nil {
		t.Fatalf("runPrompt json: %v", err)
	}
	var report promptReport
	if err := json.Unmarshal(raw.Bytes(), &report); err != nil {
		t.Fatalf("json output invalid: %v\n%s", err, raw.String())
	}
	if report.Found || report.Trace != nil || !strings.Contains(report.Message, "No prompt traces found") {
		t.Fatalf("json no trace report = %+v", report)
	}
}

func TestPromptEventTraceHumanShowsRequestAndResponse(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	registerRepo(t, roots, repo, stateDB, "test")
	writePromptTrace(t, stateDB,
		map[string]any{
			"ts":            "2026-05-04T12:00:00Z",
			"stage":         "request",
			"strategy":      "event",
			"provider":      "openai-compatible",
			"model":         "gpt-4o-mini",
			"seq":           42,
			"branch_ref":    "refs/heads/main",
			"generation":    1,
			"diff_included": true,
			"diff_cap":      1048576,
			"transform": map[string]any{
				"redaction_applied": true,
				"truncated":         false,
				"input_bytes":       200,
				"redacted_bytes":    180,
				"output_bytes":      180,
			},
			"system_message": "system event prompt",
			"user_message":   "user event prompt",
			"tool_schema":    map[string]any{"type": "object"},
			"request":        json.RawMessage(`{"model":"gpt-4o-mini","messages":[{"role":"user","content":"user event prompt"}]}`),
		},
		map[string]any{
			"ts":            "2026-05-04T12:00:01Z",
			"stage":         "response",
			"strategy":      "event",
			"provider":      "openai-compatible",
			"model":         "gpt-4o-mini",
			"seq":           42,
			"branch_ref":    "refs/heads/main",
			"generation":    1,
			"diff_included": true,
			"diff_cap":      1048576,
			"response": map[string]any{
				"status_code": 200,
				"subject":     "Add prompt command",
				"body":        "Expose trace details.",
			},
		},
	)

	var out bytes.Buffer
	if err := runPrompt(context.Background(), &out, repo, false, 42, false); err != nil {
		t.Fatalf("runPrompt: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Strategy: event",
		"Provider: openai-compatible",
		"Model: gpt-4o-mini",
		"Seq: 42",
		"Diff: included=true",
		"Validation/fallback: ok",
		"System prompt:",
		"system event prompt",
		"User prompt:",
		"user event prompt",
		"Tool schema:",
		"Request envelope:",
		"Response:",
		"Add prompt command",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("event prompt output missing %q:\n%s", want, got)
		}
	}
}

func TestPromptReadsTraceWhenRegisteredStateDBIsMissing(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	registerRepo(t, roots, repo, stateDB, "test")
	writePromptTrace(t, stateDB,
		map[string]any{
			"ts":             "2026-05-04T12:00:00Z",
			"stage":          "request",
			"strategy":       "event",
			"provider":       "deterministic",
			"seq":            42,
			"system_message": "system prompt",
			"user_message":   "user prompt",
		},
		map[string]any{
			"ts":       "2026-05-04T12:00:01Z",
			"stage":    "response",
			"strategy": "event",
			"provider": "deterministic",
			"seq":      42,
			"response": map[string]any{
				"subject": "Trace without state db",
			},
		},
	)
	if err := os.Remove(stateDB); err != nil {
		t.Fatalf("remove state.db: %v", err)
	}

	var out bytes.Buffer
	if err := runPrompt(context.Background(), &out, repo, false, 42, true); err != nil {
		t.Fatalf("runPrompt: %v", err)
	}
	var report promptReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("json output invalid: %v\n%s", err, out.String())
	}
	if !report.Found || report.Trace == nil || report.Trace.Response == nil {
		t.Fatalf("missing-state trace not found: %+v", report)
	}
	if report.Trace.Response.Subject != "Trace without state db" {
		t.Fatalf("response subject=%q", report.Trace.Response.Subject)
	}
	if _, err := os.Stat(stateDB); !os.IsNotExist(err) {
		t.Fatalf("state.db stat err=%v, want still missing", err)
	}
}

func TestPromptIntentTraceJSONShowsOfferedSeqAndFallback(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	registerRepo(t, roots, repo, stateDB, "test")
	writePromptTrace(t, stateDB,
		map[string]any{
			"ts":             "2026-05-04T13:00:00Z",
			"stage":          "request",
			"strategy":       "intent",
			"provider":       "openai-compatible",
			"model":          "gpt-4o-mini",
			"offered_seqs":   []int64{100, 101},
			"branch_ref":     "refs/heads/main",
			"generation":     2,
			"diff_included":  false,
			"system_message": "system intent prompt",
			"user_message":   "user intent prompt",
			"request":        json.RawMessage(`{"model":"gpt-4o-mini","messages":[]}`),
		},
		map[string]any{
			"ts":            "2026-05-04T13:00:01Z",
			"stage":         "response",
			"strategy":      "intent",
			"provider":      "openai-compatible",
			"model":         "gpt-4o-mini",
			"offered_seqs":  []int64{100, 101},
			"branch_ref":    "refs/heads/main",
			"generation":    2,
			"diff_included": false,
			"response": map[string]any{
				"status_code":      200,
				"subject":          "Group CLI prompt work",
				"selected_seqs":    []int64{100},
				"deferred_seqs":    []int64{101},
				"grouping_reason":  "same command",
				"validation_error": "selected stale seq",
			},
		},
		map[string]any{
			"ts":            "2026-05-04T13:00:02Z",
			"stage":         "fallback",
			"strategy":      "intent",
			"provider":      "openai-compatible+deterministic",
			"model":         "gpt-4o-mini",
			"offered_seqs":  []int64{100, 101},
			"branch_ref":    "refs/heads/main",
			"generation":    2,
			"diff_included": false,
			"response": map[string]any{
				"fallback_provider": "deterministic",
				"fallback_reason":   "selected stale seq",
			},
		},
	)

	var out bytes.Buffer
	if err := runPrompt(context.Background(), &out, repo, false, 101, true); err != nil {
		t.Fatalf("runPrompt: %v", err)
	}
	var report promptReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("json output invalid: %v\n%s", err, out.String())
	}
	if !report.Found || report.Trace == nil {
		t.Fatalf("intent trace not found: %+v", report)
	}
	trace := report.Trace
	if trace.Strategy != "intent" || trace.ValidationState != "validation_error" {
		t.Fatalf("trace strategy/state = %s/%s", trace.Strategy, trace.ValidationState)
	}
	if len(trace.OfferedSeqs) != 2 || trace.OfferedSeqs[0] != 100 || trace.OfferedSeqs[1] != 101 {
		t.Fatalf("offered seqs = %v", trace.OfferedSeqs)
	}
	if trace.Response == nil || len(trace.Response.SelectedSeqs) != 1 || trace.Response.SelectedSeqs[0] != 100 {
		t.Fatalf("response = %+v", trace.Response)
	}
	if trace.Fallback == nil || trace.Fallback.FallbackProvider != "deterministic" {
		t.Fatalf("fallback = %+v", trace.Fallback)
	}
}

func TestPromptRepeatedSameKeyAttemptsDoNotHybridize(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	registerRepo(t, roots, repo, stateDB, "test")
	writePromptTrace(t, stateDB,
		map[string]any{
			"ts":             "2026-05-04T14:00:00Z",
			"stage":          "request",
			"strategy":       "event",
			"provider":       "openai-compatible",
			"model":          "gpt-4o-mini",
			"seq":            42,
			"branch_ref":     "refs/heads/main",
			"generation":     1,
			"system_message": "first system",
			"user_message":   "first attempt",
		},
		map[string]any{
			"ts":         "2026-05-04T14:00:01Z",
			"stage":      "response",
			"strategy":   "event",
			"provider":   "openai-compatible",
			"model":      "gpt-4o-mini",
			"seq":        42,
			"branch_ref": "refs/heads/main",
			"generation": 1,
			"response": map[string]any{
				"subject": "First attempt response",
			},
		},
		map[string]any{
			"ts":             "2026-05-04T14:00:02Z",
			"stage":          "request",
			"strategy":       "event",
			"provider":       "openai-compatible",
			"model":          "gpt-4o-mini",
			"seq":            42,
			"branch_ref":     "refs/heads/main",
			"generation":     1,
			"system_message": "second system",
			"user_message":   "second attempt",
		},
		map[string]any{
			"ts":         "2026-05-04T14:00:03Z",
			"stage":      "fallback",
			"strategy":   "event",
			"provider":   "openai-compatible",
			"model":      "gpt-4o-mini",
			"seq":        42,
			"branch_ref": "refs/heads/main",
			"generation": 1,
			"response": map[string]any{
				"fallback_provider": "deterministic",
				"fallback_reason":   "second attempt failed",
			},
		},
	)

	for _, tc := range []struct {
		name string
		last bool
	}{
		{name: "seq", last: false},
		{name: "last", last: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := runPrompt(context.Background(), &out, repo, tc.last, 42, true); err != nil {
				t.Fatalf("runPrompt: %v", err)
			}
			var report promptReport
			if err := json.Unmarshal(out.Bytes(), &report); err != nil {
				t.Fatalf("json output invalid: %v\n%s", err, out.String())
			}
			if !report.Found || report.Trace == nil {
				t.Fatalf("trace not found: %+v", report)
			}
			if report.Trace.UserPrompt != "second attempt" {
				t.Fatalf("user prompt=%q", report.Trace.UserPrompt)
			}
			if report.Trace.Response != nil {
				t.Fatalf("unexpected first-attempt response in latest attempt: %+v", report.Trace.Response)
			}
			if report.Trace.Fallback == nil || report.Trace.Fallback.FallbackReason != "second attempt failed" {
				t.Fatalf("fallback=%+v", report.Trace.Fallback)
			}
			if got := strings.Join(report.Trace.Stages, ","); got != "request,fallback" {
				t.Fatalf("stages=%s", got)
			}
		})
	}
}

func TestPromptSeqLookupMiss(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	registerRepo(t, roots, repo, stateDB, "test")
	writePromptTrace(t, stateDB, map[string]any{
		"ts":       "2026-05-04T12:00:00Z",
		"stage":    "request",
		"strategy": "event",
		"provider": "deterministic",
		"seq":      7,
	})

	var out bytes.Buffer
	if err := runPrompt(context.Background(), &out, repo, false, 999, false); err != nil {
		t.Fatalf("runPrompt: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "No prompt trace found for seq 999") {
		t.Fatalf("seq miss output missing clear text:\n%s", got)
	}
}

func writePromptTrace(t *testing.T, stateDB string, records ...map[string]any) {
	t.Helper()
	path := filepath.Join(gitDirFromStateDB(stateDB), "acd", "prompt-trace", "2026-05-04.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir prompt trace: %v", err)
	}
	var lines []string
	for _, record := range records {
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshal prompt trace: %v", err)
		}
		lines = append(lines, string(raw))
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write prompt trace: %v", err)
	}
}

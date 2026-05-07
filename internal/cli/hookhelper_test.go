package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestHookStdinExtract_PrintsStringField(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(strings.NewReader(`{"session_id":"sess-123","other":1}`), &out, "session_id")
	if err != nil {
		t.Fatalf("runHookStdinExtract: %v", err)
	}
	if got := out.String(); got != "sess-123\n" {
		t.Fatalf("output=%q want sess-123 newline", got)
	}
}

func TestHookStdinExtract_PrintsScalarFields(t *testing.T) {
	tests := []struct {
		name  string
		input string
		field string
		want  string
	}{
		{name: "number", input: `{"seq":42}`, field: "seq", want: "42\n"},
		{name: "bool", input: `{"ok":true}`, field: "ok", want: "true\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := runHookStdinExtract(strings.NewReader(tc.input), &out, tc.field); err != nil {
				t.Fatalf("runHookStdinExtract: %v", err)
			}
			if got := out.String(); got != tc.want {
				t.Fatalf("output=%q want %q", got, tc.want)
			}
		})
	}
}

func TestHookStdinExtract_MissingFieldErrors(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(strings.NewReader(`{"other":"x"}`), &out, "session_id")
	if err == nil {
		t.Fatal("expected missing field error")
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on error: %q", out.String())
	}
}

func TestHookStdinExtract_ObjectFieldErrors(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(strings.NewReader(`{"session_id":{"nested":"x"}}`), &out, "session_id")
	if err == nil {
		t.Fatal("expected object field error")
	}
}

func TestHookStdinExtract_MultiFieldEmitsNewlineSeparated(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7","cwd":"/tmp/repo","other":1}`),
		&out,
		"session_id", "cwd",
	)
	if err != nil {
		t.Fatalf("runHookStdinExtract: %v", err)
	}
	if got := out.String(); got != "sess-7\n/tmp/repo\n" {
		t.Fatalf("output=%q want session_id then cwd, newline-separated", got)
	}
}

func TestHookStdinExtract_MultiFieldRespectsArgOrder(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7","cwd":"/tmp/repo"}`),
		&out,
		"cwd", "session_id",
	)
	if err != nil {
		t.Fatalf("runHookStdinExtract: %v", err)
	}
	if got := out.String(); got != "/tmp/repo\nsess-7\n" {
		t.Fatalf("output=%q want cwd first, then session_id", got)
	}
}

func TestHookStdinExtract_MultiFieldStopsOnMissing(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7"}`),
		&out,
		"session_id", "cwd",
	)
	if err == nil {
		t.Fatal("expected missing field error on second arg")
	}
	if !strings.Contains(err.Error(), "cwd") {
		t.Fatalf("error should mention missing field cwd: %v", err)
	}
	// First field already wrote before second field error surfaced;
	// callers redirect stderr separately and rely on non-zero exit.
	if got := out.String(); got != "sess-7\n" {
		t.Fatalf("partial stdout=%q want sess-7 line before failure", got)
	}
}

func TestHookStdinExtract_MultiFieldNonScalarErrors(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7","cwd":["a","b"]}`),
		&out,
		"session_id", "cwd",
	)
	if err == nil {
		t.Fatal("expected non-scalar error")
	}
}

func TestHookStdinExtract_NoFieldsErrors(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(strings.NewReader(`{}`), &out)
	if err == nil {
		t.Fatal("expected error when no fields supplied")
	}
}

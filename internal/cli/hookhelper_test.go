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

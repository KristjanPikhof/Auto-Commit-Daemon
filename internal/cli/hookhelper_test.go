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
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on object-field error: %q", out.String())
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
	// Required-field outputs are buffered: a missing required field at any
	// position must leave stdout untouched so `|| exit 0` short-circuits
	// cleanly in the bash hook bodies.
	if got := out.String(); got != "" {
		t.Fatalf("partial stdout=%q want empty (buffered until full success)", got)
	}
}

func TestHookStdinExtract_OptionalMissingFieldEmitsBlankLine(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7"}`),
		&out,
		"session_id", "cwd?",
	)
	if err != nil {
		t.Fatalf("runHookStdinExtract: %v", err)
	}
	if got := out.String(); got != "sess-7\n\n" {
		t.Fatalf("output=%q want session_id then blank cwd line", got)
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
	// Non-scalar required field must not leak the earlier session_id either.
	if got := out.String(); got != "" {
		t.Fatalf("partial stdout=%q want empty on non-scalar required field", got)
	}
}

func TestHookStdinExtract_NoFieldsErrors(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(strings.NewReader(`{}`), &out)
	if err == nil {
		t.Fatal("expected error when no fields supplied")
	}
}

// --- newline / NUL injection rejection ----------------------------------

func TestHookStdinExtract_RejectsLFInRequiredField(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7\nrm -rf /"}`),
		&out,
		"session_id",
	)
	if err == nil {
		t.Fatal("expected LF-injection error")
	}
	if !strings.Contains(err.Error(), "session_id") || !strings.Contains(err.Error(), "0x0a") {
		t.Fatalf("error should name field and offending byte: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on LF rejection: %q", out.String())
	}
}

func TestHookStdinExtract_RejectsCRInRequiredField(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"cwd":"/tmp/repo\r/etc/passwd"}`),
		&out,
		"cwd",
	)
	if err == nil {
		t.Fatal("expected CR-injection error")
	}
	if !strings.Contains(err.Error(), "cwd") || !strings.Contains(err.Error(), "0x0d") {
		t.Fatalf("error should name field and CR byte: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on CR rejection: %q", out.String())
	}
}

func TestHookStdinExtract_RejectsNULInRequiredField(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader("{\"session_id\":\"sess\\u0000-7\"}"),
		&out,
		"session_id",
	)
	if err == nil {
		t.Fatal("expected NUL-injection error")
	}
	if !strings.Contains(err.Error(), "session_id") || !strings.Contains(err.Error(), "0x00") {
		t.Fatalf("error should name field and NUL byte: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on NUL rejection: %q", out.String())
	}
}

func TestHookStdinExtract_RejectsLFInOptionalField(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7","cwd":"/tmp\nattacker"}`),
		&out,
		"session_id", "cwd?",
	)
	if err == nil {
		t.Fatal("expected LF-injection error on optional field")
	}
	if !strings.Contains(err.Error(), "cwd") {
		t.Fatalf("error should name optional field cwd: %v", err)
	}
	// Buffered: even though session_id resolved cleanly, the failure on
	// cwd must keep stdout empty.
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on optional-field LF rejection: %q", out.String())
	}
}

// --- empty-string required collapses to missing -------------------------

func TestHookStdinExtract_EmptyStringRequiredFieldErrors(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":""}`),
		&out,
		"session_id",
	)
	if err == nil {
		t.Fatal("expected empty-string required field to surface as missing")
	}
	if !strings.Contains(err.Error(), "not found") || !strings.Contains(err.Error(), "session_id") {
		t.Fatalf("error should mirror field-not-found wording: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on empty-required: %q", out.String())
	}
}

func TestHookStdinExtract_EmptyStringRequiredAfterValid(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7","cwd":""}`),
		&out,
		"session_id", "cwd",
	)
	if err == nil {
		t.Fatal("expected empty cwd required to surface as missing")
	}
	if !strings.Contains(err.Error(), "cwd") {
		t.Fatalf("error should name cwd: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("partial stdout=%q want empty (buffered)", out.String())
	}
}

func TestHookStdinExtract_EmptyStringOptionalFieldEmitsBlankLine(t *testing.T) {
	var out bytes.Buffer
	err := runHookStdinExtract(
		strings.NewReader(`{"session_id":"sess-7","cwd":""}`),
		&out,
		"session_id", "cwd?",
	)
	if err != nil {
		t.Fatalf("runHookStdinExtract: %v", err)
	}
	if got := out.String(); got != "sess-7\n\n" {
		t.Fatalf("output=%q want session_id line then blank optional cwd", got)
	}
}

// --- 1 MiB stdin truncation surfaces distinctly --------------------------

func TestHookStdinExtract_StdinTruncationIsDistinct(t *testing.T) {
	// Build a 1.5 MiB payload of valid JSON-ish noise that will be cut off
	// inside the value before the closing brace.
	const oversize = 1024*1024 + 512*1024
	var sb strings.Builder
	sb.Grow(oversize + 64)
	sb.WriteString(`{"session_id":"`)
	for sb.Len() < oversize {
		sb.WriteByte('a')
	}
	sb.WriteString(`"}`)

	var out bytes.Buffer
	err := runHookStdinExtract(strings.NewReader(sb.String()), &out, "session_id")
	if err == nil {
		t.Fatal("expected truncation error on oversized stdin")
	}
	msg := err.Error()
	if !strings.Contains(msg, "1 MiB") && !strings.Contains(msg, "1048576") {
		t.Fatalf("error should mention 1 MiB / 1048576 limit, got: %v", err)
	}
	if strings.Contains(msg, "decode stdin JSON") {
		t.Fatalf("error must be distinct from generic decode failure, got: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on truncation: %q", out.String())
	}
}

func TestHookCursorExtract_SampleCursorStdin(t *testing.T) {
	requireGitForCLIResolverTest(t)
	repo := initCLIResolverRepo(t)
	nested := filepath.Join(repo, "pkg")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	payload, err := json.Marshal(map[string]any{
		"conversation_id": "conv-9f3a2b1c",
		"workspace_roots": []string{"/not/a/repo", nested},
		"cwd":             "/ignored/when/roots/git",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var out bytes.Buffer
	if err := runHookCursorExtract(strings.NewReader(string(payload)), &out); err != nil {
		t.Fatalf("runHookCursorExtract: %v", err)
	}
	wantRepo := canonicalCLIResolverTestPath(t, repo)
	if got := out.String(); got != "conv-9f3a2b1c\n"+wantRepo+"\n" {
		t.Fatalf("output=%q want conversation_id and git toplevel from first resolvable workspace root", got)
	}
}

func TestHookCursorExtract_EmptyWorkspaceRootsUsesCwd(t *testing.T) {
	const cwd = "/tmp/cursor-hook-cwd"
	payload := `{"conversation_id":"conv-cwd","workspace_roots":[],"cwd":"` + cwd + `"}`

	var out bytes.Buffer
	if err := runHookCursorExtract(strings.NewReader(payload), &out); err != nil {
		t.Fatalf("runHookCursorExtract: %v", err)
	}
	if got := out.String(); got != "conv-cwd\n"+cwd+"\n" {
		t.Fatalf("output=%q want cwd when workspace_roots is empty", got)
	}
}

func TestHookCursorExtract_NonGitCwdWhenRootsUnresolved(t *testing.T) {
	requireGitForCLIResolverTest(t)
	gitRepo := initCLIResolverRepo(t)
	nonGit := t.TempDir()

	payload, err := json.Marshal(map[string]any{
		"conversation_id": "conv-nogit",
		"workspace_roots": []string{nonGit},
		"cwd":             nonGit,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var out bytes.Buffer
	if err := runHookCursorExtract(strings.NewReader(string(payload)), &out); err != nil {
		t.Fatalf("runHookCursorExtract: %v", err)
	}
	wantCWD := canonicalCLIResolverTestPath(t, nonGit)
	if got := out.String(); got != "conv-nogit\n"+wantCWD+"\n" {
		t.Fatalf("output=%q want non-git cwd when workspace_roots has no git root", got)
	}

	// Sanity: a resolvable git root in workspace_roots still wins over cwd.
	payload2, err := json.Marshal(map[string]any{
		"conversation_id": "conv-git",
		"workspace_roots": []string{nonGit, gitRepo},
		"cwd":             nonGit,
	})
	if err != nil {
		t.Fatalf("marshal payload2: %v", err)
	}
	out.Reset()
	if err := runHookCursorExtract(strings.NewReader(string(payload2)), &out); err != nil {
		t.Fatalf("runHookCursorExtract git root: %v", err)
	}
	wantRepo := canonicalCLIResolverTestPath(t, gitRepo)
	if got := out.String(); got != "conv-git\n"+wantRepo+"\n" {
		t.Fatalf("output=%q want first resolvable git root in workspace_roots", got)
	}
}

func TestHookCursorExtract_FallsBackToProcessCwd(t *testing.T) {
	requireGitForCLIResolverTest(t)
	repo := initCLIResolverRepo(t)
	chdirForTest(t, repo)

	payload := `{"conversation_id":"conv-pwd"}`
	var out bytes.Buffer
	if err := runHookCursorExtract(strings.NewReader(payload), &out); err != nil {
		t.Fatalf("runHookCursorExtract: %v", err)
	}
	wantRepo := canonicalCLIResolverTestPath(t, repo)
	if got := out.String(); got != "conv-pwd\n"+wantRepo+"\n" {
		t.Fatalf("output=%q want process cwd when roots and cwd are absent", got)
	}
}

func TestHookCursorExtract_MissingConversationIDErrors(t *testing.T) {
	var out bytes.Buffer
	err := runHookCursorExtract(strings.NewReader(`{"workspace_roots":[]}`), &out)
	if err == nil {
		t.Fatal("expected missing conversation_id error")
	}
	if !strings.Contains(err.Error(), "conversation_id") {
		t.Fatalf("error should mention conversation_id: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected stdout on error: %q", out.String())
	}
}

func TestHookCursorExtract_ResolveCursorHookRepoDirect(t *testing.T) {
	requireGitForCLIResolverTest(t)
	repo := initCLIResolverRepo(t)
	got, err := resolveCursorHookRepo(context.Background(), map[string]any{
		"workspace_roots": []any{repo},
	})
	if err != nil {
		t.Fatalf("resolveCursorHookRepo: %v", err)
	}
	want := canonicalCLIResolverTestPath(t, repo)
	if got != want {
		t.Fatalf("repo=%q want %q", got, want)
	}
}

func TestHookCursorExtract_WorkspaceRootCanonicalizesGitToplevel(t *testing.T) {
	requireGitForCLIResolverTest(t)
	repo := initCLIResolverRepo(t)
	linkParent := t.TempDir()
	link := filepath.Join(linkParent, "repo-link")
	if err := os.Symlink(repo, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	payload, err := json.Marshal(map[string]any{
		"conversation_id": "conv-symlink",
		"workspace_roots": []string{link},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var out bytes.Buffer
	if err := runHookCursorExtract(strings.NewReader(string(payload)), &out); err != nil {
		t.Fatalf("runHookCursorExtract: %v", err)
	}
	wantRepo := canonicalCLIResolverTestPath(t, repo)
	if got := out.String(); got != "conv-symlink\n"+wantRepo+"\n" {
		t.Fatalf("output=%q want symlinked workspace root to match canonical git toplevel %q", got, wantRepo)
	}
}

func TestHookStdinExtract_DecodeErrorWordingUnchangedForSmallPayload(t *testing.T) {
	// Sanity: small malformed payload still surfaces as a decode error,
	// not as a truncation error, so the two paths stay distinguishable.
	var out bytes.Buffer
	err := runHookStdinExtract(strings.NewReader(`{"session_id":`), &out, "session_id")
	if err == nil {
		t.Fatal("expected decode error on malformed JSON")
	}
	if !strings.Contains(err.Error(), "decode stdin JSON") {
		t.Fatalf("error should mention decode failure: %v", err)
	}
	if strings.Contains(err.Error(), "1 MiB") {
		t.Fatalf("small-payload decode error must not mention 1 MiB limit: %v", err)
	}
}

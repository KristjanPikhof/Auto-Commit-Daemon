package daemon

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestSourceScopeKeepsReceiverIdentity(t *testing.T) {
	code := "package p\ntype A struct{}\ntype B struct{}\nfunc (A) value() int {\n return 1\n}\nfunc (B) value() int {\n return 1\n}\n"
	keys, err := finishSourceScopes(t.Context(), t.TempDir(), "scope.go", []byte(code))
	if err != nil || keys[4] == keys[7] {
		t.Fatalf("identical methods matched across receivers: %v %v", keys, err)
	}
}

func TestSourceScopeLabelsStayLocal(t *testing.T) {
	f, _ := finishWorkerFixture(t)
	before := "package p\nfunc selectCamera(secret string) int {\n if secret == \"UNCHANGED_SCOPE_SECRET\" {\n" + strings.Repeat(" // unchanged context\n", 10) + " return 1\n }\n return 0\n}\n"
	after := strings.Replace(before, "return 1", "return 2", 1)
	keys, err := finishSourceScopes(t.Context(), t.TempDir(), "scope.go", []byte(before))
	if err != nil || !strings.Contains(strings.Join(keys, "\n"), "UNCHANGED_SCOPE_SECRET") {
		t.Fatalf("fixture did not exercise an enclosing private expression: %v", err)
	}
	file, text, err := finishSourceDiff(context.Background(), f.dir, t.TempDir(), finish.Change{ID: "scope.go", Path: "scope.go"}, []byte(before), []byte(after), "edit-", new(int), 128<<10)
	if err != nil || len(file.Edits) == 0 || strings.Contains(text, "UNCHANGED_SCOPE_SECRET") {
		t.Fatalf("scope metadata escaped into diff evidence: %s %v", text, err)
	}
}

func TestSourceScopeOutputCannotBypassLimit(t *testing.T) {
	var output sourceScopeOutput
	_, err := io.Copy(&output, io.LimitReader(strings.NewReader(strings.Repeat("x", 9<<20)), 9<<20))
	if err == nil || len(output.String()) > 8<<20 {
		t.Fatalf("compiler output escaped its byte cap: %d, %v", len(output.String()), err)
	}
}

func TestSourceScopeIgnoresCompilerStderr(t *testing.T) {
	dir := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\\n' '(source_file)'\nprintf '%s\\n' '(func_decl range=[scope.swift:1:1 - line:2:1] \"PRIVATE_STDERR_SCOPE\")' >&2\n"
	if err := os.WriteFile(filepath.Join(dir, "xcrun"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	keys, err := finishSourceScopes(t.Context(), dir, "scope.swift", []byte("func run() {\n}\n"))
	if err != nil || strings.Contains(strings.Join(keys, "\n"), "PRIVATE_STDERR_SCOPE") {
		t.Fatalf("compiler diagnostics altered scope identity: %v %v", keys, err)
	}
}

package daemon

import (
	"fmt"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestIntentGoImportReferenceContextRetainsActualProductionImports(t *testing.T) {
	t.Parallel()
	contents := "package daemon\n" +
		"import named \"example.org/project/internal/ai\"\n" +
		"import (\n\t\"fmt\"\n\t_ \"example.org/project/internal/state\" // actual registration\n" +
		"\t. `example.org/project/internal/git`\n\t\"example.org/project/internal/testonly\"\n)\n"
	paths := []string{"internal/ai/planner.go", "internal/state/schema.go", "internal/git/refs.go", "internal/testonly/helper_test.go"}
	got := intentGoImportReferenceContext("internal/daemon/daemon_test.go", contents, paths)
	want := " import named \"example.org/project/internal/ai\"\n import (\n \t_ \"example.org/project/internal/state\"\n \t. `example.org/project/internal/git`\n )\n"
	if got != want {
		t.Fatalf("import evidence=%q want=%q", got, want)
	}
	_, imports := intentSourcePathReferences(got)
	for _, target := range paths[:3] {
		if !intentSourceImports(imports, "internal/daemon/daemon_test.go", target) {
			t.Fatalf("complete import context did not retain package relationship to %s", target)
		}
	}
	if intentSourceImports(imports, "internal/daemon/daemon_test.go", paths[3]) {
		t.Fatal("ordinary package import consumed a test-only offered target")
	}
}

func TestIntentGoImportReferenceContextRejectsUnprovedReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, source, contents, target string }{
		{"comment", "main.go", "package main\n// import \"example.org/project/internal/ai\"\n", "internal/ai/planner.go"},
		{"block_comment", "main.go", "package main\n/* import (\n\"example.org/project/internal/ai\"\n) */\n", "internal/ai/planner.go"},
		{"raw_string", "main.go", "package main\nvar example = `import \"example.org/project/internal/ai\"`\n", "internal/ai/planner.go"},
		{"quoted_string", "main.go", "package main\nvar example = \"import \\\"example.org/project/internal/ai\\\"\"\n", "internal/ai/planner.go"},
		{"comment_in_block", "main.go", "package main\nimport (\n\"fmt\"\n// \"example.org/project/internal/ai\"\n)\n", "internal/ai/planner.go"},
		{"different_package", "main.go", "package main\nimport \"example.org/project/internal/ai_backup\"\n", "internal/ai/planner.go"},
		{"partial_package", "main.go", "package main\nimport \"example.org/project/otherinternal/ai\"\n", "internal/ai/planner.go"},
		{"test_only", "main.go", "package main\nimport \"example.org/project/internal/ai\"\n", "internal/ai/planner_test.go"},
		{"other_extension", "main.md", "package main\nimport \"example.org/project/internal/ai\"\n", "internal/ai/planner.go"},
		{"invalid_import", "main.go", "package main\nimport (\n\"example.org/project/internal/ai\"\n", "internal/ai/planner.go"},
		{"binary", "main.go", "package main\nimport \"example.org/project/internal/ai\"\n\x00", "internal/ai/planner.go"},
		{"invalid_utf8", "main.go", "package main\nimport \"example.org/project/internal/ai\"\n\xff", "internal/ai/planner.go"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := intentGoImportReferenceContext(tc.source, tc.contents, []string{tc.target}); got != "" {
				t.Fatalf("unproved import supplied evidence: %q", got)
			}
		})
	}
}

func TestIntentGoImportReferenceContextKeepsScanAndOutputBounded(t *testing.T) {
	t.Parallel()
	late := "package main\n" + strings.Repeat("// unchanged context\n", intentSourceReferenceScanCap/20+1) + "import \"example.org/project/internal/ai\"\n"
	if got := intentGoImportReferenceContext("main.go", late, []string{"internal/ai/planner.go"}); got != "" {
		t.Fatalf("import beyond scan limit supplied evidence: %q", got)
	}
	var contents strings.Builder
	contents.WriteString("package main\nimport (\n")
	var paths []string
	for i := 0; i < 256; i++ {
		directory := fmt.Sprintf("internal/package%03d", i)
		paths = append(paths, directory+"/source.go")
		fmt.Fprintf(&contents, "\t\"example.org/project/%s\"\n", directory)
	}
	contents.WriteString(")\n")
	got := intentGoImportReferenceContext("main.go", contents.String(), paths)
	if got == "" || len(got) > intentSourceReferenceContextCap || strings.Count(got, "package") >= len(paths) || !strings.HasSuffix(got, " )\n") {
		t.Fatalf("unbounded or incomplete import evidence: bytes=%d imports=%d", len(got), strings.Count(got, "package"))
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "evidence.go", "package main\n"+got, parser.ImportsOnly); err != nil {
		t.Fatalf("output clipping broke actual import syntax: %v", err)
	}
	paths = append([]string{"internal/ai/planner_test.go"}, paths...)
	paths = append(paths, "internal/last/source.go")
	if got := intentGoImportReferenceContext("main.go", "package main\nimport \"example.org/project/internal/last\"\n", paths); got != "" {
		t.Fatalf("target beyond offered-path limit supplied evidence: %q", got)
	}
}

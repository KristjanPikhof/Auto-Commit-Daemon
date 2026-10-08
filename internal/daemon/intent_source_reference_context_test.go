package daemon

import (
	"strconv"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

func TestIntentSourceReferenceContextUsesActualRecordedCalls(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, source, contents, target string }{
		{"direct_shell", "scripts/run.sh", "ACD_SHARD=$index scripts/shards.sh \\\n", "scripts/shards.sh"},
		{"interpreter", "scripts/run.sh", "python3 scripts/manifest.py balance \"$package\"\n", "scripts/manifest.py"},
		{"quoted_operand", "scripts/run.sh", "bash \"./shards.sh\"\n", "scripts/shards.sh"},
		{"python_read", "scripts/manifest.py", "timings = open(\"scripts/timings.json\")\n", "scripts/timings.json"},
		{"python_sibling", "scripts/manifest.py", "balance.add_argument(\"--timings\", default=pathlib.Path(__file__).with_name(\"timings.json\"))\n", "scripts/timings.json"},
		{"dynamic_test_import", "scripts/test_manifest.py", "spec = importlib.util.spec_from_file_location(\"manifest\", Path(__file__).with_name(\"manifest.py\"))\n", "scripts/manifest.py"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			context := intentSourceReferenceContext(tc.source, tc.contents, []string{tc.source, tc.target})
			files, _ := intentSourcePathReferences(context)
			if context == "" || !strings.HasPrefix(context, " ") ||
				!intentSourceReferencesFile(files, tc.source, tc.target, true) {
				t.Fatalf("missing actual recorded reference: %q", context)
			}
		})
	}
}

func TestIntentSourceReferenceContextRejectsProseAndLiteralExamples(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, source, contents string }{
		{"shell_comment", "scripts/run.sh", "# scripts/tool.py is related\n"},
		{"echo", "scripts/run.sh", "echo \"python3 scripts/tool.py\"\n"},
		{"assignment", "scripts/run.sh", "example='scripts/tool.py'\n"},
		{"heredoc", "scripts/run.sh", "cat <<'EOF'\npython3 scripts/tool.py\nEOF\n"},
		{"unknown_heredoc", "scripts/run.sh", "cat <<$END\npython3 scripts/tool.py\n$END\n"},
		{"array_example", "scripts/run.sh", "examples=(\npython3 scripts/tool.py\n)\n"},
		{"nested_quote_operand", "scripts/run.sh", "python3 \"'scripts/tool.py'\"\n"},
		{"multiline_string", "scripts/run.sh", "example='example\npython3 scripts/tool.py\n'\n"},
		{"python_comment", "scripts/run.py", "# open(\"scripts/tool.py\")\n"},
		{"python_string", "scripts/run.py", `example = 'open("scripts/tool.py")'`},
		{"python_docstring", "scripts/run.py", "\"\"\"Example\nopen('scripts/tool.py')\n\"\"\"\n"},
		{"arbitrary_sibling_object", "scripts/run.py", "output.with_name('tool.py')\n"},
		{"arbitrary_quote", "scripts/run.py", "example = 'scripts/tool.py'\n"},
		{"stem_similarity", "scripts/run.sh", "python3 scripts/tool.py.backup\n"},
		{"unoffered_path", "scripts/run.sh", "python3 scripts/other.py\n"},
		{"different_directory", "other/run.py", "Path(__file__).with_name('tool.py')\n"},
		{"documentation", "scripts/guide.md", "python3 scripts/tool.py\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if context := intentSourceReferenceContext(tc.source, tc.contents, []string{tc.source, "scripts/tool.py"}); context != "" {
				t.Fatalf("unproved reference became context: %q", context)
			}
		})
	}
	// A literal filename in another argument must not acquire its own edge.
	context := intentSourceReferenceContext("scripts/run.sh", "python3 scripts/real.py 'scripts/tool.py'\n", []string{"scripts/real.py", "scripts/tool.py"})
	if strings.Contains(context, "tool.py") || !strings.Contains(context, "real.py") {
		t.Fatalf("non-command argument supplied evidence: %q", context)
	}
}

func TestIntentSourceReferenceContextConnectsBoundedRegressionScripts(t *testing.T) {
	t.Parallel()
	paths := []string{"scripts/test.sh", "scripts/shards.sh", "scripts/manifest.py", "scripts/timings.json", "scripts/test_manifest.py"}
	contents := []string{
		"run_package() {\n  SHARD=$index scripts/shards.sh \"$package\"\n}\nrun_package -parallel2\n",
		"python3 scripts/manifest.py balance \"$package\"\n",
		"timings = pathlib.Path(__file__).with_name(\"timings.json\").read_text()\n",
		"{\"_parallel_tests\": {\"./internal/daemon\": []}}\n",
		"spec = importlib.util.spec_from_file_location(\"manifest\", Path(__file__).with_name(\"manifest.py\"))\n",
	}
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	goal := ai.IntentCandidateAssignment{CandidateID: "bounded-feedback", Purpose: "keep complete regression feedback under five minutes",
		Readiness: ai.IntentCandidateReady, Subject: "Keep regression feedback under five minutes",
		Body: "- Balance complete serial and parallel regression work across shards", GroupingReason: "the runner, sharder, metadata reader and its regression form one feedback goal"}
	for i, name := range paths {
		seq := int64(i + 1)
		req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: seq, Path: name, CapturedDiff: "+changed_behavior = 2\n"})
		goal.SelectedSeqs = append(goal.SelectedSeqs, seq)
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{goal}}
	if err := ValidateIntentGoalPlan(req, plan); err == nil {
		t.Fatal("same directory and generic changed lines authorized a broad goal")
	}
	for i := range req.OfferedCaptures {
		req.OfferedCaptures[i].CapturedDiff = intentSourceReferenceContext(paths[i], contents[i], paths) + req.OfferedCaptures[i].CapturedDiff
	}
	if err := ValidateIntentGoalPlan(req, plan); err != nil {
		t.Fatalf("recorded caller/reader/test references did not connect the complete goal: %v", err)
	}
}

func TestIntentSourceReferenceContextKeepsScanAndOutputBounded(t *testing.T) {
	t.Parallel()
	contents := strings.Repeat("# padding\n", intentSourceReferenceScanCap/10+1) + "python3 scripts/tool.py\n"
	if context := intentSourceReferenceContext("scripts/run.sh", contents, []string{"scripts/tool.py"}); context != "" {
		t.Fatalf("reference beyond the scan bound was used: %q", context)
	}
	paths := []string{"scripts/tool.py"}
	var calls strings.Builder
	for i := 0; i < 256; i++ {
		prefix := strings.Repeat(" ", i)
		calls.WriteString(prefix + "python3 scripts/tool.py\n")
	}
	context := intentSourceReferenceContext("scripts/run.sh", calls.String(), paths)
	if len(context) > intentSourceReferenceContextCap || strings.Count(context, "tool.py") != 1 {
		t.Fatalf("reference context was unbounded or repeated: bytes=%d context=%q", len(context), context)
	}
	paths, calls = nil, strings.Builder{}
	for i := 0; i < 256; i++ {
		name := "scripts/" + strings.Repeat("long_name_", 5) + strconv.Itoa(i) + ".py"
		paths = append(paths, name)
		calls.WriteString("python3 " + name + "\n")
	}
	context = intentSourceReferenceContext("scripts/run.sh", calls.String(), paths)
	if len(context) <= intentSourceReferenceContextCap-100 || len(context) > intentSourceReferenceContextCap ||
		strings.Count(context, "python3") >= len(paths) || !strings.HasSuffix(context, "\n") {
		t.Fatalf("many distinct references escaped the whole-fragment output bound: bytes=%d calls=%d", len(context), strings.Count(context, "python3"))
	}
}

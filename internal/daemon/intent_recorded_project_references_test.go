package daemon

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

const intentRecordedVersionProject = `// !$*UTF8*$!
{
 objects = {
  BCCF00012026041900000001 /* Version.xcconfig */ = {
   isa = PBXFileReference;
   lastKnownFileType = text.xcconfig;
   path = Version.xcconfig;
   sourceTree = "<group>";
  };
 };
}`

func TestIntentProjectReferenceContextMatchesRegisteredOfferedFiles(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, filename, expected string
		offered                  []string
	}{
		{"unique_basename", "Version.xcconfig", "Version.xcconfig", []string{"Config/Version.xcconfig"}},
		{"exact_path", "Config/Version.xcconfig", "Config/Version.xcconfig", []string{"Config/Version.xcconfig", "Other/Version.xcconfig"}},
		{"relative_path", "../Config/Version.xcconfig", "../Config/Version.xcconfig", []string{"Config/Version.xcconfig", "Other/Version.xcconfig"}},
		{"quoted_path", `"Version.xcconfig"`, `"Version.xcconfig"`, []string{"Config/Version.xcconfig"}},
		{"repeated_capture_same_path", "Version.xcconfig", "Version.xcconfig", []string{"Config/Version.xcconfig", "Config/Version.xcconfig"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorded := strings.Replace(intentRecordedVersionProject, "path = Version.xcconfig;", "path = "+test.filename+";", 1)
			got := intentProjectReferenceContext("Assistant.xcodeproj/project.pbxproj", recorded, test.offered)
			if want := " path = " + test.expected + ";\n"; got != want {
				t.Fatalf("recorded witness=%q want=%q", got, want)
			}
		})
	}
}

func TestIntentProjectReferenceContextRefusesUnprovenReferences(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, source, contents string
		offered                []string
	}{
		{name: "ambiguous_basename", contents: intentRecordedVersionProject, offered: []string{"Config/Version.xcconfig", "Other/Version.xcconfig"}},
		{name: "unoffered_file", contents: intentRecordedVersionProject, offered: []string{"Config/Other.xcconfig"}},
		{name: "non_project", source: "project.txt", contents: intentRecordedVersionProject},
		{name: "commented_project", contents: "/* " + intentRecordedVersionProject + " */"},
		{name: "line_comments", contents: "// " + strings.ReplaceAll(intentRecordedVersionProject, "\n", "\n// ")},
		{name: "quoted_prose", contents: `{ description = ` + strconv.Quote(intentRecordedVersionProject) + `; };`},
		{name: "different_object_type", contents: strings.Replace(intentRecordedVersionProject, "PBXFileReference", "PBXGroup", 1)},
		{name: "sdk_file", contents: strings.Replace(intentRecordedVersionProject, `"<group>"`, "SDKROOT", 1)},
		{name: "build_product", contents: strings.Replace(intentRecordedVersionProject, `"<group>"`, "BUILT_PRODUCTS_DIR", 1)},
		{name: "comment_only_path", contents: strings.Replace(intentRecordedVersionProject, "path = Version.xcconfig;", "/* path = Version.xcconfig; */", 1)},
		{name: "nested_path", contents: strings.Replace(intentRecordedVersionProject, "path = Version.xcconfig;", "attributes = { path = Version.xcconfig; };", 1)},
		{name: "array_path", contents: strings.Replace(intentRecordedVersionProject, "path = Version.xcconfig;", "attributes = (path = Version.xcconfig;);", 1)},
		{name: "unregistered_object", contents: strings.Replace(intentRecordedVersionProject, "objects =", "prose =", 1)},
		{name: "invalid_object_id", contents: strings.Replace(intentRecordedVersionProject, "BCCF00012026041900000001", "explanation", 1)},
		{name: "duplicate_path", contents: strings.Replace(intentRecordedVersionProject, "path = Version.xcconfig;", "path = Other.xcconfig; path = Version.xcconfig;", 1)},
		{name: "duplicate_type", contents: strings.Replace(intentRecordedVersionProject, "isa = PBXFileReference;", "isa = PBXGroup; isa = PBXFileReference;", 1)},
		{name: "incomplete_object", contents: strings.Split(intentRecordedVersionProject, "sourceTree")[0]},
		{name: "empty_path", contents: strings.Replace(intentRecordedVersionProject, "path = Version.xcconfig;", `path = "";`, 1)},
		{name: "nul", contents: intentRecordedVersionProject + "\x00"},
		{name: "invalid_utf8", contents: intentRecordedVersionProject + "\xff"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.source == "" {
				test.source = "Assistant.xcodeproj/project.pbxproj"
			}
			if test.offered == nil {
				test.offered = []string{"Config/Version.xcconfig"}
			}
			if got := intentProjectReferenceContext(test.source, test.contents, test.offered); got != "" {
				t.Fatalf("unproven reference produced %q", got)
			}
		})
	}
}

func TestIntentProjectReferenceContextKeepsWitnessesBounded(t *testing.T) {
	t.Parallel()
	var recorded strings.Builder
	recorded.WriteString("{ objects = {\n")
	var offered []string
	for index := range 256 {
		filename := fmt.Sprintf("Config/registered-project-configuration-%03d.xcconfig", index)
		offered = append(offered, filename)
		fmt.Fprintf(&recorded, "%024x = { isa = PBXFileReference; path = %s; };\n", index, filename)
	}
	recorded.WriteString("}; }")
	got := intentProjectReferenceContext("project.pbxproj", recorded.String(), offered)
	if got == "" || len(got) > intentSourceReferenceContextCap || !strings.HasSuffix(got, ";\n") {
		t.Fatalf("context was empty, oversized or clipped mid-witness: %d bytes", len(got))
	}
	if got := intentProjectReferenceContext("project.pbxproj", strings.Repeat(" ", intentSourceReferenceScanCap)+intentRecordedVersionProject, []string{"Config/Version.xcconfig"}); got != "" {
		t.Fatalf("scan escaped its immutable byte budget: %q", got)
	}
	if got := intentProjectReferenceContext("project.pbxproj", intentRecordedVersionProject, append(offered, "Config/Version.xcconfig")); got != "" {
		t.Fatalf("scan escaped its offered path budget: %q", got)
	}
}

func TestIntentProjectRecordedConfigurationProvesReleaseGoal(t *testing.T) {
	t.Parallel()
	projectDiff := "diff --git a/Assistant.xcodeproj/project.pbxproj b/Assistant.xcodeproj/project.pbxproj\n@@ -1 +1 @@\n-CURRENT_PROJECT_VERSION = 162;\n+CURRENT_PROJECT_VERSION = 163;\n"
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2, CommitFormat: ai.CommitFormatImperative, OfferedCaptures: []ai.OfferedCapture{
		{Seq: 1, Path: "Assistant.xcodeproj/project.pbxproj", Op: "modify", CapturedDiff: projectDiff},
		{Seq: 2, Path: "Config/Version.xcconfig", Op: "modify", CapturedDiff: "diff --git a/Config/Version.xcconfig b/Config/Version.xcconfig\n@@ -1 +1 @@\n-CURRENT_PROJECT_VERSION = 162\n+CURRENT_PROJECT_VERSION = 163\n"},
	}}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{
		{CandidateID: "release-metadata", SelectedSeqs: []int64{1, 2}, Purpose: "Align release build numbers across target configurations", Readiness: ai.IntentCandidateReady,
			Subject: "Align release version and build metadata", Body: "- Keep all registered target configurations on the same release", GroupingReason: "The registered shared configuration supplies the same target build version"},
	}}
	if err := ValidateIntentGoalPlan(req, plan); err == nil || !strings.Contains(err.Error(), "candidate_disconnected") {
		t.Fatalf("release goal guessed a relationship without recorded project proof: %v", err)
	}
	references := intentProjectReferenceContext(req.OfferedCaptures[0].Path, intentRecordedVersionProject, []string{req.OfferedCaptures[0].Path, req.OfferedCaptures[1].Path})
	req.OfferedCaptures[0].CapturedDiff = includeIntentRecordedReferenceContext(projectDiff, references)
	if err := ValidateIntentGoalPlan(req, plan); err != nil {
		t.Fatalf("actual recorded project configuration relationship was lost: %v", err)
	}
}

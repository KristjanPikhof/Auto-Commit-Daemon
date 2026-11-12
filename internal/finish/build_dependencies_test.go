package finish

import (
	"slices"
	"testing"
)

func TestProjectBuildReferenceRequiresNewSource(t *testing.T) {
	source := "Assistant/Core/Runtime Profile.swift"
	project := "Assistant.xcodeproj/project.pbxproj"
	evidence := Evidence{Base: Tree{project: {Mode: "100644", OID: "old"}}, Target: Tree{project: {Mode: "100644", OID: "new"}, source: {Mode: "100644", OID: "source"}}}
	evidence.Changes = NetChanges(evidence.Base, evidence.Target, "author")
	diff := `+ A /* source */ = {isa = PBXFileReference; path = "Assistant/Core/Runtime Profile.swift"; sourceTree = SOURCE_ROOT; };`
	got := WithProjectSourceDependencies(evidence, map[string]string{project: diff})
	for i, change := range got.Changes {
		if change.Path == project && !slices.Equal(change.DependsOn, []string{source}) {
			t.Fatalf("project can precede its required new source: %+v", change)
		}
		if len(evidence.Changes[i].DependsOn) != 0 {
			t.Fatal("dependency inference mutated frozen input evidence")
		}
	}
	for _, ignored := range []string{
		`- A = {isa = PBXFileReference; path = "Assistant/Core/Runtime Profile.swift"; sourceTree = SOURCE_ROOT; };`,
		`+ A = {isa = PBXFileReference; path = "Assistant/Core/Runtime Profile.swift"; sourceTree = "<group>"; };`,
	} {
		got := WithProjectSourceDependencies(evidence, map[string]string{project: ignored})
		for _, change := range got.Changes {
			if len(change.DependsOn) != 0 {
				t.Fatal("removed or unresolved group-relative reference created a prerequisite")
			}
		}
	}
	list := "Tests/Resources.xcfilelist"
	evidence.Base[list], evidence.Target[list] = Entry{Mode: "100644", OID: "old"}, Entry{Mode: "100644", OID: "new"}
	evidence.Changes = NetChanges(evidence.Base, evidence.Target, "author")
	got = WithProjectSourceDependencies(evidence, map[string]string{list: "+$(SRCROOT)/" + source + "\n+$(TARGET_TEMP_DIR)/generated.txt"})
	for _, change := range got.Changes {
		if change.Path == list && !slices.Equal(change.DependsOn, []string{source}) {
			t.Fatal("input resource list can precede a newly created file it requires")
		}
	}
}

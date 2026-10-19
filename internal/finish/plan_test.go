package finish

import (
	"maps"
	"strings"
	"testing"
)

func planEntry(char string) Entry {
	return Entry{Mode: "100644", OID: strings.Repeat(char, 40)}
}

func TestFinishPlanCompleteBatch(t *testing.T) {
	base := Tree{"app.go": planEntry("a"), "old.txt": planEntry("b"), "kept": planEntry("c")}
	target := Tree{
		"app.go": planEntry("d"), "kept": planEntry("c"),
		"script": {Mode: "100755", OID: strings.Repeat("e", 40)},
		"link":   {Mode: "120000", OID: strings.Repeat("f", 40)},
	}
	evidence := Evidence{Base: base, Target: target, Changes: NetChanges(base, target, "author")}
	var ids []string
	for _, change := range evidence.Changes {
		ids = append(ids, change.ID)
	}
	plan, err := Build(evidence, []Goal{{Purpose: "Ship the executable entry point", Message: "Add executable entry point", ChangeIDs: ids}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || !maps.Equal(plan.Steps[0].Tree, target) {
		t.Fatalf("incomplete batch: %+v", plan)
	}
	plan.Steps[0].Tree["kept"] = planEntry("a")
	if err := Validate(evidence, plan); err == nil {
		t.Fatal("accepted mutation of an unchanged path")
	}
	if base["kept"] != planEntry("c") || target["kept"] != planEntry("c") {
		t.Fatal("plan mutation changed immutable input")
	}
}

func TestFinishPlanRecordedVersionsAndPrerequisites(t *testing.T) {
	evidence := Evidence{
		Base:   Tree{"api": planEntry("a"), "ui": planEntry("b")},
		Target: Tree{"api": planEntry("d"), "ui": planEntry("e")},
		Changes: []Change{
			{ID: "api-1", Path: "api", Before: planEntry("a"), After: planEntry("c"), Author: "one"},
			{ID: "ui", Path: "ui", Before: planEntry("b"), After: planEntry("e"), Author: "one", DependsOn: []string{"api-1"}},
			{ID: "api-2", Path: "api", Before: planEntry("c"), After: planEntry("d"), Author: "one"},
		},
	}
	goals := []Goal{
		{Purpose: "Expose the required API", Boundary: "API works independently", Message: "Add API", ChangeIDs: []string{"api-1", "api-2"}},
		{Purpose: "Use the API in the interface", Boundary: "Interface can be reverted separately", Message: "Use API in interface", ChangeIDs: []string{"ui"}},
	}
	plan, err := Build(evidence, goals)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Steps[0].Tree["ui"] != planEntry("b") || plan.Steps[0].Tree["api"] != planEntry("d") {
		t.Fatal("did not materialize the complete prerequisite tree")
	}
	goals[0].ChangeIDs = []string{"ui"}
	goals[1].ChangeIDs = []string{"api-1", "api-2"}
	if _, err := Build(evidence, goals); err == nil {
		t.Fatal("accepted consumer before prerequisite")
	}
	goals[0].ChangeIDs = []string{"api-2", "api-1"}
	goals[1].ChangeIDs = []string{"ui"}
	if _, err := Build(evidence, goals); err == nil {
		t.Fatal("accepted reversed versions on the same path")
	}
}

func TestFinishPlanCannotReorderRepeatedPathVersions(t *testing.T) {
	evidence := Evidence{Base: Tree{"api": planEntry("a")}, Target: Tree{"api": planEntry("c")}, Changes: []Change{
		{ID: "first", Path: "api", Before: planEntry("a"), After: planEntry("b")},
		{ID: "undo", Path: "api", Before: planEntry("b"), After: planEntry("a")},
		{ID: "restore", Path: "api", Before: planEntry("a"), After: planEntry("b")},
		{ID: "feature", Path: "api", Before: planEntry("b"), After: planEntry("c")},
	}}
	goal := Goal{Purpose: "Complete the final behavior", Message: "Complete final behavior", ChangeIDs: []string{"restore", "undo", "first", "feature"}}
	if _, err := Build(evidence, []Goal{goal}); err == nil {
		t.Fatal("matching preimages allowed a false recorded order")
	}
	// The earlier edit/revert pair can remain neutral provenance without
	// forcing development noise into the branch's useful steps.
	goal.ChangeIDs = []string{"restore", "feature"}
	if _, err := Build(evidence, []Goal{goal}); err != nil {
		t.Fatalf("neutral cycle could not be omitted: %v", err)
	}
}

func TestFinishPlanRenameAndAuthorBoundaries(t *testing.T) {
	evidence := Evidence{
		Base: Tree{"old": planEntry("a")}, Target: Tree{"new": planEntry("b")},
		Changes: []Change{
			{ID: "delete", Path: "old", Before: planEntry("a"), Author: "one", AtomicGroup: "rename"},
			{ID: "create", Path: "new", After: planEntry("b"), Author: "one", AtomicGroup: "rename"},
		},
	}
	whole := []Goal{{Purpose: "Move the entry point", Message: "Move entry point", ChangeIDs: []string{"delete", "create"}}}
	if _, err := Build(evidence, whole); err != nil {
		t.Fatal(err)
	}
	split := []Goal{
		{Purpose: "Remove old path", Boundary: "removal", Message: "Remove old path", ChangeIDs: []string{"delete"}},
		{Purpose: "Create new path", Boundary: "creation", Message: "Create new path", ChangeIDs: []string{"create"}},
	}
	if _, err := Build(evidence, split); err == nil {
		t.Fatal("accepted a split rename")
	}
	evidence.Changes[1].Author = "two"
	if _, err := Build(evidence, whole); err == nil {
		t.Fatal("accepted a goal spanning different authors")
	}
}

func TestFinishPlanNetZero(t *testing.T) {
	base := Tree{"app": planEntry("a")}
	evidence := Evidence{Base: base, Target: CloneTree(base), Changes: []Change{
		{ID: "edit", Path: "app", Before: planEntry("a"), After: planEntry("b")},
		{ID: "revert", Path: "app", Before: planEntry("b"), After: planEntry("a")},
	}}
	plan, err := Build(evidence, nil)
	if err != nil || len(plan.Steps) != 0 {
		t.Fatalf("net-zero plan created history: %+v, %v", plan, err)
	}
	if _, err := Build(evidence, []Goal{{Purpose: "Save edits", Message: "Save edits", ChangeIDs: []string{"edit", "revert"}}}); err == nil {
		t.Fatal("accepted commits for reverted net-zero work")
	}
}

func TestFinishPlanNeutralProvenanceAcrossAuthors(t *testing.T) {
	evidence := Evidence{
		Base:   Tree{"app": planEntry("a")},
		Target: Tree{"app": planEntry("a"), "docs": planEntry("c")},
		Changes: []Change{
			{ID: "edit", Path: "app", Before: planEntry("a"), After: planEntry("b"), Author: "one"},
			{ID: "revert", Path: "app", Before: planEntry("b"), After: planEntry("a"), Author: "one"},
			{ID: "docs", Path: "docs", After: planEntry("c"), Author: "two"},
		},
	}
	goal := Goal{Purpose: "Explain the workflow", Message: "Document workflow", ChangeIDs: []string{"docs"}}
	plan, err := Build(evidence, []Goal{goal})
	if err != nil || len(plan.Steps) != 1 || len(plan.NeutralChangeIDs) != 2 || plan.Steps[0].Author != "two" {
		t.Fatalf("reverted provenance forced commits: %+v, %v", plan, err)
	}
	evidence.Changes[2].DependsOn = []string{"edit"}
	if _, err := Build(evidence, []Goal{goal}); err == nil {
		t.Fatal("accepted a consumer of an omitted prerequisite")
	}
	evidence.Changes[2].DependsOn = nil
	plan.NeutralChangeIDs = []string{"edit", "edit"}
	if err := Validate(evidence, plan); err == nil {
		t.Fatal("accepted duplicated neutral provenance")
	}
	plan.NeutralChangeIDs = []string{"edit"}
	if err := Validate(evidence, plan); err == nil {
		t.Fatal("omitted a surviving recorded change")
	}
}

func TestFinishPlanNeutralCyclesAfterSurvivingVersion(t *testing.T) {
	evidence := Evidence{
		Base: Tree{"app": planEntry("a")}, Target: Tree{"app": planEntry("d")},
		Changes: []Change{
			{ID: "surviving-1", Path: "app", Before: planEntry("a"), After: planEntry("b"), Author: "one"},
			{ID: "explore-1", Path: "app", Before: planEntry("b"), After: planEntry("c"), Author: "two"},
			{ID: "revert-1", Path: "app", Before: planEntry("c"), After: planEntry("b"), Author: "two"},
			{ID: "surviving-2", Path: "app", Before: planEntry("b"), After: planEntry("d"), Author: "one"},
			{ID: "explore-2", Path: "app", Before: planEntry("d"), After: planEntry("e"), Author: "two"},
			{ID: "revert-2", Path: "app", Before: planEntry("e"), After: planEntry("d"), Author: "two"},
		},
	}
	goal := Goal{Purpose: "Complete the implementation", Message: "Complete implementation", ChangeIDs: []string{"surviving-1", "surviving-2"}}
	plan, err := Build(evidence, []Goal{goal})
	if err != nil || len(plan.Steps) != 1 || len(plan.NeutralChangeIDs) != 4 {
		t.Fatalf("reverted later versions forced history: %+v, %v", plan, err)
	}
}

func TestFinishPlanRejectsIncompleteAndFabricatedPlans(t *testing.T) {
	base := Tree{"a": planEntry("a"), "b": planEntry("b")}
	target := Tree{"a": planEntry("c"), "b": planEntry("d")}
	evidence := Evidence{Base: base, Target: target, Changes: NetChanges(base, target, "one")}
	for name, goals := range map[string][]Goal{
		"missing":        {{Purpose: "Change a", Message: "Change a", ChangeIDs: []string{"a"}}},
		"unknown":        {{Purpose: "Change both", Message: "Change both", ChangeIDs: []string{"a", "invented"}}},
		"duplicate":      {{Purpose: "Change both", Message: "Change both", ChangeIDs: []string{"a", "a", "b"}}},
		"no purpose":     {{Message: "Change both", ChangeIDs: []string{"a", "b"}}},
		"unsafe message": {{Purpose: "Change both", Message: "Change\x1bboth", ChangeIDs: []string{"a", "b"}}},
		"empty step":     {{Purpose: "Change both", Message: "Change both"}},
		"arbitrary split": {
			{Purpose: "Change a", Message: "Change a", ChangeIDs: []string{"a"}},
			{Purpose: "Change b", Message: "Change b", ChangeIDs: []string{"b"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Build(evidence, goals); err == nil {
				t.Fatal("accepted invalid plan")
			}
		})
	}
	plan, err := Build(evidence, []Goal{{Purpose: "Complete the goal", Message: "Complete goal", ChangeIDs: []string{"a", "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	plan.Steps[0].Tree["a"] = planEntry("f")
	if err := Validate(evidence, plan); err == nil {
		t.Fatal("accepted invented intermediate bytes")
	}
}

func TestFinishPlanRejectsUnsafeTrees(t *testing.T) {
	for _, name := range []string{"../outside", "/outside", ".git/config", "dir/.GIT/config", "a/../b", "bad\x00path", "bad\xffpath"} {
		t.Run(name, func(t *testing.T) {
			target := Tree{name: planEntry("a")}
			if _, err := Build(Evidence{Base: Tree{}, Target: target, Changes: NetChanges(Tree{}, target, "")}, nil); err == nil {
				t.Fatal("accepted unsafe path")
			}
		})
	}
	for name, tree := range map[string]Tree{
		"directory collision": {"a": planEntry("a"), "a/b": planEntry("b")},
		"bad mode":            {"a": {Mode: "040000", OID: strings.Repeat("a", 40)}},
		"zero object":         {"a": {Mode: "100644", OID: strings.Repeat("0", 40)}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Build(Evidence{Base: tree, Target: tree}, nil); err == nil {
				t.Fatal("accepted invalid tree")
			}
		})
	}
}

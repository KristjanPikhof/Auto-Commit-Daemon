package finish

import (
	"bytes"
	"maps"
	"reflect"
	"strings"
	"testing"
)

func TestSourceDiagnosticLinesFollowExactSelections(t *testing.T) {
	file := SourceFile{Before: []byte("a\nold\nz\n"), After: []byte("a\nnew\nextra\nz\n"), Edits: []SourceEdit{
		{ID: "remove-old", Kind: "delete", BeforeLine: 1},
		{ID: "add-new", Kind: "insert", BeforeLine: 1, AfterLine: 1},
		{ID: "add-extra", Kind: "insert", BeforeLine: 1, AfterLine: 2},
	}}
	for _, test := range []struct {
		selected []string
		text     string
		ids      []string
	}{
		{nil, "a\nold\nz\n", []string{"", "remove-old", ""}},
		{[]string{"add-new", "add-extra"}, "a\nnew\nextra\nold\nz\n", []string{"", "add-new", "add-extra", "remove-old", ""}},
		{[]string{"remove-old", "add-new", "add-extra"}, "a\nnew\nextra\nz\n", []string{"", "add-new", "add-extra", ""}},
	} {
		selected := make(map[string]bool)
		for _, id := range test.selected {
			selected[id] = true
		}
		source := sourceFile{before: sourceLines(file.Before), after: sourceLines(file.After), edits: file.Edits}
		if string(source.compose(selected)) != test.text {
			t.Fatal("diagnostic walk changed candidate bytes")
		}
		for line, id := range test.ids {
			if got := file.LineSelection(selected, line); got != id {
				t.Fatalf("line %d: got %q, want %q", line, got, id)
			}
		}
		if file.LineSelection(selected, -1) != "" || file.LineSelection(selected, len(test.ids)) != "" {
			t.Fatal("out-of-range diagnostic selected source")
		}
	}
}

func sourceFixture() (Evidence, []SourceFile, []Goal) {
	before := []byte("package camera\ntype Status struct { Running bool }\nfunc Selected(s Status) string { return \"\" }\n")
	after := []byte("package camera\ntype Status struct { Running bool; CameraID string }\nfunc Selected(s Status) string { return s.CameraID }\n")
	base := Tree{"camera.go": {Mode: "100644", OID: sourceBlobOID(before, 40)}}
	target := Tree{"camera.go": {Mode: "100644", OID: sourceBlobOID(after, 40)}}
	evidence := Evidence{Base: base, Target: target, Changes: NetChanges(base, target, "author")}
	files := []SourceFile{{ChangeID: "camera.go", Before: before, After: after, Edits: []SourceEdit{
		{ID: "remove status", Kind: "delete", BeforeLine: 1},
		{ID: "add status", Kind: "insert", BeforeLine: 1, AfterLine: 1},
		{ID: "remove consumer", Kind: "delete", BeforeLine: 2},
		{ID: "add consumer", Kind: "insert", BeforeLine: 2, AfterLine: 2},
	}}}
	goals := []Goal{
		{Purpose: "Expose the active camera identity", Boundary: "Supply the field before its consumer", Reason: "Controls need to identify the running camera.", Message: "Expose the active camera identity\n\n- Controls need to identify the running camera.", ChangeIDs: []string{"add status", "remove status"}},
		{Purpose: "Synchronize camera selection", Boundary: "Consume the available status field", Reason: "Read status so controls follow the running camera.", Message: "Synchronize camera selection\n\n- Read status so controls follow the running camera.", ChangeIDs: []string{"add consumer", "remove consumer"}},
	}
	return evidence, files, goals
}

func TestBuildSourcePlanKeepsDefinitionsBeforeConsumers(t *testing.T) {
	evidence, files, goals := sourceFixture()
	originalIDs := append([]string(nil), goals[0].ChangeIDs...)
	result, err := BuildSourcePlan(evidence, files, goals)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan.Steps) != 2 || !maps.Equal(result.Plan.Steps[1].Tree, evidence.Target) {
		t.Fatalf("lost the complete target: %+v", result.Plan)
	}
	first := result.Blobs[result.Plan.Steps[0].Tree["camera.go"].OID]
	if !bytes.Contains(first, []byte("CameraID string")) || bytes.Contains(first, []byte("return s.CameraID")) {
		t.Fatalf("definition and consumer were not separated: %s", first)
	}
	if err := Validate(result.Evidence, result.Plan); err != nil {
		t.Fatalf("derived versions do not pass the ordinary validator: %v", err)
	}
	if result.Plan.Steps[0].Message != goals[0].Message || result.Plan.Steps[0].Reason != goals[0].Reason ||
		!reflect.DeepEqual(result.Selections[0].ChangeIDs, originalIDs) || !reflect.DeepEqual(goals[0].ChangeIDs, originalIDs) {
		t.Fatal("construction changed the explanation, selection or caller input")
	}
	if result.Evidence.Changes[0].Provenance != "camera.go" || result.Plan.Steps[0].Author != "author" {
		t.Fatal("derived evidence lost its source or author")
	}
}

func TestBuildSourcePlanRequiresEveryExistingEditOnce(t *testing.T) {
	for _, scenario := range []string{"missing", "duplicate", "unknown", "invented bytes", "omitted target line", "repeated insertion", "invalid deletion", "binary", "mode", "rename", "recorded dependency", "no-op step"} {
		t.Run(scenario, func(t *testing.T) {
			evidence, files, goals := sourceFixture()
			switch scenario {
			case "missing":
				goals[0].ChangeIDs = goals[0].ChangeIDs[:1]
			case "duplicate":
				goals[1].ChangeIDs = append(goals[1].ChangeIDs, goals[0].ChangeIDs[0])
			case "unknown":
				goals[0].ChangeIDs[0] = "unseen edit"
			case "invented bytes":
				files[0].After = []byte("new code was not in the frozen object\n")
			case "omitted target line":
				files[0].Edits = files[0].Edits[:3]
			case "repeated insertion":
				files[0].Edits[3].AfterLine = 1
			case "invalid deletion":
				files[0].Edits[0].BeforeLine = -1
			case "binary":
				files[0].Before = []byte{'a', 0}
				evidence.Base["camera.go"] = Entry{Mode: "100644", OID: sourceBlobOID(files[0].Before, 40)}
				evidence.Changes[0].Before = evidence.Base["camera.go"]
			case "mode":
				evidence.Target["camera.go"] = Entry{Mode: "100755", OID: evidence.Target["camera.go"].OID}
				evidence.Changes[0].After = evidence.Target["camera.go"]
			case "rename":
				evidence.Changes[0].AtomicGroup = "rename pair"
			case "recorded dependency":
				evidence.Target["asset"] = planEntry("d")
				evidence.Changes = append(evidence.Changes, Change{ID: "asset", Path: "asset", After: evidence.Target["asset"], Author: "author", DependsOn: []string{"camera.go"}})
				goals[1].ChangeIDs = append(goals[1].ChangeIDs, "asset")
				if err := ValidateEvidence(evidence); err != nil {
					t.Fatalf("fixture dependency is not valid recorded evidence: %v", err)
				}
			case "no-op step":
				goals = append([]Goal{{Purpose: "Do nothing", Message: "Do nothing"}}, goals...)
			}
			if _, err := BuildSourcePlan(evidence, files, goals); err == nil {
				t.Fatal("accepted an incomplete or unsupported source plan")
			}
		})
	}
}

func TestBuildSourcePlanPreservesWholeEntryAndAuthorBoundaries(t *testing.T) {
	evidence, files, goals := sourceFixture()
	evidence.Target["asset.bin"] = planEntry("d")
	evidence.Changes = append(evidence.Changes, Change{ID: "asset.bin", Path: "asset.bin", After: evidence.Target["asset.bin"], Author: "other"})
	goals[1].ChangeIDs = append(goals[1].ChangeIDs, "asset.bin")
	if _, err := BuildSourcePlan(evidence, files, goals); err == nil || !strings.Contains(err.Error(), "author") {
		t.Fatalf("mixed source and whole entries crossed authors: %v", err)
	}
	goals[1].ChangeIDs = goals[1].ChangeIDs[:2]
	goals = append(goals, Goal{Purpose: "Supply the completed resource", Boundary: "Keep the other author's resource independent", Message: "Supply the completed resource\n\n- Keep the authored resource with its original identity.", ChangeIDs: []string{"asset.bin"}})
	result, err := BuildSourcePlan(evidence, files, goals)
	if err != nil || result.Plan.Steps[2].Tree["asset.bin"] != evidence.Target["asset.bin"] || result.Plan.Steps[2].Author != "other" {
		t.Fatalf("whole entry changed: %+v %v", result.Plan, err)
	}
}

func TestBuildSourcePlanPreservesLineBytesAndEmptyFiles(t *testing.T) {
	for _, target := range []string{"first\r\nsecond", "õun\n", ""} {
		before, after := []byte{}, []byte(target)
		if target == "" {
			before = []byte("remove me\r\n")
		}
		base := Tree{"text": {Mode: "100644", OID: sourceBlobOID(before, 40)}}
		end := Tree{"text": {Mode: "100644", OID: sourceBlobOID(after, 40)}}
		evidence := Evidence{Base: base, Target: end, Changes: NetChanges(base, end, "author")}
		file := SourceFile{ChangeID: "text", Before: before, After: after}
		ids := []string{}
		if len(before) > 0 {
			file.Edits = append(file.Edits, SourceEdit{ID: "remove", Kind: "delete"})
			ids = append(ids, "remove")
		}
		for i := range sourceLines(after) {
			id := string(rune('a' + i))
			file.Edits = append(file.Edits, SourceEdit{ID: id, Kind: "insert", AfterLine: i})
			ids = append(ids, id)
		}
		goal := Goal{Purpose: "Preserve exact text", Message: "Preserve exact text\n\n- Keep the selected bytes and line endings intact.", ChangeIDs: ids}
		result, err := BuildSourcePlan(evidence, []SourceFile{file}, []Goal{goal})
		if err != nil || !maps.Equal(result.Plan.Steps[0].Tree, end) {
			t.Fatalf("line bytes changed for %q: %+v %v", target, result.Plan, err)
		}
	}
	if got := sourceBlobOID(nil, 40); got != "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" {
		t.Fatalf("Git empty blob identity changed: %s", got)
	}
	if got := sourceBlobOID(nil, 64); got != "473a0f4c3be8a93681a267e3b1e9a7dcda1185436fe141f7749120a303721813" {
		t.Fatalf("Git SHA-256 empty blob identity changed: %s", got)
	}
}

func TestBuildSourcePlanPreservesSHA256Objects(t *testing.T) {
	evidence, files, goals := sourceFixture()
	evidence.Base["camera.go"] = Entry{Mode: "100644", OID: sourceBlobOID(files[0].Before, 64)}
	evidence.Target["camera.go"] = Entry{Mode: "100644", OID: sourceBlobOID(files[0].After, 64)}
	evidence.Changes = NetChanges(evidence.Base, evidence.Target, "author")
	result, err := BuildSourcePlan(evidence, files, goals)
	if err != nil || len(result.Plan.Steps[0].Tree["camera.go"].OID) != 64 || !maps.Equal(result.Plan.Target, evidence.Target) {
		t.Fatalf("source selections changed object format: %+v %v", result.Plan, err)
	}
}

func TestBuildSourcePlanPreservesFrozenProvenance(t *testing.T) {
	evidence, files, goals := sourceFixture()
	evidence.Changes[0].Provenance = "frozen-capture"
	result, err := BuildSourcePlan(evidence, files, goals)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range result.Evidence.Changes {
		if change.Provenance != "frozen-capture" {
			t.Fatalf("source selection replaced immutable provenance: %+v", change)
		}
	}
}

func TestBuildSourcePlanStagesNewFileWithoutInventingCode(t *testing.T) {
	for _, oidLength := range []int{40, 64} {
		body := []byte("package settings\nfunc Camera() string { return \"camera\" }\nfunc GenerateRoom() string { return \"room\" }\n")
		target := Tree{"settings.go": {Mode: "100755", OID: sourceBlobOID(body, oidLength)}}
		evidence := Evidence{Base: Tree{}, Target: target, Changes: NetChanges(Tree{}, target, "author")}
		file := SourceFile{ChangeID: "settings.go", After: body, Edits: []SourceEdit{
			{ID: "header", Kind: "insert", AfterLine: 0}, {ID: "settings", Kind: "insert", AfterLine: 1}, {ID: "room", Kind: "insert", AfterLine: 2},
		}}
		goals := []Goal{
			{Purpose: "Organize camera settings", Boundary: "Introduce working settings before the independent room feature", Message: "Organize camera settings", ChangeIDs: []string{"header", "settings"}},
			{Purpose: "Add room generation", Boundary: "Keep the optional room feature independently revertible", Message: "Add room generation", ChangeIDs: []string{"room"}},
		}
		plan, err := BuildSourcePlan(evidence, []SourceFile{file}, goals)
		if err != nil || !maps.Equal(plan.Plan.Target, target) || len(plan.Plan.Steps) != 2 {
			t.Fatalf("new file stages: %+v %v", plan, err)
		}
		first := plan.Plan.Steps[0].Tree["settings.go"]
		if first.Mode != "100755" || string(plan.Blobs[first.OID]) != "package settings\nfunc Camera() string { return \"camera\" }\n" {
			t.Fatalf("new file lost exact bytes or mode: %+v", first)
		}
		// A new file stays absent until its owner runs, including preceding goals.
		evidence.Target["notes"] = planEntry("d")
		evidence.Changes = NetChanges(evidence.Base, evidence.Target, "author")
		goals = append([]Goal{{Purpose: "Document camera behavior", Boundary: "Keep independent notes separate from source changes", Message: "Document camera behavior", ChangeIDs: []string{"notes"}}}, goals...)
		plan, err = BuildSourcePlan(evidence, []SourceFile{file}, goals)
		if err != nil || plan.Plan.Steps[0].Tree["settings.go"] != (Entry{}) {
			t.Fatalf("new file appeared before its first selection: %+v %v", plan, err)
		}
	}
}

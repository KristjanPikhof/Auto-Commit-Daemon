package ai

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishPathOwnershipIgnoresEmptyEntries(t *testing.T) {
	evidence := finishTestEvidence()
	goals := []finish.Goal{finishTestGoal(evidence)}
	goals[0].ChangeIDs = nil
	assignments := []finishPathOwnership{{}, {Goal: 1, Paths: []string{"settings.go"}}}
	if err := expandFinishPathOwnership(evidence.Changes, goals, assignments); err != nil || !slices.Equal(goals[0].ChangeIDs, []string{"settings.go"}) {
		t.Fatalf("empty entry changed ownership: %+v %v", goals, err)
	}
	if err := expandFinishPathOwnership(evidence.Changes, goals, []finishPathOwnership{{Goal: 0, Paths: []string{"settings.go"}}}); err == nil || !strings.Contains(err.Error(), "names goal 0") {
		t.Fatalf("invalid goal for a real path was accepted: %v", err)
	}
}

func TestFinishPathOwnershipUsesMostSpecificAssignment(t *testing.T) {
	const gemini = "Assistant/AI/Services/GeminiService.swift"
	changes := []finish.Change{
		{ID: "checkpoint-35:" + gemini, Path: gemini},
		{ID: "final:" + gemini, Path: gemini},
		{ID: "provider", Path: "Assistant/AI/Services/OpenRouterService.swift"},
		{ID: "settings", Path: "Assistant/AI/Settings.swift"},
	}
	goals := make([]finish.Goal, 4)
	goals[0].ChangeIDs = []string{changes[0].ID}
	assignments := []finishPathOwnership{
		{Goal: 2, Paths: []string{"Assistant/AI/"}},
		{Goal: 3, Paths: []string{"Assistant/AI/Services/"}},
		{Goal: 4, Paths: []string{gemini, gemini}},
	}
	if err := expandFinishPathOwnership(changes, goals, assignments); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{changes[0].ID, "settings", "provider", changes[1].ID} {
		if !slices.Equal(goals[i].ChangeIDs, []string{want}) {
			t.Fatalf("goal %d owns %v, want %s", i+1, goals[i].ChangeIDs, want)
		}
	}
	assignments = append(assignments, finishPathOwnership{Goal: 2, Paths: []string{gemini}})
	if err := expandFinishPathOwnership(changes, goals, assignments); err == nil || !strings.Contains(err.Error(), "conflicting goals") {
		t.Fatalf("conflicting identical selector passed: %v", err)
	}
}

func TestFinishPathOwnershipRejectsUnknownAndMissingFiles(t *testing.T) {
	evidence := finishTestEvidence()
	evidence.Target["docs/guide.md"] = finishTestEntry("c")
	evidence.Target["docs-extra/guide.md"] = finishTestEntry("d")
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author")
	goal := finishTestGoal(evidence)
	goal.ChangeIDs = nil
	options := finishTestOptions(evidence)
	options.AllowPathOwnership = true
	for _, tc := range []struct {
		name  string
		paths []string
		want  string
	}{
		{"unknown", []string{"missing/"}, "matches no recorded changes"},
		{"missing", []string{"docs/", "settings.go"}, "docs-extra/guide.md"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(finishPlanOutput{
				Goals:         []finish.Goal{goal},
				PathOwnership: []finishPathOwnership{{Goal: 1, Paths: tc.paths}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := validateFinishOutput(raw, evidence, options); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("invalid path ownership passed or lost its diagnosis: %v", err)
			}
		})
	}
}

package finish

import (
	"fmt"
	"maps"
	"path"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ValidateEvidence checks immutable input before provider or repository work.
func ValidateEvidence(evidence Evidence) error {
	_, err := validateEvidence(evidence)
	return err
}

// Validate proves structural completeness and recorded-version safety. Purpose
// quality and project correctness require the caller's planner and verifier.
func Validate(evidence Evidence, plan Plan) error {
	changes, err := validateEvidence(evidence)
	if err != nil {
		return err
	}
	if !maps.Equal(plan.Base, evidence.Base) || !maps.Equal(plan.Target, evidence.Target) {
		return planError("plan changed the frozen base or target")
	}
	neutral, err := validateNeutral(changes, plan.NeutralChangeIDs)
	if err != nil {
		return err
	}
	if maps.Equal(evidence.Base, evidence.Target) {
		if len(plan.Steps) != 0 || len(neutral) != len(changes) {
			return planError("net-zero work must not create commits")
		}
		return nil
	}
	current := CloneTree(evidence.Base)
	used := make(map[string]bool, len(changes))
	atomicOwners := make(map[string]int)
	for id := range neutral {
		if group := changes[id].AtomicGroup; group != "" {
			atomicOwners[group] = -1
		}
	}
	for index, step := range plan.Steps {
		if strings.TrimSpace(step.Purpose) == "" || !safeText(step.Purpose) ||
			strings.TrimSpace(step.Message) == "" || !safeText(step.Message) || len(step.ChangeIDs) == 0 {
			return planError("step %d needs a purpose, safe message, and changes", index+1)
		}
		if len(plan.Steps) > 1 && (strings.TrimSpace(step.Boundary) == "" || !safeText(step.Boundary)) {
			return planError("step %d needs a useful split boundary", index+1)
		}
		previous := plan.Base
		if index > 0 {
			previous = plan.Steps[index-1].Tree
		}
		author := changes[step.ChangeIDs[0]].Author
		for _, id := range step.ChangeIDs {
			change, ok := changes[id]
			if !ok || used[id] || neutral[id] {
				return planError("unknown or multiply assigned change %q", id)
			}
			if change.Author != author || step.Author != author {
				return planError("step %d crosses an author boundary", index+1)
			}
			for _, prerequisite := range change.DependsOn {
				if !used[prerequisite] {
					return planError("change %q is missing prerequisite %q", id, prerequisite)
				}
			}
			if group := change.AtomicGroup; group != "" {
				if owner, exists := atomicOwners[group]; exists && owner != index {
					return planError("atomic group %q was split", group)
				}
				atomicOwners[group] = index
			}
			if err := apply(current, change); err != nil {
				return err
			}
			used[id] = true
		}
		if err := validateTree(step.Tree); err != nil {
			return err
		}
		if !maps.Equal(current, step.Tree) {
			return planError("step %d contains an unrecorded tree", index+1)
		}
		if maps.Equal(previous, current) {
			return planError("step %d has no net change", index+1)
		}
	}
	if len(used)+len(neutral) != len(changes) || !maps.Equal(current, evidence.Target) {
		return planError("plan does not cover the exact frozen target")
	}
	return nil
}

// Neutral changes retain provenance without manufacturing edit/revert commits.
// Each omitted path cycle must return to its exact recorded starting version.
// Emitted steps still replay from the base, so an omitted version cannot become
// an unproven prerequisite for a surviving goal.
func validateNeutral(changes map[string]Change, ids []string) (map[string]bool, error) {
	neutral := make(map[string]bool, len(ids))
	current := make(Tree)
	starts := make(Tree)
	for _, id := range ids {
		change, ok := changes[id]
		if !ok || neutral[id] {
			return nil, planError("unknown or repeated neutral change %q", id)
		}
		if _, active := starts[change.Path]; !active {
			starts[change.Path] = change.Before
			current[change.Path] = change.Before
		}
		if current[change.Path] != change.Before {
			return nil, planError("neutral changes omit a surviving version for %q", change.Path)
		}
		current[change.Path] = change.After
		if change.After == starts[change.Path] {
			delete(starts, change.Path)
			delete(current, change.Path)
		}
		neutral[id] = true
	}
	if len(starts) != 0 {
		return nil, planError("omitted changes are not independently net-zero")
	}
	return neutral, nil
}

func validateEvidence(evidence Evidence) (map[string]Change, error) {
	if err := validateTree(evidence.Base); err != nil {
		return nil, err
	}
	if err := validateTree(evidence.Target); err != nil {
		return nil, err
	}
	changes := make(map[string]Change, len(evidence.Changes))
	current := CloneTree(evidence.Base)
	for _, change := range evidence.Changes {
		if change.ID == "" || changes[change.ID].ID != "" || !safePath(change.Path) {
			return nil, planError("invalid or duplicate recorded change %q", change.ID)
		}
		for _, entry := range []Entry{change.Before, change.After} {
			if entry != (Entry{}) && !validEntry(entry) {
				return nil, planError("invalid recorded entry for %q", change.Path)
			}
		}
		if change.Before == change.After {
			return nil, planError("recorded change %q is empty", change.ID)
		}
		for _, prerequisite := range change.DependsOn {
			if _, exists := changes[prerequisite]; !exists {
				return nil, planError("invalid recorded prerequisite %q", prerequisite)
			}
		}
		if err := apply(current, change); err != nil {
			return nil, err
		}
		changes[change.ID] = change
	}
	if !maps.Equal(current, evidence.Target) {
		return nil, planError("recorded versions do not reach the frozen target")
	}
	return changes, nil
}

func apply(tree Tree, change Change) error {
	if tree[change.Path] != change.Before {
		return planError("prerequisite version is missing for %q", change.Path)
	}
	if change.After == (Entry{}) {
		delete(tree, change.Path)
	} else {
		tree[change.Path] = change.After
	}
	return nil
}

func validateTree(tree Tree) error {
	for name, entry := range tree {
		if !safePath(name) || !validEntry(entry) {
			return planError("invalid tree entry %q", name)
		}
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, exists := tree[parent]; exists {
				return planError("tree path %q is nested under a leaf", name)
			}
		}
	}
	return nil
}

func safePath(name string) bool {
	if !utf8.ValidString(name) || name == "" || name == "." || path.Clean(name) != name || strings.HasPrefix(name, "/") ||
		name == ".." || strings.HasPrefix(name, "../") || strings.ContainsRune(name, '\x00') {
		return false
	}
	for _, component := range strings.Split(name, "/") {
		if strings.EqualFold(component, ".git") {
			return false
		}
	}
	return true
}

func validEntry(entry Entry) bool {
	switch entry.Mode {
	case "100644", "100755", "120000", "160000":
	default:
		return false
	}
	if len(entry.OID) != 40 && len(entry.OID) != 64 {
		return false
	}
	for _, char := range entry.OID {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return strings.Trim(entry.OID, "0") != ""
}

func safeText(text string) bool {
	if !utf8.ValidString(text) {
		return false
	}
	for _, char := range text {
		if unicode.IsControl(char) && char != '\n' && char != '\t' {
			return false
		}
	}
	return true
}

func changedPaths(base, target Tree) []string {
	paths := make(map[string]bool)
	for name, entry := range base {
		if target[name] != entry {
			paths[name] = true
		}
	}
	for name, entry := range target {
		if base[name] != entry {
			paths[name] = true
		}
	}
	result := make([]string, 0, len(paths))
	for name := range paths {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func planError(format string, args ...any) error {
	return fmt.Errorf("finish plan: "+format, args...)
}

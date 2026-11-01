package ai

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

// FinishSourceEdit contains positions and identity, never unchanged source text.
// ChangeID links the selection to its frozen file witnesses in Changes.
type FinishSourceEdit struct {
	ID         string `json:"id"`
	ChangeID   string `json:"change_id"`
	Kind       string `json:"kind"`
	BeforeLine int    `json:"before_line"`
	AfterLine  int    `json:"after_line"`
}

// Each run owns the selections after the previous endpoint through ThroughID.
// Walking one frozen order makes overlap impossible without repairing AI output.
type finishOwnershipRun struct {
	ThroughID string `json:"through_id"`
	Goal      int    `json:"goal"`
}

func allocateFinishSourceGoals(goals []finish.Goal, catalogue []finish.Change, runs []finishOwnershipRun) ([]finish.Goal, error) {
	result := slices.Clone(goals)
	for index, goal := range result {
		if len(goal.ChangeIDs) != 0 {
			return nil, errors.New("use ownership runs instead of goal change_ids")
		}
		result[index].ChangeIDs = nil
	}
	positions := make(map[string]int, len(catalogue))
	for index, selection := range catalogue {
		positions[selection.ID] = index
	}
	next := 0
	for _, run := range runs {
		end, known := positions[run.ThroughID]
		if !known || end < next || run.Goal < 1 || run.Goal > len(result) {
			return nil, fmt.Errorf("ownership endpoints must advance through selection_order with a valid goal: %q, goal %d", run.ThroughID, run.Goal)
		}
		for _, selection := range catalogue[next : end+1] {
			result[run.Goal-1].ChangeIDs = append(result[run.Goal-1].ChangeIDs, selection.ID)
		}
		next = end + 1
	}
	if next != len(catalogue) {
		return nil, errors.New("ownership must end at the last selection_order entry")
	}
	return result, nil
}

// SourceCheckFailure keeps raw approved-command output local. Only the step and
// diagnostic names already visible in approved redacted diffs can reach AI.
type SourceCheckFailure struct {
	Step   int
	Output string
}

func (failure *SourceCheckFailure) Error() string {
	return fmt.Sprintf("approved project checks rejected source step %d", failure.Step)
}

var sourceDiagnosticNames = regexp.MustCompile(`(?i)(?:value of type ['"]([A-Za-z_][A-Za-z_0-9]*)['"] has no member ['"]([A-Za-z_][A-Za-z_0-9]*)['"]|cannot find ['"]([A-Za-z_][A-Za-z_0-9]*)['"] in scope|has no member ['"]([A-Za-z_][A-Za-z_0-9]*)['"]|undefined:\s*([A-Za-z_][A-Za-z_0-9]*)|\b([A-Za-z_][A-Za-z_0-9]*)\s+undefined\b)`)

func sourceCheckFeedback(failure *SourceCheckFailure, approved []FinishDiff) error {
	visible := make(map[string]bool)
	var paths []string
	for _, diff := range approved {
		if len(paths) < 8 && strings.Contains(failure.Output, "/"+diff.ChangeID+":") && strings.Contains(diff.Text, diff.ChangeID) {
			paths = append(paths, diff.ChangeID)
		}
		for _, word := range strings.FieldsFunc(diff.Text, func(r rune) bool {
			return !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_')
		}) {
			visible[word] = true
		}
	}
	seen := make(map[string]bool)
	var names []string
	for _, match := range sourceDiagnosticNames.FindAllStringSubmatch(failure.Output, 64) {
		for _, name := range match[1:] {
			if visible[name] && !seen[name] && len(name) <= 128 && len(names) < 16 {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	detail := ""
	if len(names) > 0 {
		detail = "; diagnostic names already present in approved diffs: " + strings.Join(names, ", ")
	}
	if len(paths) > 0 {
		detail += "; affected approved files: " + strings.Join(paths, ", ")
	}
	return fmt.Errorf("%s%s. Correct the complete selections and prerequisite order; keep definitions no later than their first consumer", failure.Error(), detail)
}

func sourceChecksFeedback(err error, approved []FinishDiff) error {
	if combined, ok := err.(interface{ Unwrap() []error }); ok {
		var feedback []error
		for _, failure := range combined.Unwrap() {
			feedback = append(feedback, sourceChecksFeedback(failure, approved))
		}
		return errors.Join(feedback...)
	}
	var failure *SourceCheckFailure
	if errors.As(err, &failure) {
		return sourceCheckFeedback(failure, approved)
	}
	return errors.New("source verification also failed without approved diagnostics")
}

func finishSourceCatalogue(evidence finish.Evidence, files []finish.SourceFile) ([]FinishSourceEdit, []finish.Change, error) {
	byChange := make(map[string]finish.SourceFile)
	for _, file := range files {
		if _, exists := byChange[file.ChangeID]; exists {
			return nil, nil, fmt.Errorf("duplicate source catalogue %q", file.ChangeID)
		}
		byChange[file.ChangeID] = file
	}
	var edits []FinishSourceEdit
	var catalogue []finish.Change
	var ids []string
	for _, change := range evidence.Changes {
		file, exists := byChange[change.ID]
		if !exists {
			catalogue = append(catalogue, change)
			ids = append(ids, change.ID)
			continue
		}
		for _, edit := range file.Edits {
			edits = append(edits, FinishSourceEdit{edit.ID, change.ID, edit.Kind, edit.BeforeLine, edit.AfterLine})
			// Used only for message-quality path lookup, not version replay.
			catalogue = append(catalogue, finish.Change{ID: edit.ID, Path: change.Path, Author: change.Author})
			ids = append(ids, edit.ID)
		}
	}
	// Prove the entire local catalogue before spending a provider call.
	_, err := finish.BuildSourcePlan(evidence, files, []finish.Goal{{Purpose: "Validate frozen source selections", Message: "Validate frozen source selections", ChangeIDs: ids}})
	return edits, catalogue, err
}

func expandFinishSourceGoals(goals []finish.Goal, catalogue []finish.Change, edits []FinishSourceEdit) ([]finish.Goal, error) {
	known, sources := make(map[string]bool), make(map[string]bool)
	for _, change := range catalogue {
		known[change.ID] = true
	}
	for _, edit := range edits {
		sources[edit.ID] = true
	}
	result := append([]finish.Goal(nil), goals...)
	var problems []error
	selected := 0
	for index, goal := range goals {
		var ids []string
		for _, id := range goal.ChangeIDs {
			members := []string{id}
			if !known[id] {
				first, last, isRange := strings.Cut(id, "..")
				if !isRange || !sources[first] || !sources[last] || len(first) < 6 || len(last) < 6 || first[:len(first)-6] != last[:len(last)-6] {
					problems = append(problems, fmt.Errorf("unknown source selection %q", id))
				} else {
					start, e1 := strconv.Atoi(first[len(first)-6:])
					end, e2 := strconv.Atoi(last[len(last)-6:])
					if e1 != nil || e2 != nil || start > end || end-start >= len(catalogue) {
						problems = append(problems, fmt.Errorf("invalid source selection range %q", id))
					} else {
						members = nil
						for n := start; n <= end; n++ {
							member := fmt.Sprintf("%s%06d", first[:len(first)-6], n)
							if !sources[member] {
								problems = append(problems, fmt.Errorf("source range %q contains unknown edit %q", id, member))
							}
							members = append(members, member)
						}
					}
				}
			}
			if selected+len(members) > len(catalogue) {
				result[index].ChangeIDs = append(ids, id)
				return result, errors.Join(append(problems, errors.New("source membership exceeds the complete catalogue"))...)
			}
			selected += len(members)
			ids = append(ids, members...)
		}
		result[index].ChangeIDs = ids
	}
	return result, errors.Join(problems...)
}

const finishSourcePrompt = `
This request provides source_edits and selection_order. These rules replace recorded-version membership and ordering ONLY for represented Changes entries. Return goals in prerequisite order with purpose, boundary, reason and message, without change_ids. Return ownership as an array of {through_id, goal}, where goal is a 1-based index into goals. Walk selection_order exactly once from first to last: each run owns every entry after the preceding endpoint up to and including through_id. The first run starts at the first entry. Endpoints must strictly advance and the last must be the last selection_order entry. Do not output overlapping per-goal lists or calculate range lengths. For example, runs ending at edit-000003 with goal 2 and edit-000005 with goal 1 assign the first three ordered entries to goal 2 and the next two to goal 1. Ownership order follows the catalogue, not commit order. Every edit and whole recorded change must have one owner.
An insertion copies the exact target line at its base-relative anchor; a deletion removes the exact base line. ACD constructs cumulative subsets from those immutable bytes and cannot invent code. New Go and Swift files may be introduced in complete working stages using their exact target lines. Other new files and rename groups remain whole. Shared files and UI integration do not make unrelated outcomes one goal. Separate a feature's independently testable implementation from an independent Settings or layout reorganization, including when both edit a newly added view. A complete API, model or processing pipeline with its tests can land before its main-window entry point. Put the small connection to that prerequisite with the later UI consumer when the exact target line couples feature wiring to the new layout. Do not require an invented version of the old UI to expose the feature first. Feature-specific settings can land with their feature or its later interface consumer; reorganizing existing settings and daily controls is a separate decision. Do not turn those two outcomes into one integration goal. Keep each intermediate file syntactically complete. The first owner of a new file must include its required imports, declarations and closing syntax; later owners can add independent complete members or sections inside that structure.
Return goals in a valid prerequisite order. ACD does not impose whole-file version order on independent source selections. Shared supporting edits belong once to the earliest goal that needs them; later consumers depend on that goal, not a repeated ID. Pair replacements and retain complete function and container syntax. Keep callback signatures with their consumers. Definitions must precede their first consumer. When removing or moving an interface, migrate every old caller in the same step or an earlier one; ordering a new definition first is not enough if a caller still uses the removed receiver. Trace those calls in the exact code before returning ownership. Rename groups remain whole. Scope and diff boundaries are evidence, never purposes. ACD will run approved project checks on every candidate before publication.
When evidence_reviews is present, each summary maps distinct mechanisms to source edit IDs or ranges. Selectable files also retain their exact approved annotated diffs: trace definitions, signatures, syntax and consumers there before assigning IDs. Use the mapped observations for context alongside that code; line positions and summaries alone cannot establish valid source selections. Whole recorded changes may have only observations. Related observations may span fragments and files. The mappings describe code, not prescribed commit boundaries.
Do not claim independent mechanisms are inseparable merely because they occur in one initializer, callback, file, or replacement region. Plan useful completed decisions first, then select their complete implementation, wiring and regression coverage. Respect actual byte constraints: if a line truly cannot be separated, choose a working prerequisite order or explain the concrete conflict. Retain the same grounded reason and message quality requirements. Before refusing a split, consider an independently testable prerequisite followed by its separate UI consumer. Lack of executed checks is not a block: return a grounded complete candidate for the approved checks. Refuse only a concrete conflicting selection, identifying its exact edit IDs and why no prerequisite-first order works; do not refuse merely because the complete ownership has not yet been written. Never invent code or merge unrelated outcomes to avoid selection work.`

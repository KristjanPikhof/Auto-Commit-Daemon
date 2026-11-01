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

// SourceCheckFailure keeps raw approved-command output local. Only the step and
// diagnostic names already visible in approved redacted diffs can reach AI.
type SourceCheckFailure struct {
	Step   int
	Output string
}

func (failure *SourceCheckFailure) Error() string {
	return fmt.Sprintf("approved project checks rejected source step %d", failure.Step)
}

var sourceDiagnosticNames = regexp.MustCompile(`(?i)(?:cannot find ['"]([A-Za-z_][A-Za-z_0-9]*)['"] in scope|has no member ['"]([A-Za-z_][A-Za-z_0-9]*)['"]|undefined:\s*([A-Za-z_][A-Za-z_0-9]*)|\b([A-Za-z_][A-Za-z_0-9]*)\s+undefined\b)`)

func sourceCheckFeedback(failure *SourceCheckFailure, approved []FinishDiff) error {
	visible := make(map[string]bool)
	for _, diff := range approved {
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
	return fmt.Errorf("%s%s. Correct the complete selections and prerequisite order; keep definitions no later than their first consumer", failure.Error(), detail)
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
This request provides source_edits. These rules replace recorded-version membership and ordering ONLY for the represented Changes entries. The Changes entries remain frozen file witnesses, not selectable whole-file IDs when source_edits refer to them. Assign every source_edits ID exactly once; assign every other Changes ID as one whole entry. An insertion copies the exact target line at its base-relative anchor; a deletion removes the exact base line. ACD constructs cumulative subsets from those immutable bytes. It cannot invent new code. You may abbreviate consecutive numeric edit IDs using an inclusive range such as edit-000001..edit-000008; every ID in the range must exist and belong to this goal.
Return goals in a valid prerequisite order. ACD does not impose whole-file version order on independent source selections. Shared supporting edits belong once to the earliest goal that needs them; later consumers depend on that goal, not a repeated ID. Pair replacements and retain complete function and container syntax. Keep callback signatures with their consumers. Definitions must precede their first consumer. New files and rename groups remain whole. Scope and diff boundaries are evidence, never purposes. ACD will run approved project checks on every candidate before publication.
When evidence_reviews is present, each summary maps distinct mechanisms to source edit IDs or ranges. Selectable files also retain their exact approved annotated diffs: trace definitions, signatures, syntax and consumers there before assigning IDs. Use the mapped observations for context alongside that code; line positions and summaries alone cannot establish valid source selections. Whole recorded changes may have only observations. Related observations may span fragments and files. The mappings describe code, not prescribed commit boundaries.
Do not claim independent mechanisms are inseparable merely because they occur in one initializer, callback, file, or replacement region. Plan useful completed decisions first, then select their complete implementation, wiring and regression coverage. Respect actual byte constraints: if a line truly cannot be separated, choose a working prerequisite order or explain the concrete conflict. Retain the same grounded reason and message quality requirements. Refuse unsupported purposes rather than inventing code or merging unrelated outcomes to avoid selection work.`

const finishSourceReviewPrompt = `
Each evidence_review.parts entry may include source_edit_ids for the exact annotated lines in that fragment. In its summary, associate each distinct mechanism and its supporting edits with their IDs or inclusive ranges such as edit-000001..edit-000008. Cover every supplied source_edit_ids entry exactly once. Use only that fragment's IDs; name cross-fragment prerequisites by interface instead. Pair replacements, and distinguish interface definitions, consumers, imports, tests and build wiring so the global planner can construct valid intermediate steps. Do not append an undifferentiated list of IDs to a file-level summary or group unrelated edits by fragment or scope. A long annotated line may span fragments; describe only the visible evidence for its supplied ID. The global planner receives these mapped summaries, the complete edit catalogue and exact approved diffs for selectable files. Positions are metadata; raw scope expressions stay local.`

// Associate fragments with exact annotated lines before splitting can separate
// an ID from the rest of a long line. Neither code nor fragment bytes change.
func finishReviewSourceIDs(texts []string, ids []string) ([][]string, error) {
	mapped := make([][]string, len(texts))
	if len(ids) == 0 {
		return mapped, nil
	}
	remaining := make(map[string]bool, len(ids))
	for _, id := range ids {
		remaining[id] = true
	}
	ends := make([]int, len(texts))
	total := 0
	for i, text := range texts {
		total += len(text)
		ends[i] = total
	}
	offset, fragment := 0, 0
	for line := range strings.SplitAfterSeq(strings.Join(texts, ""), "\n") {
		end := offset + len(line)
		if strings.HasPrefix(line, "[") {
			id, code, ok := strings.Cut(line[1:], "] ")
			if ok && remaining[id] && (strings.HasPrefix(code, "+") || strings.HasPrefix(code, "-")) {
				for fragment < len(ends) && ends[fragment] <= offset {
					fragment++
				}
				for i := fragment; i < len(ends); i++ {
					mapped[i] = append(mapped[i], id)
					if ends[i] >= end {
						break
					}
				}
				delete(remaining, id)
			}
		}
		offset = end
	}
	if len(remaining) != 0 {
		return nil, errors.New("source review evidence must retain every edit annotation")
	}
	return mapped, nil
}

var finishReviewSourceSelection = regexp.MustCompile(`\b_*edit-[0-9]{6}(?:\.\._*edit-[0-9]{6})?\b`)

func validateFinishReviewSourceIDs(summary string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	catalogue := make([]finish.Change, 0, len(ids))
	edits := make([]FinishSourceEdit, 0, len(ids))
	for _, id := range ids {
		catalogue = append(catalogue, finish.Change{ID: id})
		edits = append(edits, FinishSourceEdit{ID: id})
	}
	goals, err := expandFinishSourceGoals([]finish.Goal{{ChangeIDs: finishReviewSourceSelection.FindAllString(summary, -1)}}, catalogue, edits)
	if err != nil {
		return fmt.Errorf("source summary must map only its supplied edit IDs: %w", err)
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range goals[0].ChangeIDs {
		if seen[id] {
			return fmt.Errorf("source summary maps %s more than once", id)
		}
		seen[id] = true
	}
	if len(seen) != len(ids) {
		return errors.New("source summary must associate every supplied edit ID with its mechanism using IDs or inclusive ranges")
	}
	return nil
}

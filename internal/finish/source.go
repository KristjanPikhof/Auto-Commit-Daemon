package finish

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"maps"
	"sort"
	"unicode/utf8"
)

// SourceEdit selects one existing line. A deletion names a BeforeLine; an
// insertion copies AfterLine before BeforeLine in the original file. Positions
// are zero-based and never refer to a partly constructed intermediate file.
type SourceEdit struct {
	ID         string
	Kind       string
	BeforeLine int
	AfterLine  int
}

// SourceFile binds a complete edit catalogue to one frozen net change. Modes,
// paths, authors and object identities come from that change, not the planner.
type SourceFile struct {
	ChangeID string
	Before   []byte
	After    []byte
	Edits    []SourceEdit
}

// SourcePlan translates source selections into exact file-version evidence for
// the ordinary plan validator. Blobs must be written and the complete candidate
// chain checked by the caller before publication. Structural validity alone
// does not prove that intermediate code compiles or completes a useful purpose.
type SourcePlan struct {
	Evidence   Evidence
	Plan       Plan
	Selections []Goal
	Blobs      map[string][]byte
}

type sourceFile struct {
	change Change
	before [][]byte
	after  [][]byte
	edits  []SourceEdit
}

// BuildSourcePlan accepts only selections of the supplied frozen bytes. It
// cannot accept new code, omit surviving edits, cross authors, or split renames.
// Files without a source catalogue remain indivisible recorded changes.
func BuildSourcePlan(evidence Evidence, files []SourceFile, goals []Goal) (SourcePlan, error) {
	if err := ValidateEvidence(evidence); err != nil {
		return SourcePlan{}, err
	}
	original := make(map[string]Change)
	byPath := make(map[string]Change)
	catalogue := make(map[string]Change)
	for _, change := range evidence.Changes {
		if _, exists := byPath[change.Path]; exists || len(change.DependsOn) != 0 ||
			change.Before != evidence.Base[change.Path] || change.After != evidence.Target[change.Path] {
			return SourcePlan{}, planError("source selections require frozen net changes")
		}
		original[change.ID], byPath[change.Path], catalogue[change.ID] = change, change, change
	}
	sources := make(map[string]sourceFile)
	for _, file := range files {
		change, ok := original[file.ChangeID]
		if !ok {
			return SourcePlan{}, planError("unknown source change %q", file.ChangeID)
		}
		if _, exists := sources[change.Path]; exists {
			return SourcePlan{}, planError("duplicate source file %q", change.Path)
		}
		if change.AtomicGroup != "" || (change.Before.Mode != "100644" && change.Before.Mode != "100755") ||
			change.Before.Mode != change.After.Mode || len(change.Before.OID) != len(change.After.OID) {
			return SourcePlan{}, planError("source selections require an existing text file with unchanged mode: %q", change.Path)
		}
		if !utf8.Valid(file.Before) || !utf8.Valid(file.After) || bytes.IndexByte(file.Before, 0) >= 0 || bytes.IndexByte(file.After, 0) >= 0 ||
			sourceBlobOID(file.Before, len(change.Before.OID)) != change.Before.OID || sourceBlobOID(file.After, len(change.After.OID)) != change.After.OID {
			return SourcePlan{}, planError("source bytes do not match the frozen objects for %q", change.Path)
		}
		source := sourceFile{change: change, before: sourceLines(file.Before), after: sourceLines(file.After), edits: file.Edits}
		deletions, insertions := make(map[int]bool), make(map[int]bool)
		delete(catalogue, change.ID)
		for _, edit := range file.Edits {
			if edit.ID == "" || !safeText(edit.ID) {
				return SourcePlan{}, planError("source edit needs a safe identifier")
			}
			if _, exists := catalogue[edit.ID]; exists {
				return SourcePlan{}, planError("duplicate source edit %q", edit.ID)
			}
			if _, exists := original[edit.ID]; exists {
				return SourcePlan{}, planError("source edit %q collides with a recorded change", edit.ID)
			}
			switch edit.Kind {
			case "delete":
				if edit.BeforeLine < 0 || edit.BeforeLine >= len(source.before) || edit.AfterLine != 0 || deletions[edit.BeforeLine] {
					return SourcePlan{}, planError("invalid or repeated deletion %q", edit.ID)
				}
				deletions[edit.BeforeLine] = true
			case "insert":
				if edit.BeforeLine < 0 || edit.BeforeLine > len(source.before) || edit.AfterLine < 0 || edit.AfterLine >= len(source.after) || insertions[edit.AfterLine] {
					return SourcePlan{}, planError("invalid or repeated insertion %q", edit.ID)
				}
				insertions[edit.AfterLine] = true
			default:
				return SourcePlan{}, planError("unknown source edit kind %q", edit.Kind)
			}
			catalogue[edit.ID] = change
		}
		all := make(map[string]bool)
		for _, edit := range file.Edits {
			all[edit.ID] = true
		}
		if !bytes.Equal(source.compose(all), file.After) {
			return SourcePlan{}, planError("source edits do not reproduce the frozen target for %q", change.Path)
		}
		sources[change.Path] = source
	}
	used := make(map[string]bool)
	for _, goal := range goals {
		for _, id := range goal.ChangeIDs {
			if _, exists := catalogue[id]; !exists || used[id] {
				return SourcePlan{}, planError("unknown or multiply assigned source selection %q", id)
			}
			used[id] = true
		}
	}
	if len(used) != len(catalogue) {
		return SourcePlan{}, planError("source plan must assign every frozen edit exactly once")
	}
	result := SourcePlan{Evidence: Evidence{Base: CloneTree(evidence.Base), Target: CloneTree(evidence.Target)}, Blobs: make(map[string][]byte)}
	current, selected := CloneTree(evidence.Base), make(map[string]bool)
	var derivedGoals []Goal
	for index, goal := range goals {
		previous := CloneTree(current)
		touched := make(map[string]bool)
		var author string
		for n, id := range goal.ChangeIDs {
			change := catalogue[id]
			if n > 0 && author != change.Author {
				return SourcePlan{}, planError("source step %d crosses an author boundary", index+1)
			}
			author = change.Author
			selected[id] = true
			if _, exists := sources[change.Path]; exists {
				touched[change.Path] = true
			} else {
				if err := apply(current, change); err != nil {
					return SourcePlan{}, err
				}
			}
		}
		for path := range touched {
			source := sources[path]
			content := source.compose(selected)
			oid := sourceBlobOID(content, len(source.change.Before.OID))
			current[path] = Entry{Mode: source.change.Before.Mode, OID: oid}
			if oid != source.change.Before.OID && oid != source.change.After.OID {
				result.Blobs[oid] = content
			}
		}
		selection := goal
		selection.ChangeIDs = append([]string(nil), goal.ChangeIDs...)
		result.Selections = append(result.Selections, selection)
		goal.ChangeIDs = nil
		for n, change := range NetChanges(previous, current, author) {
			origin := byPath[change.Path]
			change.ID = fmt.Sprintf("source-step-%d-%d", index+1, n+1)
			change.Author = origin.Author
			change.AtomicGroup = origin.AtomicGroup
			change.Provenance = origin.Provenance
			if change.Provenance == "" {
				change.Provenance = origin.ID
			}
			result.Evidence.Changes = append(result.Evidence.Changes, change)
			goal.ChangeIDs = append(goal.ChangeIDs, change.ID)
		}
		derivedGoals = append(derivedGoals, goal)
	}
	if !maps.Equal(current, evidence.Target) {
		return SourcePlan{}, planError("source plan changed the frozen target")
	}
	plan, err := Build(result.Evidence, derivedGoals)
	if err != nil {
		return SourcePlan{}, err
	}
	result.Plan = plan
	return result, nil
}

func (source sourceFile) compose(selected map[string]bool) []byte {
	deleted := make(map[int]bool)
	inserted := make(map[int][]SourceEdit)
	for _, edit := range source.edits {
		if !selected[edit.ID] {
			continue
		}
		if edit.Kind == "delete" {
			deleted[edit.BeforeLine] = true
		} else {
			inserted[edit.BeforeLine] = append(inserted[edit.BeforeLine], edit)
		}
	}
	var result bytes.Buffer
	for index := 0; index <= len(source.before); index++ {
		edits := inserted[index]
		sort.Slice(edits, func(i, j int) bool { return edits[i].AfterLine < edits[j].AfterLine })
		for _, edit := range edits {
			result.Write(source.after[edit.AfterLine])
		}
		if index < len(source.before) && !deleted[index] {
			result.Write(source.before[index])
		}
	}
	return result.Bytes()
}

func sourceLines(content []byte) [][]byte {
	if len(content) == 0 {
		return nil
	}
	lines := bytes.SplitAfter(content, []byte{'\n'})
	if len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func sourceBlobOID(content []byte, oidLength int) string {
	var digest hash.Hash = sha1.New()
	if oidLength == 64 {
		digest = sha256.New()
	}
	fmt.Fprintf(digest, "blob %d\x00", len(content))
	digest.Write(content)
	return hex.EncodeToString(digest.Sum(nil))
}

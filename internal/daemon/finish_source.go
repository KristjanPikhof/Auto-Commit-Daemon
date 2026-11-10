package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

type finishSourceFileProof struct {
	ChangeID string              `json:"change_id"`
	Path     string              `json:"path"`
	Before   finish.Entry        `json:"before"`
	After    finish.Entry        `json:"after"`
	Edits    []finish.SourceEdit `json:"edits"`
}

type finishSourceProof struct {
	Version    int                     `json:"version"`
	Files      []finishSourceFileProof `json:"files"`
	Selections []finish.Goal           `json:"selections"`
}

type finishPlanningResult struct {
	Plan       finish.Plan
	Evidence   finish.Evidence
	Disclosure ai.FinishDisclosure
	Blobs      map[string][]byte
	Source     *finishSourceProof
	Checked    *finishPreparedPlan
}

type finishPreparedPlan struct {
	saved FinishSavedPlan
	proof git.FinishPublicationProof
}

type finishSourcePreparation struct {
	files []finish.SourceFile
	diffs []ai.FinishDiff
}

// Refine the named transition through the final version on its path so later
// edits to either purpose can stay with that purpose. Preserve earlier
// versions and all history on other paths.
func prepareRecordedFinishSources(ctx context.Context, repo, targetTree string, evidence finish.Evidence, diffs []ai.FinishDiff, maxBytes int, reason string) (ai.FinishBoundaryRefinement, error) {
	var result ai.FinishBoundaryRefinement
	starts := make(map[string]finish.Change)
	for _, change := range evidence.Changes {
		if change.Provenance != "" && starts[change.Path].ID == "" && strings.Contains(reason, change.ID) {
			starts[change.Path] = change
			if len(starts) == 8 {
				break
			}
		}
	}
	if len(starts) == 0 {
		return result, nil
	}
	required := make(map[string]bool)
	for _, change := range evidence.Changes {
		for _, id := range change.DependsOn {
			required[id] = true
		}
	}
	// Skip paths where replacing versions would erase a recorded dependency,
	// rename boundary or author boundary.
	active := make(map[string]bool)
	for _, change := range evidence.Changes {
		start, ok := starts[change.Path]
		if !ok {
			continue
		}
		active[change.Path] = active[change.Path] || start.ID == change.ID
		if active[change.Path] && (required[change.ID] || len(change.DependsOn) > 0 || change.AtomicGroup != "" || change.Author != start.Author) {
			delete(starts, change.Path)
		}
	}
	var selected finish.Evidence
	for _, change := range evidence.Changes {
		if starts[change.Path].ID == change.ID && change.Before != evidence.Target[change.Path] {
			change.After, change.AfterTree = evidence.Target[change.Path], targetTree
			selected.Changes = append(selected.Changes, change)
		}
	}
	if len(selected.Changes) == 0 {
		return result, nil
	}
	projectedDiffs, err := finishProviderDiffs(ctx, repo, "", targetTree, selected.Changes, true, maxBytes)
	if err != nil {
		return result, err
	}
	files, annotated, err := prepareFinishSources(ctx, repo, selected, projectedDiffs, maxBytes, true)
	if err != nil || len(files) == 0 {
		return result, err
	}
	byID := make(map[string]finish.Change)
	for _, change := range selected.Changes {
		byID[change.ID] = change
	}
	refined := make(map[string]finish.Change)
	for _, file := range files {
		refined[file.ChangeID] = byID[file.ChangeID]
	}
	result = ai.FinishBoundaryRefinement{Evidence: finish.Evidence{Base: evidence.Base, Target: evidence.Target}, Files: files, Replaced: make(map[string]string)}
	replacedPath := make(map[string]string)
	for _, change := range evidence.Changes {
		if replacement, ok := refined[change.ID]; ok {
			replacedPath[change.Path] = change.ID
			result.Evidence.Changes = append(result.Evidence.Changes, replacement)
		} else if id := replacedPath[change.Path]; id != "" {
			result.Replaced[change.ID] = id
		} else {
			result.Evidence.Changes = append(result.Evidence.Changes, change)
		}
	}
	for _, diff := range diffs {
		if _, ok := refined[diff.ChangeID]; !ok && result.Replaced[diff.ChangeID] == "" {
			result.Diffs = append(result.Diffs, diff)
		}
	}
	for _, diff := range annotated {
		if _, ok := refined[diff.ChangeID]; ok {
			result.Diffs = append(result.Diffs, diff)
		}
	}
	return result, nil
}

func sourceProof(evidence finish.Evidence, files []finish.SourceFile, selections []finish.Goal) *finishSourceProof {
	proof := &finishSourceProof{Version: 1, Selections: selections}
	byID := make(map[string]finish.Change)
	for _, change := range evidence.Changes {
		byID[change.ID] = change
	}
	for _, file := range files {
		change := byID[file.ChangeID]
		proof.Files = append(proof.Files, finishSourceFileProof{ChangeID: file.ChangeID, Path: change.Path, Before: change.Before, After: change.After, Edits: file.Edits})
	}
	return proof
}

var finishSourceHunk = regexp.MustCompile(`(?m)^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@`)

// Bound catalogue construction before encoding its descriptors and evidence.
const finishSourceEditLimit = ai.FinishPlanningByteCap / 128

// Only frozen objects are read here. Scope hashes stay local; the provider sees
// selected diff lines and their IDs, never compiler dumps or enclosing secrets.
func prepareFinishSources(ctx context.Context, repo string, evidence finish.Evidence, diffs []ai.FinishDiff, maxBytes int, newFiles bool) ([]finish.SourceFile, []ai.FinishDiff, error) {
	limit, err := ai.FinishInputByteLimit(maxBytes)
	if err != nil {
		return nil, nil, err
	}
	dir, err := os.MkdirTemp("", "acd-finish-source-")
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(dir)
	result := append([]ai.FinishDiff(nil), diffs...)
	byID := make(map[string]int)
	for i, diff := range diffs {
		byID[diff.ChangeID] = i
	}
	prefix := "edit-"
	for {
		collision := false
		for _, change := range evidence.Changes {
			collision = collision || strings.HasPrefix(change.ID, prefix)
		}
		if !collision {
			break
		}
		prefix = "_" + prefix
	}
	var files []finish.SourceFile
	usedBytes, nextID := 0, 0
	for _, change := range evidence.Changes {
		i, exists := byID[change.ID]
		created := change.Before == (finish.Entry{})
		if !exists || result[i].Binary || change.AtomicGroup != "" ||
			(change.After.Mode != "100644" && change.After.Mode != "100755") ||
			(!created && change.Before.Mode != change.After.Mode) ||
			(created && (!newFiles || result[i].AfterSize == 0 || (filepath.Ext(change.Path) != ".swift" && filepath.Ext(change.Path) != ".go"))) {
			continue
		}
		if result[i].BeforeSize+result[i].AfterSize > int64(limit-usedBytes) {
			continue // This file remains one complete, fully evidenced transition.
		}
		var before []byte
		if !created {
			before, err = git.CatFileBlobLimited(ctx, repo, change.Before.OID, int64(limit-usedBytes))
			if err != nil {
				return nil, nil, err
			}
		}
		after, err := git.CatFileBlobLimited(ctx, repo, change.After.OID, int64(limit-usedBytes-len(before)))
		if err != nil {
			return nil, nil, err
		}
		if !utf8.Valid(before) || !utf8.Valid(after) || bytes.IndexByte(before, 0) >= 0 || bytes.IndexByte(after, 0) >= 0 {
			continue
		}
		file, text, err := finishSourceDiff(ctx, repo, dir, change, before, after, prefix, &nextID, limit)
		if err != nil {
			return nil, nil, fmt.Errorf("prepare source selections for %q: %w", change.Path, err)
		}
		usedBytes += len(before) + len(after)
		files = append(files, file)
		result[i].Text = text
	}
	return files, result, nil
}

func finishSourceLines(content []byte) [][]byte {
	if len(content) == 0 {
		return nil
	}
	lines := bytes.SplitAfter(content, []byte{'\n'})
	if len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func finishSourceDiff(ctx context.Context, repo, dir string, change finish.Change, before, after []byte, prefix string, nextID *int, limit int) (finish.SourceFile, string, error) {
	file := finish.SourceFile{ChangeID: change.ID, Before: before, After: after}
	a, b := finishSourceLines(before), finishSourceLines(after)
	keysA, err := finishSourceScopes(ctx, dir, change.Path, before)
	if err != nil {
		return file, "", err
	}
	keysB, err := finishSourceScopes(ctx, dir, change.Path, after)
	if err != nil {
		return file, "", err
	}
	write := func(name string, lines [][]byte, keys []string) (string, error) {
		if len(lines) > (4*limit-len(before)-len(after))/65 {
			return "", errors.New("scope-aligned source evidence exceeds its byte limit")
		}
		var annotated bytes.Buffer
		for i, line := range lines {
			fmt.Fprintf(&annotated, "%x\t", sha256.Sum256([]byte(keys[i])))
			annotated.Write(line)
		}
		path := filepath.Join(dir, name)
		return path, os.WriteFile(path, annotated.Bytes(), 0600)
	}
	oldPath, err := write("before", a, keysA)
	if err != nil {
		return file, "", err
	}
	newPath, err := write("after", b, keysB)
	if err != nil {
		return file, "", err
	}
	raw, diffErr := git.RunWithLimit(ctx, git.RunOpts{Dir: repo}, int64(4*limit), "diff", "--no-index", "--text", "--no-color", "--no-ext-diff", "--no-textconv", "--diff-algorithm=myers", "--unified=0", "--", oldPath, newPath)
	var exit *exec.ExitError
	if diffErr != nil && !(errors.As(diffErr, &exit) && exit.ExitCode() == 1) {
		return file, "", diffErr
	}
	var text strings.Builder
	fmt.Fprintf(&text, "diff --acd-source %q\n", change.Path)
	for _, match := range finishSourceHunk.FindAllStringSubmatch(string(raw), -1) {
		startA, countA := finishSourceRange(match[1], match[2])
		startB, countB := finishSourceRange(match[3], match[4])
		if startA < 0 || startA+countA > len(a) || startB < 0 || startB+countB > len(b) {
			return file, "", errors.New("source diff returned invalid positions")
		}
		fmt.Fprintf(&text, "@@ base %d,%d target %d,%d @@\n", startA+1, countA, startB+1, countB)
		for n := max(0, startA-3); n < startA; n++ {
			writeSourceEvidenceLine(&text, " ", a[n])
		}
		for n := startA; n < startA+countA; n++ {
			if *nextID >= finishSourceEditLimit {
				return file, "", &ai.FinishPlanningError{Code: "provider_input_too_large", Err: errors.New("source edit catalogue exceeds the bounded global planning context; finish verified milestones to retain useful file versions")}
			}
			*nextID++
			id := fmt.Sprintf("%s%06d", prefix, *nextID)
			file.Edits = append(file.Edits, finish.SourceEdit{ID: id, Kind: "delete", BeforeLine: n})
			writeSourceEvidenceLine(&text, "["+id+"] -", a[n])
		}
		for n := startB; n < startB+countB; n++ {
			if *nextID >= finishSourceEditLimit {
				return file, "", &ai.FinishPlanningError{Code: "provider_input_too_large", Err: errors.New("source edit catalogue exceeds the bounded global planning context; finish verified milestones to retain useful file versions")}
			}
			*nextID++
			id := fmt.Sprintf("%s%06d", prefix, *nextID)
			file.Edits = append(file.Edits, finish.SourceEdit{ID: id, Kind: "insert", BeforeLine: startA, AfterLine: n})
			writeSourceEvidenceLine(&text, "["+id+"] +", b[n])
		}
		for n := startA + countA; n < min(len(a), startA+countA+3); n++ {
			writeSourceEvidenceLine(&text, " ", a[n])
		}
	}
	return file, text.String(), nil
}

func finishSourceRange(start, count string) (int, int) {
	first, _ := strconv.Atoi(start)
	size := 1
	if count != "" {
		size, _ = strconv.Atoi(count)
	}
	if size > 0 {
		first--
	}
	return first, size
}

func writeSourceEvidenceLine(out *strings.Builder, prefix string, line []byte) {
	out.WriteString(prefix)
	out.Write(line)
	if !bytes.HasSuffix(line, []byte{'\n'}) {
		out.WriteString("\n\\ No newline at end of file\n")
	}
}

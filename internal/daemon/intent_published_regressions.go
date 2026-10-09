package daemon

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type intentGoRegressionSource struct {
	packageName string
	names       map[string]bool
	types       map[string]bool
	calls       map[string]bool
	stems       map[string]bool
}

// Discovery is bounded and same-directory only. Directory proximity admits a
// lookup, never a relationship: a recorded direct call must match exact source
// ownership before the shared published post-image proof admits the context.
func discoverIntentPublishedGoRegressions(ctx context.Context, db *state.DB, input IntentCandidateEvaluation) ([]state.IntentCandidate, map[string]map[string]string, error) {
	sources := make(map[string]intentGoRegressionSource)
	for _, capture := range input.Captures {
		if path.Ext(capture.Event.Path) != ".go" || strings.HasSuffix(capture.Event.Path, "_test.go") {
			continue
		}
		raw, err := BuildOpsDiffWithCap(ctx, input.RepoPath, capture.Ops, intentSourceReferenceScanCap)
		if err != nil {
			return nil, nil, err
		}
		for _, op := range capture.Ops {
			if op.Path != capture.Event.Path {
				continue
			}
			calls, err := loadIntentRecordedGoCalls(ctx, input.RepoPath, op.Path, op.AfterOID.String, op.AfterMode.String, intentCompleteRawDiff(raw), intentSourceReferenceContextCap)
			if err != nil {
				return nil, nil, err
			}
			if len(calls.ownedFunctions) == 0 && len(calls.ownedTypes) == 0 && len(calls.calledFunctions) == 0 {
				continue
			}
			directory := path.Dir(op.Path)
			source, known := sources[directory]
			if known && source.packageName != calls.packageName {
				continue
			}
			if !known {
				source = intentGoRegressionSource{packageName: calls.packageName, names: make(map[string]bool), types: make(map[string]bool), calls: make(map[string]bool), stems: make(map[string]bool)}
			}
			for _, name := range calls.ownedFunctions {
				source.names[name] = true
			}
			for _, name := range calls.ownedTypes {
				source.types[name] = true
			}
			for _, name := range calls.calledFunctions {
				source.calls[name] = true
			}
			source.stems[strings.TrimSuffix(path.Base(op.Path), ".go")] = true
			sources[directory] = source
		}
	}
	if len(sources) == 0 {
		return nil, nil, nil
	}
	var directories []string
	for directory := range sources {
		directories = append(directories, directory)
	}
	sort.Strings(directories)
	if len(directories) > 32 {
		directories = directories[:32]
	}
	args := []any{input.BranchRef, input.BranchGeneration}
	var filters []string
	for _, directory := range directories {
		kind := "substr(event.path,-8)='_test.go'"
		if len(sources[directory].types) > 0 || len(sources[directory].calls) > 0 {
			kind = "substr(event.path,-3)='.go'"
		}
		if directory == "." {
			filters = append(filters, "(instr(event.path,'/')=0 AND "+kind+")")
		} else {
			filters = append(filters, "(substr(event.path,1,?)=? AND instr(substr(event.path,?),'/')=0 AND "+kind+")")
			args = append(args, len(directory)+1, directory+"/", len(directory)+2)
		}
	}
	args = append(args, state.IntentCandidateMaxCaptures)
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT DISTINCT candidate.id,event.path,operation.after_oid,operation.after_mode
FROM capture_events event
JOIN intent_candidate_events member ON member.event_seq=event.seq AND member.membership_state='active'
JOIN intent_candidates candidate ON candidate.id=member.candidate_id
JOIN capture_ops operation ON operation.event_seq=event.seq AND operation.path=event.path
	WHERE event.branch_ref=? AND event.branch_generation=? AND event.state='published'
	 AND candidate.status='published' AND event.commit_oid=candidate.published_commit_oid
	 AND operation.after_oid IS NOT NULL AND operation.after_mode='100644'
	 AND event.seq=(SELECT MAX(latest.seq) FROM capture_events latest
	  WHERE latest.path=event.path AND latest.branch_ref=event.branch_ref
	   AND latest.branch_generation=event.branch_generation AND latest.state='published')
	 AND (`+strings.Join(filters, " OR ")+`)
ORDER BY event.seq DESC LIMIT ?`, args...)
	if err != nil {
		return nil, nil, err
	}
	type recordedTest struct {
		id, path, oid, mode string
		size                int64
		focused             bool
		callee              bool
	}
	var tests []recordedTest
	for rows.Next() {
		var test recordedTest
		if err := rows.Scan(&test.id, &test.path, &test.oid, &test.mode); err != nil {
			rows.Close()
			return nil, nil, err
		}
		tests = append(tests, test)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	var objectNames []string
	for _, test := range tests {
		if intentTypeScriptBaseOID.MatchString(test.oid) {
			objectNames = append(objectNames, test.oid)
		}
	}
	if len(objectNames) == 0 {
		return nil, nil, nil
	}
	inventory, err := git.RunWithLimit(ctx, git.RunOpts{Dir: input.RepoPath, Stdin: strings.NewReader(strings.Join(objectNames, "\n") + "\n")}, 32<<10, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)")
	if err != nil {
		return nil, nil, err
	}
	sizes := make(map[string]int64)
	for _, line := range strings.Split(string(inventory), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[1] != "blob" {
			continue
		}
		if size, err := strconv.ParseInt(fields[2], 10, 64); err == nil && size >= 0 {
			sizes[fields[0]] = size
		}
	}
	for i := range tests {
		tests[i].size = -1
		if size, found := sizes[tests[i].oid]; found {
			tests[i].size = size
		}
		name := path.Base(tests[i].path)
		if !strings.HasSuffix(name, "_test.go") {
			stem := strings.ReplaceAll(strings.TrimSuffix(name, ".go"), "_", "")
			for callee := range sources[path.Dir(tests[i].path)].calls {
				// File names affect bounded lookup order, never the proof.
				tests[i].callee = tests[i].callee || len(stem) >= 8 && strings.Contains(strings.ToLower(callee), stem)
			}
		}
		for stem := range sources[path.Dir(tests[i].path)].stems {
			if strings.HasSuffix(name, "_test.go") && strings.HasPrefix(name, stem+"_") {
				tests[i].focused = true
			}
		}
	}
	// Favor complete small witnesses instead of spending the entire budget
	// reading an unrelated oversized test. This order proves no relationship.
	sort.SliceStable(tests, func(i, j int) bool {
		if tests[i].callee != tests[j].callee {
			return tests[i].callee
		}
		if tests[i].focused != tests[j].focused {
			return tests[i].focused
		}
		return tests[i].size < tests[j].size
	})
	matched := make(map[string]map[string]string)
	// Large unrelated test files cannot turn a focused lookup into a full
	// repository read. Keep the same aggregate bound as supplied evidence.
	remaining := 128 << 10
	for _, test := range tests {
		if remaining <= 0 || test.mode != git.RegularFileMode || test.size < 0 || test.size > int64(remaining) {
			continue
		}
		limit := min(remaining, intentSourceReferenceScanCap)
		contents, err := git.CatFileBlobLimited(ctx, input.RepoPath, test.oid, int64(limit))
		if errors.Is(err, git.ErrStdoutOverflow) {
			remaining -= limit
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		remaining -= len(contents)
		references := intentGoPublishedReferenceContext(test.path, contents, sources[path.Dir(test.path)])
		if references == "" {
			continue
		}
		if matched[test.id] == nil {
			matched[test.id] = make(map[string]string)
		}
		matched[test.id][test.path] = references
	}
	var candidates []state.IntentCandidate
	seen := make(map[string]bool)
	for _, test := range tests {
		if len(matched[test.id]) == 0 || seen[test.id] || len(candidates) >= state.IntentCandidateMaxOpenPerPair {
			continue
		}
		seen[test.id] = true
		candidate, found, err := state.IntentCandidateByID(ctx, db, test.id)
		if err != nil {
			return nil, nil, err
		}
		if found && candidate.BranchRef == input.BranchRef && candidate.BranchGeneration == input.BranchGeneration {
			candidates = append(candidates, candidate)
		}
	}
	return candidates, matched, nil
}

func intentGoTestCallsOwnedFunction(sourcePath string, contents []byte, source intentGoRegressionSource) bool {
	return intentGoPublishedReferenceContext(sourcePath, contents, source) != ""
}

// Preserve the exact call or named-type lines that admitted published support.
// Generic shared helper lines cannot displace this witness during clipping.
func intentGoPublishedReferenceContext(sourcePath string, contents []byte, source intentGoRegressionSource) string {
	if len(contents) > intentSourceReferenceScanCap || !utf8.Valid(contents) || strings.ContainsRune(string(contents), 0) {
		return ""
	}
	positions := token.NewFileSet()
	file, err := parser.ParseFile(positions, sourcePath, contents, 0)
	if err != nil || file.Name.Name != source.packageName {
		return ""
	}
	for _, imported := range file.Imports {
		if imported.Name != nil && imported.Name.Name == "." {
			return ""
		}
	}
	lines := strings.Split(string(contents), "\n")
	seen := make(map[int]bool)
	var references strings.Builder
	retain := func(node ast.Node) {
		first, last := positions.Position(node.Pos()).Line, positions.Position(node.End()).Line
		var fragment strings.Builder
		for line := first; line <= last; line++ {
			if line < 1 || line > len(lines) {
				return
			}
			if !seen[line] {
				fragment.WriteString(" " + lines[line-1] + "\n")
			}
		}
		if references.Len()+fragment.Len() <= intentSourceReferenceContextCap {
			references.WriteString(fragment.String())
			for line := first; line <= last; line++ {
				seen[line] = true
			}
		}
	}
	ownedType := func(expression ast.Expr) bool {
		for {
			pointer, ok := expression.(*ast.StarExpr)
			if !ok {
				break
			}
			expression = pointer.X
		}
		name, ok := expression.(*ast.Ident)
		return ok && name.Obj == nil && source.types[name.Name]
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		if function.Recv == nil && source.calls[function.Name.Name] {
			retain(function.Type)
		}
		for _, fields := range []*ast.FieldList{function.Recv, function.Type.Params, function.Type.Results} {
			if fields != nil {
				for _, field := range fields.List {
					if ownedType(field.Type) {
						retain(field.Type)
					}
				}
			}
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if _, nested := node.(*ast.FuncLit); nested {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if ok && function.Recv == nil && strings.HasPrefix(function.Name.Name, "Test") {
				callee, ok := call.Fun.(*ast.Ident)
				if ok && callee.Obj == nil && source.names[callee.Name] {
					retain(call)
				}
			}
			if literal, ok := node.(*ast.CompositeLit); ok {
				if ownedType(literal.Type) {
					retain(literal.Type)
				}
			}
			return true
		})
	}
	return references.String()
}

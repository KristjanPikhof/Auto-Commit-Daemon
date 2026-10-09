package daemon

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

type intentRecordedGoCalls struct {
	context, packageName string
	names                []string
}

var intentGoDiffNewLine = regexp.MustCompile(`^@@ -[0-9]+(?:,[0-9]+)? \+([0-9]+)(?:,[0-9]+)? @@`)

func intentGoChangedLinePositions(sourcePath, diff string) (map[int]bool, bool) {
	code := make(map[int]bool)
	for _, witness := range intentSourceCodeWitnessesForPath(sourcePath, diff) {
		code[witness.Index] = true
	}
	changed := make(map[int]bool)
	line, hasHunk := 0, false
	for index, raw := range strings.Split(diff, "\n") {
		if match := intentGoDiffNewLine.FindStringSubmatch(raw); len(match) > 1 {
			line, _ = strconv.Atoi(match[1])
			hasHunk = true
			continue
		}
		if line == 0 || raw == "" || strings.HasPrefix(raw, "+++") || strings.HasPrefix(raw, "---") {
			continue
		}
		switch raw[0] {
		case '+':
			if code[index] {
				changed[line] = true
			}
			line++
		case '-':
			if code[index] {
				changed[line] = true
			}
		case ' ':
			line++
		}
	}
	return changed, hasHunk
}

// A table-only edit can still exercise an existing helper outside its hunk.
// Inspect only the changed function in the immutable post-image, and preserve
// original lines for direct, unshadowed calls absent from the supplied diff.
func intentGoRecordedCallContext(sourcePath, contents, diff string, budget int) intentRecordedGoCalls {
	var result intentRecordedGoCalls
	if path.Ext(sourcePath) != ".go" || budget <= 0 || len(contents) > intentSourceReferenceScanCap {
		return result
	}
	changed := make(map[string]bool)
	for _, line := range append(intentSourceCodeWitnessesForPath(sourcePath, diff), intentSourceEnclosingDeclarationsForPath(sourcePath, diff)...) {
		if intentSourceFunctionDeclaration.MatchString(line.Code) {
			name, _ := intentSourceLineSymbols(line.Code)
			changed[name] = true
		}
	}
	positions, hasHunk := intentGoChangedLinePositions(sourcePath, diff)
	if (hasHunk && len(positions) == 0) || (!hasHunk && len(changed) == 0) {
		return result
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourcePath, contents, parser.ParseComments)
	if err != nil {
		return result
	}
	for _, group := range file.Comments {
		for line, end := fset.Position(group.Pos()).Line, fset.Position(group.End()).Line; line <= end; line++ {
			delete(positions, line)
		}
	}
	for _, imported := range file.Imports {
		if imported.Name != nil && imported.Name.Name == "." {
			// Unqualified calls can belong to the imported package.
			return result
		}
	}
	present := intentSourceUsedNames(sourcePath, diff)
	lines := strings.Split(contents, "\n")
	seen := make(map[string]bool)
	var context strings.Builder
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv != nil || function.Body == nil {
			continue
		}
		ownsChange := changed[function.Name.Name]
		if hasHunk {
			ownsChange = false
			first, last := fset.Position(function.Pos()).Line, fset.Position(function.End()).Line
			for line := range positions {
				ownsChange = ownsChange || (line >= first && line <= last)
			}
		}
		if !ownsChange {
			continue
		}
		type call struct {
			name        string
			first, last int
		}
		var calls []call
		unsafe := make(map[int]bool)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			expression, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			first, last := fset.Position(expression.Pos()).Line, fset.Position(expression.End()).Line
			identifier, direct := expression.Fun.(*ast.Ident)
			if !direct || (identifier.Obj != nil && identifier.Obj.Kind != ast.Fun && identifier.Obj.Kind != ast.Typ) {
				// An enclosing t.Run closure does not make its inner direct
				// helper calls ambiguous. Exclude the unsafe callee's lines.
				for i, end := fset.Position(expression.Fun.Pos()).Line, fset.Position(expression.Fun.End()).Line; i <= end; i++ {
					unsafe[i] = true
				}
				return true
			}
			switch identifier.Name {
			case "append", "cap", "clear", "close", "complex", "copy", "delete", "imag", "len", "make", "max", "min", "new", "panic", "print", "println", "real", "recover":
				return true
			}
			if _, already := present[identifier.Name]; !already {
				calls = append(calls, call{identifier.Name, first, last})
			}
			return true
		})
		for _, call := range calls {
			if seen[call.name] {
				continue
			}
			var fragment strings.Builder
			valid := true
			for i := call.first; i <= call.last; i++ {
				if unsafe[i] || i < 1 || i > len(lines) {
					valid = false
					break
				}
				fragment.WriteString(" " + lines[i-1] + "\n")
			}
			if valid && context.Len()+fragment.Len() <= min(budget, intentSourceReferenceContextCap) {
				context.WriteString(fragment.String())
				seen[call.name] = true
				result.names = append(result.names, call.name)
			}
		}
	}
	result.context, result.packageName = context.String(), file.Name.Name
	return result
}

func mergeIntentRecordedGoCallContext(calls, references string) string {
	remaining := intentSourceReferenceContextCap - len(calls)
	if len(references) > remaining {
		references = references[:max(0, remaining)]
		if end := strings.LastIndexByte(references, '\n'); end >= 0 {
			references = references[:end+1]
		} else {
			references = ""
		}
	}
	return calls + references
}

func loadIntentRecordedGoCalls(ctx context.Context, repo, sourcePath, oid, mode, diff string, budget int) (intentRecordedGoCalls, error) {
	if path.Ext(sourcePath) != ".go" || oid == "" || mode == "120000" || mode == "160000" || budget <= 0 {
		return intentRecordedGoCalls{}, nil
	}
	contents, err := git.CatFileBlobLimited(ctx, repo, oid, intentSourceReferenceScanCap)
	if errors.Is(err, git.ErrStdoutOverflow) {
		return intentRecordedGoCalls{}, nil
	}
	if err != nil {
		return intentRecordedGoCalls{}, err
	}
	if !utf8.Valid(contents) || strings.ContainsRune(string(contents), 0) {
		return intentRecordedGoCalls{}, nil
	}
	return intentGoRecordedCallContext(sourcePath, string(contents), diff, budget), nil
}

func addIntentRecordedGoCallNames(names intentReferenceNames, sourcePath string, calls intentRecordedGoCalls) {
	for _, name := range calls.names {
		if _, known := names[name]; !known {
			names[name] = intentReferenceName{path: sourcePath, goPackage: calls.packageName}
		}
	}
}

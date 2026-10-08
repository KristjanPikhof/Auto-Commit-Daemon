package daemon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"strings"
)

// An immutable declaration can prove which changed file owns a referenced
// function or type, even when the edit corrects its documentation or fixture.
func intentRecordedDeclarationContext(sourcePath, contents string, referenced intentReferenceNames, available ...int) string {
	budget := intentSourceReferenceContextCap
	if len(available) > 0 {
		budget = min(budget, max(0, available[0]))
	}
	if budget == 0 || contents == "" {
		return ""
	}
	var declarations []string
	switch path.Ext(sourcePath) {
	case ".go":
		if len(referenced) == 0 {
			return ""
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, sourcePath, contents, parser.SkipObjectResolution)
		if err != nil {
			return ""
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Body == nil {
				continue
			}
			if !referenced.outside(function.Name.Name, sourcePath) {
				continue
			}
			start := positions.Position(function.Pos()).Offset
			end := positions.Position(function.Body.Lbrace).Offset + 1
			declarations = append(declarations, contents[start:end])
		}
	case ".swift":
		owner := strings.Split(strings.TrimSuffix(path.Base(sourcePath), ".swift"), "+")[0]
		// Lexing the recorded source is internal; emitted lines remain unchanged.
		for _, line := range intentSourceCodeWitnessesForPath(sourcePath, "+"+strings.ReplaceAll(contents, "\n", "\n+")) {
			code := intentSourceQuoted.ReplaceAllString(line.Code, "")
			match := intentSourceDeclaration.FindStringSubmatchIndex(code)
			if len(match) < 4 || code[match[2]:match[3]] != owner {
				continue
			}
			fields := strings.Fields(code[:match[2]])
			kind := fields[len(fields)-1]
			if kind == "class" || kind == "struct" || kind == "enum" || kind == "protocol" || kind == "extension" {
				declarations = append(declarations, line.Raw[1:])
			}
		}
	}
	var result strings.Builder
	for _, declaration := range declarations {
		var fragment strings.Builder
		for _, line := range strings.Split(declaration, "\n") {
			fragment.WriteString(" " + line + "\n")
		}
		if result.Len()+fragment.Len() <= budget {
			result.WriteString(fragment.String())
		}
	}
	return result.String()
}

type intentReferenceName struct {
	path   string
	shared bool
}

type intentReferenceNames map[string]intentReferenceName

func (names intentReferenceNames) outside(name, sourcePath string) bool {
	owner, found := names[name]
	return found && (owner.shared || owner.path != sourcePath)
}

func intentOtherCaptureReferenceNames(captures []IntentCandidateCapture) intentReferenceNames {
	owners := make(intentReferenceNames)
	for _, capture := range captures {
		if capture.CapturedDiff == "" {
			continue
		}
		switch intentCaptureRole(capture) {
		case "code", "test", "migration":
		default:
			continue
		}
		for name := range intentSourceUsedNames(capture.Event.Path, capture.CapturedDiff) {
			owner, found := owners[name]
			if !found {
				owners[name] = intentReferenceName{path: capture.Event.Path}
			} else if !owner.shared && owner.path != capture.Event.Path {
				owner.shared = true
				owners[name] = owner
			}
		}
	}
	return owners
}

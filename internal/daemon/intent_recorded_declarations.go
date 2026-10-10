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
			if constants, ok := declaration.(*ast.GenDecl); ok && constants.Tok == token.CONST {
				needed := false
				for _, specification := range constants.Specs {
					values, ok := specification.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, name := range values.Names {
						owner := referenced[name.Name]
						needed = needed || referenced.outside(name.Name, sourcePath) &&
							!owner.crossDirectory && !owner.crossPackage && owner.goPackage == file.Name.Name &&
							path.Dir(owner.path) == path.Dir(sourcePath)
					}
				}
				if needed {
					first := positions.Position(constants.Pos()).Offset
					last := positions.Position(constants.End()).Offset
					declarations = append(declarations, contents[first:last])
				}
				continue
			}
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Body == nil {
				continue
			}
			if !referenced.outside(function.Name.Name, sourcePath) {
				continue
			}
			owner := referenced[function.Name.Name]
			if owner.goPackage != "" && (owner.goPackage != file.Name.Name || path.Dir(owner.path) != path.Dir(sourcePath)) {
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
	path           string
	shared         bool
	crossDirectory bool
	crossPackage   bool
	goPackage      string
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
		goPackage := ""
		if path.Ext(capture.Event.Path) == ".go" {
			packageDiff := capture.CapturedDiff
			if len(packageDiff) > intentSourceReferenceContextCap {
				packageDiff = packageDiff[:intentSourceReferenceContextCap]
				if end := strings.LastIndexByte(packageDiff, '\n'); end >= 0 {
					packageDiff = packageDiff[:end+1]
				} else {
					packageDiff = ""
				}
			}
			for _, line := range intentSourceCodeWitnessesWithContext(capture.Event.Path, packageDiff, true) {
				if strings.HasPrefix(line.Code, "package ") {
					file, err := parser.ParseFile(token.NewFileSet(), capture.Event.Path, line.Code+"\n", parser.PackageClauseOnly)
					if err == nil {
						goPackage = file.Name.Name
					}
					break
				}
			}
		}
		for name := range intentSourceUsedNames(capture.Event.Path, capture.CapturedDiff) {
			owner, found := owners[name]
			if !found {
				owners[name] = intentReferenceName{path: capture.Event.Path, goPackage: goPackage}
			} else if owner.path != capture.Event.Path {
				owner.shared = true
				owner.crossDirectory = owner.crossDirectory || path.Dir(owner.path) != path.Dir(capture.Event.Path)
				owner.crossPackage = owner.crossPackage || owner.goPackage != "" && goPackage != "" && owner.goPackage != goPackage
				if owner.goPackage == "" {
					owner.goPackage = goPackage
				}
				owners[name] = owner
			}
		}
	}
	return owners
}

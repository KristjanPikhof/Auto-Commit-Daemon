package daemon

import (
	"go/ast"
	"go/token"
	"strings"
)

// A public entry point can exercise a changed private function. Retain only
// original same-file declarations and direct calls whose AST owner is exact.
func intentGoRecordedCallerContext(file *ast.File, positions *token.FileSet, contents string, changed map[*ast.FuncDecl]bool, budget int) (string, []string) {
	lines := strings.Split(contents, "\n")
	var context strings.Builder
	var names []string
	for _, declaration := range file.Decls {
		caller, ok := declaration.(*ast.FuncDecl)
		if !ok || caller.Recv != nil || caller.Body == nil || changed[caller] {
			continue
		}
		var call *ast.CallExpr
		ast.Inspect(caller.Body, func(node ast.Node) bool {
			if _, nested := node.(*ast.FuncLit); nested {
				return false
			}
			expression, ok := node.(*ast.CallExpr)
			if !ok || call != nil {
				return true
			}
			callee, ok := expression.Fun.(*ast.Ident)
			if !ok || callee.Obj == nil {
				return true
			}
			function, ok := callee.Obj.Decl.(*ast.FuncDecl)
			if ok && changed[function] {
				call = expression
			}
			return true
		})
		if call == nil {
			continue
		}
		var fragment strings.Builder
		written := make(map[int]bool)
		for _, span := range [][2]int{{positions.Position(caller.Pos()).Line, positions.Position(caller.Body.Lbrace).Line}, {positions.Position(call.Pos()).Line, positions.Position(call.End()).Line}} {
			for line := span[0]; line <= span[1]; line++ {
				if line > 0 && line <= len(lines) && !written[line] {
					fragment.WriteString(" " + lines[line-1] + "\n")
					written[line] = true
				}
			}
		}
		if context.Len()+fragment.Len() <= min(budget, intentSourceReferenceContextCap) {
			context.WriteString(fragment.String())
			names = append(names, caller.Name.Name)
		}
	}
	return context.String(), names
}

func intentGoRecordedTypeContext(file *ast.File, positions *token.FileSet, contents string, changed map[int]bool, budget int) (string, []string) {
	lines := strings.Split(contents, "\n")
	var context strings.Builder
	var names []string
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok != token.TYPE {
			continue
		}
		for _, specification := range group.Specs {
			typeSpec := specification.(*ast.TypeSpec)
			first, last := positions.Position(typeSpec.Pos()).Line, positions.Position(typeSpec.End()).Line
			ownsChange := false
			for line, added := range changed {
				ownsChange = ownsChange || line >= first && line <= last && (added || line > first)
			}
			if !ownsChange || first < 1 || first > len(lines) {
				continue
			}
			line := lines[first-1]
			name, _ := intentSourceLineSymbols(line)
			// Grouped or opaque declarations remain on ordinary planning.
			if name != typeSpec.Name.Name || !strings.HasPrefix(strings.TrimSpace(line), "type ") {
				continue
			}
			fragment := " " + line + "\n"
			if context.Len()+len(fragment) <= min(budget, intentSourceReferenceContextCap) {
				context.WriteString(fragment)
				names = append(names, name)
			}
		}
	}
	return context.String(), names
}

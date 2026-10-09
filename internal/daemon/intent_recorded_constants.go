package daemon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"strings"
)

// Grouped Go constants are emitted only from immutable top-level AST
// declarations. Keep the whole original block (including iota ordering) as
// evidence; a member's parsed name is used only for relationship analysis.
func intentRecordedGoConstantWitnesses(sourcePath, diff string) []intentSourceCodeWitness {
	if path.Ext(sourcePath) != ".go" {
		return nil
	}
	lines := strings.Split(intentProvidedDeclarationContext(diff), "\n")
	var result []intentSourceCodeWitness
	for i := 0; i < len(lines); i++ {
		if !strings.HasPrefix(lines[i], " const (") {
			continue
		}
		first := i
		for i++; i < len(lines) && lines[i] != " )"; i++ {
		}
		if i == len(lines) {
			break
		}
		raw := strings.Join(lines[first:i+1], "\n")
		if len(raw) > intentSourceReferenceContextCap {
			continue
		}
		var contents strings.Builder
		contents.WriteString("package recorded\n")
		for _, line := range lines[first : i+1] {
			if !strings.HasPrefix(line, " ") {
				break
			}
			contents.WriteString(line[1:])
			contents.WriteByte('\n')
		}
		file, err := parser.ParseFile(token.NewFileSet(), sourcePath, contents.String(), parser.SkipObjectResolution)
		if err != nil || len(file.Decls) != 1 {
			continue
		}
		declaration, ok := file.Decls[0].(*ast.GenDecl)
		if !ok || declaration.Tok != token.CONST || !declaration.Lparen.IsValid() {
			continue
		}
		for _, specification := range declaration.Specs {
			values, ok := specification.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, name := range values.Names {
				if name.Name != "_" && len(result) < 128 {
					result = append(result, intentSourceCodeWitness{Raw: raw, Code: "const " + name.Name, Index: first})
				}
			}
		}
	}
	return result
}

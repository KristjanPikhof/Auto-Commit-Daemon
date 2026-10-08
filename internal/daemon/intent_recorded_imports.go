package daemon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

// Keep actual imports from an immutable post-image when its changed lines omit
// the package relationship. Import targets must be offered production files.
func intentGoImportReferenceContext(sourcePath, recordedContents string, offeredPaths []string) string {
	if path.Ext(sourcePath) != ".go" {
		return ""
	}
	if len(recordedContents) > intentSourceReferenceScanCap {
		recordedContents = recordedContents[:intentSourceReferenceScanCap]
		if end := strings.LastIndexByte(recordedContents, '\n'); end >= 0 {
			recordedContents = recordedContents[:end+1]
		} else {
			return ""
		}
	}
	if strings.ContainsRune(recordedContents, 0) || !utf8.ValidString(recordedContents) {
		return ""
	}
	file, err := parser.ParseFile(token.NewFileSet(), sourcePath, recordedContents, parser.ImportsOnly)
	if err != nil {
		return ""
	}
	packages := make(map[string]bool)
	for _, target := range offeredPaths[:min(len(offeredPaths), ai.IntentCandidateCaptureCap)] {
		target = path.Clean(target)
		if target != path.Clean(sourcePath) && !path.IsAbs(target) && !strings.HasPrefix(target, "../") &&
			path.Ext(target) == ".go" && !strings.HasSuffix(target, "_test.go") {
			packages[path.Dir(target)] = true
		}
	}
	var result strings.Builder
	for _, declaration := range file.Decls {
		imports, ok := declaration.(*ast.GenDecl)
		if !ok || imports.Tok != token.IMPORT {
			continue
		}
		var selected strings.Builder
		for _, specification := range imports.Specs {
			imported := specification.(*ast.ImportSpec)
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil || strings.ContainsAny(name, "\\ \t\r\n") {
				continue
			}
			matches := false
			for directory := range packages {
				if directory != "." && (name == directory || strings.HasSuffix(name, "/"+directory)) {
					matches = true
					break
				}
			}
			if !matches {
				continue
			}
			fragment := imported.Path.Value
			if imported.Name != nil {
				fragment = imported.Name.Name + " " + fragment
			}
			if !imports.Lparen.IsValid() {
				fragment = " import " + fragment + "\n"
				if result.Len()+len(fragment) <= intentSourceReferenceContextCap {
					result.WriteString(fragment)
				}
				continue
			}
			fragment = " \t" + fragment + "\n"
			// Reserve the opener and closing line, so clipping never creates an
			// unterminated block or an incomplete import witness.
			if result.Len()+len(" import (\n )\n")+selected.Len()+len(fragment) <= intentSourceReferenceContextCap {
				selected.WriteString(fragment)
			}
		}
		if selected.Len() > 0 {
			result.WriteString(" import (\n")
			result.WriteString(selected.String())
			result.WriteString(" )\n")
		}
	}
	return result.String()
}

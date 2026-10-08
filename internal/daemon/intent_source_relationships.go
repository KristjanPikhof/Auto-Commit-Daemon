package daemon

import (
	"path"
	"regexp"
	"strings"
	"unicode"
)

var (
	intentSourceDeclaration = regexp.MustCompile(`^(?:(?:public|private|protected|internal|open|fileprivate|static|async|export|default|final|abstract)\s+)*(?:func(?:\s+\([^)]*\))?|function|def|class|struct|type|enum|interface|protocol|const|let|var)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	intentSourceQuoted      = regexp.MustCompile("\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*'|`[^`]*`")
	intentSourceMultiline   = regexp.MustCompile(`(?s)""".*?(?:"""|$)|'''.*?(?:'''|$)` + "|`[^`]*(?:`|$)")
	intentSourceFilePath    = regexp.MustCompile(`[a-zA-Z0-9_.-]+(?:/[a-zA-Z0-9_.-]+)*\.[a-zA-Z0-9_-]+`)
)

// These are bounded lexical relationships, not a language parser. Declarations
// and uses must occur in changed code, so prose and quoted labels cannot provide
// symbolic cohesion. Unrecognized syntax supplies no evidence.
func intentSourceCodeLines(diff string) []string {
	var lines []string
	inBlockComment := false
	for _, raw := range strings.Split(intentSourceMultiline.ReplaceAllString(diff, ""), "\n") {
		if len(raw) == 0 || (raw[0] != '+' && raw[0] != '-' && raw[0] != ' ') ||
			strings.HasPrefix(raw, "+++") || strings.HasPrefix(raw, "---") {
			continue
		}
		changed := raw[0] == '+' || raw[0] == '-'
		line := raw[1:]
		var code strings.Builder
		for len(line) > 0 {
			if inBlockComment {
				end := strings.Index(line, "*/")
				if end < 0 {
					break
				}
				line, inBlockComment = line[end+2:], false
				continue
			}
			if strings.HasPrefix(line, "//") || strings.HasPrefix(line, "#") {
				break
			}
			if strings.HasPrefix(line, "/*") {
				line, inBlockComment = line[2:], true
				continue
			}
			if line[0] == '"' || line[0] == '\'' || line[0] == '`' {
				if quoted := intentSourceQuoted.FindStringIndex(line); quoted != nil && quoted[0] == 0 {
					code.WriteString(line[:quoted[1]])
					line = line[quoted[1]:]
					continue
				}
			}
			code.WriteByte(line[0])
			line = line[1:]
		}
		if cleaned := strings.TrimSpace(code.String()); cleaned != "" && changed {
			lines = append(lines, cleaned)
		}
	}
	return lines
}

func intentSourceSymbols(diff string) (map[string]struct{}, map[string]struct{}) {
	declared, used := make(map[string]struct{}), make(map[string]struct{})
	for _, line := range intentSourceCodeLines(diff) {
		line = intentSourceQuoted.ReplaceAllString(line, "")
		if match := intentSourceDeclaration.FindStringSubmatchIndex(line); len(match) > 3 {
			name := line[match[2]:match[3]]
			if len(name) >= 3 {
				declared[name] = struct{}{}
			}
			// A declaration is not a reference to another capture's symbol.
			// Keep genuine references in its same-line initializer or body.
			line = line[:match[2]] + " " + line[match[3]:]
		}
		for _, token := range strings.FieldsFunc(line, func(r rune) bool {
			return r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}) {
			if len(token) >= 3 && len(used) < 128 {
				used[token] = struct{}{}
			}
		}
		if len(declared) >= 128 {
			break
		}
	}
	return declared, used
}

func intentSourcePathReferences(diff string) (map[string]struct{}, map[string]struct{}) {
	files, imports := make(map[string]struct{}), make(map[string]struct{})
	inImport := false
	for _, raw := range strings.Split(strings.ToLower(diff), "\n") {
		if strings.HasPrefix(raw, "diff --git") || strings.HasPrefix(raw, "+++") ||
			strings.HasPrefix(raw, "---") || strings.HasPrefix(raw, "index ") || strings.HasPrefix(raw, "@@") {
			continue
		}
		line := strings.TrimSpace(strings.TrimLeft(raw, "+- "))
		for _, file := range intentSourceFilePath.FindAllString(line, 128) {
			if len(files) < 128 {
				files[file] = struct{}{}
			}
		}
		if line == ")" {
			inImport = false
		}
		isImport := inImport || strings.HasPrefix(line, "import ") || strings.HasPrefix(line, "from ") ||
			strings.HasPrefix(line, "#include ") || strings.Contains(line, "require(")
		if !isImport {
			continue
		}
		for _, quoted := range intentSourceQuoted.FindAllString(line, 128) {
			if len(imports) < 128 {
				imports[strings.Trim(quoted, "\"'`")] = struct{}{}
			}
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && (fields[0] == "from" || fields[0] == "import") && fields[1] != "(" {
			imports[strings.Trim(fields[1], "\"';,")] = struct{}{}
		}
		inImport = inImport || strings.HasPrefix(line, "import (")
	}
	return files, imports
}

func intentSourceReferencesFile(files map[string]struct{}, importer, target string, uniqueBase bool) bool {
	for file := range files {
		if file == target || strings.HasSuffix(file, "/"+target) ||
			path.Clean(path.Join(path.Dir(importer), file)) == target ||
			(uniqueBase && file == path.Base(target)) {
			return true
		}
	}
	return false
}

func intentSourceImports(imports map[string]struct{}, importer, target string) bool {
	stem, directory := strings.TrimSuffix(target, path.Ext(target)), path.Dir(target)
	for imported := range imports {
		resolved := path.Clean(path.Join(path.Dir(importer), imported))
		if imported == stem || resolved == stem || imported == target || resolved == target ||
			(directory != "." && (imported == directory || strings.HasSuffix(imported, "/"+directory))) {
			return true
		}
	}
	return false
}

package daemon

import (
	"path"
	"regexp"
	"strings"
	"unicode"
)

var (
	intentSourceDeclaration = regexp.MustCompile(`^(?:(?:public|private|protected|internal|open|fileprivate|static|async|export|default|final|abstract)\s+)*(?:func(?:\s+\([^)]*\))?|function|def|class|struct|type|enum|interface|protocol)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	intentSourceQuoted      = regexp.MustCompile("\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*'|`[^`]*`")
	intentSourceFilePath    = regexp.MustCompile(`[a-zA-Z0-9_.-]+(?:/[a-zA-Z0-9_.-]+)*\.[a-zA-Z0-9_-]+`)
)

// These are bounded lexical relationships, not a language parser. Declarations
// and uses must occur in changed code, so prose and quoted labels cannot provide
// symbolic cohesion. Unrecognized syntax supplies no evidence.
func intentSourceCodeLines(diff string) []string {
	var lines []string
	inBlockComment := false
	for _, raw := range strings.Split(diff, "\n") {
		if len(raw) == 0 || (raw[0] != '+' && raw[0] != '-') ||
			strings.HasPrefix(raw, "+++") || strings.HasPrefix(raw, "---") {
			continue
		}
		line := intentSourceQuoted.ReplaceAllString(raw[1:], "")
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
			code.WriteByte(line[0])
			line = line[1:]
		}
		if cleaned := strings.TrimSpace(code.String()); cleaned != "" {
			lines = append(lines, cleaned)
		}
	}
	return lines
}

func intentSourceSymbols(diff string) (map[string]struct{}, map[string]struct{}) {
	declared, used := make(map[string]struct{}), make(map[string]struct{})
	for _, line := range intentSourceCodeLines(diff) {
		if match := intentSourceDeclaration.FindStringSubmatch(line); len(match) > 1 && len(match[1]) >= 3 {
			declared[strings.ToLower(match[1])] = struct{}{}
		}
		for _, token := range strings.FieldsFunc(line, func(r rune) bool {
			return r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}) {
			if len(token) >= 3 && len(used) < 128 {
				used[strings.ToLower(token)] = struct{}{}
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

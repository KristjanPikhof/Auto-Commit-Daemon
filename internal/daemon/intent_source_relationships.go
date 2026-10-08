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
	intentPublicFlagCall    = regexp.MustCompile(`\.(?:PersistentFlags|Flags)\(\)\.(?:String|Bool|Int(?:32|64)?|Uint(?:32|64)?|Duration|Float(?:32|64)|StringSlice|StringArray|Count)(?:Var)?P?\(`)
	intentPublicFlagName    = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{1,63}$`)
	intentPublicStatusName  = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{7,63}$`)
	intentDocumentInline    = regexp.MustCompile("`([^`\n]+)`")
	intentReferenceWords    = regexp.MustCompile(`"(?:\\.|[^"\\])*"|'[^']*'|[^\s]+`)
	intentReferenceAssign   = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*=`)
	intentReferenceHereDoc  = regexp.MustCompile(`<<-?\s*['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?`)
	intentReferencePython   = regexp.MustCompile(`\b(?:(?:pathlib\.)?Path\(__file__\)\.with_name|(?:pathlib\.)?Path|open)\(\s*['"]([^'"\n]+)['"]\s*\)`)
)

const intentSourceReferenceScanCap = 256 << 10
const intentSourceReferenceContextCap = 4096

// The caller supplies an immutable recorded post-image, never the live file.
// Return bounded context fragments that actually execute or open another
// offered path. Comments, arbitrary quoted labels, and proximity prove nothing.
func intentSourceReferenceContext(sourcePath, recordedContents string, offeredPaths []string) string {
	ext := path.Ext(sourcePath)
	if ext != ".sh" && ext != ".py" {
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
	targets := make(map[string]bool)
	for _, target := range offeredPaths[:min(len(offeredPaths), 256)] {
		if target != sourcePath {
			targets[path.Clean(target)] = true
		}
	}
	references := func(name string) bool {
		return targets[path.Clean(name)] || targets[path.Clean(path.Join(path.Dir(sourcePath), name))]
	}
	var result strings.Builder
	seen := make(map[string]bool)
	appendWitness := func(fragment string) {
		if !seen[fragment] && result.Len()+len(fragment)+2 <= intentSourceReferenceContextCap {
			result.WriteString(" " + fragment + "\n")
			seen[fragment] = true
		}
	}
	// Reuse the comment/docstring lexer on the recorded source. The artificial
	// '+' prefix is internal to lexing; returned evidence is unchanged context.
	lines := intentSourceCodeLines("+" + strings.ReplaceAll(recordedContents, "\n", "\n+"))
	var quote byte
	opaqueEnd := ""
	for _, line := range lines {
		if opaqueEnd != "" {
			if line == opaqueEnd {
				opaqueEnd = ""
			}
			continue
		}
		insideString := quote != 0
		for i := 0; i < len(line); i++ {
			if line[i] == '\\' && quote != '\'' {
				i++
				continue
			}
			if quote != 0 {
				if line[i] == quote {
					quote = 0
				}
			} else if line[i] == '\'' || line[i] == '"' {
				quote = line[i]
			}
		}
		if insideString || quote != 0 {
			continue
		}
		if ext == ".sh" {
			if strings.Contains(line, "<<") {
				match := intentReferenceHereDoc.FindStringSubmatch(line)
				if match == nil {
					// Unknown delimiters cannot prove where literal input ends.
					break
				}
				opaqueEnd = match[1]
				continue
			}
			if intentReferenceAssign.MatchString(line) && strings.HasSuffix(line, "=(") {
				opaqueEnd = ")"
				continue
			}
			words := intentReferenceWords.FindAllStringIndex(line, -1)
			first := 0
			for first < len(words) && intentReferenceAssign.MatchString(line[words[first][0]:words[first][1]]) {
				first++
			}
			if first == len(words) {
				continue
			}
			operand := first
			switch intentReferenceLiteral(line[words[first][0]:words[first][1]]) {
			case "bash", "sh", "python", "python2", "python3", "node", "source", ".", "exec":
				operand++
			}
			if operand < len(words) && references(intentReferenceLiteral(line[words[operand][0]:words[operand][1]])) {
				appendWitness(line[words[first][0]:words[operand][1]])
			}
			continue
		}
		quoted := intentSourceQuoted.FindAllStringIndex(line, -1)
		for _, match := range intentReferencePython.FindAllStringSubmatchIndex(line, -1) {
			inside := false
			for _, span := range quoted {
				inside = inside || (span[0] < match[0] && match[0] < span[1])
			}
			if !inside && references(line[match[2]:match[3]]) {
				appendWitness(line[match[0]:match[1]])
			}
		}
	}
	return result.String()
}

func intentReferenceLiteral(token string) string {
	if len(token) >= 2 && (token[0] == '"' || token[0] == '\'') && token[len(token)-1] == token[0] {
		token = token[1 : len(token)-1]
	}
	if strings.ContainsAny(token, "$\\`\"'") {
		return ""
	}
	return token
}

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

func intentSourcePublicReferences(diff string) map[string]struct{} {
	public := make(map[string]struct{})
	for _, line := range intentSourceCodeLines(diff) {
		quoted := intentSourceQuoted.FindString(line)
		if quoted == "" {
			continue
		}
		value := strings.Trim(quoted, "\"'")
		if intentPublicFlagCall.MatchString(line) && intentPublicFlagName.MatchString(value) {
			public["--"+value] = struct{}{}
		} else if strings.HasPrefix(line, "return "+quoted) &&
			intentPublicStatusName.MatchString(value) && strings.ContainsAny(value, "-_") {
			public[value] = struct{}{}
		}
		if len(public) >= 128 {
			break
		}
	}
	return public
}

func intentDocumentPublicReferences(diff string) map[string]struct{} {
	used := make(map[string]struct{})
	inFence := false
	for _, raw := range strings.Split(diff, "\n") {
		if len(raw) == 0 || (raw[0] != '+' && raw[0] != '-' && raw[0] != ' ') ||
			strings.HasPrefix(raw, "+++") || strings.HasPrefix(raw, "---") {
			continue
		}
		line := strings.TrimSpace(raw[1:])
		if strings.HasPrefix(line, "```") || strings.HasPrefix(line, "~~~") {
			inFence = !inFence
			continue
		}
		if raw[0] != '+' && raw[0] != '-' {
			continue
		}
		var fragments []string
		if inFence {
			fragments = append(fragments, line)
		}
		for _, match := range intentDocumentInline.FindAllStringSubmatch(line, 128) {
			fragments = append(fragments, match[1])
		}
		for _, fragment := range fragments {
			for _, token := range strings.FieldsFunc(fragment, func(r rune) bool {
				return unicode.IsSpace(r) || strings.ContainsRune("`'\",;()[]{}=.:", r)
			}) {
				if len(used) < 128 && len(token) <= 66 {
					used[token] = struct{}{}
				}
			}
		}
	}
	return used
}

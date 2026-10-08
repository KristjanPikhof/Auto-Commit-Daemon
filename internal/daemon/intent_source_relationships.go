package daemon

import (
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	intentSourceDeclaration         = regexp.MustCompile(`^(?:(?:public|private|protected|internal|open|fileprivate|static|async|export|default|final|abstract)\s+)*(?:func(?:\s+\([^)]*\))?|function|def|class|struct|type|enum|interface|protocol|extension|const|let|var)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	intentSourceTypeDeclaration     = regexp.MustCompile(`^(?:(?:public|private|protected|internal|open|fileprivate|static|export|default|final|abstract)\s+)*(?:class|struct|type|enum|interface|protocol|extension)\s+`)
	intentSourceFunctionDeclaration = regexp.MustCompile(`^(?:(?:public|private|protected|internal|open|fileprivate|static|async|export|default|final|abstract)\s+)*(?:func|function|def)\b`)
	intentSourceBindingDeclaration  = regexp.MustCompile(`^(?:(?:public|private|protected|internal|open|fileprivate|static|export|default|final)\s+)*(?:let|var|const)\s+`)
	intentSwiftCodeMacro            = regexp.MustCompile(`^#(?:expect|require)\s*\(`)
	intentSourceQuoted              = regexp.MustCompile("\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*'|`[^`]*`")
	intentSourceMultiline           = regexp.MustCompile(`(?s)""".*?(?:"""|$)|'''.*?(?:'''|$)` + "|`[^`]*(?:`|$)")
	intentSourceFilePath            = regexp.MustCompile(`[a-zA-Z0-9_.-]+(?:/[a-zA-Z0-9_.-]+)*\.[a-zA-Z0-9_-]+`)
	intentPublicFlagCall            = regexp.MustCompile(`\.(?:PersistentFlags|Flags)\(\)\.(?:String|Bool|Int(?:32|64)?|Uint(?:32|64)?|Duration|Float(?:32|64)|StringSlice|StringArray|Count)(?:Var)?P?\(`)
	intentPublicFlagName            = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{1,63}$`)
	intentPublicStatusName          = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{7,63}$`)
	intentDocumentInline            = regexp.MustCompile("`([^`\n]+)`")
	intentReferenceWords            = regexp.MustCompile(`"(?:\\.|[^"\\])*"|'[^']*'|[^\s]+`)
	intentReferenceAssign           = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*=`)
	intentReferenceHereDoc          = regexp.MustCompile(`<<-?\s*['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?`)
	intentReferencePython           = regexp.MustCompile(`\b(?:(?:pathlib\.)?Path\(__file__\)\.with_name|(?:pathlib\.)?Path|open)\(\s*['"]([^'"\n]+)['"]\s*\)`)
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
			words := intentReferenceWords.FindAllStringIndex(line, 128)
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
		quoted := intentSourceQuoted.FindAllStringIndex(line, 128)
		for _, match := range intentReferencePython.FindAllStringSubmatchIndex(line, 128) {
			inside := len(quoted) == 128 && match[0] >= quoted[len(quoted)-1][1]
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
	return intentSourceCodeLinesForPath("", diff)
}

func intentSourceCodeLinesForPath(sourcePath, diff string) []string {
	var lines []string
	for _, witness := range intentSourceCodeWitnessesForPath(sourcePath, diff) {
		lines = append(lines, witness.Code)
	}
	return lines
}

type intentSourceCodeWitness struct {
	Raw, Code string
	Index     int
}

func intentSourceCodeWitnesses(diff string) []intentSourceCodeWitness {
	return intentSourceCodeWitnessesForPath("", diff)
}

func intentSourceCodeWitnessesForPath(sourcePath, diff string) []intentSourceCodeWitness {
	return intentSourceCodeWitnessesWithContext(sourcePath, diff, false)
}

func intentSourceCodeWitnessesWithContext(sourcePath, diff string, includeContext bool) []intentSourceCodeWitness {
	var lines []intentSourceCodeWitness
	original := strings.Split(diff, "\n")
	masked := intentSourceMultiline.ReplaceAllStringFunc(diff, func(value string) string {
		return strings.Map(func(r rune) rune {
			if r == '\n' {
				return r
			}
			return ' '
		}, value)
	})
	inBlockComment := false
	for i, maskedLine := range strings.Split(masked, "\n") {
		raw := original[i]
		if len(raw) == 0 || (raw[0] != '+' && raw[0] != '-' && raw[0] != ' ') ||
			strings.HasPrefix(raw, "+++") || strings.HasPrefix(raw, "---") {
			continue
		}
		changed := raw[0] == '+' || raw[0] == '-'
		line := maskedLine[1:]
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
			if strings.HasPrefix(line, "//") || (strings.HasPrefix(line, "#") &&
				!(strings.HasSuffix(strings.ToLower(sourcePath), ".swift") && intentSwiftCodeMacro.MatchString(line))) {
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
		if cleaned := strings.TrimSpace(code.String()); cleaned != "" && (changed || includeContext) {
			lines = append(lines, intentSourceCodeWitness{Raw: raw, Code: cleaned, Index: i})
		}
	}
	return lines
}

func intentSourceSymbols(diff string) (map[string]struct{}, map[string]struct{}) {
	return intentSourceSymbolsForPath("", diff)
}

func intentSourceSymbolsForPath(sourcePath, diff string) (map[string]struct{}, map[string]struct{}) {
	declared, used := make(map[string]struct{}), intentSourceUsedNamesWithCap(sourcePath, diff, 128)
	for _, declaration := range intentSourceEnclosingDeclarationsForPath(sourcePath, diff) {
		name, _ := intentSourceLineSymbols(declaration.Code)
		declared[name] = struct{}{}
	}
	for _, declaration := range intentSourceCodeWitnessesWithContext(sourcePath, intentProvidedDeclarationContext(diff), true) {
		name := intentSourceCrossFileDeclaration(declaration)
		if name != "" && len(declared) < 128 {
			declared[name] = struct{}{}
		}
	}
	for _, line := range intentSourceCodeWitnessesForPath(sourcePath, diff) {
		name := intentSourceCrossFileDeclaration(line)
		if name != "" {
			declared[name] = struct{}{}
		}
		if len(declared) >= 128 {
			break
		}
	}
	return declared, used
}

func intentSourceCrossFileDeclaration(line intentSourceCodeWitness) string {
	name, _ := intentSourceLineSymbols(line.Code)
	// Indentation preserves the recorded lexical scope after Code is trimmed.
	// Local variables and instance fields cannot declare a cross-file symbol.
	if name != "" && intentSourceBindingDeclaration.MatchString(line.Code) &&
		len(line.Raw) > 1 && (line.Raw[1] == ' ' || line.Raw[1] == '\t') {
		return ""
	}
	return name
}

func intentSourceUsedNames(sourcePath, diff string) map[string]struct{} {
	return intentSourceUsedNamesWithCap(sourcePath, diff, 4096)
}

func intentSourceUsedNamesWithCap(sourcePath, diff string, cap int) map[string]struct{} {
	used := make(map[string]struct{})
	for _, line := range intentSourceCodeWitnessesWithContext(sourcePath, diff, true) {
		_, tokens := intentSourceLineSymbols(line.Code)
		ordered := make([]string, 0, len(tokens))
		for token := range tokens {
			ordered = append(ordered, token)
		}
		sort.Strings(ordered)
		for _, token := range ordered {
			if len(used) < cap {
				used[token] = struct{}{}
			}
		}
	}
	return used
}

func intentProvidedDeclarationContext(diff string) string {
	const prefix = "Recorded post-image references:\n"
	const separator = "\nRecorded diff:\n"
	if strings.HasPrefix(diff, prefix) {
		if end := strings.Index(diff, separator); end >= 0 {
			return diff[len(prefix):end]
		}
	}
	return ""
}

func intentSourceAPIDeclarationWitnesses(sourcePath, diff string) []intentSourceCodeWitness {
	lines := intentSourceCodeWitnessesForPath(sourcePath, diff)
	lines = append(lines, intentSourceEnclosingDeclarationsForPath(sourcePath, diff)...)
	lines = append(lines, intentSourceCodeWitnessesWithContext(sourcePath, intentProvidedDeclarationContext(diff), true)...)
	var declarations []intentSourceCodeWitness
	seen := make(map[string]bool)
	for _, line := range lines {
		if !intentSourceTypeDeclaration.MatchString(line.Code) && !intentSourceFunctionDeclaration.MatchString(line.Code) {
			continue
		}
		name, _ := intentSourceLineSymbols(line.Code)
		if name != "" && !seen[name] {
			seen[name] = true
			declarations = append(declarations, line)
		}
		if len(declarations) >= 128 {
			break
		}
	}
	return declarations
}

func intentSourceAPIReferences(sourcePath, diff string) map[string]struct{} {
	api := make(map[string]struct{})
	for _, declaration := range intentSourceAPIDeclarationWitnesses(sourcePath, diff) {
		name, _ := intentSourceLineSymbols(declaration.Code)
		api[name] = struct{}{}
	}
	return api
}

// Git's recorded hunk heading identifies an existing function whose body was
// changed. A heading without an actual changed code line proves no relationship.
func intentSourceEnclosingDeclarations(diff string) []intentSourceCodeWitness {
	return intentSourceEnclosingDeclarationsForPath("", diff)
}

func intentSourceEnclosingDeclarationsForPath(sourcePath, diff string) []intentSourceCodeWitness {
	raw := strings.Split(diff, "\n")
	code := intentSourceCodeWitnessesForPath(sourcePath, diff)
	var result []intentSourceCodeWitness
	for i, line := range raw {
		if !strings.HasPrefix(line, "@@ ") {
			continue
		}
		end := strings.Index(line[3:], " @@")
		if end < 0 {
			continue
		}
		declaration := strings.TrimSpace(line[3+end+3:])
		if !intentSourceFunctionDeclaration.MatchString(declaration) {
			continue
		}
		name, _ := intentSourceLineSymbols(declaration)
		if name == "" {
			continue
		}
		next := i + 1
		for next < len(raw) && !strings.HasPrefix(raw[next], "@@ ") && !strings.HasPrefix(raw[next], "diff --git ") {
			next++
		}
		for _, witness := range code {
			if witness.Index > i && witness.Index < next {
				result = append(result, intentSourceCodeWitness{Raw: line + "\n" + witness.Raw, Code: declaration, Index: i})
				break
			}
		}
		if len(result) >= 128 {
			break
		}
	}
	return result
}

func intentSourceLineSymbols(line string) (string, map[string]struct{}) {
	line = intentSourceQuoted.ReplaceAllString(line, "")
	name := ""
	if match := intentSourceDeclaration.FindStringSubmatchIndex(line); len(match) > 3 {
		if len(line[match[2]:match[3]]) >= 3 {
			name = line[match[2]:match[3]]
		}
		// A declaration is not a reference. Keep genuine initializer/body uses.
		line = line[:match[2]] + " " + line[match[3]:]
	}
	used := make(map[string]struct{})
	for _, token := range strings.FieldsFunc(line, func(r rune) bool {
		return r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if len(token) >= 3 && len(used) < 128 {
			used[token] = struct{}{}
		}
	}
	return name, used
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
		for _, quoted := range intentSourceQuoted.FindAllString(line, 128) {
			file := quoted[1 : len(quoted)-1]
			if len(files) < 128 && intentExactQuotedSourcePath(file) {
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

func intentExactQuotedSourcePath(value string) bool {
	if len(value) > 4096 || !utf8.ValidString(value) || path.Ext(value) == "" {
		return false
	}
	for _, r := range value {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune(" _.-/", r) {
			return false
		}
	}
	return true
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
	if strings.HasSuffix(target, "_test.go") {
		return false
	}
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
		if value := intentSourcePublicLineReference(line); value != "" {
			public[value] = struct{}{}
		}
		if len(public) >= 128 {
			break
		}
	}
	return public
}

func intentSourcePublicLineReference(line string) string {
	quoted := intentSourceQuoted.FindString(line)
	if quoted == "" {
		return ""
	}
	value := strings.Trim(quoted, "\"'")
	if intentPublicFlagCall.MatchString(line) && intentPublicFlagName.MatchString(value) {
		return "--" + value
	}
	if strings.HasPrefix(line, "return "+quoted) && intentPublicStatusName.MatchString(value) && strings.ContainsAny(value, "-_") {
		return value
	}
	return ""
}

func intentDocumentPublicReferences(diff string) map[string]struct{} {
	used := make(map[string]struct{})
	for token := range intentDocumentPublicWitnesses(diff) {
		used[token] = struct{}{}
	}
	return used
}

func intentDocumentPublicWitnesses(diff string) map[string][]string {
	used := make(map[string][]string)
	inFence := false
	fenceStart := ""
	fenced := make(map[string][]string)
	appendUse := func(token, raw string) {
		if (len(used) < 128 || len(used[token]) > 0) && len(used[token]) < 8 {
			used[token] = append(used[token], raw)
		}
	}
	for _, raw := range strings.Split(diff, "\n") {
		if len(raw) == 0 || (raw[0] != '+' && raw[0] != '-' && raw[0] != ' ') ||
			strings.HasPrefix(raw, "+++") || strings.HasPrefix(raw, "---") {
			continue
		}
		line := strings.TrimSpace(raw[1:])
		if strings.HasPrefix(line, "```") || strings.HasPrefix(line, "~~~") {
			if inFence {
				tokens := make([]string, 0, len(fenced))
				for token := range fenced {
					tokens = append(tokens, token)
				}
				sort.Strings(tokens)
				for _, token := range tokens {
					lines := fenced[token]
					for _, witness := range lines {
						appendUse(token, fenceStart+"\n"+witness+"\n"+raw)
					}
				}
				fenced = make(map[string][]string)
			} else {
				fenceStart = raw
			}
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
				if len(token) <= 66 {
					if inFence && (len(fenced) < 128 || len(fenced[token]) > 0) && len(fenced[token]) < 8 {
						fenced[token] = append(fenced[token], raw)
					} else if !inFence {
						appendUse(token, raw)
					}
				}
			}
		}
	}
	// An omitted closing fence cannot be copied as a self-contained witness.
	// It still supplies the same original relationship to ordinary validation.
	tokens := make([]string, 0, len(fenced))
	for token := range fenced {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	for _, token := range tokens {
		appendUse(token, "")
	}
	return used
}

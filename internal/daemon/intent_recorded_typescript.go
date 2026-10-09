package daemon

import (
	"context"
	"errors"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

const intentTypeScriptReferenceTotalCap = 2 << 20

var (
	intentTypeScriptImport     = regexp.MustCompile(`^import\s+(?:type\s+)?\{([^{}]+)\}\s+from\s+["']([^"'\n]+)["']\s*;?$`)
	intentTypeScriptImportName = regexp.MustCompile(`^(?:type\s+)?([A-Za-z_$][A-Za-z0-9_$]*)(?:\s+as\s+([A-Za-z_$][A-Za-z0-9_$]*))?$`)
	intentTypeScriptExport     = regexp.MustCompile(`^export\s+(?:(?:declare|abstract|async)\s+)*(?:class|interface|type|enum|function|const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)`)
	intentTypeScriptBaseOID    = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
)

type intentTypeScriptRecordedFile struct {
	seq               int64
	path, contents    string
	baseHead          string
	javaScriptTargets map[string]bool
}

type intentTypeScriptRecordedImport struct {
	witness, target string
	names           []string
}

// Only exact static named imports and actual top-level exports can connect
// recorded TypeScript files. Unknown syntax supplies no inferred dependency.
func intentTypeScriptRecordedDeclarations(source string) ([]intentTypeScriptRecordedImport, map[string]string) {
	var imports []intentTypeScriptRecordedImport
	exports := make(map[string]string)
	if len(source) > intentSourceReferenceScanCap || !utf8.ValidString(source) || strings.ContainsRune(source, 0) {
		return imports, exports
	}
	depth := 0
	for _, line := range intentSourceCodeWitnessesForPath("source.ts", "+"+strings.ReplaceAll(source, "\n", "\n+")) {
		code := line.Code
		unquoted := intentSourceQuoted.ReplaceAllString(code, "")
		// Regex and division syntax are outside this small recognizer. Stop
		// before they can obscure scope; earlier declarations remain exact.
		if strings.ContainsAny(unquoted, `/"'`+"`") {
			break
		}
		if depth == 0 && len(line.Raw) > 1 && line.Raw[1] != ' ' && line.Raw[1] != '\t' && line.Raw[1:] == code {
			if match := intentTypeScriptImport.FindStringSubmatch(code); match != nil && strings.HasPrefix(match[2], ".") && !strings.ContainsAny(match[2], `*?[:\`) {
				item := intentTypeScriptRecordedImport{witness: code, target: match[2]}
				valid := true
				for _, specification := range strings.Split(match[1], ",") {
					name := intentTypeScriptImportName.FindStringSubmatch(strings.TrimSpace(specification))
					if name == nil {
						valid = false
						break
					}
					item.names = append(item.names, name[1])
				}
				if valid && len(imports) < 128 {
					imports = append(imports, item)
				}
			}
			if match := intentTypeScriptExport.FindStringSubmatch(code); match != nil && len(exports) < 128 {
				// Emit an unchanged prefix of the actual declaration, not its body.
				exports[match[1]] = match[0]
			}
		}
		depth += strings.Count(unquoted, "{") - strings.Count(unquoted, "}")
		if depth < 0 {
			break
		}
	}
	return imports, exports
}

func intentTypeScriptReferenceContexts(files []intentTypeScriptRecordedFile) map[int64]string {
	result := make(map[int64]string)
	byPath := make(map[string][]int)
	imports := make([][]intentTypeScriptRecordedImport, len(files))
	exports := make([]map[string]string, len(files))
	for i, file := range files {
		if path.Ext(file.path) != ".ts" {
			continue
		}
		byPath[file.path] = append(byPath[file.path], i)
		imports[i], exports[i] = intentTypeScriptRecordedDeclarations(file.contents)
	}
	seen := make(map[int64]map[string]bool)
	appendWitness := func(seq int64, witness string) {
		if seen[seq] == nil {
			seen[seq] = make(map[string]bool)
		}
		fragment := " " + witness + "\n"
		if !seen[seq][witness] && len(result[seq])+len(fragment) <= intentSourceReferenceContextCap {
			result[seq] += fragment
			seen[seq][witness] = true
		}
	}
	for i, file := range files {
		for _, imported := range imports[i] {
			resolved := path.Clean(path.Join(path.Dir(file.path), imported.target))
			var choices []string
			switch path.Ext(resolved) {
			case ".ts":
				choices = []string{resolved}
			case ".js":
				if !file.javaScriptTargets[resolved] {
					continue
				}
				choices = []string{strings.TrimSuffix(resolved, ".js") + ".ts"}
			case "":
				choices = []string{resolved + ".ts", path.Join(resolved, "index.ts")}
			default:
				continue
			}
			var targets []int
			ambiguous := false
			for _, choice := range choices {
				if indices := byPath[choice]; len(indices) > 0 {
					if len(targets) > 0 {
						ambiguous = true
						break
					}
					targets = indices
				}
			}
			if ambiguous || len(targets) == 0 {
				continue
			}
			// One proved named owner establishes this exact module import.
			// Opaque later declarations supply no extra owner witnesses.
			var witnessed []string
			for _, name := range imported.names {
				valid := true
				for _, target := range targets {
					valid = valid && exports[target][name] != ""
				}
				if valid {
					witnessed = append(witnessed, name)
				}
			}
			if len(witnessed) == 0 {
				continue
			}
			appendWitness(file.seq, imported.witness)
			for _, target := range targets {
				for _, name := range witnessed {
					appendWitness(files[target].seq, exports[target][name])
				}
			}
		}
	}
	return result
}

func loadIntentRecordedTypeScriptReferences(ctx context.Context, repo string, captures []IntentCandidateCapture) (map[int64]string, error) {
	var files []intentTypeScriptRecordedFile
	bytes := 0
	for _, capture := range captures[:min(len(captures), 256)] {
		if path.Ext(capture.Event.Path) != ".ts" || len(capture.Ops) != 1 {
			continue
		}
		op := capture.Ops[0]
		if op.Path != capture.Event.Path || !op.AfterOID.Valid || op.AfterMode.String != "100644" || op.Op == "rename" {
			continue
		}
		contents, err := git.CatFileBlobLimited(ctx, repo, op.AfterOID.String, intentSourceReferenceScanCap)
		if errors.Is(err, git.ErrStdoutOverflow) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if bytes+len(contents) > intentTypeScriptReferenceTotalCap {
			break
		}
		bytes += len(contents)
		files = append(files, intentTypeScriptRecordedFile{seq: capture.Event.Seq, path: op.Path, contents: string(contents), baseHead: capture.Event.BaseHead})
	}
	if err := proveIntentTypeScriptJavaScriptTargets(ctx, repo, files, captures); err != nil {
		return nil, err
	}
	return intentTypeScriptReferenceContexts(files), nil
}

// Node-style .js imports may name the corresponding recorded .ts source.
// A tracked or pending .js entry would make that interpretation ambiguous.
func proveIntentTypeScriptJavaScriptTargets(ctx context.Context, repo string, files []intentTypeScriptRecordedFile, captures []IntentCandidateCapture) error {
	pending := make(map[string]bool)
	for _, capture := range captures {
		for name := range captureEventPathSet(capture.Event) {
			pending[name] = true
		}
		for _, op := range capture.Ops {
			pending[op.Path] = true
			if op.OldPath.Valid {
				pending[op.OldPath.String] = true
			}
		}
	}
	byHead := make(map[string]map[string]bool)
	for _, file := range files {
		if !intentTypeScriptBaseOID.MatchString(file.baseHead) {
			continue
		}
		imports, _ := intentTypeScriptRecordedDeclarations(file.contents)
		for _, imported := range imports {
			resolved := path.Clean(path.Join(path.Dir(file.path), imported.target))
			if path.Ext(resolved) != ".js" || pending[resolved] {
				continue
			}
			if byHead[file.baseHead] == nil {
				if len(byHead) == 8 {
					continue
				}
				byHead[file.baseHead] = make(map[string]bool)
			}
			if len(byHead[file.baseHead]) < 128 {
				byHead[file.baseHead][resolved] = true
			}
		}
	}
	for head, candidates := range byHead {
		var paths []string
		for name := range candidates {
			paths = append(paths, name)
		}
		sort.Strings(paths)
		entries, err := git.LsTreeLimited(ctx, repo, head, false, 16<<10, paths...)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			delete(candidates, entry.Path)
		}
	}
	for i := range files {
		files[i].javaScriptTargets = byHead[files[i].baseHead]
	}
	return nil
}

func attachIntentTypeScriptReferences(captures []IntentCandidateCapture, references map[int64]string) {
	for i := range captures {
		if context := references[captures[i].Event.Seq]; context != "" {
			captures[i].CapturedDiff = prependIntentRecordedReferenceContext(captures[i].CapturedDiff, context)
		}
	}
}

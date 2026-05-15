// subject_fallback.go — diff-aware subject extraction used by the
// deterministic provider and the replay forced-singleton gate.
//
// The legacy single-op subject was always "Update <basename>" (and "Add"
// / "Remove" for create/delete). When a unified diff is available we can
// often do better: the diff carries added or modified lines that hint at
// the symbol the change touches (Go function, TS class, Python def,
// Markdown heading, ...). Replacing "<basename>" with that symbol turns
// "Update intent_planner.go" into "Update ValidateIntentPlan", which is
// far more useful when scanning git log.
//
// Behavior is fully deterministic: same input always produces the same
// subject. Failure modes (no diff, no symbol found, unsupported file type)
// fall back to the legacy basename form so existing tests with empty
// DiffText still pass.
//
// Scope:
//   - subjects only — body bullets are unchanged.
//   - per-language extraction is best-effort and bounded to the first
//     `subjectFallbackLineCap` lines of diff text for performance.
//   - delete and rename ops always use the basename verb form because the
//     post-image diff is empty (delete) or the symbol semantics belong to
//     the rename target, not the body.
package ai

import (
	"path"
	"regexp"
	"strings"
)

// subjectFallbackLineCap bounds how many diff lines DiffAwareSubject scans
// when looking for a symbol. Most useful headers appear in the first dozen
// lines of a per-op diff section; the cap exists so a 1 MiB pasted blob
// does not turn this helper into a hot loop.
const subjectFallbackLineCap = 200

// DiffAwareSubject returns a subject line for a single op, preferring a
// symbol extracted from `diff` and falling back to the legacy basename verb
// form. The returned string is byte-stable for the same (op, diff) input.
//
// `diff` is a unified-diff section (the output of BuildOpsDiff for a single
// op). When `diff == ""` or no symbol can be extracted, the result equals
// `singleOpSubject(op)` — preserving the legacy default that existing tests
// depend on.
func DiffAwareSubject(op OpItem, diff string) string {
	// Rename ops carry their own dedicated verb format ("Rename a to b")
	// in the legacy renderer. The post-image diff for a rename is often
	// just the new name; substituting an extracted symbol would lose the
	// from→to information that callers depend on.
	if op.Op == "rename" {
		return singleOpSubject(op)
	}
	verb := opVerb(op)
	if verb == "" {
		return singleOpSubject(op)
	}
	if symbol := extractSymbol(op.Path, diff); symbol != "" {
		return verb + " " + symbol
	}
	return singleOpSubject(op)
}

// opVerb maps an op kind to its leading verb. Rename is handled separately
// by the caller because it has a dedicated two-name format.
func opVerb(op OpItem) string {
	switch op.Op {
	case "create":
		return "Add"
	case "delete":
		return "Remove"
	case "modify", "mode", "":
		return "Update"
	default:
		return "Update"
	}
}

// extractSymbol tries every per-language extractor in priority order and
// returns the first non-empty hit. Order is: language by file extension,
// then markdown heading, then nothing.
//
// Path is used only to decide which language extractor to run; diff content
// is the source of truth for the actual symbol name.
func extractSymbol(filePath, diff string) string {
	if diff == "" {
		return ""
	}
	added := addedLines(diff)
	if len(added) == 0 {
		return ""
	}

	switch detectLanguage(filePath) {
	case langGo:
		if name := extractGoSymbol(added); name != "" {
			return name
		}
	case langTSJS:
		if name := extractTSJSSymbol(added); name != "" {
			return name
		}
	case langPython:
		if name := extractPythonSymbol(added); name != "" {
			return name
		}
	case langMarkdown:
		if name := extractMarkdownHeading(added); name != "" {
			return name
		}
	}
	return ""
}

type language int

const (
	langUnknown language = iota
	langGo
	langTSJS
	langPython
	langMarkdown
)

// detectLanguage maps a file extension to its symbol-extraction language.
// Unknown extensions return langUnknown so the caller falls back to the
// basename verb form.
func detectLanguage(filePath string) language {
	ext := strings.ToLower(path.Ext(filePath))
	switch ext {
	case ".go":
		return langGo
	case ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs":
		return langTSJS
	case ".py":
		return langPython
	case ".md", ".markdown":
		return langMarkdown
	default:
		return langUnknown
	}
}

// addedLines returns the lines added or unchanged by the diff (those not
// removed by the patch). Diff hunk headers, file headers, and "-" lines are
// dropped. Result is bounded to `subjectFallbackLineCap` entries so callers
// that hand us a pathological diff do not stall.
//
// We keep both "+" and " " (context) lines because some symbol headers sit
// in unchanged context above the actual edit; for a multi-line modify, the
// surrounding signature is often the most informative anchor.
func addedLines(diff string) []string {
	if diff == "" {
		return nil
	}
	out := make([]string, 0, 32)
	scan := strings.Split(diff, "\n")
	if len(scan) > subjectFallbackLineCap {
		scan = scan[:subjectFallbackLineCap]
	}
	for _, line := range scan {
		if line == "" {
			continue
		}
		// Skip diff metadata.
		if strings.HasPrefix(line, "diff --git") ||
			strings.HasPrefix(line, "index ") ||
			strings.HasPrefix(line, "+++") ||
			strings.HasPrefix(line, "---") ||
			strings.HasPrefix(line, "@@") ||
			strings.HasPrefix(line, "new file mode") ||
			strings.HasPrefix(line, "deleted file mode") ||
			strings.HasPrefix(line, "old mode") ||
			strings.HasPrefix(line, "new mode") ||
			strings.HasPrefix(line, "rename from") ||
			strings.HasPrefix(line, "rename to") ||
			strings.HasPrefix(line, "similarity index") ||
			strings.HasPrefix(line, "Binary files") {
			continue
		}
		switch line[0] {
		case '+':
			out = append(out, line[1:])
		case ' ':
			out = append(out, line[1:])
		case '-':
			// Removed line — skip; the post-image is what we want.
			continue
		default:
			// Some BuildOpsDiff sections begin with a header without a
			// leading marker (mode-only ops, etc). Treat as raw content
			// so markdown headings still extract.
			out = append(out, line)
		}
	}
	return out
}

// goFuncRE matches a Go top-level function or method declaration. It
// captures the function name in the first non-empty capture group:
//
//	func Foo(...
//	func (r *Receiver) Foo(...
//	func (Receiver) foo(...
//
// Generic type parameters are tolerated because they appear before the
// arg list; we anchor on `(` after the name.
var goFuncRE = regexp.MustCompile(`^func\s+(?:\(\s*[^)]+\)\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*[\(\[]`)

func extractGoSymbol(lines []string) string {
	for _, line := range lines {
		trim := strings.TrimLeft(line, " \t")
		if m := goFuncRE.FindStringSubmatch(trim); len(m) >= 2 {
			return m[1]
		}
	}
	return ""
}

// tsJsRE matches common JS/TS top-level declarations. Capture order tries
// function/class/const in that priority. We tolerate `export`, `export
// default`, and `async`. Arrow-function const declarations are picked up
// via the const branch.
var (
	tsClassRE   = regexp.MustCompile(`^(?:export\s+(?:default\s+)?)?(?:abstract\s+)?class\s+([A-Za-z_$][A-Za-z0-9_$]*)`)
	tsFuncRE    = regexp.MustCompile(`^(?:export\s+(?:default\s+)?)?(?:async\s+)?function\s*\*?\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*[\(\<]`)
	tsConstRE   = regexp.MustCompile(`^(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*[:=]`)
	tsMethodRE  = regexp.MustCompile(`^(?:public\s+|private\s+|protected\s+|static\s+|async\s+)*([A-Za-z_$][A-Za-z0-9_$]*)\s*\([^)]*\)\s*[:{]`)
)

func extractTSJSSymbol(lines []string) string {
	for _, line := range lines {
		trim := strings.TrimLeft(line, " \t")
		if m := tsClassRE.FindStringSubmatch(trim); len(m) >= 2 {
			return m[1]
		}
		if m := tsFuncRE.FindStringSubmatch(trim); len(m) >= 2 {
			return m[1]
		}
		if m := tsConstRE.FindStringSubmatch(trim); len(m) >= 2 {
			return m[1]
		}
	}
	// Methods are lower priority because the regex is broader; second pass
	// keeps method matches from outranking real function declarations on
	// the same line.
	for _, line := range lines {
		trim := strings.TrimLeft(line, " \t")
		if m := tsMethodRE.FindStringSubmatch(trim); len(m) >= 2 {
			name := m[1]
			if isJSReservedWord(name) {
				continue
			}
			return name
		}
	}
	return ""
}

// isJSReservedWord filters out common reserved words that the broad
// method regex would otherwise match (if, for, while, switch, return).
func isJSReservedWord(name string) bool {
	switch name {
	case "if", "for", "while", "switch", "return", "do", "catch",
		"function", "constructor", "class", "throw", "yield", "await":
		return true
	}
	return false
}

// pyDefRE and pyClassRE match Python def and class declarations at any
// indent. Decorators are skipped automatically because we only look at
// lines that begin with `def` / `async def` / `class` after lstrip.
var (
	pyDefRE   = regexp.MustCompile(`^(?:async\s+)?def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)
	pyClassRE = regexp.MustCompile(`^class\s+([A-Za-z_][A-Za-z0-9_]*)\s*[\(:]`)
)

func extractPythonSymbol(lines []string) string {
	for _, line := range lines {
		trim := strings.TrimLeft(line, " \t")
		if m := pyClassRE.FindStringSubmatch(trim); len(m) >= 2 {
			return m[1]
		}
		if m := pyDefRE.FindStringSubmatch(trim); len(m) >= 2 {
			return m[1]
		}
	}
	return ""
}

// mdHeadingRE matches a Markdown ATX heading (one to six leading hashes).
// The captured group is the heading text without leading hashes / spaces.
var mdHeadingRE = regexp.MustCompile(`^#{1,6}\s+(.+?)\s*#*\s*$`)

func extractMarkdownHeading(lines []string) string {
	for _, line := range lines {
		trim := strings.TrimLeft(line, " \t")
		if m := mdHeadingRE.FindStringSubmatch(trim); len(m) >= 2 {
			return strings.TrimSpace(m[1])
		}
	}
	return ""
}

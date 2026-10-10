package daemon

import (
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

const intentRelationshipWitnessCap = 2 << 10
const intentRelationshipEvidencePrefix = "Recorded changed relationship witnesses:\n"
const intentRelationshipEvidenceSeparator = "\nRecorded diff:\n"

// Retain actual recorded producer/consumer lines before generic diff clipping.
// This changes the evidence budget, never the rules that validate relationships.
func prioritizeIntentRelationshipEvidence(captures []IntentCandidateCapture) []string {
	type witness struct {
		capture  int
		raw      string
		priority int
	}
	type relationship struct{ producer, consumer witness }
	declarations := make(map[string][]witness)
	code := make([][]intentSourceCodeWitness, len(captures))
	documents := make([]map[string][]string, len(captures))
	public := make(map[string][]witness)
	api := make(map[string][]witness)
	for i, capture := range captures[:min(len(captures), ai.IntentCandidateCaptureCap)] {
		if capture.CapturedDiff == "" {
			continue
		}
		role := intentCaptureRole(capture)
		if role == "documentation" {
			documents[i] = intentDocumentPublicWitnesses(capture.CapturedDiff)
			continue
		}
		code[i] = intentSourceCodeWitnessesWithContext(capture.Event.Path, capture.CapturedDiff, true)
		var declarationLines []intentSourceCodeWitness
		for _, line := range code[i] {
			if line.Raw[0] == '+' || line.Raw[0] == '-' {
				declarationLines = append(declarationLines, line)
			}
		}
		declarationLines = append(declarationLines, intentSourceEnclosingDeclarationsForPath(capture.Event.Path, capture.CapturedDiff)...)
		declarationLines = append(declarationLines, intentSourceCodeWitnessesWithContext(capture.Event.Path, intentProvidedDeclarationContext(capture.CapturedDiff), true)...)
		declarationLines = append(declarationLines, intentRecordedGoConstantWitnesses(capture.Event.Path, capture.CapturedDiff)...)
		apiNames := make(map[string]bool)
		for _, line := range declarationLines {
			if role == "code" && len(apiNames) < 128 &&
				(intentSourceTypeDeclaration.MatchString(line.Code) || intentSourceFunctionDeclaration.MatchString(line.Code)) {
				name, _ := intentSourceLineSymbols(line.Code)
				if name != "" && !apiNames[name] {
					apiNames[name] = true
					api[name] = append(api[name], witness{i, line.Raw, -1})
				}
			}
			name := intentSourceCrossFileDeclaration(line)
			if name != "" && len(declarations[name]) < ai.IntentCandidateCaptureCap {
				priority := 2
				if intentSourceTypeDeclaration.MatchString(line.Code) {
					priority = 0
				} else if intentSourceFunctionDeclaration.MatchString(line.Code) {
					priority = 1
				}
				declarations[name] = append(declarations[name], witness{i, line.Raw, priority})
			}
			if token := intentSourcePublicLineReference(line.Code); token != "" {
				public[token] = append(public[token], witness{i, line.Raw, -1})
			}
		}
	}
	var relationships []relationship
	seenPairs := make(map[[2]int]int)
	addRelationship := func(producer, consumer witness) {
		pair := [2]int{min(producer.capture, consumer.capture), max(producer.capture, consumer.capture)}
		if index, exists := seenPairs[pair]; exists {
			if producer.priority < relationships[index].producer.priority {
				relationships[index] = relationship{producer, consumer}
			}
		} else {
			seenPairs[pair] = len(relationships)
			relationships = append(relationships, relationship{producer, consumer})
		}
	}
	for i, lines := range code {
		seen := make(map[string]bool)
		for _, line := range lines {
			_, used := intentSourceLineSymbols(line.Code)
			var names []string
			for name := range used {
				if len(declarations[name]) > 0 && !seen[name] {
					names = append(names, name)
				}
			}
			sort.Strings(names)
			for _, name := range names {
				seen[name] = true
				for _, producer := range declarations[name] {
					if producer.capture != i && captures[producer.capture].Event.Path != captures[i].Event.Path {
						addRelationship(producer, witness{i, line.Raw, 0})
					}
				}
			}
		}
	}
	// Exact changed path literals prove a reference to the offered target even
	// when that target has no named declaration (configuration or packaging).
	for i, lines := range code {
		for _, line := range lines {
			files, _ := intentSourcePathReferences(line.Code)
			if len(files) == 0 {
				continue
			}
			for j, target := range captures {
				if i != j && intentSourceReferencesFile(files, strings.ToLower(captures[i].Event.Path), strings.ToLower(target.Event.Path), false) {
					addRelationship(witness{j, "", 0}, witness{i, line.Raw, 0})
				}
			}
		}
	}
	for i, references := range documents {
		var tokens []string
		for token := range references {
			tokens = append(tokens, token)
		}
		sort.Strings(tokens)
		for _, token := range tokens {
			producers := append(append([]witness(nil), public[token]...), api[token]...)
			for _, producer := range producers {
				for _, raw := range references[token] {
					if raw != "" {
						addRelationship(producer, witness{i, raw, 0})
					}
				}
			}
		}
	}
	degrees := make([]int, len(captures))
	for _, edge := range relationships {
		degrees[edge.producer.capture]++
		degrees[edge.consumer.capture]++
	}
	sort.SliceStable(relationships, func(i, j int) bool {
		a, b := relationships[i], relationships[j]
		if a.producer.priority != b.producer.priority {
			return a.producer.priority < b.producer.priority
		}
		aDegree, bDegree := min(degrees[a.producer.capture], degrees[a.consumer.capture]), min(degrees[b.producer.capture], degrees[b.consumer.capture])
		return aDegree < bDegree
	})
	// Prefer enough witnessed edges to connect the original relationship graph.
	// Extra repeated references must not displace a late bridge to another file.
	parent := make([]int, len(captures))
	for i := range parent {
		parent[i] = i
	}
	root := func(i int) int {
		for parent[i] != i {
			i = parent[i]
		}
		return i
	}
	selected := make([][]string, len(captures))
	seen := make([]map[string]bool, len(captures))
	sizes := make([]int, len(captures))
	canAppend := func(w witness) bool {
		return seen[w.capture][w.raw] || sizes[w.capture]+len(w.raw)+1 <= intentRelationshipWitnessCap
	}
	appendWitness := func(w witness) {
		if w.raw == "" {
			return
		}
		if seen[w.capture] == nil {
			seen[w.capture] = make(map[string]bool)
		}
		if !seen[w.capture][w.raw] && sizes[w.capture]+len(w.raw)+1 <= intentRelationshipWitnessCap {
			seen[w.capture][w.raw] = true
			selected[w.capture] = append(selected[w.capture], w.raw)
			sizes[w.capture] += len(w.raw) + 1
		}
	}
	for _, edge := range relationships {
		a, b := root(edge.producer.capture), root(edge.consumer.capture)
		if a != b && canAppend(edge.producer) && canAppend(edge.consumer) {
			appendWitness(edge.producer)
			appendWitness(edge.consumer)
			parent[b] = a
		}
	}
	result := make([]string, len(captures))
	for i, capture := range captures {
		prefix, diff := splitIntentRelationshipEvidence(capture.CapturedDiff)
		if len(selected[i]) > 0 {
			prefix += intentRelationshipEvidencePrefix + strings.Join(selected[i], "\n") + "\n" + intentRelationshipEvidenceSeparator
		}
		prefix = ai.RedactDiffSecrets(prefix)
		result[i] = prefix + truncateIntentEvidenceDiff(ai.RedactDiffSecrets(diff), max(0, ai.IntentStageDiffCap-len(prefix)))
	}
	return result
}

func splitIntentRelationshipEvidence(diff string) (string, string) {
	prefix := ""
	const contextPrefix = "Recorded post-image references:\n"
	const contextEnd = "\nRecorded diff:\n"
	if strings.HasPrefix(diff, contextPrefix) {
		if end := strings.Index(diff, contextEnd); end >= 0 {
			prefix, diff = diff[:end+len(contextEnd)], diff[end+len(contextEnd):]
		}
	}
	if strings.HasPrefix(diff, intentRelationshipEvidencePrefix) {
		if end := strings.Index(diff, intentRelationshipEvidenceSeparator); end >= 0 {
			diff = diff[end+len(intentRelationshipEvidenceSeparator):]
		}
	}
	return prefix, diff
}

package daemon

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

// The optional proof uses final recorded post-images, never a mixture of saves
// or the live worktree. HEAD supplies only a known constructor's actual parent.
func proveIntentCobraCLIReferences(ctx context.Context, repo, head string, captures []IntentCandidateCapture) ([]IntentCandidateCapture, error) {
	var invocations []intentCLIInvocation
	for _, capture := range captures {
		if intentCaptureRole(capture) == "documentation" {
			invocations = append(invocations, intentDocumentCLIInvocations(capture.CapturedDiff)...)
			if len(invocations) > 32 {
				return captures, nil
			}
		}
	}
	if repo == "" || len(invocations) == 0 {
		return captures, nil
	}
	ordered := append([]IntentCandidateCapture(nil), captures...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Event.Seq < ordered[j].Event.Seq })
	chains := make(map[string][]IntentCandidateCapture)
	knownPaths := make(map[string]bool)
	for _, capture := range ordered {
		knownPaths[capture.Event.Path] = true
		if path.Ext(capture.Event.Path) == ".go" && !strings.HasSuffix(capture.Event.Path, "_test.go") {
			chains[capture.Event.Path] = append(chains[capture.Event.Path], capture)
		}
	}
	var names []string
	for name := range chains {
		names = append(names, name)
	}
	sort.Strings(names)
	var sources []intentCobraSource
	remaining := 2 << 20
	for _, name := range names {
		chain := chains[name]
		valid := true
		previousOID, previousMode := "", ""
		for i, capture := range chain {
			if len(capture.Ops) != 1 {
				valid = false
				break
			}
			op := capture.Ops[0]
			if op.Path != name || (op.Op != "create" && op.Op != "modify") || !op.AfterOID.Valid || op.AfterMode.String != "100644" ||
				(i > 0 && (op.BeforeOID.String != previousOID || op.BeforeMode.String != previousMode)) {
				valid = false
				break
			}
			previousOID, previousMode = op.AfterOID.String, op.AfterMode.String
		}
		if !valid || len(sources) >= 128 || remaining <= 0 {
			continue
		}
		contents, err := git.CatFileBlobLimited(ctx, repo, previousOID, int64(min(intentSourceReferenceScanCap, remaining)))
		remaining -= min(remaining, len(contents))
		if errors.Is(err, git.ErrStdoutOverflow) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !utf8.Valid(contents) || strings.ContainsRune(string(contents), 0) || !strings.Contains(string(contents), `"github.com/spf13/cobra"`) {
			continue
		}
		sources = append(sources, intentCobraSource{path: name, oid: previousOID, contents: string(contents), seq: chain[len(chain)-1].Event.Seq})
	}
	if len(sources) == 0 {
		return captures, nil
	}
	if head == "" {
		head = "HEAD"
	}
	pinned, err := git.RevParse(ctx, repo, head+"^{commit}")
	if err != nil {
		return nil, err
	}
	queries, baselineFiles := 0, 0
	searched := make(map[string]bool)
	// Only already recorded constructors with the documented first subcommand
	// can authorize a parent lookup. No filename or root command is guessed.
	for _, invocation := range invocations {
		for _, source := range append([]intentCobraSource(nil), sources...) {
			factories := intentCobraFactories([]intentCobraSource{source})
			var candidates []string
			for name, factory := range factories {
				use := strings.Fields(factory.use)
				if len(use) > 0 && use[0] == invocation.command[1] {
					candidates = append(candidates, name)
				}
			}
			sort.Strings(candidates)
			for _, name := range candidates {
				query := path.Dir(source.path) + ":" + name
				if searched[query] {
					continue
				}
				if queries >= 2 || baselineFiles >= 4 {
					break
				}
				searched[query] = true
				queries++
				paths, err := git.RunWithLimit(ctx, git.RunOpts{Dir: repo, Timeout: 5 * time.Second}, 8192, "grep", "-l", "-z", "-F", "-e", name+"(", pinned, "--", path.Dir(source.path)+"/*.go")
				var commandError *git.Error
				if errors.Is(err, git.ErrStdoutOverflow) || (errors.As(err, &commandError) && commandError.ExitCode == 1) {
					continue
				}
				if err != nil {
					return nil, err
				}
				for _, qualified := range strings.Split(string(paths), "\x00") {
					name := strings.TrimPrefix(qualified, pinned+":")
					if name == "" || knownPaths[name] || strings.HasSuffix(name, "_test.go") || path.Dir(name) != path.Dir(source.path) || baselineFiles >= 4 {
						continue
					}
					knownPaths[name] = true
					baselineFiles++
					entries, err := git.LsTreeLimited(ctx, repo, pinned, false, 2048, name)
					if err != nil {
						return nil, err
					}
					if len(entries) != 1 || entries[0].Path != name || entries[0].Mode != "100644" || entries[0].Type != "blob" || remaining <= 0 {
						continue
					}
					contents, err := git.CatFileBlobLimited(ctx, repo, entries[0].OID, int64(min(intentSourceReferenceScanCap, remaining)))
					remaining -= min(remaining, len(contents))
					if errors.Is(err, git.ErrStdoutOverflow) {
						continue
					}
					if err != nil {
						return nil, err
					}
					if utf8.Valid(contents) && !strings.ContainsRune(string(contents), 0) {
						sources = append(sources, intentCobraSource{path: name, oid: entries[0].OID, contents: string(contents)})
					}
				}
			}
		}
	}
	references := make(map[int64]map[string]string)
	// Constructors from separate packages must never jointly prove one command.
	scopes := make(map[string][]intentCobraSource)
	for _, source := range sources {
		scopes[path.Dir(source.path)] = append(scopes[path.Dir(source.path)], source)
	}
	for _, scoped := range scopes {
		factories := intentCobraFactories(scoped)
		for _, invocation := range invocations {
			owners := intentCobraQualifiedOwners(factories, invocation)
			if len(owners) == 0 {
				continue
			}
			var provenance []string
			for _, owner := range owners {
				provenance = append(provenance, owner.path+":"+owner.oid)
			}
			sort.Strings(provenance)
			digest := intentEvidenceHash(strings.Join(provenance, "\n"))
			for _, owner := range owners {
				if owner.seq <= 0 {
					continue
				}
				if references[owner.seq] == nil {
					references[owner.seq] = make(map[string]string)
				}
				references[owner.seq][invocation.key()] = digest
			}
		}
	}
	result := append([]IntentCandidateCapture(nil), captures...)
	for i := range result {
		capture := &result[i]
		for _, source := range sources {
			if source.seq == capture.Event.Seq && len(references[source.seq]) > 0 {
				capture.FileMetadata = ai.WithIntentCLIReferenceProof(capture.FileMetadata, source.seq, source.path, int64(len(source.contents)), references[source.seq])
			}
		}
	}
	return result, nil
}

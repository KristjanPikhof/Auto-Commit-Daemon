package daemon

import (
	"context"
	"errors"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

// Reference context comes from a captured blob, never the live worktree. Only
// script calls and file reads naming another offered path can add evidence.
func loadIntentRecordedReferenceContext(ctx context.Context, repo, sourcePath, oid, mode string, offeredPaths []string, names ...intentReferenceNames) (string, error) {
	if oid == "" || mode == "120000" || mode == "160000" {
		return "", nil
	}
	switch path.Ext(sourcePath) {
	case ".sh", ".py", ".go", ".swift", ".pbxproj":
	default:
		return "", nil
	}
	contents, err := git.CatFileBlobLimited(ctx, repo, oid, intentSourceReferenceScanCap)
	if err != nil && !errors.Is(err, git.ErrStdoutOverflow) {
		return "", err
	}
	if errors.Is(err, git.ErrStdoutOverflow) {
		// A clipped token or line must not invent a reference.
		if end := strings.LastIndexByte(string(contents), '\n'); end >= 0 {
			contents = contents[:end+1]
		} else {
			return "", nil
		}
	}
	if strings.ContainsRune(string(contents), 0) || !utf8.Valid(contents) {
		return "", nil
	}
	switch path.Ext(sourcePath) {
	case ".go":
		references := intentGoImportReferenceContext(sourcePath, string(contents), offeredPaths)
		if len(names) > 0 {
			references += intentRecordedDeclarationContext(sourcePath, string(contents), names[0], intentSourceReferenceContextCap-len(references))
		}
		return references, nil
	case ".swift":
		owner := strings.Split(strings.TrimSuffix(path.Base(sourcePath), ".swift"), "+")[0]
		if len(names) > 0 && !names[0].outside(owner, sourcePath) {
			return "", nil
		}
		return intentRecordedDeclarationContext(sourcePath, string(contents), nil), nil
	case ".pbxproj":
		return intentProjectReferenceContext(sourcePath, string(contents), offeredPaths), nil
	default:
		return intentSourceReferenceContext(sourcePath, string(contents), offeredPaths), nil
	}
}

func prependIntentRecordedReferenceContext(diff, references string) string {
	const prefix = "Recorded post-image references:\n"
	const separator = "\nRecorded diff:\n"
	if strings.HasPrefix(diff, prefix) {
		if end := strings.Index(diff, separator); end >= 0 {
			diff = diff[end+len(separator):]
		}
	}
	if references == "" {
		return diff
	}
	header := ai.RedactDiffSecrets(prefix + references + separator)
	return header + diff
}

func intentRecordedReferenceLines(diff string) string {
	const prefix = "Recorded post-image references:\n"
	const separator = "\nRecorded diff:\n"
	if strings.HasPrefix(diff, prefix) {
		if end := strings.Index(diff, separator); end >= 0 {
			return diff[len(prefix):end]
		}
	}
	return ""
}

func includeIntentRecordedReferenceContext(diff, references string) string {
	return truncateIntentEvidenceDiff(ai.RedactDiffSecrets(prependIntentRecordedReferenceContext(diff, references)), ai.IntentStageDiffCap)
}

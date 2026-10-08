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
func loadIntentRecordedReferenceContext(ctx context.Context, repo, sourcePath, oid, mode string, offeredPaths []string) (string, error) {
	if oid == "" || mode == "120000" || mode == "160000" {
		return "", nil
	}
	switch path.Ext(sourcePath) {
	case ".sh", ".py":
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
	return intentSourceReferenceContext(sourcePath, string(contents), offeredPaths), nil
}

func includeIntentRecordedReferenceContext(diff, references string) string {
	const prefix = "Recorded post-image references:\n"
	const separator = "\nRecorded diff:\n"
	if strings.HasPrefix(diff, prefix) {
		if end := strings.Index(diff, separator); end >= 0 {
			diff = diff[end+len(separator):]
		}
	}
	if references == "" {
		return ai.Truncate(ai.RedactDiffSecrets(diff), ai.IntentStageDiffCap)
	}
	header := ai.RedactDiffSecrets(prefix + references + separator)
	return header + ai.Truncate(ai.RedactDiffSecrets(diff), max(0, ai.IntentStageDiffCap-len(header)))
}

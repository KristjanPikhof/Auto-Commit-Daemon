package git

import (
	"context"
	"fmt"
	"strings"
)

func commitAuthorEnv(ctx context.Context, repoDir, oid string) (map[string]string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%an%x00%ae%x00%aI", oid)
	if err != nil {
		return nil, fmt.Errorf("git rewrite apply: read author for %s: %w", shortApplyOID(oid), err)
	}
	parts := strings.Split(strings.TrimRight(string(out), "\n"), "\x00")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, fmt.Errorf("git rewrite apply: malformed author metadata for %s", shortApplyOID(oid))
	}
	return map[string]string{
		"GIT_AUTHOR_NAME":  parts[0],
		"GIT_AUTHOR_EMAIL": parts[1],
		"GIT_AUTHOR_DATE":  parts[2],
	}, nil
}

func commitMessage(ctx context.Context, repoDir, oid string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%B", oid)
	if err != nil {
		return "", fmt.Errorf("git rewrite apply: read message for %s: %w", shortApplyOID(oid), err)
	}
	return strings.TrimRight(string(out), "\n"), nil
}

func firstParent(ctx context.Context, repoDir, oid string) (string, error) {
	parents, err := parentsOf(ctx, repoDir, oid)
	if err != nil {
		return "", err
	}
	if len(parents) == 0 {
		return "", nil
	}
	return parents[0], nil
}

func parentsOf(ctx context.Context, repoDir, oid string) ([]string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%P", oid)
	if err != nil {
		return nil, fmt.Errorf("git rewrite apply: inspect parents for %s: %w", shortApplyOID(oid), err)
	}
	return strings.Fields(string(out)), nil
}

func sanitizeRewriteRefPart(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

func shortApplyOID(oid string) string {
	if len(oid) > 12 {
		return oid[:12]
	}
	return oid
}

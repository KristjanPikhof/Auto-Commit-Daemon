package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

type commitAllPath struct {
	Path     string `json:"path"`
	OldPath  string `json:"old_path,omitempty"`
	Staged   bool   `json:"staged"`
	Unstaged bool   `json:"unstaged"`
}

type commitAllScope struct {
	ChangedPaths []commitAllPath `json:"changed_paths"`
	QueuedPaths  []string        `json:"queued_paths"`
	IndexDigest  string          `json:"index_digest"`
	Digest       string          `json:"preview_digest"`
}

// inspectCommitAllScope never writes Git objects or stages paths. The digest
// binds path membership, staged content and queued capture identities.
func inspectCommitAllScope(ctx context.Context, repo, dbPath string) (commitAllScope, error) {
	result := commitAllScope{ChangedPaths: []commitAllPath{}, QueuedPaths: []string{}}
	status, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return result, err
	}
	items := strings.Split(string(status), "\x00")
	for i := 0; i < len(items); i++ {
		item := items[i]
		if len(item) < 4 {
			continue
		}
		path := commitAllPath{Path: item[3:], Staged: item[0] != ' ' && item[0] != '?', Unstaged: item[1] != ' '}
		if strings.ContainsAny(item[:2], "RC") && i+1 < len(items) {
			i++
			path.OldPath = items[i]
		}
		result.ChangedPaths = append(result.ChangedPaths, path)
	}
	staged, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "diff", "--cached", "--raw", "--no-abbrev", "-z")
	if err != nil {
		return result, err
	}
	result.IndexDigest, err = gitpkg.IndexContentDigest(ctx, repo)
	if err != nil {
		return result, err
	}
	db, err := openStateDBReadOnly(ctx, dbPath)
	if err != nil {
		return result, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "SELECT seq, path FROM capture_events WHERE state NOT IN ('published','recovered') ORDER BY seq")
	if err != nil {
		return result, err
	}
	defer rows.Close()
	hash := sha256.New()
	_, _ = hash.Write(status)
	_, _ = hash.Write(staged)
	_, _ = hash.Write([]byte(result.IndexDigest))
	seen := map[string]bool{}
	for rows.Next() {
		var seq int64
		var path string
		if err := rows.Scan(&seq, &path); err != nil {
			return result, err
		}
		_ = json.NewEncoder(hash).Encode([]any{seq, path})
		if !seen[path] {
			result.QueuedPaths = append(result.QueuedPaths, path)
			seen[path] = true
		}
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	result.Digest = fmt.Sprintf("%x", hash.Sum(nil))
	return result, nil
}

func renderCommitAllScope(out io.Writer, scope commitAllScope) {
	fmt.Fprintln(out, "Changed paths (staged and unstaged content will be included):")
	for _, path := range scope.ChangedPaths {
		state := "unstaged"
		if path.Staged {
			state = "staged"
			if path.Unstaged {
				state += " and unstaged"
			}
		}
		fmt.Fprintf(out, "  %q (%s)\n", path.Path, state)
	}
	fmt.Fprintln(out, "Already queued paths (may overlap the list above):")
	for _, path := range scope.QueuedPaths {
		fmt.Fprintf(out, "  %q\n", path)
	}
	fmt.Fprintln(out, "Staging: included staged content will be consumed after checkpoint protection. Later edits stay outside this target.")
	fmt.Fprintln(out, "If a previous request stopped because staging changed, its protected work will be saved separately and regrouped with this request.")
}

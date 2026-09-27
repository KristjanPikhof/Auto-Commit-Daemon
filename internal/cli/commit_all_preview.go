package cli

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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
	queuedSeqs   []int64
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
	rows, err := db.QueryContext(ctx, `
SELECT e.seq,e.path,e.old_path,o.ord,o.path,o.old_path
FROM capture_events e LEFT JOIN capture_ops o ON o.event_seq=e.seq
WHERE e.state NOT IN ('published','recovered') ORDER BY e.seq,o.ord`)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	hash := sha256.New()
	_, _ = hash.Write(status)
	_, _ = hash.Write(staged)
	_, _ = hash.Write([]byte(result.IndexDigest))
	seen := map[string]bool{}
	seenSeqs := map[int64]bool{}
	for rows.Next() {
		var seq int64
		var path string
		var oldPath, opPath, opOldPath sql.NullString
		var ord sql.NullInt64
		if err := rows.Scan(&seq, &path, &oldPath, &ord, &opPath, &opOldPath); err != nil {
			return result, err
		}
		_ = json.NewEncoder(hash).Encode([]any{seq, path, oldPath, ord, opPath, opOldPath})
		if !seenSeqs[seq] {
			result.queuedSeqs = append(result.queuedSeqs, seq)
			seenSeqs[seq] = true
		}
		for _, endpoint := range []string{path, oldPath.String, opPath.String, opOldPath.String} {
			if endpoint != "" && !seen[endpoint] {
				result.QueuedPaths = append(result.QueuedPaths, endpoint)
				seen[endpoint] = true
			}
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
	fmt.Fprintln(out, "Staging: included staged content will be consumed after checkpoint protection. Edits after the target freezes stay outside it.")
	fmt.Fprintln(out, "If a previous request stopped because staging changed, its protected work will be saved separately and regrouped with this request.")
}

// The checkpoint may complete after more files arrive. Compare its immutable
// members to the admitted preview; live status alone can miss a captured path
// that was subsequently removed or reverted.
func commitAllTargetMatchesScope(ctx context.Context, db *state.DB, target publicationDrainTarget, scope commitAllScope) (bool, error) {
	paths := make(map[string]bool)
	for _, path := range scope.ChangedPaths {
		paths[path.Path] = true
		if path.OldPath != "" {
			paths[path.OldPath] = true
		}
	}
	for _, path := range scope.QueuedPaths {
		paths[path] = true
	}
	queued := make(map[int64]bool, len(scope.queuedSeqs))
	for _, seq := range scope.queuedSeqs {
		queued[seq] = true
	}
	query, err := db.ReadSQL().PrepareContext(ctx, "SELECT path,old_path FROM capture_events WHERE seq=?")
	if err != nil {
		return false, err
	}
	defer query.Close()
	for _, seq := range target.EventSeqs {
		if queued[seq] {
			continue
		}
		var path string
		var oldPath sql.NullString
		if err := query.QueryRowContext(ctx, seq).Scan(&path, &oldPath); err != nil {
			return false, err
		}
		if !paths[path] || oldPath.Valid && oldPath.String != "" && !paths[oldPath.String] {
			return false, nil
		}
		ops, err := state.LoadCaptureOps(ctx, db, seq)
		if err != nil {
			return false, err
		}
		for _, op := range ops {
			if !paths[op.Path] || op.OldPath.Valid && op.OldPath.String != "" && !paths[op.OldPath.String] {
				return false, nil
			}
		}
	}
	return true, nil
}

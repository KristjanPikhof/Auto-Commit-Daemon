package git

import (
	"path/filepath"
	"strings"
)

// LiteralPathspec returns a Git pathspec that treats path bytes literally.
// Captured repo paths may contain characters such as '*', '[', or ':' that are
// magic in Git pathspec syntax; callers that are proving history for one
// captured path must not let those characters expand to unrelated files.
func LiteralPathspec(path string) string {
	path = strings.TrimSpace(filepath.ToSlash(path))
	path = strings.TrimPrefix(path, "./")
	if path == "" {
		return ""
	}
	return ":(literal)" + path
}

// LiteralPathspecs converts repo-relative paths to literal Git pathspecs,
// dropping empty paths and preserving order.
func LiteralPathspecs(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		if spec := LiteralPathspec(path); spec != "" {
			out = append(out, spec)
		}
	}
	return out
}

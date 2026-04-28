// message.go is the Phase 1 commit-message helper.
//
// Phase 5 (internal/ai) will subsume this with an AI-backed provider plus a
// deterministic fallback; for now this package owns the deterministic
// implementation so the replay path has a stable, reviewable contract.
//
// Format mirrors snapshot-replay.deterministic_message verbatim:
//
//	1 op:
//	  create   -> "Add <basename>"
//	  modify   -> "Update <basename>"
//	  delete   -> "Remove <basename>"
//	  rename   -> "Rename <oldbasename> to <newbasename>"
//	  mode     -> "Update <basename>"
//
//	N ops:
//	  subject  -> "Update N files in <commonDir>" (when present)
//	              or "Update N files"
//
// Body bullets are emitted only when there are >1 ops; for single-op events
// the message is just the subject. The trailing "tool: daemon" footer is
// dropped here because v1 captures already record the tool name in
// daemon_meta.
package daemon

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// DeterministicMessage produces a commit subject + optional body from the
// event + ops alone. Pure function; ignores ctx (kept in the signature so
// it satisfies MessageFn without an adapter).
func DeterministicMessage(_ context.Context, ec EventContext) (string, error) {
	ops := ec.Ops
	if len(ops) == 0 {
		return "Update files", nil
	}
	if len(ops) == 1 {
		return singleOpSubject(ops[0]), nil
	}
	// Multi-op subject + body bullets.
	subject := multiOpSubject(ops)
	var b strings.Builder
	b.WriteString(subject)
	b.WriteString("\n\n")
	for i, op := range ops {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(formatBullet(op))
	}
	return b.String(), nil
}

func singleOpSubject(op state.CaptureOp) string {
	name := basenameOf(op.Path)
	switch op.Op {
	case "create":
		return "Add " + name
	case "modify":
		return "Update " + name
	case "delete":
		return "Remove " + name
	case "rename":
		oldName := basenameOf(op.OldPath.String)
		if oldName == "" {
			oldName = name
		}
		return "Rename " + oldName + " to " + name
	case "mode":
		return "Update " + name
	default:
		return "Update " + name
	}
}

func multiOpSubject(ops []state.CaptureOp) string {
	paths := make([]string, 0, len(ops))
	for _, op := range ops {
		paths = append(paths, op.Path)
	}
	shared := commonDir(paths)
	if shared != "" {
		return fmt.Sprintf("Update %d files in %s", len(ops), shared)
	}
	return fmt.Sprintf("Update %d files", len(ops))
}

func formatBullet(op state.CaptureOp) string {
	if op.Op == "rename" {
		return fmt.Sprintf("- Rename %s -> %s", op.OldPath.String, op.Path)
	}
	return fmt.Sprintf("- %s %s", titleCase(op.Op), op.Path)
}

// titleCase capitalises the leading rune; ASCII-only is fine here because
// op kinds are a fixed alphabet.
func titleCase(s string) string {
	if s == "" {
		return s
	}
	if c := s[0]; c >= 'a' && c <= 'z' {
		return string(c-'a'+'A') + s[1:]
	}
	return s
}

// basenameOf returns the last path component. Mirrors the legacy _basename:
// "" -> "", "a/b/" -> "b", "foo" -> "foo".
func basenameOf(p string) string {
	p = strings.TrimRight(p, "/")
	if p == "" {
		return ""
	}
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// commonDir returns the longest shared directory prefix across paths. If
// the entire prefix equals one of the paths (i.e. the paths fully nest),
// the trailing component is dropped so we don't claim a single file as a
// "directory". Mirrors legacy _common_dir.
func commonDir(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	parts := make([][]string, len(paths))
	for i, p := range paths {
		parts[i] = strings.Split(p, "/")
	}
	var common []string
	for col := 0; ; col++ {
		var first string
		ok := true
		for row, segs := range parts {
			if col >= len(segs) {
				ok = false
				break
			}
			if row == 0 {
				first = segs[col]
				continue
			}
			if segs[col] != first {
				ok = false
				break
			}
		}
		if !ok {
			break
		}
		common = append(common, first)
	}
	if len(common) == 0 {
		return ""
	}
	// If every path's full prefix equals `common`, pop the trailing element
	// — every file lives directly in this dir, but the dir itself is one
	// segment shorter.
	allFullMatch := true
	for _, segs := range parts {
		if len(segs) != len(common) {
			allFullMatch = false
			break
		}
		for i := range common {
			if segs[i] != common[i] {
				allFullMatch = false
				break
			}
		}
		if !allFullMatch {
			break
		}
	}
	if allFullMatch && len(common) > 0 {
		common = common[:len(common)-1]
	}
	return path.Join(common...)
}

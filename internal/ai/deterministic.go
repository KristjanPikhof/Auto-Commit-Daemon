// deterministic.go — rule-based commit message generator.
//
// Output contract is **byte-identical** to the daemon's Phase-1
// DeterministicMessage helper (internal/daemon/message.go) so the replay
// path can swap providers without changing commit content. The
// daemon-package implementation will become a thin wrapper over this
// provider once Phase 5 wires through.
//
// Subject formats:
//
//	1 op:
//	  create   -> "Add <basename>"
//	  modify   -> "Update <basename>"
//	  delete   -> "Remove <basename>"
//	  rename   -> "Rename <oldbasename> to <newbasename>"
//	  mode     -> "Update <basename>"
//	  (other)  -> "Update <basename>"
//
//	N ops:
//	  "Update N files in <commonDir>" (when present)
//	  or "Update N files"
//
// Body bullets are emitted **only** for multi-op events (matches the v1
// daemon: single-op commits are subject-only, the deterministic helper
// does NOT include the legacy "Snapshot seq: N tool: acd" footer because
// the daemon already records that in daemon_meta).
package ai

import (
	"context"
	"fmt"
	"path"
	"strings"
)

// DeterministicProvider is the always-available rule-based provider.
// The struct is empty so callers may construct it as a value literal
// without needing a constructor — it has no dependencies and no state.
type DeterministicProvider struct{}

// Name returns the canonical provider identifier used in Result.Source.
func (DeterministicProvider) Name() string { return "deterministic" }

// NeedsDiff reports that deterministic generation only needs event metadata.
func (DeterministicProvider) NeedsDiff() bool { return false }

// Generate composes a Result from CommitContext alone. ctx is honoured for
// cancellation but the function is otherwise pure.
func (p DeterministicProvider) Generate(ctx context.Context, cc CommitContext) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	ops := normalizeOps(cc)
	if len(ops) == 0 {
		return Result{
			Subject: "Update files",
			Source:  p.Name(),
		}, nil
	}
	if len(ops) == 1 {
		return Result{
			Subject: singleOpSubject(ops[0]),
			Source:  p.Name(),
		}, nil
	}

	subject := multiOpSubject(ops)
	var b strings.Builder
	for i, op := range ops {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(formatBullet(op))
	}
	return Result{
		Subject: subject,
		Body:    b.String(),
		Source:  p.Name(),
	}, nil
}

// normalizeOps collapses the CommitContext input shape (which carries
// either a single Op + Path or a MultiOp slice) into a single OpItem
// list. When MultiOp is set we use it verbatim; otherwise we synthesize
// a single OpItem from Op/Path/OldPath. This mirrors how the daemon
// passes ops to the legacy generator.
func normalizeOps(cc CommitContext) []OpItem {
	if len(cc.MultiOp) > 0 {
		return cc.MultiOp
	}
	if cc.Op == "" && cc.Path == "" {
		return nil
	}
	return []OpItem{{Path: cc.Path, Op: cc.Op, OldPath: cc.OldPath}}
}

// singleOpSubject mirrors daemon.singleOpSubject 1:1.
func singleOpSubject(op OpItem) string {
	name := basenameOf(op.Path)
	switch op.Op {
	case "create":
		return "Add " + name
	case "modify":
		return "Update " + name
	case "delete":
		return "Remove " + name
	case "rename":
		oldName := basenameOf(op.OldPath)
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

// multiOpSubject mirrors daemon.multiOpSubject 1:1.
func multiOpSubject(ops []OpItem) string {
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

// formatBullet mirrors daemon.formatBullet 1:1.
func formatBullet(op OpItem) string {
	if op.Op == "rename" {
		return fmt.Sprintf("- Rename %s -> %s", op.OldPath, op.Path)
	}
	return fmt.Sprintf("- %s %s", titleCase(op.Op), op.Path)
}

// titleCase capitalises the leading rune (ASCII-only — op kinds are
// drawn from a fixed alphabet).
func titleCase(s string) string {
	if s == "" {
		return s
	}
	if c := s[0]; c >= 'a' && c <= 'z' {
		return string(c-'a'+'A') + s[1:]
	}
	return s
}

// basenameOf returns the last path component. Mirrors the legacy
// _basename: "" -> "", "a/b/" -> "b", "foo" -> "foo".
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

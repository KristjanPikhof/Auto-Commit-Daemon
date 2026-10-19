package daemon

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

func validateOps(ops []state.CaptureOp) string {
	for _, op := range ops {
		if op.Path == "" {
			return fmt.Sprintf("missing path for op %q", op.Op)
		}
		switch op.Op {
		case "create", "modify", "mode", "rename":
			if !op.AfterOID.Valid || op.AfterOID.String == "" {
				return fmt.Sprintf("missing after_oid for %s %s", op.Op, op.Path)
			}
			if !op.AfterMode.Valid || op.AfterMode.String == "" {
				return fmt.Sprintf("missing after_mode for %s %s", op.Op, op.Path)
			}
		}
		switch op.Op {
		case "modify", "mode", "delete":
			if !op.BeforeOID.Valid || op.BeforeOID.String == "" {
				return fmt.Sprintf("missing before_oid for %s %s", op.Op, op.Path)
			}
			if !op.BeforeMode.Valid || op.BeforeMode.String == "" {
				return fmt.Sprintf("missing before_mode for %s %s", op.Op, op.Path)
			}
		case "rename":
			if !op.OldPath.Valid || op.OldPath.String == "" {
				return fmt.Sprintf("missing old_path for rename %s", op.Path)
			}
		}
	}
	return ""
}

// touchedPaths is the set of paths that an event's ops will read or write.
func touchedPaths(ops []state.CaptureOp) []string {
	seen := map[string]struct{}{}
	for _, op := range ops {
		if op.Path != "" {
			seen[op.Path] = struct{}{}
		}
		if op.OldPath.Valid && op.OldPath.String != "" {
			seen[op.OldPath.String] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// resolveTreeOID returns the tree OID at the given commit. Empty input
// returns ("", nil) so the caller can short-circuit. Missing refs are also
// surfaced as ("", nil) — there is no parent tree to compare against.
func resolveTreeOID(ctx context.Context, repoRoot, commit string) (string, error) {
	if commit == "" {
		return "", nil
	}
	tree, err := git.RevParse(ctx, repoRoot, commit+"^{tree}")
	if err != nil {
		if errors.Is(err, git.ErrRefNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("rev-parse %s^{tree}: %w", commit, err)
	}
	return tree, nil
}

func traceLegacyRecovery(logger acdtrace.Logger, repoRoot string, cctx CaptureContext, ev state.CaptureEvent, class, decision, reason string, output map[string]any) {
	input := map[string]any{
		"operation": ev.Operation,
		"path":      ev.Path,
	}
	recordTrace(logger, acdtrace.Event{
		Repo:       repoRoot,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: class,
		Decision:   decision,
		Reason:     reason,
		Input:      input,
		Output:     output,
		Error:      traceError(decision, reason),
		Seq:        ev.Seq,
		Generation: cctx.BranchGeneration,
	})
}

func traceError(decision, reason string) string {
	if reason == "" {
		return ""
	}
	switch decision {
	case state.EventStateFailed, state.EventStateBlockedConflict:
		return reason
	default:
		return ""
	}
}

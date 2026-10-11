// These helpers reconstruct already-recorded legacy publication operations.
// They read immutable members only; they never offer, enqueue or publish work.
package daemon

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type legacyPublicationOps struct {
	Primary   state.CaptureEvent
	MergedOps []state.CaptureOp
}

func reconstructLegacyPublicationOps(ctx context.Context, db *state.DB, events []state.CaptureEvent, load func(context.Context, *state.DB, int64) ([]state.CaptureOp, error)) ([]legacyPublicationOps, error) {
	opsBySeq := make(map[int64][]state.CaptureOp, len(events))
	for _, event := range events {
		ops, err := load(ctx, db, event.Seq)
		if err != nil {
			return nil, fmt.Errorf("legacy publication operations %d: %w", event.Seq, err)
		}
		opsBySeq[event.Seq] = ops
	}
	var result []legacyPublicationOps
	for start := 0; start < len(events); {
		first := events[start]
		path, eligible := singleCoalescePath(first, opsBySeq[first.Seq])
		end := start + 1
		if eligible {
			for end < len(events) {
				next := events[end]
				nextPath, nextEligible := singleCoalescePath(next, opsBySeq[next.Seq])
				if !nextEligible || nextPath != path || !sameBranchTokenForCoalesce(first, next) {
					break
				}
				end++
			}
		}
		if end-start == 1 {
			result = append(result, legacyPublicationOps{first, opsBySeq[first.Seq]})
		} else {
			merged := mergeSamePathOps(events[start:end], opsBySeq, path)
			if len(merged) > 0 {
				result = append(result, legacyPublicationOps{first, merged})
			} else {
				for _, event := range events[start:end] {
					result = append(result, legacyPublicationOps{event, opsBySeq[event.Seq]})
				}
			}
		}
		start = end
	}
	return result, nil
}

// singleCoalescePath reports the touched path when ev's ops touch EXACTLY
// one path AND the op kind is foldable (modify, mode, create). Returns
// ("", false) for renames (path identity changes), deletes (path existence
// changes), multi-path events, and zero-op events. ev.Path is checked
// against the ops' touched-paths set so a malformed event whose header path
// differs from its op path is rejected (cannot prove what the user
// intended).
func singleCoalescePath(ev state.CaptureEvent, ops []state.CaptureOp) (string, bool) {
	if ev.Path == "" || len(ops) == 0 {
		return "", false
	}
	if ev.OldPath.Valid && ev.OldPath.String != "" {
		// Rename or move: header carries an old path distinct from the
		// new path. Renames change the path identity itself, so a same-
		// path run cannot include them.
		return "", false
	}
	touched := touchedPaths(ops)
	if len(touched) != 1 || touched[0] != ev.Path {
		return "", false
	}
	for _, op := range ops {
		switch op.Op {
		case "modify", "mode", "create":
			// foldable
		default:
			return "", false
		}
	}
	return ev.Path, true
}

// sameBranchTokenForCoalesce reports whether two consecutive pending events
// share the same branch token. The token is (branch_ref, branch_generation,
// base_head): a divergence in any field means a branch transition or an
// external committer moved HEAD between captures, both of which break the
// "linear edits on the same path" invariant the squash depends on.
func sameBranchTokenForCoalesce(a, b state.CaptureEvent) bool {
	return a.BranchRef == b.BranchRef &&
		a.BranchGeneration == b.BranchGeneration &&
		a.BaseHead == b.BaseHead
}

// mergeSamePathOps squashes a run of single-path event op lists into one
// CaptureOp whose before-state matches the run's first observed state and
// whose after-state matches the run's last observed state. Returns nil when
// the run cannot be merged unambiguously (no foldable op found).
//
// Op label promotion:
//   - first=create, last=create   -> create  (idempotent re-create)
//   - first=create, last=modify   -> create  (still no prior tree entry)
//   - first=modify, last=modify   -> modify
//   - first=mode,   last=mode     -> mode    (mode-only run)
//   - mixed (modify after mode, etc.) -> modify (the strongest write op).
//
// The before-state comes from the FIRST op of the FIRST event whose op
// touches runPath, so the merged op preserves the original baseline the
// captures were taken against. The after-state comes from the LAST op of
// the LAST event whose op touches runPath, so write-tree sees the latest
// blob/mode the user actually settled on.
func mergeSamePathOps(events []state.CaptureEvent, opsBySeq map[int64][]state.CaptureOp, runPath string) []state.CaptureOp {
	if len(events) == 0 {
		return nil
	}
	var (
		firstOp        state.CaptureOp
		lastOp         state.CaptureOp
		firstFound     bool
		lastFound      bool
		firstWasCreate bool
		anyMutation    bool
	)
	for i, ev := range events {
		ops := opsBySeq[ev.Seq]
		for _, op := range ops {
			if op.Path != runPath {
				continue
			}
			switch op.Op {
			case "modify", "mode", "create":
				// foldable
			default:
				// Defensive: singleCoalescePath should have rejected
				// non-foldable ops upstream. Bail out so the caller can
				// fall back to per-event passthrough rather than emit a
				// silently wrong merge.
				return nil
			}
			anyMutation = true
			if !firstFound {
				firstOp = op
				firstFound = true
				firstWasCreate = op.Op == "create" && i == 0
			}
			lastOp = op
			lastFound = true
		}
	}
	if !firstFound || !lastFound || !anyMutation {
		return nil
	}
	merged := state.CaptureOp{
		EventSeq:   events[len(events)-1].Seq,
		Ord:        0,
		Path:       runPath,
		BeforeOID:  firstOp.BeforeOID,
		BeforeMode: firstOp.BeforeMode,
		AfterOID:   lastOp.AfterOID,
		AfterMode:  lastOp.AfterMode,
		Fidelity:   lastOp.Fidelity,
	}
	// Promote the op label. A run that starts with a create keeps the
	// create label even if subsequent events were modify/mode — the
	// repository never saw the path until this run, so the merged op must
	// be a create from write-tree's perspective. A pure mode-only run
	// stays as mode. Otherwise the strongest write op (modify) wins, which
	// also matches write-tree's update-index --index-info encoding.
	switch {
	case firstWasCreate:
		merged.Op = "create"
		// A create must not carry a BeforeOID — write-tree will reject
		// the row otherwise (the path did not exist).
		merged.BeforeOID = sql.NullString{}
		merged.BeforeMode = sql.NullString{}
	case firstOp.Op == "mode" && lastOp.Op == "mode" && allOpsAreMode(events, opsBySeq, runPath):
		merged.Op = "mode"
	default:
		merged.Op = "modify"
	}
	return []state.CaptureOp{merged}
}

// allOpsAreMode reports whether every op in the run that touches runPath is
// a "mode" op. Used by mergeSamePathOps to keep a pure mode-only run from
// being upgraded to a modify (which would re-rewrite identical blob data).
func allOpsAreMode(events []state.CaptureEvent, opsBySeq map[int64][]state.CaptureOp, runPath string) bool {
	for _, ev := range events {
		for _, op := range opsBySeq[ev.Seq] {
			if op.Path != runPath {
				continue
			}
			if op.Op != "mode" {
				return false
			}
		}
	}
	return true
}

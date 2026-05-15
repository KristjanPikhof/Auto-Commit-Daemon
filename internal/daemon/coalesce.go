// coalesce.go folds runs of consecutive single-path captures into a single
// offered window entry before the intent planner sees them.
//
// Why: a burst of edits on one file (save, save, save, save) currently fans
// the planner request out to N entries. The planner sees N decisions where
// the operator made one. Collapsing same-path runs upstream lets the planner
// reason about user intent (one file evolving) instead of save-key noise.
//
// Coalescing only applies to consecutive captures touching EXACTLY ONE path,
// and only when every capture in the run targets the same path. Multi-path
// events (renames, multi-file batches) and same-path captures interleaved
// with other paths NEVER coalesce — preserving causal order is more
// important than minimizing the planner offer.
//
// Boundaries that stop a run:
//
//   - The next pending row touches a different path (or any additional path).
//   - The next pending row's branch token (branch_ref + branch_generation +
//     base_head) differs from the run head's. A different base_head means an
//     external committer or replay pass moved HEAD between captures, so
//     squashing across the seam would chain off a stale anchor. Different
//     branch_ref / branch_generation would silently land work on the wrong
//     ref or against a rewritten history.
//   - state.PendingEvents already filters past blocked_conflict / failed
//     barriers per (branch_ref, branch_generation) and never returns
//     published rows, so terminal barriers do not need an in-loop check —
//     they simply do not appear in the input slice.
//
// The squash composes ops in seq order: the first op's BeforeOID/BeforeMode
// is preserved as the merged entry's before-state, and the last op's
// AfterOID/AfterMode wins as the merged entry's after-state. The Op label
// promotes create -> modify if a later op overwrites the create (the path
// existed before the run completed, even if the first capture saw a
// non-existent file mid-burst). Rename and delete ops never coalesce — they
// either change the path identity or the path's existence, both of which
// break the "single same-path edit chain" invariant.
//
// ACD_INTENT_PATH_COALESCE controls the feature. It defaults ON; set to
// "0", "false", "no", or "off" (case-insensitive) to disable. The daemon
// resolves the env once per replay pass, so changes require a daemon
// restart (matches the existing env-restart pattern documented in
// CLAUDE.md).
package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// coalesceToken records the additional capture events that a coalesced
// intent offer covers beyond its representative event. Empty for the
// degenerate single-event case. Callers that publish a coalesced offer must
// settle every event in OriginalSeqs / Covered so the resulting commit
// carries one decision_records row per original seq (joined by commit_oid).
type coalesceToken struct {
	// OriginalSeqs lists every capture_events.seq folded into the entry,
	// in seq-ascending order. Always includes the representative seq as the
	// first element so callers iterating the token never need to special-
	// case the primary.
	OriginalSeqs []int64
	// Covered is the slice of capture events the representative absorbs
	// (all events in the run except the representative). Used at publish
	// time to mark every covered row as published with the same commit_oid
	// and append per-seq decision rows. Empty for non-coalesced entries.
	Covered []state.CaptureEvent
}

// coalescedOffer is a single offered planner entry after the same-path
// coalesce pass. For non-coalesced rows, Token.OriginalSeqs has length 1 and
// Token.Covered is empty.
type coalescedOffer struct {
	// Primary is the representative event for the coalesced run. It is the
	// EARLIEST event in the run (lowest seq) so the planner offer keeps the
	// stable "first observed at seq=X" semantics it had pre-coalesce.
	Primary state.CaptureEvent
	// MergedOps is the squashed op list applied at publish time. For a
	// non-coalesced entry it equals the primary event's stored ops. For a
	// coalesced run it is a single entry per touched path whose
	// BeforeOID/BeforeMode comes from the run's first op and
	// AfterOID/AfterMode comes from the run's last op.
	MergedOps []state.CaptureOp
	// Token carries the original seqs + additional covered events. Always
	// populated even for length-1 runs (Token.OriginalSeqs has one element,
	// Token.Covered is empty) so downstream code never needs nil checks.
	Token coalesceToken
}

// envIntentPathCoalesce is the env var that controls the coalesce pass.
// Default ON to ship the new behavior automatically; "0"/"false"/"no"/"off"
// (case-insensitive) opts out. Mirrors the existing env-restart pattern —
// the daemon resolves the env once per replay pass, so toggling requires a
// restart.
const envIntentPathCoalesce = "ACD_INTENT_PATH_COALESCE"

// pathCoalesceEnabled reports whether ACD_INTENT_PATH_COALESCE permits
// folding. Empty / unset / any non-disable spelling enables coalesce.
func pathCoalesceEnabled() bool {
	raw := strings.TrimSpace(os.Getenv(envIntentPathCoalesce))
	if raw == "" {
		return true
	}
	switch strings.ToLower(raw) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// loadCaptureOpsFn indirects state.LoadCaptureOps so tests can inject ops
// without writing to a real DB. Production wires straight through.
type loadCaptureOpsFn func(ctx context.Context, db *state.DB, seq int64) ([]state.CaptureOp, error)

// coalesceIntentWindow folds runs of consecutive single-path captures in
// the supplied window into one offered entry per run. Returns one
// coalescedOffer per resulting offer in seq order. Each offer carries the
// merged ops and the token of original seqs that fed into it.
//
// Behavior:
//   - When enabled is false, returns one offer per input event (Token has a
//     single OriginalSeqs entry, no Covered). Same-path runs are not folded.
//   - Multi-path captures are passed through as their own offer (Token still
//     has one OriginalSeqs entry, no Covered).
//   - A run extends only while the next event touches the SAME single path
//     AND shares the run head's branch token (branch_ref, branch_generation,
//     base_head). Either mismatch closes the current run; the next event
//     starts a fresh one.
//   - The merged ops slice contains exactly one CaptureOp per touched path
//     (always exactly one path for a coalesced run). The op label inherits
//     from the LAST event in the run, except a create followed by a modify
//     promotes to the strongest applicable kind so write-tree sees the
//     correct after-state. Empty merged ops is treated as a degenerate run
//     and the constituent events are emitted individually.
//
// Errors from the ops loader propagate up. Each per-event load happens at
// most once even when enabled=false so the planner request builder can
// reuse the loaded ops without a second DB round trip.
func coalesceIntentWindow(
	ctx context.Context,
	db *state.DB,
	window []state.CaptureEvent,
	enabled bool,
	load loadCaptureOpsFn,
) ([]coalescedOffer, error) {
	if load == nil {
		load = state.LoadCaptureOps
	}
	if len(window) == 0 {
		return nil, nil
	}

	// Pre-load ops for every event so the run builder can decide whether a
	// row is a single-path candidate without a second loader call.
	opsBySeq := make(map[int64][]state.CaptureOp, len(window))
	for _, ev := range window {
		ops, err := load(ctx, db, ev.Seq)
		if err != nil {
			return nil, fmt.Errorf("daemon: coalesce: load ops seq=%d: %w", ev.Seq, err)
		}
		opsBySeq[ev.Seq] = ops
	}

	out := make([]coalescedOffer, 0, len(window))
	if !enabled {
		for _, ev := range window {
			out = append(out, coalescedOffer{
				Primary:   ev,
				MergedOps: opsBySeq[ev.Seq],
				Token: coalesceToken{
					OriginalSeqs: []int64{ev.Seq},
				},
			})
		}
		return out, nil
	}

	i := 0
	for i < len(window) {
		head := window[i]
		headOps := opsBySeq[head.Seq]
		runPath, headEligible := singleCoalescePath(head, headOps)
		if !headEligible {
			out = append(out, coalescedOffer{
				Primary:   head,
				MergedOps: headOps,
				Token: coalesceToken{
					OriginalSeqs: []int64{head.Seq},
				},
			})
			i++
			continue
		}

		// Greedily extend the run while the next event qualifies.
		runEnd := i
		for j := i + 1; j < len(window); j++ {
			next := window[j]
			if !sameBranchTokenForCoalesce(head, next) {
				break
			}
			nextOps := opsBySeq[next.Seq]
			nextPath, nextEligible := singleCoalescePath(next, nextOps)
			if !nextEligible || nextPath != runPath {
				break
			}
			runEnd = j
		}

		if runEnd == i {
			// Single-event "run": passthrough.
			out = append(out, coalescedOffer{
				Primary:   head,
				MergedOps: headOps,
				Token: coalesceToken{
					OriginalSeqs: []int64{head.Seq},
				},
			})
			i++
			continue
		}

		// Multi-event run: fold ops + collect token.
		runEvents := window[i : runEnd+1]
		merged := mergeSamePathOps(runEvents, opsBySeq, runPath)
		if len(merged) == 0 {
			// Defensive: degenerate run merge produced nothing. Fall back
			// to per-event passthrough so we never silently drop rows.
			for _, ev := range runEvents {
				out = append(out, coalescedOffer{
					Primary:   ev,
					MergedOps: opsBySeq[ev.Seq],
					Token: coalesceToken{
						OriginalSeqs: []int64{ev.Seq},
					},
				})
			}
			i = runEnd + 1
			continue
		}

		seqs := make([]int64, 0, len(runEvents))
		for _, ev := range runEvents {
			seqs = append(seqs, ev.Seq)
		}
		covered := make([]state.CaptureEvent, 0, len(runEvents)-1)
		covered = append(covered, runEvents[1:]...)
		out = append(out, coalescedOffer{
			Primary:   runEvents[0],
			MergedOps: merged,
			Token: coalesceToken{
				OriginalSeqs: seqs,
				Covered:      covered,
			},
		})
		i = runEnd + 1
	}
	return out, nil
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

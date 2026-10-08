package daemon

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const intentGoalLookaheadDiffCap = 4096

var intentGoalDeclaration = regexp.MustCompile(`\b(?:func(?:\s+\([^)]*\))?|function|def|class|struct|type|enum|interface|protocol)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)

type intentGoalReferences struct {
	declarations []string
	symbols      map[string]struct{}
}

// Expand from a processing window into its recorded dependency closure. This
// reads captured objects only, and never includes captures outside a frozen
// publication target or treats directory/time similarity as a companion.
func expandIntentGoalWindow(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	active CaptureContext,
	pending, window []state.CaptureEvent,
	cfg intentReplayConfig,
	now time.Time,
) ([]state.CaptureEvent, []IntentDependencyHint, string, error) {
	target := make(map[int64]bool, len(cfg.targetEventSeqs))
	for _, seq := range cfg.targetEventSeqs {
		target[seq] = true
	}
	selected := make(map[int64]bool, len(window))
	for _, event := range window {
		selected[event.Seq] = true
	}
	captures := make([]IntentCandidateCapture, 0, state.IntentCandidateMaxCaptures)
	bySeq := make(map[int64]IntentCandidateCapture)
	load := func(event state.CaptureEvent) error {
		ops, err := state.LoadCaptureOps(ctx, db, event.Seq)
		if err != nil {
			return err
		}
		diff, err := BuildOpsDiffWithCap(ctx, repoRoot, ops, intentGoalLookaheadDiffCap)
		if err != nil {
			return err
		}
		capture := IntentCandidateCapture{Event: event, Ops: ops, CapturedDiff: diff}
		captures = append(captures, capture)
		bySeq[event.Seq] = capture
		return nil
	}
	for _, event := range window {
		if err := load(event); err != nil {
			return nil, nil, "", err
		}
	}
	for _, event := range pending {
		if selected[event.Seq] || (len(target) > 0 && !target[event.Seq]) {
			continue
		}
		if len(captures) >= state.IntentCandidateMaxCaptures {
			break
		}
		if err := load(event); err != nil {
			return nil, nil, "", err
		}
	}
	dependencies, err := BuildIntentCandidateDependencies(active.BranchRef,
		active.BranchGeneration, captures, nil, now)
	if err != nil {
		return nil, nil, "", err
	}
	request := ai.IntentPlanRequestV2{}
	for _, capture := range captures {
		request.OfferedCaptures = append(request.OfferedCaptures, ai.OfferedCapture{
			Seq: capture.Event.Seq, Path: capture.Event.Path, CapturedDiff: capture.CapturedDiff,
		})
	}
	for _, edge := range dependencies {
		request.Dependencies = append(request.Dependencies, ai.IntentCaptureDependency{
			FromSeq: edge.PrerequisiteSeq, ToSeq: edge.DependentSeq,
			Strength: ai.IntentDependencyStrength(edge.Strength), Kind: edge.Kind, EvidenceHash: edge.Evidence,
		})
	}
	var companions []ai.IntentCaptureDependency
	references := make(map[int64]intentGoalReferences, len(captures))
	for _, capture := range captures {
		item := intentGoalReferences{symbols: runtimeIntentSymbols(capture.CapturedDiff)}
		for _, match := range intentGoalDeclaration.FindAllStringSubmatch(capture.CapturedDiff, 128) {
			if len(match[1]) >= 8 {
				item.declarations = append(item.declarations, strings.ToLower(match[1]))
			}
		}
		references[capture.Event.Seq] = item
	}
	for _, edge := range groundedIntentRequestDependencies(request) {
		if edge.Strength != ai.IntentDependencyHard && !intentGoalCompanionEdge(edge, references) {
			continue
		}
		companions = append(companions, edge)
	}
	for changed := true; changed; {
		changed = false
		for _, edge := range companions {
			if selected[edge.FromSeq] == selected[edge.ToSeq] {
				continue
			}
			selected[edge.FromSeq], selected[edge.ToSeq] = true, true
			changed = true
		}
	}
	var expanded []state.CaptureEvent
	paths := make(map[string]struct{})
	for _, event := range pending {
		if !selected[event.Seq] {
			continue
		}
		capture, known := bySeq[event.Seq]
		if !known {
			return nil, nil, "skipped_due_intent_goal_context_limit", nil
		}
		if !pathQuiescentForEvent(event, capture.Ops, cfg.pathQuiescence, now) {
			return nil, nil, "skipped_due_path_quiescence", nil
		}
		expanded = append(expanded, event)
		for _, name := range intentCapturePaths(capture) {
			paths[name] = struct{}{}
		}
	}
	// A known same-path successor beyond the bounded detailed context must
	// remain protected rather than be silently cut off by the context limit.
	for _, event := range pending {
		if len(target) > 0 && !target[event.Seq] {
			continue
		}
		if !selected[event.Seq] && captureEventTouchesAnyPath(event, paths) {
			return nil, nil, "skipped_due_intent_goal_context_limit", nil
		}
	}
	var hints []IntentDependencyHint
	for _, edge := range companions {
		if selected[edge.FromSeq] && selected[edge.ToSeq] {
			hints = append(hints, IntentDependencyHint{
				PrerequisiteSeq: edge.FromSeq, DependentSeq: edge.ToSeq,
				Strength: edge.Strength, Kind: edge.Kind,
				Evidence: "recorded companion relationship: " + edge.EvidenceHash,
			})
		}
	}
	return expanded, hints, "", nil
}

func intentGoalCompanionEdge(edge ai.IntentCaptureDependency, references map[int64]intentGoalReferences) bool {
	switch edge.Kind {
	case "test_source", "migration_test", "import_reference", "generated_artifact_reference":
		return true
	case "symbol_hash":
		for _, seq := range []int64{edge.FromSeq, edge.ToSeq} {
			for _, name := range references[seq].declarations {
				other := edge.ToSeq
				if seq == other {
					other = edge.FromSeq
				}
				if _, used := references[other].symbols[name]; used {
					return true
				}
			}
		}
	}
	return false
}

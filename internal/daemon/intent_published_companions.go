package daemon

import (
	"context"
	"errors"
	"path"
	"sort"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// A finalized former companion can explain a late correction without becoming
// newly offered work. Its complete recorded post-image must still be in HEAD.
func loadPublishedIntentFormerCompanions(ctx context.Context, db *state.DB, input *IntentCandidateEvaluation, existing []state.IntentCandidate) ([]state.IntentCandidate, error) {
	input.frozenPublishedContext = nil
	if input.RepoPath == "" || !input.IncludeDiffs || input.LatestCommit == nil || input.LatestCommit.OID == "" {
		return existing, nil
	}
	var ids []string
	var active, published []state.IntentCandidate
	known := make(map[string]bool)
	for _, candidate := range existing {
		known[candidate.ID] = true
		if candidate.Status == state.IntentCandidateSoftPublished || candidate.Status == state.IntentCandidatePublished {
			published = append(published, candidate)
		} else {
			active = append(active, candidate)
			ids = append(ids, candidate.ID)
		}
	}
	if len(active) >= state.IntentCandidateMaxOpenPerPair {
		return existing, nil
	}
	companions, err := state.IntentPublishedFormerCompanions(ctx, db, input.BranchRef, input.BranchGeneration, ids, state.IntentCandidateMaxOpenPerPair-len(active))
	if err != nil {
		return nil, err
	}
	frozen, err := discoverIntentPublishedFrozenContext(ctx, db, *input)
	if err != nil {
		return nil, err
	}
	frozenIDs := make(map[string]bool, len(frozen))
	for _, candidate := range frozen {
		frozenIDs[candidate.ID] = true
	}
	companions = append(frozen, companions...)
	regressions, matchedTests, err := discoverIntentPublishedGoRegressions(ctx, db, *input)
	if err != nil {
		return nil, err
	}
	companions = append(companions, regressions...)
	tsRegressions, tsTests, err := discoverIntentPublishedTypeScriptRegressions(ctx, db, *input)
	if err != nil {
		return nil, err
	}
	companions = append(companions, tsRegressions...)
	if matchedTests == nil {
		matchedTests = make(map[string]map[string]string)
	}
	for id, paths := range tsTests {
		if matchedTests[id] == nil {
			matchedTests[id] = make(map[string]string)
		}
		for name := range paths {
			matchedTests[id][name] = ""
		}
	}
	if len(companions) == 0 {
		return existing, nil
	}
	// Planner history summaries use abbreviated IDs. Availability proof pins
	// a complete commit ID so its final HEAD comparison stays exact.
	head, err := git.RevParse(ctx, input.RepoPath, input.LatestCommit.OID+"^{commit}")
	if errors.Is(err, git.ErrRefNotFound) || errors.Is(err, git.ErrRefAmbiguous) {
		return existing, nil
	}
	if err != nil {
		return nil, err
	}
	members := 0
	var verified []state.IntentCandidate
	verifiedIDs := make(map[string]bool)
	for _, candidate := range companions {
		if len(active)+len(verified) >= state.IntentCandidateMaxOpenPerPair {
			break
		}
		if verifiedIDs[candidate.ID] || (known[candidate.ID] && !frozenIDs[candidate.ID]) || candidate.Status != state.IntentCandidatePublished || !candidate.PublishedCommitOID.Valid || len(candidate.Events) == 0 || len(candidate.Events) > state.IntentCandidateMaxCaptures {
			continue
		}
		if members+len(candidate.Events) > state.IntentCandidateMaxCaptures {
			continue
		}
		members += len(candidate.Events)
		candidate, captures, valid, err := normalizedPublishedIntentContext(ctx, db, candidate)
		if err != nil {
			return nil, err
		}
		var ops []state.CaptureOp
		for _, capture := range captures {
			ops = append(ops, capture.Ops...)
		}
		if !valid || len(ops) == 0 || len(ops) > state.IntentCandidateMaxCaptures {
			continue
		}
		// Supported reconstruction may replace the recorded publishing commit.
		// The exact immutable post-image and pinned current HEAD prove availability.
		_, proven, err := git.ProvePublicationAtHEAD(ctx, input.RepoPath, "",
			head, publicationProofOps(ops), git.PublicationProofPolicy{MissingRefIsMismatch: true})
		if err != nil {
			return nil, err
		}
		if proven {
			for i := range captures {
				if path.Ext(captures[i].Event.Path) == ".ts" {
					// Available published imports resolve in this pinned baseline.
					// The original capture ledger and post-images stay unchanged.
					captures[i].Event.BaseHead = head
				}
				if references, matched := matchedTests[candidate.ID][captures[i].Event.Path]; matched {
					raw, err := BuildOpsDiffWithCap(ctx, input.RepoPath, captures[i].Ops, intentSourceReferenceScanCap)
					if err != nil {
						return nil, err
					}
					captures[i].CapturedDiff = prependIntentRecordedReferenceContext(intentCompleteRawDiff(raw), references)
				}
			}
			if input.publishedContext == nil {
				input.publishedContext = make(map[string][]IntentCandidateCapture)
			}
			input.publishedContext[candidate.ID] = captures
			if frozenIDs[candidate.ID] {
				if input.frozenPublishedContext == nil {
					input.frozenPublishedContext = make(map[int64]bool)
				}
				for _, capture := range captures {
					input.frozenPublishedContext[capture.Event.Seq] = true
				}
			}
			verified = append(verified, candidate)
			verifiedIDs[candidate.ID] = true
			known[candidate.ID] = true
		}
	}
	if len(verified) == 0 {
		return existing, nil
	}
	// Preserve pending groups first. Proven former companions precede optional
	// old soft-publication context so the bounded request cannot clip them away.
	result := append(active, verified...)
	for _, candidate := range published {
		if !verifiedIDs[candidate.ID] && len(result) < state.IntentCandidateMaxOpenPerPair {
			result = append(result, candidate)
		}
	}
	return result, nil
}

// Published context is a complete net change. Intermediate post-images are
// provenance, not competing versions that must all equal the current tree.
func normalizedPublishedIntentContext(ctx context.Context, db *state.DB, candidate state.IntentCandidate) (state.IntentCandidate, []IntentCandidateCapture, bool, error) {
	members := append([]state.IntentCandidateEvent(nil), candidate.Events...)
	sort.Slice(members, func(i, j int) bool { return members[i].EventSeq < members[j].EventSeq })
	var paths []string
	byPath := make(map[string][]state.CaptureEvent)
	roles := make(map[int64]string)
	opsBySeq := make(map[int64][]state.CaptureOp)
	lastOp := make(map[string]state.CaptureOp)
	for _, member := range members {
		event, err := loadIntentCaptureEvent(ctx, db, member.EventSeq)
		if err != nil {
			return candidate, nil, false, err
		}
		ops, err := state.LoadCaptureOps(ctx, db, member.EventSeq)
		if err != nil {
			return candidate, nil, false, err
		}
		capturePath, foldable := singleCoalescePath(event, ops)
		if !foldable || len(ops) != 1 || event.State != state.EventStatePublished ||
			event.CommitOID.String != candidate.PublishedCommitOID.String || event.BranchRef != candidate.BranchRef ||
			event.BranchGeneration != candidate.BranchGeneration {
			return candidate, nil, false, nil
		}
		if previous, found := lastOp[capturePath]; found {
			if ops[0].BeforeOID != previous.AfterOID || ops[0].BeforeMode != previous.AfterMode {
				return candidate, nil, false, nil
			}
		} else {
			paths = append(paths, capturePath)
		}
		lastOp[capturePath] = ops[0]
		byPath[capturePath] = append(byPath[capturePath], event)
		opsBySeq[event.Seq], roles[event.Seq] = ops, member.EventRole
	}
	candidate.Events = nil
	var captures []IntentCandidateCapture
	for _, capturePath := range paths {
		events := byPath[capturePath]
		merged := mergeSamePathOps(events, opsBySeq, capturePath)
		if len(merged) != 1 {
			return candidate, nil, false, nil
		}
		captures = append(captures, IntentCandidateCapture{Event: events[0], Ops: merged, CoveredEvents: append([]state.CaptureEvent(nil), events[1:]...)})
		for i, event := range events {
			role := roles[event.Seq]
			if i > 0 {
				role = "coalesced"
			}
			candidate.Events = append(candidate.Events, state.IntentCandidateEvent{
				CandidateID: candidate.ID, Ord: len(candidate.Events), EventSeq: event.Seq,
				EventRole: role, MembershipState: "active",
			})
		}
	}
	return candidate, captures, len(captures) > 0, nil
}

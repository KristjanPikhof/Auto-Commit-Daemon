package daemon

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	maxExternalRepairBridgeCommits = state.CompletedBranchTransitionProofLimit
	maxExternalRepairBridgePaths   = state.CompletedBranchTransitionProofLimit
	externalBridgeGitArgvByteCap   = 32 << 10
)

type externalRepairBridgeProof struct {
	TargetCommit string
	RepairID     string
	EventCount   int
}

type externalRepairCandidate struct {
	ID      string
	OldHead string
	NewHead string
	Ops     []state.CaptureOp
}

type externalRepairBridgeAncestry struct {
	commits   map[string]struct{}
	truncated bool
}

type externalRepairProofBudget struct {
	remaining int
}

func (budget *externalRepairProofBudget) consume(rows int, evidence string) error {
	if rows < 0 || rows > budget.remaining {
		return fmt.Errorf(
			"%w: external repair bridge: %s exceeds remaining proof budget %d",
			state.ErrCompletedBranchTransitionProof, evidence, budget.remaining)
	}
	budget.remaining -= rows
	return nil
}

type externalRepairEvidence struct {
	db               *state.DB
	repoRoot         string
	branchRef        string
	branchGeneration int64
	budget           externalRepairProofBudget
	repairs          map[string]state.IntentRepair
	publications     map[string]state.SelfPublication
	canonicalCommits map[string][]string
}

func newExternalRepairEvidence(
	db *state.DB,
	repoRoot string,
	branchRef string,
	branchGeneration int64,
) *externalRepairEvidence {
	return &externalRepairEvidence{
		db: db, repoRoot: repoRoot,
		branchRef: branchRef, branchGeneration: branchGeneration,
		budget: externalRepairProofBudget{
			remaining: state.CompletedBranchTransitionProofLimit,
		},
		repairs:          make(map[string]state.IntentRepair),
		publications:     make(map[string]state.SelfPublication),
		canonicalCommits: make(map[string][]string),
	}
}

func reconcileExternalRepairBridge(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	opts RecoveryReconcileOptions,
	live recoveryLiveState,
	chain []state.RecoveryChainEvent,
	bridge externalRepairBridgeProof,
) (RecoveryChainResult, error) {
	var result RecoveryChainResult
	if bridge.EventCount < 1 || bridge.EventCount > len(chain) ||
		live.head == "" || tokenBranchRef(live.token) != opts.BranchRef {
		return result, fmt.Errorf(
			"%w: external repair bridge adoption identity changed",
			state.ErrCompletedBranchTransitionProof)
	}
	if err := requireRecoveryInputsUnchanged(
		ctx, repoRoot, live, opts.ExpectedMissingRef,
	); err != nil {
		return result, err
	}
	prefix := chain[:bridge.EventCount]
	later := chain[bridge.EventCount:]
	first := prefix[0].Event
	last := prefix[len(prefix)-1].Event
	if len(later) > 0 {
		if err := validateRecoveryObjects(ctx, repoRoot, later); err != nil {
			if errors.Is(err, errInvalidRecoveryObjectEvidence) {
				return result, fmt.Errorf(
					"%w: external repair bridge: validate later capture objects: %v",
					state.ErrCompletedBranchTransitionProof, err)
			}
			return result, fmt.Errorf(
				"daemon: external repair bridge: validate later capture objects: %w", err)
		}
	}
	shadowRows, err := BuildShadowFromHeadPreservingUnpublished(
		ctx, repoRoot, CaptureContext{
			BranchRef: opts.BranchRef, BranchGeneration: opts.BranchGeneration,
			BaseHead: live.head,
		}, later)
	if err != nil {
		return result, err
	}
	ref := recoveryProofRefName(
		opts.BranchRef, opts.BranchGeneration,
		first.Seq, last.Seq, bridge.TargetCommit)
	if _, err := git.EnsureRecoveryRef(
		ctx, repoRoot, ref, bridge.TargetCommit); err != nil {
		return result, fmt.Errorf(
			"daemon: reconcile external repair bridge: protect exact target: %w", err)
	}
	commitOID := bridge.TargetCommit
	if opts.beforeFinalHeadCheck != nil {
		opts.beforeFinalHeadCheck()
	}
	if err := requireRecoveryInputsUnchanged(
		ctx, repoRoot, live, opts.ExpectedMissingRef,
	); err != nil {
		return result, err
	}
	if opts.beforeStateTransition != nil {
		opts.beforeStateTransition()
	}
	if err := requireRecoveryInputsUnchanged(
		ctx, repoRoot, live, opts.ExpectedMissingRef,
	); err != nil {
		return result, err
	}
	trigger := strings.TrimSpace(opts.Trigger)
	if trigger == "" {
		trigger = "automatic_chain_reconciliation"
	}
	transition := state.RecoveryChainTransition{
		Expected:              prefix,
		ExpectedLater:         later,
		TargetState:           state.EventStatePublished,
		CommitOID:             commitOID,
		RecoveryRef:           ref,
		Reason:                trigger,
		DecisionKind:          opts.decisionKind,
		AllowLaterUnpublished: len(later) > 0,
		AdoptedBranchHead:     live.head,
		ShadowRows:            shadowRows,
	}
	var snapshot state.RecoverySnapshot
	err = git.WithLockedRecoveryRefAndExpectedRef(
		ctx, repoRoot, ref, commitOID,
		opts.BranchRef, live.head, func(lockCtx context.Context) error {
			if opts.afterRecoveryRefLocked != nil {
				opts.afterRecoveryRefLocked()
			}
			var transitionErr error
			snapshot, transitionErr = state.TransitionRecoveryChain(
				lockCtx, db, transition)
			return transitionErr
		})
	if err != nil {
		return result, fmt.Errorf(
			"daemon: reconcile external repair bridge: protected transition: %w", err)
	}
	result = RecoveryChainResult{
		Handled:      true,
		Outcome:      snapshot.Outcome,
		SnapshotID:   snapshot.ID,
		RecoveryRef:  snapshot.RecoveryRef.String,
		CommitOID:    snapshot.CommitOID,
		FirstSeq:     snapshot.FirstEventSeq,
		LastSeq:      snapshot.LastEventSeq,
		EventCount:   snapshot.EventCount,
		AcceptedHead: live.head,
	}
	traceReplay(opts.Trace, repoRoot, CaptureContext{
		BranchRef: opts.BranchRef, BranchGeneration: opts.BranchGeneration,
		BaseHead: live.head,
	}, first, "replay.chain_reconcile", snapshot.Outcome, trigger, map[string]any{
		"snapshot_id":   snapshot.ID,
		"outcome":       snapshot.Outcome,
		"commit":        snapshot.CommitOID,
		"live_head":     live.head,
		"recovery_ref":  snapshot.RecoveryRef.String,
		"first_seq":     snapshot.FirstEventSeq,
		"last_seq":      snapshot.LastEventSeq,
		"event_count":   snapshot.EventCount,
		"repair_id":     bridge.RepairID,
		"external_base": opts.ExternalParentHead,
	})
	return result, nil
}

// proveExternalRepairBridge recognizes one narrow recovery shape: a persisted
// branch parent is followed by a commit that first restores a completed Intent
// repair and then contains the exact unpublished capture chains. Later commits
// may descend from that target, but they are not attributed to ACD or inspected
// as part of the publication proof.
func proveExternalRepairBridge(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	opts RecoveryReconcileOptions,
	live recoveryLiveState,
	chain []state.RecoveryChainEvent,
) (externalRepairBridgeProof, bool, error) {
	var zero externalRepairBridgeProof
	if opts.ExternalParentHead == "" || opts.ArchiveOnly || !live.hasHead ||
		tokenBranchRef(live.token) != opts.BranchRef {
		return zero, false, nil
	}
	frozenCount, frozen, err := externalBridgeMatchesFrozenDrain(
		ctx, db, opts, chain)
	if err != nil || !frozen {
		return zero, false, err
	}
	chain = chain[:frozenCount]
	if err := validateExternalRepairBridgeChain(chain); err != nil {
		return zero, false, err
	}
	target, ok, err := firstParentChildSince(
		ctx, repoRoot, opts.ExternalParentHead, live.head)
	if err != nil || !ok {
		return zero, false, err
	}
	ancestry, err := loadExternalRepairBridgeAncestry(
		ctx, repoRoot, target)
	if err != nil {
		return zero, false, err
	}
	evidence := newExternalRepairEvidence(
		db, repoRoot, opts.BranchRef, opts.BranchGeneration)
	chain, err = canonicalizeExternalRepairBridgeEvents(
		ctx, evidence, ancestry, chain)
	if err != nil {
		return zero, false, err
	}
	if err := validateRecoveryObjects(ctx, repoRoot, chain); err != nil {
		if errors.Is(err, errInvalidRecoveryObjectEvidence) {
			return zero, false, fmt.Errorf(
				"%w: external repair bridge: invalid frozen capture objects: %v",
				state.ErrCompletedBranchTransitionProof, err)
		}
		return zero, false, err
	}
	changedPaths, err := externalBridgeChangedPaths(
		ctx, repoRoot, opts.ExternalParentHead, target)
	if err != nil {
		return zero, false, err
	}
	if len(changedPaths) == 0 {
		return zero, false, nil
	}

	pendingPaths := make(map[string]struct{})
	for _, item := range chain {
		if len(item.Ops) == 0 {
			return zero, false, fmt.Errorf(
				"daemon: external repair bridge: event %d has no operations",
				item.Event.Seq)
		}
		if problem := validateOps(item.Ops); problem != "" {
			return zero, false, fmt.Errorf(
				"daemon: external repair bridge: invalid ops seq=%d: %s",
				item.Event.Seq, problem)
		}
		for _, path := range touchedPaths(item.Ops) {
			pendingPaths[path] = struct{}{}
		}
	}
	if len(pendingPaths) == 0 {
		return zero, false, nil
	}
	changedSet := make(map[string]struct{}, len(changedPaths))
	for _, path := range changedPaths {
		changedSet[path] = struct{}{}
	}
	repairs, foundRepairs, err := externalRepairCandidates(
		ctx, repoRoot, evidence, opts.ExternalParentHead)
	if err != nil {
		return zero, false, err
	}
	if !foundRepairs {
		return zero, false, nil
	}
	candidate := repairs[0]
	repairOps := make([]state.CaptureOp, 0)
	repairOwnedPaths := make(map[string]struct{})
	for _, repair := range repairs {
		if len(repairOps)+len(repair.Ops) > state.CompletedBranchTransitionProofLimit {
			return zero, false, fmt.Errorf(
				"%w: external repair bridge: repair chain exceeds %d operations",
				state.ErrCompletedBranchTransitionProof,
				state.CompletedBranchTransitionProofLimit)
		}
		if err := validateRecoveryObjects(ctx, repoRoot,
			[]state.RecoveryChainEvent{{
				Event: state.CaptureEvent{Seq: 1, BaseHead: repair.OldHead},
				Ops:   repair.Ops,
			}}); err != nil {
			if errors.Is(err, errInvalidRecoveryObjectEvidence) {
				return zero, false, fmt.Errorf(
					"%w: external repair bridge: repair %s has invalid captured objects: %v",
					state.ErrCompletedBranchTransitionProof, repair.ID, err)
			}
			return zero, false, err
		}
		validRepairTree, err := externalRepairCandidateTreeMatches(
			ctx, repoRoot, repair)
		if err != nil {
			return zero, false, err
		}
		if !validRepairTree {
			return zero, false, fmt.Errorf(
				"%w: external repair bridge: repair %s tree does not match its members",
				state.ErrCompletedBranchTransitionProof, repair.ID)
		}
		repairOps = append(repairOps, repair.Ops...)
		for _, path := range touchedPaths(repair.Ops) {
			repairOwnedPaths[path] = struct{}{}
		}
	}
	repairOnTarget, err := ancestry.contains(candidate.NewHead)
	if err != nil {
		return zero, false, fmt.Errorf(
			"daemon: external repair bridge: prove repair %s ancestry: %w",
			candidate.ID, err)
	}
	if !repairOnTarget {
		return zero, false, nil
	}
	publicationSuffix, ownedSuffix, err := loadExternalRepairPublicationSuffix(
		ctx, repoRoot, evidence,
		candidate.NewHead, opts.ExternalParentHead)
	if err != nil {
		return zero, false, err
	}
	if !ownedSuffix {
		return zero, false, nil
	}
	if err := validateRecoveryObjects(ctx, repoRoot, publicationSuffix); err != nil {
		if errors.Is(err, errInvalidRecoveryObjectEvidence) {
			return zero, false, fmt.Errorf(
				"%w: external repair bridge: publication suffix has invalid captured objects: %v",
				state.ErrCompletedBranchTransitionProof, err)
		}
		return zero, false, err
	}
	baselineChanges, err := externalBridgeChangedPaths(
		ctx, repoRoot, candidate.NewHead, opts.ExternalParentHead)
	if err != nil {
		return zero, false, err
	}
	proofSet := make(map[string]struct{}, len(changedSet))
	for path := range changedSet {
		proofSet[path] = struct{}{}
	}
	proofInputs := [][]string{
		baselineChanges,
		touchedPaths(repairOps),
		touchedPathsFromRecoveryChain(publicationSuffix),
		touchedPathsFromRecoveryChain(chain),
	}
	for _, paths := range proofInputs {
		for _, path := range paths {
			proofSet[path] = struct{}{}
			if len(proofSet) > maxExternalRepairBridgePaths {
				return zero, false, fmt.Errorf(
					"%w: external repair bridge: repair proof exceeds %d paths",
					state.ErrCompletedBranchTransitionProof,
					maxExternalRepairBridgePaths)
			}
		}
	}
	proofPaths := make([]string, 0, len(proofSet))
	for path := range proofSet {
		proofPaths = append(proofPaths, path)
	}
	sort.Strings(proofPaths)
	actualSource, err := externalBridgeIndexState(
		ctx, repoRoot, opts.ExternalParentHead, proofPaths)
	if err != nil {
		return zero, false, err
	}
	targetIndex, err := externalBridgeIndexState(
		ctx, repoRoot, target, proofPaths)
	if err != nil {
		return zero, false, err
	}
	index, err := externalBridgeIndexState(
		ctx, repoRoot, candidate.NewHead, proofPaths)
	if err != nil {
		return zero, false, err
	}
	valid := true
	for _, item := range publicationSuffix {
		if conflict := applyRecoveryOpsInMemory(index, item.Ops); conflict != "" {
			valid = false
			break
		}
	}
	if !valid {
		return zero, false, nil
	}
	restoredRepairPath := false
	for _, path := range proofPaths {
		if sameExternalBridgeEntry(index, actualSource, path) {
			continue
		}
		if _, owned := repairOwnedPaths[path]; !owned {
			return zero, false, nil
		}
		restoredRepairPath = true
	}
	if !restoredRepairPath {
		return zero, false, nil
	}
	lastUnpublishedBase := make(map[string]string)
	for _, item := range chain {
		conflict, err := applyRecoveryEventInMemory(
			ctx, repoRoot, index, item, true, lastUnpublishedBase)
		if err != nil {
			return zero, false, fmt.Errorf(
				"daemon: external repair bridge: prove event %d base state: %w",
				item.Event.Seq, err)
		}
		if conflict != "" {
			valid = false
			break
		}
	}
	if !valid || !externalBridgeIndexesMatch(index, targetIndex, proofPaths) {
		return zero, false, nil
	}
	finalState := externalBridgeFinalState(index, proofSet)
	chainOnTarget, err := externalRepairBridgeBasesReachTarget(ancestry, chain)
	if err != nil || !chainOnTarget {
		return zero, false, err
	}
	chainMatches, err := recoveryStatesMatchTree(
		ctx, repoRoot, target, finalState)
	if err != nil {
		return zero, false, err
	}
	if !chainMatches {
		return zero, false, nil
	}
	return externalRepairBridgeProof{
		TargetCommit: target,
		RepairID:     candidate.ID,
		EventCount:   len(chain),
	}, true, nil
}

func validateExternalRepairBridgeChain(chain []state.RecoveryChainEvent) error {
	if len(chain) == 0 {
		return fmt.Errorf(
			"%w: external repair bridge: frozen drain has no unresolved events",
			state.ErrCompletedBranchTransitionProof)
	}
	if len(chain) > state.CompletedBranchTransitionProofLimit {
		return fmt.Errorf(
			"%w: external repair bridge: frozen drain exceeds %d events",
			state.ErrCompletedBranchTransitionProof,
			state.CompletedBranchTransitionProofLimit)
	}
	opCount := 0
	for _, item := range chain {
		opCount += len(item.Ops)
		if opCount > state.CompletedBranchTransitionProofLimit {
			return fmt.Errorf(
				"%w: external repair bridge: frozen drain exceeds %d operations",
				state.ErrCompletedBranchTransitionProof,
				state.CompletedBranchTransitionProofLimit)
		}
	}
	return nil
}

func loadExternalRepairPublicationSuffix(
	ctx context.Context,
	repoRoot string,
	evidence *externalRepairEvidence,
	repairHead string,
	externalParent string,
) ([]state.RecoveryChainEvent, bool, error) {
	if repairHead == externalParent {
		return nil, false, nil
	}
	current := repairHead
	seen := map[string]struct{}{current: {}}
	events := make([]state.RecoveryChainEvent, 0)
	for current != externalParent {
		transitions, err := evidence.outgoingTransitions(ctx, current)
		if err != nil {
			return nil, false, err
		}
		switch len(transitions) {
		case 0:
			return nil, false, nil
		case 1:
			// Continue below.
		default:
			return nil, false, fmt.Errorf(
				"%w: external repair bridge: publication transition from %s is ambiguous",
				state.ErrCompletedBranchTransitionProof, current)
		}
		transition := transitions[0]
		if transition.Kind != state.CompletedBranchTransitionSelfPublication {
			return nil, false, nil
		}
		if transition.TargetHead == "" || transition.TargetHead == current {
			return nil, false, fmt.Errorf(
				"%w: external repair bridge: publication %s has invalid target",
				state.ErrCompletedBranchTransitionProof, transition.ID)
		}
		if _, duplicate := seen[transition.TargetHead]; duplicate {
			return nil, false, fmt.Errorf(
				"%w: external repair bridge: publication transition cycle at %s",
				state.ErrCompletedBranchTransitionProof, transition.TargetHead)
		}
		publicationEvents, err := loadExternalRepairPublication(
			ctx, repoRoot, evidence, current, transition)
		if err != nil {
			return nil, false, err
		}
		events = append(events, publicationEvents...)
		seen[transition.TargetHead] = struct{}{}
		current = transition.TargetHead
	}
	return events, len(events) > 0, nil
}

func loadExternalRepairPublication(
	ctx context.Context,
	repoRoot string,
	evidence *externalRepairEvidence,
	sourceHead string,
	transition externalRepairOutgoingTransition,
) ([]state.RecoveryChainEvent, error) {
	parents, err := selfPublicationParents(ctx, repoRoot, transition.TargetHead)
	if err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: read publication %s parents: %w",
			transition.ID, err)
	}
	if len(parents) != 1 || parents[0] != sourceHead {
		return nil, fmt.Errorf(
			"%w: external repair bridge: publication %s does not map the Git edge %s..%s",
			state.ErrCompletedBranchTransitionProof, transition.ID,
			sourceHead, transition.TargetHead)
	}

	publication, ok, err := evidence.loadPublication(ctx, transition.ID)
	if err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: load publication %s: %w",
			transition.ID, err)
	}
	membershipDigest, digestErr := state.SelfPublicationMembershipDigest(
		publication.Members)
	if !ok || publication.Phase != state.SelfPublicationCompleted ||
		!publication.CompletedTS.Valid || publication.CompletedTS.Float64 <= 0 ||
		publication.BranchRef != evidence.branchRef ||
		publication.BranchGeneration != evidence.branchGeneration ||
		publication.SourceHead != sourceHead ||
		publication.TargetCommitOID != transition.TargetHead ||
		publication.MemberCount != len(publication.Members) ||
		digestErr != nil || membershipDigest != publication.MembershipDigest {
		return nil, fmt.Errorf(
			"%w: external repair bridge: publication %s identity changed",
			state.ErrCompletedBranchTransitionProof, transition.ID)
	}
	treeOID, err := resolveTreeOID(ctx, repoRoot, transition.TargetHead)
	if err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: resolve publication %s tree: %w",
			transition.ID, err)
	}
	if treeOID != publication.TargetTreeOID {
		return nil, fmt.Errorf(
			"%w: external repair bridge: publication %s tree proof changed",
			state.ErrCompletedBranchTransitionProof, transition.ID)
	}

	window := make([]state.CaptureEvent, len(publication.Members))
	opsBySeq := make(map[int64][]state.CaptureOp, len(publication.Members))
	for i, member := range publication.Members {
		if member.Ord != i || member.EventSeq < 1 ||
			(member.CandidateID.Valid && strings.TrimSpace(member.CandidateID.String) == "") {
			return nil, fmt.Errorf(
				"%w: external repair bridge: publication %s membership changed",
				state.ErrCompletedBranchTransitionProof, transition.ID)
		}
		if err := evidence.budget.consume(
			1, "self-publication member events"); err != nil {
			return nil, err
		}
		event, err := loadIntentCaptureEvent(
			ctx, evidence.db, member.EventSeq)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf(
					"%w: external repair bridge: publication %s member %d is missing",
					state.ErrCompletedBranchTransitionProof,
					transition.ID, member.EventSeq)
			}
			return nil, fmt.Errorf(
				"daemon: external repair bridge: load publication member %d: %w",
				member.EventSeq, err)
		}
		if event.State != state.EventStatePublished ||
			!event.CommitOID.Valid ||
			event.CommitOID.String != transition.TargetHead {
			return nil, fmt.Errorf(
				"%w: external repair bridge: publication %s member %d drifted",
				state.ErrCompletedBranchTransitionProof,
				transition.ID, member.EventSeq)
		}
		ops, err := state.LoadCaptureOpsBounded(
			ctx, evidence.db, member.EventSeq, evidence.budget.remaining)
		if err != nil {
			return nil, fmt.Errorf(
				"daemon: external repair bridge: load publication member ops %d: %w",
				member.EventSeq, err)
		}
		if err := evidence.budget.consume(
			len(ops), "self-publication member operations"); err != nil {
			return nil, err
		}
		if len(ops) == 0 || validateOps(ops) != "" {
			return nil, fmt.Errorf(
				"%w: external repair bridge: publication %s member %d has invalid operations",
				state.ErrCompletedBranchTransitionProof,
				transition.ID, member.EventSeq)
		}
		window[i] = event
		opsBySeq[event.Seq] = ops
	}

	loadOps := func(
		_ context.Context, _ *state.DB, seq int64,
	) ([]state.CaptureOp, error) {
		ops, ok := opsBySeq[seq]
		if !ok {
			return nil, fmt.Errorf("publication member %d is not sealed", seq)
		}
		return ops, nil
	}
	applied := make([]state.RecoveryChainEvent, 0, len(window))
	for start := 0; start < len(window); {
		end := start + 1
		candidateID := publication.Members[start].CandidateID
		if candidateID.Valid {
			for end < len(window) &&
				publication.Members[end].CandidateID.Valid &&
				publication.Members[end].CandidateID.String == candidateID.String {
				end++
			}
		} else {
			for end < len(window) &&
				!publication.Members[end].CandidateID.Valid {
				end++
			}
		}
		offers, err := coalesceIntentWindow(
			ctx, evidence.db, window[start:end], true, loadOps)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: external repair bridge: publication %s cannot reconstruct applied operations: %v",
				state.ErrCompletedBranchTransitionProof, transition.ID, err)
		}
		for _, offer := range offers {
			if len(offer.MergedOps) == 0 || validateOps(offer.MergedOps) != "" {
				return nil, fmt.Errorf(
					"%w: external repair bridge: publication %s reconstructed invalid operations",
					state.ErrCompletedBranchTransitionProof, transition.ID)
			}
			applied = append(applied, state.RecoveryChainEvent{
				Event: offer.Primary,
				Ops:   offer.MergedOps,
			})
		}
		start = end
	}
	return applied, nil
}

func touchedPathsFromRecoveryChain(chain []state.RecoveryChainEvent) []string {
	paths := make([]string, 0)
	for _, item := range chain {
		paths = append(paths, touchedPaths(item.Ops)...)
	}
	return paths
}

func externalBridgeMatchesFrozenDrain(
	ctx context.Context,
	db *state.DB,
	opts RecoveryReconcileOptions,
	chain []state.RecoveryChainEvent,
) (int, bool, error) {
	drains, err := state.ActivePublicationDrainsForPair(
		ctx, db, opts.BranchRef, opts.BranchGeneration)
	if err != nil {
		return 0, false, fmt.Errorf(
			"daemon: external repair bridge: load active publication drains: %w", err)
	}
	if len(drains) != 1 {
		return 0, false, nil
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT event.seq
FROM publication_drain_events member
JOIN capture_events event ON event.seq=member.event_seq
WHERE member.drain_id=?
  AND event.state IN (?, ?, ?)
ORDER BY member.ord
LIMIT ?`, drains[0].ID,
		state.EventStatePending,
		state.EventStateBlockedConflict,
		state.EventStateFailed,
		state.CompletedBranchTransitionProofLimit+1)
	if err != nil {
		return 0, false, fmt.Errorf(
			"daemon: external repair bridge: load unresolved drain target: %w", err)
	}
	defer rows.Close()
	var unresolved []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return 0, false, fmt.Errorf(
				"daemon: external repair bridge: scan unresolved drain target: %w", err)
		}
		unresolved = append(unresolved, seq)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf(
			"daemon: external repair bridge: iterate unresolved drain target: %w", err)
	}
	if len(unresolved) > state.CompletedBranchTransitionProofLimit {
		return 0, false, fmt.Errorf(
			"%w: external repair bridge: frozen drain exceeds %d events",
			state.ErrCompletedBranchTransitionProof,
			state.CompletedBranchTransitionProofLimit)
	}
	if len(unresolved) == 0 || len(chain) < len(unresolved) {
		return 0, false, nil
	}
	for i, seq := range unresolved {
		item := chain[i]
		if seq != item.Event.Seq {
			return 0, false, nil
		}
	}
	return len(unresolved), true, nil
}

func loadExternalRepairBridgeAncestry(
	ctx context.Context,
	repoRoot string,
	target string,
) (externalRepairBridgeAncestry, error) {
	maxCount := state.CompletedBranchTransitionProofLimit + 1
	maxOutput := int64(maxCount * (64 + 1))
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, maxOutput, "rev-list", fmt.Sprintf("--max-count=%d", maxCount),
		target, "--")
	if err != nil {
		if errors.Is(err, git.ErrStdoutOverflow) {
			return externalRepairBridgeAncestry{}, fmt.Errorf(
				"%w: external repair bridge: target ancestry output exceeded its bound",
				state.ErrCompletedBranchTransitionProof)
		}
		return externalRepairBridgeAncestry{}, fmt.Errorf(
			"daemon: external repair bridge: list bounded target ancestry: %w", err)
	}
	commits := strings.Fields(string(out))
	truncated := len(commits) > state.CompletedBranchTransitionProofLimit
	if truncated {
		commits = commits[:state.CompletedBranchTransitionProofLimit]
	}
	proof := externalRepairBridgeAncestry{
		commits:   make(map[string]struct{}, len(commits)),
		truncated: truncated,
	}
	for _, commit := range commits {
		proof.commits[commit] = struct{}{}
	}
	if _, ok := proof.commits[target]; !ok {
		return externalRepairBridgeAncestry{}, fmt.Errorf(
			"%w: external repair bridge: target ancestry omitted %s",
			state.ErrCompletedBranchTransitionProof, target)
	}
	return proof, nil
}

func (proof externalRepairBridgeAncestry) contains(commit string) (bool, error) {
	if _, ok := proof.commits[commit]; ok {
		return true, nil
	}
	if proof.truncated {
		return false, fmt.Errorf(
			"%w: external repair bridge: target ancestry exceeds %d commits before %s",
			state.ErrCompletedBranchTransitionProof,
			state.CompletedBranchTransitionProofLimit, commit)
	}
	return false, nil
}

func (evidence *externalRepairEvidence) loadPublication(
	ctx context.Context,
	publicationID string,
) (state.SelfPublication, bool, error) {
	if publication, ok := evidence.publications[publicationID]; ok {
		return publication, true, nil
	}
	publication, ok, rows, err := state.SelfPublicationByIDBounded(
		ctx, evidence.db, publicationID, evidence.budget.remaining)
	if err != nil {
		return state.SelfPublication{}, false, fmt.Errorf(
			"daemon: external repair bridge: load bounded publication %s: %w",
			publicationID, err)
	}
	if err := evidence.budget.consume(
		rows, "self-publication members"); err != nil {
		return state.SelfPublication{}, false, err
	}
	if ok {
		evidence.publications[publicationID] = publication
	}
	return publication, ok, nil
}

func (evidence *externalRepairEvidence) loadRepair(
	ctx context.Context,
	repairID string,
) (state.IntentRepair, error) {
	if repair, ok := evidence.repairs[repairID]; ok {
		return repair, nil
	}
	repair, ok, rows, err := state.IntentRepairByIDBounded(
		ctx, evidence.db, repairID, evidence.budget.remaining)
	if err != nil {
		return state.IntentRepair{}, fmt.Errorf(
			"daemon: external repair bridge: load bounded repair %s: %w",
			repairID, err)
	}
	if err := evidence.budget.consume(rows, "intent repair evidence"); err != nil {
		return state.IntentRepair{}, err
	}
	if !ok || repair.Status != state.IntentRepairCompleted ||
		repair.BranchRef != evidence.branchRef ||
		repair.BranchGeneration != evidence.branchGeneration ||
		!repair.OldHead.Valid || repair.OldHead.String == "" ||
		!repair.NewHead.Valid || repair.NewHead.String == "" ||
		repair.ExpectedHead != repair.OldHead.String ||
		!repair.BackupRef.Valid || repair.BackupRef.String == "" ||
		!repair.CompletedTS.Valid ||
		len(repair.Commits) == 0 ||
		len(repair.Commits) > state.IntentRepairMaxCommits {
		return state.IntentRepair{}, fmt.Errorf(
			"%w: external repair bridge: repair %s identity changed",
			state.ErrCompletedBranchTransitionProof, repairID)
	}
	commitCandidates := make(map[string]struct{}, len(repair.Commits))
	for ord, commit := range repair.Commits {
		if commit.RepairID != repair.ID || commit.Ord != ord ||
			!commit.CandidateID.Valid || commit.CandidateID.String == "" ||
			commit.OldOID == "" || !commit.NewOID.Valid ||
			commit.NewOID.String == "" {
			return state.IntentRepair{}, fmt.Errorf(
				"%w: external repair bridge: repair %s has invalid commit mapping %d",
				state.ErrCompletedBranchTransitionProof, repairID, ord)
		}
		commitCandidates[commit.CandidateID.String] = struct{}{}
	}
	switch repair.MembershipMode {
	case state.IntentRepairMembershipLegacy:
		if len(repair.Members) != 0 {
			return state.IntentRepair{}, fmt.Errorf(
				"%w: external repair bridge: legacy repair %s has members",
				state.ErrCompletedBranchTransitionProof, repairID)
		}
	case state.IntentRepairMembershipFrozen:
		if len(repair.Members) == 0 ||
			len(repair.Members) > state.IntentRepairMaxMembers {
			return state.IntentRepair{}, fmt.Errorf(
				"%w: external repair bridge: repair %s has invalid membership size",
				state.ErrCompletedBranchTransitionProof, repairID)
		}
		seenEvents := make(map[int64]struct{}, len(repair.Members))
		for ord, member := range repair.Members {
			_, candidateExists := commitCandidates[member.CandidateID]
			_, duplicate := seenEvents[member.EventSeq]
			if member.RepairID != repair.ID || member.Ord != ord ||
				member.EventSeq < 1 || member.CandidateID == "" ||
				!candidateExists || duplicate ||
				(member.PriorState != state.EventStatePending &&
					member.PriorState != state.EventStatePublished) {
				return state.IntentRepair{}, fmt.Errorf(
					"%w: external repair bridge: repair %s has invalid member %d",
					state.ErrCompletedBranchTransitionProof, repairID, ord)
			}
			seenEvents[member.EventSeq] = struct{}{}
		}
	default:
		return state.IntentRepair{}, fmt.Errorf(
			"%w: external repair bridge: repair %s has invalid membership mode %q",
			state.ErrCompletedBranchTransitionProof,
			repairID, repair.MembershipMode)
	}
	lastMapping := repair.Commits[len(repair.Commits)-1]
	if !lastMapping.NewOID.Valid ||
		lastMapping.NewOID.String != repair.NewHead.String ||
		strings.TrimSpace(repair.PlanDigest) == "" {
		return state.IntentRepair{}, fmt.Errorf(
			"%w: external repair bridge: repair %s lacks a sealed final head",
			state.ErrCompletedBranchTransitionProof, repairID)
	}
	planDigest, err := recoveredIntentRepairPlanDigest(
		ctx, evidence.repoRoot, repair, repair.Commits)
	if err != nil {
		if errors.Is(err, ErrIntentRepairRecoveryProof) {
			return state.IntentRepair{}, fmt.Errorf(
				"%w: external repair bridge: repair %s plan proof failed: %v",
				state.ErrCompletedBranchTransitionProof, repairID, err)
		}
		return state.IntentRepair{}, fmt.Errorf(
			"daemon: external repair bridge: rebuild repair %s plan digest: %w",
			repairID, err)
	}
	if planDigest != repair.PlanDigest {
		return state.IntentRepair{}, fmt.Errorf(
			"%w: external repair bridge: repair %s plan digest changed",
			state.ErrCompletedBranchTransitionProof, repairID)
	}
	evidence.repairs[repairID] = repair
	return repair, nil
}

type externalRepairCommitMapping struct {
	repairID string
	newOID   string
}

func (evidence *externalRepairEvidence) commitMappingsFrom(
	ctx context.Context,
	oldOID string,
) ([]externalRepairCommitMapping, error) {
	rows, err := evidence.db.ReadSQL().QueryContext(ctx, `
SELECT r.id,c.new_oid
FROM intent_repair_commits c
JOIN intent_repairs r ON r.id=c.repair_id
WHERE r.branch_ref=? AND r.branch_generation=?
  AND r.status='completed' AND c.old_oid=?
  AND c.new_oid IS NOT NULL AND c.new_oid<>''
ORDER BY r.created_ts,r.id
LIMIT 2`, evidence.branchRef, evidence.branchGeneration, oldOID)
	if err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: load completed repair mapping: %w", err)
	}
	defer rows.Close()
	var mappings []externalRepairCommitMapping
	for rows.Next() {
		var mapping externalRepairCommitMapping
		if err := rows.Scan(&mapping.repairID, &mapping.newOID); err != nil {
			return nil, fmt.Errorf(
				"daemon: external repair bridge: scan completed repair mapping: %w", err)
		}
		mappings = append(mappings, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: iterate completed repair mapping: %w", err)
	}
	if err := evidence.budget.consume(
		len(mappings), "completed repair mappings"); err != nil {
		return nil, err
	}
	return mappings, nil
}

func (evidence *externalRepairEvidence) canonicalCommitChain(
	ctx context.Context,
	commitOID string,
) ([]string, error) {
	if cached, ok := evidence.canonicalCommits[commitOID]; ok {
		return append([]string(nil), cached...), nil
	}
	current := commitOID
	seen := map[string]struct{}{current: {}}
	chain := []string{current}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if cached, ok := evidence.canonicalCommits[current]; ok {
			chain = append(chain, cached[1:]...)
			break
		}
		mappings, err := evidence.commitMappingsFrom(ctx, current)
		if err != nil {
			return nil, err
		}
		switch len(mappings) {
		case 0:
			// The validated chain ends here.
		case 1:
			mapping := mappings[0]
			repair, err := evidence.loadRepair(ctx, mapping.repairID)
			if err != nil {
				return nil, err
			}
			mappingOwned := false
			for _, commit := range repair.Commits {
				if commit.OldOID == current && commit.NewOID.Valid &&
					commit.NewOID.String == mapping.newOID {
					mappingOwned = true
					break
				}
			}
			if !mappingOwned {
				return nil, fmt.Errorf(
					"%w: external repair bridge: repair %s does not own mapping %s..%s",
					state.ErrCompletedBranchTransitionProof,
					repair.ID, current, mapping.newOID)
			}
			if _, duplicate := seen[mapping.newOID]; duplicate {
				return nil, fmt.Errorf(
					"%w: external repair bridge: repair mapping cycle at %s",
					state.ErrCompletedBranchTransitionProof, mapping.newOID)
			}
			seen[mapping.newOID] = struct{}{}
			current = mapping.newOID
			chain = append(chain, current)
			continue
		default:
			return nil, fmt.Errorf(
				"%w: external repair bridge: repair mapping from %s is ambiguous",
				state.ErrCompletedBranchTransitionProof, current)
		}
		break
	}
	for i, representative := range chain {
		evidence.canonicalCommits[representative] =
			append([]string(nil), chain[i:]...)
	}
	return chain, nil
}

func canonicalizeExternalRepairBridgeEvents(
	ctx context.Context,
	evidence *externalRepairEvidence,
	ancestry externalRepairBridgeAncestry,
	events []state.RecoveryChainEvent,
) ([]state.RecoveryChainEvent, error) {
	canonicalByOID := make(map[string]string)
	canonicalize := func(oid string) (string, error) {
		if canonical, ok := canonicalByOID[oid]; ok {
			return canonical, nil
		}
		canonical, err := canonicalExternalRepairBridgeCommit(
			ctx, evidence, ancestry, oid)
		if err != nil {
			return "", err
		}
		canonicalByOID[oid] = canonical
		return canonical, nil
	}

	proofEvents := make([]state.RecoveryChainEvent, len(events))
	copy(proofEvents, events)
	for i := range proofEvents {
		baseHead, err := canonicalize(proofEvents[i].Event.BaseHead)
		if err != nil {
			return nil, err
		}
		proofEvents[i].Event.BaseHead = baseHead
		if !proofEvents[i].Event.CommitOID.Valid ||
			proofEvents[i].Event.CommitOID.String == "" {
			continue
		}
		commitOID, err := canonicalize(proofEvents[i].Event.CommitOID.String)
		if err != nil {
			return nil, err
		}
		proofEvents[i].Event.CommitOID.String = commitOID
	}
	return proofEvents, nil
}

func canonicalExternalRepairBridgeCommit(
	ctx context.Context,
	evidence *externalRepairEvidence,
	ancestry externalRepairBridgeAncestry,
	commitOID string,
) (string, error) {
	representatives, err := evidence.canonicalCommitChain(ctx, commitOID)
	if err != nil {
		return "", fmt.Errorf(
			"daemon: external repair bridge: resolve repaired commit %s: %w",
			commitOID, err)
	}
	if len(representatives) == 1 {
		return commitOID, nil
	}
	for i := len(representatives) - 1; i >= 0; i-- {
		representative := representatives[i]
		reachable, err := ancestry.contains(representative)
		if err != nil {
			return "", fmt.Errorf(
				"daemon: external repair bridge: prove repaired commit representative %s: %w",
				representative, err)
		}
		if reachable {
			return representative, nil
		}
	}
	return representatives[len(representatives)-1], nil
}

func externalRepairBridgeBasesReachTarget(
	ancestry externalRepairBridgeAncestry,
	chain []state.RecoveryChainEvent,
) (bool, error) {
	seen := make(map[string]struct{})
	for _, item := range chain {
		base := item.Event.BaseHead
		if _, duplicate := seen[base]; duplicate {
			continue
		}
		reachable, err := ancestry.contains(base)
		if err != nil {
			return false, fmt.Errorf(
				"daemon: external repair bridge: prove event %d base ancestry: %w",
				item.Event.Seq, err)
		}
		if !reachable {
			return false, nil
		}
		seen[base] = struct{}{}
	}
	return true, nil
}

func firstParentChildSince(
	ctx context.Context,
	repoRoot string,
	parent string,
	liveHead string,
) (string, bool, error) {
	if parent == liveHead {
		return "", false, nil
	}
	maxCount := maxExternalRepairBridgeCommits + 1
	maxOutput := int64(maxCount * (64 + 1))
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, maxOutput, "rev-list", "--first-parent",
		fmt.Sprintf("--max-count=%d", maxCount), liveHead)
	if err != nil {
		if errors.Is(err, git.ErrStdoutOverflow) {
			return "", false, fmt.Errorf(
				"%w: external repair bridge: first-parent proof output exceeded its bound",
				state.ErrCompletedBranchTransitionProof)
		}
		return "", false, fmt.Errorf(
			"daemon: external repair bridge: list bounded first-parent chain: %w", err)
	}
	commits := strings.Fields(string(out))
	parentIndex := -1
	for i, commit := range commits {
		if commit == parent {
			parentIndex = i
			break
		}
	}
	if parentIndex < 0 && len(commits) == maxCount {
		return "", false, fmt.Errorf(
			"%w: external repair bridge: first-parent proof exceeds %d commits",
			state.ErrCompletedBranchTransitionProof,
			maxExternalRepairBridgeCommits)
	}
	if parentIndex <= 0 || parentIndex > maxExternalRepairBridgeCommits {
		return "", false, nil
	}
	target := commits[parentIndex-1]
	parents, err := selfPublicationParents(ctx, repoRoot, target)
	if err != nil {
		return "", false, fmt.Errorf(
			"daemon: external repair bridge: read target parents: %w", err)
	}
	if len(parents) != 1 || parents[0] != parent {
		return "", false, nil
	}
	return target, true, nil
}

func externalBridgeChangedPaths(
	ctx context.Context,
	repoRoot string,
	parent string,
	target string,
) ([]string, error) {
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, git.DefaultDiffCap, "diff-tree", "--no-commit-id", "--name-only",
		"-r", "-z", "--no-renames", parent, target, "--")
	if err != nil {
		if errors.Is(err, git.ErrStdoutOverflow) {
			return nil, fmt.Errorf(
				"%w: external repair bridge: changed-path proof exceeds %d bytes",
				state.ErrCompletedBranchTransitionProof, git.DefaultDiffCap)
		}
		return nil, fmt.Errorf(
			"daemon: external repair bridge: diff parent and target: %w", err)
	}
	seen := make(map[string]struct{})
	paths := make([]string, 0)
	for _, raw := range bytes.Split(out, []byte{0}) {
		if len(raw) == 0 {
			continue
		}
		path := string(raw)
		if _, duplicate := seen[path]; duplicate {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
		if len(paths) > maxExternalRepairBridgePaths {
			return nil, fmt.Errorf(
				"%w: external repair bridge: target changes more than %d paths",
				state.ErrCompletedBranchTransitionProof,
				maxExternalRepairBridgePaths)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func externalBridgeIndexState(
	ctx context.Context,
	repoRoot string,
	rev string,
	paths []string,
) (map[string]git.IndexEntry, error) {
	chunks, err := externalBridgePathspecChunks(rev, paths)
	if err != nil {
		return nil, err
	}
	index := make(map[string]git.IndexEntry, len(paths))
	remainingOutput := int64(git.DefaultDiffCap)
	for _, chunk := range chunks {
		readLimit := remainingOutput
		if readLimit == 0 {
			// A zero RunWithLimit cap disables the bound. One byte still lets an
			// empty chunk succeed while any tree record deterministically overflows.
			readLimit = 1
		}
		entries, err := git.LsTreeLimited(
			ctx, repoRoot, rev, false, readLimit, chunk...)
		if err != nil {
			if errors.Is(err, git.ErrStdoutOverflow) {
				return nil, fmt.Errorf(
					"%w: external repair bridge: tree proof exceeds %d bytes",
					state.ErrCompletedBranchTransitionProof, git.DefaultDiffCap)
			}
			return nil, fmt.Errorf(
				"daemon: external repair bridge: read tree %s: %w", rev, err)
		}
		requested := make(map[string]struct{}, len(chunk))
		for _, spec := range chunk {
			requested[strings.TrimPrefix(spec, ":(literal)")] = struct{}{}
		}
		for _, entry := range entries {
			if _, ok := requested[entry.Path]; !ok {
				return nil, fmt.Errorf(
					"%w: external repair bridge: tree proof returned unrequested path %q",
					state.ErrCompletedBranchTransitionProof, entry.Path)
			}
			if _, duplicate := index[entry.Path]; duplicate {
				return nil, fmt.Errorf(
					"%w: external repair bridge: tree proof returned duplicate path %q",
					state.ErrCompletedBranchTransitionProof, entry.Path)
			}
			recordBytes := int64(len(entry.Mode) + len(entry.Type) +
				len(entry.OID) + len(entry.Path) + 4)
			if recordBytes > remainingOutput {
				return nil, fmt.Errorf(
					"%w: external repair bridge: tree proof exceeds %d bytes",
					state.ErrCompletedBranchTransitionProof, git.DefaultDiffCap)
			}
			remainingOutput -= recordBytes
			index[entry.Path] = git.IndexEntry{
				Mode: entry.Mode, OID: entry.OID, Path: entry.Path,
			}
		}
	}
	return index, nil
}

func externalBridgePathspecChunks(
	rev string,
	paths []string,
) ([][]string, error) {
	if len(paths) > maxExternalRepairBridgePaths {
		return nil, fmt.Errorf(
			"%w: external repair bridge: tree proof exceeds %d paths",
			state.ErrCompletedBranchTransitionProof,
			maxExternalRepairBridgePaths)
	}
	if len(paths) == 0 {
		return nil, nil
	}
	// Keep the explicitly controlled argv far below macOS ARG_MAX. Count the
	// executable and each terminating NUL as well as Git's fixed arguments.
	baseBytes := argvBytes("git", "ls-tree", "-z", rev, "--")
	if baseBytes >= externalBridgeGitArgvByteCap {
		return nil, fmt.Errorf(
			"%w: external repair bridge: tree revision exceeds the Git argument bound",
			state.ErrCompletedBranchTransitionProof)
	}
	seen := make(map[string]struct{}, len(paths))
	chunks := make([][]string, 0, 1)
	chunk := make([]string, 0, len(paths))
	chunkBytes := baseBytes
	for _, path := range paths {
		spec := git.LiteralPathspec(path)
		if spec == "" {
			return nil, fmt.Errorf(
				"%w: external repair bridge: tree proof has an empty path",
				state.ErrCompletedBranchTransitionProof)
		}
		if _, duplicate := seen[spec]; duplicate {
			return nil, fmt.Errorf(
				"%w: external repair bridge: tree proof has duplicate path %q",
				state.ErrCompletedBranchTransitionProof, path)
		}
		seen[spec] = struct{}{}
		specBytes := len(spec) + 1
		if baseBytes+specBytes > externalBridgeGitArgvByteCap {
			return nil, fmt.Errorf(
				"%w: external repair bridge: tree proof path %q exceeds the Git argument bound",
				state.ErrCompletedBranchTransitionProof, path)
		}
		if chunkBytes+specBytes > externalBridgeGitArgvByteCap {
			chunks = append(chunks, chunk)
			chunk = make([]string, 0, len(paths)-len(seen)+1)
			chunkBytes = baseBytes
		}
		chunk = append(chunk, spec)
		chunkBytes += specBytes
	}
	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func argvBytes(args ...string) int {
	total := 0
	for _, arg := range args {
		total += len(arg) + 1
	}
	return total
}

type externalRepairIncomingTransition struct {
	Kind       state.CompletedBranchTransitionKind
	ID         string
	SourceHead string
}

// externalRepairCandidates walks backward from the stale publication parent.
// It returns the nearest repair first, followed by any immediately adjacent
// earlier repairs. The walk stops at the first ordinary publication before the
// repair run so inherited history cannot authorize restored paths.
func externalRepairCandidates(
	ctx context.Context,
	repoRoot string,
	evidence *externalRepairEvidence,
	externalParent string,
) ([]externalRepairCandidate, bool, error) {
	current := externalParent
	sawPublication := false
	var candidates []externalRepairCandidate
	for step := 0; step < maxExternalRepairBridgeCommits; step++ {
		incoming, err := externalRepairIncomingTransitions(
			ctx, evidence.db, evidence.branchRef,
			evidence.branchGeneration, current)
		if err != nil {
			return nil, false, err
		}
		if err := evidence.budget.consume(
			len(incoming), "incoming repair transitions"); err != nil {
			return nil, false, err
		}
		switch len(incoming) {
		case 0:
			return candidates, len(candidates) > 0, nil
		case 1:
			// Continue below.
		default:
			return nil, false, fmt.Errorf(
				"%w: external repair bridge: transition into %s is ambiguous",
				state.ErrCompletedBranchTransitionProof, current)
		}
		transition := incoming[0]
		if transition.SourceHead == "" || transition.SourceHead == current {
			return nil, false, fmt.Errorf(
				"%w: external repair bridge: transition %s has invalid source",
				state.ErrCompletedBranchTransitionProof, transition.ID)
		}
		switch transition.Kind {
		case state.CompletedBranchTransitionSelfPublication:
			if len(candidates) > 0 {
				return candidates, true, nil
			}
			sawPublication = true
			current = transition.SourceHead
		case state.CompletedBranchTransitionIntentRepair:
			if !sawPublication {
				return nil, false, nil
			}
			candidate, err := loadExternalRepairCandidate(
				ctx, repoRoot, evidence, transition.ID)
			if err != nil {
				return nil, false, err
			}
			candidates = append(candidates, candidate)
			current = transition.SourceHead
		default:
			return nil, false, fmt.Errorf(
				"%w: external repair bridge: transition %s has unknown kind %q",
				state.ErrCompletedBranchTransitionProof,
				transition.ID, transition.Kind)
		}
	}
	return nil, false, fmt.Errorf(
		"%w: external repair bridge: incoming transition proof exceeds %d steps",
		state.ErrCompletedBranchTransitionProof,
		maxExternalRepairBridgeCommits)
}

func externalRepairIncomingTransitions(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	targetHead string,
) ([]externalRepairIncomingTransition, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT kind,id,source_head
FROM (
    SELECT 'self_publication' AS kind,id,source_head,created_ts
    FROM self_publications
    WHERE branch_ref=? AND branch_generation=?
      AND target_commit_oid=? AND phase='completed'
    UNION ALL
    SELECT 'intent_repair' AS kind,id,old_head AS source_head,created_ts
    FROM intent_repairs
    WHERE branch_ref=? AND branch_generation=?
      AND new_head=? AND status='completed'
)
ORDER BY created_ts,id
LIMIT 2`,
		branchRef, branchGeneration, targetHead,
		branchRef, branchGeneration, targetHead)
	if err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: load incoming transitions: %w", err)
	}
	defer rows.Close()
	var transitions []externalRepairIncomingTransition
	for rows.Next() {
		var transition externalRepairIncomingTransition
		if err := rows.Scan(
			&transition.Kind, &transition.ID,
			&transition.SourceHead); err != nil {
			return nil, fmt.Errorf(
				"daemon: external repair bridge: scan incoming transition: %w", err)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: iterate incoming transitions: %w", err)
	}
	return transitions, nil
}

type externalRepairOutgoingTransition struct {
	Kind       state.CompletedBranchTransitionKind
	ID         string
	TargetHead string
}

func (evidence *externalRepairEvidence) outgoingTransitions(
	ctx context.Context,
	sourceHead string,
) ([]externalRepairOutgoingTransition, error) {
	rows, err := evidence.db.ReadSQL().QueryContext(ctx, `
SELECT kind,id,target_head
FROM (
    SELECT 'self_publication' AS kind,id,target_commit_oid AS target_head,
           created_ts
    FROM self_publications
    WHERE branch_ref=? AND branch_generation=?
      AND source_head=? AND phase='completed'
    UNION ALL
    SELECT 'intent_repair' AS kind,id,new_head AS target_head,created_ts
    FROM intent_repairs
    WHERE branch_ref=? AND branch_generation=?
      AND old_head=? AND status='completed'
      AND new_head IS NOT NULL AND new_head<>''
)
ORDER BY created_ts,id
LIMIT 2`,
		evidence.branchRef, evidence.branchGeneration, sourceHead,
		evidence.branchRef, evidence.branchGeneration, sourceHead)
	if err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: load outgoing transitions: %w", err)
	}
	defer rows.Close()
	var transitions []externalRepairOutgoingTransition
	for rows.Next() {
		var transition externalRepairOutgoingTransition
		if err := rows.Scan(
			&transition.Kind, &transition.ID,
			&transition.TargetHead); err != nil {
			return nil, fmt.Errorf(
				"daemon: external repair bridge: scan outgoing transition: %w", err)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: iterate outgoing transitions: %w", err)
	}
	if err := evidence.budget.consume(
		len(transitions), "outgoing repair transitions"); err != nil {
		return nil, err
	}
	return transitions, nil
}

func loadExternalRepairCandidate(
	ctx context.Context,
	repoRoot string,
	evidence *externalRepairEvidence,
	repairID string,
) (externalRepairCandidate, error) {
	repair, err := evidence.loadRepair(ctx, repairID)
	if err != nil {
		return externalRepairCandidate{}, fmt.Errorf(
			"daemon: external repair bridge: load repair %s: %w",
			repairID, err)
	}
	if repair.Status != state.IntentRepairCompleted ||
		repair.MembershipMode != state.IntentRepairMembershipFrozen ||
		repair.BranchRef != evidence.branchRef ||
		repair.BranchGeneration != evidence.branchGeneration {
		return externalRepairCandidate{}, fmt.Errorf(
			"%w: external repair bridge: repair %s identity changed",
			state.ErrCompletedBranchTransitionProof, repairID)
	}
	if !repair.OldHead.Valid || repair.OldHead.String == "" ||
		!repair.NewHead.Valid || repair.NewHead.String == "" {
		return externalRepairCandidate{}, fmt.Errorf(
			"%w: external repair bridge: repair %s lacks head mapping",
			state.ErrCompletedBranchTransitionProof, repairID)
	}
	transitions, err := evidence.outgoingTransitions(
		ctx, repair.OldHead.String)
	if err != nil {
		return externalRepairCandidate{}, err
	}
	if len(transitions) != 1 ||
		transitions[0].Kind != state.CompletedBranchTransitionIntentRepair ||
		transitions[0].ID != repair.ID ||
		transitions[0].TargetHead != repair.NewHead.String {
		return externalRepairCandidate{}, fmt.Errorf(
			"%w: external repair bridge: repair %s transition is not unique",
			state.ErrCompletedBranchTransitionProof, repairID)
	}
	canonicalOIDByCandidate := make(map[string]string, len(repair.Commits))
	originalOIDByCandidate := make(map[string]string, len(repair.Commits))
	for _, commit := range repair.Commits {
		if commit.CandidateID.Valid && commit.NewOID.Valid {
			canonical, err := evidence.canonicalCommitChain(
				ctx, commit.NewOID.String)
			if err != nil {
				return externalRepairCandidate{}, err
			}
			canonicalOID := canonical[len(canonical)-1]
			if existing := canonicalOIDByCandidate[commit.CandidateID.String]; existing != "" && existing != canonicalOID {
				return externalRepairCandidate{}, fmt.Errorf(
					"%w: external repair bridge: repair %s candidate %s has conflicting mappings",
					state.ErrCompletedBranchTransitionProof,
					repair.ID, commit.CandidateID.String)
			}
			if existing := originalOIDByCandidate[commit.CandidateID.String]; existing != "" && existing != commit.NewOID.String {
				return externalRepairCandidate{}, fmt.Errorf(
					"%w: external repair bridge: repair %s candidate %s has conflicting original mappings",
					state.ErrCompletedBranchTransitionProof,
					repair.ID, commit.CandidateID.String)
			}
			canonicalOIDByCandidate[commit.CandidateID.String] = canonicalOID
			originalOIDByCandidate[commit.CandidateID.String] = commit.NewOID.String
		}
	}
	candidate := externalRepairCandidate{
		ID: repair.ID, OldHead: repair.OldHead.String,
		NewHead: repair.NewHead.String,
	}
	for _, member := range repair.Members {
		if err := evidence.budget.consume(
			1, "intent repair member events"); err != nil {
			return externalRepairCandidate{}, err
		}
		event, err := loadIntentCaptureEvent(
			ctx, evidence.db, member.EventSeq)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return externalRepairCandidate{}, fmt.Errorf(
					"%w: external repair bridge: repair %s member %d is missing",
					state.ErrCompletedBranchTransitionProof,
					repair.ID, member.EventSeq)
			}
			return externalRepairCandidate{}, fmt.Errorf(
				"daemon: external repair bridge: load member %d: %w",
				member.EventSeq, err)
		}
		canonicalOID := canonicalOIDByCandidate[member.CandidateID]
		if canonicalOID == "" {
			return externalRepairCandidate{}, fmt.Errorf(
				"%w: external repair bridge: repair %s member %d lacks commit mapping",
				state.ErrCompletedBranchTransitionProof,
				repair.ID, member.EventSeq)
		}
		originalOID := originalOIDByCandidate[member.CandidateID]
		if event.State != state.EventStatePublished ||
			!event.CommitOID.Valid ||
			(event.CommitOID.String != canonicalOID &&
				event.CommitOID.String != originalOID) {
			return externalRepairCandidate{}, fmt.Errorf(
				"%w: external repair bridge: repair %s member %d drifted",
				state.ErrCompletedBranchTransitionProof,
				repair.ID, member.EventSeq)
		}
		ops, err := state.LoadCaptureOpsBounded(
			ctx, evidence.db, member.EventSeq, evidence.budget.remaining)
		if err != nil {
			return externalRepairCandidate{}, fmt.Errorf(
				"daemon: external repair bridge: load member ops %d: %w",
				member.EventSeq, err)
		}
		if err := evidence.budget.consume(
			len(ops), "intent repair member operations"); err != nil {
			return externalRepairCandidate{}, err
		}
		if len(ops) == 0 || validateOps(ops) != "" {
			return externalRepairCandidate{}, fmt.Errorf(
				"%w: external repair bridge: repair %s member %d has invalid operations",
				state.ErrCompletedBranchTransitionProof,
				repair.ID, member.EventSeq)
		}
		switch member.PriorState {
		case state.EventStatePending, state.EventStatePublished:
			// Both states own paths in the frozen repair. Raw member deltas are
			// not replayed here because repair materialization may have coalesced
			// them before it created NewHead.
		default:
			return externalRepairCandidate{}, fmt.Errorf(
				"%w: external repair bridge: repair %s member %d has prior state %q",
				state.ErrCompletedBranchTransitionProof,
				repair.ID, member.EventSeq, member.PriorState)
		}
		candidate.Ops = append(candidate.Ops, ops...)
	}
	if len(candidate.Ops) == 0 {
		return externalRepairCandidate{}, fmt.Errorf(
			"%w: external repair bridge: repair %s has no operations",
			state.ErrCompletedBranchTransitionProof, repair.ID)
	}
	return candidate, nil
}

func externalRepairCandidateTreeMatches(
	ctx context.Context,
	repoRoot string,
	candidate externalRepairCandidate,
) (bool, error) {
	touched := make(map[string]struct{})
	for _, path := range touchedPaths(candidate.Ops) {
		touched[path] = struct{}{}
		if len(touched) > maxExternalRepairBridgePaths {
			return false, fmt.Errorf(
				"%w: external repair bridge: repair %s exceeds %d paths",
				state.ErrCompletedBranchTransitionProof, candidate.ID,
				maxExternalRepairBridgePaths)
		}
	}
	if len(touched) == 0 {
		return false, nil
	}
	changedPaths, err := externalBridgeChangedPaths(
		ctx, repoRoot, candidate.OldHead, candidate.NewHead)
	if err != nil {
		return false, err
	}
	for _, path := range changedPaths {
		if _, owned := touched[path]; !owned {
			return false, nil
		}
	}
	return true, nil
}

func sameExternalBridgeEntry(
	left map[string]git.IndexEntry,
	right map[string]git.IndexEntry,
	path string,
) bool {
	leftEntry, leftOK := left[path]
	rightEntry, rightOK := right[path]
	return leftOK == rightOK && (!leftOK || leftEntry == rightEntry)
}

func externalBridgeIndexesMatch(
	left map[string]git.IndexEntry,
	right map[string]git.IndexEntry,
	paths []string,
) bool {
	for _, path := range paths {
		if !sameExternalBridgeEntry(left, right, path) {
			return false
		}
	}
	return true
}

func externalBridgeFinalState(
	index map[string]git.IndexEntry,
	paths map[string]struct{},
) []recoveryPathState {
	ordered := make([]string, 0, len(paths))
	for path := range paths {
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	states := make([]recoveryPathState, 0, len(ordered))
	for _, path := range ordered {
		entry, present := index[path]
		states = append(states, recoveryPathState{
			Path: path, Present: present, Mode: entry.Mode, OID: entry.OID,
		})
	}
	return states
}

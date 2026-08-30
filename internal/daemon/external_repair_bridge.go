package daemon

import (
	"bytes"
	"context"
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
)

type externalRepairBridgeProof struct {
	TargetCommit string
	RepairID     string
	EventCount   int
	FinalState   []recoveryPathState
}

type externalRepairCandidate struct {
	ID      string
	OldHead string
	NewHead string
	Ops     []state.CaptureOp
}

func reconcileExternalRepairBridge(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	opts RecoveryReconcileOptions,
	live recoveryLiveState,
	chain []state.RecoveryChainEvent,
	proofChain []state.RecoveryChainEvent,
	bridge externalRepairBridgeProof,
) (RecoveryChainResult, error) {
	var result RecoveryChainResult
	if err := requireRecoveryInputsUnchanged(
		ctx, repoRoot, live, opts.ExpectedMissingRef,
	); err != nil {
		return result, err
	}
	first := chain[0].Event
	last := chain[len(chain)-1].Event
	ref := recoveryProofRefName(
		opts.BranchRef, opts.BranchGeneration,
		first.Seq, last.Seq, bridge.TargetCommit)
	commitOID, err := ensurePublishedProofRef(
		ctx, repoRoot, ref, bridge.TargetCommit, proofChain, bridge.FinalState)
	if err != nil {
		return result, err
	}
	if commitOID != bridge.TargetCommit {
		return result, fmt.Errorf(
			"%w: external repair bridge ref %s points at %s, want exact target %s",
			git.ErrRecoveryRefCollision, ref, commitOID, bridge.TargetCommit)
	}
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
		Expected:              chain,
		TargetState:           state.EventStatePublished,
		CommitOID:             commitOID,
		RecoveryRef:           ref,
		Reason:                trigger,
		DecisionKind:          opts.decisionKind,
		AllowLaterUnpublished: true,
	}
	var snapshot state.RecoverySnapshot
	err = git.WithLockedRecoveryRef(
		ctx, repoRoot, ref, commitOID, func() error {
			if opts.afterRecoveryRefLocked != nil {
				opts.afterRecoveryRefLocked()
			}
			var transitionErr error
			snapshot, transitionErr = state.TransitionRecoveryChain(
				ctx, db, transition)
			return transitionErr
		})
	if err != nil {
		return result, fmt.Errorf(
			"daemon: reconcile external repair bridge: protected transition: %w", err)
	}
	result = RecoveryChainResult{
		Handled:     true,
		Outcome:     snapshot.Outcome,
		SnapshotID:  snapshot.ID,
		RecoveryRef: snapshot.RecoveryRef.String,
		CommitOID:   snapshot.CommitOID,
		FirstSeq:    snapshot.FirstEventSeq,
		LastSeq:     snapshot.LastEventSeq,
		EventCount:  snapshot.EventCount,
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
	chain, err = canonicalizeRecoveryProofEvents(
		ctx, repoRoot, db, opts.BranchRef, opts.BranchGeneration,
		sameBranchRecoveryHead(live, opts.BranchRef), chain)
	if err != nil {
		return zero, false, err
	}
	if err := validateRecoveryObjects(ctx, repoRoot, chain); err != nil {
		return zero, false, err
	}
	target, ok, err := firstParentChildSince(
		ctx, repoRoot, opts.ExternalParentHead, live.head)
	if err != nil || !ok {
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
		ctx, db, opts.BranchRef, opts.BranchGeneration,
		opts.ExternalParentHead)
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
	repairOnParent, err := git.IsAncestor(
		ctx, repoRoot, candidate.NewHead, opts.ExternalParentHead)
	if err != nil {
		return zero, false, fmt.Errorf(
			"daemon: external repair bridge: prove repair %s ancestry: %w",
			candidate.ID, err)
	}
	if !repairOnParent {
		return zero, false, nil
	}
	publicationSuffix, ownedSuffix, err := loadExternalRepairPublicationSuffix(
		ctx, repoRoot, db, opts.BranchRef, opts.BranchGeneration,
		candidate.NewHead, opts.ExternalParentHead)
	if err != nil {
		return zero, false, err
	}
	if !ownedSuffix {
		return zero, false, nil
	}
	if err := validateRecoveryObjects(ctx, repoRoot, publicationSuffix); err != nil {
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
	chainMatches, err := recoveryChainMatchesHEAD(
		ctx, repoRoot, target, chain, finalState)
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
		FinalState:   finalState,
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
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	repairHead string,
	externalParent string,
) ([]state.RecoveryChainEvent, bool, error) {
	transitions, owned, err := state.CompletedBranchTransitionChain(
		ctx, db, branchRef, branchGeneration, repairHead, externalParent)
	if err != nil || !owned || len(transitions) == 0 {
		return nil, false, err
	}
	events := make([]state.RecoveryChainEvent, 0)
	opCount := 0
	for _, transition := range transitions {
		if transition.Kind != state.CompletedBranchTransitionSelfPublication {
			return nil, false, nil
		}
		publicationEvents, rawOps, err := loadExternalRepairPublication(
			ctx, repoRoot, db, branchRef, branchGeneration, transition)
		if err != nil {
			return nil, false, err
		}
		opCount += rawOps
		if opCount > state.CompletedBranchTransitionProofLimit ||
			len(events)+len(publicationEvents) > state.CompletedBranchTransitionProofLimit {
			return nil, false, fmt.Errorf(
				"%w: external repair bridge: publication suffix exceeds %d events or operations",
				state.ErrCompletedBranchTransitionProof,
				state.CompletedBranchTransitionProofLimit)
		}
		events = append(events, publicationEvents...)
	}
	return events, true, nil
}

func loadExternalRepairPublication(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	transition state.CompletedBranchTransition,
) ([]state.RecoveryChainEvent, int, error) {
	parents, err := selfPublicationParents(ctx, repoRoot, transition.TargetHead)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"%w: external repair bridge: read publication %s parents: %v",
			state.ErrCompletedBranchTransitionProof, transition.ID, err)
	}
	if len(parents) != 1 || parents[0] != transition.SourceHead {
		return nil, 0, fmt.Errorf(
			"%w: external repair bridge: publication %s does not map the Git edge %s..%s",
			state.ErrCompletedBranchTransitionProof, transition.ID,
			transition.SourceHead, transition.TargetHead)
	}

	publication, ok, err := state.SelfPublicationByID(ctx, db, transition.ID)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"daemon: external repair bridge: load publication %s: %w",
			transition.ID, err)
	}
	membershipDigest, digestErr := state.SelfPublicationMembershipDigest(
		publication.Members)
	if !ok || publication.Phase != state.SelfPublicationCompleted ||
		publication.BranchRef != branchRef ||
		publication.BranchGeneration != branchGeneration ||
		publication.SourceHead != transition.SourceHead ||
		publication.TargetCommitOID != transition.TargetHead ||
		publication.MemberCount != len(publication.Members) ||
		len(publication.Members) != len(transition.EventSeqs) ||
		digestErr != nil || membershipDigest != publication.MembershipDigest {
		return nil, 0, fmt.Errorf(
			"%w: external repair bridge: publication %s identity changed",
			state.ErrCompletedBranchTransitionProof, transition.ID)
	}
	treeOID, err := resolveTreeOID(ctx, repoRoot, transition.TargetHead)
	if err != nil || treeOID != publication.TargetTreeOID {
		return nil, 0, fmt.Errorf(
			"%w: external repair bridge: publication %s tree proof changed",
			state.ErrCompletedBranchTransitionProof, transition.ID)
	}

	window := make([]state.CaptureEvent, len(publication.Members))
	opsBySeq := make(map[int64][]state.CaptureOp, len(publication.Members))
	rawOpCount := 0
	for i, member := range publication.Members {
		if member.Ord != i || member.EventSeq != transition.EventSeqs[i] ||
			(member.CandidateID.Valid && strings.TrimSpace(member.CandidateID.String) == "") {
			return nil, 0, fmt.Errorf(
				"%w: external repair bridge: publication %s membership changed",
				state.ErrCompletedBranchTransitionProof, transition.ID)
		}
		event, err := loadIntentCaptureEvent(ctx, db, member.EventSeq)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"daemon: external repair bridge: load publication member %d: %w",
				member.EventSeq, err)
		}
		if event.State != state.EventStatePublished ||
			!event.CommitOID.Valid ||
			event.CommitOID.String != transition.TargetHead {
			return nil, 0, fmt.Errorf(
				"%w: external repair bridge: publication %s member %d drifted",
				state.ErrCompletedBranchTransitionProof,
				transition.ID, member.EventSeq)
		}
		ops, err := state.LoadCaptureOps(ctx, db, member.EventSeq)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"daemon: external repair bridge: load publication member ops %d: %w",
				member.EventSeq, err)
		}
		if len(ops) == 0 || validateOps(ops) != "" {
			return nil, 0, fmt.Errorf(
				"%w: external repair bridge: publication %s member %d has invalid operations",
				state.ErrCompletedBranchTransitionProof,
				transition.ID, member.EventSeq)
		}
		rawOpCount += len(ops)
		if rawOpCount > state.CompletedBranchTransitionProofLimit {
			return nil, 0, fmt.Errorf(
				"%w: external repair bridge: publication %s exceeds %d operations",
				state.ErrCompletedBranchTransitionProof, transition.ID,
				state.CompletedBranchTransitionProofLimit)
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
		}
		offers, err := coalesceIntentWindow(
			ctx, db, window[start:end], true, loadOps)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"%w: external repair bridge: publication %s cannot reconstruct applied operations: %v",
				state.ErrCompletedBranchTransitionProof, transition.ID, err)
		}
		for _, offer := range offers {
			if len(offer.MergedOps) == 0 || validateOps(offer.MergedOps) != "" {
				return nil, 0, fmt.Errorf(
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
	return applied, rawOpCount, nil
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
	drains, err := state.ActivePublicationDrains(ctx, db)
	if err != nil {
		return 0, false, fmt.Errorf(
			"daemon: external repair bridge: load active publication drains: %w", err)
	}
	var matched []state.PublicationDrain
	for _, drain := range drains {
		if drain.BranchRef == opts.BranchRef &&
			drain.BranchGeneration == opts.BranchGeneration {
			matched = append(matched, drain)
		}
	}
	if len(matched) != 1 {
		return 0, false, nil
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT event.seq
FROM publication_drain_events member
JOIN capture_events event ON event.seq=member.event_seq
WHERE member.drain_id=?
  AND event.state IN (?, ?, ?)
ORDER BY member.ord
LIMIT ?`, matched[0].ID,
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
	entries, err := git.LsTreeLimited(
		ctx, repoRoot, rev, false, git.DefaultDiffCap,
		git.LiteralPathspecs(paths)...)
	if err != nil {
		if errors.Is(err, git.ErrStdoutOverflow) {
			return nil, fmt.Errorf(
				"%w: external repair bridge: tree proof exceeds %d bytes",
				state.ErrCompletedBranchTransitionProof, git.DefaultDiffCap)
		}
		return nil, fmt.Errorf(
			"daemon: external repair bridge: read tree %s: %w", rev, err)
	}
	index := make(map[string]git.IndexEntry, len(entries))
	for _, entry := range entries {
		index[entry.Path] = git.IndexEntry{
			Mode: entry.Mode, OID: entry.OID, Path: entry.Path,
		}
	}
	return index, nil
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
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	externalParent string,
) ([]externalRepairCandidate, bool, error) {
	current := externalParent
	sawPublication := false
	var candidates []externalRepairCandidate
	for step := 0; step < maxExternalRepairBridgeCommits; step++ {
		incoming, err := externalRepairIncomingTransitions(
			ctx, db, branchRef, branchGeneration, current)
		if err != nil {
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
				ctx, db, branchRef, branchGeneration, transition.ID)
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

func loadExternalRepairCandidate(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	repairID string,
) (externalRepairCandidate, error) {
	repair, ok, err := state.IntentRepairByID(ctx, db, repairID)
	if err != nil {
		return externalRepairCandidate{}, fmt.Errorf(
			"daemon: external repair bridge: load repair %s: %w",
			repairID, err)
	}
	if !ok || repair.Status != state.IntentRepairCompleted ||
		repair.MembershipMode != state.IntentRepairMembershipFrozen ||
		repair.BranchRef != branchRef ||
		repair.BranchGeneration != branchGeneration {
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
	transitions, owned, err := state.CompletedBranchTransitionChain(
		ctx, db, branchRef, branchGeneration,
		repair.OldHead.String, repair.NewHead.String)
	if err != nil {
		return externalRepairCandidate{}, err
	}
	if !owned || len(transitions) != 1 ||
		transitions[0].Kind != state.CompletedBranchTransitionIntentRepair ||
		transitions[0].ID != repair.ID {
		return externalRepairCandidate{}, fmt.Errorf(
			"%w: external repair bridge: repair %s transition is not unique",
			state.ErrCompletedBranchTransitionProof, repairID)
	}
	canonicalOIDByCandidate := make(map[string]string, len(repair.Commits))
	originalOIDByCandidate := make(map[string]string, len(repair.Commits))
	for _, commit := range repair.Commits {
		if commit.CandidateID.Valid && commit.NewOID.Valid {
			canonical, err := state.CompletedIntentRepairCommitChain(
				ctx, db, branchRef, branchGeneration, commit.NewOID.String)
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
		event, err := loadIntentCaptureEvent(ctx, db, member.EventSeq)
		if err != nil {
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
		ops, err := state.LoadCaptureOps(ctx, db, member.EventSeq)
		if err != nil {
			return externalRepairCandidate{}, fmt.Errorf(
				"daemon: external repair bridge: load member ops %d: %w",
				member.EventSeq, err)
		}
		if len(ops) == 0 || validateOps(ops) != "" {
			return externalRepairCandidate{}, fmt.Errorf(
				"%w: external repair bridge: repair %s member %d has invalid operations",
				state.ErrCompletedBranchTransitionProof,
				repair.ID, member.EventSeq)
		}
		if len(candidate.Ops)+len(ops) > maxExternalRepairBridgePaths {
			return externalRepairCandidate{}, fmt.Errorf(
				"%w: external repair bridge: repair %s exceeds %d operations",
				state.ErrCompletedBranchTransitionProof, repair.ID,
				maxExternalRepairBridgePaths)
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

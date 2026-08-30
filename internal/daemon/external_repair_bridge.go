package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
		Expected:     chain,
		TargetState:  state.EventStatePublished,
		CommitOID:    commitOID,
		RecoveryRef:  ref,
		Reason:       trigger,
		DecisionKind: opts.decisionKind,
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
	frozen, err := externalBridgeMatchesFrozenDrain(ctx, db, opts, chain)
	if err != nil || !frozen {
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
	for path := range pendingPaths {
		if _, changed := changedSet[path]; !changed {
			return zero, false, nil
		}
	}

	candidate, foundCandidate, err := nearestExternalRepairCandidate(
		ctx, db, opts.BranchRef, opts.BranchGeneration,
		opts.ExternalParentHead)
	if err != nil {
		return zero, false, err
	}
	if !foundCandidate {
		return zero, false, nil
	}

	if err := validateRecoveryObjects(ctx, repoRoot,
		[]state.RecoveryChainEvent{{
			Event: state.CaptureEvent{Seq: 1, BaseHead: candidate.OldHead},
			Ops:   candidate.Ops,
		}}); err != nil {
		return zero, false, err
	}
	validRepairTree, err := externalRepairCandidateTreeMatches(
		ctx, repoRoot, candidate)
	if err != nil {
		return zero, false, err
	}
	if !validRepairTree {
		return zero, false, fmt.Errorf(
			"%w: external repair bridge: repair %s tree does not match its members",
			state.ErrCompletedBranchTransitionProof, candidate.ID)
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
		ctx, db, opts.BranchRef, opts.BranchGeneration,
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
		touchedPaths(candidate.Ops),
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
	if !valid || externalBridgeIndexesMatch(index, actualSource, proofPaths) {
		return zero, false, nil
	}
	for _, item := range chain {
		if conflict := applyRecoveryOpsInMemory(index, item.Ops); conflict != "" {
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
		FinalState:   finalState,
	}, true, nil
}

func loadExternalRepairPublicationSuffix(
	ctx context.Context,
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
		for _, seq := range transition.EventSeqs {
			event, err := loadIntentCaptureEvent(ctx, db, seq)
			if err != nil {
				return nil, false, fmt.Errorf(
					"daemon: external repair bridge: load publication member %d: %w",
					seq, err)
			}
			if event.State != state.EventStatePublished ||
				!event.CommitOID.Valid ||
				event.CommitOID.String != transition.TargetHead {
				return nil, false, fmt.Errorf(
					"%w: external repair bridge: publication %s member %d drifted",
					state.ErrCompletedBranchTransitionProof,
					transition.ID, seq)
			}
			ops, err := state.LoadCaptureOps(ctx, db, seq)
			if err != nil {
				return nil, false, fmt.Errorf(
					"daemon: external repair bridge: load publication member ops %d: %w",
					seq, err)
			}
			if len(ops) == 0 || validateOps(ops) != "" {
				return nil, false, fmt.Errorf(
					"%w: external repair bridge: publication %s member %d has invalid operations",
					state.ErrCompletedBranchTransitionProof,
					transition.ID, seq)
			}
			opCount += len(ops)
			if opCount > maxExternalRepairBridgePaths {
				return nil, false, fmt.Errorf(
					"%w: external repair bridge: publication suffix exceeds %d operations",
					state.ErrCompletedBranchTransitionProof,
					maxExternalRepairBridgePaths)
			}
			events = append(events, state.RecoveryChainEvent{Event: event, Ops: ops})
		}
	}
	return events, true, nil
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
) (bool, error) {
	drains, err := state.ActivePublicationDrains(ctx, db)
	if err != nil {
		return false, fmt.Errorf(
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
		return false, nil
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT event.seq
FROM publication_drain_events member
JOIN capture_events event ON event.seq=member.event_seq
WHERE member.drain_id=?
  AND event.state IN (?, ?, ?)
ORDER BY member.ord`, matched[0].ID,
		state.EventStatePending,
		state.EventStateBlockedConflict,
		state.EventStateFailed)
	if err != nil {
		return false, fmt.Errorf(
			"daemon: external repair bridge: load unresolved drain target: %w", err)
	}
	defer rows.Close()
	var unresolved []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return false, fmt.Errorf(
				"daemon: external repair bridge: scan unresolved drain target: %w", err)
		}
		unresolved = append(unresolved, seq)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf(
			"daemon: external repair bridge: iterate unresolved drain target: %w", err)
	}
	if len(unresolved) != len(chain) {
		return false, nil
	}
	for i, item := range chain {
		if unresolved[i] != item.Event.Seq {
			return false, nil
		}
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
		distance, direct, err := externalBridgeFirstParentDistance(
			ctx, repoRoot, parent, liveHead)
		if err != nil {
			return "", false, err
		}
		if direct && distance > maxExternalRepairBridgeCommits {
			return "", false, fmt.Errorf(
				"%w: external repair bridge: first-parent proof exceeds %d commits",
				state.ErrCompletedBranchTransitionProof,
				maxExternalRepairBridgeCommits)
		}
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

func externalBridgeFirstParentDistance(
	ctx context.Context,
	repoRoot string,
	parent string,
	liveHead string,
) (int, bool, error) {
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, 32, "rev-list", "--first-parent", "--count", parent+".."+liveHead)
	if err != nil {
		return 0, false, fmt.Errorf(
			"daemon: external repair bridge: count first-parent distance: %w", err)
	}
	distance, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil || distance < 0 {
		return 0, false, fmt.Errorf(
			"daemon: external repair bridge: invalid first-parent distance %q",
			strings.TrimSpace(string(out)))
	}
	if distance == 0 {
		return 0, false, nil
	}
	ancestor, err := git.RevParse(
		ctx, repoRoot, fmt.Sprintf("%s~%d", liveHead, distance))
	if errors.Is(err, git.ErrRefNotFound) {
		return distance, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf(
			"daemon: external repair bridge: resolve first-parent distance: %w", err)
	}
	return distance, ancestor == parent, nil
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
				"daemon: external repair bridge: target changes more than %d paths",
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

func nearestExternalRepairCandidate(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	externalParent string,
) (externalRepairCandidate, bool, error) {
	current := externalParent
	sawPublication := false
	for step := 0; step < maxExternalRepairBridgeCommits; step++ {
		incoming, err := externalRepairIncomingTransitions(
			ctx, db, branchRef, branchGeneration, current)
		if err != nil {
			return externalRepairCandidate{}, false, err
		}
		switch len(incoming) {
		case 0:
			return externalRepairCandidate{}, false, nil
		case 1:
			// Continue below.
		default:
			return externalRepairCandidate{}, false, fmt.Errorf(
				"%w: external repair bridge: transition into %s is ambiguous",
				state.ErrCompletedBranchTransitionProof, current)
		}
		transition := incoming[0]
		if transition.SourceHead == "" || transition.SourceHead == current {
			return externalRepairCandidate{}, false, fmt.Errorf(
				"%w: external repair bridge: transition %s has invalid source",
				state.ErrCompletedBranchTransitionProof, transition.ID)
		}
		switch transition.Kind {
		case state.CompletedBranchTransitionSelfPublication:
			sawPublication = true
			current = transition.SourceHead
		case state.CompletedBranchTransitionIntentRepair:
			if !sawPublication {
				return externalRepairCandidate{}, false, nil
			}
			candidate, err := loadExternalRepairCandidate(
				ctx, db, branchRef, branchGeneration, transition.ID)
			return candidate, err == nil, err
		default:
			return externalRepairCandidate{}, false, fmt.Errorf(
				"%w: external repair bridge: transition %s has unknown kind %q",
				state.ErrCompletedBranchTransitionProof,
				transition.ID, transition.Kind)
		}
	}
	return externalRepairCandidate{}, false, fmt.Errorf(
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
	for _, commit := range repair.Commits {
		if commit.CandidateID.Valid && commit.NewOID.Valid {
			canonical, err := state.CompletedIntentRepairCommitChain(
				ctx, db, branchRef, branchGeneration, commit.NewOID.String)
			if err != nil {
				return externalRepairCandidate{}, err
			}
			canonicalOID := canonical[len(canonical)-1]
			if existing := canonicalOIDByCandidate[commit.CandidateID.String];
				existing != "" && existing != canonicalOID {
				return externalRepairCandidate{}, fmt.Errorf(
					"%w: external repair bridge: repair %s candidate %s has conflicting mappings",
					state.ErrCompletedBranchTransitionProof,
					repair.ID, commit.CandidateID.String)
			}
			canonicalOIDByCandidate[commit.CandidateID.String] = canonicalOID
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
		if event.State != state.EventStatePublished ||
			!event.CommitOID.Valid || event.CommitOID.String != canonicalOID {
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

func cloneExternalBridgeIndex(
	index map[string]git.IndexEntry,
) map[string]git.IndexEntry {
	clone := make(map[string]git.IndexEntry, len(index))
	for path, entry := range index {
		clone[path] = entry
	}
	return clone
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

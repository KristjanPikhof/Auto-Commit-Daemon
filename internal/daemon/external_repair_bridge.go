package daemon

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	maxExternalRepairBridgeCommits = 64
	maxExternalRepairBridgePaths   = 4096
	maxExternalRepairCandidates    = 64
)

type externalRepairBridgeProof struct {
	TargetCommit string
	RepairID     string
	FinalState   []recoveryPathState
}

type externalRepairCandidate struct {
	ID      string
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

	sourceIndex, err := externalBridgeIndexState(
		ctx, repoRoot, opts.ExternalParentHead, changedPaths)
	if err != nil {
		return zero, false, err
	}
	targetIndex, err := externalBridgeIndexState(ctx, repoRoot, target, changedPaths)
	if err != nil {
		return zero, false, err
	}
	candidates, err := loadExternalRepairCandidates(
		ctx, db, opts.BranchRef, opts.BranchGeneration, changedPaths)
	if err != nil {
		return zero, false, err
	}

	var matches []externalRepairBridgeProof
	for _, candidate := range candidates {
		repairOnParent, err := git.IsAncestor(
			ctx, repoRoot, candidate.NewHead, opts.ExternalParentHead)
		if err != nil {
			return zero, false, fmt.Errorf(
				"daemon: external repair bridge: prove repair %s ancestry: %w",
				candidate.ID, err)
		}
		if !repairOnParent {
			continue
		}
		index := cloneExternalBridgeIndex(sourceIndex)
		repairChanged := make(map[string]struct{})
		valid := true
		for _, op := range candidate.Ops {
			opPaths := touchedPaths([]state.CaptureOp{op})
			if !externalBridgePathsIntersect(opPaths, changedSet) {
				continue
			}
			for _, path := range opPaths {
				if _, changed := changedSet[path]; !changed {
					valid = false
					break
				}
			}
			if !valid {
				break
			}
			before := cloneExternalBridgeIndex(index)
			if conflict := applyRecoveryOpsInMemory(index, []state.CaptureOp{op}); conflict != "" {
				valid = false
				break
			}
			for _, path := range opPaths {
				if !sameExternalBridgeEntry(before, index, path) {
					repairChanged[path] = struct{}{}
				}
			}
		}
		if !valid || len(repairChanged) == 0 {
			continue
		}
		for _, path := range changedPaths {
			if _, pending := pendingPaths[path]; pending {
				continue
			}
			if _, restored := repairChanged[path]; !restored {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		for _, item := range chain {
			if conflict := applyRecoveryOpsInMemory(index, item.Ops); conflict != "" {
				valid = false
				break
			}
		}
		if !valid || !externalBridgeIndexesMatch(index, targetIndex, changedPaths) {
			continue
		}
		finalState := externalBridgeFinalState(index, pendingPaths)
		chainMatches, err := recoveryChainMatchesHEAD(
			ctx, repoRoot, target, chain, finalState)
		if err != nil {
			return zero, false, err
		}
		if !chainMatches {
			continue
		}
		matches = append(matches, externalRepairBridgeProof{
			TargetCommit: target,
			RepairID:     candidate.ID,
			FinalState:   finalState,
		})
	}
	if len(matches) == 0 {
		return zero, false, nil
	}
	if len(matches) != 1 {
		return zero, false, fmt.Errorf(
			"daemon: external repair bridge: %d completed repairs match target %s",
			len(matches), target)
	}
	return matches[0], true, nil
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
	ancestor, err := git.IsAncestor(ctx, repoRoot, parent, liveHead)
	if err != nil {
		return "", false, fmt.Errorf(
			"daemon: external repair bridge: prove live ancestry: %w", err)
	}
	if !ancestor || parent == liveHead {
		return "", false, nil
	}
	maxOutput := int64(maxExternalRepairBridgeCommits * (64 + 1))
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, maxOutput, "rev-list", "--first-parent", "--reverse", parent+".."+liveHead)
	if err != nil {
		return "", false, fmt.Errorf(
			"daemon: external repair bridge: list bounded first-parent chain: %w", err)
	}
	commits := strings.Fields(string(out))
	if len(commits) == 0 || len(commits) > maxExternalRepairBridgeCommits {
		return "", false, nil
	}
	target := commits[0]
	parentsOut, err := git.Run(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, "show", "-s", "--format=%P", target)
	if err != nil {
		return "", false, fmt.Errorf(
			"daemon: external repair bridge: read target parents: %w", err)
	}
	parents := strings.Fields(string(parentsOut))
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
	entries, err := git.LsTree(
		ctx, repoRoot, rev, false, git.LiteralPathspecs(paths)...)
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

func loadExternalRepairCandidates(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	paths []string,
) ([]externalRepairCandidate, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(paths)), ",")
	args := make([]any, 0, 3+len(paths)*2)
	args = append(args, branchRef, branchGeneration)
	for _, path := range paths {
		args = append(args, path)
	}
	for _, path := range paths {
		args = append(args, path)
	}
	args = append(args, maxExternalRepairCandidates+1)
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT DISTINCT repair.id
FROM intent_repairs repair
JOIN intent_repair_member_seals seal ON seal.repair_id=repair.id
JOIN intent_repair_members member ON member.repair_id=repair.id
JOIN capture_ops op ON op.event_seq=member.event_seq
WHERE repair.branch_ref=? AND repair.branch_generation=?
  AND repair.status='completed'
  AND seal.membership_mode='frozen'
  AND seal.member_count=(
      SELECT COUNT(*) FROM intent_repair_members owned
      WHERE owned.repair_id=repair.id)
  AND (op.path IN (`+placeholders+`) OR op.old_path IN (`+placeholders+`))
ORDER BY repair.completed_ts DESC, repair.id
LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: list completed repairs: %w", err)
	}
	defer rows.Close()
	var repairIDs []string
	for rows.Next() {
		var repairID string
		if err := rows.Scan(&repairID); err != nil {
			return nil, fmt.Errorf(
				"daemon: external repair bridge: scan repair: %w", err)
		}
		repairIDs = append(repairIDs, repairID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: iterate repairs: %w", err)
	}
	if len(repairIDs) > maxExternalRepairCandidates {
		return nil, fmt.Errorf(
			"daemon: external repair bridge: more than %d repair candidates",
			maxExternalRepairCandidates)
	}

	candidates := make([]externalRepairCandidate, 0, len(repairIDs))
	for _, repairID := range repairIDs {
		repair, ok, err := state.IntentRepairByID(ctx, db, repairID)
		if err != nil {
			return nil, fmt.Errorf(
				"daemon: external repair bridge: load repair %s: %w",
				repairID, err)
		}
		if !ok || repair.Status != state.IntentRepairCompleted ||
			repair.MembershipMode != state.IntentRepairMembershipFrozen ||
			repair.BranchRef != branchRef ||
			repair.BranchGeneration != branchGeneration {
			continue
		}
		newOIDByCandidate := make(map[string]string, len(repair.Commits))
		for _, commit := range repair.Commits {
			if commit.CandidateID.Valid && commit.NewOID.Valid {
				newOIDByCandidate[commit.CandidateID.String] = commit.NewOID.String
			}
		}
		if !repair.NewHead.Valid || repair.NewHead.String == "" {
			continue
		}
		candidate := externalRepairCandidate{
			ID: repair.ID, NewHead: repair.NewHead.String,
		}
		valid := true
		for _, member := range repair.Members {
			event, err := loadIntentCaptureEvent(ctx, db, member.EventSeq)
			if err != nil {
				return nil, fmt.Errorf(
					"daemon: external repair bridge: load member %d: %w",
					member.EventSeq, err)
			}
			newOID := newOIDByCandidate[member.CandidateID]
			if event.State != state.EventStatePublished ||
				!event.CommitOID.Valid || event.CommitOID.String != newOID {
				valid = false
				break
			}
			ops, err := state.LoadCaptureOps(ctx, db, member.EventSeq)
			if err != nil {
				return nil, fmt.Errorf(
					"daemon: external repair bridge: load member ops %d: %w",
					member.EventSeq, err)
			}
			if len(ops) == 0 || validateOps(ops) != "" {
				valid = false
				break
			}
			candidate.Ops = append(candidate.Ops, ops...)
		}
		if valid && len(candidate.Ops) > 0 {
			candidates = append(candidates, candidate)
		}
	}
	return candidates, nil
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

func externalBridgePathsIntersect(
	paths []string,
	set map[string]struct{},
) bool {
	for _, path := range paths {
		if _, ok := set[path]; ok {
			return true
		}
	}
	return false
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

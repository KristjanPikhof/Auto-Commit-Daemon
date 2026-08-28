package state

import (
	"context"
	"errors"
	"fmt"
)

// CompletedBranchTransitionKind identifies the durable journal that proves an
// ACD-authored branch move.
type CompletedBranchTransitionKind string

const (
	CompletedBranchTransitionSelfPublication CompletedBranchTransitionKind = "self_publication"
	CompletedBranchTransitionIntentRepair    CompletedBranchTransitionKind = "intent_repair"

	// CompletedBranchTransitionProofLimit bounds journal traversal when ACD
	// must distinguish its own ref movement from an external Git operation.
	CompletedBranchTransitionProofLimit = 4096
)

// CompletedBranchTransition is one immutable, completed ACD-authored ref
// movement. Ordinary self-publications expose EventSeqs. Intent repairs expose
// their immutable membership when the repair was prepared by schema v24 or
// later; legacy repairs deliberately have no member rows.
type CompletedBranchTransition struct {
	Kind                CompletedBranchTransitionKind
	ID                  string
	SourceHead          string
	TargetHead          string
	CompletedTS         float64
	EventSeqs           []int64
	CommitMappings      []IntentRepairCommit
	IntentRepairMembers []IntentRepairMember
}

// CompletedBranchTransitionChain proves a unique completed ACD-authored path
// between two heads on one exact branch pair. A missing path is not an error;
// ambiguous, cyclic, or overlong journal history fails closed.
func CompletedBranchTransitionChain(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	sourceHead string,
	targetHead string,
) ([]CompletedBranchTransition, bool, error) {
	if d == nil || branchRef == "" || branchGeneration < 0 ||
		sourceHead == "" || targetHead == "" {
		return nil, false, errors.New(
			"state: CompletedBranchTransitionChain: invalid input")
	}
	if sourceHead == targetHead {
		return []CompletedBranchTransition{}, true, nil
	}

	current := sourceHead
	seen := map[string]struct{}{sourceHead: {}}
	chain := make([]CompletedBranchTransition, 0, 2)
	for step := 0; step < CompletedBranchTransitionProofLimit; step++ {
		outgoing, err := completedBranchTransitionsFrom(
			ctx, d, branchRef, branchGeneration, current)
		if err != nil {
			return nil, false, err
		}
		switch len(outgoing) {
		case 0:
			return nil, false, nil
		case 1:
			// Continue below.
		default:
			return nil, false, fmt.Errorf(
				"state: completed branch transition from %s is ambiguous", current)
		}

		transition := outgoing[0]
		if transition.TargetHead == current {
			return nil, false, fmt.Errorf(
				"state: completed branch transition does not move %s", current)
		}
		if _, duplicate := seen[transition.TargetHead]; duplicate {
			return nil, false, fmt.Errorf(
				"state: completed branch transition cycle at %s", transition.TargetHead)
		}
		seen[transition.TargetHead] = struct{}{}
		chain = append(chain, transition)
		current = transition.TargetHead
		if current == targetHead {
			return chain, true, nil
		}
	}
	return nil, false, fmt.Errorf(
		"state: completed branch transition proof exceeds %d steps",
		CompletedBranchTransitionProofLimit)
}

// CompletedBranchTransitionOwnsCheckpointTarget proves that a completed ACD
// transition chain did not publish captures outside one checkpoint's frozen
// membership. A legacy Intent repair is accepted only when it completed before
// the checkpoint existed; newer post-checkpoint repairs must expose immutable
// members so pending captures can be matched exactly.
func CompletedBranchTransitionOwnsCheckpointTarget(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	sourceHead string,
	targetHead string,
	checkpointCreatedTS float64,
	targetEventSeqs []int64,
) (bool, error) {
	if checkpointCreatedTS <= 0 {
		return false, errors.New(
			"state: completed checkpoint transition proof requires creation time")
	}
	chain, owned, err := CompletedBranchTransitionChain(
		ctx, d, branchRef, branchGeneration, sourceHead, targetHead)
	if err != nil || !owned {
		return false, err
	}
	unusedEvents := make(map[int64]struct{}, len(targetEventSeqs))
	for _, seq := range targetEventSeqs {
		if seq <= 0 {
			return false, errors.New(
				"state: completed checkpoint transition proof has invalid event")
		}
		if _, duplicate := unusedEvents[seq]; duplicate {
			return false, errors.New(
				"state: completed checkpoint transition proof has duplicate event")
		}
		unusedEvents[seq] = struct{}{}
	}

	for _, transition := range chain {
		switch transition.Kind {
		case CompletedBranchTransitionSelfPublication:
			for _, seq := range transition.EventSeqs {
				if _, allowed := unusedEvents[seq]; !allowed {
					return false, nil
				}
				delete(unusedEvents, seq)
			}
		case CompletedBranchTransitionIntentRepair:
			if transition.CompletedTS <= checkpointCreatedTS {
				continue
			}
			if len(transition.IntentRepairMembers) == 0 {
				return false, nil
			}
			for _, member := range transition.IntentRepairMembers {
				switch member.PriorState {
				case EventStatePublished:
					continue
				case EventStatePending:
					if _, allowed := unusedEvents[member.EventSeq]; !allowed {
						return false, nil
					}
					delete(unusedEvents, member.EventSeq)
				default:
					return false, nil
				}
			}
		default:
			return false, nil
		}
	}
	return true, nil
}

func completedBranchTransitionsFrom(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	sourceHead string,
) ([]CompletedBranchTransition, error) {
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT kind,id,target_head,completed_ts
FROM (
    SELECT 'self_publication' AS kind,id,target_commit_oid AS target_head,
           created_ts,COALESCE(completed_ts,0) AS completed_ts
    FROM self_publications
    WHERE branch_ref=? AND branch_generation=? AND source_head=?
      AND phase='completed'
    UNION ALL
    SELECT 'intent_repair' AS kind,id,new_head AS target_head,created_ts,
           COALESCE(completed_ts,0) AS completed_ts
    FROM intent_repairs
    WHERE branch_ref=? AND branch_generation=? AND old_head=?
      AND status='completed' AND new_head IS NOT NULL AND new_head<>''
)
ORDER BY created_ts,id
LIMIT 2`,
		branchRef, branchGeneration, sourceHead,
		branchRef, branchGeneration, sourceHead)
	if err != nil {
		return nil, fmt.Errorf(
			"state: query completed branch transitions: %w", err)
	}
	defer rows.Close()

	var transitions []CompletedBranchTransition
	for rows.Next() {
		var transition CompletedBranchTransition
		if err := rows.Scan(
			&transition.Kind, &transition.ID, &transition.TargetHead,
			&transition.CompletedTS); err != nil {
			return nil, fmt.Errorf(
				"state: scan completed branch transition: %w", err)
		}
		transition.SourceHead = sourceHead
		if transition.TargetHead == "" || transition.CompletedTS <= 0 {
			return nil, errors.New(
				"state: completed branch transition has incomplete completion proof")
		}
		switch transition.Kind {
		case CompletedBranchTransitionSelfPublication:
			members, err := completedSelfPublicationMembers(
				ctx, d, transition.ID)
			if err != nil {
				return nil, err
			}
			transition.EventSeqs = members
		case CompletedBranchTransitionIntentRepair:
			repair, err := completedIntentRepairProof(
				ctx, d, transition)
			if err != nil {
				return nil, err
			}
			transition.CommitMappings = repair.Commits
			transition.IntentRepairMembers = repair.Members
		default:
			return nil, fmt.Errorf(
				"state: unknown completed branch transition kind %q",
				transition.Kind)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"state: iterate completed branch transitions: %w", err)
	}
	return transitions, nil
}

func completedIntentRepairProof(
	ctx context.Context,
	d *DB,
	transition CompletedBranchTransition,
) (IntentRepair, error) {
	repair, ok, err := IntentRepairByID(ctx, d, transition.ID)
	if err != nil {
		return IntentRepair{}, err
	}
	if !ok || repair.Status != IntentRepairCompleted ||
		repair.BranchRef == "" ||
		repair.ExpectedHead != transition.SourceHead ||
		!repair.OldHead.Valid || repair.OldHead.String != transition.SourceHead ||
		!repair.NewHead.Valid || repair.NewHead.String != transition.TargetHead ||
		!repair.BackupRef.Valid || repair.BackupRef.String == "" ||
		len(repair.Commits) == 0 || len(repair.Commits) > IntentRepairMaxCommits {
		return IntentRepair{}, fmt.Errorf(
			"state: completed intent repair %s has incomplete transition proof",
			transition.ID)
	}
	for _, mapping := range repair.Commits {
		if mapping.OldOID == "" || !mapping.NewOID.Valid ||
			mapping.NewOID.String == "" || !mapping.CandidateID.Valid ||
			mapping.CandidateID.String == "" {
			return IntentRepair{}, fmt.Errorf(
				"state: completed intent repair %s has incomplete commit mapping",
				transition.ID)
		}
	}
	if len(repair.Members) > 0 {
		if err := validateIntentRepairMembers(repair); err != nil {
			return IntentRepair{}, fmt.Errorf(
				"state: completed intent repair %s has invalid membership: %w",
				transition.ID, err)
		}
	}
	return repair, nil
}

// CanonicalCompletedIntentRepairCommit follows the unique completed Intent
// repair mapping for one commit within an exact branch pair. The caller must
// still prove the resulting commit's Git ancestry before using it as a replay
// or recovery base.
func CanonicalCompletedIntentRepairCommit(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	commitOID string,
) (string, bool, error) {
	if d == nil || branchRef == "" || branchGeneration < 0 || commitOID == "" {
		return "", false, errors.New(
			"state: CanonicalCompletedIntentRepairCommit: invalid input")
	}

	current := commitOID
	seen := map[string]struct{}{current: {}}
	mapped := false
	for step := 0; step < CompletedBranchTransitionProofLimit; step++ {
		rows, err := d.readSQL().QueryContext(ctx, `
SELECT r.id,c.new_oid
FROM intent_repair_commits c
JOIN intent_repairs r ON r.id=c.repair_id
WHERE r.branch_ref=? AND r.branch_generation=?
  AND r.status='completed' AND c.old_oid=?
  AND c.new_oid IS NOT NULL AND c.new_oid<>''
ORDER BY r.created_ts,r.id
LIMIT 2`, branchRef, branchGeneration, current)
		if err != nil {
			return "", false, fmt.Errorf(
				"state: query completed intent repair mapping: %w", err)
		}
		type repairMapping struct{ repairID, targetOID string }
		var mappings []repairMapping
		for rows.Next() {
			var mapping repairMapping
			if err := rows.Scan(&mapping.repairID, &mapping.targetOID); err != nil {
				rows.Close()
				return "", false, fmt.Errorf(
					"state: scan completed intent repair mapping: %w", err)
			}
			mappings = append(mappings, mapping)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return "", false, fmt.Errorf(
				"state: iterate completed intent repair mapping: %w", err)
		}
		if err := rows.Close(); err != nil {
			return "", false, err
		}
		switch len(mappings) {
		case 0:
			return current, mapped, nil
		case 1:
			// Validate the whole repair before trusting one interior mapping.
		default:
			return "", false, fmt.Errorf(
				"state: completed intent repair mapping from %s is ambiguous",
				current)
		}

		repair, ok, err := IntentRepairByID(ctx, d, mappings[0].repairID)
		if err != nil {
			return "", false, err
		}
		if !ok || !repair.OldHead.Valid || !repair.NewHead.Valid ||
			!repair.CompletedTS.Valid {
			return "", false, fmt.Errorf(
				"state: completed intent repair %s has incomplete proof",
				mappings[0].repairID)
		}
		transition := CompletedBranchTransition{
			Kind: CompletedBranchTransitionIntentRepair, ID: repair.ID,
			SourceHead: repair.OldHead.String, TargetHead: repair.NewHead.String,
			CompletedTS: repair.CompletedTS.Float64,
		}
		if _, err := completedIntentRepairProof(ctx, d, transition); err != nil {
			return "", false, err
		}
		next := mappings[0].targetOID
		if _, duplicate := seen[next]; duplicate {
			return "", false, fmt.Errorf(
				"state: completed intent repair mapping cycle at %s", next)
		}
		seen[next] = struct{}{}
		current = next
		mapped = true
	}
	return "", false, fmt.Errorf(
		"state: completed intent repair mapping exceeds %d steps",
		CompletedBranchTransitionProofLimit)
}

func completedSelfPublicationMembers(
	ctx context.Context,
	d *DB,
	publicationID string,
) ([]int64, error) {
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT event_seq
FROM self_publication_members
WHERE publication_id=?
ORDER BY ord`, publicationID)
	if err != nil {
		return nil, fmt.Errorf(
			"state: query completed publication members: %w", err)
	}
	defer rows.Close()

	var members []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf(
				"state: scan completed publication member: %w", err)
		}
		members = append(members, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"state: iterate completed publication members: %w", err)
	}
	if len(members) == 0 {
		return nil, fmt.Errorf(
			"state: completed self-publication %s has no members", publicationID)
	}
	return members, nil
}

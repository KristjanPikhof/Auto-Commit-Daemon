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
	CompletedBranchTransitionProofLimit = 64
)

// CompletedBranchTransition is one immutable, completed ACD-authored ref
// movement. EventSeqs is populated only for ordinary self-publications; an
// Intent repair rewrites already-published commits and therefore consumes no
// new capture membership.
type CompletedBranchTransition struct {
	Kind       CompletedBranchTransitionKind
	ID         string
	SourceHead string
	TargetHead string
	EventSeqs  []int64
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

func completedBranchTransitionsFrom(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	sourceHead string,
) ([]CompletedBranchTransition, error) {
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT kind,id,target_head
FROM (
    SELECT 'self_publication' AS kind,id,target_commit_oid AS target_head,
           created_ts
    FROM self_publications
    WHERE branch_ref=? AND branch_generation=? AND source_head=?
      AND phase='completed'
    UNION ALL
    SELECT 'intent_repair' AS kind,id,new_head AS target_head,created_ts
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
			&transition.Kind, &transition.ID, &transition.TargetHead); err != nil {
			return nil, fmt.Errorf(
				"state: scan completed branch transition: %w", err)
		}
		transition.SourceHead = sourceHead
		if transition.TargetHead == "" {
			return nil, errors.New(
				"state: completed branch transition has an empty target")
		}
		if transition.Kind == CompletedBranchTransitionSelfPublication {
			members, err := completedSelfPublicationMembers(
				ctx, d, transition.ID)
			if err != nil {
				return nil, err
			}
			transition.EventSeqs = members
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"state: iterate completed branch transitions: %w", err)
	}
	return transitions, nil
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

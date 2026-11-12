package ai

import "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"

// InferFinishBuildDependencies retains exact versions and provenance while
// ordering explicit new source-file prerequisites in the replay catalogue.
func InferFinishBuildDependencies(evidence finish.Evidence, diffs []FinishDiff) (finish.Evidence, error) {
	if err := finish.ValidateEvidence(evidence); err != nil {
		return evidence, err
	}
	texts := make(map[string]string, len(diffs))
	for _, diff := range diffs {
		texts[diff.ChangeID] = diff.Text
	}
	inferred := finish.WithProjectSourceDependencies(evidence, texts)
	changed := false
	for i, change := range inferred.Changes {
		changed = changed || len(change.DependsOn) != len(evidence.Changes[i].DependsOn)
	}
	if !changed {
		return evidence, nil
	}
	var goals []finish.Goal
	changes := make(map[string]finish.Change, len(inferred.Changes))
	for _, change := range inferred.Changes {
		goals = append(goals, finish.Goal{ChangeIDs: []string{change.ID}})
		changes[change.ID] = change
	}
	ordered, err := OrderFinishGoals(inferred, goals)
	if err != nil {
		return evidence, err
	}
	inferred.Changes = nil
	for _, goal := range ordered {
		inferred.Changes = append(inferred.Changes, changes[goal.ChangeIDs[0]])
	}
	return inferred, finish.ValidateEvidence(inferred)
}

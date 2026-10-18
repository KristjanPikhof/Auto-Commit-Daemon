package ai

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

// The provider chooses membership. Exact versions and explicit prerequisites
// determine replay order; conflicting groups need correction, never merging.
func orderFinishGoals(evidence finish.Evidence, goals []finish.Goal) ([]finish.Goal, error) {
	order := make(map[string]int, len(evidence.Changes))
	for index, change := range evidence.Changes {
		order[change.ID] = index
	}
	owners := make(map[string]int, len(evidence.Changes))
	var membershipErrors []error
	for index, goal := range goals {
		for _, id := range goal.ChangeIDs {
			if _, exists := order[id]; !exists {
				membershipErrors = append(membershipErrors, fmt.Errorf("goal %d: unknown change %q", index+1, id))
				continue
			}
			if prior, exists := owners[id]; exists {
				membershipErrors = append(membershipErrors, fmt.Errorf("multiply assigned change %q in goals %d and %d; keep it in exactly one goal", id, prior+1, index+1))
				continue
			}
			owners[id] = index
		}
		slices.SortStableFunc(goal.ChangeIDs, func(a, b string) int {
			return cmp.Compare(order[a], order[b])
		})
	}
	if err := errors.Join(membershipErrors...); err != nil {
		return nil, err
	}
	// Each consumer records the goal it needs first and a change explaining why.
	requires := make([]map[int]string, len(goals))
	for index := range requires {
		requires[index] = make(map[int]string)
	}
	previous := make(map[string]int)
	for _, change := range evidence.Changes {
		owner, selected := owners[change.ID]
		if !selected {
			continue // Build still proves omitted versions are neutral.
		}
		if prior, exists := previous[change.Path]; exists && prior != owner {
			requires[owner][prior] = change.ID
		}
		previous[change.Path] = owner
		for _, id := range change.DependsOn {
			if prior, selected := owners[id]; selected && prior != owner {
				requires[owner][prior] = change.ID
			}
		}
	}
	var ordered []finish.Goal
	used := make([]bool, len(goals))
	for len(ordered) < len(goals) {
		ready := -1
		for index := range goals {
			if !used[index] && len(requires[index]) == 0 {
				ready = index
				break
			}
		}
		if ready < 0 {
			var conflicts []string
			for consumer := range goals {
				for prerequisite := range goals {
					if id, exists := requires[consumer][prerequisite]; exists {
						conflicts = append(conflicts, fmt.Sprintf("goal %d needs goal %d before change %q", consumer+1, prerequisite+1, id))
					}
				}
			}
			return nil, fmt.Errorf("goal groups interleave required versions: %s; regroup the conflicting changes into complete steps, keeping unpublished corrections with the change they correct", strings.Join(conflicts, "; "))
		}
		ordered = append(ordered, goals[ready])
		used[ready] = true
		for _, dependencies := range requires {
			delete(dependencies, ready)
		}
	}
	return ordered, nil
}

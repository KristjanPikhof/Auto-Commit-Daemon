package ai

import (
	"fmt"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

type finishPathOwnership struct {
	Goal  int      `json:"goal"`
	Paths []string `json:"paths"`
}

// Selectors expand against the frozen catalogue. Explicit change IDs take
// precedence over files, then the nearest matching directory supplies a default.
func expandFinishPathOwnership(changes []finish.Change, goals []finish.Goal, assignments []finishPathOwnership) error {
	known := make(map[string]bool)
	for _, change := range changes {
		known[change.Path] = true
		for i, b := range change.Path {
			if b == '/' {
				known[change.Path[:i+1]] = true
			}
		}
	}
	selected := make(map[string]bool, len(changes))
	for _, goal := range goals {
		for _, id := range goal.ChangeIDs {
			selected[id] = true
		}
	}
	owners := make(map[string]int)
	for _, assignment := range assignments {
		if len(assignment.Paths) == 0 {
			continue
		}
		if assignment.Goal < 1 || assignment.Goal > len(goals) {
			return fmt.Errorf("path_ownership for %q names goal %d outside 1 through %d", assignment.Paths[0], assignment.Goal, len(goals))
		}
		for _, path := range assignment.Paths {
			if !known[path] {
				return fmt.Errorf("path_ownership selector %q matches no recorded changes", path)
			}
			if prior := owners[path]; prior != 0 && prior != assignment.Goal {
				return fmt.Errorf("path_ownership selector %q has conflicting goals %d and %d", path, prior, assignment.Goal)
			}
			owners[path] = assignment.Goal
		}
	}
	for _, change := range changes {
		if selected[change.ID] {
			continue
		}
		owner := owners[change.Path]
		for prefix := change.Path; owner == 0; {
			slash := strings.LastIndexByte(prefix, '/')
			if slash < 0 {
				break
			}
			owner = owners[prefix[:slash+1]]
			prefix = prefix[:slash]
		}
		if owner != 0 {
			goal := &goals[owner-1]
			goal.ChangeIDs = append(goal.ChangeIDs, change.ID)
		}
	}
	return nil
}

func namesRecordedConflict(changes []finish.Change, ids []string) bool {
	if len(ids) == 0 {
		return false
	}
	known := make(map[string]bool, len(changes))
	for _, change := range changes {
		known[change.ID] = true
	}
	for _, id := range ids {
		if !known[id] {
			return false
		}
	}
	return true
}

// Point out unassigned files in the correction. Build still proves partial
// assignments and omitted neutral cycles against the complete frozen target.
func finishPathCoverage(evidence finish.Evidence, goals []finish.Goal) error {
	missing := unassignedFinishPaths(evidence, goals)
	if len(missing) > 0 {
		for i, path := range missing {
			missing[i] = fmt.Sprintf("%q", path)
		}
		return fmt.Errorf("no goal covers these changed files: %s; assign each using path_ownership or exact change IDs", strings.Join(missing, ", "))
	}
	return nil
}

func unassignedFinishPaths(evidence finish.Evidence, goals []finish.Goal) []string {
	selected := map[string]bool{}
	for _, goal := range goals {
		for _, id := range goal.ChangeIDs {
			selected[id] = true
		}
	}
	covered := map[string]bool{}
	for _, change := range evidence.Changes {
		if selected[change.ID] {
			covered[change.Path] = true
		}
	}
	var missing []string
	for _, change := range evidence.Changes {
		if !covered[change.Path] && evidence.Base[change.Path] != evidence.Target[change.Path] {
			covered[change.Path] = true
			missing = append(missing, change.Path)
		}
	}
	return missing
}

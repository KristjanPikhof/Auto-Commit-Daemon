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

// Selectors expand only against the frozen change catalogue. A directory ends
// with a slash so similarly named files and directories cannot match it.
func expandFinishPathOwnership(changes []finish.Change, goals []finish.Goal, assignments []finishPathOwnership) error {
	byPath := make(map[string][]string)
	for _, change := range changes {
		byPath[change.Path] = append(byPath[change.Path], change.ID)
		for i, b := range change.Path {
			if b == '/' {
				prefix := change.Path[:i+1]
				byPath[prefix] = append(byPath[prefix], change.ID)
			}
		}
	}
	selected := make(map[string]bool, len(changes))
	for _, goal := range goals {
		for _, id := range goal.ChangeIDs {
			selected[id] = true
		}
	}
	for _, assignment := range assignments {
		if assignment.Goal < 1 || assignment.Goal > len(goals) || len(assignment.Paths) == 0 {
			return fmt.Errorf("path_ownership requires a valid goal number and at least one path")
		}
		goal := &goals[assignment.Goal-1]
		for _, path := range assignment.Paths {
			ids := byPath[path]
			if len(ids) == 0 {
				return fmt.Errorf("path_ownership selector %q matches no recorded changes", path)
			}
			for _, id := range ids {
				if selected[id] {
					return fmt.Errorf("path_ownership selector %q overlaps assigned change %q", path, id)
				}
				selected[id] = true
			}
			goal.ChangeIDs = append(goal.ChangeIDs, ids...)
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

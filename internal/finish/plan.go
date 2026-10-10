// Package finish describes explicit completion plans using recorded file versions.
// It owns no repository, provider, queue, or durable lifecycle state.
package finish

// Entry is an exact Git leaf. The zero value represents an absent path in a
// recorded change, never an entry in a complete tree.
type Entry struct {
	Mode string `json:"mode"`
	OID  string `json:"oid"`
}

type Tree map[string]Entry

// Change is a recorded before/after version. Dependencies name changes that
// must precede it; an AtomicGroup keeps rename companions in the same goal.
// Author is an opaque local boundary ID, not provider-visible personal data.
type Change struct {
	ID          string   `json:"id"`
	Path        string   `json:"path"`
	Before      Entry    `json:"before"`
	After       Entry    `json:"after"`
	Author      string   `json:"author,omitempty"`
	DependsOn   []string `json:"depends_on,omitempty"`
	AtomicGroup string   `json:"atomic_group,omitempty"`
	Provenance  string   `json:"provenance,omitempty"`
}

// Evidence is supplied by the caller from immutable snapshots or history.
// Changes retain recorded order; they need not become individual commits.
type Evidence struct {
	Base    Tree     `json:"base"`
	Target  Tree     `json:"target"`
	Changes []Change `json:"changes"`
}

// Goal explains one complete step before assigning its recorded changes.
// Boundary explains why a split is useful to review or revert.
type Goal struct {
	Purpose   string   `json:"purpose"`
	Boundary  string   `json:"boundary,omitempty"`
	Message   string   `json:"message"`
	ChangeIDs []string `json:"change_ids"`
}

type Step struct {
	Goal
	Tree   Tree   `json:"tree"`
	Author string `json:"author,omitempty"`
}

type Plan struct {
	Base   Tree   `json:"base"`
	Target Tree   `json:"target"`
	Steps  []Step `json:"steps"`
}

func CloneTree(tree Tree) Tree {
	copy := make(Tree, len(tree))
	for path, entry := range tree {
		copy[path] = entry
	}
	return copy
}

// NetChanges records the exact batch diff when there are no finer immutable
// versions to offer. Sorting paths stabilizes evidence without defining commits.
func NetChanges(base, target Tree, author string) []Change {
	paths := changedPaths(base, target)
	changes := make([]Change, 0, len(paths))
	for _, path := range paths {
		changes = append(changes, Change{
			ID: path, Path: path, Before: base[path], After: target[path], Author: author,
		})
	}
	return changes
}

// Build materializes goals in memory from exact versions. Git objects and
// project verification remain the caller's responsibility.
func Build(evidence Evidence, goals []Goal) (Plan, error) {
	plan := Plan{Base: CloneTree(evidence.Base), Target: CloneTree(evidence.Target)}
	changes, err := validateEvidence(evidence)
	if err != nil {
		return Plan{}, err
	}
	current := CloneTree(evidence.Base)
	for _, goal := range goals {
		step := Step{Goal: goal}
		step.ChangeIDs = append([]string(nil), goal.ChangeIDs...)
		for _, id := range goal.ChangeIDs {
			change, ok := changes[id]
			if !ok {
				return Plan{}, planError("unknown change %q", id)
			}
			if err := apply(current, change); err != nil {
				return Plan{}, err
			}
			step.Author = change.Author
		}
		step.Tree = CloneTree(current)
		plan.Steps = append(plan.Steps, step)
	}
	if err := Validate(evidence, plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

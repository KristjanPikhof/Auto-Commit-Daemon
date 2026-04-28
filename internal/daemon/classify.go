// classify.go diffs a captured live worktree map against the persisted
// shadow_paths table and emits create/modify/delete/mode/rename ops per §8.2.
//
// Rename heuristic: when the (oid, mode) signature of a deletion uniquely
// matches a single creation (no other create shares it), we pair them as a
// rename. Multi-match collisions fall back to plain create+delete because
// guessing wrong would produce a misleading commit history.
package daemon

import "sort"

// LiveEntry is one path in the live worktree snapshot. Mirrors the legacy
// daemon's `live[rel] = {path, mode, oid}` shape.
type LiveEntry struct {
	Path string
	Mode string // git mode bits ("100644", "100755", "120000")
	OID  string // blob OID
}

// ShadowEntry is one row from shadow_paths reduced to the fields classify
// actually consumes. Avoids leaking sql.Null* into the diff function.
type ShadowEntry struct {
	Path string
	Mode string
	OID  string
}

// ClassifiedOp is one op emitted by Classify. The output is consumed by the
// capture writer (which persists capture_events + capture_ops) and later by
// the replay step (which feeds it through update-index --index-info).
type ClassifiedOp struct {
	Op         string // "create" | "modify" | "delete" | "mode" | "rename"
	Path       string
	OldPath    string
	BeforeOID  string
	BeforeMode string
	AfterOID   string
	AfterMode  string
	Fidelity   string // "rescan" for poll-driven capture
}

// Classify compares a shadow snapshot against a live snapshot and returns
// the ordered list of ops. Output is deterministic: rename ops appear first
// (driven by deletion order), followed by create/modify/mode ops sorted by
// path, then plain delete ops sorted by path. This matches the legacy
// _classify_changes output ordering, which downstream tests pin.
func Classify(shadow map[string]ShadowEntry, live map[string]LiveEntry) []ClassifiedOp {
	const fidelity = "rescan"

	// Sets of paths that exist on each side. Intersection drives modify/mode
	// detection; symmetric difference drives create/delete + rename pairing.
	var deletes []ShadowEntry // present in shadow, missing from live
	var creates []LiveEntry   // present in live, missing from shadow

	for p, s := range shadow {
		if _, ok := live[p]; !ok {
			deletes = append(deletes, s)
		}
	}
	for p, l := range live {
		if _, ok := shadow[p]; !ok {
			creates = append(creates, l)
		}
	}

	// Deterministic deletion order so rename pairing is reproducible across
	// runs (Go map iteration is randomized).
	sort.Slice(deletes, func(i, j int) bool { return deletes[i].Path < deletes[j].Path })
	sort.Slice(creates, func(i, j int) bool { return creates[i].Path < creates[j].Path })

	// Build a (oid|mode) -> []create index. Only signatures with exactly one
	// create become rename targets.
	type sig struct{ oid, mode string }
	createsBySig := make(map[sig][]int, len(creates))
	for i, c := range creates {
		key := sig{c.OID, c.Mode}
		createsBySig[key] = append(createsBySig[key], i)
	}

	pairedCreates := make(map[int]bool, len(creates))
	pairedDeletes := make(map[string]bool, len(deletes))

	var out []ClassifiedOp

	// First pass: rename pairing.
	for _, d := range deletes {
		key := sig{d.OID, d.Mode}
		matches := createsBySig[key]
		if len(matches) != 1 {
			continue
		}
		ci := matches[0]
		if pairedCreates[ci] {
			continue
		}
		c := creates[ci]
		pairedCreates[ci] = true
		pairedDeletes[d.Path] = true
		out = append(out, ClassifiedOp{
			Op:         "rename",
			Path:       c.Path,
			OldPath:    d.Path,
			BeforeOID:  d.OID,
			BeforeMode: d.Mode,
			AfterOID:   c.OID,
			AfterMode:  c.Mode,
			Fidelity:   fidelity,
		})
	}

	// Second pass: walk live in path order, emit create/modify/mode for paths
	// that aren't already part of a rename pairing.
	livePaths := make([]string, 0, len(live))
	for p := range live {
		livePaths = append(livePaths, p)
	}
	sort.Strings(livePaths)
	for _, path := range livePaths {
		l := live[path]
		s, hadShadow := shadow[path]
		if !hadShadow {
			// New path. Only emit create when not paired off as a rename.
			alreadyPaired := false
			for ci, c := range creates {
				if c.Path == path && pairedCreates[ci] {
					alreadyPaired = true
					break
				}
			}
			if alreadyPaired {
				continue
			}
			out = append(out, ClassifiedOp{
				Op:        "create",
				Path:      path,
				AfterOID:  l.OID,
				AfterMode: l.Mode,
				Fidelity:  fidelity,
			})
			continue
		}
		// Path present in both: detect modify (oid changed) or mode change.
		if s.OID != l.OID {
			out = append(out, ClassifiedOp{
				Op:         "modify",
				Path:       path,
				BeforeOID:  s.OID,
				BeforeMode: s.Mode,
				AfterOID:   l.OID,
				AfterMode:  l.Mode,
				Fidelity:   fidelity,
			})
		} else if s.Mode != l.Mode {
			out = append(out, ClassifiedOp{
				Op:         "mode",
				Path:       path,
				BeforeOID:  s.OID,
				BeforeMode: s.Mode,
				AfterOID:   l.OID,
				AfterMode:  l.Mode,
				Fidelity:   fidelity,
			})
		}
	}

	// Third pass: plain deletes (those not paired off as renames), in path
	// order. Sort already done above on `deletes`.
	for _, d := range deletes {
		if pairedDeletes[d.Path] {
			continue
		}
		out = append(out, ClassifiedOp{
			Op:         "delete",
			Path:       d.Path,
			BeforeOID:  d.OID,
			BeforeMode: d.Mode,
			Fidelity:   fidelity,
		})
	}

	return out
}

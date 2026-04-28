// message.go is the daemon-side adapter onto the Phase 5 ai package.
//
// Phase 1 owned a local rule-based generator; Phase 5 (this lane) moved
// the canonical implementation into internal/ai/deterministic.go so the
// replay path can swap providers without code churn here. This file is
// now a thin wrapper that:
//   1. translates the daemon's EventContext into ai.CommitContext;
//   2. invokes ai.DeterministicProvider.Generate;
//   3. composes the resulting Result.Subject + Result.Body into the
//      single-string message MessageFn returns.
//
// Output is **byte-identical** to the previous Phase 1 implementation:
// single-op events produce just the subject, multi-op events produce
// `subject + "\n\n" + bullets`. Existing replay tests pin the subject
// shape and continue to pass unchanged.
package daemon

import (
	"context"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

// DeterministicMessage produces a commit subject + optional body from the
// event + ops alone. Pure forwarder over ai.DeterministicProvider.
func DeterministicMessage(ctx context.Context, ec EventContext) (string, error) {
	cc := commitContextFromEvent(ec)
	r, err := (ai.DeterministicProvider{}).Generate(ctx, cc)
	if err != nil {
		return "", err
	}
	if r.Body == "" {
		return r.Subject, nil
	}
	return r.Subject + "\n\n" + r.Body, nil
}

// commitContextFromEvent translates the daemon's EventContext into the
// ai package's CommitContext. Multi-op events are flattened into MultiOp;
// single-op events populate the top-level Path/Op/OldPath fields so the
// deterministic generator can take the single-op path.
func commitContextFromEvent(ec EventContext) ai.CommitContext {
	cc := ai.CommitContext{
		Branch: ec.Event.BranchRef,
	}
	switch len(ec.Ops) {
	case 0:
		// no-op — Generator returns "Update files".
	case 1:
		op := ec.Ops[0]
		cc.Path = op.Path
		cc.Op = op.Op
		if op.OldPath.Valid {
			cc.OldPath = op.OldPath.String
		}
	default:
		cc.MultiOp = make([]ai.OpItem, 0, len(ec.Ops))
		for _, op := range ec.Ops {
			old := ""
			if op.OldPath.Valid {
				old = op.OldPath.String
			}
			cc.MultiOp = append(cc.MultiOp, ai.OpItem{
				Path:    op.Path,
				Op:      op.Op,
				OldPath: old,
			})
		}
	}
	return cc
}

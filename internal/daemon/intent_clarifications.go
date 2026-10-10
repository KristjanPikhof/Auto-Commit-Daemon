package daemon

import (
	"context"
	"fmt"
	"go/scanner"
	"go/token"
	"path"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// Protected explanatory updates can clarify a frozen target without adding
// implementation to it. Matching Go tokens and rejecting compiler directives
// keep this context separate from code that still belongs to later publication.
func loadProtectedIntentClarifications(ctx context.Context, db *state.DB, input IntentCandidateEvaluation, captures []IntentCandidateCapture) ([]IntentCandidateCapture, error) {
	if len(input.TargetEventSeqs) == 0 || !input.IncludeDiffs {
		return captures, nil
	}
	for i := range captures {
		capture := &captures[i]
		if capture.Event.State != state.EventStatePending || path.Ext(capture.Event.Path) != ".go" || len(capture.Ops) != 1 {
			continue
		}
		op := capture.Ops[0]
		if !op.AfterOID.Valid || op.AfterMode.String != "100644" {
			continue
		}
		var seq int64
		var oid string
		err := db.ReadSQL().QueryRowContext(ctx, `
SELECT event.seq,operation.after_oid FROM (
 SELECT seq FROM capture_events WHERE branch_ref=? AND branch_generation=? AND seq>? AND state='pending'
 ORDER BY seq LIMIT 32
) later JOIN capture_events event ON event.seq=later.seq
JOIN capture_ops operation ON operation.event_seq=event.seq
WHERE operation.path=? AND operation.before_oid=? AND operation.before_mode='100644' AND operation.after_mode='100644'
 AND EXISTS(SELECT 1 FROM checkpoint_events member JOIN checkpoints checkpoint ON checkpoint.id=member.checkpoint_id
  WHERE member.event_seq=event.seq AND checkpoint.phase='completed' AND checkpoint.retained=1 AND checkpoint.coverage_complete=1)
ORDER BY event.seq LIMIT 1`, input.BranchRef, input.BranchGeneration, capture.Event.Seq, op.Path, op.AfterOID.String).Scan(&seq, &oid)
		if err != nil {
			continue
		}
		before, err := git.RunWithLimit(ctx, git.RunOpts{Dir: input.RepoPath}, intentSourceReferenceScanCap, "cat-file", "blob", op.AfterOID.String)
		if err != nil {
			continue
		}
		after, err := git.RunWithLimit(ctx, git.RunOpts{Dir: input.RepoPath}, intentSourceReferenceScanCap, "cat-file", "blob", oid)
		if err != nil {
			continue
		}
		oldTokens, oldOK := intentClarificationGoTokens(before)
		newTokens, newOK := intentClarificationGoTokens(after)
		if !oldOK || !newOK || oldTokens != newTokens {
			continue
		}
		ops, err := state.LoadCaptureOps(ctx, db, seq)
		if err != nil {
			return nil, err
		}
		diff, err := BuildOpsDiffWithCap(ctx, input.RepoPath, ops, 2048)
		if err != nil {
			return nil, err
		}
		clarification := fmt.Sprintf("Protected explanatory capture %d has identical Go tokens to this target. Its comments clarify intent; its contents are not selected for publication:\n%s", seq, diff)
		capture.CapturedDiff = prependIntentRecordedReferenceContext(capture.CapturedDiff, clarification)
	}
	return captures, nil
}

func intentClarificationGoTokens(source []byte) (string, bool) {
	var lex scanner.Scanner
	valid := true
	lex.Init(token.NewFileSet().AddFile("capture.go", -1, len(source)), source, func(token.Position, string) { valid = false }, scanner.ScanComments)
	var out strings.Builder
	for {
		_, kind, literal := lex.Scan()
		if kind == token.EOF {
			return out.String(), valid
		}
		if kind == token.COMMENT {
			if strings.HasPrefix(literal, "//go:") || strings.HasPrefix(literal, "// +build") || strings.HasPrefix(literal, "//line") || strings.Contains(literal, "#cgo") {
				return "", false
			}
			continue
		}
		if kind == token.STRING && literal == `"C"` {
			return "", false
		}
		fmt.Fprintf(&out, "%d:%s\x00", kind, literal)
	}
}

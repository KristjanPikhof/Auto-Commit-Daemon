package daemon

import acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"

func recordTrace(logger acdtrace.Logger, ev acdtrace.Event) {
	if logger == nil {
		return
	}
	logger.Record(ev)
}

func traceSeedDecision(rows int) string {
	if rows > 0 {
		return "seeded"
	}
	return "skip"
}

func traceBootstrapShadow(logger acdtrace.Logger, repoRoot string, cctx CaptureContext, decision, reason string, rows int) {
	recordTrace(logger, acdtrace.Event{
		Repo:       repoRoot,
		BranchRef:  cctx.BranchRef,
		HeadSHA:    cctx.BaseHead,
		EventClass: "bootstrap_shadow.reseed",
		Decision:   decision,
		Reason:     reason,
		Output:     map[string]any{"rows": rows},
		Generation: cctx.BranchGeneration,
	})
}

func traceErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

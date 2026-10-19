package daemon

import acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"

func recordTrace(logger acdtrace.Logger, ev acdtrace.Event) {
	if logger == nil {
		return
	}
	logger.Record(ev)
}

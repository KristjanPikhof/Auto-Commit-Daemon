package cli

import "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"

func newIntentActivityBoundary(
	kind string,
	source string,
) state.IntentActivityBoundary {
	return state.IntentActivityBoundary{
		Kind:   kind,
		Source: source,
	}
}

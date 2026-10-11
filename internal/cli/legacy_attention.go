package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// Full evidence remains in JSON diagnostics; routine human output stays bounded.
func legacyAttentionSummary(reason string) string {
	lines := strings.Split(reason, "\n")
	if len(lines) > 3 {
		reason = strings.Join(lines[:3], "; ") + "; more details in `acd support diagnose --json`."
	}
	return sanitizeObservabilityText(reason)
}

func legacyAttentionAction(legacy state.FinishLegacyCutover) string {
	switch legacy.ReasonCode {
	case "legacy_object_missing", "legacy_inventory_invalid", "legacy_proof_ambiguous", "legacy_publication_ambiguous", "legacy_repair_ambiguous", "legacy_archive_unreadable":
		return "Run acd support diagnose --json to inspect older saved data. Current protection, finish, and history rewrite remain available."
	default:
		return "Run acd setup to review older saved data. Current protection, finish, and history rewrite remain available."
	}
}

func renderLegacyAttention(out io.Writer, legacy state.FinishLegacyCutover) {
	fmt.Fprintln(out, "Older saved data needs review. Current protection, finish, and history rewrite remain available.")
	fmt.Fprintln(out, "For details: Run acd support diagnose --json.")
}

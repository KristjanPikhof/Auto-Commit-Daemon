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
		return "Inspect `acd support diagnose --json` to recover the archived proof. Archived errors do not block current protection, finish, or history rewrite."
	default:
		return "Run `acd setup` to inspect the preserved legacy archive. Archived errors do not block current protection, finish, or history rewrite."
	}
}

func renderLegacyAttention(out io.Writer, legacy state.FinishLegacyCutover) {
	fmt.Fprintf(out, "Legacy attention: %s %s\n", legacyAttentionSummary(legacy.Reason), legacyAttentionAction(legacy))
}

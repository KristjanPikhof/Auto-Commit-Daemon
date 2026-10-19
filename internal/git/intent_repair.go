package git

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
)

// IntentRepairBackupRef returns the private, deterministic namespace for a
// repair backup without exposing branch names in refs.
func IntentRepairBackupRef(branchRef, repairID string) (string, error) {
	if !isFullBranchRef(branchRef) {
		return "", fmt.Errorf("git intent repair backup: invalid branch ref %q", branchRef)
	}
	repairID = sanitizeRewriteRefPart(strings.TrimSpace(repairID))
	if repairID == "" || repairID == "unknown" {
		return "", errors.New("git intent repair backup: invalid repair id")
	}
	sum := sha256.Sum256([]byte(branchRef))
	ref := fmt.Sprintf("refs/acd/intent-repair/%x/%s/backup", sum[:8], repairID)
	if _, err := Run(context.Background(), RunOpts{Timeout: DefaultReadTimeout}, "check-ref-format", ref); err != nil {
		return "", fmt.Errorf("git intent repair backup: invalid generated ref: %w", err)
	}
	return ref, nil
}

func isFullBranchRef(ref string) bool {
	return strings.HasPrefix(ref, "refs/heads/") &&
		len(ref) > len("refs/heads/") &&
		!strings.ContainsAny(ref, " \t\r\n\x00")
}

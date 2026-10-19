package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

// Reproduce files left by older runtimes; production only deletes this format.
func legacyStartCachePath(gitDir, sessionID string) string {
	suffix := "empty"
	if sessionID != "" {
		sum := sha256.Sum256([]byte(sessionID))
		suffix = hex.EncodeToString(sum[:8])
	}
	return filepath.Join(gitDir, "acd", startCacheFilenamePrefix+suffix+".json")
}

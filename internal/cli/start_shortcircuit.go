package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

// Retained only to remove cache files left by pre-supervisor runtimes.
const startCacheFilenamePrefix = "start-cache-"

func startCachePath(gitDir, sessionID string) string {
	return filepath.Join(gitDir, "acd", startCacheFilenamePrefix+sessionCacheSuffix(sessionID)+".json")
}

// sessionCacheSuffix is the 16-hex prefix of sha256(session_id). We only
// take 16 hex chars (64 bits) which is enough to make accidental
// collisions astronomically unlikely while keeping the filename short.
func sessionCacheSuffix(sessionID string) string {
	if sessionID == "" {
		return "empty"
	}
	sum := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(sum[:8])
}

package paths

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
)

// repoHashLen is the truncation point for the repo short-hash.
//
// Plan §6.4 / §13.1 examples show 8-char hashes ("a1b2c3d4") in JSON
// payloads, but those are illustrative — actual collision resistance at
// 8 hex chars (32 bits) is too weak for a global registry that may grow
// over time. We use 12 hex chars (48 bits) which keeps the path short
// enough to scan visually and still gives ~2.8e14 distinct values, well
// past the registry's expected scale.
const repoHashLen = 12

// RepoHash returns a stable short hash for a repository's absolute path.
// The input is canonicalized via filepath.Clean so trailing slashes,
// `.` segments, and similar noise don't shift the hash.
//
// The hash is sha256(cleanedAbsPath) truncated to the first 12 hex
// characters. It MUST be deterministic across processes, machines, and
// `acd` versions: the central stats DB joins by repo_hash even when the
// repo has been moved on disk.
//
// An empty or non-absolute path is rejected so callers can't accidentally
// conflate repos by passing in a relative or "" path.
func RepoHash(absRepoPath string) (string, error) {
	if absRepoPath == "" {
		return "", errors.New("paths: repo path must not be empty")
	}
	if !filepath.IsAbs(absRepoPath) {
		return "", errors.New("paths: repo path must be absolute")
	}
	cleaned := filepath.Clean(absRepoPath)
	sum := sha256.Sum256([]byte(cleaned))
	return hex.EncodeToString(sum[:])[:repoHashLen], nil
}

// MustRepoHash is the panicking variant for callers (mostly tests +
// startup) that have already validated the input.
func MustRepoHash(absRepoPath string) string {
	h, err := RepoHash(absRepoPath)
	if err != nil {
		panic(err)
	}
	return h
}

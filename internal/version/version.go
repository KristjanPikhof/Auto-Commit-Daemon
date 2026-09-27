package version

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"regexp"
	"strconv"
)

var describedVersion = regexp.MustCompile(`^v(\d{4})-(\d{2})-(\d{2})(?:-(\d+)-g[0-9a-f]+)?(-dirty)?(?:\s|$)`)

// These are populated by -ldflags at build time.
var (
	Version = "dev"
	GitSHA  = "unknown"
)

// String returns a single human-readable version line.
func String() string {
	return Version + " (" + GitSHA + ")"
}

// FileDigest identifies the exact executable bytes independently of build
// metadata. This lets local dirty builds replace an older compatible runtime.
func FileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// Compare reports whether left is older (-1), equal (0), or newer (1) than
// right when both use ACD's git-describe version format. Different commits at
// the same release distance are intentionally incomparable.
func Compare(left, right string) (int, bool) {
	l, ok := parse(left)
	if !ok {
		return 0, false
	}
	r, ok := parse(right)
	if !ok {
		return 0, false
	}
	for i := range 4 {
		if l[i] < r[i] {
			return -1, true
		}
		if l[i] > r[i] {
			return 1, true
		}
	}
	if l[4] != r[4] {
		if l[4] > r[4] {
			return 1, true
		}
		return -1, true
	}
	return 0, true
}

func parse(value string) ([5]int, bool) {
	match := describedVersion.FindStringSubmatch(value)
	if match == nil {
		return [5]int{}, false
	}
	var parsed [5]int
	for i := range 4 {
		if match[i+1] == "" {
			continue
		}
		part, err := strconv.Atoi(match[i+1])
		if err != nil {
			return [5]int{}, false
		}
		parsed[i] = part
	}
	if match[5] != "" {
		parsed[4] = 1
	}
	return parsed, true
}

package daemon

import (
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
)

// FingerprintToken identifies the exact process recorded by the worker.
func FingerprintToken(fp identity.Fingerprint) string {
	if fp.Empty() {
		return ""
	}
	return fp.StartTime + "|" + fp.ArgvHash
}

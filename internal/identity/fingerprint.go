package identity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Fingerprint binds a pid to OS-level identity that PID-reuse cannot
// reproduce: process start time + a hash over the original argv vector.
//
// PID recycling can hand the same numeric pid to an unrelated process
// seconds after the original exits. Without a fingerprint, the daemon
// would happily refresh refcount entries for the new occupant. By stamping
// start time at registration and re-checking it on every GC sweep, the
// recycled pid fails Match and the row is evicted. See §3.4 case 3.
type Fingerprint struct {
	// StartTime is the process boot time, taken verbatim from `ps`'s
	// `lstart` column (e.g. "Mon Apr 28 14:22:13 2026"). Stored as a
	// string rather than time.Time so we don't perturb the legacy format
	// the operator sees in `ps` output.
	StartTime string

	// ArgvHash is sha256(argv[0]\x00argv[1]\x00...) hex-encoded. The NUL
	// separator is the same convention `/proc/<pid>/cmdline` uses on
	// Linux and avoids ambiguity around argv elements containing spaces.
	ArgvHash string
}

// fingerprintTimeout bounds how long we wait on `ps` before giving up.
// `ps` is part of the base system on darwin and linux but networked
// container fs / forensics edge cases can hang it; a 2s ceiling matches
// the legacy daemon.
const fingerprintTimeout = 2 * time.Second

// Empty reports whether fp carries any usable identity.
func (fp Fingerprint) Empty() bool {
	return fp.StartTime == "" && fp.ArgvHash == ""
}

// Capture builds a Fingerprint for the given pid by shelling out to
// `ps -p <pid> -o lstart=,command=`. This form works identically on darwin
// and linux: the trailing `=` suppresses the column header, and lstart is
// a fixed-width 24-char field so the rest of the line is the command.
//
// Returns an error if ps fails, the process is gone, or the output is
// shorter than the lstart prefix.
//
// Note: `command` is the kernel-reported argv joined by spaces. We hash
// it as one string. We can't recover the original NUL-separated vector
// from `ps`, so the hash is a deterministic function of "what ps shows"
// rather than a true argv-NUL hash. CaptureFromArgv is offered for cases
// where the caller already has the unjoined argv vector (e.g. fingerprinting
// the current process).
func Capture(pid int) (Fingerprint, error) {
	if pid <= 0 {
		return Fingerprint{}, errors.New("identity: pid must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), fingerprintTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-p", itoa(pid), "-o", "lstart=,command=").Output()
	if err != nil {
		return Fingerprint{}, err
	}
	line := strings.TrimRight(string(out), "\r\n")
	if line == "" {
		return Fingerprint{}, errors.New("identity: empty ps output")
	}
	// `lstart` is a 24-character fixed-width column: "Mon Apr 28 14:22:13 2026".
	// Anything shorter means we read a header or partial line.
	const lstartWidth = 24
	if len(line) < lstartWidth+1 {
		return Fingerprint{}, errors.New("identity: ps output too short for lstart")
	}
	startTime := strings.TrimSpace(line[:lstartWidth])
	command := strings.TrimSpace(line[lstartWidth:])
	return Fingerprint{
		StartTime: startTime,
		ArgvHash:  hashArgvJoined(command),
	}, nil
}

// CaptureSelf is a convenience for fingerprinting the running process. It
// uses os.Args directly (preserving the original NUL-joined hash) rather
// than parsing ps output for the current pid.
func CaptureSelf() (Fingerprint, error) {
	fp, err := Capture(os.Getpid())
	if err != nil {
		return Fingerprint{}, err
	}
	// Override the argv hash with the in-process argv: we have access to
	// the unjoined vector here, which is more accurate than the spaced
	// `command` rendering ps gives us.
	fp.ArgvHash = HashArgv(os.Args)
	return fp, nil
}

// HashArgv returns sha256(argv[0]\x00argv[1]\x00...) as a hex string.
//
// NUL separation matches the on-disk format of `/proc/<pid>/cmdline` and
// removes ambiguity for argv elements containing spaces.
func HashArgv(argv []string) string {
	h := sha256.New()
	for i, s := range argv {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(s))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// hashArgvJoined treats the input as a single pre-joined string. Used for
// `ps`-derived fingerprints where we no longer have the unsplit argv.
func hashArgvJoined(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// Match returns true when both fingerprints carry usable identity AND
// agree on every component. An empty fingerprint never matches: refusing
// to confirm identity is the safe default when we have no prior stamp,
// per legacy verify_process_identity.
func Match(a, b Fingerprint) bool {
	if a.Empty() || b.Empty() {
		return false
	}
	return a.StartTime == b.StartTime && a.ArgvHash == b.ArgvHash
}

// itoa is a tiny strconv.Itoa shim to avoid pulling strconv into a file
// that uses no other strconv functions; it keeps the import surface flat.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

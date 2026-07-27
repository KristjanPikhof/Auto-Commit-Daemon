// lock.go provides per-repo flock primitives:
//
//   - <git-common-dir>/acd-daemon.lock — held exclusively by the live daemon
//     for its entire run. The stable location keeps ownership fenced if the
//     movable <git-dir>/acd state directory is replaced and makes linked
//     worktrees contend as one repository.
//   - every existing worktree's <git-dir>/acd/daemon.lock, plus discovered
//     immediately relocated legacy locks — acquired after the stable lock for
//     compatibility with older ACD versions.
//     Contention on either lock => another daemon is already alive; the caller
//     exits with EX_TEMPFAIL (75) so wrappers can distinguish "peer running"
//     from "started cleanly".
//   - control.lock — held briefly by `acd start`/`stop`/`wake`/`touch` to
//     serialize read-modify-write of daemon_clients. The daemon itself does
//     NOT hold this except during sweeps where the GC needs an atomic view
//     of the table. Brief acquisition/release pattern.
//
// Implementation uses syscall.Flock (no cgo). LOCK_EX | LOCK_NB returns
// immediately if the lock is contended, which is what every caller wants.
package daemon

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
)

// ErrDaemonLockHeld is returned by AcquireDaemonLock when another daemon
// already holds the per-repo daemon.lock. Callers can check via errors.Is
// to map onto EX_TEMPFAIL (75).
var ErrDaemonLockHeld = errors.New("daemon: daemon.lock held by another process")

// ErrControlLockHeld is the equivalent for the brief control lock used by
// CLI subcommands.
var ErrControlLockHeld = errors.New("daemon: control.lock held by another process")

// ExitTempFail is the EX_TEMPFAIL exit code from sysexits.h. Callers that
// observe ErrDaemonLockHeld should exit with this code.
const ExitTempFail = 75

// DaemonLock is the held-for-life-of-daemon flock handle.
type DaemonLock struct {
	stable *os.File
	legacy []*os.File
}

// AcquireDaemonLock first acquires <git-common-dir>/acd-daemon.lock, then all
// existing legacy worktree locks, all with LOCK_EX|LOCK_NB. Linked worktree Git
// dirs identify their common dir through Git's commondir file. Keeping the
// stable lock outside the replaceable acd state directory prevents a rename or
// deletion of that directory from creating a second new-version lock inode.
//
// Returns an error matching ErrDaemonLockHeld on contention; any other error is
// a hard failure that callers should surface verbatim.
func AcquireDaemonLock(gitDir string) (*DaemonLock, error) {
	if gitDir == "" {
		return nil, fmt.Errorf("daemon: AcquireDaemonLock: empty gitDir")
	}
	gitDir, commonDir, err := daemonLockDirs(gitDir)
	if err != nil {
		return nil, err
	}

	stablePath := filepath.Join(commonDir, "acd-daemon.lock")
	stable, err := acquireFileLock(stablePath)
	if err != nil {
		if errors.Is(err, ErrDaemonLockHeld) {
			return nil, fmt.Errorf(
				"%w: stable repository lock %s is held; another ACD daemon, linked worktree, or mixed-version owner is active",
				ErrDaemonLockHeld, stablePath,
			)
		}
		return nil, fmt.Errorf("daemon: acquire stable repository lock: %w", err)
	}

	legacy, err := acquireLegacyDaemonLocks(gitDir, commonDir)
	if err != nil {
		_ = releaseFileLock(stable)
		return nil, err
	}
	if daemonLockBeforeOwnerProbe != nil {
		daemonLockBeforeOwnerProbe()
	}
	probeCtx, cancel := context.WithTimeout(
		context.Background(), daemonLockOwnerProbeTimeout)
	owners, err := daemonLockOwnerProbe(probeCtx, commonDir, os.Getpid())
	cancel()
	if err != nil {
		releaseFileLocksReverse(legacy)
		_ = releaseFileLock(stable)
		return nil, fmt.Errorf("daemon: prove absence of old daemon owners: %w", err)
	}
	if len(owners) > 0 {
		releaseFileLocksReverse(legacy)
		_ = releaseFileLock(stable)
		return nil, fmt.Errorf(
			"%w: older daemon process owners %v still identify this repository",
			ErrDaemonLockHeld, owners,
		)
	}
	return &DaemonLock{stable: stable, legacy: legacy}, nil
}

const maxLegacyDaemonLockPaths = 4096
const maxLegacyDaemonLockScanPasses = 16
const daemonLockOwnerProbeTimeout = 2 * time.Second
const maxDaemonLockOwnerCandidates = 16
const daemonOwnerWorktreeReadChunk = 128

var daemonLockAfterLegacyScan func()
var daemonLockBeforeOwnerProbe func()
var daemonLockOwnerProbe = findLegacyDaemonOwners
var daemonLockProcessList = listDaemonOwnerProcesses
var daemonOwnerWorktreeEntryLimit = maxLegacyDaemonLockPaths

func acquireLegacyDaemonLocks(gitDir, commonDir string) ([]*os.File, error) {
	var locks []*os.File
	covered := make(map[string]*os.File)
	for pass := 0; pass < maxLegacyDaemonLockScanPasses; pass++ {
		paths, err := legacyDaemonLockPaths(gitDir, commonDir)
		if err != nil {
			releaseFileLocksReverse(locks)
			return nil, err
		}
		if daemonLockAfterLegacyScan != nil {
			daemonLockAfterLegacyScan()
		}

		added := false
		for _, path := range paths {
			if lock, ok := covered[path]; ok {
				if lockFileCoversPath(path, lock) {
					continue
				}
				delete(covered, path)
			}
			if lock := findLockCoveringPath(path, locks); lock != nil {
				covered[path] = lock
				continue
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				releaseFileLocksReverse(locks)
				return nil, fmt.Errorf("daemon: mkdir legacy lock parent: %w", err)
			}
			lock, err := acquireFileLock(path)
			if err != nil {
				releaseFileLocksReverse(locks)
				if errors.Is(err, ErrDaemonLockHeld) {
					return nil, fmt.Errorf(
						"%w: legacy worktree lock %s is held; an older or concurrently starting ACD daemon may own this repository",
						ErrDaemonLockHeld, path,
					)
				}
				return nil, fmt.Errorf("daemon: acquire legacy worktree lock: %w", err)
			}
			locks = append(locks, lock)
			covered[path] = lock
			added = true
		}
		if !added {
			return locks, nil
		}
	}
	releaseFileLocksReverse(locks)
	return nil, fmt.Errorf(
		"daemon: legacy lock set did not stabilize after %d scans",
		maxLegacyDaemonLockScanPasses,
	)
}

func lockFileCoversPath(path string, lock *os.File) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	lockInfo, err := lock.Stat()
	return err == nil && os.SameFile(info, lockInfo)
}

func findLockCoveringPath(path string, locks []*os.File) *os.File {
	for _, lock := range locks {
		if lockFileCoversPath(path, lock) {
			return lock
		}
	}
	return nil
}

type daemonOwnerProcess struct {
	pid     int
	command string
}

func findLegacyDaemonOwners(
	ctx context.Context,
	commonDir string,
	excludePID int,
) ([]int, error) {
	processes, err := daemonLockProcessList(ctx)
	if err != nil {
		return nil, err
	}
	knownRoots, err := daemonOwnerKnownRoots(ctx, commonDir)
	if err != nil {
		return nil, err
	}
	identityCache := make(map[string]string)
	var owners []int
	uniqueCandidates := 0
	for _, process := range processes {
		if process.pid == excludePID {
			continue
		}
		repo, ok := ParseDaemonRunRepoArg(process.command)
		if !ok {
			continue
		}
		canonicalRepo, err := canonicalDaemonOwnerRepoPath(repo)
		if err != nil {
			continue
		}
		if _, plausible := knownRoots[canonicalRepo]; !plausible {
			// An exact canonical worktree-root match is the cheap scope gate.
			// Stale or malformed processes for unrelated repositories must
			// not deny startup for this repository.
			continue
		}
		identity, ok := identityCache[canonicalRepo]
		if !ok {
			uniqueCandidates++
			if uniqueCandidates > maxDaemonLockOwnerCandidates {
				return nil, fmt.Errorf(
					"more than %d plausible repository owner paths",
					maxDaemonLockOwnerCandidates)
			}
			identity, err = daemonOwnerRepoIdentity(ctx, canonicalRepo)
			if err != nil {
				// A candidate that cannot be identified cannot be proved
				// unrelated after its exact known-root match, so mixed-version
				// startup must fail closed.
				return nil, fmt.Errorf(
					"resolve daemon process %d repository: %w",
					process.pid, err)
			}
			identityCache[canonicalRepo] = identity
		}
		if identity == commonDir {
			owners = append(owners, process.pid)
		}
	}
	slices.Sort(owners)
	return owners, nil
}

func daemonOwnerKnownRoots(
	ctx context.Context,
	commonDir string,
) (map[string]struct{}, error) {
	roots := make(map[string]struct{})
	if filepath.Base(commonDir) == ".git" {
		root, err := canonicalDaemonOwnerRepoPath(filepath.Dir(commonDir))
		if err != nil {
			return nil, err
		}
		roots[root] = struct{}{}
	}
	worktreesDir := filepath.Join(commonDir, "worktrees")
	dir, err := os.Open(worktreesDir)
	if errors.Is(err, os.ErrNotExist) {
		return roots, nil
	}
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	entryLimit := daemonOwnerWorktreeEntryLimit
	if entryLimit <= 0 || entryLimit > maxLegacyDaemonLockPaths {
		entryLimit = maxLegacyDaemonLockPaths
	}
	seen := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// ReadDir itself is a local filesystem syscall and cannot be
		// interrupted once entered. Small chunks plus context checks bound
		// work between cancellation observations.
		entries, readErr := dir.ReadDir(daemonOwnerWorktreeReadChunk)
		for _, entry := range entries {
			seen++
			if seen > entryLimit {
				return nil, fmt.Errorf(
					"daemon: linked worktree owner scan exceeds %d entries",
					entryLimit)
			}
			if !entry.IsDir() {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			adminDir := filepath.Join(worktreesDir, entry.Name())
			body, err := os.ReadFile(filepath.Join(adminDir, "gitdir"))
			if err != nil {
				return nil, err
			}
			gitFile := strings.TrimSpace(string(body))
			if gitFile == "" {
				return nil, fmt.Errorf(
					"empty linked worktree gitdir for %s", entry.Name())
			}
			if !filepath.IsAbs(gitFile) {
				gitFile = filepath.Join(adminDir, gitFile)
			}
			root, err := canonicalDaemonOwnerRepoPath(filepath.Dir(gitFile))
			if err != nil {
				return nil, err
			}
			roots[root] = struct{}{}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return roots, nil
}

func canonicalDaemonOwnerRepoPath(repo string) (string, error) {
	abs, err := filepath.Abs(repo)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		abs = filepath.Clean(real)
	}
	return abs, nil
}

func listDaemonOwnerProcesses(ctx context.Context) ([]daemonOwnerProcess, error) {
	psPath := "ps"
	switch runtime.GOOS {
	case "darwin":
		psPath = "/bin/ps"
	case "linux":
		psPath = "/usr/bin/ps"
	}
	out, err := exec.CommandContext(
		ctx, psPath, "-axo", "pid=,command=").Output()
	if err != nil {
		return nil, err
	}
	var processes []daemonOwnerProcess
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		processes = append(processes, daemonOwnerProcess{
			pid: pid, command: strings.TrimSpace(
				strings.TrimPrefix(line, fields[0])),
		})
	}
	return processes, scanner.Err()
}

func daemonOwnerRepoIdentity(ctx context.Context, repo string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	abs, err := filepath.Abs(repo)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		abs = filepath.Clean(real)
	}
	gitDir := filepath.Join(abs, ".git")
	info, err := os.Stat(gitDir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		body, err := os.ReadFile(gitDir)
		if err != nil {
			return "", err
		}
		value, found := strings.CutPrefix(
			strings.TrimSpace(string(body)), "gitdir:")
		if !found || strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("invalid Git dir file %s", gitDir)
		}
		value = strings.TrimSpace(value)
		if filepath.IsAbs(value) {
			gitDir = filepath.Clean(value)
		} else {
			gitDir = filepath.Clean(filepath.Join(abs, value))
		}
	}
	_, commonDir, err := daemonLockDirs(gitDir)
	return commonDir, err
}

// ParseDaemonRunRepoArg recognizes the direct command shape emitted by
// defaultSpawnDaemon: <acd-executable> daemon run --repo <absolute-root>.
// The repo argument is last in that production command, so its raw suffix is
// path data rather than shell syntax. Quotes, backslashes, and " - " path
// segments are intentionally preserved. Shell/wrapper command lines are not
// treated as daemon owners.
func ParseDaemonRunRepoArg(command string) (string, bool) {
	executable, rest, ok := cutDaemonCommandField(command)
	if !ok || filepath.Base(executable) != "acd" {
		return "", false
	}
	subcommand, rest, ok := cutDaemonCommandField(rest)
	if !ok || subcommand != "daemon" {
		return "", false
	}
	action, rest, ok := cutDaemonCommandField(rest)
	if !ok || action != "run" {
		return "", false
	}
	rest = strings.TrimLeftFunc(rest, unicode.IsSpace)
	switch {
	case strings.HasPrefix(rest, "--repo="):
		value := strings.TrimPrefix(rest, "--repo=")
		return value, value != ""
	case strings.HasPrefix(rest, "--repo"):
		after := strings.TrimPrefix(rest, "--repo")
		if after == "" || !unicode.IsSpace(rune(after[0])) {
			return "", false
		}
		value := strings.TrimLeftFunc(after, unicode.IsSpace)
		return value, value != ""
	default:
		return "", false
	}
}

func cutDaemonCommandField(value string) (string, string, bool) {
	value = strings.TrimLeftFunc(value, unicode.IsSpace)
	if value == "" {
		return "", "", false
	}
	index := strings.IndexFunc(value, unicode.IsSpace)
	if index < 0 {
		return value, "", true
	}
	return value[:index], value[index:], true
}

// legacyDaemonLockPaths returns every old-version lock path that exists at
// acquisition time. Older ACD versions lock a worktree-local
// <git-dir>/acd/daemon.lock, so a new daemon must fence every existing linked
// worktree, not just the worktree from which it was started. It also retains
// immediately relocated state-directory locks (for example acd-moved) so
// replacing .git/acd cannot orphan a still-running old owner's flock.
//
// The stable repository lock is acquired before this scan. Paths are sorted
// before acquisition to keep the mixed-version lock order deterministic.
// This compatibility fence is necessarily point-in-time. The independent
// canonical process probe closes relocation races involving an already-running
// old owner before return, but no new binary can retroactively fence an old
// writer whose locked directory was moved outside its Git admin dir after the
// probe, nor a worktree created after acquisition.
func legacyDaemonLockPaths(gitDir, commonDir string) ([]string, error) {
	adminDirs := map[string]struct{}{
		filepath.Clean(gitDir):    {},
		filepath.Clean(commonDir): {},
	}
	worktreesDir := filepath.Join(commonDir, "worktrees")
	entries, err := os.ReadDir(worktreesDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("daemon: list linked worktree Git dirs: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		adminDirs[filepath.Join(worktreesDir, entry.Name())] = struct{}{}
		if len(adminDirs) > maxLegacyDaemonLockPaths {
			return nil, fmt.Errorf("daemon: legacy lock scan exceeds %d Git dirs", maxLegacyDaemonLockPaths)
		}
	}

	paths := make(map[string]struct{}, len(adminDirs))
	for adminDir := range adminDirs {
		paths[filepath.Join(adminDir, stateSubdir, "daemon.lock")] = struct{}{}

		// An old daemon keeps its flock on the original inode when the state
		// directory is renamed. Discover immediate sibling directories that
		// still contain a daemon.lock and retain those compatibility fences.
		children, err := os.ReadDir(adminDir)
		if err != nil {
			return nil, fmt.Errorf("daemon: scan Git dir %s for relocated legacy locks: %w", adminDir, err)
		}
		for _, child := range children {
			if !child.IsDir() || child.Name() == stateSubdir {
				continue
			}
			candidate := filepath.Join(adminDir, child.Name(), "daemon.lock")
			info, err := os.Stat(candidate)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("daemon: inspect relocated legacy lock %s: %w", candidate, err)
			}
			if !info.Mode().IsRegular() {
				continue
			}
			paths[candidate] = struct{}{}
			if len(paths) > maxLegacyDaemonLockPaths {
				return nil, fmt.Errorf("daemon: legacy lock scan exceeds %d paths", maxLegacyDaemonLockPaths)
			}
		}
	}

	out := make([]string, 0, len(paths))
	for path := range paths {
		out = append(out, path)
	}
	slices.Sort(out)
	return out, nil
}

func daemonLockDirs(gitDir string) (canonicalGitDir, commonDir string, retErr error) {
	abs, err := filepath.Abs(gitDir)
	if err != nil {
		return "", "", fmt.Errorf("daemon: resolve git dir %q: %w", gitDir, err)
	}
	canonicalGitDir = filepath.Clean(abs)
	if real, err := filepath.EvalSymlinks(canonicalGitDir); err == nil {
		canonicalGitDir = filepath.Clean(real)
	}

	commonDir = canonicalGitDir
	body, err := os.ReadFile(filepath.Join(canonicalGitDir, "commondir"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return canonicalGitDir, commonDir, nil
		}
		return "", "", fmt.Errorf("daemon: read Git commondir: %w", err)
	}
	value := string(body)
	for len(value) > 0 && (value[len(value)-1] == '\n' || value[len(value)-1] == '\r') {
		value = value[:len(value)-1]
	}
	if value == "" {
		return "", "", errors.New("daemon: Git commondir is empty")
	}
	if filepath.IsAbs(value) {
		commonDir = filepath.Clean(value)
	} else {
		commonDir = filepath.Clean(filepath.Join(canonicalGitDir, value))
	}
	if real, err := filepath.EvalSymlinks(commonDir); err == nil {
		commonDir = filepath.Clean(real)
	}
	return canonicalGitDir, commonDir, nil
}

func acquireFileLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrDaemonLockHeld
		}
		return nil, fmt.Errorf("flock %s: %w", path, err)
	}
	return f, nil
}

func releaseFileLock(f *os.File) error {
	if f == nil {
		return nil
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return f.Close()
}

func releaseFileLocksReverse(locks []*os.File) error {
	var errs []error
	for i := len(locks) - 1; i >= 0; i-- {
		if err := releaseFileLock(locks[i]); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Release drops the daemon lock and closes the underlying file. Safe to call
// multiple times; subsequent calls are no-ops. Locks are released in reverse
// acquisition order so the legacy compatibility fence remains held until the
// stable repository fence is ready to drop.
func (l *DaemonLock) Release() error {
	if l == nil {
		return nil
	}
	legacyErr := releaseFileLocksReverse(l.legacy)
	l.legacy = nil
	stableErr := releaseFileLock(l.stable)
	l.stable = nil
	return errors.Join(legacyErr, stableErr)
}

// ControlLock is the brief flock held by CLI subcommands.
type ControlLock struct {
	f *os.File
}

// AcquireControlLock acquires <gitDir>/acd/control.lock with LOCK_EX|LOCK_NB.
// Returns ErrControlLockHeld on contention.
func AcquireControlLock(gitDir string) (*ControlLock, error) {
	if gitDir == "" {
		return nil, fmt.Errorf("daemon: AcquireControlLock: empty gitDir")
	}
	dir := filepath.Join(gitDir, stateSubdir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("daemon: mkdir control lock parent: %w", err)
	}
	path := filepath.Join(dir, "control.lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("daemon: open control.lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrControlLockHeld
		}
		return nil, fmt.Errorf("daemon: flock control.lock: %w", err)
	}
	return &ControlLock{f: f}, nil
}

// Release drops the control lock and closes the file.
func (l *ControlLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}

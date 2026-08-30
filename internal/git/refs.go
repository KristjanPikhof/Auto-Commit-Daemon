package git

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
)

// ErrRefNotFound is returned by RevParse when the requested rev does not
// resolve. Callers use it to distinguish "missing ref" (expected, e.g. on
// an initial commit) from a real git failure.
var ErrRefNotFound = errors.New("git: ref not found")

// ErrRefAmbiguous is returned by RevParse when the requested name resolves
// to multiple objects (e.g. a branch and a tag with the same short name).
// Pre-fix RevParse used --quiet which silently coerced this case to a
// successful resolution, hiding repo misconfiguration; we now surface it as
// a distinct error so callers can fail loudly.
var ErrRefAmbiguous = errors.New("git: ref is ambiguous")

// RecoveryRefPrefix is the private namespace used for captured-work recovery
// commits. It intentionally lives outside refs/heads so snapshots do not
// appear as user branches or affect normal branch enumeration.
const RecoveryRefPrefix = "refs/acd/recovery/"

// ErrRecoveryRefCollision means a deterministic recovery ref already exists
// but points at a different commit. Callers must fail closed: overwriting the
// ref could make a previously preserved snapshot unreachable.
var ErrRecoveryRefCollision = errors.New("git: recovery ref collision")

// RecoveryRefResult reports whether EnsureRecoveryRef created the ref or
// reused an identical target left by an earlier attempt.
type RecoveryRefResult struct {
	Ref       string
	CommitOID string
	Created   bool
	Reused    bool
}

// RefExpectation is an exact private-ref target used by transactional
// uninstall and migration cleanup.
type RefExpectation struct {
	Ref string `json:"ref"`
	OID string `json:"oid"`
}

// DeleteRefsCAS deletes all refs in one Git reference transaction. If any
// expected target changed, Git aborts the complete transaction.
func DeleteRefsCAS(ctx context.Context, repoDir string, refs []RefExpectation) error {
	commands := make([]string, 0, len(refs))
	for _, item := range refs {
		if !isValidFullRef(item.Ref) || !strings.HasPrefix(item.Ref, "refs/acd/") ||
			item.OID == "" || strings.ContainsAny(item.OID, " \t\r\n\x00") {
			return fmt.Errorf("git: invalid private ref expectation %q", item.Ref)
		}
		commands = append(commands, "delete "+item.Ref+" "+item.OID)
	}
	return applyRefCommands(ctx, repoDir, commands)
}

// CreateRefsCAS recreates all expected refs atomically and only while each is
// absent. It is the rollback counterpart of DeleteRefsCAS.
func CreateRefsCAS(ctx context.Context, repoDir string, refs []RefExpectation) error {
	commands := make([]string, 0, len(refs))
	for _, item := range refs {
		if !isValidFullRef(item.Ref) || !strings.HasPrefix(item.Ref, "refs/acd/") ||
			item.OID == "" || strings.ContainsAny(item.OID, " \t\r\n\x00") {
			return fmt.Errorf("git: invalid private ref expectation %q", item.Ref)
		}
		commands = append(commands, "create "+item.Ref+" "+item.OID)
	}
	return applyRefCommands(ctx, repoDir, commands)
}

func applyRefCommands(ctx context.Context, repoDir string, commands []string) error {
	if len(commands) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultWriteTimeout)
	defer cancel()
	var input strings.Builder
	input.WriteString("start\n")
	for _, command := range commands {
		input.WriteString(command)
		input.WriteByte('\n')
	}
	input.WriteString("prepare\ncommit\n")
	cmd := exec.CommandContext(ctx, "git", "update-ref", "--no-deref", "--stdin")
	cmd.Dir = repoDir
	cmd.Env = scrubEnv(nil)
	cmd.Stdin = strings.NewReader(input.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git: reference transaction: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

type synchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// RevParse resolves rev (any acceptable revision spec — HEAD, refs/...,
// short hash, etc.) to a full SHA. Returns ErrRefNotFound when the rev does
// not exist, ErrRefAmbiguous when the name maps to multiple objects, and
// any other failure as a real *Error wrapping git's stderr.
//
// Disambiguation contract (git semantics with --verify but WITHOUT --quiet):
//
//   - Exit 0, no warning → resolved successfully.
//   - Exit 0, "warning: refname '...' is ambiguous." on stderr → multiple
//     refs share the short name. The reported OID is whichever git resolved
//     first (refs/heads order). We surface ErrRefAmbiguous wrapping a
//     *Error so callers can either fail or downgrade by passing a fully
//     qualified ref like refs/heads/foo.
//   - Exit 128, "fatal: Needed a single revision" → ref does not exist.
//     This is git's canonical missing-rev shape when --quiet is not set.
//
// Pre-fix the call passed --quiet which suppressed both the ambiguity
// warning AND collapsed missing-ref to exit 1, masking misconfiguration.
func RevParse(ctx context.Context, repoDir, rev string) (string, error) {
	out, stderr, err := RunWithStderr(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "rev-parse", "--verify", rev)
	if err != nil {
		var gerr *Error
		if errors.As(err, &gerr) {
			gerrStderr := strings.TrimSpace(gerr.Stderr)
			// "Needed a single revision" is git's missing-ref message at
			// exit 128 without --quiet. Treat it (and the historical
			// --quiet form, exit 1 + empty stderr) as ErrRefNotFound.
			if strings.Contains(gerrStderr, "Needed a single revision") {
				return "", ErrRefNotFound
			}
			if gerr.ExitCode == 1 && gerrStderr == "" {
				return "", ErrRefNotFound
			}
		}
		return "", err
	}
	// Exit 0 with an "ambiguous" warning on stderr means the short name
	// resolved by accident — git picked the first match. Surface this so
	// callers don't silently consume the wrong OID.
	if bytes.Contains(stderr, []byte("is ambiguous")) {
		return "", fmt.Errorf("%w: %s", ErrRefAmbiguous, strings.TrimSpace(string(stderr)))
	}
	return strings.TrimSpace(string(out)), nil
}

// ShowToplevel returns the absolute path of the worktree root.
func ShowToplevel(ctx context.Context, dir string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultReadTimeout}, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// AbsoluteGitDir returns the absolute path of the worktree's git dir
// (`.git` for normal repos, the linked git dir for worktrees).
func AbsoluteGitDir(ctx context.Context, dir string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultReadTimeout}, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// UpdateRef sets ref to newOID. If oldOID is non-empty, it is supplied as
// the expected current value (compare-and-swap); pass the empty string for
// a new ref or an unconditional update.
//
// Mirrors the legacy replay's compare-and-swap update-ref invocation in
// snapshot-replay.py. --no-deref makes named refs explicit instead of
// following symbolic refs; literal HEAD is allowed to dereference so linked
// worktrees update their active branch rather than detaching HEAD.
func UpdateRef(ctx context.Context, repoDir, ref, newOID, oldOID string) error {
	args := []string{"update-ref"}
	if ref != "HEAD" {
		args = append(args, "--no-deref")
	}
	args = append(args, ref, newOID)
	if oldOID != "" {
		args = append(args, oldOID)
	}
	_, err := Run(ctx, RunOpts{Dir: repoDir}, args...)
	return err
}

// EnsureRecoveryRef creates a private recovery ref with compare-and-swap
// semantics. An absent ref is created only when it is still absent; an
// existing ref is reusable only when it already targets commitOID. A distinct
// target returns ErrRecoveryRefCollision and is never overwritten.
func EnsureRecoveryRef(ctx context.Context, repoDir, ref, commitOID string) (RecoveryRefResult, error) {
	var result RecoveryRefResult
	if repoDir == "" {
		return result, fmt.Errorf("git: EnsureRecoveryRef called with empty repoDir")
	}
	if !strings.HasPrefix(ref, RecoveryRefPrefix) || len(ref) == len(RecoveryRefPrefix) {
		return result, fmt.Errorf("git: recovery ref %q must be below %s", ref, RecoveryRefPrefix)
	}
	if commitOID == "" {
		return result, fmt.Errorf("git: EnsureRecoveryRef called with empty commitOID")
	}
	resolvedCommit, err := RevParse(ctx, repoDir, commitOID+"^{commit}")
	if err != nil {
		return result, fmt.Errorf("git: validate recovery commit %s: %w", commitOID, err)
	}
	if resolvedCommit != commitOID {
		return result, fmt.Errorf("git: recovery commit resolved to %s, want %s", resolvedCommit, commitOID)
	}

	result.Ref = ref
	result.CommitOID = commitOID
	existing, err := RevParse(ctx, repoDir, ref)
	if err == nil {
		if existing == commitOID {
			result.Reused = true
			return result, nil
		}
		return RecoveryRefResult{}, fmt.Errorf("%w: %s points at %s, want %s",
			ErrRecoveryRefCollision, ref, existing, commitOID)
	}
	if !errors.Is(err, ErrRefNotFound) {
		return RecoveryRefResult{}, fmt.Errorf("git: resolve recovery ref %s: %w", ref, err)
	}

	// A forty-zero expected old value is git update-ref's create-only CAS.
	const zeroOID = "0000000000000000000000000000000000000000"
	if err := UpdateRef(ctx, repoDir, ref, commitOID, zeroOID); err == nil {
		result.Created = true
		return result, nil
	} else {
		// Another writer may have created the same deterministic ref between
		// our read and CAS. Re-read: equal is an idempotent success; different
		// is a collision and must preserve the winner.
		existing, readErr := RevParse(ctx, repoDir, ref)
		if readErr == nil {
			if existing == commitOID {
				result.Reused = true
				return result, nil
			}
			return RecoveryRefResult{}, fmt.Errorf("%w: %s raced to %s, want %s",
				ErrRecoveryRefCollision, ref, existing, commitOID)
		}
		if !errors.Is(readErr, ErrRefNotFound) {
			return RecoveryRefResult{}, fmt.Errorf("git: re-read recovery ref %s after CAS failure: %w", ref, readErr)
		}
		return RecoveryRefResult{}, fmt.Errorf("git: create recovery ref %s: %w", ref, err)
	}
}

// WithLockedRecoveryRef verifies ref still targets expectedOID, then holds
// Git's update-ref transaction lock while fn runs. The lock closes the gap
// between proof-ref verification and a cross-store mutation such as pruning
// the SQLite capture rows protected by that ref.
//
// The transaction only verifies the ref; it does not change its value. Git
// nevertheless locks the ref at prepare time, so another well-behaved Git
// process cannot move or delete it before fn returns and the transaction is
// committed or aborted.
func WithLockedRecoveryRef(ctx context.Context, repoDir, ref, expectedOID string, fn func() error) error {
	return withLockedExpectedRef(ctx, repoDir, ref, expectedOID, "", "", true, fn)
}

// WithLockedRecoveryRefAndAbsentRef verifies and locks both a recovery ref
// and an expected-missing ref in one update-ref transaction. Dead-branch
// recovery uses this to prevent branch recreation between its last absence
// check and the SQLite transition protected by fn.
func WithLockedRecoveryRefAndAbsentRef(
	ctx context.Context,
	repoDir string,
	ref string,
	expectedOID string,
	expectedAbsentRef string,
	fn func() error,
) error {
	if !isValidFullRef(expectedAbsentRef) {
		return fmt.Errorf("git: expected-absent ref %q must be a valid full ref name", expectedAbsentRef)
	}
	if expectedAbsentRef == ref {
		return fmt.Errorf("git: expected-absent ref must differ from recovery ref %q", ref)
	}
	zeroOID, err := zeroOIDForRepo(ctx, repoDir)
	if err != nil {
		return err
	}
	return withLockedExpectedRef(
		ctx, repoDir, ref, expectedOID, expectedAbsentRef, zeroOID, true, fn)
}

// WithLockedRecoveryRefAndExpectedRef verifies and locks both a protected
// recovery ref and one exact live ref in a single Git transaction. The
// callback can then adopt the live ref in another store without a ref-movement
// gap.
func WithLockedRecoveryRefAndExpectedRef(
	ctx context.Context,
	repoDir string,
	recoveryRef string,
	recoveryOID string,
	expectedRef string,
	expectedOID string,
	fn func() error,
) error {
	if !isValidFullRef(expectedRef) {
		return fmt.Errorf("git: expected ref %q must be a valid full ref name", expectedRef)
	}
	if expectedRef == recoveryRef {
		return fmt.Errorf("git: expected ref must differ from recovery ref %q", recoveryRef)
	}
	return withLockedExpectedRef(
		ctx, repoDir, recoveryRef, recoveryOID,
		expectedRef, expectedOID, true, fn)
}

// WithLockedExpectedRef verifies and locks one literal full ref at an exact
// object ID while fn performs a cross-store mutation. It is intentionally
// narrower than a general ref transaction: it cannot create, delete, or move
// refs, and the callback runs only after Git has prepared the verification.
func WithLockedExpectedRef(
	ctx context.Context,
	repoDir string,
	ref string,
	expectedOID string,
	fn func() error,
) error {
	return withLockedExpectedRef(ctx, repoDir, ref, expectedOID, "", "", false, fn)
}

// WithLockedAbsentRef verifies and locks one literal full ref while it is
// absent. The callback can safely update a second store knowing another Git
// process cannot create the ref until the transaction finishes.
func WithLockedAbsentRef(
	ctx context.Context,
	repoDir string,
	ref string,
	fn func() error,
) error {
	zeroOID, err := zeroOIDForRepo(ctx, repoDir)
	if err != nil {
		return err
	}
	return withLockedExpectedRef(ctx, repoDir, ref, zeroOID, "", "", false, fn)
}

func withLockedExpectedRef(
	ctx context.Context,
	repoDir string,
	ref string,
	expectedOID string,
	secondaryRef string,
	secondaryExpectedOID string,
	requireRecoveryNamespace bool,
	fn func() error,
) error {
	if repoDir == "" {
		return fmt.Errorf("git: locked ref verification called with empty repoDir")
	}
	if !isValidFullRef(ref) {
		return fmt.Errorf("git: locked ref %q must be a valid full ref name", ref)
	}
	if requireRecoveryNamespace &&
		(!strings.HasPrefix(ref, RecoveryRefPrefix) ||
			len(ref) == len(RecoveryRefPrefix)) {
		return fmt.Errorf(
			"git: recovery ref %q must be below %s", ref, RecoveryRefPrefix)
	}
	if expectedOID == "" || strings.ContainsAny(expectedOID, " \t\r\n\x00") {
		return fmt.Errorf("git: locked ref verification called with invalid expected OID")
	}
	if fn == nil {
		return fmt.Errorf("git: locked ref verification called with nil callback")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if secondaryRef != "" {
		if !isValidFullRef(secondaryRef) || secondaryRef == ref {
			return fmt.Errorf("git: secondary locked ref %q is invalid", secondaryRef)
		}
		if secondaryExpectedOID == "" ||
			strings.ContainsAny(secondaryExpectedOID, " \t\r\n\x00") {
			return fmt.Errorf("git: secondary locked ref has invalid expected OID")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultWriteTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "git", "update-ref", "--no-deref", "--stdin")
	cmd.Dir = repoDir
	cmd.Env = scrubEnv(nil)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("git: open recovery ref transaction stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return fmt.Errorf("git: open recovery ref transaction stdout: %w", err)
	}
	var stderr synchronizedBuffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return fmt.Errorf("git: start recovery ref transaction: %w", err)
	}
	reader := bufio.NewReader(stdout)
	finished := false
	defer func() {
		if finished {
			return
		}
		_ = stdin.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	write := func(line string) error {
		if _, err := io.WriteString(stdin, line+"\n"); err != nil {
			return fmt.Errorf("write %q: %w", line, err)
		}
		return nil
	}
	expectOK := func(action string) error {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read %s response: %w", action, err)
		}
		if strings.TrimSpace(line) != action+": ok" {
			return fmt.Errorf("unexpected %s response %q", action, strings.TrimSpace(line))
		}
		return nil
	}
	fail := func(action string, err error) error {
		if detail := strings.TrimSpace(stderr.String()); detail != "" {
			return fmt.Errorf("git: recovery ref transaction %s: %w: %s", action, err, detail)
		}
		return fmt.Errorf("git: recovery ref transaction %s: %w", action, err)
	}

	if err := write("start"); err != nil {
		return fail("start", err)
	}
	if err := expectOK("start"); err != nil {
		return fail("start", err)
	}
	if err := write("verify " + ref + " " + expectedOID); err != nil {
		return fail("verify", err)
	}
	if secondaryRef != "" {
		if err := write("verify " + secondaryRef + " " + secondaryExpectedOID); err != nil {
			return fail("verify secondary", err)
		}
	}
	if err := write("prepare"); err != nil {
		return fail("prepare", err)
	}
	if err := expectOK("prepare"); err != nil {
		return fail("prepare", err)
	}

	if callbackErr := fn(); callbackErr != nil {
		abortErr := write("abort")
		if abortErr == nil {
			abortErr = expectOK("abort")
		}
		_ = stdin.Close()
		waitErr := cmd.Wait()
		finished = true
		if abortErr != nil {
			return errors.Join(callbackErr, fail("abort", abortErr))
		}
		if waitErr != nil {
			return errors.Join(callbackErr, fail("abort wait", waitErr))
		}
		return callbackErr
	}

	if err := write("commit"); err != nil {
		return fail("commit", err)
	}
	if err := expectOK("commit"); err != nil {
		return fail("commit", err)
	}
	if err := stdin.Close(); err != nil {
		return fail("close", err)
	}
	waitErr := cmd.Wait()
	finished = true
	if waitErr != nil {
		return fail("wait", waitErr)
	}
	return nil
}

func isValidFullRef(ref string) bool {
	return strings.HasPrefix(ref, "refs/") && len(ref) > len("refs/") &&
		!strings.ContainsAny(ref, " \t\r\n\x00")
}

func zeroOIDForRepo(ctx context.Context, repoDir string) (string, error) {
	out, err := Run(ctx, RunOpts{
		Dir: repoDir, Timeout: DefaultReadTimeout,
	}, "rev-parse", "--show-object-format")
	if err != nil {
		return "", fmt.Errorf("git: resolve repository object format: %w", err)
	}
	switch strings.TrimSpace(string(out)) {
	case "sha1":
		return strings.Repeat("0", 40), nil
	case "sha256":
		return strings.Repeat("0", 64), nil
	default:
		return "", fmt.Errorf("git: unsupported repository object format %q",
			strings.TrimSpace(string(out)))
	}
}

// RunBranchRef returns the symbolic ref the worktree's HEAD points at,
// e.g. "refs/heads/main". Returns ("", nil) on a detached HEAD; surfaces
// any other git failure verbatim.
//
// The shell-out is `git symbolic-ref --quiet HEAD`. Detached HEAD makes
// git exit 1 with no stderr; we map that to ("", nil) so the run loop can
// fall back to a default branch name.
func RunBranchRef(ctx context.Context, repoDir string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir}, "symbolic-ref", "--quiet", "HEAD")
	if err != nil {
		var gerr *Error
		if errors.As(err, &gerr) && gerr.ExitCode == 1 {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// RefExists probes whether a fully-qualified ref (e.g. "refs/heads/main")
// currently resolves in the repo. It uses `git show-ref --verify --quiet`
// which is a cheaper probe than RevParse: no SHA is returned, no object
// dereferencing happens, and git's ambiguity logic for short names is
// bypassed because show-ref --verify only matches exact ref names.
//
// Exit-code contract (git show-ref --verify --quiet):
//
//   - Exit 0   → ref exists; returns (true, nil).
//   - Exit 1   → ref does not exist; git writes nothing to stderr;
//     returns (false, nil).
//   - Any other exit, or a non-*Error failure (e.g. exec not found),
//     returns (false, err) with err wrapping git's stderr.
//
// An empty ref argument is rejected immediately without shelling out,
// mirroring the defensive guards in other helpers in this package.
//
// The call is bounded by DefaultReadTimeout via RunOpts.Timeout; it also
// respects ctx cancellation because RunWithStderr honors it.
func RefExists(ctx context.Context, repoDir, ref string) (bool, error) {
	if ref == "" {
		return false, fmt.Errorf("git: RefExists called with empty ref")
	}
	_, _, err := RunWithStderr(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show-ref", "--verify", "--quiet", ref)
	if err == nil {
		return true, nil
	}
	var gerr *Error
	if errors.As(err, &gerr) && gerr.ExitCode == 1 {
		return false, nil
	}
	return false, err
}

// LiveBranchSet returns a set of every refname under refs/heads/ in repoDir.
// Used by the dead-branch sweep to perform set-membership probes against
// many candidate refs without one shellout per ref.
//
// The shell-out is `git for-each-ref --format=%(refname) refs/heads/`. An
// empty repo (no refs) returns an empty set with no error. Honors ctx
// cancellation. Times out via DefaultReadTimeout.
//
// Empty repoDir is rejected immediately. A non-zero exit from git surfaces
// as a real error (callers must fail closed — preserving rows when
// liveness cannot be determined).
func LiveBranchSet(ctx context.Context, repoDir string) (map[string]struct{}, error) {
	if repoDir == "" {
		return nil, fmt.Errorf("git: LiveBranchSet called with empty repoDir")
	}
	out, _, err := RunWithStderr(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
		"for-each-ref", "--format=%(refname)", "refs/heads/")
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for _, line := range strings.Split(string(out), "\n") {
		ref := strings.TrimSpace(line)
		if ref == "" {
			continue
		}
		set[ref] = struct{}{}
	}
	return set, nil
}

// IsAncestor reports whether ancestor is an ancestor of descendant.
// Returns (true, nil) when ancestor, (false, nil) when not. A real git
// failure (e.g. unresolved oid) returns a non-nil error.
func IsAncestor(ctx context.Context, repoDir, ancestor, descendant string) (bool, error) {
	_, _, err := RunWithStderr(ctx,
		RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
		"merge-base", "--is-ancestor", ancestor, descendant)
	if err == nil {
		return true, nil
	}
	var gerr *Error
	if errors.As(err, &gerr) && gerr.ExitCode == 1 {
		return false, nil
	}
	return false, err
}

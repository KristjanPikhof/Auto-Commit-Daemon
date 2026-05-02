// Package git wraps the system git binary as a subprocess.
//
// Per locked decision D17, this package never imports go-git. Every git
// invocation goes through Run, which scrubs the environment, captures
// stdout/stderr, and respects context cancellation.
//
// See plan §8.2 (capture) and §8.3 (replay) for the calling protocols.
package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

// envAllowlist is the set of host environment variables that survive the
// scrub when invoking git. Everything else (including all GIT_* vars) is
// dropped before the child process inherits the env.
//
// Mirrors the legacy daemon's _clean_git_env() in snapshot_shared.py — the
// allowlist is intentionally narrow to keep the daemon's commit pipeline
// reproducible across operator environments.
var envAllowlist = []string{
	"PATH",
	"HOME",
	"USER",
	"LOGNAME",
	"TMPDIR",
	"TEMP",
	"TMP",
	"SYSTEMROOT", // harmless on unix; future-proofing
	"LANG",       // overridden by LC_ALL=C below, but kept stable
}

// scrubEnv builds a minimal env for a git child process. It drops every
// GIT_* variable from the host env (callers that legitimately need to set
// GIT_* — e.g. an alternate GIT_INDEX_FILE — pass them via RunOpts.ExtraEnv)
// and forces LC_ALL=C so we can parse git's output without surprises from
// localized error messages.
func scrubEnv(extra map[string]string) []string {
	out := make([]string, 0, len(envAllowlist)+8)
	for _, key := range envAllowlist {
		if v, ok := os.LookupEnv(key); ok {
			out = append(out, key+"="+v)
		}
	}
	out = append(out,
		"LC_ALL=C",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_OPTIONAL_LOCKS=0",
	)
	for k, v := range extra {
		out = append(out, k+"="+v)
	}
	return out
}

// Default per-call timeouts applied when callers do not specify one.
//
// DefaultReadTimeout covers ordinary read-side helpers (rev-parse, cat-file,
// diff, ls-files, etc.) and is wired into helpers via RunOpts.Timeout. The
// write-side budget is intentionally larger because commit-tree, write-tree,
// and update-ref can stall on lock contention or fsync on busy filesystems.
//
// Callers that supply their own RunOpts.Timeout override the default; a
// zero value means "no synthetic deadline" and run() leaves the caller's
// ctx untouched. Helpers that wrap Run/RunWithLimit decide their own
// per-helper default.
const (
	DefaultReadTimeout  = 30 * time.Second
	DefaultWriteTimeout = 60 * time.Second
)

// DefaultDiffCap caps the byte size of stdout for diff/blob helpers. A
// runaway blob or pathological diff would otherwise pin the daemon's RSS
// to whatever git emits. Callers can override per-call via the limited
// variants (CatFileBlobLimited / DiffBlobsLimited).
const DefaultDiffCap = 1 << 20 // 1 MiB

// ErrStdoutOverflow is returned by RunWithLimit (and the limited helper
// wrappers) when the child process writes more than maxBytes to stdout.
// The subprocess is killed and any partial stdout is returned alongside
// the error so callers can decide whether to surface a truncated payload.
var ErrStdoutOverflow = errors.New("git: stdout exceeded byte limit")

// RunOpts configures a single git invocation.
type RunOpts struct {
	// Dir is the working directory for the child process. Empty falls back
	// to the parent's CWD, but most callers should set it to the repo root
	// or git dir explicitly.
	Dir string
	// Stdin is fed to the child on stdin. Nil means no stdin.
	Stdin io.Reader
	// ExtraEnv extends the scrubbed env. Use this for GIT_INDEX_FILE etc.
	ExtraEnv map[string]string
	// Timeout, if > 0, wraps the caller ctx with context.WithTimeout for
	// the duration of this single invocation. A zero value means "leave
	// caller ctx as-is" — run() does NOT impose a synthetic deadline. The
	// helpers in this package set a default; ad-hoc callers of Run can
	// opt-in by setting it explicitly.
	Timeout time.Duration
}

// Error is returned when git exits non-zero. Stderr is captured for callers
// that want to surface the underlying message; ExitCode is -1 if the process
// was killed (typically by context cancellation).
type Error struct {
	Args     []string
	ExitCode int
	Stderr   string
	Err      error
}

func (e *Error) Error() string {
	msg := strings.TrimSpace(e.Stderr)
	if msg == "" && e.Err != nil {
		msg = e.Err.Error()
	}
	if msg == "" {
		msg = fmt.Sprintf("git %s exited %d", strings.Join(e.Args, " "), e.ExitCode)
	}
	return fmt.Sprintf("git %s: %s", strings.Join(e.Args, " "), msg)
}

func (e *Error) Unwrap() error { return e.Err }

// Run executes `git <args...>` and returns stdout. Stderr is captured into a
// typed Error on failure. The context is honored: cancellation kills the
// child process.
func Run(ctx context.Context, opts RunOpts, args ...string) ([]byte, error) {
	stdout, _, err := run(ctx, opts, args, 0)
	return stdout, err
}

// RunWithStderr is like Run but also returns captured stderr (useful for
// commands like check-ignore that overload exit codes).
func RunWithStderr(ctx context.Context, opts RunOpts, args ...string) ([]byte, []byte, error) {
	return run(ctx, opts, args, 0)
}

// RunWithLimit executes `git <args...>` and returns stdout, capping the
// captured stdout at maxBytes. If the child exceeds the cap, the process is
// killed and the helper returns ErrStdoutOverflow alongside the captured
// prefix. A maxBytes <= 0 disables the cap and behaves like Run.
//
// This is the entry point for commands whose output size is not bounded by
// the protocol (cat-file blob, diff). Use Run for fixed-shape commands.
func RunWithLimit(ctx context.Context, opts RunOpts, maxBytes int64, args ...string) ([]byte, error) {
	stdout, _, err := run(ctx, opts, args, maxBytes)
	return stdout, err
}

// limitWriter is a stdout sink that records up to cap bytes and signals
// overflow once a write would exceed the cap. The first write that trips
// the cap stores its prefix in buf and sets overflow=true; subsequent
// writes return errStdoutOverflowSentinel so io.Copy / cmd.Run unwind
// promptly. We treat the sentinel as a marker; the public ErrStdoutOverflow
// is what callers see.
type limitWriter struct {
	cap      int64
	buf      bytes.Buffer
	overflow bool
}

var errStdoutOverflowSentinel = errors.New("limitWriter overflow")

func (lw *limitWriter) Write(p []byte) (int, error) {
	if lw.overflow {
		return 0, errStdoutOverflowSentinel
	}
	remaining := lw.cap - int64(lw.buf.Len())
	if int64(len(p)) <= remaining {
		return lw.buf.Write(p)
	}
	if remaining > 0 {
		_, _ = lw.buf.Write(p[:remaining])
	}
	lw.overflow = true
	// Report a short write so the io contract is honored; the next call
	// (if any) returns the sentinel.
	return int(remaining), errStdoutOverflowSentinel
}

func run(ctx context.Context, opts RunOpts, args []string, maxBytes int64) ([]byte, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = opts.Dir
	cmd.Env = scrubEnv(opts.ExtraEnv)
	if opts.Stdin != nil {
		cmd.Stdin = opts.Stdin
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	var (
		stdoutBytes []byte
		overflow    bool
	)
	if maxBytes > 0 {
		lw := &limitWriter{cap: maxBytes}
		cmd.Stdout = lw
		err := cmd.Run()
		stdoutBytes = lw.buf.Bytes()
		overflow = lw.overflow
		if overflow {
			// Best-effort kill in case the process is still around (it
			// usually exits on its own when its stdout pipe shuts).
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			return stdoutBytes, stderr.Bytes(), ErrStdoutOverflow
		}
		if err != nil {
			exitCode := -1
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				exitCode = exitErr.ExitCode()
			}
			return stdoutBytes, stderr.Bytes(), &Error{
				Args:     args,
				ExitCode: exitCode,
				Stderr:   stderr.String(),
				Err:      err,
			}
		}
		return stdoutBytes, stderr.Bytes(), nil
	}

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	err := cmd.Run()
	if err != nil {
		exitCode := -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
		return stdout.Bytes(), stderr.Bytes(), &Error{
			Args:     args,
			ExitCode: exitCode,
			Stderr:   stderr.String(),
			Err:      err,
		}
	}
	return stdout.Bytes(), stderr.Bytes(), nil
}

// Init creates an empty git repo at dir (running `git init -q`). Used by
// tests and bootstrap flows. The directory must exist.
func Init(ctx context.Context, dir string) error {
	_, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "init", "-q")
	return err
}

// SymlinkMode is the canonical git mode bits for a symlink entry. The Go
// port encodes every symlink as 120000 regardless of target type — see the
// CLAUDE.md gotcha and §8.2.
const SymlinkMode = "120000"

// RegularFileMode is 100644. Executable regular files use 100755.
const (
	RegularFileMode    = "100644"
	ExecutableFileMode = "100755"
)

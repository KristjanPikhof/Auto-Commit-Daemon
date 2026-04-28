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
	stdout, _, err := run(ctx, opts, args)
	return stdout, err
}

// RunWithStderr is like Run but also returns captured stderr (useful for
// commands like check-ignore that overload exit codes).
func RunWithStderr(ctx context.Context, opts RunOpts, args ...string) ([]byte, []byte, error) {
	return run(ctx, opts, args)
}

func run(ctx context.Context, opts RunOpts, args []string) ([]byte, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = opts.Dir
	cmd.Env = scrubEnv(opts.ExtraEnv)
	if opts.Stdin != nil {
		cmd.Stdin = opts.Stdin
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

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
	_, err := Run(ctx, RunOpts{Dir: dir}, "init", "-q")
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

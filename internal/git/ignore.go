package git

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// killWaitTimeout bounds how long killLocked waits for the underlying git
// subprocess to exit after stdin is closed and the context is cancelled. A
// process stuck in uninterruptible sleep (D state on NFS, paging, etc.) is
// not killable from userspace; we leak the cmd handle in that case rather
// than wedge the daemon shutdown path. The kernel reaps the process when it
// eventually unblocks.
const killWaitTimeout = 2 * time.Second

// waitFn is a test seam: tests can replace it to simulate a wedged
// subprocess that does not respond to SIGKILL. Production calls cmd.Wait
// directly.
var waitFn = func(cmd *exec.Cmd) error { return cmd.Wait() }

// IgnoreChecker batches paths through a single long-lived
// `git check-ignore --stdin -z` process per repo. Reusing one subprocess
// across many ticks beats forking once per file on a large worktree, and
// the batched legacy invocation in snapshot-capture.py shows the same shape.
//
// Fail-closed: a hard error from git (exit > 1) returns from the call, and
// the checker is left usable for retry by spawning a fresh subprocess on
// the next call. Each Check is its own request/response on the long-lived
// pipe — concurrent callers serialize through mu.
//
// Pipe-buffer hazard: macOS pipes default to a 16 KiB buffer (Linux 64 KiB).
// A naive implementation that writes the whole NUL-delimited path payload
// before reading any results deadlocks once the payload exceeds the pipe
// buffer: the daemon blocks on stdin.Write while git simultaneously blocks
// on stdout.Write because nobody is draining stdout. Check therefore pumps
// stdin in a writer goroutine while the main goroutine concurrently reads
// stdout. The writer surfaces its error through errCh; on a read error we
// kill the subprocess AND drain errCh so the writer goroutine cannot leak.
type IgnoreChecker struct {
	repoDir string

	// cancelPtr carries the subprocess cancel func atomically so Close
	// can fire it WITHOUT acquiring c.mu. If a Check call is mid-Wait
	// inside killLocked (with mu held) when the daemon shuts down, an
	// mu-bound Close would deadlock behind it. Storing cancel atomically
	// lets Close kick the subprocess immediately, after which the holder
	// of mu observes the cancel and unwinds.
	cancelPtr atomic.Pointer[context.CancelFunc]

	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	closed bool
}

// NewIgnoreChecker constructs an IgnoreChecker for the given repo. The
// underlying git subprocess is lazy: it is spawned on the first Check call
// and re-spawned if it dies between calls.
func NewIgnoreChecker(repoDir string) *IgnoreChecker {
	return &IgnoreChecker{repoDir: repoDir}
}

func (c *IgnoreChecker) ensureLocked() error {
	if c.closed {
		return errors.New("git: IgnoreChecker is closed")
	}
	if c.cmd != nil {
		return nil
	}
	// Detach from the caller's context: the subprocess outlives any
	// individual Check. Close() cancels it explicitly. Using
	// context.Background() here keeps Check's own ctx semantics
	// (cancellation kills the in-flight write/read, not the long-lived
	// process).
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "git",
		"check-ignore",
		"--stdin",
		"-z",
		"--non-matching",
		"--verbose",
	)
	cmd.Dir = c.repoDir
	cmd.Env = scrubEnv(nil)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return fmt.Errorf("check-ignore stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		_ = stdin.Close()
		return fmt.Errorf("check-ignore stdout pipe: %w", err)
	}
	cmd.Stderr = nil // discard; failures surface via exit + io errors
	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("check-ignore start: %w", err)
	}
	c.cmd = cmd
	c.stdin = stdin
	c.stdout = bufio.NewReader(stdout)
	cancelCopy := cancel
	c.cancelPtr.Store(&cancelCopy)
	return nil
}

// Check classifies each input path as ignored or not. The returned slice is
// 1:1 with paths. Order is preserved.
//
// Implementation note: the `--verbose --non-matching` flags coerce
// check-ignore into emitting one record per input path on stdout, so we can
// read back exactly len(paths) results without needing to peek at the
// process's exit status (which would block on the pipe-pair).
//
// Output record format (NUL-delimited fields, each record terminated by an
// extra NUL):
//
//	<source>\0<linenum>\0<pattern>\0<path>\0
//
// Ignored paths have a non-empty <source> (the .gitignore that matched);
// non-matching paths emit a record with empty <source>, <linenum>, and
// <pattern>.
func (c *IgnoreChecker) Check(ctx context.Context, paths []string) ([]bool, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	for _, p := range paths {
		if strings.ContainsRune(p, 0) {
			return nil, fmt.Errorf("git: path contains NUL: %q", p)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureLocked(); err != nil {
		return nil, err
	}

	// Honor ctx cancellation: cancel the subprocess context so any
	// in-flight Write/Read returns an error. We deliberately do NOT touch
	// shared state (c.stdin/c.cmd/etc) from this goroutine — killLocked
	// runs in the error path below under c.mu. Capture cancel locally so
	// the goroutine never reads c.cancel concurrently with killLocked.
	cancelLocal := c.cancel
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		select {
		case <-ctx.Done():
			if cancelLocal != nil {
				cancelLocal()
			}
		case <-doneCh:
		}
	}()

	// Build the NUL-delimited stdin payload.
	var buf bytes.Buffer
	for _, p := range paths {
		buf.WriteString(p)
		buf.WriteByte(0)
	}

	// Pump stdin from a writer goroutine concurrently with the main
	// goroutine's stdout read loop. This avoids the pipe-buffer deadlock
	// described on the type comment: the writer can block on a full stdin
	// pipe while the reader drains stdout, and vice versa. The writer
	// reports its outcome through errCh.
	stdin := c.stdin
	errCh := make(chan error, 1)
	go func(payload []byte) {
		_, err := stdin.Write(payload)
		errCh <- err
	}(buf.Bytes())

	// Read 4 NUL-delimited fields per input path.
	results := make([]bool, len(paths))
	for i := range paths {
		fields := make([]string, 4)
		for f := 0; f < 4; f++ {
			tok, err := c.stdout.ReadBytes(0)
			if err != nil {
				// Kill the subprocess and drain the writer goroutine
				// so it cannot leak. After killLocked closes stdin,
				// the in-flight Write returns immediately.
				c.killLocked()
				<-errCh
				return nil, fmt.Errorf("check-ignore read: %w", err)
			}
			if len(tok) == 0 {
				c.killLocked()
				<-errCh
				return nil, fmt.Errorf("check-ignore: short read")
			}
			fields[f] = string(tok[:len(tok)-1]) // strip NUL
		}
		// Ignored when source (gitignore file) is non-empty AND the
		// matched pattern is not a negation. Git emits negation
		// patterns with a leading "!" — those un-ignore the path.
		source, pattern := fields[0], fields[2]
		results[i] = source != "" && !strings.HasPrefix(pattern, "!")
	}

	// Reads completed cleanly; surface any deferred writer error. A write
	// error here means git accepted enough bytes to satisfy len(paths)
	// records but then closed its stdin half early, which is still a
	// protocol violation — fail closed and reset the subprocess. Honor
	// ctx cancellation so a hung writer (git accepted only part of the
	// payload and never closed stdin) does not deadlock the call; the
	// killLocked path then unblocks the writer goroutine for a clean drain.
	select {
	case werr := <-errCh:
		if werr != nil {
			c.killLocked()
			return nil, fmt.Errorf("check-ignore write: %w", werr)
		}
	case <-ctx.Done():
		c.killLocked()
		<-errCh
		return nil, fmt.Errorf("check-ignore write: %w", ctx.Err())
	}
	return results, nil
}

// Close terminates the underlying git subprocess. Safe to call multiple
// times; safe to call concurrently with Check (Check serializes through
// the same mutex).
func (c *IgnoreChecker) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.killLocked()
	return nil
}

func (c *IgnoreChecker) killLocked() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	if c.stdin != nil {
		_ = c.stdin.Close()
		c.stdin = nil
	}
	if c.cmd != nil {
		_ = c.cmd.Wait()
		c.cmd = nil
	}
	c.stdout = nil
}

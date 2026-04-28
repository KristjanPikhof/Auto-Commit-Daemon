// plugin_subprocess.go — subprocess plugin runner per spec §10.3.
//
// A subprocess plugin is an external binary on $PATH named
// `acd-provider-<name>`. The daemon resolves the binary at construction,
// spawns it once per daemon lifetime, and multiplexes JSONL requests over
// the plugin's stdin (one request per line) and stdout (one response per
// line). The plugin is single-threaded by contract; we serialize requests
// from the daemon side too via a single owner goroutine.
//
// Wire format (one JSON object per line, both directions):
//
//	request:  {"version":1,"path":"...","op":"modify","old_path":"",
//	            "diff":"...","repo_root":"/abs","branch":"refs/heads/main",
//	            "multi_op":[{"path":"...","op":"...","old_path":"..."}],
//	            "now":"2026-04-28T12:00:00Z"}
//	response: {"version":1,"subject":"...","body":"...","error":""}
//
// The legacy snapshot daemon never shipped a subprocess provider, so the
// canonical wire shape lives in this file (and in the docstring above) as
// the contract every harness must speak. The `version` field exists so
// future shapes can be negotiated without breaking older plugins.
//
// Lifecycle:
//   - Spawn on first Generate after construction (or after a crash/timeout).
//   - One owner goroutine per provider holds (stdin, stdout, process); a
//     buffered request channel feeds it serialised work.
//   - Per-request timeout (default 30s) is enforced via context.WithTimeout
//     on the caller side. On timeout we kill the plugin so the next request
//     gets a fresh process — a stuck plugin must never wedge the daemon.
//   - On any I/O error or unexpected EOF on stdin/stdout the provider is
//     marked "crashed"; the next Generate respawns the plugin from scratch.
//   - Soft errors (plugin returns a non-empty `error` field) keep the
//     plugin alive; only the current request fails so Compose() can fall
//     back to deterministic.
//   - Close() closes stdin (signalling EOF), waits up to 5s for clean exit,
//     and escalates to SIGKILL if the plugin is still running.
package ai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// DefaultSubprocessTimeout is the per-request default per spec §10.3.
const DefaultSubprocessTimeout = 30 * time.Second

// subprocessShutdownGrace is the budget given to a closed plugin before we
// escalate from EOF-on-stdin to SIGKILL.
const subprocessShutdownGrace = 5 * time.Second

// pluginProtocolVersion is the integer carried in every request and
// response. Bump when the wire shape changes incompatibly.
const pluginProtocolVersion = 1

// LookPathFunc matches exec.LookPath's signature so tests can inject a
// fake binary lookup without touching the real $PATH.
type LookPathFunc func(string) (string, error)

// SubprocessOptions tunes a SubprocessProvider. Zero-valued fields fall
// back to safe defaults (Timeout=30s, LookPath=exec.LookPath, Logger=
// slog.Default).
type SubprocessOptions struct {
	Timeout  time.Duration // per-request hard timeout; 0 -> DefaultSubprocessTimeout
	Logger   *slog.Logger  // optional; nil -> slog.Default
	Env      []string      // additional env entries appended to os.Environ
	LookPath LookPathFunc  // resolves binary path; nil -> exec.LookPath
}

// SubprocessProvider implements Provider by talking JSONL to a long-lived
// child process. Safe for concurrent Generate calls from multiple
// goroutines: requests are serialised through a single owner goroutine.
type SubprocessProvider struct {
	name    string // logical plugin name (without acd-provider- prefix)
	binary  string // resolved absolute path to acd-provider-<name>
	resolveErr error // sticky error from initial LookPath; surfaced from Generate

	timeout time.Duration
	env     []string
	logger  *slog.Logger

	mu       sync.Mutex // guards plugin/closed
	plugin   *pluginSession
	closed   bool
}

// NewSubprocessProvider resolves acd-provider-<name> on $PATH (via opts.
// LookPath) and returns a provider ready for Generate calls. If the
// binary cannot be found the error is stored on the provider and surfaced
// from the first Generate; the constructor does not fail so callers can
// still wire `Compose(plugin, deterministic)` and have the deterministic
// fallback fire cleanly when the plugin is missing.
func NewSubprocessProvider(name string, opts SubprocessOptions) *SubprocessProvider {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultSubprocessTimeout
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	lookPath := opts.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}

	p := &SubprocessProvider{
		name:    name,
		timeout: timeout,
		env:     append([]string(nil), opts.Env...),
		logger:  logger,
	}

	if strings.TrimSpace(name) == "" {
		p.resolveErr = errors.New("subprocess: plugin name is empty")
		return p
	}

	binName := "acd-provider-" + name
	bin, err := lookPath(binName)
	if err != nil {
		p.resolveErr = fmt.Errorf("subprocess: lookup %s: %w", binName, err)
		return p
	}
	p.binary = bin
	return p
}

// Name reports the canonical Source identifier; mirrors the
// `subprocess:<name>` selector used in ACD_AI_PROVIDER.
func (p *SubprocessProvider) Name() string { return "subprocess:" + p.name }

// Generate marshals cc into a JSONL line, writes it to the plugin, reads
// one JSONL line back, sanitises subject + body, and returns the Result.
//
// Concurrent calls are safe: requests are serialised through the owner
// goroutine (the plugin protocol is single-threaded by contract). Cancel
// the supplied ctx to abort an in-flight request; if the deadline expires
// or the caller cancels mid-flight we kill the plugin so the next Generate
// gets a fresh process.
func (p *SubprocessProvider) Generate(ctx context.Context, cc CommitContext) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if p.resolveErr != nil {
		return Result{}, p.resolveErr
	}

	session, err := p.acquire()
	if err != nil {
		return Result{}, err
	}

	// Per-request timeout layered on top of the caller's ctx.
	reqCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	req := subprocessRequest{
		Version:  pluginProtocolVersion,
		Path:     cc.Path,
		Op:       cc.Op,
		OldPath:  cc.OldPath,
		Diff:     cc.DiffText,
		RepoRoot: cc.RepoRoot,
		Branch:   cc.Branch,
	}
	for _, item := range cc.MultiOp {
		req.MultiOp = append(req.MultiOp, subprocessOp{
			Path:    item.Path,
			Op:      item.Op,
			OldPath: item.OldPath,
		})
	}
	if !cc.Now.IsZero() {
		req.Now = cc.Now.UTC().Format(time.RFC3339Nano)
	}

	resp, err := session.exchange(reqCtx, req)
	if err != nil {
		// Any I/O / context error trips the crash path so the next
		// Generate respawns. Soft errors come back via resp.Error and
		// keep the plugin alive (handled below).
		p.markCrashed(session)
		return Result{}, err
	}
	if strings.TrimSpace(resp.Error) != "" {
		// Soft fail: plugin is healthy, just couldn't satisfy this
		// particular request. Compose() will fall back.
		return Result{}, fmt.Errorf("subprocess:%s: %s", p.name, resp.Error)
	}

	composed := resp.Subject
	if strings.TrimSpace(resp.Body) != "" {
		composed = resp.Subject + "\n\n" + resp.Body
	}
	cleaned := SanitizeMessage(composed)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	subj := parts[0]
	var body string
	if len(parts) == 2 {
		body = parts[1]
	}
	if strings.TrimSpace(subj) == "" {
		return Result{}, fmt.Errorf("subprocess:%s: empty subject after sanitize", p.name)
	}
	return Result{
		Subject: subj,
		Body:    body,
		Source:  p.Name(),
	}, nil
}

// Close shuts down the plugin process if running. Idempotent and safe to
// call from any goroutine. After Close, Generate returns an error.
func (p *SubprocessProvider) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	session := p.plugin
	p.plugin = nil
	p.mu.Unlock()

	if session == nil {
		return nil
	}
	return session.shutdown(subprocessShutdownGrace)
}

// acquire returns a live session, spawning one if necessary. The mutex
// guards the (plugin, closed) pair so concurrent Generate calls only race
// to claim the existing session, never to spawn duplicates.
func (p *SubprocessProvider) acquire() (*pluginSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, fmt.Errorf("subprocess:%s: provider closed", p.name)
	}
	if p.plugin != nil && !p.plugin.dead() {
		return p.plugin, nil
	}
	if p.plugin != nil {
		// Previous session crashed; reap then respawn.
		_ = p.plugin.shutdown(0)
		p.plugin = nil
	}
	session, err := startPlugin(p.binary, p.env, p.logger.With(slog.String("plugin", p.name)))
	if err != nil {
		return nil, fmt.Errorf("subprocess:%s: start: %w", p.name, err)
	}
	p.plugin = session
	return session, nil
}

// markCrashed tears down `session` if it is still the active plugin. The
// next Generate will respawn through acquire().
func (p *SubprocessProvider) markCrashed(session *pluginSession) {
	p.mu.Lock()
	if p.plugin == session {
		p.plugin = nil
	}
	p.mu.Unlock()
	// shutdown is safe to call multiple times.
	_ = session.shutdown(0)
}

// subprocessRequest is the JSONL request envelope. Field tags fix the wire
// names so the JSON shape matches the contract regardless of struct
// renames.
type subprocessRequest struct {
	Version  int             `json:"version"`
	Path     string          `json:"path,omitempty"`
	Op       string          `json:"op,omitempty"`
	OldPath  string          `json:"old_path,omitempty"`
	Diff     string          `json:"diff,omitempty"`
	RepoRoot string          `json:"repo_root,omitempty"`
	Branch   string          `json:"branch,omitempty"`
	MultiOp  []subprocessOp  `json:"multi_op,omitempty"`
	Now      string          `json:"now,omitempty"`
}

// subprocessOp mirrors OpItem on the wire (field tags decouple the wire
// shape from the Go field names).
type subprocessOp struct {
	Path    string `json:"path"`
	Op      string `json:"op"`
	OldPath string `json:"old_path,omitempty"`
}

// subprocessResponse is the JSONL response envelope.
type subprocessResponse struct {
	Version int    `json:"version"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
	Error   string `json:"error"`
}

// pluginRequest packages a request with its reply channel. The owner
// goroutine reads these from the work channel one at a time.
type pluginRequest struct {
	bytes []byte
	reply chan pluginReply
}

type pluginReply struct {
	resp subprocessResponse
	err  error
}

// pluginSession owns a single child process plus the goroutine that
// serialises stdin writes and stdout reads. Lifecycle: startPlugin -> run
// -> shutdown. The dead flag is set when the owner goroutine exits, so
// callers can check liveness without racing with the process.
type pluginSession struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	logger *slog.Logger

	work chan pluginRequest
	done chan struct{}    // closed when run() exits
	quit chan struct{}    // closed by shutdown to ask run() to exit promptly
	quitOnce sync.Once

	deadMu sync.Mutex
	deadFl bool
}

// startPlugin spawns the binary and launches the owner goroutine. The
// plugin process owns its own stderr; we inherit it so plugin diagnostics
// land in the daemon's log stream by default.
func startPlugin(binary string, extraEnv []string, logger *slog.Logger) (*pluginSession, error) {
	cmd := exec.Command(binary)
	if len(extraEnv) > 0 {
		// Compose: parent env + extras. We intentionally do not
		// strip parent env — plugin authors may rely on standard
		// shell vars.
		cmd.Env = append(cmd.Environ(), extraEnv...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	// stderr inherits to the parent for diagnostic visibility.
	cmd.Stderr = nil // os.Stderr by default? No — exec.Cmd default is no inheritance; use nil to discard.
	// Discard plugin stderr by default; loud plugins shouldn't pollute
	// the daemon log unless wired through Logger explicitly.
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("start: %w", err)
	}
	s := &pluginSession{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		logger: logger,
		work:   make(chan pluginRequest),
		done:   make(chan struct{}),
		quit:   make(chan struct{}),
	}
	go s.run()
	return s, nil
}

// run is the owner goroutine. One request in flight at a time; reads
// exactly one stdout line per request. Any I/O error tears down the
// session by closing s.done, which unblocks any goroutines waiting in
// exchange().
func (s *pluginSession) run() {
	defer close(s.done)
	defer s.markDead()

	reader := bufio.NewReader(s.stdout)

	for {
		var req pluginRequest
		select {
		case req = <-s.work:
		case <-s.quit:
			return
		}

		// Write request line.
		toWrite := append(req.bytes, '\n')
		if _, err := s.stdin.Write(toWrite); err != nil {
			req.reply <- pluginReply{err: fmt.Errorf("stdin write: %w", err)}
			s.logger.Debug("plugin stdin write failed", slog.Any("err", err))
			return
		}

		// Read one response line.
		line, err := readLine(reader)
		if err != nil {
			req.reply <- pluginReply{err: fmt.Errorf("stdout read: %w", err)}
			s.logger.Debug("plugin stdout read failed", slog.Any("err", err))
			return
		}
		var resp subprocessResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			req.reply <- pluginReply{err: fmt.Errorf("decode response: %w", err)}
			s.logger.Debug("plugin response decode failed", slog.Any("err", err), slog.String("line", string(line)))
			return
		}
		req.reply <- pluginReply{resp: resp}
	}
}

// readLine consumes one full line (possibly long) from the buffered
// reader, returning the bytes without the trailing newline. EOF before any
// newline is reported as io.ErrUnexpectedEOF when partial data was read,
// or io.EOF when the stream was empty.
func readLine(r *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		chunk, err := r.ReadSlice('\n')
		if len(chunk) > 0 {
			// ReadSlice's buffer is reused; copy before continuing.
			cp := make([]byte, len(chunk))
			copy(cp, chunk)
			out = append(out, cp...)
		}
		if err == nil {
			// Strip trailing \n (and \r if present).
			out = out[:len(out)-1]
			if n := len(out); n > 0 && out[n-1] == '\r' {
				out = out[:n-1]
			}
			return out, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(out) == 0 {
				return nil, io.EOF
			}
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
}

// exchange sends one request through the work channel, waits for the
// reply, and respects ctx cancellation. If ctx fires before the reply
// arrives we kill the process — this guarantees the next request gets a
// fresh plugin rather than waiting on a stuck one.
func (s *pluginSession) exchange(ctx context.Context, req subprocessRequest) (subprocessResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return subprocessResponse{}, fmt.Errorf("encode request: %w", err)
	}
	if bytesContainNewline(body) {
		// JSON encoding never produces a literal newline by default,
		// but defense-in-depth: a future struct field with a custom
		// marshaller could. JSONL framing breaks the moment a request
		// straddles two lines.
		return subprocessResponse{}, errors.New("subprocess: encoded request contains newline")
	}
	reply := make(chan pluginReply, 1)
	pr := pluginRequest{bytes: body, reply: reply}

	select {
	case s.work <- pr:
	case <-s.done:
		return subprocessResponse{}, errors.New("subprocess: plugin terminated")
	case <-ctx.Done():
		s.kill()
		return subprocessResponse{}, ctx.Err()
	}

	select {
	case rep := <-reply:
		return rep.resp, rep.err
	case <-ctx.Done():
		s.kill()
		return subprocessResponse{}, ctx.Err()
	}
}

// bytesContainNewline reports whether b includes a literal LF byte.
// JSON marshalling escapes \n as \\n inside strings, so this should
// always be false for our request struct, but we double-check.
func bytesContainNewline(b []byte) bool {
	for _, c := range b {
		if c == '\n' {
			return true
		}
	}
	return false
}

// kill sends SIGKILL (via Process.Kill) and closes pipes. Safe to call
// multiple times; subsequent calls are no-ops once the process has exited.
func (s *pluginSession) kill() {
	if s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	_ = s.stdin.Close()
	_ = s.stdout.Close()
}

// shutdown signals the owner goroutine to exit, closes stdin to give the
// plugin a clean EOF, and waits up to `grace` for the process to exit. On
// timeout it escalates to SIGKILL. Always reaps the process to avoid
// zombies. Pass grace=0 to skip the polite phase entirely (used on the
// crash path where we already know the plugin is broken).
func (s *pluginSession) shutdown(grace time.Duration) error {
	// Tell run() to exit; safe to call multiple times.
	s.quitOnce.Do(func() { close(s.quit) })

	// Close stdin to signal EOF to the plugin.
	_ = s.stdin.Close()

	if grace > 0 {
		exited := make(chan error, 1)
		go func() { exited <- s.cmd.Wait() }()
		select {
		case err := <-exited:
			_ = s.stdout.Close()
			<-s.done
			return err
		case <-time.After(grace):
			// fall through to kill
		}
	}
	if s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	_ = s.stdout.Close()
	_ = s.cmd.Wait()
	<-s.done
	return nil
}

// markDead flips the dead flag once the owner goroutine exits.
func (s *pluginSession) markDead() {
	s.deadMu.Lock()
	s.deadFl = true
	s.deadMu.Unlock()
}

// dead reports whether the owner goroutine has exited.
func (s *pluginSession) dead() bool {
	s.deadMu.Lock()
	defer s.deadMu.Unlock()
	return s.deadFl
}

// Subprocess Finish exchanges use one bounded JSONL request per call.
package ai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// DefaultSubprocessTimeout is the per-request default per spec §10.3.
const DefaultSubprocessTimeout = 30 * time.Second

// subprocessShutdownGrace is the budget given to a closed plugin before we
// escalate from EOF-on-stdin to SIGKILL.
const subprocessShutdownGrace = 5 * time.Second

// subprocessResponseByteCap bounds one JSONL response before decoding. It
// matches the OpenAI-compatible response cap and prevents a faulty local
// plugin from growing daemon memory without bound.
const subprocessResponseByteCap = 1 << 20

// LookPathFunc matches exec.LookPath's signature so tests can inject a
// fake binary lookup without touching the real $PATH.
type LookPathFunc func(string) (string, error)

// SubprocessOptions tunes a SubprocessProvider. Zero-valued fields fall
// back to safe defaults (Timeout=30s, LookPath=exec.LookPath, Logger=
// slog.Default, Stderr=~/.local/state/acd/plugin-<name>.log).
type SubprocessOptions struct {
	Timeout  time.Duration // per-request hard timeout; 0 -> DefaultSubprocessTimeout
	Logger   *slog.Logger  // optional; nil -> slog.Default
	Env      []string      // additional env entries appended to os.Environ
	LookPath LookPathFunc  // resolves binary path; nil -> exec.LookPath
	Stderr   io.Writer     // plugin stderr sink; nil -> per-plugin log file
}

// SubprocessProvider implements FinishTransport using a long-lived JSONL
// child process. Safe for concurrent ExchangeFinish calls from multiple
// goroutines: requests are serialised through a single owner goroutine.
type SubprocessProvider struct {
	name       string // logical plugin name (without acd-provider- prefix)
	binary     string // resolved absolute path to acd-provider-<name>
	resolveErr error  // sticky error from initial LookPath; surfaced from ExchangeFinish

	timeout time.Duration
	env     []string
	stderr  io.Writer
	logger  *slog.Logger

	mu     sync.Mutex // guards plugin/closed
	plugin *pluginSession
	closed bool
}

// NewSubprocessProvider resolves the plugin without spawning it. A lookup error
// is returned by construction validation or the first explicit exchange.
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
		stderr:  opts.Stderr,
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

func (p *SubprocessProvider) EncodeFinishRequest(request FinishPlanRequest) ([]byte, error) {
	requestType := "finish_plan"
	if request.Review != nil {
		requestType = "finish_evidence"
	}
	return json.Marshal(struct {
		Version       int               `json:"version"`
		RequestType   string            `json:"request_type"`
		SystemPrompt  string            `json:"system_prompt"`
		FinishRequest FinishPlanRequest `json:"finish_request"`
	}{Version: 1, RequestType: requestType, SystemPrompt: finishPrompt(request), FinishRequest: finishWireRequest(request)})
}

// ExchangeFinish deliberately bypasses capability negotiation and the legacy
// crash retry. A failed exchange consumes one caller-owned planning call.
func (p *SubprocessProvider) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	if p.resolveErr != nil {
		return nil, p.resolveErr
	}
	reqCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	session, err := p.acquire()
	if err != nil {
		return nil, err
	}
	response, err := session.exchangeBytes(reqCtx, body)
	if err != nil {
		p.markCrashed(session)
		var decodeError *subprocessResponseDecodeError
		var tooLarge *subprocessResponseTooLargeError
		if errors.As(err, &decodeError) && !errors.As(err, &tooLarge) {
			return nil, &finishOutputError{Err: err, Raw: decodeError.raw}
		}
		return nil, err
	}
	if response.Error != "" {
		return nil, errors.New("subprocess finish: " + SanitizePlannerError(response.Error))
	}
	var sent struct {
		RequestType string `json:"request_type"`
	}
	if json.Unmarshal(body, &sent) == nil && sent.RequestType == "finish_evidence" {
		if response.Version != 1 || len(response.FinishEvidence) == 0 {
			return nil, errors.New("subprocess provider must support version 1 finish_evidence for large batches")
		}
		return response.FinishEvidence, nil
	}
	if response.Version != 1 || len(response.FinishPlan) == 0 {
		return nil, &finishOutputError{Err: errors.New("subprocess finish requires version 1 and finish_plan"), Raw: response.FinishPlan}
	}
	return response.FinishPlan, nil
}

// Close shuts down the plugin process if running. Idempotent and safe to
// call from any goroutine. After Close, ExchangeFinish returns an error.
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
// guards the (plugin, closed) pair so concurrent ExchangeFinish calls only race
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
	session, err := startPlugin(p.name, p.binary, p.env, p.stderr, p.logger.With(slog.String("plugin", p.name)))
	if err != nil {
		return nil, fmt.Errorf("subprocess:%s: start: %w", p.name, err)
	}
	p.plugin = session
	return session, nil
}

// markCrashed tears down `session` if it is still the active plugin. The
// next ExchangeFinish will respawn through acquire().
func (p *SubprocessProvider) markCrashed(session *pluginSession) {
	p.mu.Lock()
	if p.plugin == session {
		p.plugin = nil
	}
	p.mu.Unlock()
	// shutdown is safe to call multiple times.
	_ = session.shutdown(0)
}

// subprocessResponse is the bounded Finish JSONL envelope.
type subprocessResponse struct {
	Version        int             `json:"version"`
	Error          string          `json:"error,omitempty"`
	FinishPlan     json.RawMessage `json:"finish_plan"`
	FinishEvidence json.RawMessage `json:"finish_evidence,omitempty"`
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

type subprocessResponseDecodeError struct {
	err error
	raw []byte
}

func (e *subprocessResponseDecodeError) Error() string {
	if e == nil || e.err == nil {
		return "decode response"
	}
	return "decode response: " + e.err.Error()
}

func (e *subprocessResponseDecodeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

type subprocessResponseTooLargeError struct {
	Limit int
}

func (e *subprocessResponseTooLargeError) Error() string {
	return fmt.Sprintf("response exceeds %d-byte limit", e.Limit)
}

// pluginSession owns a single child process plus the goroutine that
// serialises stdin writes and stdout reads. Lifecycle: startPlugin -> run
// -> shutdown. The dead flag is set when the owner goroutine exits, so
// callers can check liveness without racing with the process.
type pluginSession struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.Closer
	logger *slog.Logger

	work         chan pluginRequest
	done         chan struct{} // closed when run() exits
	quit         chan struct{} // closed by shutdown to ask run() to exit promptly
	quitOnce     sync.Once
	shutdownOnce sync.Once
	shutdownErr  error

	deadMu sync.Mutex
	deadFl bool
}

// startPlugin spawns the binary and launches the owner goroutine.
func startPlugin(name, binary string, extraEnv []string, stderr io.Writer, logger *slog.Logger) (*pluginSession, error) {
	cmd := exec.Command(binary)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// A descendant retaining stderr must not leave Wait blocked indefinitely.
	cmd.WaitDelay = time.Second
	if len(extraEnv) > 0 {
		// Add requested environment entries without replacing the parent environment.
		// We intentionally do not
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
	var stderrClose io.Closer
	if stderr == nil {
		stderr, stderrClose, err = openDefaultPluginStderr(name)
		if err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			return nil, fmt.Errorf("stderr log: %w", err)
		}
	}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		if stderrClose != nil {
			_ = stderrClose.Close()
		}
		return nil, fmt.Errorf("start: %w", err)
	}
	s := &pluginSession{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		stderr: stderrClose,
		logger: logger,
		work:   make(chan pluginRequest),
		done:   make(chan struct{}),
		quit:   make(chan struct{}),
	}
	go s.run()
	return s, nil
}

func openDefaultPluginStderr(name string) (io.Writer, io.Closer, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, nil, err
	}
	dir := filepath.Join(home, ".local", "state", "acd")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, "plugin-"+safePluginLogName(name)+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

func safePluginLogName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
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
		line, err := readLine(reader, subprocessResponseByteCap)
		if err != nil {
			var tooLarge *subprocessResponseTooLargeError
			if errors.As(err, &tooLarge) {
				req.reply <- pluginReply{err: &subprocessResponseDecodeError{
					err: err,
					raw: append([]byte(nil), line...),
				}}
			} else {
				req.reply <- pluginReply{err: fmt.Errorf("stdout read: %w", err)}
			}
			s.logger.Debug("plugin stdout read failed", slog.Any("err", err))
			return
		}
		var resp subprocessResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			req.reply <- pluginReply{err: &subprocessResponseDecodeError{
				err: err,
				raw: append([]byte(nil), line...),
			}}
			s.logger.Debug("plugin response decode failed", slog.Any("err", err), slog.Int("response_bytes", len(line)))
			return
		}
		req.reply <- pluginReply{resp: resp}
	}
}

// readLine consumes one full line (possibly long) from the buffered
// reader, returning the bytes without the trailing newline. EOF before any
// newline is reported as io.ErrUnexpectedEOF when partial data was read,
// or io.EOF when the stream was empty.
func readLine(r *bufio.Reader, limit int) ([]byte, error) {
	if limit <= 0 {
		return nil, &subprocessResponseTooLargeError{Limit: limit}
	}
	var out []byte
	for {
		chunk, err := r.ReadSlice('\n')
		complete := err == nil
		if complete {
			chunk = chunk[:len(chunk)-1]
			if n := len(chunk); n > 0 && chunk[n-1] == '\r' {
				chunk = chunk[:n-1]
			}
		}
		if len(chunk) > 0 {
			remaining := limit - len(out)
			if len(chunk) > remaining {
				out = append(out, chunk[:remaining]...)
				return out, &subprocessResponseTooLargeError{Limit: limit}
			}
			out = append(out, chunk...)
		}
		if complete {
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

func (s *pluginSession) exchangeBytes(ctx context.Context, body []byte) (subprocessResponse, error) {
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

// kill stops the plugin's process group and closes its pipes.
func (s *pluginSession) kill() {
	if s.cmd.Process != nil {
		if err := syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL); err != nil {
			_ = s.cmd.Process.Kill()
		}
	}
	_ = s.stdin.Close()
	_ = s.stdout.Close()
}

// shutdown signals the owner goroutine to exit, closes stdin to give the
// plugin a clean EOF, and waits up to `grace` for the process to exit. On
// timeout it kills the process group and bounds the cleanup wait. Pass
// grace=0 to skip the polite phase entirely (used on the
// crash path where we already know the plugin is broken).
func (s *pluginSession) shutdown(grace time.Duration) error {
	s.shutdownOnce.Do(func() {
		s.quitOnce.Do(func() { close(s.quit) })
		_ = s.stdin.Close()
		exited := make(chan error, 1)
		// This is the only owner of cmd.Wait, including concurrent shutdowns.
		go func() { exited <- s.cmd.Wait() }()
		if grace > 0 {
			select {
			case s.shutdownErr = <-exited:
				// Reap completed; stop any remaining descendants as well.
				s.kill()
				s.finishShutdown()
				return
			case <-time.After(grace):
			}
		}
		s.kill()
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
			s.shutdownErr = errors.New("subprocess: process cleanup exceeded its deadline")
		}
		s.finishShutdown()
	})
	return s.shutdownErr
}

func (s *pluginSession) finishShutdown() {
	if s.stderr != nil {
		_ = s.stderr.Close()
	}
	select {
	case <-s.done:
	case <-time.After(time.Second):
		s.shutdownErr = errors.Join(s.shutdownErr, errors.New("subprocess: response reader cleanup exceeded its deadline"))
	}
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

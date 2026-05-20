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
// the contract every harness must speak. Diff text is empty unless the daemon
// opted in to sending diffs, then redacted and truncated before it reaches the
// plugin. The `version` field exists so future shapes can be negotiated without
// breaking older plugins.
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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
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
// slog.Default, Stderr=~/.local/state/acd/plugin-<name>.log).
type SubprocessOptions struct {
	Timeout  time.Duration // per-request hard timeout; 0 -> DefaultSubprocessTimeout
	Logger   *slog.Logger  // optional; nil -> slog.Default
	Env      []string      // additional env entries appended to os.Environ
	LookPath LookPathFunc  // resolves binary path; nil -> exec.LookPath
	Stderr   io.Writer     // plugin stderr sink; nil -> per-plugin log file
}

// SubprocessProvider implements Provider by talking JSONL to a long-lived
// child process. Safe for concurrent Generate calls from multiple
// goroutines: requests are serialised through a single owner goroutine.
type SubprocessProvider struct {
	name       string // logical plugin name (without acd-provider- prefix)
	binary     string // resolved absolute path to acd-provider-<name>
	resolveErr error  // sticky error from initial LookPath; surfaced from Generate

	timeout time.Duration
	env     []string
	stderr  io.Writer
	logger  *slog.Logger

	mu     sync.Mutex // guards plugin/closed
	plugin *pluginSession
	closed bool
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

	req := subprocessRequest{
		Version:  pluginProtocolVersion,
		Path:     cc.Path,
		Op:       cc.Op,
		OldPath:  cc.OldPath,
		Diff:     Truncate(RedactDiffSecrets(cc.DiffText), DiffCap),
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
	requestBody, transform, err := marshalSubprocessPromptRequest(req, cc.DiffText, req.Diff)
	if err != nil {
		return Result{}, err
	}
	p.recordSubprocessRequest(ctx, requestBody, transform, prompttrace.Metadata{
		Strategy:     "event",
		DiffIncluded: req.Diff != "",
		DiffCap:      DiffCap,
	})

	// Per-request timeout layered on top of the caller's ctx.
	reqCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Two-attempt loop: an exited-but-not-yet-detected plugin produces
	// EPIPE / EOF on the first I/O; we mark crashed, respawn, and retry
	// exactly once. A second crash on the fresh process surfaces as an
	// error so Compose() falls back to deterministic.
	var resp subprocessResponse
	for attempt := 0; attempt < 2; attempt++ {
		var session *pluginSession
		session, err = p.acquire()
		if err != nil {
			p.recordSubprocessResponse(ctx, "event", prompttrace.Response{Error: err.Error()})
			return Result{}, err
		}
		resp, err = session.exchangeBytes(reqCtx, requestBody)
		if err == nil {
			break
		}
		// Context errors are not the plugin's fault; never retry.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			p.markCrashed(session)
			p.recordSubprocessResponse(ctx, "event", prompttrace.Response{Error: err.Error()})
			return Result{}, err
		}
		p.markCrashed(session)
		// Loop and retry once with a fresh process.
	}
	if err != nil {
		p.recordSubprocessResponse(ctx, "event", prompttrace.Response{Error: err.Error()})
		return Result{}, err
	}
	if strings.TrimSpace(resp.Error) != "" {
		// Soft fail: plugin is healthy, just couldn't satisfy this
		// particular request. Compose() will fall back.
		err := fmt.Errorf("subprocess:%s: %s", p.name, resp.Error)
		p.recordSubprocessResponse(ctx, "event", prompttrace.Response{Error: err.Error()})
		return Result{}, err
	}

	composed := resp.Subject
	if strings.TrimSpace(resp.Body) != "" {
		composed = resp.Subject + "\n\n" + resp.Body
	}
	cleaned := SanitizeMessage(composed)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	subj := parts[0]
	var bodyOut string
	if len(parts) == 2 {
		bodyOut = parts[1]
	}
	if strings.TrimSpace(subj) == "" {
		err := fmt.Errorf("subprocess:%s: empty subject after sanitize", p.name)
		p.recordSubprocessResponse(ctx, "event", prompttrace.Response{ValidationError: err.Error()})
		return Result{}, err
	}
	p.recordSubprocessResponse(ctx, "event", prompttrace.Response{Subject: subj, Body: bodyOut})
	return Result{
		Subject: subj,
		Body:    bodyOut,
		Source:  p.Name(),
	}, nil
}

// PlanIntent asks the subprocess plugin to select/defer every offered capture.
// The JSONL request uses request_type=intent_plan and carries the shared
// IntentPlanRequest payload; response fields mirror IntentPlan.
func (p *SubprocessProvider) PlanIntent(ctx context.Context, plannerReq IntentPlanRequest) (IntentPlan, error) {
	if err := ctx.Err(); err != nil {
		return IntentPlan{}, err
	}
	if p.resolveErr != nil {
		return IntentPlan{}, p.resolveErr
	}

	req := subprocessRequest{
		Version:        pluginProtocolVersion,
		RequestType:    "intent_plan",
		PlannerRequest: &plannerReq,
	}
	body, err := marshalSubprocessRequest(req)
	if err != nil {
		return IntentPlan{}, err
	}
	p.recordSubprocessRequest(ctx, body, plannerReq.CapturedDiffTransform, prompttrace.Metadata{
		Strategy:     "intent",
		OfferedSeqs:  offeredSeqs(plannerReq),
		DiffIncluded: intentDiffIncluded(plannerReq),
		// Intent stage uses IntentStageDiffCap so the planner sees enough
		// per-capture context to group multi-file changes; per-event
		// commit messages still cap at the legacy DiffCap.
		DiffCap: IntentStageDiffCap,
	})

	reqCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	var resp subprocessResponse
	for attempt := 0; attempt < 2; attempt++ {
		var session *pluginSession
		session, err = p.acquire()
		if err != nil {
			p.recordSubprocessResponse(ctx, "intent", prompttrace.Response{Error: err.Error()})
			return IntentPlan{}, err
		}
		resp, err = session.exchangeBytes(reqCtx, body)
		if err == nil {
			break
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			p.markCrashed(session)
			p.recordSubprocessResponse(ctx, "intent", prompttrace.Response{Error: err.Error()})
			return IntentPlan{}, err
		}
		p.markCrashed(session)
	}
	if err != nil {
		p.recordSubprocessResponse(ctx, "intent", prompttrace.Response{Error: err.Error()})
		return IntentPlan{}, err
	}
	if strings.TrimSpace(resp.Error) != "" {
		err := fmt.Errorf("subprocess:%s: %s", p.name, resp.Error)
		p.recordSubprocessResponse(ctx, "intent", prompttrace.Response{Error: err.Error()})
		return IntentPlan{}, err
	}

	plan := IntentPlan{
		SelectedSeqs:    resp.SelectedSeqs,
		DeferredSeqs:    resp.DeferredSeqs,
		Subject:         resp.Subject,
		Body:            resp.Body,
		GroupingReason:  resp.GroupingReason,
		DeferredReasons: resp.DeferredReasons,
		Source:          p.Name(),
	}
	if strings.TrimSpace(plan.Subject) == "" {
		err := fmt.Errorf("subprocess:%s: intent plan returned empty subject", p.name)
		p.recordSubprocessResponse(ctx, "intent", prompttrace.Response{ValidationError: err.Error()})
		return IntentPlan{}, err
	}
	cleaned := SanitizeMessage(plan.Subject + "\n\n" + plan.Body)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	plan.Subject = parts[0]
	if len(parts) == 2 {
		plan.Body = parts[1]
	} else {
		plan.Body = ""
	}
	plan = NormalizeIntentPlanReasons(plan)
	plan, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	if len(dropped) > 0 || len(synthesized) > 0 || len(overlapRemoved) > 0 {
		attrs := []any{
			slog.String("provider", p.Name()),
			slog.String("plugin", p.name),
		}
		if len(dropped) > 0 {
			attrs = append(attrs, slog.Any("dropped_seqs", dropped))
		}
		if len(synthesized) > 0 {
			attrs = append(attrs, slog.Any("synthesized_seqs", synthesized))
		}
		if len(overlapRemoved) > 0 {
			attrs = append(attrs, slog.Any("overlap_removed_seqs", overlapRemoved))
		}
		p.logger.Warn("intent planner: normalized deferred_reasons", attrs...)
	}
	if err := ValidateIntentPlan(plannerReq, plan); err != nil {
		p.recordSubprocessResponse(ctx, "intent", prompttrace.Response{ValidationError: err.Error()})
		// Best-effort: serialize the plugin response for forensic replay.
		// Marshal failure means the raw payload stays empty in the rejects
		// log entry; the validator code/message are still persisted so an
		// operator can always recover the validation context.
		var rawBytes []byte
		if marshaled, mErr := json.Marshal(resp); mErr == nil {
			rawBytes = marshaled
		}
		LogRejectedIntentPlan(ctx, p.Name(), plannerReq, string(rawBytes), err)
		return IntentPlan{}, err
	}
	p.recordSubprocessResponse(ctx, "intent", prompttrace.Response{
		Subject:        plan.Subject,
		Body:           plan.Body,
		SelectedSeqs:   plan.SelectedSeqs,
		DeferredSeqs:   plan.DeferredSeqs,
		GroupingReason: plan.GroupingReason,
	})
	return plan, nil
}

// RewriteIntentMessage asks the plugin to rewrite only subject/body for a
// locked intent plan. The request_type keeps this distinct from intent_plan so
// plugins cannot accidentally change grouping fields.
func (p *SubprocessProvider) RewriteIntentMessage(ctx context.Context, rewriteReq IntentMessageRewriteRequest) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	ctx = withPromptTraceStrategy(ctx, "intent_message_rewrite", rewriteReq.LockedPlan.SelectedSeqs, intentDiffIncluded(rewriteReq.PlannerRequest), IntentStageDiffCap)
	if p.resolveErr != nil {
		return Result{}, p.resolveErr
	}

	req := subprocessRequest{
		Version:               pluginProtocolVersion,
		RequestType:           "intent_message_rewrite",
		MessageRewriteRequest: &rewriteReq,
	}
	body, err := marshalSubprocessRequest(req)
	if err != nil {
		return Result{}, err
	}
	p.recordSubprocessRequest(ctx, body, rewriteReq.PlannerRequest.CapturedDiffTransform, prompttrace.Metadata{
		Strategy:     "intent_message_rewrite",
		OfferedSeqs:  append([]int64(nil), rewriteReq.LockedPlan.SelectedSeqs...),
		DiffIncluded: intentDiffIncluded(rewriteReq.PlannerRequest),
		DiffCap:      IntentStageDiffCap,
	})

	reqCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	var resp subprocessResponse
	for attempt := 0; attempt < 2; attempt++ {
		var session *pluginSession
		session, err = p.acquire()
		if err != nil {
			p.recordSubprocessResponse(ctx, "intent_message_rewrite", prompttrace.Response{Error: err.Error()})
			return Result{}, err
		}
		resp, err = session.exchangeBytes(reqCtx, body)
		if err == nil {
			break
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			p.markCrashed(session)
			p.recordSubprocessResponse(ctx, "intent_message_rewrite", prompttrace.Response{Error: err.Error()})
			return Result{}, err
		}
		p.markCrashed(session)
	}
	if err != nil {
		p.recordSubprocessResponse(ctx, "intent_message_rewrite", prompttrace.Response{Error: err.Error()})
		return Result{}, err
	}
	if strings.TrimSpace(resp.Error) != "" {
		err := fmt.Errorf("subprocess:%s: %s", p.name, resp.Error)
		p.recordSubprocessResponse(ctx, "intent_message_rewrite", prompttrace.Response{Error: err.Error()})
		return Result{}, err
	}

	cleaned := SanitizeMessage(resp.Subject + "\n\n" + resp.Body)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	result := Result{Subject: parts[0], Source: p.Name()}
	if len(parts) == 2 {
		result.Body = parts[1]
	}
	if strings.TrimSpace(result.Subject) == "" {
		err := fmt.Errorf("subprocess:%s: empty subject after intent message rewrite sanitize", p.name)
		p.recordSubprocessResponse(ctx, "intent_message_rewrite", prompttrace.Response{ValidationError: err.Error()})
		return Result{}, err
	}
	p.recordSubprocessResponse(ctx, "intent_message_rewrite", prompttrace.Response{Subject: result.Subject, Body: result.Body})
	return result, nil
}

// ProposeCommitRewrite asks the plugin to rewrite one existing commit message.
func (p *SubprocessProvider) ProposeCommitRewrite(ctx context.Context, rewriteReq CommitRewriteRequest) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if p.resolveErr != nil {
		return Result{}, p.resolveErr
	}
	req := subprocessRequest{Version: pluginProtocolVersion, RequestType: "commit_rewrite_proposal", CommitRewriteRequest: &rewriteReq}
	body, err := marshalSubprocessRequest(req)
	if err != nil {
		return Result{}, err
	}
	reqCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var resp subprocessResponse
	for attempt := 0; attempt < 2; attempt++ {
		session, err := p.acquire()
		if err != nil {
			return Result{}, err
		}
		resp, err = session.exchangeBytes(reqCtx, body)
		if err == nil {
			break
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			p.markCrashed(session)
			return Result{}, err
		}
		p.markCrashed(session)
	}
	if err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(resp.Error) != "" {
		return Result{}, fmt.Errorf("subprocess:%s: %s", p.name, resp.Error)
	}
	return ValidateCommitRewriteProposal(rewriteReq, Result{Subject: resp.Subject, Body: resp.Body, Source: p.Name()})
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
	session, err := startPlugin(p.name, p.binary, p.env, p.stderr, p.logger.With(slog.String("plugin", p.name)))
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

func (p *SubprocessProvider) recordSubprocessRequest(ctx context.Context, body []byte, transform prompttrace.TransformMetadata, fallback prompttrace.Metadata) {
	logger, meta, ok := prompttrace.From(ctx)
	if !ok {
		return
	}
	if meta.Strategy == "" {
		meta.Strategy = fallback.Strategy
	}
	if len(meta.OfferedSeqs) == 0 {
		meta.OfferedSeqs = append([]int64(nil), fallback.OfferedSeqs...)
	}
	if meta.DiffCap == 0 {
		meta.DiffCap = fallback.DiffCap
	}
	meta.DiffIncluded = meta.DiffIncluded || fallback.DiffIncluded
	meta = promptTraceMetadata(meta, p.Name(), "")
	logger.Record(prompttrace.Record{
		Stage:              "request",
		Strategy:           meta.Strategy,
		Provider:           meta.Provider,
		Model:              meta.Model,
		Seq:                meta.Seq,
		OfferedSeqs:        append([]int64(nil), meta.OfferedSeqs...),
		BranchRef:          meta.BranchRef,
		Generation:         meta.Generation,
		DiffIncluded:       meta.DiffIncluded,
		DiffCap:            meta.DiffCap,
		Transform:          transform,
		SubprocessEnvelope: append([]byte(nil), body...),
	})
}

func (p *SubprocessProvider) recordSubprocessResponse(ctx context.Context, strategy string, response prompttrace.Response) {
	logger, meta, ok := prompttrace.From(ctx)
	if !ok {
		return
	}
	if meta.Strategy == "" {
		meta.Strategy = strategy
	}
	meta = promptTraceMetadata(meta, p.Name(), "")
	logger.Record(prompttrace.Record{
		Stage:        "response",
		Strategy:     meta.Strategy,
		Provider:     meta.Provider,
		Model:        meta.Model,
		Seq:          meta.Seq,
		OfferedSeqs:  append([]int64(nil), meta.OfferedSeqs...),
		BranchRef:    meta.BranchRef,
		Generation:   meta.Generation,
		DiffIncluded: meta.DiffIncluded,
		DiffCap:      meta.DiffCap,
		Response:     &response,
	})
}

// subprocessRequest is the JSONL request envelope. Field tags fix the wire
// names so the JSON shape matches the contract regardless of struct
// renames.
type subprocessRequest struct {
	Version               int                          `json:"version"`
	RequestType           string                       `json:"request_type,omitempty"`
	Path                  string                       `json:"path,omitempty"`
	Op                    string                       `json:"op,omitempty"`
	OldPath               string                       `json:"old_path,omitempty"`
	Diff                  string                       `json:"diff,omitempty"`
	RepoRoot              string                       `json:"repo_root,omitempty"`
	Branch                string                       `json:"branch,omitempty"`
	MultiOp               []subprocessOp               `json:"multi_op,omitempty"`
	Now                   string                       `json:"now,omitempty"`
	PlannerRequest        *IntentPlanRequest           `json:"planner_request,omitempty"`
	MessageRewriteRequest *IntentMessageRewriteRequest `json:"message_rewrite_request,omitempty"`
	CommitRewriteRequest  *CommitRewriteRequest        `json:"commit_rewrite_request,omitempty"`
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
	Version         int              `json:"version"`
	Subject         string           `json:"subject"`
	Body            string           `json:"body"`
	Error           string           `json:"error"`
	SelectedSeqs    []int64          `json:"selected_seqs,omitempty"`
	DeferredSeqs    []int64          `json:"deferred_seqs,omitempty"`
	GroupingReason  string           `json:"grouping_reason,omitempty"`
	DeferredReasons []DeferredReason `json:"deferred_reasons,omitempty"`
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
	stderr io.Closer
	logger *slog.Logger

	work     chan pluginRequest
	done     chan struct{} // closed when run() exits
	quit     chan struct{} // closed by shutdown to ask run() to exit promptly
	quitOnce sync.Once

	deadMu sync.Mutex
	deadFl bool
}

// startPlugin spawns the binary and launches the owner goroutine.
func startPlugin(name, binary string, extraEnv []string, stderr io.Writer, logger *slog.Logger) (*pluginSession, error) {
	cmd := exec.Command(binary)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
	body, err := marshalSubprocessRequest(req)
	if err != nil {
		return subprocessResponse{}, err
	}
	return s.exchangeBytes(ctx, body)
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

func marshalSubprocessPromptRequest(req subprocessRequest, inputDiff, outputDiff string) ([]byte, prompttrace.TransformMetadata, error) {
	body, err := marshalSubprocessRequest(req)
	if err != nil {
		return nil, prompttrace.TransformMetadata{}, err
	}
	redacted := RedactDiffSecrets(inputDiff)
	return body, promptTransformMetadata(inputDiff, redacted, outputDiff), nil
}

func marshalSubprocessRequest(req subprocessRequest) ([]byte, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	if bytesContainNewline(body) {
		// JSON encoding never produces a literal newline by default,
		// but defense-in-depth: a future struct field with a custom
		// marshaller could. JSONL framing breaks the moment a request
		// straddles two lines.
		return nil, errors.New("subprocess: encoded request contains newline")
	}
	return body, nil
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
		if err := syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL); err != nil {
			_ = s.cmd.Process.Kill()
		}
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
			if s.stderr != nil {
				_ = s.stderr.Close()
			}
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
	if s.stderr != nil {
		_ = s.stderr.Close()
	}
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

// Package prompttrace persists opt-in AI provider request diagnostics under
// repo-local ACD state. Records are written only when ACD_AI_PROMPT_TRACE is
// enabled; request payloads are captured after provider redaction/truncation,
// but may still contain source text and should be treated as sensitive local
// diagnostics.
package prompttrace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// EnvTrace enables local AI prompt persistence when set to 1, true, or yes.
	EnvTrace = "ACD_AI_PROMPT_TRACE"

	DefaultCapacity = 256
)

type Logger interface {
	Record(Record)
	Close() error
	Dropped() uint64
}

type Metadata struct {
	Strategy         string
	Protocol         string
	Provider         string
	Model            string
	Seq              int64
	OfferedSeqs      []int64
	BranchRef        string
	Generation       int64
	DiffIncluded     bool
	DiffCap          int
	ConfigRevisionID int64
	ConfigProfile    string
	RetryCount       int
}

type TransformMetadata struct {
	RedactionApplied bool `json:"redaction_applied"`
	Truncated        bool `json:"truncated"`
	InputBytes       int  `json:"input_bytes"`
	RedactedBytes    int  `json:"redacted_bytes"`
	OutputBytes      int  `json:"output_bytes"`
}

type Response struct {
	StatusCode       int     `json:"status_code,omitempty"`
	Subject          string  `json:"subject,omitempty"`
	Body             string  `json:"body,omitempty"`
	SelectedSeqs     []int64 `json:"selected_seqs,omitempty"`
	DeferredSeqs     []int64 `json:"deferred_seqs,omitempty"`
	GroupingReason   string  `json:"grouping_reason,omitempty"`
	ValidationError  string  `json:"validation_error,omitempty"`
	Error            string  `json:"error,omitempty"`
	FallbackProvider string  `json:"fallback_provider,omitempty"`
	FallbackReason   string  `json:"fallback_reason,omitempty"`
}

type Record struct {
	TS                 time.Time         `json:"-"`
	Repo               string            `json:"repo,omitempty"`
	Stage              string            `json:"stage"`
	Strategy           string            `json:"strategy,omitempty"`
	Protocol           string            `json:"protocol,omitempty"`
	Provider           string            `json:"provider,omitempty"`
	Model              string            `json:"model,omitempty"`
	Seq                int64             `json:"seq,omitempty"`
	OfferedSeqs        []int64           `json:"offered_seqs,omitempty"`
	BranchRef          string            `json:"branch_ref,omitempty"`
	Generation         int64             `json:"generation,omitempty"`
	DiffIncluded       bool              `json:"diff_included"`
	DiffCap            int               `json:"diff_cap,omitempty"`
	ConfigRevisionID   int64             `json:"config_revision_id,omitempty"`
	ConfigProfile      string            `json:"config_profile,omitempty"`
	RetryCount         int               `json:"retry_count,omitempty"`
	Transform          TransformMetadata `json:"transform,omitempty"`
	SystemMessage      string            `json:"system_message,omitempty"`
	UserMessage        string            `json:"user_message,omitempty"`
	ToolSchema         any               `json:"tool_schema,omitempty"`
	Request            json.RawMessage   `json:"request,omitempty"`
	SubprocessEnvelope json.RawMessage   `json:"subprocess_envelope,omitempty"`
	Response           *Response         `json:"response,omitempty"`
	Error              string            `json:"error,omitempty"`
}

type Options struct {
	Repo     string
	GitDir   string
	Dir      string
	Capacity int
	Now      func() time.Time

	skipWorker bool
}

type Noop struct{}

func (Noop) Record(Record)   {}
func (Noop) Close() error    { return nil }
func (Noop) Dropped() uint64 { return 0 }

func EnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvTrace))) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func FromEnv(repo, gitDir string) Logger {
	l, _ := NewFromEnv(repo, gitDir)
	return l
}

func NewFromEnv(repo, gitDir string) (Logger, error) {
	if !EnabledFromEnv() {
		return nil, nil
	}
	w, err := New(Options{Repo: repo, GitDir: gitDir})
	if err != nil {
		return nil, err
	}
	return w, nil
}

func New(opts Options) (*Writer, error) {
	dir := opts.Dir
	if dir == "" {
		if opts.GitDir == "" {
			return nil, fmt.Errorf("prompttrace: Dir or GitDir required")
		}
		dir = filepath.Join(opts.GitDir, "acd", "prompt-trace")
	}
	if err := ensureTraceDir(dir); err != nil {
		return nil, err
	}
	capacity := opts.Capacity
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	w := &Writer{
		repo: opts.Repo,
		dir:  dir,
		now:  now,
		ch:   make(chan Record, capacity),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	if opts.skipWorker {
		close(w.done)
		return w, nil
	}
	go w.run()
	return w, nil
}

type Writer struct {
	repo string
	dir  string
	now  func() time.Time

	ch   chan Record
	stop chan struct{}
	done chan struct{}

	enqueueMu sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
	dropped   atomic.Uint64
}

func (w *Writer) Record(rec Record) {
	if w == nil {
		return
	}
	if rec.TS.IsZero() {
		rec.TS = w.now().UTC()
	} else {
		rec.TS = rec.TS.UTC()
	}
	if rec.Repo == "" {
		rec.Repo = w.repo
	}

	w.enqueueMu.Lock()
	defer w.enqueueMu.Unlock()
	if w.closed {
		return
	}
	select {
	case w.ch <- rec:
		return
	default:
	}
	select {
	case <-w.ch:
		w.dropped.Add(1)
	default:
	}
	select {
	case w.ch <- rec:
	default:
		w.dropped.Add(1)
	}
}

func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		w.enqueueMu.Lock()
		w.closed = true
		w.enqueueMu.Unlock()
		close(w.stop)
		<-w.done
	})
	return w.closeErr
}

func (w *Writer) Dropped() uint64 {
	if w == nil {
		return 0
	}
	return w.dropped.Load()
}

func (w *Writer) run() {
	var active *os.File
	var activeDay string
	defer func() {
		if active != nil {
			w.rememberErr(active.Close())
		}
		close(w.done)
	}()

	write := func(rec Record) {
		day := rec.TS.UTC().Format("2006-01-02")
		if active == nil || activeDay != day {
			if active != nil {
				w.rememberErr(active.Close())
				active = nil
			}
			if err := ensureTraceDir(w.dir); err != nil {
				w.rememberErr(err)
				return
			}
			f, err := openRegularNoFollow(filepath.Join(w.dir, day+".jsonl"), syscall.O_APPEND|syscall.O_CREAT|syscall.O_WRONLY, 0o600)
			if err != nil {
				w.rememberErr(err)
				return
			}
			active = f
			activeDay = day
		}
		line, err := marshalRecord(rec)
		if err != nil {
			w.rememberErr(err)
			return
		}
		if _, err := active.Write(append(line, '\n')); err != nil {
			w.rememberErr(err)
		}
	}

	for {
		select {
		case rec := <-w.ch:
			write(rec)
		case <-w.stop:
			for {
				select {
				case rec := <-w.ch:
					write(rec)
				default:
					return
				}
			}
		}
	}
}

func (w *Writer) rememberErr(err error) {
	if err != nil && w.closeErr == nil {
		w.closeErr = err
	}
}

func ensureTraceDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("prompttrace: mkdir trace dir: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("prompttrace: stat trace dir: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("prompttrace: trace dir must not be a symlink: %s", dir)
	}
	if !info.IsDir() {
		return fmt.Errorf("prompttrace: trace dir is not a directory: %s", dir)
	}
	return nil
}

func openRegularNoFollow(path string, flags int, perm uint32) (*os.File, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("prompttrace: trace file must not be a symlink: %s", path)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("prompttrace: trace file is not regular: %s", path)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("prompttrace: stat trace file: %w", err)
	}

	fd, err := syscall.Open(path, flags|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, perm)
	if err != nil {
		return nil, fmt.Errorf("prompttrace: open trace file: %w", err)
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("prompttrace: open trace file: invalid fd")
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("prompttrace: stat opened trace file: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("prompttrace: opened trace file is not regular: %s", path)
	}
	return f, nil
}

type jsonRecord struct {
	TS                 string            `json:"ts"`
	Repo               string            `json:"repo,omitempty"`
	Stage              string            `json:"stage"`
	Strategy           string            `json:"strategy,omitempty"`
	Protocol           string            `json:"protocol,omitempty"`
	Provider           string            `json:"provider,omitempty"`
	Model              string            `json:"model,omitempty"`
	Seq                int64             `json:"seq,omitempty"`
	OfferedSeqs        []int64           `json:"offered_seqs,omitempty"`
	BranchRef          string            `json:"branch_ref,omitempty"`
	Generation         int64             `json:"generation,omitempty"`
	DiffIncluded       bool              `json:"diff_included"`
	DiffCap            int               `json:"diff_cap,omitempty"`
	ConfigRevisionID   int64             `json:"config_revision_id,omitempty"`
	ConfigProfile      string            `json:"config_profile,omitempty"`
	RetryCount         int               `json:"retry_count,omitempty"`
	Transform          TransformMetadata `json:"transform,omitempty"`
	SystemMessage      string            `json:"system_message,omitempty"`
	UserMessage        string            `json:"user_message,omitempty"`
	ToolSchema         any               `json:"tool_schema,omitempty"`
	Request            json.RawMessage   `json:"request,omitempty"`
	SubprocessEnvelope json.RawMessage   `json:"subprocess_envelope,omitempty"`
	Response           *Response         `json:"response,omitempty"`
	Error              string            `json:"error,omitempty"`
}

func marshalRecord(rec Record) ([]byte, error) {
	if rec.TS.IsZero() {
		return nil, errors.New("prompttrace: missing timestamp")
	}
	return json.Marshal(jsonRecord{
		TS:                 rec.TS.UTC().Format(time.RFC3339Nano),
		Repo:               rec.Repo,
		Stage:              rec.Stage,
		Strategy:           rec.Strategy,
		Protocol:           rec.Protocol,
		Provider:           rec.Provider,
		Model:              rec.Model,
		Seq:                rec.Seq,
		OfferedSeqs:        rec.OfferedSeqs,
		BranchRef:          rec.BranchRef,
		Generation:         rec.Generation,
		DiffIncluded:       rec.DiffIncluded,
		DiffCap:            rec.DiffCap,
		ConfigRevisionID:   rec.ConfigRevisionID,
		ConfigProfile:      rec.ConfigProfile,
		RetryCount:         rec.RetryCount,
		Transform:          rec.Transform,
		SystemMessage:      rec.SystemMessage,
		UserMessage:        rec.UserMessage,
		ToolSchema:         rec.ToolSchema,
		Request:            rec.Request,
		SubprocessEnvelope: rec.SubprocessEnvelope,
		Response:           rec.Response,
		Error:              rec.Error,
	})
}

type contextKey struct{}

type Context struct {
	Logger Logger
	Meta   Metadata
}

func With(ctx context.Context, logger Logger, meta Metadata) context.Context {
	if loggerDisabled(logger) {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, Context{Logger: logger, Meta: meta})
}

// WithRetryCount preserves the active prompt logger and annotates one
// correction attempt without changing any other immutable replay metadata.
func WithRetryCount(ctx context.Context, retryCount int) context.Context {
	if retryCount < 0 {
		retryCount = 0
	}
	current, ok := ctx.Value(contextKey{}).(Context)
	if !ok || loggerDisabled(current.Logger) {
		return ctx
	}
	current.Meta.RetryCount = retryCount
	return context.WithValue(ctx, contextKey{}, current)
}

func From(ctx context.Context) (Logger, Metadata, bool) {
	v, ok := ctx.Value(contextKey{}).(Context)
	if !ok || loggerDisabled(v.Logger) {
		return nil, Metadata{}, false
	}
	return v.Logger, v.Meta, true
}

func loggerDisabled(logger Logger) bool {
	if logger == nil {
		return true
	}
	switch logger.(type) {
	case Noop, *Noop:
		return true
	default:
		return false
	}
}

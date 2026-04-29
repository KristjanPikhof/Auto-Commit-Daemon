package trace

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// EnvTrace enables trace output when set to 1, true, or yes.
	EnvTrace = "ACD_TRACE"
	// EnvTraceDir overrides the default <git_dir>/acd/trace directory.
	EnvTraceDir = "ACD_TRACE_DIR"

	DefaultCapacity = 1024
)

// Logger is the trace sink used by daemon/replay/capture code. Implementations
// are best-effort: callers should never branch on trace write failures.
type Logger interface {
	Record(Event)
	Close() error
	Dropped() uint64
}

// Event is one decision record before JSON encoding.
type Event struct {
	TS         time.Time
	Repo       string
	BranchRef  string
	HeadSHA    string
	EventClass string
	Decision   string
	Reason     string
	Input      any
	Output     any
	Error      string
	Seq        int64
	Generation int64
}

// Options configures an enabled writer. Dir may be left empty when GitDir is
// set; the default location is <git_dir>/acd/trace.
type Options struct {
	Repo     string
	GitDir   string
	Dir      string
	Capacity int
	Now      func() time.Time

	skipWorker bool
}

// Noop is returned when tracing is disabled. Its methods intentionally do the
// minimum possible work on hot paths.
type Noop struct{}

func (Noop) Record(Event)    {}
func (Noop) Close() error    { return nil }
func (Noop) Dropped() uint64 { return 0 }

// EnabledFromEnv reports whether ACD_TRACE is truthy.
func EnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvTrace))) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

// FromEnv creates an enabled writer only when ACD_TRACE is truthy. Setup
// failures fall back to Noop so trace availability never aborts daemon work.
func FromEnv(repo, gitDir string) Logger {
	if !EnabledFromEnv() {
		return Noop{}
	}
	opts := Options{
		Repo:   repo,
		GitDir: gitDir,
		Dir:    strings.TrimSpace(os.Getenv(EnvTraceDir)),
	}
	w, err := New(opts)
	if err != nil {
		return Noop{}
	}
	return w
}

// New starts a non-blocking JSONL trace writer.
func New(opts Options) (*Writer, error) {
	dir := opts.Dir
	if dir == "" {
		if opts.GitDir == "" {
			return nil, fmt.Errorf("trace: Dir or GitDir required")
		}
		dir = filepath.Join(opts.GitDir, "acd", "trace")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("trace: mkdir trace dir: %w", err)
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
		ch:   make(chan Event, capacity),
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

// Writer owns the async channel and file handle for enabled tracing.
type Writer struct {
	repo string
	dir  string
	now  func() time.Time

	ch   chan Event
	stop chan struct{}
	done chan struct{}

	enqueueMu sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
	dropped   atomic.Uint64
}

// Record queues an event without waiting for disk I/O. When the buffer is
// already full, the oldest buffered event is discarded and counted.
func (w *Writer) Record(ev Event) {
	if w == nil {
		return
	}
	if ev.TS.IsZero() {
		ev.TS = w.now().UTC()
	} else {
		ev.TS = ev.TS.UTC()
	}
	if ev.Repo == "" {
		ev.Repo = w.repo
	}

	w.enqueueMu.Lock()
	defer w.enqueueMu.Unlock()
	if w.closed {
		return
	}
	select {
	case w.ch <- ev:
		return
	default:
	}
	select {
	case <-w.ch:
		w.dropped.Add(1)
	default:
	}
	select {
	case w.ch <- ev:
	default:
		w.dropped.Add(1)
	}
}

// Close stops the worker after draining queued events. Any write/close error
// is returned for diagnostics only; trace callers should keep treating writes
// as best-effort.
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

// Dropped returns the number of records discarded because the channel was full.
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

	write := func(ev Event) {
		day := ev.TS.UTC().Format("2006-01-02")
		if active == nil || activeDay != day {
			if active != nil {
				w.rememberErr(active.Close())
				active = nil
			}
			if err := os.MkdirAll(w.dir, 0o700); err != nil {
				w.rememberErr(err)
				return
			}
			f, err := os.OpenFile(filepath.Join(w.dir, day+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
			if err != nil {
				w.rememberErr(err)
				return
			}
			active = f
			activeDay = day
		}
		line, err := marshalEvent(ev)
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
		case ev := <-w.ch:
			write(ev)
		case <-w.stop:
			for {
				select {
				case ev := <-w.ch:
					write(ev)
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

type jsonEvent struct {
	TS         string `json:"ts"`
	Repo       string `json:"repo"`
	BranchRef  string `json:"branch_ref"`
	HeadSHA    string `json:"head_sha"`
	EventClass string `json:"event_class"`
	Decision   string `json:"decision"`
	Reason     string `json:"reason"`
	Input      any    `json:"input"`
	Output     any    `json:"output"`
	Error      string `json:"error"`
	Seq        int64  `json:"seq"`
	Generation int64  `json:"generation"`
}

func marshalEvent(ev Event) ([]byte, error) {
	if ev.TS.IsZero() {
		return nil, errors.New("trace: missing timestamp")
	}
	return json.Marshal(jsonEvent{
		TS:         ev.TS.UTC().Format(time.RFC3339Nano),
		Repo:       ev.Repo,
		BranchRef:  ev.BranchRef,
		HeadSHA:    ev.HeadSHA,
		EventClass: ev.EventClass,
		Decision:   ev.Decision,
		Reason:     ev.Reason,
		Input:      ev.Input,
		Output:     ev.Output,
		Error:      ev.Error,
		Seq:        ev.Seq,
		Generation: ev.Generation,
	})
}

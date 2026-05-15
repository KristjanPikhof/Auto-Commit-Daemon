package ai

// intent_rejects.go — durable JSONL log of rejected intent plans.
//
// Whenever a planner provider's call to ValidateIntentPlan fails, the
// provider records the raw model output, the offered seqs, and the typed
// validator code/message to <gitDir>/acd/planner-rejects.jsonl. The log is
// the operator's primary forensic surface for "why does the daemon keep
// falling back to deterministic commits": it captures the wire-level model
// response immediately before the daemon's deterministic fallback fires,
// which is the data we need to reproduce planner errors offline.
//
// Path layout follows the project convention (DBPathFromGitDir et al.) and
// is configured once at daemon startup via ConfigureIntentRejectsLogger.
// The CLI / tests can leave it unset; LogRejectedIntentPlan is a no-op
// when no logger is configured so unit tests never write to disk.
//
// Rotation:
//
//   - Current file is `<gitDir>/acd/planner-rejects.jsonl`.
//   - When a write would push the current file past the 5 MiB rotation
//     threshold, the writer renames the current file to
//     `planner-rejects.jsonl.1` (overwriting any prior `.1`) and starts a
//     fresh current file. Only two files are kept on disk; older `.1`
//     archives are unlinked by the next rotation.
//   - Rotation is best-effort: failure to rename or reopen returns the
//     error to the caller, which logs slog.Warn and continues. The daemon
//     never blocks on rejects-log failures.
//
// Concurrency:
//
//   - The writer holds a sync.Mutex for the entire append+rotate
//     operation. Multiple provider goroutines (composed primary + fallback,
//     concurrent intent windows) can call LogRejectedIntentPlan safely.
//   - The package-level configuration RWMutex separates "swap the active
//     logger" from "log into the active logger". Tests use
//     SetIntentRejectsLoggerForTest to inject a writer rooted in a
//     t.TempDir() and can assert on file contents without racing.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// IntentRejectsFileName is the on-disk name of the rejects log under
// <gitDir>/acd/. Exported so tests and operators can reference the canonical
// name without re-typing the literal.
const IntentRejectsFileName = "planner-rejects.jsonl"

// IntentRejectsRotateBytes is the size threshold at which the current
// rejects log is rotated to .1 and a fresh file is started. 5 MiB is large
// enough to capture roughly hundreds of typical planner-output JSON blobs
// before rotation; small enough that two files together never overwhelm a
// repo's .git/acd directory.
const IntentRejectsRotateBytes int64 = 5 * 1024 * 1024

// IntentRejectsKept is the number of rotated files retained alongside the
// current file. With Kept=1 the on-disk footprint is current + .1; the next
// rotation overwrites the existing .1.
const IntentRejectsKept = 1

// IntentRejectedPlan is the JSON shape persisted into the rejects log. The
// field names are stable for downstream tooling that scrapes the log for
// trend analysis. RawResponse may be empty when the provider could not
// stringify the plan (e.g., subprocess transport error before the response
// was buffered). Code and Message together identify the validator failure;
// callers should always pass the IntentPlanValidationCode value, not its
// String representation, so that telemetry can match on the integer code
// even after the message text changes.
type IntentRejectedPlan struct {
	TS          string                   `json:"ts"`
	Provider    string                   `json:"provider,omitempty"`
	OfferedSeqs []int64                  `json:"offered_seqs"`
	RawResponse string                   `json:"raw_response,omitempty"`
	Code        IntentPlanValidationCode `json:"code"`
	Message     string                   `json:"message"`
}

// IntentRejectsWriter encapsulates the append-and-rotate JSONL writer.
type IntentRejectsWriter struct {
	mu    sync.Mutex
	dir   string
	limit int64
	kept  int
	clock func() time.Time
}

// NewIntentRejectsWriter returns a writer rooted at acdDir; the file is not
// created eagerly. Callers must ensure acdDir exists; the writer's first
// Append will MkdirAll best-effort but a permission error on the parent
// surfaces to the caller. clock is injectable for tests.
func NewIntentRejectsWriter(acdDir string, clock func() time.Time) *IntentRejectsWriter {
	if clock == nil {
		clock = time.Now
	}
	return &IntentRejectsWriter{
		dir:   acdDir,
		limit: IntentRejectsRotateBytes,
		kept:  IntentRejectsKept,
		clock: clock,
	}
}

// Path returns the absolute path of the current rejects log. Useful for the
// diagnose remediation hint and for tests.
func (w *IntentRejectsWriter) Path() string {
	if w == nil {
		return ""
	}
	return filepath.Join(w.dir, IntentRejectsFileName)
}

// Append writes one JSON line to the current rejects log, rotating the file
// first when adding the new line would push past the rotation threshold.
// Returns the bytes written (excluding the trailing newline) on success.
//
// Concurrency: holds w.mu for the entire append+rotate path. Failure
// to write is non-fatal at the caller — LogRejectedIntentPlan emits
// slog.Warn and discards the error so planner replay continues unimpeded.
func (w *IntentRejectsWriter) Append(rec IntentRejectedPlan) (int, error) {
	if w == nil {
		return 0, errors.New("ai: nil rejects writer")
	}
	if rec.TS == "" {
		rec.TS = w.clock().UTC().Format(time.RFC3339Nano)
	}
	if rec.OfferedSeqs == nil {
		rec.OfferedSeqs = []int64{}
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("ai: marshal rejects record: %w", err)
	}
	body = append(body, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := os.MkdirAll(w.dir, 0o700); err != nil {
		return 0, fmt.Errorf("ai: mkdir rejects dir: %w", err)
	}
	current := w.Path()
	if err := w.rotateIfNeededLocked(current, int64(len(body))); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(current, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, fmt.Errorf("ai: open rejects log: %w", err)
	}
	defer f.Close()
	n, err := f.Write(body)
	if err != nil {
		return n, fmt.Errorf("ai: write rejects log: %w", err)
	}
	return n, nil
}

// rotateIfNeededLocked inspects the current file size and rotates when the
// projected size after appending `incoming` bytes would exceed w.limit. The
// rotation overwrites any pre-existing .1 file (Kept=1 invariant); older
// archives are unlinked.
//
// Caller must hold w.mu.
func (w *IntentRejectsWriter) rotateIfNeededLocked(current string, incoming int64) error {
	fi, err := os.Stat(current)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("ai: stat rejects log: %w", err)
	}
	if fi.Size()+incoming <= w.limit {
		return nil
	}
	// Drop archives beyond the kept window before renaming current. With
	// Kept=1 the loop body runs once for the .2 slot and is a no-op when
	// the older archive is absent. Future Kept values would shift the
	// chain (.2 -> .3, .1 -> .2) here.
	for i := w.kept + 1; i >= 2; i-- {
		older := fmt.Sprintf("%s.%d", current, i)
		if err := os.Remove(older); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("ai: remove archived rejects log: %w", err)
		}
	}
	rotated := current + ".1"
	if err := os.Remove(rotated); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("ai: clear prior rotated rejects log: %w", err)
	}
	if err := os.Rename(current, rotated); err != nil {
		return fmt.Errorf("ai: rotate rejects log: %w", err)
	}
	return nil
}

var (
	intentRejectsMu     sync.RWMutex
	intentRejectsWriter *IntentRejectsWriter
)

// ConfigureIntentRejectsLogger installs the package-level rejects writer
// rooted at <gitDir>/acd/planner-rejects.jsonl. Called by the daemon at
// startup. Passing an empty gitDir disables the logger (subsequent
// LogRejectedIntentPlan calls become no-ops). Safe to call multiple times.
func ConfigureIntentRejectsLogger(gitDir string) {
	intentRejectsMu.Lock()
	defer intentRejectsMu.Unlock()
	if gitDir == "" {
		intentRejectsWriter = nil
		return
	}
	intentRejectsWriter = NewIntentRejectsWriter(filepath.Join(gitDir, "acd"), time.Now)
}

// SetIntentRejectsLoggerForTest swaps the package-level writer. Tests use
// this to install a writer rooted in t.TempDir() and to restore the prior
// writer on cleanup. The previous writer is returned so the caller can
// defer its restoration.
func SetIntentRejectsLoggerForTest(w *IntentRejectsWriter) *IntentRejectsWriter {
	intentRejectsMu.Lock()
	defer intentRejectsMu.Unlock()
	prev := intentRejectsWriter
	intentRejectsWriter = w
	return prev
}

// IntentRejectsLoggerForTest exposes the active writer for tests that need
// to assert on Path() or call Append directly. Returns nil when unset.
func IntentRejectsLoggerForTest() *IntentRejectsWriter {
	intentRejectsMu.RLock()
	defer intentRejectsMu.RUnlock()
	return intentRejectsWriter
}

// LogRejectedIntentPlan persists one validator-rejected planner response to
// the rotating JSONL log. Best-effort: failures are logged at warn level and
// the function returns without surfacing the error so the planner path
// stays unblocked. The function is a no-op when no logger is configured.
//
// `provider` identifies the source (e.g. "openai-compat", "subprocess:foo").
// `req` supplies the offered seqs. `raw` is the verbatim model output (HTTP
// body for openai-compat; marshaled subprocess response for plugin
// providers). `valErr` MUST be non-nil — callers wrap the typed validator
// error so the JSONL row carries the IntentPlanValidationCode integer for
// downstream telemetry.
//
// Caller usage convention:
//
//	if err := ValidateIntentPlan(req, plan); err != nil {
//	    LogRejectedIntentPlan(ctx, p.Name(), req, raw, err)
//	    return IntentPlan{}, err
//	}
//
// The logger ignores ctx today (writes are short and synchronous) but the
// signature reserves it for future cancellation if rotation ever moves to a
// background goroutine.
func LogRejectedIntentPlan(ctx context.Context, provider string, req IntentPlanRequest, raw string, valErr error) {
	_ = ctx
	if valErr == nil {
		return
	}
	intentRejectsMu.RLock()
	w := intentRejectsWriter
	intentRejectsMu.RUnlock()
	if w == nil {
		return
	}
	rec := IntentRejectedPlan{
		Provider:    provider,
		OfferedSeqs: offeredSeqsCopy(req),
		RawResponse: raw,
		Message:     valErr.Error(),
	}
	var typed *IntentPlanValidationError
	if errors.As(valErr, &typed) {
		rec.Code = typed.Code
	} else {
		rec.Code = IntentPlanValidationUnknown
	}
	if _, err := w.Append(rec); err != nil {
		slog.Warn("intent planner: rejects log write failed",
			slog.String("provider", provider),
			slog.String("path", w.Path()),
			slog.String("err", err.Error()),
		)
	}
}

func offeredSeqsCopy(req IntentPlanRequest) []int64 {
	if len(req.OfferedCaptures) == 0 {
		return []int64{}
	}
	out := make([]int64, 0, len(req.OfferedCaptures))
	for _, c := range req.OfferedCaptures {
		out = append(out, c.Seq)
	}
	return out
}

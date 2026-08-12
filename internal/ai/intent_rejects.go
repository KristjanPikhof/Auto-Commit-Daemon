package ai

// intent_rejects.go — durable JSONL log of rejected intent plans.
//
// Whenever a planner provider's call to ValidateIntentPlan fails, the
// provider records the offered seqs, typed validator code/message, raw
// response size + sha256, and a small parsed-plan summary to
// <gitDir>/acd/planner-rejects.jsonl. Raw model output is redacted by default;
// ACD_INTENT_REJECTS_RAW=1 opts back into verbatim retention. The log is the
// operator's primary forensic surface for "why does the daemon keep falling
// back to deterministic commits" without durably retaining raw planner
// payloads unless the operator explicitly asks for that debugging mode.
//
// Path layout follows the project convention (DBPathFromGitDir et al.). Each
// daemon run binds its worktree's writer to the request context. The CLI and
// tests can leave it unset; LogRejectedIntentPlan is a no-op when the context
// carries no writer, so callers never inherit another run's destination.
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
//   - Each writer has its own lock, while context binding keeps concurrent
//     worktree runs isolated without a process-wide registry or singleton.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
// was buffered) or — by default — when the raw response was redacted before
// persistence (see RawResponseRedacted, RawResponseSizeBytes,
// RawResponseSHA256, ParsedPlanSummary). Code and Message together identify
// the validator failure; callers should always pass the
// IntentPlanValidationCode value, not its String representation, so that
// telemetry can match on the integer code even after the message text
// changes.
//
// Redaction policy: by default LogRejectedIntentPlan strips RawResponse and
// records size + sha256 + a small ParsedPlanSummary instead. Operators can
// opt back in to verbatim retention by setting ACD_INTENT_REJECTS_RAW=1 (or
// true/yes/on); a single startup warn announces the opt-in. When verbatim
// is enabled, RawResponse is populated AND size + sha256 are still recorded
// so downstream tooling can cross-check that the on-disk payload matches.
type IntentRejectedPlan struct {
	TS                   string                         `json:"ts"`
	Provider             string                         `json:"provider,omitempty"`
	OfferedSeqs          []int64                        `json:"offered_seqs"`
	RawResponse          string                         `json:"raw_response,omitempty"`
	RawResponseRedacted  bool                           `json:"raw_response_redacted,omitempty"`
	RawResponseSizeBytes int                            `json:"raw_response_size_bytes,omitempty"`
	RawResponseSHA256    string                         `json:"raw_response_sha256,omitempty"`
	ParsedPlanSummary    *IntentRejectedPlanPlanSummary `json:"parsed_plan_summary,omitempty"`
	Code                 IntentPlanValidationCode       `json:"code"`
	Message              string                         `json:"message"`
}

// IntentRejectedPlanPlanSummary records the post-parse shape of the
// rejected plan when raw retention is disabled. Operators inspecting the
// log can still see how many seqs the planner selected vs deferred, even
// without the verbatim model output.
type IntentRejectedPlanPlanSummary struct {
	SelectedCount int `json:"selected_count"`
	DeferredCount int `json:"deferred_count"`
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
	// Atomic rotation: rely on POSIX os.Rename to overwrite an existing
	// rotated archive in a single step. The previous code did
	// os.Remove(rotated) followed by os.Rename(current, rotated); a crash
	// between those two calls would lose the prior .1 entirely. On Linux
	// (any FS) and macOS APFS, rename(2) is atomic and replaces the
	// destination if it exists, so observers always see either the old
	// .1 or the new .1 — never an absent file.
	if err := os.Rename(current, rotated); err != nil {
		return fmt.Errorf("ai: rotate rejects log: %w", err)
	}
	return nil
}

type intentRejectsWriterContextKey struct{}
type intentRejectsWriterBinding struct {
	writer *IntentRejectsWriter
}

// WithIntentRejectsWriter returns a child context that routes rejected plans
// to w. A nil writer deliberately disables logging for that context.
func WithIntentRejectsWriter(ctx context.Context, w *IntentRejectsWriter) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, intentRejectsWriterContextKey{}, intentRejectsWriterBinding{writer: w})
}

func intentRejectsWriterFromContext(ctx context.Context) *IntentRejectsWriter {
	if ctx == nil {
		return nil
	}
	binding, _ := ctx.Value(intentRejectsWriterContextKey{}).(intentRejectsWriterBinding)
	return binding.writer
}

// LogRejectedIntentPlan persists one validator-rejected planner response to
// the rotating JSONL log. Best-effort: failures are logged at warn level and
// the function returns without surfacing the error so the planner path
// stays unblocked. The function is a no-op when ctx carries no writer.
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
func LogRejectedIntentPlan(ctx context.Context, provider string, req IntentPlanRequest, raw string, valErr error) {
	if valErr == nil {
		return
	}
	w := intentRejectsWriterFromContext(ctx)
	if w == nil {
		return
	}
	verbatim := intentRejectsRawVerbatim()
	notifyIntentRejectsRawOptIn(verbatim)

	rec := IntentRejectedPlan{
		Provider:    provider,
		OfferedSeqs: offeredSeqsCopy(req),
		Message:     valErr.Error(),
	}
	var typed *IntentPlanValidationError
	if errors.As(valErr, &typed) {
		rec.Code = typed.Code
	} else {
		rec.Code = IntentPlanValidationUnknown
	}

	// Always populate size + sha256 so verbatim opt-in can be cross-checked
	// against later forensic copies and so the redacted default still
	// carries enough fingerprint to spot duplicate planner outputs across
	// distinct rejection events.
	rec.RawResponseSizeBytes = len(raw)
	if raw != "" {
		sum := sha256.Sum256([]byte(raw))
		rec.RawResponseSHA256 = hex.EncodeToString(sum[:])
	}
	rec.ParsedPlanSummary = parsedPlanSummaryFromRaw(raw)

	if verbatim {
		rec.RawResponse = raw
		rec.RawResponseRedacted = false
	} else {
		rec.RawResponseRedacted = true
	}

	if _, err := w.Append(rec); err != nil {
		slog.Warn("intent planner: rejects log write failed",
			slog.String("provider", provider),
			slog.String("path", w.Path()),
			slog.String("err", err.Error()),
		)
	}
}

// LogRejectedIntentPlanV2 applies the same redaction, fingerprint, rotation,
// and best-effort policy to native candidate-plan failures. The legacy numeric
// code remains "unknown"; the bounded v2 finding code is retained in Message.
func LogRejectedIntentPlanV2(ctx context.Context, provider string, req IntentPlanRequestV2, raw string, valErr error) {
	if valErr == nil {
		return
	}
	w := intentRejectsWriterFromContext(ctx)
	if w == nil {
		return
	}
	verbatim := intentRejectsRawVerbatim()
	notifyIntentRejectsRawOptIn(verbatim)
	rec := IntentRejectedPlan{
		Provider:    provider,
		OfferedSeqs: offeredSeqsV2(req),
		Code:        IntentPlanValidationUnknown,
		Message:     SanitizePlannerError(valErr.Error()),
	}
	rec.RawResponseSizeBytes = len(raw)
	if raw != "" {
		sum := sha256.Sum256([]byte(raw))
		rec.RawResponseSHA256 = hex.EncodeToString(sum[:])
	}
	rec.ParsedPlanSummary = parsedPlanV2SummaryFromRaw(raw)
	if verbatim {
		rec.RawResponse = raw
	} else {
		rec.RawResponseRedacted = true
	}
	if _, err := w.Append(rec); err != nil {
		slog.Warn("intent planner v2: rejects log write failed",
			slog.String("provider", provider),
			slog.String("path", w.Path()),
			slog.String("err", err.Error()),
		)
	}
}

func parsedPlanV2SummaryFromRaw(raw string) *IntentRejectedPlanPlanSummary {
	if raw == "" {
		return nil
	}
	var plan IntentPlanV2
	var envelope struct {
		IntentPlanV2 json.RawMessage `json:"intent_plan_v2"`
		Choices      []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
				FunctionCall *struct {
					Arguments string `json:"arguments"`
				} `json:"function_call"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err == nil {
		switch {
		case len(envelope.IntentPlanV2) > 0:
			_ = json.Unmarshal(envelope.IntentPlanV2, &plan)
		case len(envelope.Choices) > 0 &&
			len(envelope.Choices[0].Message.ToolCalls) > 0:
			_ = json.Unmarshal(
				[]byte(envelope.Choices[0].Message.ToolCalls[0].Function.Arguments),
				&plan,
			)
		case len(envelope.Choices) > 0 &&
			envelope.Choices[0].Message.FunctionCall != nil:
			_ = json.Unmarshal(
				[]byte(envelope.Choices[0].Message.FunctionCall.Arguments),
				&plan,
			)
		default:
			_ = json.Unmarshal([]byte(raw), &plan)
		}
	}
	if len(plan.Candidates) == 0 {
		return nil
	}
	summary := &IntentRejectedPlanPlanSummary{}
	for _, candidate := range plan.Candidates {
		if candidate.Readiness == IntentCandidateReady {
			summary.SelectedCount += len(candidate.SelectedSeqs)
		} else {
			summary.DeferredCount += len(candidate.SelectedSeqs)
		}
	}
	return summary
}

// intentRejectsRawVerbatim reports whether the operator opted in to
// persisting the verbatim model RawResponse in the rejects log via
// ACD_INTENT_REJECTS_RAW. The default (unset / 0 / false / no / off) keeps
// the response redacted; truthy values ("1", "true", "yes", "on") enable
// verbatim retention. Casing is ignored. Other values fall back to the
// safe (redacted) default.
func intentRejectsRawVerbatim() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("ACD_INTENT_REJECTS_RAW"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

var intentRejectsRawWarnOnce = &sync.Once{}

// notifyIntentRejectsRawOptIn emits a one-shot startup warn so an operator
// who flipped on verbatim retention sees a visible reminder that planner
// payloads are now durably persisted unredacted. No-op when the opt-in is
// off.
func notifyIntentRejectsRawOptIn(enabled bool) {
	if !enabled {
		return
	}
	intentRejectsRawWarnOnce.Do(func() {
		slog.Warn("intent planner: ACD_INTENT_REJECTS_RAW=1; raw planner responses will be persisted verbatim in the rejects log")
	})
}

// parsedPlanSummaryFromRaw best-effort decodes the planner output enough
// to record SelectedCount / DeferredCount in the rejected-plan record.
// Returns nil when the raw payload is empty or cannot be parsed; callers
// must tolerate the absent summary because malformed planner output is
// the common rejection path.
func parsedPlanSummaryFromRaw(raw string) *IntentRejectedPlanPlanSummary {
	if raw == "" {
		return nil
	}
	// Try the openai-compat tool-call shape first (common case for
	// LogRejectedIntentPlan callers). Fall through to bare-IntentPlan JSON
	// for subprocess plugins that already serialized just the plan body.
	if plan, err := parseIntentPlanToolCall([]byte(raw)); err == nil {
		return &IntentRejectedPlanPlanSummary{
			SelectedCount: len(plan.SelectedSeqs),
			DeferredCount: len(plan.DeferredSeqs),
		}
	}
	var bare IntentPlan
	if err := json.Unmarshal([]byte(raw), &bare); err == nil {
		return &IntentRejectedPlanPlanSummary{
			SelectedCount: len(bare.SelectedSeqs),
			DeferredCount: len(bare.DeferredSeqs),
		}
	}
	return nil
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

package ai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestNewIntentRejectsWriter_AppendCreatesAndPersists asserts that the
// writer creates the parent dir lazily, writes a single JSON line per
// Append, and that the on-disk record round-trips back through json.
// Establishes the baseline shape future regressions can pivot on.
func TestNewIntentRejectsWriter_AppendCreatesAndPersists(t *testing.T) {
	dir := t.TempDir()
	acd := filepath.Join(dir, "acd")
	clk := newFixedClock(time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC))
	w := NewIntentRejectsWriter(acd, clk.Now)
	rec := IntentRejectedPlan{
		Provider:    "openai-compat",
		OfferedSeqs: []int64{10, 11},
		RawResponse: `{"selected_seqs":[10],"deferred_seqs":[]}`,
		Code:        IntentPlanValidationDeferredReasonNotDeferred,
		Message:     "intent planner: deferred reason references non-deferred seq 99",
	}
	if _, err := w.Append(rec); err != nil {
		t.Fatalf("Append: %v", err)
	}
	body, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.HasSuffix(string(body), "\n") {
		t.Fatalf("expected trailing newline, got %q", string(body))
	}
	var got IntentRejectedPlan
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(body))), &got); err != nil {
		t.Fatalf("Unmarshal: %v body=%q", err, string(body))
	}
	if got.TS == "" {
		t.Fatalf("ts must be auto-stamped, got empty")
	}
	if got.Provider != "openai-compat" {
		t.Fatalf("Provider=%q want openai-compat", got.Provider)
	}
	if got.Code != IntentPlanValidationDeferredReasonNotDeferred {
		t.Fatalf("Code=%d want %d", got.Code, IntentPlanValidationDeferredReasonNotDeferred)
	}
	if len(got.OfferedSeqs) != 2 {
		t.Fatalf("OfferedSeqs len=%d want 2", len(got.OfferedSeqs))
	}
}

// TestNewIntentRejectsWriter_RotateAtSize asserts that crossing the
// configured size threshold renames the current file to .1 and starts a
// fresh current file. The threshold is enforced via the projected size
// after the next append (current_size + incoming_bytes > limit triggers
// rotation), which matches the writer's reserve-then-write contract.
func TestNewIntentRejectsWriter_RotateAtSize(t *testing.T) {
	dir := t.TempDir()
	w := NewIntentRejectsWriter(dir, time.Now)
	// Shrink the threshold so the test stays fast and deterministic.
	// Pre-fill the current file just below the limit so the next append
	// triggers rotation, then assert the current file holds only the
	// post-rotation record while the .1 archive holds the pre-rotation
	// payload.
	w.limit = 1024
	pad := strings.Repeat("X", 700) // each record's raw_response > 700 bytes
	first := IntentRejectedPlan{
		Provider:    "openai-compat",
		OfferedSeqs: []int64{1},
		RawResponse: pad,
		Code:        IntentPlanValidationShape,
		Message:     "first",
	}
	if _, err := w.Append(first); err != nil {
		t.Fatalf("Append first: %v", err)
	}
	second := first
	second.RawResponse = pad
	second.Message = "second"
	if _, err := w.Append(second); err != nil {
		t.Fatalf("Append second: %v", err)
	}

	current, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("ReadFile current: %v", err)
	}
	rotated, err := os.ReadFile(w.Path() + ".1")
	if err != nil {
		t.Fatalf("ReadFile .1: %v", err)
	}
	// Current should hold one record (the second append). Rotated
	// should hold the first append's record.
	if got := lineCount(current); got != 1 {
		t.Fatalf("current line count=%d want 1; body=%q", got, string(current))
	}
	if got := lineCount(rotated); got != 1 {
		t.Fatalf("rotated line count=%d want 1; body=%q", got, string(rotated))
	}
	if !strings.Contains(string(current), `"second"`) {
		t.Fatalf("current must contain second record, got %q", string(current))
	}
	if !strings.Contains(string(rotated), `"first"`) {
		t.Fatalf("rotated must contain first record, got %q", string(rotated))
	}
}

// TestNewIntentRejectsWriter_TwoFileRetention asserts that a third
// rotation overwrites the existing .1 instead of growing a .2 archive.
// The Kept=1 invariant is the contract: at most current + .1 on disk.
func TestNewIntentRejectsWriter_TwoFileRetention(t *testing.T) {
	dir := t.TempDir()
	w := NewIntentRejectsWriter(dir, time.Now)
	w.limit = 1024
	pad := strings.Repeat("X", 700)
	for i := 0; i < 4; i++ {
		rec := IntentRejectedPlan{
			Provider:    "openai-compat",
			OfferedSeqs: []int64{int64(i + 1)},
			RawResponse: pad,
			Code:        IntentPlanValidationShape,
			Message:     fmt.Sprintf("record-%d", i+1),
		}
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	// Only current and .1 may exist; any .2 archive is a regression.
	if _, err := os.Stat(w.Path()); err != nil {
		t.Fatalf("current must exist: %v", err)
	}
	if _, err := os.Stat(w.Path() + ".1"); err != nil {
		t.Fatalf(".1 must exist: %v", err)
	}
	if _, err := os.Stat(w.Path() + ".2"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".2 must NOT exist; stat err=%v", err)
	}
	// Verify the .1 archive holds the third record (the most recently
	// rotated one) — not the first or second, which were already
	// overwritten by the prior rotation.
	rotated, err := os.ReadFile(w.Path() + ".1")
	if err != nil {
		t.Fatalf("ReadFile .1: %v", err)
	}
	if !strings.Contains(string(rotated), `"record-3"`) {
		t.Fatalf("rotated .1 must contain record-3, got %q", string(rotated))
	}
}

// TestIntentRejectsWriter_RotateAtomicityNoIntermediateGap asserts that the
// rotated archive (.1) is always observable by a concurrent reader during
// the writer's append+rotate cycle. The previous implementation did
// os.Remove(rotated) followed by os.Rename(current, rotated); a stat call
// in between would briefly observe a missing .1. The atomic os.Rename
// (POSIX rename(2) overwrites the destination) closes the gap.
//
// The test races a stat goroutine against rotation appends; once the .1
// archive has been seen at least once, every subsequent stat must succeed.
// A regression to the remove+rename pattern surfaces as a transient
// fs.ErrNotExist after the file appeared.
func TestIntentRejectsWriter_RotateAtomicityNoIntermediateGap(t *testing.T) {
	dir := t.TempDir()
	w := NewIntentRejectsWriter(dir, time.Now)
	w.limit = 1024 // tiny so each append rotates promptly
	rotated := w.Path() + ".1"
	pad := strings.Repeat("X", 700) // > limit/2 so two appends rotate

	// Seed two rotations so .1 already exists before the racing goroutine
	// starts watching for the no-gap invariant.
	for i := 0; i < 2; i++ {
		rec := IntentRejectedPlan{
			Provider:    "openai-compat",
			OfferedSeqs: []int64{int64(i + 1)},
			RawResponse: pad,
			Code:        IntentPlanValidationShape,
			Message:     fmt.Sprintf("seed-%d", i+1),
		}
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("seed Append %d: %v", i, err)
		}
	}
	if _, err := os.Stat(rotated); err != nil {
		t.Fatalf("rotated must exist after seed: %v", err)
	}

	const writes = 30
	stop := make(chan struct{})
	gapErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		seenOnce := false
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, err := os.Stat(rotated)
			if err == nil {
				seenOnce = true
				continue
			}
			if seenOnce && errors.Is(err, os.ErrNotExist) {
				select {
				case gapErr <- fmt.Errorf("rotated .1 disappeared mid-rotation: %w", err):
				default:
				}
				return
			}
		}
	}()

	for i := 0; i < writes; i++ {
		rec := IntentRejectedPlan{
			Provider:    "openai-compat",
			OfferedSeqs: []int64{int64(i + 100)},
			RawResponse: pad,
			Code:        IntentPlanValidationShape,
			Message:     fmt.Sprintf("rot-%d", i+1),
		}
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-gapErr:
		t.Fatalf("rotation gap detected: %v", err)
	default:
	}
}

// TestNewIntentRejectsWriter_NoRotateBelowThreshold asserts the writer
// does not rotate when projected size stays at or below the configured
// limit. Guards against an off-by-one regression that would rotate every
// append on small thresholds.
func TestNewIntentRejectsWriter_NoRotateBelowThreshold(t *testing.T) {
	dir := t.TempDir()
	w := NewIntentRejectsWriter(dir, time.Now)
	w.limit = 1 << 20 // 1 MiB; far above record size
	for i := 0; i < 5; i++ {
		rec := IntentRejectedPlan{
			Provider:    "openai-compat",
			OfferedSeqs: []int64{int64(i + 1)},
			RawResponse: "small",
			Code:        IntentPlanValidationShape,
			Message:     fmt.Sprintf("r%d", i+1),
		}
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if _, err := os.Stat(w.Path() + ".1"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".1 must NOT exist; stat err=%v", err)
	}
	body, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("ReadFile current: %v", err)
	}
	if got := lineCount(body); got != 5 {
		t.Fatalf("current line count=%d want 5", got)
	}
}

// TestNewIntentRejectsWriter_ConcurrentAppendsDoNotInterleave asserts
// that the per-writer mutex serializes concurrent Appends so each line
// remains a parsable JSON object. Detects byte interleaving regressions.
func TestNewIntentRejectsWriter_ConcurrentAppendsDoNotInterleave(t *testing.T) {
	dir := t.TempDir()
	w := NewIntentRejectsWriter(dir, time.Now)
	const goroutines = 16
	const perGoroutine = 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				rec := IntentRejectedPlan{
					Provider:    "concurrent",
					OfferedSeqs: []int64{int64(g*1000 + i)},
					RawResponse: strings.Repeat("y", 200),
					Code:        IntentPlanValidationShape,
					Message:     fmt.Sprintf("g%d-i%d", g, i),
				}
				if _, err := w.Append(rec); err != nil {
					t.Errorf("Append g=%d i=%d: %v", g, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	body, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	count := 0
	for scanner.Scan() {
		var rec IntentRejectedPlan
		if err := json.Unmarshal([]byte(scanner.Text()), &rec); err != nil {
			t.Fatalf("line %d not valid JSON: %v line=%q", count, err, scanner.Text())
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner: %v", err)
	}
	if count != goroutines*perGoroutine {
		t.Fatalf("got %d lines, want %d", count, goroutines*perGoroutine)
	}
}

// TestLogRejectedIntentPlan_NoLoggerIsNoOp asserts that the helper is
// safe to call before ConfigureIntentRejectsLogger. Tests in this package
// commonly never configure a logger; the call must be a silent no-op so
// adding LogRejectedIntentPlan to providers does not break unit tests.
func TestLogRejectedIntentPlan_NoLoggerIsNoOp(t *testing.T) {
	prev := SetIntentRejectsLoggerForTest(nil)
	t.Cleanup(func() { SetIntentRejectsLoggerForTest(prev) })
	// Must not panic, must not return error (function returns nothing).
	LogRejectedIntentPlan(context.Background(), "openai-compat",
		IntentPlanRequest{OfferedCaptures: []OfferedCapture{{Seq: 1}}},
		`{"raw":true}`,
		errors.New("validator failure"),
	)
}

// TestLogRejectedIntentPlan_PersistsTypedCode asserts the helper extracts
// the IntentPlanValidationCode from a typed *IntentPlanValidationError
// and persists it as the integer Code. Untyped errors fall back to
// IntentPlanValidationUnknown so the column is always populated.
func TestLogRejectedIntentPlan_PersistsTypedCode(t *testing.T) {
	dir := t.TempDir()
	w := NewIntentRejectsWriter(dir, time.Now)
	prev := SetIntentRejectsLoggerForTest(w)
	t.Cleanup(func() { SetIntentRejectsLoggerForTest(prev) })

	typed := &IntentPlanValidationError{
		Code:    IntentPlanValidationOfferedWindow,
		Seq:     42,
		Message: "intent planner: selected seq 42 outside offered window",
	}
	LogRejectedIntentPlan(context.Background(), "openai-compat",
		IntentPlanRequest{OfferedCaptures: []OfferedCapture{{Seq: 1}, {Seq: 2}}},
		`{"selected_seqs":[42]}`,
		typed,
	)
	LogRejectedIntentPlan(context.Background(), "subprocess:foo",
		IntentPlanRequest{OfferedCaptures: []OfferedCapture{{Seq: 5}}},
		"",
		errors.New("plain"),
	)

	body, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %v", len(lines), lines)
	}
	var first IntentRejectedPlan
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("Unmarshal[0]: %v", err)
	}
	if first.Code != IntentPlanValidationOfferedWindow {
		t.Fatalf("first.Code=%d want %d", first.Code, IntentPlanValidationOfferedWindow)
	}
	if first.Provider != "openai-compat" {
		t.Fatalf("first.Provider=%q", first.Provider)
	}
	if !strings.Contains(first.Message, "outside offered window") {
		t.Fatalf("first.Message missing expected text: %q", first.Message)
	}

	var second IntentRejectedPlan
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("Unmarshal[1]: %v", err)
	}
	if second.Code != IntentPlanValidationUnknown {
		t.Fatalf("second.Code=%d want IntentPlanValidationUnknown", second.Code)
	}
	if second.Provider != "subprocess:foo" {
		t.Fatalf("second.Provider=%q", second.Provider)
	}
}

// TestLogRejectedIntentPlan_DefaultsToRedacted asserts that without the
// ACD_INTENT_REJECTS_RAW opt-in, the rejects log row carries a redacted
// RawResponse (empty), the redaction marker is set, and size + sha256 of
// the original payload are still recorded for forensic cross-check.
func TestLogRejectedIntentPlan_DefaultsToRedacted(t *testing.T) {
	dir := t.TempDir()
	w := NewIntentRejectsWriter(dir, time.Now)
	prev := SetIntentRejectsLoggerForTest(w)
	t.Cleanup(func() { SetIntentRejectsLoggerForTest(prev) })

	// Ensure the env opt-in is unset for this test.
	t.Setenv("ACD_INTENT_REJECTS_RAW", "")
	resetIntentRejectsRawWarnOnceForTest(t)

	raw := `{"choices":[{"message":{"tool_calls":[{"function":{"name":"capture_intent_plan","arguments":"{\"selected_seqs\":[101],\"deferred_seqs\":[102,103],\"subject\":\"x\",\"body\":\"\",\"grouping_reason\":\"y\",\"deferred_reasons\":[]}"}}]}}]}`
	LogRejectedIntentPlan(context.Background(), "openai-compat",
		IntentPlanRequest{OfferedCaptures: []OfferedCapture{{Seq: 101}, {Seq: 102}, {Seq: 103}}},
		raw,
		&IntentPlanValidationError{Code: IntentPlanValidationDeferredReasonMissing, Seq: 102, Message: "missing reason"},
	)

	body, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var got IntentRejectedPlan
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(body))), &got); err != nil {
		t.Fatalf("Unmarshal: %v body=%q", err, string(body))
	}
	if got.RawResponse != "" {
		t.Fatalf("RawResponse must be empty by default, got %q", got.RawResponse)
	}
	if !got.RawResponseRedacted {
		t.Fatalf("RawResponseRedacted=false want true (default redaction)")
	}
	if got.RawResponseSizeBytes != len(raw) {
		t.Fatalf("RawResponseSizeBytes=%d want %d", got.RawResponseSizeBytes, len(raw))
	}
	if got.RawResponseSHA256 == "" {
		t.Fatalf("RawResponseSHA256 must be populated alongside redacted body")
	}
	// sha256 hex is 64 chars.
	if len(got.RawResponseSHA256) != 64 {
		t.Fatalf("RawResponseSHA256 len=%d want 64", len(got.RawResponseSHA256))
	}
}

// TestLogRejectedIntentPlan_VerbatimOnEnvOptIn asserts that
// ACD_INTENT_REJECTS_RAW=1 (or true/yes/on) populates RawResponse verbatim,
// flips RawResponseRedacted off, and still records size + sha256 so
// downstream tooling can cross-check the body.
func TestLogRejectedIntentPlan_VerbatimOnEnvOptIn(t *testing.T) {
	for _, val := range []string{"1", "true", "TRUE", "yes", "on"} {
		t.Run("env="+val, func(t *testing.T) {
			dir := t.TempDir()
			w := NewIntentRejectsWriter(dir, time.Now)
			prev := SetIntentRejectsLoggerForTest(w)
			t.Cleanup(func() { SetIntentRejectsLoggerForTest(prev) })

			t.Setenv("ACD_INTENT_REJECTS_RAW", val)
			resetIntentRejectsRawWarnOnceForTest(t)

			raw := `{"selected_seqs":[101],"deferred_seqs":[]}`
			LogRejectedIntentPlan(context.Background(), "openai-compat",
				IntentPlanRequest{OfferedCaptures: []OfferedCapture{{Seq: 101}, {Seq: 102}}},
				raw,
				&IntentPlanValidationError{Code: IntentPlanValidationShape, Message: "missing seq"},
			)
			body, err := os.ReadFile(w.Path())
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			var got IntentRejectedPlan
			if err := json.Unmarshal([]byte(strings.TrimSpace(string(body))), &got); err != nil {
				t.Fatalf("Unmarshal: %v body=%q", err, string(body))
			}
			if got.RawResponse != raw {
				t.Fatalf("RawResponse=%q want %q (verbatim)", got.RawResponse, raw)
			}
			if got.RawResponseRedacted {
				t.Fatalf("RawResponseRedacted=true want false (verbatim opt-in)")
			}
			if got.RawResponseSizeBytes != len(raw) {
				t.Fatalf("RawResponseSizeBytes=%d want %d (cross-check)", got.RawResponseSizeBytes, len(raw))
			}
			if got.RawResponseSHA256 == "" {
				t.Fatalf("RawResponseSHA256 empty; verbatim path must still record sha256 for cross-check")
			}
		})
	}
}

// TestLogRejectedIntentPlan_RedactedRoundTripsParsedPlanSummary asserts
// that even with the raw response stripped, the rejected-plan record
// carries a ParsedPlanSummary so operators can see the plan shape that
// triggered rejection.
func TestLogRejectedIntentPlan_RedactedRoundTripsParsedPlanSummary(t *testing.T) {
	dir := t.TempDir()
	w := NewIntentRejectsWriter(dir, time.Now)
	prev := SetIntentRejectsLoggerForTest(w)
	t.Cleanup(func() { SetIntentRejectsLoggerForTest(prev) })

	t.Setenv("ACD_INTENT_REJECTS_RAW", "")
	resetIntentRejectsRawWarnOnceForTest(t)

	raw := `{"choices":[{"message":{"tool_calls":[{"function":{"name":"capture_intent_plan","arguments":"{\"selected_seqs\":[101,102],\"deferred_seqs\":[103],\"subject\":\"x\",\"body\":\"\",\"grouping_reason\":\"y\",\"deferred_reasons\":[]}"}}]}}]}`
	LogRejectedIntentPlan(context.Background(), "openai-compat",
		IntentPlanRequest{OfferedCaptures: []OfferedCapture{{Seq: 101}, {Seq: 102}, {Seq: 103}}},
		raw,
		&IntentPlanValidationError{Code: IntentPlanValidationDeferredReasonMissing, Seq: 103, Message: "missing reason"},
	)
	body, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var got IntentRejectedPlan
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(body))), &got); err != nil {
		t.Fatalf("Unmarshal: %v body=%q", err, string(body))
	}
	if got.RawResponse != "" {
		t.Fatalf("RawResponse must be empty under default redaction, got %q", got.RawResponse)
	}
	if got.ParsedPlanSummary == nil {
		t.Fatalf("ParsedPlanSummary nil; redacted record must still carry plan shape")
	}
	if got.ParsedPlanSummary.SelectedCount != 2 {
		t.Fatalf("SelectedCount=%d want 2", got.ParsedPlanSummary.SelectedCount)
	}
	if got.ParsedPlanSummary.DeferredCount != 1 {
		t.Fatalf("DeferredCount=%d want 1", got.ParsedPlanSummary.DeferredCount)
	}
}

// resetIntentRejectsRawWarnOnceForTest swaps the package-level sync.Once
// so each verbatim-opt-in test sees the warn fire (at most once per test
// invocation). Callers register cleanup automatically.
func resetIntentRejectsRawWarnOnceForTest(t *testing.T) {
	t.Helper()
	prev := intentRejectsRawWarnOnce
	intentRejectsRawWarnOnce = &sync.Once{}
	t.Cleanup(func() { intentRejectsRawWarnOnce = prev })
}

// TestConfigureIntentRejectsLogger_EmptyDirDisables asserts that
// configuring with an empty gitDir clears the writer so subsequent calls
// no-op. The CLI / pre-daemon paths use this to disable the writer in
// contexts where state lives nowhere on disk.
func TestConfigureIntentRejectsLogger_EmptyDirDisables(t *testing.T) {
	dir := t.TempDir()
	prev := SetIntentRejectsLoggerForTest(nil)
	t.Cleanup(func() { SetIntentRejectsLoggerForTest(prev) })

	ConfigureIntentRejectsLogger(dir)
	if got := IntentRejectsLoggerForTest(); got == nil {
		t.Fatalf("expected configured writer, got nil")
	}
	ConfigureIntentRejectsLogger("")
	if got := IntentRejectsLoggerForTest(); got != nil {
		t.Fatalf("expected nil writer after empty configure, got %#v", got)
	}
}

func lineCount(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	return strings.Count(string(b), "\n")
}

type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFixedClock(t time.Time) *fixedClock { return &fixedClock{now: t} }

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

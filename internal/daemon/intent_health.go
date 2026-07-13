package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	// MetaKeyIntentPlannerHealth stores the crash-safe planner circuit state.
	// The value is versioned JSON so later CLI observability can read the same
	// record without adding a schema migration.
	MetaKeyIntentPlannerHealth = "intent.planner.health"

	intentPlannerHealthVersion = 1
)

var intentPlannerCircuitBackoffs = [...]time.Duration{
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}

var (
	// ErrIntentPlannerHealthInvalidRecord indicates that persisted planner
	// health cannot be safely projected into operator output.
	ErrIntentPlannerHealthInvalidRecord = errors.New("invalid intent planner health record")
	// ErrIntentPlannerHealthUnsupportedVersion indicates a well-formed record
	// whose schema is newer or otherwise unknown to this binary.
	ErrIntentPlannerHealthUnsupportedVersion = errors.New("unsupported intent planner health record version")
)

// IntentPlannerCircuitState is the persisted planner health state.
type IntentPlannerCircuitState string

const (
	IntentPlannerCircuitClosed   IntentPlannerCircuitState = "closed"
	IntentPlannerCircuitOpen     IntentPlannerCircuitState = "open"
	IntentPlannerCircuitHalfOpen IntentPlannerCircuitState = "half_open"
)

// IntentPlannerFailureKind is deliberately small: integration code must
// classify failures explicitly instead of relying on provider error strings.
type IntentPlannerFailureKind string

const (
	IntentPlannerFailureTransport  IntentPlannerFailureKind = "transport"
	IntentPlannerFailureValidation IntentPlannerFailureKind = "validation"
)

// IntentPlannerProviderIdentity contains only non-secret provider identity.
// API keys and authorization headers intentionally have no field here.
type IntentPlannerProviderIdentity struct {
	Provider      string
	Model         string
	Endpoint      string
	Deterministic bool
}

// IntentPlannerHealthOptions configures one process-local circuit. Now is a
// test seam; production leaves it nil.
type IntentPlannerHealthOptions struct {
	Provider IntentPlannerProviderIdentity
	Now      func() time.Time
}

// IntentPlannerHealthSnapshot is safe to expose through status/diagnose. It
// contains a hashed provider identity and a bounded, redacted error only.
type IntentPlannerHealthSnapshot struct {
	State               IntentPlannerCircuitState `json:"state"`
	ProviderFingerprint string                    `json:"provider_fingerprint"`
	ConsecutiveFailures int                       `json:"consecutive_failures"`
	BackoffLevel        int                       `json:"backoff_level"`
	NextProbeTS         float64                   `json:"next_probe_ts,omitempty"`
	OpenedTS            float64                   `json:"opened_ts,omitempty"`
	LastFailureTS       float64                   `json:"last_failure_ts,omitempty"`
	LastFailureClass    IntentPlannerFailureKind  `json:"last_failure_class,omitempty"`
	LastError           string                    `json:"last_error,omitempty"`
	BypassCount         uint64                    `json:"bypass_count"`
	UpdatedTS           float64                   `json:"updated_ts,omitempty"`
}

type intentPlannerHealthRecord struct {
	Version int `json:"version"`
	IntentPlannerHealthSnapshot
}

// DecodeIntentPlannerHealthSnapshot validates the persisted observability
// record without mutating it. It returns fixed sentinel errors so callers can
// warn without echoing malformed JSON or secret-shaped values.
func DecodeIntentPlannerHealthSnapshot(raw string) (IntentPlannerHealthSnapshot, error) {
	var record intentPlannerHealthRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return IntentPlannerHealthSnapshot{}, ErrIntentPlannerHealthInvalidRecord
	}
	if record.Version != intentPlannerHealthVersion {
		return IntentPlannerHealthSnapshot{}, ErrIntentPlannerHealthUnsupportedVersion
	}
	snapshot := record.IntentPlannerHealthSnapshot
	if !validIntentPlannerHealthState(snapshot.State) ||
		!validIntentPlannerHealthFingerprint(snapshot.ProviderFingerprint) ||
		snapshot.ConsecutiveFailures < 0 ||
		snapshot.BackoffLevel < 0 || snapshot.BackoffLevel >= len(intentPlannerCircuitBackoffs) ||
		!validIntentPlannerHealthFailureClass(snapshot.LastFailureClass) ||
		!validIntentPlannerHealthTimestamp(snapshot.NextProbeTS) ||
		!validIntentPlannerHealthTimestamp(snapshot.OpenedTS) ||
		!validIntentPlannerHealthTimestamp(snapshot.LastFailureTS) ||
		!validIntentPlannerHealthTimestamp(snapshot.UpdatedTS) {
		return IntentPlannerHealthSnapshot{}, ErrIntentPlannerHealthInvalidRecord
	}
	snapshot.LastError = ai.SanitizePlannerError(snapshot.LastError)
	return snapshot, nil
}

func validIntentPlannerHealthState(state IntentPlannerCircuitState) bool {
	switch state {
	case IntentPlannerCircuitClosed, IntentPlannerCircuitOpen, IntentPlannerCircuitHalfOpen:
		return true
	default:
		return false
	}
}

func validIntentPlannerHealthFingerprint(fingerprint string) bool {
	const prefix = "sha256:"
	if !strings.HasPrefix(fingerprint, prefix) || len(fingerprint) != len(prefix)+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(fingerprint, prefix))
	return err == nil
}

func validIntentPlannerHealthFailureClass(kind IntentPlannerFailureKind) bool {
	switch kind {
	case "", IntentPlannerFailureTransport, IntentPlannerFailureValidation:
		return true
	default:
		return false
	}
}

func validIntentPlannerHealthTimestamp(ts float64) bool {
	return ts >= 0 && !math.IsNaN(ts) && !math.IsInf(ts, 0)
}

// IntentPlannerCircuitOpenError tells replay to use its deterministic fallback
// without invoking the remote planner. HalfOpen is true when another caller
// already owns the single probe lease.
type IntentPlannerCircuitOpenError struct {
	RetryAt  time.Time
	HalfOpen bool
}

func (e *IntentPlannerCircuitOpenError) Error() string {
	if e == nil {
		return ""
	}
	if e.HalfOpen {
		return "intent planner circuit half-open probe already in progress"
	}
	if e.RetryAt.IsZero() {
		return "intent planner circuit open"
	}
	return fmt.Sprintf("intent planner circuit open until %s", e.RetryAt.UTC().Format(time.RFC3339Nano))
}

// IntentPlannerTransportFailure opens the circuit immediately. Callers should
// wrap timeouts, HTTP/subprocess failures, and other failures that occur before
// a validated plan is available.
type IntentPlannerTransportFailure struct{ Err error }

func (e *IntentPlannerTransportFailure) Error() string {
	if e == nil || e.Err == nil {
		return "intent planner transport failure"
	}
	return e.Err.Error()
}

func (e *IntentPlannerTransportFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IntentPlannerValidationFailure counts toward the three-failure validation
// threshold. It includes provider validation, message-quality validation, and
// daemon selection-safety validation.
type IntentPlannerValidationFailure struct{ Err error }

func (e *IntentPlannerValidationFailure) Error() string {
	if e == nil || e.Err == nil {
		return "intent planner validation failure"
	}
	return e.Err.Error()
}

func (e *IntentPlannerValidationFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IntentPlannerFailureTypeError means integration passed a non-nil failure
// without classifying it as transport or validation. The circuit is unchanged.
type IntentPlannerFailureTypeError struct{ Err error }

func (e *IntentPlannerFailureTypeError) Error() string {
	return "intent planner health: unclassified failure"
}

func (e *IntentPlannerFailureTypeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IntentPlannerHealthPermit proves a caller passed the circuit gate. Its
// fields are private so only this package can manufacture valid permits.
type IntentPlannerHealthPermit struct {
	epoch         uint64
	lease         uint64
	halfOpenProbe bool
	deterministic bool
}

// IntentPlannerHealth owns the process-local lease and mirrors its durable
// state to daemon_meta. The state mutex is never held during provider calls or
// SQLite writes.
type IntentPlannerHealth struct {
	mu        sync.Mutex
	persistMu sync.Mutex

	db            *state.DB
	now           func() time.Time
	deterministic bool
	fingerprint   string

	state               IntentPlannerCircuitState
	consecutiveFailures int
	backoffLevel        int
	retryAt             time.Time
	openedAt            time.Time
	lastFailureAt       time.Time
	lastFailureClass    IntentPlannerFailureKind
	lastError           string
	bypassCount         uint64
	updatedAt           time.Time

	epoch     uint64
	nextLease uint64
}

// NewIntentPlannerHealth loads the persisted state best-effort. A provider
// identity change resets the circuit to closed. A persisted half-open state is
// normalized to open with an immediately claimable probe because its prior
// process-local lease cannot survive a restart.
func NewIntentPlannerHealth(ctx context.Context, db *state.DB, opts IntentPlannerHealthOptions) *IntentPlannerHealth {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	h := &IntentPlannerHealth{
		db:            db,
		now:           now,
		deterministic: opts.Provider.Deterministic,
		fingerprint:   IntentPlannerProviderFingerprint(opts.Provider),
		state:         IntentPlannerCircuitClosed,
	}
	if db == nil || ctx == nil || ctx.Err() != nil {
		return h
	}

	var record intentPlannerHealthRecord
	ok, err := state.MetaGetJSON(ctx, db, MetaKeyIntentPlannerHealth, &record)
	if err != nil {
		warnIntentPlannerHealthPersistence("load", err)
		return h
	}
	if !ok {
		h.updatedAt = now().UTC()
		h.persistLatest(ctx)
		return h
	}
	normalized := h.loadRecord(record, now().UTC())
	if normalized {
		h.persistLatest(ctx)
	}
	return h
}

// IntentPlannerProviderFingerprint hashes only provider, model, and a
// sanitized endpoint identity. URL userinfo, query parameters, and fragments
// are excluded before hashing; API keys are never accepted by this API.
func IntentPlannerProviderFingerprint(identity IntentPlannerProviderIdentity) string {
	provider := strings.TrimSpace(identity.Provider)
	model := strings.TrimSpace(identity.Model)
	endpoint := sanitizeIntentPlannerEndpoint(identity.Endpoint)
	mode := "remote"
	if identity.Deterministic {
		mode = "deterministic"
	}
	sum := sha256.Sum256([]byte(provider + "\x00" + model + "\x00" + endpoint + "\x00" + mode))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Acquire returns the sole half-open probe lease when cooldown has elapsed.
// Closed and deterministic circuits pass through. Cancellation is checked
// before any state mutation.
func (h *IntentPlannerHealth) Acquire(ctx context.Context) (IntentPlannerHealthPermit, error) {
	if h == nil {
		return IntentPlannerHealthPermit{deterministic: true}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return IntentPlannerHealthPermit{}, err
	}
	if h.deterministic {
		return IntentPlannerHealthPermit{deterministic: true}, nil
	}

	now := h.now().UTC()
	h.mu.Lock()
	if err := ctx.Err(); err != nil {
		h.mu.Unlock()
		return IntentPlannerHealthPermit{}, err
	}
	switch h.state {
	case IntentPlannerCircuitOpen:
		if now.Before(h.retryAt) {
			h.bypassCount++
			h.updatedAt = now
			retryAt := h.retryAt
			h.mu.Unlock()
			h.persistLatest(ctx)
			return IntentPlannerHealthPermit{}, &IntentPlannerCircuitOpenError{RetryAt: retryAt}
		}
		h.state = IntentPlannerCircuitHalfOpen
		h.updatedAt = now
		h.nextLease++
		permit := IntentPlannerHealthPermit{
			epoch:         h.epoch,
			lease:         h.nextLease,
			halfOpenProbe: true,
		}
		h.mu.Unlock()
		h.persistLatest(ctx)
		return permit, nil
	case IntentPlannerCircuitHalfOpen:
		h.bypassCount++
		h.updatedAt = now
		h.mu.Unlock()
		h.persistLatest(ctx)
		return IntentPlannerHealthPermit{}, &IntentPlannerCircuitOpenError{HalfOpen: true}
	default:
		permit := IntentPlannerHealthPermit{epoch: h.epoch}
		h.mu.Unlock()
		return permit, nil
	}
}

// Complete records a validated success when failure is nil, or a typed
// transport/validation failure otherwise. Caller cancellation wins over any
// concurrently returned provider failure and does not mutate state; a
// provider-internal cancellation with a live caller is a transport failure.
func (h *IntentPlannerHealth) Complete(ctx context.Context, permit IntentPlannerHealthPermit, failure error) error {
	if h == nil || permit.deterministic || h.deterministic {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if callerErr := ctx.Err(); callerErr != nil {
		h.releaseCanceledHalfOpenProbe(ctx, permit)
		return callerErr
	}

	kind, classified := classifyIntentPlannerFailure(failure)
	if failure != nil && !classified {
		return &IntentPlannerFailureTypeError{Err: failure}
	}

	now := h.now().UTC()
	changed := false
	h.mu.Lock()
	if callerErr := ctx.Err(); callerErr != nil {
		changed = h.releaseCanceledHalfOpenProbeLocked(now, permit)
		h.mu.Unlock()
		if changed {
			h.persistLatest(context.WithoutCancel(ctx))
		}
		return callerErr
	}
	if !h.permitCurrentLocked(permit) {
		h.mu.Unlock()
		return nil
	}
	if failure == nil {
		h.closeLocked(now)
		changed = true
	} else {
		changed = h.failLocked(now, kind, failure, permit.halfOpenProbe)
	}
	h.mu.Unlock()
	if changed {
		h.persistLatest(ctx)
	}
	return nil
}

func (h *IntentPlannerHealth) releaseCanceledHalfOpenProbe(ctx context.Context, permit IntentPlannerHealthPermit) {
	now := h.now().UTC()
	h.mu.Lock()
	changed := h.releaseCanceledHalfOpenProbeLocked(now, permit)
	h.mu.Unlock()
	if changed {
		h.persistLatest(context.WithoutCancel(ctx))
	}
}

func (h *IntentPlannerHealth) releaseCanceledHalfOpenProbeLocked(now time.Time, permit IntentPlannerHealthPermit) bool {
	if !permit.halfOpenProbe || !h.permitCurrentLocked(permit) {
		return false
	}
	h.state = IntentPlannerCircuitOpen
	h.retryAt = now
	h.updatedAt = now
	h.epoch++
	return true
}

// Snapshot returns an immutable copy for tests and future CLI observability.
func (h *IntentPlannerHealth) Snapshot() IntentPlannerHealthSnapshot {
	if h == nil {
		return IntentPlannerHealthSnapshot{State: IntentPlannerCircuitClosed}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshotLocked()
}

func (h *IntentPlannerHealth) permitCurrentLocked(permit IntentPlannerHealthPermit) bool {
	if permit.epoch != h.epoch {
		return false
	}
	if permit.halfOpenProbe {
		return h.state == IntentPlannerCircuitHalfOpen && permit.lease == h.nextLease
	}
	return h.state == IntentPlannerCircuitClosed
}

func (h *IntentPlannerHealth) failLocked(now time.Time, kind IntentPlannerFailureKind, failure error, halfOpen bool) bool {
	h.consecutiveFailures++
	h.lastFailureAt = now
	h.lastFailureClass = kind
	h.lastError = ai.SanitizePlannerError(failure.Error())
	h.updatedAt = now

	if halfOpen {
		if h.backoffLevel < len(intentPlannerCircuitBackoffs)-1 {
			h.backoffLevel++
		}
		h.openLocked(now)
		return true
	}
	if kind == IntentPlannerFailureValidation {
		if h.consecutiveFailures < 3 {
			return true
		}
	}
	h.backoffLevel = 0
	h.openLocked(now)
	return true
}

func (h *IntentPlannerHealth) openLocked(now time.Time) {
	h.state = IntentPlannerCircuitOpen
	h.openedAt = now
	h.retryAt = now.Add(intentPlannerCircuitBackoffs[h.backoffLevel])
	h.epoch++
}

func (h *IntentPlannerHealth) closeLocked(now time.Time) {
	h.state = IntentPlannerCircuitClosed
	h.consecutiveFailures = 0
	h.backoffLevel = 0
	h.retryAt = time.Time{}
	h.openedAt = time.Time{}
	h.lastFailureAt = time.Time{}
	h.lastFailureClass = ""
	h.lastError = ""
	h.updatedAt = now
	h.epoch++
}

func (h *IntentPlannerHealth) loadRecord(record intentPlannerHealthRecord, now time.Time) bool {
	if record.Version != intentPlannerHealthVersion || record.ProviderFingerprint != h.fingerprint {
		h.updatedAt = now
		return true
	}

	h.state = record.State
	h.consecutiveFailures = record.ConsecutiveFailures
	h.backoffLevel = record.BackoffLevel
	h.retryAt = intentPlannerHealthTime(record.NextProbeTS)
	h.openedAt = intentPlannerHealthTime(record.OpenedTS)
	h.lastFailureAt = intentPlannerHealthTime(record.LastFailureTS)
	h.lastFailureClass = record.LastFailureClass
	sanitizedLastError := ai.SanitizePlannerError(record.LastError)
	h.lastError = sanitizedLastError
	h.bypassCount = record.BypassCount
	h.updatedAt = intentPlannerHealthTime(record.UpdatedTS)

	normalized := sanitizedLastError != record.LastError
	if h.backoffLevel < 0 {
		h.backoffLevel = 0
		normalized = true
	} else if h.backoffLevel >= len(intentPlannerCircuitBackoffs) {
		h.backoffLevel = len(intentPlannerCircuitBackoffs) - 1
		normalized = true
	}
	if h.consecutiveFailures < 0 {
		h.consecutiveFailures = 0
		normalized = true
	}
	switch h.state {
	case IntentPlannerCircuitClosed:
		if h.consecutiveFailures > 2 {
			h.consecutiveFailures = 2
			normalized = true
		}
		if !h.retryAt.IsZero() || !h.openedAt.IsZero() || h.backoffLevel != 0 {
			h.retryAt = time.Time{}
			h.openedAt = time.Time{}
			h.backoffLevel = 0
			normalized = true
		}
	case IntentPlannerCircuitHalfOpen:
		h.state = IntentPlannerCircuitOpen
		h.retryAt = now
		h.updatedAt = now
		normalized = true
	case IntentPlannerCircuitOpen:
		if h.retryAt.IsZero() {
			h.retryAt = now
			h.updatedAt = now
			normalized = true
		}
	default:
		h.state = IntentPlannerCircuitClosed
		h.consecutiveFailures = 0
		h.backoffLevel = 0
		h.retryAt = time.Time{}
		h.openedAt = time.Time{}
		h.lastFailureAt = time.Time{}
		h.lastFailureClass = ""
		h.lastError = ""
		h.updatedAt = now
		normalized = true
	}
	if normalized {
		h.epoch++
	}
	return normalized
}

func (h *IntentPlannerHealth) snapshotLocked() IntentPlannerHealthSnapshot {
	return IntentPlannerHealthSnapshot{
		State:               h.state,
		ProviderFingerprint: h.fingerprint,
		ConsecutiveFailures: h.consecutiveFailures,
		BackoffLevel:        h.backoffLevel,
		NextProbeTS:         intentPlannerHealthTimestamp(h.retryAt),
		OpenedTS:            intentPlannerHealthTimestamp(h.openedAt),
		LastFailureTS:       intentPlannerHealthTimestamp(h.lastFailureAt),
		LastFailureClass:    h.lastFailureClass,
		LastError:           h.lastError,
		BypassCount:         h.bypassCount,
		UpdatedTS:           intentPlannerHealthTimestamp(h.updatedAt),
	}
}

func (h *IntentPlannerHealth) persistLatest(ctx context.Context) {
	if h == nil || h.db == nil || ctx == nil || ctx.Err() != nil {
		return
	}
	// Serialize writes, then re-read the latest in-memory state. If two callers
	// update concurrently, a delayed older persist therefore writes the newest
	// snapshot rather than overwriting it with stale state.
	h.persistMu.Lock()
	defer h.persistMu.Unlock()
	h.mu.Lock()
	record := intentPlannerHealthRecord{
		Version:                     intentPlannerHealthVersion,
		IntentPlannerHealthSnapshot: h.snapshotLocked(),
	}
	h.mu.Unlock()
	if err := state.MetaSetJSON(ctx, h.db, MetaKeyIntentPlannerHealth, record); err != nil {
		warnIntentPlannerHealthPersistence("save", err)
	}
}

func warnIntentPlannerHealthPersistence(operation string, err error) {
	if err == nil {
		return
	}
	slog.Warn("intent planner health persistence failed",
		"operation", operation,
		"error", ai.SanitizePlannerError(err.Error()))
}

func classifyIntentPlannerFailure(err error) (IntentPlannerFailureKind, bool) {
	if err == nil {
		return "", true
	}
	var transport *IntentPlannerTransportFailure
	if errors.As(err, &transport) {
		return IntentPlannerFailureTransport, true
	}
	var validation *IntentPlannerValidationFailure
	if errors.As(err, &validation) {
		return IntentPlannerFailureValidation, true
	}
	// Providers commonly create their own child timeout context. Its
	// cancellation does not cancel the caller context passed to Complete, so
	// these unwrapped sentinel errors are transport failures, not user aborts.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return IntentPlannerFailureTransport, true
	}
	return "", false
}

func sanitizeIntentPlannerEndpoint(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		// The value is hashed before persistence. Removing controls and obvious
		// query/fragment suffixes still makes malformed identities stable without
		// ever exposing the source value.
		if i := strings.IndexAny(raw, "?#"); i >= 0 {
			raw = raw[:i]
		}
		return strings.Map(func(r rune) rune {
			if unicode.IsControl(r) {
				return -1
			}
			return r
		}, raw)
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	return u.String()
}

func intentPlannerHealthTimestamp(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.UTC().UnixNano()) / 1e9
}

func intentPlannerHealthTime(ts float64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	seconds, fraction := math.Modf(ts)
	return time.Unix(int64(seconds), int64(fraction*1e9)).UTC()
}

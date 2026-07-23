package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	acdconfig "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// RuntimeBundle is an immutable lease unit for one capture/replay pass. Every
// provider, planner, privacy policy, format, and hot intent value carries the
// same revision identity, preventing a pass from observing mixed settings.
type RuntimeBundle struct {
	RevisionID int64
	Profile    string

	Provider             ai.Provider
	ProviderCloser       io.Closer
	MessageFn            MessageFn
	IntentPlanner        ai.IntentPlanner
	IntentHealth         *IntentPlannerHealth
	HealthIdentity       IntentPlannerProviderIdentity
	HealthFingerprint    string
	Model                string
	DiffEgress           bool
	CommitStrategy       ai.CommitStrategy
	CommitFormat         ai.CommitFormat
	IntentRetryLimit     int
	IntentWindow         int
	IntentMinPending     int
	IntentSettleWindow   time.Duration
	IntentMaxPendingAge  time.Duration
	IntentRecentCommits  int
	IntentDeferLimit     int
	IntentPathCoalescing bool
	ExperimentID         int64
	ExperimentBaselineID int64
	ExperimentPolicy     string
}

type runtimeBundleBuildFunc func(ai.ProviderConfig) (ai.Provider, io.Closer, error)

type RuntimeBundleBuilder struct {
	DB            *state.DB
	RepoRoot      string
	PromptTrace   prompttrace.Logger
	Logger        *slog.Logger
	Now           func() time.Time
	BuildProvider runtimeBundleBuildFunc
}

// BuildRevision validates and constructs one complete candidate without
// mutating the active bundle. Persisted snapshots never contain credentials;
// the API key remains environment-only through LoadProviderConfigFromEnv.
func (b RuntimeBundleBuilder) BuildRevision(ctx context.Context, revision state.ConfigRevision, previous *RuntimeBundle) (*RuntimeBundle, error) {
	values, confirmations, err := decodeRuntimeSnapshot(revision.SnapshotJSON)
	if err != nil {
		return nil, err
	}
	cfg := ai.LoadProviderConfigFromEnv()
	if err := applyRuntimeValues(&cfg, values); err != nil {
		return nil, err
	}
	validation, err := ai.ValidateProviderConfig(cfg)
	if err != nil {
		return nil, err
	}
	for _, required := range validation.Confirmations {
		if _, ok := confirmations[string(required)]; !ok {
			return nil, fmt.Errorf("runtime config: confirmation %q is required", required)
		}
	}
	build := b.BuildProvider
	if build == nil {
		build = buildValidatedRuntimeProvider
	}
	provider, closer, err := build(cfg)
	if err != nil {
		return nil, aiError(err)
	}
	failed := true
	defer func() {
		if failed && closer != nil {
			_ = closer.Close()
		}
	}()

	repoRoot := b.RepoRoot
	if !cfg.DiffEgress || !ai.ProviderNeedsDiff(provider) {
		repoRoot = ""
	}
	messageFn := providerMessageFnWithPromptTrace(provider, repoRoot, b.PromptTrace)
	planner, ok := provider.(ai.IntentPlanner)
	if cfg.CommitStrategy == ai.CommitStrategyIntent && !ok {
		return nil, errors.New("runtime config: provider does not support intent planning")
	}
	if cfg.CommitStrategy != ai.CommitStrategyIntent {
		planner = nil
	}
	providerName := ai.PrimaryProviderName(provider)
	model := ""
	if providerName == "openai-compat" {
		model = cfg.Model
	}
	identity := IntentPlannerProviderIdentity{
		Provider: providerName, Model: model, Endpoint: cfg.BaseURL,
		TrustFingerprint: runtimeTrustFingerprint(cfg.CAFile),
		Deterministic:    providerName == (ai.DeterministicProvider{}).Name(),
	}
	fingerprint := IntentPlannerProviderFingerprint(identity)
	var health *IntentPlannerHealth
	if planner != nil {
		if previous != nil && previous.IntentHealth != nil && previous.HealthFingerprint == fingerprint {
			health = previous.IntentHealth
		} else {
			health = NewIntentPlannerHealth(ctx, b.DB, IntentPlannerHealthOptions{Provider: identity, Now: b.Now})
		}
	}
	bundle := &RuntimeBundle{
		RevisionID: revision.ID, Profile: revision.Profile,
		Provider: provider, ProviderCloser: closer, MessageFn: messageFn,
		IntentPlanner: planner, IntentHealth: health, HealthIdentity: identity,
		HealthFingerprint: fingerprint, Model: model, DiffEgress: cfg.DiffEgress,
		CommitStrategy: cfg.CommitStrategy, CommitFormat: cfg.CommitFormat,
		IntentRetryLimit: runtimeInt(values, acdconfig.FieldIntentRetryOnInvalid, resolvedIntentRetryLimit()),
		IntentWindow:     cfg.IntentWindow, IntentMinPending: cfg.IntentMinPending,
		IntentSettleWindow:   cfg.IntentSettleWindow,
		IntentMaxPendingAge:  cfg.IntentMaxPendingAge,
		IntentRecentCommits:  cfg.IntentRecentCommits,
		IntentDeferLimit:     cfg.IntentDeferLimit,
		IntentPathCoalescing: runtimeBool(values, acdconfig.FieldIntentPathCoalescing, false),
	}
	if experiment, ok, err := runtimeExperimentForRevision(ctx, b.DB, revision.ID); err != nil {
		return nil, err
	} else if ok {
		bundle.ExperimentID = experiment.ID
		bundle.ExperimentBaselineID = experiment.BaselineRevisionID
		bundle.ExperimentPolicy = experiment.FailurePolicy
	}
	failed = false
	return bundle, nil
}

func runtimeExperimentForRevision(ctx context.Context, db *state.DB, revisionID int64) (state.ConfigExperiment, bool, error) {
	if db == nil || revisionID <= 0 {
		return state.ConfigExperiment{}, false, nil
	}
	var out state.ConfigExperiment
	err := db.SQL().QueryRowContext(ctx, `
SELECT id, baseline_revision_id, candidate_revision_id, window_budget,
       completed_windows, expires_ts, failure_policy, status, created_ts,
       updated_ts, completed_ts, terminal_reason
FROM config_experiments
WHERE candidate_revision_id=?
ORDER BY id DESC LIMIT 1`, revisionID).Scan(
		&out.ID, &out.BaselineRevisionID, &out.CandidateRevisionID,
		&out.WindowBudget, &out.CompletedWindows, &out.ExpiresTS,
		&out.FailurePolicy, &out.Status, &out.CreatedTS, &out.UpdatedTS,
		&out.CompletedTS, &out.TerminalReason)
	if errors.Is(err, sql.ErrNoRows) {
		return state.ConfigExperiment{}, false, nil
	}
	if err != nil {
		return state.ConfigExperiment{}, false, fmt.Errorf("runtime config: load experiment: %w", err)
	}
	return out, true, nil
}

type runtimeTelemetry struct {
	revisionID   int64
	profile      string
	experimentID int64
}
type runtimeTelemetryContextKey struct{}

func withRuntimeTelemetry(ctx context.Context, bundle *RuntimeBundle) context.Context {
	if bundle == nil {
		return ctx
	}
	return context.WithValue(ctx, runtimeTelemetryContextKey{}, runtimeTelemetry{
		revisionID: bundle.RevisionID, profile: bundle.Profile,
		experimentID: bundle.ExperimentID,
	})
}

func runtimeTelemetryFromContext(ctx context.Context) runtimeTelemetry {
	if ctx == nil {
		return runtimeTelemetry{}
	}
	telemetry, _ := ctx.Value(runtimeTelemetryContextKey{}).(runtimeTelemetry)
	return telemetry
}

func stampDecisionRuntime(ctx context.Context, rec *state.DecisionRecord) {
	if rec == nil {
		return
	}
	telemetry := runtimeTelemetryFromContext(ctx)
	if telemetry.revisionID > 0 {
		rec.ConfigRevisionID = sql.NullInt64{Int64: telemetry.revisionID, Valid: true}
	}
	if telemetry.profile != "" {
		rec.ConfigProfile = sql.NullString{String: telemetry.profile, Valid: true}
	}
}

// buildValidatedRuntimeProvider performs strict construction first so missing
// credentials and unavailable subprocesses reject activation, then constructs
// the normal composed runtime provider to preserve deterministic fallback and
// planner retry compatibility during ordinary daemon operation.
func buildValidatedRuntimeProvider(cfg ai.ProviderConfig) (ai.Provider, io.Closer, error) {
	_, validationCloser, err := ai.BuildStrictProvider(cfg)
	if err != nil {
		return nil, nil, err
	}
	if validationCloser != nil {
		if err := validationCloser.Close(); err != nil {
			return nil, nil, err
		}
	}
	return ai.BuildProvider(cfg)
}

func aiError(err error) error {
	return errors.New(ai.SanitizePlannerError(err.Error()))
}

func decodeRuntimeSnapshot(raw string) (map[string]string, map[string]struct{}, error) {
	var object map[string]json.RawMessage
	dec := json.NewDecoder(strings.NewReader(raw))
	if err := dec.Decode(&object); err != nil {
		return nil, nil, fmt.Errorf("runtime config: decode snapshot: %w", err)
	}
	values := make(map[string]string)
	confirmations := make(map[string]struct{})
	for key, encoded := range object {
		if key == "confirmations" {
			var items []string
			if err := json.Unmarshal(encoded, &items); err != nil {
				return nil, nil, errors.New("runtime config: confirmations must be a string array")
			}
			for _, item := range items {
				confirmations[strings.TrimSpace(item)] = struct{}{}
			}
			continue
		}
		field, ok := acdconfig.LookupField(key)
		if !ok {
			// Compatibility with the state v14 fixture used before the flat
			// settings-service snapshot contract was finalized.
			if key == "model" {
				key = acdconfig.FieldModel
				field, ok = acdconfig.LookupField(key)
			} else if key == "intent" {
				var nested map[string]json.RawMessage
				if json.Unmarshal(encoded, &nested) == nil {
					if window, exists := nested["window"]; exists {
						text, err := runtimeScalar(window)
						if err != nil {
							return nil, nil, err
						}
						values[acdconfig.FieldIntentWindow] = text
						continue
					}
				}
			}
		}
		if !ok {
			return nil, nil, fmt.Errorf("runtime config: unknown snapshot field %q", key)
		}
		if field.Boundary != acdconfig.ApplyHot {
			return nil, nil, fmt.Errorf("runtime config: field %q requires restart", key)
		}
		text, err := runtimeScalar(encoded)
		if err != nil {
			return nil, nil, fmt.Errorf("runtime config: field %q: %w", key, err)
		}
		values[key] = text
	}
	return values, confirmations, nil
}

func runtimeScalar(raw json.RawMessage) (string, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	switch current := value.(type) {
	case string:
		return current, nil
	case bool:
		return strconv.FormatBool(current), nil
	case float64:
		if current != float64(int64(current)) {
			return "", errors.New("value must be an integer")
		}
		return strconv.FormatInt(int64(current), 10), nil
	default:
		return "", errors.New("value must be a scalar")
	}
}

func applyRuntimeValues(cfg *ai.ProviderConfig, values map[string]string) error {
	if value, ok := values[acdconfig.FieldProvider]; ok {
		cfg.Mode = strings.TrimSpace(value)
	}
	if value, ok := values[acdconfig.FieldBaseURL]; ok {
		cfg.BaseURL = strings.TrimSpace(value)
	}
	if value, ok := values[acdconfig.FieldModel]; ok {
		cfg.Model = strings.TrimSpace(value)
	}
	if value, ok := values[acdconfig.FieldCAFile]; ok {
		cfg.CAFile = strings.TrimSpace(value)
	}
	if value, ok := values[acdconfig.FieldDiffEgress]; ok {
		cfg.DiffEgress = parseRuntimeBool(value)
	}
	if value, ok := values[acdconfig.FieldTimeout]; ok {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return errors.New("runtime config: invalid provider timeout")
		}
		cfg.Timeout = d
	}
	if value, ok := values[acdconfig.FieldCommitStrategy]; ok {
		cfg.CommitStrategy = ai.CommitStrategy(value)
	}
	if value, ok := values[acdconfig.FieldCommitFormat]; ok {
		cfg.CommitFormat = ai.CommitFormat(value)
	}
	integerFields := []struct {
		key       string
		target    *int
		allowZero bool
	}{
		{acdconfig.FieldIntentWindow, &cfg.IntentWindow, false},
		{acdconfig.FieldIntentMinPending, &cfg.IntentMinPending, false},
		{acdconfig.FieldIntentRecentCommits, &cfg.IntentRecentCommits, false},
		{acdconfig.FieldIntentDeferLimit, &cfg.IntentDeferLimit, true},
	}
	for _, field := range integerFields {
		if value, ok := values[field.key]; ok {
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 || (!field.allowZero && n == 0) {
				return fmt.Errorf("runtime config: invalid field %q", field.key)
			}
			*field.target = n
		}
	}
	if value, ok := values[acdconfig.FieldIntentSettleWindow]; ok {
		d, err := time.ParseDuration(value)
		if err != nil || d < 0 {
			return errors.New("runtime config: invalid settle window")
		}
		cfg.IntentSettleWindow = d
	}
	if value, ok := values[acdconfig.FieldIntentMaxPendingAge]; ok {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return errors.New("runtime config: invalid max pending age")
		}
		cfg.IntentMaxPendingAge = d
	}
	return nil
}

func runtimeInt(values map[string]string, key string, fallback int) int {
	if raw, ok := values[key]; ok {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			return n
		}
	}
	return fallback
}
func runtimeBool(values map[string]string, key string, fallback bool) bool {
	if raw, ok := values[key]; ok {
		return parseRuntimeBool(raw)
	}
	return fallback
}
func parseRuntimeBool(raw string) bool {
	value, _ := strconv.ParseBool(strings.TrimSpace(raw))
	return value
}

func resolvedIntentRetryLimit() int {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(ai.EnvIntentRetryOnInvalid)))
	switch raw {
	case "":
		return ai.DefaultIntentRetryOnInvalid
	case "false", "no", "off":
		return 0
	case "true", "yes", "on":
		return ai.DefaultIntentRetryOnInvalid
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return ai.DefaultIntentRetryOnInvalid
	}
	return value
}

func runtimeTrustFingerprint(caFile string) string {
	caFile = strings.TrimSpace(caFile)
	if caFile == "" {
		return "system"
	}
	body, err := os.ReadFile(caFile)
	if err != nil {
		return "custom-unreadable"
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

type runtimeBundleSlot struct {
	bundle  *RuntimeBundle
	leases  int
	retired bool
	closed  bool
}

type RuntimeBundleManager struct {
	mu           sync.Mutex
	active       *runtimeBundleSlot
	builder      RuntimeBundleBuilder
	db           *state.DB
	logger       *slog.Logger
	closeTimeout time.Duration
}

func NewRuntimeBundleManager(initial *RuntimeBundle, builder RuntimeBundleBuilder, closeTimeout time.Duration) *RuntimeBundleManager {
	if closeTimeout <= 0 {
		closeTimeout = providerCloseTimeout
	}
	logger := builder.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &RuntimeBundleManager{active: &runtimeBundleSlot{bundle: initial}, builder: builder, db: builder.DB, logger: logger, closeTimeout: closeTimeout}
}

type RuntimeBundleLease struct {
	manager *RuntimeBundleManager
	slot    *runtimeBundleSlot
	once    sync.Once
}

func (l *RuntimeBundleLease) Bundle() *RuntimeBundle {
	if l == nil || l.slot == nil {
		return nil
	}
	return l.slot.bundle
}
func (l *RuntimeBundleLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() { l.manager.release(l.slot) })
}

func (m *RuntimeBundleManager) Lease() *RuntimeBundleLease {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active.leases++
	return &RuntimeBundleLease{manager: m, slot: m.active}
}
func (m *RuntimeBundleManager) Current() *RuntimeBundle {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active.bundle
}

func (m *RuntimeBundleManager) release(slot *runtimeBundleSlot) {
	m.mu.Lock()
	slot.leases--
	closeNow := slot.retired && slot.leases == 0 && !slot.closed
	if closeNow {
		slot.closed = true
	}
	m.mu.Unlock()
	if closeNow {
		m.closeBundle(slot.bundle)
	}
}
func (m *RuntimeBundleManager) swap(candidate *RuntimeBundle) {
	m.mu.Lock()
	old := m.active
	m.active = &runtimeBundleSlot{bundle: candidate}
	old.retired = true
	closeNow := old.leases == 0 && !old.closed
	if closeNow {
		old.closed = true
	}
	m.mu.Unlock()
	if closeNow {
		m.closeBundle(old.bundle)
	}
}

func (m *RuntimeBundleManager) closeBundle(bundle *RuntimeBundle) {
	if bundle == nil || bundle.ProviderCloser == nil {
		return
	}
	done := make(chan error, 1)
	go func() { done <- bundle.ProviderCloser.Close() }()
	select {
	case err := <-done:
		if err != nil {
			m.logger.Warn("close retired ai provider", "err", ai.SanitizePlannerError(err.Error()))
		}
	case <-time.After(m.closeTimeout):
		m.logger.Warn("close retired ai provider timed out", "timeout", m.closeTimeout.String())
		if exposed, ok := bundle.ProviderCloser.(processExposer); ok {
			if process := exposed.Process(); process != nil {
				_ = process.Kill()
			}
		}
	}
}

// ActivateDesired converges through rapid desired changes before returning, so
// no capture/replay pass can lease an intermediate stale candidate.
func (m *RuntimeBundleManager) ActivateDesired(ctx context.Context) error {
	for attempt := 0; attempt < 32; attempt++ {
		projection, err := state.RuntimeConfigActivationState(ctx, m.db)
		if err != nil {
			return err
		}
		revisionID := int64(0)
		if projection.DesiredRevisionID.Valid {
			revisionID = projection.DesiredRevisionID.Int64
		} else if projection.LastKnownGoodRevisionID.Valid {
			revisionID = projection.LastKnownGoodRevisionID.Int64
		}
		if revisionID == 0 {
			return nil
		}
		var request state.ConfigActivationRequest
		hasRequest := projection.DesiredRequestID.Valid && projection.DesiredRevisionID.Valid && projection.DesiredRevisionID.Int64 == revisionID
		if hasRequest {
			request, err = state.ActivationRequestByID(ctx, m.db, projection.DesiredRequestID.Int64)
			if err != nil {
				return err
			}
			if request.Status == state.ActivationRejected || request.Status == state.ActivationCancelled {
				hasRequest = false
				if projection.LastKnownGoodRevisionID.Valid {
					revisionID = projection.LastKnownGoodRevisionID.Int64
				} else {
					return nil
				}
			}
		}
		current := m.Current()
		currentMatches := current != nil && current.RevisionID == revisionID
		if currentMatches {
			if !hasRequest || request.Status == state.ActivationApplied {
				return nil
			}
			if request.Status == state.ActivationPending {
				ok, err := state.AcknowledgeConfigActivation(ctx, m.db, request.ID, revisionID)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
			}
			ok, err := state.ApplyConfigActivation(ctx, m.db, request.ID, revisionID)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			continue
		}
		revision, err := state.ConfigRevisionByID(ctx, m.db, revisionID)
		if err != nil {
			return err
		}
		candidate, buildErr := m.builder.BuildRevision(ctx, revision, current)
		if buildErr != nil {
			if hasRequest {
				experimentID, _, rejectErr := state.RejectConfigActivationAndExperiment(
					ctx, m.db, request.ID, revisionID,
					ai.SanitizePlannerError(buildErr.Error()),
				)
				if rejectErr != nil {
					return errors.Join(buildErr, rejectErr)
				}
				if experimentID > 0 {
					if _, _, _, revertErr := state.QueueExperimentBaselineRevert(
						ctx, m.db, experimentID,
						float64(time.Now().UnixNano())/1e9,
					); revertErr != nil {
						return errors.Join(buildErr, revertErr)
					}
				}
			}
			return buildErr
		}
		if hasRequest && request.Status == state.ActivationPending {
			ok, err := state.AcknowledgeConfigActivation(ctx, m.db, request.ID, revisionID)
			if err != nil {
				m.closeBundle(candidate)
				return err
			}
			if !ok {
				m.closeBundle(candidate)
				continue
			}
		}
		m.swap(candidate)
		if hasRequest && request.Status != state.ActivationApplied {
			ok, err := state.ApplyConfigActivation(ctx, m.db, request.ID, revisionID)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		// Re-read so an A-to-B-to-C request that raced this swap converges
		// before the caller leases a bundle for the next pass.
		continue
	}
	return errors.New("runtime config: desired revision changed too frequently")
}

// QueueExperimentRevert is called only between passes. State performs expiry
// and baseline-derived revision/request creation atomically, making repeated
// restart recovery calls idempotent.
func (m *RuntimeBundleManager) QueueExperimentRevert(ctx context.Context, now time.Time) (bool, error) {
	experiment, pending, err := state.ConfigExperimentPendingRevert(ctx, m.db)
	if err != nil {
		return false, err
	}
	if !pending {
		return false, nil
	}
	_, _, queued, err := state.QueueExperimentBaselineRevert(
		ctx, m.db, experiment.ID, float64(now.UnixNano())/1e9,
	)
	return queued, err
}

func (m *RuntimeBundleManager) Close() {
	m.mu.Lock()
	slot := m.active
	slot.retired = true
	closeNow := slot.leases == 0 && !slot.closed
	if closeNow {
		slot.closed = true
	}
	m.mu.Unlock()
	if closeNow {
		m.closeBundle(slot.bundle)
	}
}

package settingsui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
)

// SettingsService is the production service seam used by the terminal model.
// Keeping it narrow makes every potentially mutating call independently
// replaceable in tests.
type SettingsService interface {
	Snapshot(context.Context, settings.Scope, string) (settings.Snapshot, error)
	Save(context.Context, settings.SaveRequest) (settings.SaveResult, error)
	Validate(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.Validation, error)
	TestProvider(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.ProviderTestResult, error)
	Apply(context.Context, settings.ApplyRequest) (settings.ApplyResult, error)
	Revert(context.Context, settings.RevertRequest) (settings.ApplyResult, error)
}

type experimentService interface {
	StartExperiment(context.Context, settings.ExperimentRequest) (settings.ExperimentResult, error)
	ExperimentProgress(context.Context, int64) (settings.ExperimentSnapshot, error)
	CancelExperiment(context.Context, int64) (settings.ExperimentResult, error)
	RevertExperiment(context.Context, int64) (settings.ExperimentResult, error)
	Compare(context.Context, ...int64) (settings.Comparison, error)
}

type BackendAdapterOptions struct {
	Scope         settings.Scope
	Profile       string
	Confirmations []ai.ConfirmationRequirement
}

type ServiceBackend struct {
	service SettingsService
	opts    BackendAdapterOptions

	mu                  sync.Mutex
	interactiveConfirms []ai.ConfirmationRequirement
	confirmationDraft   string
	pendingConfirmDraft string
	last                settings.Snapshot
	testedFingerprint   string
	testedDraftIdentity string
}

func NewServiceBackend(service SettingsService, opts BackendAdapterOptions) *ServiceBackend {
	return &ServiceBackend{service: service, opts: opts}
}

func (b *ServiceBackend) Confirm(requirements []ConfirmationRequirement) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pendingConfirmDraft == "" {
		return
	}
	if b.confirmationDraft != b.pendingConfirmDraft {
		b.interactiveConfirms = nil
		b.confirmationDraft = b.pendingConfirmDraft
	}
	seen := make(map[ai.ConfirmationRequirement]struct{}, len(b.interactiveConfirms)+len(requirements))
	for _, confirmation := range b.interactiveConfirms {
		seen[confirmation] = struct{}{}
	}
	for _, requirement := range requirements {
		confirmation, ok := aiConfirmation(requirement.ID)
		if !ok {
			continue
		}
		if _, exists := seen[confirmation]; exists {
			continue
		}
		b.interactiveConfirms = append(b.interactiveConfirms, confirmation)
		seen[confirmation] = struct{}{}
	}
	b.pendingConfirmDraft = ""
}

func (b *ServiceBackend) currentConfirmations(clean map[string]string) []ai.ConfirmationRequirement {
	b.mu.Lock()
	defer b.mu.Unlock()
	confirmations := append([]ai.ConfirmationRequirement(nil), b.opts.Confirmations...)
	if b.confirmationDraft == draftIdentity(clean) {
		confirmations = append(confirmations, b.interactiveConfirms...)
	}
	return confirmations
}

func (b *ServiceBackend) confirmationError(values []ai.ConfirmationRequirement, clean map[string]string) error {
	b.mu.Lock()
	b.pendingConfirmDraft = draftIdentity(clean)
	b.mu.Unlock()
	return newConfirmationRequiredError(values)
}

func (b *ServiceBackend) projectConfirmationError(err error, clean map[string]string) error {
	var confirmationErr *settings.ConfirmationRequiredError
	if errors.As(err, &confirmationErr) {
		return b.confirmationError(confirmationErr.Missing, clean)
	}
	return sanitizeAdapterError(err)
}

func (b *ServiceBackend) Snapshot(ctx context.Context) (Snapshot, error) {
	if b == nil || b.service == nil {
		return Snapshot{}, errors.New("settings service unavailable")
	}
	s, err := b.service.Snapshot(ctx, b.opts.Scope, b.opts.Profile)
	if err != nil {
		return Snapshot{}, sanitizeAdapterError(err)
	}
	b.mu.Lock()
	b.last = s
	b.mu.Unlock()
	out := projectSnapshot(s)
	if s.Experiment != nil {
		if experiments, ok := b.service.(experimentService); ok {
			comparison, compareErr := experiments.Compare(ctx, s.Experiment.BaselineRevisionID, s.Experiment.CandidateRevisionID)
			if compareErr != nil {
				return Snapshot{}, sanitizeAdapterError(compareErr)
			}
			out.Comparison = safeText(comparison.Interpretation)
		}
	}
	return out, nil
}

func (b *ServiceBackend) Test(ctx context.Context, draft map[string]string) (TestResult, error) {
	clean := sanitizedDraft(draft)
	result, err := b.service.TestProvider(ctx, clean, b.currentConfirmations(clean))
	if err != nil {
		return TestResult{}, b.projectConfirmationError(err, clean)
	}
	b.mu.Lock()
	b.testedFingerprint = result.Fingerprint
	b.testedDraftIdentity = draftIdentity(clean)
	b.mu.Unlock()
	return TestResult{Fingerprint: result.Fingerprint, OK: result.Success,
		Summary: fmt.Sprintf("strict synthetic %s test passed", fallback(result.Provider, "provider"))}, nil
}

func (b *ServiceBackend) Save(ctx context.Context, draft map[string]string) (ApplyResult, error) {
	clean := sanitizedDraft(draft)
	b.mu.Lock()
	last := b.last
	b.mu.Unlock()
	saved, err := b.service.Save(ctx, settings.SaveRequest{Scope: b.opts.Scope, Profile: b.opts.Profile,
		Values: changedValues(last, clean), ExpectedGeneration: last.SavedGeneration})
	if err != nil {
		return ApplyResult{}, sanitizeAdapterError(err)
	}
	b.mu.Lock()
	b.last.SavedGeneration = saved.Generation
	b.mu.Unlock()
	summary := "draft saved; test and apply explicitly"
	if b.opts.Scope == settings.ScopeGlobal {
		summary = "global defaults saved; running repositories were not changed"
	} else if b.opts.Scope == settings.ScopeProfile {
		summary = "profile saved; select it for a repository to activate"
	}
	return ApplyResult{DesiredRevision: last.DesiredRevisionID, AppliedRevision: last.AppliedRevisionID, Summary: summary}, nil
}

func (b *ServiceBackend) Apply(ctx context.Context, draft map[string]string, _ string) (ApplyResult, error) {
	clean := sanitizedDraft(draft)
	b.mu.Lock()
	last, testedFingerprint, testedDraft := b.last, b.testedFingerprint, b.testedDraftIdentity
	b.mu.Unlock()
	if testedFingerprint == "" || testedDraft != draftIdentity(clean) {
		return ApplyResult{}, errors.New("tested settings are stale; test the current draft again")
	}
	changes := changedValues(last, clean)
	if b.opts.Scope != settings.ScopeRepository {
		_, err := b.service.Save(ctx, settings.SaveRequest{Scope: b.opts.Scope, Profile: b.opts.Profile,
			Values: changes, ExpectedGeneration: last.SavedGeneration})
		if err != nil {
			return ApplyResult{}, sanitizeAdapterError(err)
		}
		summary := "profile saved; select it for a repository to activate"
		if b.opts.Scope == settings.ScopeGlobal {
			summary = "global defaults saved; running repositories were not changed"
		}
		return ApplyResult{DesiredRevision: last.DesiredRevisionID,
			AppliedRevision: last.AppliedRevisionID, Summary: summary}, nil
	}
	confirmations := b.currentConfirmations(clean)
	validation, err := b.service.Validate(ctx, clean, confirmations)
	if err != nil {
		return ApplyResult{}, sanitizeAdapterError(err)
	}
	if len(validation.Missing) > 0 {
		return ApplyResult{}, b.confirmationError(validation.Missing, clean)
	}
	saved, err := b.service.Save(ctx, settings.SaveRequest{Scope: b.opts.Scope, Profile: b.opts.Profile,
		Values: changes, ExpectedGeneration: last.SavedGeneration})
	if err != nil {
		return ApplyResult{}, sanitizeAdapterError(err)
	}
	result, err := b.service.Apply(ctx, settings.ApplyRequest{Values: clean,
		TestedFingerprint: testedFingerprint, Confirmations: confirmations,
		ExpectedGeneration: saved.Generation, ExpectedDesiredRevision: last.DesiredRevisionID})
	if err != nil {
		return ApplyResult{}, sanitizeAdapterError(err)
	}
	summary := "activation requested for the next safe work boundary"
	if !last.DaemonRunning {
		summary = "daemon is stopped; desired revision saved for next start"
	}
	return ApplyResult{DesiredRevision: result.RevisionID, AppliedRevision: last.AppliedRevisionID,
		Queued: result.RevisionID != last.AppliedRevisionID, Summary: summary}, nil
}

func (b *ServiceBackend) Revert(ctx context.Context, _ int64) (ApplyResult, error) {
	b.mu.Lock()
	last := b.last
	b.mu.Unlock()
	if last.Experiment != nil && last.Experiment.ID > 0 {
		if experiments, ok := b.service.(experimentService); ok {
			result, err := experiments.RevertExperiment(ctx, last.Experiment.ID)
			if err != nil {
				return ApplyResult{}, sanitizeAdapterError(err)
			}
			return projectExperimentApply(result, last.AppliedRevisionID, "experiment baseline revert queued"), nil
		}
	}
	result, err := b.service.Revert(ctx, settings.RevertRequest{ExpectedGeneration: last.SavedGeneration,
		ExpectedDesiredRevision: last.DesiredRevisionID})
	if err != nil {
		return ApplyResult{}, sanitizeAdapterError(err)
	}
	return ApplyResult{DesiredRevision: result.RevisionID, AppliedRevision: last.AppliedRevisionID,
		Queued: result.RevisionID != last.AppliedRevisionID, Summary: "known-good revision queued"}, nil
}

func (b *ServiceBackend) StartExperiment(ctx context.Context, draft map[string]string, options ExperimentOptions) (Experiment, error) {
	windows := options.WindowBudget
	if windows <= 0 || windows > settings.MaxExperimentWindows {
		return Experiment{}, errors.New("experiment window budget must be between 1 and 1000")
	}
	experiments, ok := b.service.(experimentService)
	if !ok {
		return Experiment{}, errors.New("experiment control is unavailable in this settings service")
	}
	clean := sanitizedDraft(draft)
	b.mu.Lock()
	last, testedFingerprint, testedDraft := b.last, b.testedFingerprint, b.testedDraftIdentity
	b.mu.Unlock()
	if b.opts.Scope != settings.ScopeRepository {
		return Experiment{}, errors.New("experiments require repository scope")
	}
	if testedFingerprint == "" || testedDraft != draftIdentity(clean) {
		return Experiment{}, errors.New("tested settings are stale; test the current draft again")
	}
	confirmations := b.currentConfirmations(clean)
	validation, err := b.service.Validate(ctx, clean, confirmations)
	if err != nil {
		return Experiment{}, sanitizeAdapterError(err)
	}
	if len(validation.Missing) > 0 {
		return Experiment{}, b.confirmationError(validation.Missing, clean)
	}
	saved, err := b.service.Save(ctx, settings.SaveRequest{Scope: settings.ScopeRepository,
		Values: changedValues(last, clean), ExpectedGeneration: last.SavedGeneration})
	if err != nil {
		return Experiment{}, sanitizeAdapterError(err)
	}
	policy := options.FailurePolicy
	if policy == "" {
		policy = settings.ExperimentPolicyContinue
	}
	var expires time.Time
	if options.ExpiresAfter > 0 {
		expires = time.Now().Add(options.ExpiresAfter)
	}
	result, err := experiments.StartExperiment(ctx, settings.ExperimentRequest{Values: clean,
		TestedFingerprint: testedFingerprint, Confirmations: confirmations,
		ExpectedGeneration: saved.Generation, ExpectedDesiredRevision: last.DesiredRevisionID,
		WindowBudget: windows, ExpiresAt: expires, FailurePolicy: policy})
	if err != nil {
		return Experiment{}, sanitizeAdapterError(err)
	}
	return projectExperiment(result.Experiment, last.Profile), nil
}

func (b *ServiceBackend) CancelExperiment(ctx context.Context, id int64) (ApplyResult, error) {
	experiments, ok := b.service.(experimentService)
	if !ok {
		return ApplyResult{}, errors.New("experiment control is unavailable in this settings service")
	}
	b.mu.Lock()
	applied := b.last.AppliedRevisionID
	b.mu.Unlock()
	result, err := experiments.CancelExperiment(ctx, id)
	if err != nil {
		return ApplyResult{}, sanitizeAdapterError(err)
	}
	return projectExperimentApply(result, applied, "experiment cancelled; baseline revert queued"), nil
}

func (b *ServiceBackend) SelectProfile(ctx context.Context, profile string) (ApplyResult, error) {
	if b.opts.Scope != settings.ScopeRepository {
		return ApplyResult{}, errors.New("profile selection requires repository scope")
	}
	b.mu.Lock()
	last := b.last
	b.mu.Unlock()
	profile = safeText(strings.TrimSpace(profile))
	_, err := b.service.Save(ctx, settings.SaveRequest{Scope: settings.ScopeRepository,
		RepositoryProfile: &profile, Values: map[string]*string{}, ExpectedGeneration: last.SavedGeneration})
	if err != nil {
		return ApplyResult{}, sanitizeAdapterError(err)
	}
	return ApplyResult{DesiredRevision: last.DesiredRevisionID, AppliedRevision: last.AppliedRevisionID,
		Summary: "repository profile selected; test and apply explicitly"}, nil
}

func projectSnapshot(s settings.Snapshot) Snapshot {
	out := Snapshot{ActiveRevision: s.AppliedRevisionID, DesiredRevision: s.DesiredRevisionID,
		AppliedRevision: s.AppliedRevisionID, LastKnownGood: s.LastKnownGoodRevisionID,
		PendingSince: s.PendingSince, PendingError: safeText(s.PendingError), DaemonRunning: s.DaemonRunning,
		PendingStatus:   safeText(s.PendingStatus),
		SavedGeneration: s.SavedGeneration, Profile: safeText(s.Profile)}
	for _, profile := range s.Profiles {
		out.Profiles = append(out.Profiles, safeText(profile))
	}
	for _, field := range s.Fields {
		value := safeText(field.DraftValue)
		set := field.Sensitive && value == "set"
		activeValue := safeText(field.ActiveValue)
		if field.Sensitive {
			value = ""
			activeValue = ""
		}
		shadow := ""
		if field.EnvironmentSet {
			shadow = "environment"
			if !field.Sensitive && field.ShadowedEnvironment != "" {
				shadow += ": " + safeText(field.ShadowedEnvironment)
			}
		}
		out.Fields = append(out.Fields, FieldValue{Key: safeText(field.Name), Value: value,
			ActiveValue: activeValue, Source: safeText(string(field.Source)), Shadowed: shadow,
			Restart: field.Boundary == config.ApplyRestart, SensitiveSet: set})
	}
	if s.Experiment != nil {
		out.Experiment = projectExperiment(*s.Experiment, out.Profile)
	}
	return out
}

func projectExperiment(value settings.ExperimentSnapshot, profile string) Experiment {
	return Experiment{ID: value.ID, Profile: safeText(profile), CompletedWindows: value.CompletedWindows,
		TotalWindows: value.WindowBudget, ExpiresAt: value.ExpiresAt, Active: value.Status == "active",
		FailurePolicy: safeText(value.FailurePolicy), Status: safeText(value.Status)}
}

func projectExperimentApply(result settings.ExperimentResult, applied int64, summary string) ApplyResult {
	desired := result.Revert.RevisionID
	return ApplyResult{DesiredRevision: desired, AppliedRevision: applied,
		Queued: result.Revert.Queued || desired != 0 && desired != applied, Summary: summary}
}

func changedValues(last settings.Snapshot, draft map[string]string) map[string]*string {
	current := map[string]string{}
	for _, field := range last.Fields {
		if !field.Sensitive && field.Persistable {
			current[field.Name] = field.DraftValue
		}
	}
	out := map[string]*string{}
	keys := make([]string, 0, len(draft))
	for key := range draft {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := draft[key]
		if value == current[key] {
			continue
		}
		if strings.TrimSpace(value) == "" {
			out[key] = nil
			continue
		}
		copyValue := value
		out[key] = &copyValue
	}
	return out
}

func draftIdentity(draft map[string]string) string {
	keys := make([]string, 0, len(draft))
	for key := range draft {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		fmt.Fprintf(h, "%s\x00%s\x00", key, draft[key])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func newConfirmationRequiredError(values []ai.ConfirmationRequirement) *ConfirmationRequiredError {
	missing := make([]ConfirmationRequirement, 0, len(values))
	for _, value := range values {
		missing = append(missing, ConfirmationRequirement{ID: string(value), Label: confirmationLabel(value)})
	}
	return &ConfirmationRequiredError{Missing: missing}
}

func confirmationLabel(value ai.ConfirmationRequirement) string {
	switch value {
	case ai.ConfirmationEndpointCredentials:
		return "send credentials to a non-default endpoint"
	case ai.ConfirmationSubprocessExecution:
		return "execute the configured provider subprocess"
	case ai.ConfirmationDiffEgress:
		return "allow redacted repository diff egress"
	default:
		return "approve an unknown provider risk"
	}
}

func aiConfirmation(id string) (ai.ConfirmationRequirement, bool) {
	value := ai.ConfirmationRequirement(safeText(id))
	switch value {
	case ai.ConfirmationEndpointCredentials, ai.ConfirmationSubprocessExecution, ai.ConfirmationDiffEgress:
		return value, true
	default:
		return "", false
	}
}

func sanitizeAdapterError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(safeText(err.Error()))
}

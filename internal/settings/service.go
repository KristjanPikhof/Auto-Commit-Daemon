// Package settings manages approved configuration and explicit connection probes.
package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type Scope string

const (
	ScopeGlobal     Scope = "global"
	ScopeProfile    Scope = "profile"
	ScopeRepository Scope = "repository"
)

type ProbeFunc func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error)

type Options struct {
	Roots            paths.Roots
	RepoPath         string
	LookupEnv        func(string) (string, bool)
	CredentialLookup func(string) (string, bool)
	Probe            ProbeFunc
	Now              func() time.Time
}

type Service struct {
	store            *config.Store
	db               *state.DB
	worktree         gitpkg.Worktree
	repoHash         string
	globalOnly       bool
	lookupEnv        func(string) (string, bool)
	credentialLookup func(string) (string, bool)
	probe            ProbeFunc
	now              func() time.Time
	credentials      credentials.Store
}

func NewService(ctx context.Context, opts Options) (*Service, error) {
	return newService(ctx, opts, true)
}

// NewValidationService resolves and validates authoring drafts without opening
// or migrating repository state. Callers must not use mutation/runtime methods
// on the returned service.
func NewValidationService(ctx context.Context, opts Options) (*Service, error) {
	return newService(ctx, opts, false)
}

// NewGlobalService creates an authoring service without resolving a Git
// repository or opening repository state. Only global authoring operations are
// available on the returned service.
func NewGlobalService(_ context.Context, opts Options) (*Service, error) {
	lookup, credentialLookup, probe, now := serviceDefaults(opts)
	return &Service{
		store:            config.NewStore(opts.Roots),
		globalOnly:       true,
		lookupEnv:        lookup,
		credentialLookup: credentialLookup,
		probe:            probe,
		now:              now,
		credentials:      credentials.NewStore(opts.Roots),
	}, nil
}

func newService(ctx context.Context, opts Options, openState bool) (*Service, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	wt, err := gitpkg.ResolveWorktree(ctx, opts.RepoPath)
	if err != nil {
		return nil, fmt.Errorf("acd settings: resolve repository: %w", err)
	}
	repoHash := central.CanonicalID(wt.Root)
	var db *state.DB
	if openState {
		db, err = state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
		if err != nil {
			return nil, fmt.Errorf("acd settings: open state: %w", err)
		}
	}
	lookup, credentialLookup, probe, now := serviceDefaults(opts)
	return &Service{store: config.NewStore(opts.Roots), db: db, worktree: wt,
		repoHash: repoHash, lookupEnv: lookup, credentialLookup: credentialLookup,
		probe: probe, now: now,
		credentials: credentials.NewStore(opts.Roots)}, nil
}

func serviceDefaults(opts Options) (
	func(string) (string, bool),
	func(string) (string, bool),
	ProbeFunc,
	func() time.Time,
) {
	rawLookup := opts.LookupEnv
	if rawLookup == nil {
		rawLookup = os.LookupEnv
	}
	credentialLookup := opts.CredentialLookup
	if credentialLookup == nil {
		credentialLookup = rawLookup
	}
	lookup := settingsLookup(rawLookup)
	probe := opts.Probe
	if probe == nil {
		probe = ai.ProbeProviderConfig
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return lookup, credentialLookup, probe, now
}

// settingsLookup keeps the provider credential out of the generic settings
// resolver. The resolver only needs to know whether the environment value is
// present; provider construction resolves the actual value separately.
func settingsLookup(lookup func(string) (string, bool)) func(string) (string, bool) {
	return func(name string) (string, bool) {
		if name != ai.EnvAPIKey {
			return lookup(name)
		}
		value, set := lookup(name)
		if !set || strings.TrimSpace(value) == "" {
			return "", false
		}
		return "configured", true
	}
}

type AuthoringPreview struct {
	Values     map[string]string
	Sources    map[string]config.Source
	Generation uint64
}

// AuthoringPreview returns presentation-safe effective authoring values. It is
// safe on a validation-only service and never reads or writes repository state.
func (s *Service) AuthoringPreview() (AuthoringPreview, error) {
	if s == nil || s.store == nil {
		return AuthoringPreview{}, errors.New("acd settings: service unavailable")
	}
	doc, err := s.store.Load()
	if err != nil {
		return AuthoringPreview{}, sanitizeError(err)
	}
	input := s.authoringResolutionInput(doc)
	fields, err := config.ResolveAll(input)
	if err != nil {
		return AuthoringPreview{}, sanitizeError(err)
	}
	values := make(map[string]string, len(fields))
	sources := make(map[string]config.Source, len(fields))
	for _, definition := range config.Catalog() {
		field := fields[definition.Name]
		values[definition.Name] = field.EffectiveValue()
		sources[definition.Name] = field.Source
	}
	return AuthoringPreview{
		Values: hotValues(values), Sources: sources,
		Generation: doc.Generation,
	}, nil
}

// AuthoringProviderConfig resolves the provider configuration currently
// authored for this repository, including the protected credential. Callers
// must use the returned value only for provider construction and must never
// log or serialize it.
func (s *Service) AuthoringProviderConfig() (ai.ProviderConfig, error) {
	preview, err := s.AuthoringPreview()
	if err != nil {
		return ai.ProviderConfig{}, err
	}
	return s.providerConfig(preview.Values)
}

func (s *Service) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

type SaveRequest struct {
	Scope              Scope
	Profile            string
	Values             map[string]*string
	RepositoryProfile  *string
	ExpectedGeneration uint64
}

type SaveResult struct {
	Generation uint64
	Scope      Scope
}

// Save updates exactly one XDG authoring scope. It does not open another
// repository, signal a daemon, or create a runtime revision.
func (s *Service) Save(_ context.Context, req SaveRequest) (SaveResult, error) {
	if s == nil || s.store == nil {
		return SaveResult{}, errors.New("acd settings: service unavailable")
	}
	if err := validateScope(req.Scope, req.Profile); err != nil {
		return SaveResult{}, err
	}
	if s.globalOnly && req.Scope != ScopeGlobal {
		return SaveResult{}, s.requireRepository("save non-global scope")
	}
	err := s.store.UpdateExpected(req.ExpectedGeneration, func(doc *config.Document) error {
		var target config.Overrides
		switch req.Scope {
		case ScopeGlobal:
			target = doc.Settings.Global
		case ScopeProfile:
			profile := doc.Settings.Profiles[req.Profile]
			if profile.Fields == nil {
				profile.Fields = config.Overrides{}
			}
			target = profile.Fields
			defer func() { doc.Settings.Profiles[req.Profile] = profile }()
		case ScopeRepository:
			repo := doc.Settings.Repositories[s.repoHash]
			if repo.Fields == nil {
				repo.Fields = config.Overrides{}
			}
			if req.RepositoryProfile != nil {
				repo.Profile = cleanText(*req.RepositoryProfile)
			}
			target = repo.Fields
			defer func() {
				if repo.Profile == "" && len(repo.Fields) == 0 &&
					len(repo.Extra) == 0 {
					delete(doc.Settings.Repositories, s.repoHash)
					return
				}
				doc.Settings.Repositories[s.repoHash] = repo
			}()
		}
		for name, value := range req.Values {
			field, err := persistedField(name)
			if err != nil {
				return err
			}
			if value == nil {
				delete(target, name)
				continue
			}
			raw, err := encodePersistedField(field, *value)
			if err != nil {
				return err
			}
			target[name] = raw
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, config.ErrStaleGeneration) {
			return SaveResult{}, fmt.Errorf("acd settings: stale saved generation: %w", err)
		}
		return SaveResult{}, sanitizeError(err)
	}
	return SaveResult{Generation: req.ExpectedGeneration + 1, Scope: req.Scope}, nil
}

// SaveGlobalSetupRequest is the reviewed global setup draft. Values must be
// persistable non-secret fields; the fingerprint and confirmations must match
// a fresh validation of this exact draft.
type SaveGlobalSetupRequest struct {
	// Changes, when set, saves only edited overrides. Nil values restore inheritance.
	Changes            map[string]*string
	Values             map[string]string
	TestedFingerprint  string
	Confirmations      []ai.ConfirmationRequirement
	ExpectedGeneration uint64
	Replace            bool
	Prepare            func([]byte) error
}

type SaveGlobalSetupResult struct {
	Generation  uint64
	Fingerprint string
}

// SaveGlobalSetup atomically stores global authoring values and their exact
// fingerprint-bound approval. The generation records its originating CAS
// write. Repository shell commands cannot be approved or activated here.
func (s *Service) SaveGlobalSetup(ctx context.Context, req SaveGlobalSetupRequest) (SaveGlobalSetupResult, error) {
	if s == nil || s.store == nil {
		return SaveGlobalSetupResult{}, errors.New("acd settings: service unavailable")
	}
	if !s.globalOnly {
		return SaveGlobalSetupResult{}, errors.New("acd settings: global setup requires a global service")
	}
	if req.Replace {
		for _, field := range config.Catalog() {
			if field.Boundary != config.ApplyHot ||
				!field.Persistable || field.Sensitive {
				continue
			}
			if _, ok := req.Values[field.Name]; !ok {
				return SaveGlobalSetupResult{}, fmt.Errorf(
					"acd settings: replacement setup is missing field %q",
					field.Name,
				)
			}
		}
	}
	validation, err := s.Validate(ctx, req.Values, req.Confirmations)
	if err != nil {
		return SaveGlobalSetupResult{}, err
	}
	if validation.SourceGeneration != req.ExpectedGeneration {
		return SaveGlobalSetupResult{}, errors.New("acd settings: stale saved generation; refresh before saving")
	}
	if req.TestedFingerprint != "" && req.TestedFingerprint != validation.Fingerprint {
		return SaveGlobalSetupResult{}, errors.New("acd settings: reviewed settings are stale; review the current draft again")
	}
	if len(validation.Missing) > 0 {
		return SaveGlobalSetupResult{}, &ConfirmationRequiredError{Missing: validation.Missing}
	}
	if mode := validation.ResolvedHot[config.FieldIntentVerification]; mode != "none" && mode != "structural" {
		return SaveGlobalSetupResult{}, errors.New("acd settings: project verification is configured per repository")
	}
	for _, field := range []string{config.FieldVerificationFastCommand, config.FieldVerificationFullCommand} {
		if strings.TrimSpace(req.Values[field]) != "" {
			return SaveGlobalSetupResult{}, errors.New("acd settings: project verification commands cannot be saved by global setup")
		}
	}
	confirmations, err := globalSetupConfirmations(req.Confirmations)
	if err != nil {
		return SaveGlobalSetupResult{}, err
	}
	err = s.store.UpdateExpectedPrepared(req.ExpectedGeneration, req.Prepare, func(doc *config.Document) error {
		if req.Replace {
			doc.Settings.Global = config.Overrides{}
		}
		changes := req.Changes
		if changes == nil {
			changes = make(map[string]*string, len(req.Values))
			for name, value := range req.Values {
				changes[name] = &value
			}
		}
		for name, value := range changes {
			field, err := persistedField(name)
			if err != nil {
				return err
			}
			if value == nil {
				delete(doc.Settings.Global, name)
				continue
			}
			raw, err := encodePersistedField(field, *value)
			if err != nil {
				return err
			}
			doc.Settings.Global[name] = raw
		}
		if req.Changes != nil {
			fields, err := config.ResolveAll(config.ResolveInput{Global: doc.Settings.Global, LookupEnv: s.lookupEnv, LegacyDefaults: config.LegacyFinishDefaults(doc, []string{"global"}, s.lookupEnv)})
			if err != nil {
				return err
			}
			for name, expected := range validation.ResolvedHot {
				if fields[name].EffectiveValue() != expected {
					return fmt.Errorf("acd settings: saved field %s does not match the reviewed settings", name)
				}
			}
		}
		doc.Settings.GlobalSetupApproval = &config.GlobalSetupApproval{
			Generation:    req.ExpectedGeneration + 1,
			Fingerprint:   validation.Fingerprint,
			Confirmations: confirmations,
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, config.ErrStaleGeneration) {
			return SaveGlobalSetupResult{}, fmt.Errorf("acd settings: stale saved generation: %w", err)
		}
		return SaveGlobalSetupResult{}, sanitizeError(err)
	}
	return SaveGlobalSetupResult{
		Generation:  req.ExpectedGeneration + 1,
		Fingerprint: validation.Fingerprint,
	}, nil
}

func globalSetupConfirmations(values []ai.ConfirmationRequirement) ([]string, error) {
	seen := make(map[ai.ConfirmationRequirement]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == ai.ConfirmationVerificationCommand {
			return nil, errors.New("acd settings: project verification command approval cannot be global")
		}
		switch value {
		case ai.ConfirmationEndpointCredentials, ai.ConfirmationInsecureEndpointCredentials,
			ai.ConfirmationSubprocessExecution,
			ai.ConfirmationDiffEgress:
		default:
			return nil, fmt.Errorf("acd settings: unsupported global confirmation %q", cleanText(string(value)))
		}
		if !seen[value] {
			seen[value] = true
			out = append(out, string(value))
		}
	}
	sort.Strings(out)
	return out, nil
}

type Validation struct {
	Fingerprint      string
	Confirmations    []ai.ConfirmationRequirement
	Missing          []ai.ConfirmationRequirement
	RestartChanged   []string
	ResolvedHot      map[string]string
	ProviderConfig   ai.ProviderConfig
	SourceGeneration uint64
}

type ConfirmationRequiredError struct{ Missing []ai.ConfirmationRequirement }

func (e *ConfirmationRequiredError) Error() string {
	values := make([]string, len(e.Missing))
	for i := range e.Missing {
		values[i] = string(e.Missing[i])
	}
	return "These settings need your approval. Open acd config to review the requested permissions: " + strings.Join(values, ", ")
}

func (s *Service) Validate(ctx context.Context, draft map[string]string, confirmed []ai.ConfirmationRequirement) (Validation, error) {
	doc, err := s.store.Load()
	if err != nil {
		return Validation{}, sanitizeError(err)
	}
	resolved, restartChanged, err := s.resolveDraft(doc, draft)
	if err != nil {
		return Validation{}, err
	}
	cfg, err := s.providerConfig(resolved)
	if err != nil {
		return Validation{}, err
	}
	var required []ai.ConfirmationRequirement
	if cfg.CommitStrategy == ai.CommitStrategyIntent {
		providerValidation, err := ai.ValidateProviderConfig(cfg)
		if err != nil {
			return Validation{}, sanitizeError(err)
		}
		required = append(required, providerValidation.Confirmations...)
	}
	verificationMode := resolved[config.FieldIntentVerification]
	verificationCommand := ""
	switch verificationMode {
	case "fast":
		verificationCommand = resolved[config.FieldVerificationFastCommand]
	case "full":
		verificationCommand = resolved[config.FieldVerificationFullCommand]
	}
	if (verificationMode == "fast" || verificationMode == "full") &&
		strings.TrimSpace(verificationCommand) == "" {
		return Validation{}, errors.New("acd settings: approved verification command is not configured")
	}
	if verificationMode == "fast" || verificationMode == "full" {
		required = append(required, ai.ConfirmationVerificationCommand)
	}
	confirmedSet := make(map[ai.ConfirmationRequirement]bool, len(confirmed))
	for _, item := range confirmed {
		confirmedSet[item] = true
	}
	var missing []ai.ConfirmationRequirement
	for _, item := range required {
		if !confirmedSet[item] {
			missing = append(missing, item)
		}
	}
	fingerprint, err := config.SettingsFingerprint(resolved)
	if err != nil {
		return Validation{}, err
	}
	return Validation{Fingerprint: fingerprint, Confirmations: required,
		Missing: missing, RestartChanged: restartChanged, ResolvedHot: hotValues(resolved),
		ProviderConfig: cfg, SourceGeneration: doc.Generation}, nil
}

type ProviderTestResult struct {
	Fingerprint   string
	Provider      string
	Latency       time.Duration
	Success       bool
	Confirmations []ai.ConfirmationRequirement
}

func (s *Service) TestProvider(ctx context.Context, draft map[string]string, confirmed []ai.ConfirmationRequirement) (ProviderTestResult, error) {
	validation, err := s.Validate(ctx, draft, confirmed)
	if err != nil {
		return ProviderTestResult{}, err
	}
	testConfirmations := providerTestConfirmations(validation.Confirmations)
	missing := missingConfirmations(testConfirmations, confirmed)
	if len(missing) > 0 {
		return ProviderTestResult{}, &ConfirmationRequiredError{Missing: missing}
	}
	if validation.ProviderConfig.CommitStrategy == ai.CommitStrategyLocal {
		return ProviderTestResult{Fingerprint: validation.Fingerprint, Provider: "local", Success: true}, nil
	}
	result, err := s.probe(ctx, validation.ProviderConfig)
	out := ProviderTestResult{Fingerprint: validation.Fingerprint,
		Provider: cleanText(result.Provider), Latency: result.Latency,
		Success: result.Success, Confirmations: testConfirmations}
	if err != nil {
		return out, sanitizeErrorWithSecrets(err, validation.ProviderConfig.APIKey)
	}
	return out, nil
}

func providerTestConfirmations(confirmations []ai.ConfirmationRequirement) []ai.ConfirmationRequirement {
	out := make([]ai.ConfirmationRequirement, 0, len(confirmations))
	for _, confirmation := range confirmations {
		if confirmation != ai.ConfirmationDiffEgress &&
			confirmation != ai.ConfirmationVerificationCommand {
			out = append(out, confirmation)
		}
	}
	return out
}

func missingConfirmations(required, confirmed []ai.ConfirmationRequirement) []ai.ConfirmationRequirement {
	confirmedSet := make(map[ai.ConfirmationRequirement]struct{}, len(confirmed))
	for _, confirmation := range confirmed {
		confirmedSet[confirmation] = struct{}{}
	}
	missing := make([]ai.ConfirmationRequirement, 0, len(required))
	for _, confirmation := range required {
		if _, ok := confirmedSet[confirmation]; !ok {
			missing = append(missing, confirmation)
		}
	}
	return missing
}

func (s *Service) resolveDraft(doc *config.Document, draft map[string]string) (map[string]string, []string, error) {
	baseInput := s.authoringResolutionInput(doc)
	baseFields, err := config.ResolveAll(baseInput)
	if err != nil {
		return nil, nil, sanitizeError(err)
	}
	draftOverrides := config.Overrides{}
	var restartChanged []string
	for name, raw := range draft {
		field, ok := config.LookupField(name)
		if !ok {
			return nil, nil, config.UnsupportedFieldError(cleanText(name))
		}
		if field.Sensitive || !field.Persistable {
			continue
		}
		if hasUnsafeText(raw) {
			return nil, nil, fmt.Errorf("acd settings: field %q contains unsafe text", field.Name)
		}
		normalized, err := normalizeDraftValue(field, raw)
		if err != nil {
			return nil, nil, err
		}
		draftOverrides[name], _ = json.Marshal(normalized)
	}
	draftInput := baseInput
	draftInput.Draft = draftOverrides
	resolvedFields, err := config.ResolveAll(draftInput)
	if err != nil {
		return nil, nil, sanitizeError(err)
	}
	resolved := make(map[string]string, len(resolvedFields))
	for _, field := range config.Catalog() {
		value := resolvedFields[field.Name].EffectiveValue()
		resolved[field.Name] = value
		if field.Boundary == config.ApplyRestart && value != baseFields[field.Name].EffectiveValue() {
			restartChanged = append(restartChanged, field.Name)
		}
	}
	sort.Strings(restartChanged)
	return resolved, restartChanged, nil
}

func (s *Service) authoringResolutionInput(doc *config.Document) config.ResolveInput {
	input := config.ResolveInput{Global: doc.Settings.Global, LookupEnv: s.lookupEnv, LegacyDefaults: config.LegacyFinishDefaults(doc, []string{"global"}, s.lookupEnv)}
	if s.globalOnly {
		return input
	}
	repository := doc.Settings.Repositories[s.repoHash]
	input.Repository = repository.Fields
	input.Profile = doc.Settings.Profiles[repository.Profile].Fields
	input.LegacyDefaults = config.LegacyFinishDefaults(doc, []string{"repository:" + s.repoHash, "profile:" + repository.Profile, "global"}, s.lookupEnv)
	return input
}

func persistedField(name string) (config.FieldDefinition, error) {
	field, ok := config.LookupField(name)
	if !ok {
		return config.FieldDefinition{}, config.UnsupportedFieldError(cleanText(name))
	}
	if !field.Persistable || (field.Sensitive && name == config.FieldAPIKey) {
		return config.FieldDefinition{}, fmt.Errorf("acd settings: field %q is environment-only", field.Name)
	}
	return field, nil
}

func encodePersistedField(field config.FieldDefinition, value string) (json.RawMessage, error) {
	if hasUnsafeText(value) {
		return nil, fmt.Errorf("acd settings: field %q contains unsafe text", field.Name)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("acd settings: encode field %q: %w", field.Name, err)
	}
	return raw, nil
}

var errRepositoryRequired = errors.New("acd settings: repository runtime service required")

func (s *Service) requireRepository(operation string) error {
	if s != nil && s.db != nil {
		return nil
	}
	return fmt.Errorf("acd settings: %s: %w", operation, errRepositoryRequired)
}

func normalizeDraftValue(field config.FieldDefinition, value string) (string, error) {
	doc := config.NewDocument()
	doc.Settings.Global[field.Name], _ = json.Marshal(value)
	if err := config.ValidateDocument(doc); err != nil {
		return "", sanitizeError(err)
	}
	resolved, err := config.ResolveField(field.Name, config.ResolveInput{Global: doc.Settings.Global})
	if err != nil {
		return "", sanitizeError(err)
	}
	return resolved.EffectiveValue(), nil
}

func (s *Service) providerConfig(values map[string]string) (ai.ProviderConfig, error) {
	base := ai.ProviderConfig{
		Mode: values[config.FieldProvider], BaseURL: values[config.FieldBaseURL],
		Model: values[config.FieldModel], CAFile: values[config.FieldCAFile],
		DiffEgress:     values[config.FieldDiffEgress] == "true",
		CommitStrategy: ai.CommitStrategy(values[config.FieldCommitStrategy]),
		CommitFormat:   ai.CommitFormat(values[config.FieldCommitFormat]),
	}
	var err error
	if base.Timeout, err = time.ParseDuration(values[config.FieldTimeout]); err != nil {
		return ai.ProviderConfig{}, errors.New("acd settings: invalid provider timeout")
	}
	if base.CommitStrategy == ai.CommitStrategyLocal {
		// Provider values are inert in local mode; do not read credentials.
		base.Mode = "deterministic"
		base.DiffEgress = false
		return base, nil
	}
	if base.Mode != "openai-compat" {
		return base, nil
	}
	key, _, err := credentials.Resolve(s.credentials, s.credentialLookup)
	if err != nil {
		return ai.ProviderConfig{}, sanitizeError(err)
	}
	base.APIKey = key
	return base, nil
}

func hotValues(values map[string]string) map[string]string {
	out := map[string]string{}
	for _, field := range config.Catalog() {
		if field.Boundary == config.ApplyHot && field.Persistable && !field.Sensitive {
			out[field.Name] = values[field.Name]
		}
	}
	return out
}

func validateScope(scope Scope, profile string) error {
	switch scope {
	case ScopeGlobal, ScopeRepository:
		return nil
	case ScopeProfile:
		if strings.TrimSpace(profile) == "" || hasUnsafeText(profile) {
			return errors.New("acd settings: profile name is required")
		}
		return nil
	default:
		return fmt.Errorf("acd settings: unsupported scope %q", cleanText(string(scope)))
	}
}

func cleanText(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) && r != '\u007f' {
			return r
		}
		return ' '
	}, value)
	return strings.Join(strings.Fields(value), " ")
}

func hasUnsafeText(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool {
		return !unicode.IsPrint(r) || r == '\u007f'
	}) >= 0
}

func sanitizeError(err error) error {
	if err == nil {
		return nil
	}
	clean := cleanText(err.Error())
	if len(clean) > 1024 {
		clean = clean[:1024]
	}
	return errors.New(clean)
}

func sanitizeErrorWithSecrets(err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	message = ai.SanitizePlannerError(message)
	return sanitizeError(errors.New(message))
}

func nullableID(id int64) sql.NullInt64 {
	return sql.NullInt64{Int64: id, Valid: id > 0}
}

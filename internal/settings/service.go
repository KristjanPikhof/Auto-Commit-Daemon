// Package settings coordinates operator-authored configuration, strict
// provider probes, and per-repository runtime activation state.
package settings

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
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
type NudgeFunc func(context.Context, state.DaemonState) error

type Options struct {
	Roots     paths.Roots
	RepoPath  string
	LookupEnv func(string) (string, bool)
	Probe     ProbeFunc
	Nudge     NudgeFunc
	Now       func() time.Time
}

type Service struct {
	store     *config.Store
	db        *state.DB
	worktree  gitpkg.Worktree
	repoHash  string
	lookupEnv func(string) (string, bool)
	probe     ProbeFunc
	nudge     NudgeFunc
	now       func() time.Time
}

func NewService(ctx context.Context, opts Options) (*Service, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	wt, err := gitpkg.ResolveWorktree(ctx, opts.RepoPath)
	if err != nil {
		return nil, fmt.Errorf("acd settings: resolve repository: %w", err)
	}
	repoHash, err := paths.RepoHash(wt.Root)
	if err != nil {
		return nil, fmt.Errorf("acd settings: repository identity: %w", err)
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		return nil, fmt.Errorf("acd settings: open state: %w", err)
	}
	lookup := opts.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	probe := opts.Probe
	if probe == nil {
		probe = ai.ProbeProviderConfig
	}
	nudge := opts.Nudge
	if nudge == nil {
		nudge = fingerprintNudge
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Service{store: config.NewStore(opts.Roots), db: db, worktree: wt,
		repoHash: repoHash, lookupEnv: lookup, probe: probe, nudge: nudge, now: now}, nil
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
			defer func() { doc.Settings.Repositories[s.repoHash] = repo }()
		}
		for name, value := range req.Values {
			field, ok := config.LookupField(name)
			if !ok {
				return fmt.Errorf("acd settings: unsupported field %q", cleanText(name))
			}
			if !field.Persistable || field.Sensitive && name == config.FieldAPIKey {
				return fmt.Errorf("acd settings: field %q is environment-only", field.Name)
			}
			if value == nil {
				delete(target, name)
				continue
			}
			if hasUnsafeText(*value) {
				return fmt.Errorf("acd settings: field %q contains unsafe text", field.Name)
			}
			raw, err := json.Marshal(*value)
			if err != nil {
				return fmt.Errorf("acd settings: encode field %q: %w", field.Name, err)
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
	return "acd settings: explicit confirmation required: " + strings.Join(values, ", ")
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
	providerValidation, err := ai.ValidateProviderConfig(cfg)
	if err != nil {
		return Validation{}, sanitizeError(err)
	}
	confirmedSet := make(map[ai.ConfirmationRequirement]bool, len(confirmed))
	for _, item := range confirmed {
		confirmedSet[item] = true
	}
	var missing []ai.ConfirmationRequirement
	for _, item := range providerValidation.Confirmations {
		if !confirmedSet[item] {
			missing = append(missing, item)
		}
	}
	fingerprint, err := settingsFingerprint(resolved, confirmed)
	if err != nil {
		return Validation{}, err
	}
	return Validation{Fingerprint: fingerprint, Confirmations: providerValidation.Confirmations,
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
	if len(validation.Missing) > 0 {
		return ProviderTestResult{}, &ConfirmationRequiredError{Missing: validation.Missing}
	}
	result, err := s.probe(ctx, validation.ProviderConfig)
	out := ProviderTestResult{Fingerprint: validation.Fingerprint,
		Provider: cleanText(result.Provider), Latency: result.Latency,
		Success: result.Success, Confirmations: append([]ai.ConfirmationRequirement(nil), validation.Confirmations...)}
	if err != nil {
		return out, sanitizeErrorWithSecrets(err, validation.ProviderConfig.APIKey)
	}
	return out, nil
}

func (s *Service) resolveDraft(doc *config.Document, draft map[string]string) (map[string]string, []string, error) {
	repo := doc.Settings.Repositories[s.repoHash]
	profile := doc.Settings.Profiles[repo.Profile]
	resolved := make(map[string]string, len(config.Catalog()))
	for _, field := range config.Catalog() {
		value, err := config.ResolveField(field.Name, config.ResolveInput{
			Repository: repo.Fields, Profile: profile.Fields, Global: doc.Settings.Global,
			LookupEnv: s.lookupEnv,
		})
		if err != nil {
			return nil, nil, sanitizeError(err)
		}
		resolved[field.Name] = value.Value
	}
	var restartChanged []string
	for name, raw := range draft {
		field, ok := config.LookupField(name)
		if !ok {
			return nil, nil, fmt.Errorf("acd settings: unsupported field %q", cleanText(name))
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
		if field.Boundary == config.ApplyRestart && normalized != resolved[name] {
			restartChanged = append(restartChanged, name)
		}
		resolved[name] = normalized
	}
	sort.Strings(restartChanged)
	return resolved, restartChanged, nil
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
	return resolved.Value, nil
}

func (s *Service) providerConfig(values map[string]string) (ai.ProviderConfig, error) {
	base := ai.LoadProviderConfigFromEnv()
	base.Mode = values[config.FieldProvider]
	base.BaseURL = values[config.FieldBaseURL]
	base.Model = values[config.FieldModel]
	base.CAFile = values[config.FieldCAFile]
	base.DiffEgress = values[config.FieldDiffEgress] == "true"
	base.CommitStrategy = ai.CommitStrategy(values[config.FieldCommitStrategy])
	base.CommitFormat = ai.CommitFormat(values[config.FieldCommitFormat])
	var err error
	if base.Timeout, err = time.ParseDuration(values[config.FieldTimeout]); err != nil {
		return ai.ProviderConfig{}, errors.New("acd settings: invalid provider timeout")
	}
	base.IntentWindow, err = strconv.Atoi(values[config.FieldIntentWindow])
	if err != nil {
		return ai.ProviderConfig{}, errors.New("acd settings: invalid intent window")
	}
	base.IntentMinPending, _ = strconv.Atoi(values[config.FieldIntentMinPending])
	base.IntentSettleWindow, _ = time.ParseDuration(values[config.FieldIntentSettleWindow])
	base.IntentMaxPendingAge, _ = time.ParseDuration(values[config.FieldIntentMaxPendingAge])
	base.IntentRecentCommits, _ = strconv.Atoi(values[config.FieldIntentRecentCommits])
	base.IntentDeferLimit, _ = strconv.Atoi(values[config.FieldIntentDeferLimit])
	base.APIKey = ""
	if key, ok := s.lookupEnv(ai.EnvAPIKey); ok {
		base.APIKey = strings.TrimSpace(key)
	}
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

func settingsFingerprint(values map[string]string, confirmations []ai.ConfirmationRequirement) (string, error) {
	payload := map[string]any{}
	for key, value := range hotValues(values) {
		payload[key] = value
	}
	consents := make([]string, len(confirmations))
	for i := range confirmations {
		consents[i] = string(confirmations[i])
	}
	sort.Strings(consents)
	payload["confirmations"] = consents
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("acd settings: fingerprint: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
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

func fingerprintNudge(_ context.Context, st state.DaemonState) error {
	if st.PID <= 0 || !identity.Alive(st.PID) {
		return nil
	}
	if !st.DaemonFingerprint.Valid || st.DaemonFingerprint.String == "" {
		return errors.New("acd settings: daemon fingerprint unavailable")
	}
	fingerprint, err := identity.Capture(st.PID)
	if err != nil {
		return errors.New("acd settings: daemon identity unavailable")
	}
	if daemon.FingerprintToken(fingerprint) != st.DaemonFingerprint.String {
		return errors.New("acd settings: daemon fingerprint mismatch")
	}
	if err := syscall.Kill(st.PID, syscall.SIGUSR1); err != nil {
		return fmt.Errorf("acd settings: signal daemon: %w", err)
	}
	return nil
}

func cleanText(value string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r == '\x1b' || (!unicode.IsPrint(r) && !unicode.IsSpace(r)) {
			return -1
		}
		return r
	}, value))
}

func hasUnsafeText(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool {
		return r == '\x1b' || (!unicode.IsPrint(r) && !unicode.IsSpace(r))
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
	return sanitizeError(errors.New(message))
}

func nullableID(id int64) sql.NullInt64 {
	return sql.NullInt64{Int64: id, Valid: id > 0}
}

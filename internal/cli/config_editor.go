package cli

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

var runConfigEditorUI = settingsui.RunEditor

func newConfigEditorCmd() *cobra.Command {
	legacy := newConfigureCmd()
	legacyRun := legacy.RunE
	var scope string
	cmd := &cobra.Command{
		Use: "edit", Short: "Edit and save global or repository settings",
		Long: `Open an editable settings menu. Choose Global defaults or This repository,
change values, then Save changes to review, test, and apply them together.

Only edited fields are saved. Repository fields can return to inherited values.
API keys are masked and saved in the protected credential store after testing.
Global changes update enabled repositories that inherit them at a safe boundary.
Stopped repositories remain stopped. Restart-required fields are identified.

Without flags, the editor starts at Global defaults. --repo selects a repository.
--accessible uses linear prompts suitable for screen readers.`,
		Example: "  acd config\n  acd config edit --scope global\n  acd config edit --repo .\n  acd config --accessible",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			for _, name := range []string{"replace", "inherit", "strategy", "preset", "wait", "credential-stdin", "dry-run"} {
				if cmd.Flags().Changed(name) {
					if scope != "" {
						return invalidCommandError("acd config: --scope cannot be combined with legacy setup flags")
					}
					return legacyRun(cmd, nil)
				}
			}
			accessible, _ := cmd.Flags().GetBool("accessible")
			if scope != "" && scope != "global" && scope != "repo" {
				return invalidCommandError("acd config: --scope must be global or repo")
			}
			repoFlag, _ := cmd.Flags().GetString("repo")
			if scope == "global" && cmd.Flags().Changed("repo") {
				return invalidCommandError("acd config: --repo conflicts with --scope global")
			}
			useAccessible := accessible || strings.EqualFold(os.Getenv("TERM"), "dumb") || configureTerminalTooShort(cmd.OutOrStdout())
			if !useAccessible && (!settingsInputTTY(cmd.InOrStdin()) || !settingsOutputTTY(cmd.OutOrStdout())) {
				return invalidCommandError("acd config: use an interactive terminal or --accessible")
			}
			wt, err := gitpkg.ResolveWorktree(cmd.Context(), repoFlag)
			repo := ""
			if err == nil {
				repo = wt.Root
			} else if repoFlag != "" || scope == "repo" || !errors.Is(err, gitpkg.ErrNotWorktree) {
				return err
			}
			selected := scope
			if selected == "" {
				selected = "global"
				if cmd.Flags().Changed("repo") {
					selected = "repo"
				}
			}
			roots, err := paths.Resolve()
			if err != nil {
				return err
			}
			backend := &configEditor{roots: roots, repo: repo, lookup: os.LookupEnv}
			return runConfigEditorUI(cmd.Context(), backend, settingsui.EditorOptions{Input: cmd.InOrStdin(), Output: cmd.OutOrStdout(), Scope: selected, Repo: repo, Accessible: useAccessible, NoColor: os.Getenv("NO_COLOR") != ""})
		},
	}
	cmd.Flags().AddFlagSet(legacy.Flags())
	cmd.Flags().StringVar(&scope, "scope", "", "Start at global or repo settings")
	for _, name := range []string{"replace", "inherit", "strategy", "preset", "wait", "credential-stdin", "dry-run"} {
		_ = cmd.Flags().MarkHidden(name)
	}
	return cmd
}

type configEditor struct {
	roots  paths.Roots
	repo   string
	lookup func(string) (string, bool)
	probe  settings.ProbeFunc
	nudge  settings.NudgeFunc
}

type editorProjection struct {
	document *config.Document
	values   map[string]string
	fields   []settingsui.EditorField
}

func (e *configEditor) project(scope string, changes map[string]*string) (editorProjection, error) {
	doc, err := config.NewStore(e.roots).Load()
	if err != nil {
		return editorProjection{}, err
	}
	return e.projectDocument(doc, scope, changes)
}

func (e *configEditor) projectDocument(doc *config.Document, scope string, changes map[string]*string) (editorProjection, error) {
	if scope != "global" && scope != "repo" {
		return editorProjection{}, errors.New("choose global or repository settings")
	}
	if scope == "repo" && e.repo == "" {
		return editorProjection{}, errors.New("repository settings require a Git worktree")
	}
	input := config.ResolveInput{Global: doc.Settings.Global, LookupEnv: e.lookup}
	selected := maps.Clone(doc.Settings.Global)
	if scope == "repo" {
		repo := doc.Settings.Repositories[central.CanonicalID(e.repo)]
		input.Profile = doc.Settings.Profiles[repo.Profile].Fields
		selected = maps.Clone(repo.Fields)
	}
	if selected == nil {
		selected = config.Overrides{}
	}
	for key, value := range changes {
		definition, ok := config.LookupField(key)
		if !ok || !definition.Persistable || definition.Sensitive {
			return editorProjection{}, fmt.Errorf("setting %s cannot be edited here", key)
		}
		if value == nil {
			delete(selected, key)
			continue
		}
		if err := validateEditorField(key, *value); err != nil {
			return editorProjection{}, err
		}
		raw, err := normalizedConfigRaw(definition, *value)
		if err != nil {
			return editorProjection{}, err
		}
		selected[key] = raw
	}
	if scope == "repo" {
		input.Repository = selected
	} else {
		input.Global = selected
	}
	inheritedInput := input
	if scope == "repo" {
		inheritedInput.Repository = nil
	} else {
		inheritedInput.Global = nil
	}
	resolved, _, err := config.ResolveAll(input, selected)
	if err != nil {
		return editorProjection{}, err
	}
	inherited, _, err := config.ResolveAll(inheritedInput, nil)
	if err != nil {
		return editorProjection{}, err
	}
	projection := editorProjection{document: doc, values: map[string]string{}}
	for _, definition := range config.Catalog() {
		if definition.Sensitive || !definition.Persistable {
			continue
		}
		field := resolved[definition.Name]
		value := field.EffectiveValue()
		_, overridden := selected[definition.Name]
		projection.values[definition.Name] = value
		projection.fields = append(projection.fields, settingsui.EditorField{Key: definition.Name, Value: value, Source: string(field.Source), Overridden: overridden, Inherited: inherited[definition.Name].EffectiveValue()})
	}
	return projection, nil
}

func (e *configEditor) Load(_ context.Context, scope string, changes map[string]*string) (settingsui.EditorSnapshot, error) {
	projection, err := e.project(scope, changes)
	if err != nil {
		return settingsui.EditorSnapshot{}, err
	}
	_, source, err := credentials.Resolve(credentials.NewStore(e.roots), e.lookup)
	found := source != credentials.SourceNone
	if err != nil {
		return settingsui.EditorSnapshot{}, err
	}
	credential := "Not configured (select to enter)"
	if found {
		credential = "Configured (select to replace)"
	}
	if source == credentials.SourceEnvironment {
		credential = "Provided by ACD_AI_API_KEY"
	}
	return settingsui.EditorSnapshot{Generation: projection.document.Generation, Fields: projection.fields, Credential: credential, CredentialFromEnvironment: source == credentials.SourceEnvironment}, nil
}

func (e *configEditor) options(repo, secret string) settings.Options {
	lookup := e.lookup
	return settings.Options{Roots: e.roots, RepoPath: repo, LookupEnv: lookup, Probe: e.probe, Nudge: e.nudge, CredentialLookup: func(name string) (string, bool) {
		value, found := lookup(name)
		if name == ai.EnvAPIKey && strings.TrimSpace(value) == "" && secret != "" {
			return secret, true
		}
		return value, found
	}}
}

func (e *configEditor) validationService(ctx context.Context, scope, secret string) (*settings.Service, error) {
	if scope == "global" {
		return settings.NewGlobalService(ctx, e.options("", secret))
	}
	return settings.NewValidationService(ctx, e.options(e.repo, secret))
}

type editorActivation struct {
	repo, database   string
	desired, applied int64
	values           map[string]string
	confirmations    []ai.ConfirmationRequirement
	fingerprint      string
}

type editorPlan struct {
	draft      settingsui.EditorDraft
	values     map[string]string
	validation settings.Validation
	targets    []editorActivation
	text       string
}

func (e *configEditor) Review(ctx context.Context, draft settingsui.EditorDraft) (settingsui.EditorReview, error) {
	plan, err := e.prepare(ctx, draft)
	if err != nil {
		return settingsui.EditorReview{}, err
	}
	return settingsui.EditorReview{Text: plan.text, Save: func(ctx context.Context) (string, error) { return e.save(ctx, plan) }}, nil
}

func (e *configEditor) prepare(ctx context.Context, draft settingsui.EditorDraft) (editorPlan, error) {
	projection, err := e.project(draft.Scope, draft.Changes)
	if err != nil {
		return editorPlan{}, err
	}
	if projection.document.Generation != draft.Generation {
		return editorPlan{}, errors.New("settings changed while this editor was open; reopen it to review the latest values")
	}
	if draft.Credential != "" {
		if value, _ := e.lookup(ai.EnvAPIKey); strings.TrimSpace(value) != "" {
			return editorPlan{}, errors.New("unset ACD_AI_API_KEY before replacing the stored API key")
		}
		if projection.values[config.FieldProvider] != "openai-compat" {
			return editorPlan{}, errors.New("select OpenAI-compatible before entering an API key")
		}
	}
	service, err := e.validationService(ctx, draft.Scope, draft.Credential)
	if err != nil {
		return editorPlan{}, err
	}
	defer service.Close()
	validation, err := service.Validate(ctx, projection.values, nil)
	if err != nil {
		return editorPlan{}, err
	}
	draft.Changes = cloneEditorChanges(draft.Changes)
	plan := editorPlan{draft: draft, values: projection.values, validation: validation}
	var review strings.Builder
	scope := "Global defaults"
	if draft.Scope == "repo" {
		scope = "Repository: " + safeRepoPreview(e.repo)
	}
	fmt.Fprintln(&review, scope)
	before, err := e.projectDocument(projection.document, draft.Scope, nil)
	if err != nil {
		return editorPlan{}, err
	}
	keys := make([]string, 0, len(draft.Changes))
	for key := range draft.Changes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		suffix := ""
		if draft.Changes[key] == nil {
			suffix = " (restore inheritance)"
		}
		fmt.Fprintf(&review, "  %s: %s → %s%s\n", key, editorDisplayValue(key, before.values[key]), editorDisplayValue(key, projection.values[key]), suffix)
	}
	if draft.Credential != "" {
		fmt.Fprintln(&review, "  API key: replace in the protected store (shared by repositories)")
	}
	if len(keys) == 0 && draft.Credential == "" {
		fmt.Fprintln(&review, "  Test and apply the saved settings.")
	}
	fmt.Fprintf(&review, "Connection: %s / %s / %s\n", safePreviewText(plan.values[config.FieldProvider], 100), safePreviewText(plan.values[config.FieldModel], 100), safeEndpointPreview(plan.values[config.FieldBaseURL]))
	writeEditorPermissions(&review, validation)
	if len(validation.RestartChanged) > 0 {
		fmt.Fprintln(&review, "Restart required for:", strings.Join(validation.RestartChanged, ", "))
	}
	if draft.Scope == "repo" {
		target, err := e.repositoryActivation(ctx, e.repo, plan.values)
		if err != nil {
			return editorPlan{}, err
		}
		plan.targets = append(plan.targets, target)
		fmt.Fprintln(&review, "Apply to this repository at its next safe work boundary. A stopped worker stays stopped.")
	} else {
		targets, err := e.globalActivations(ctx, projection.document, draft, &review)
		if err != nil {
			return editorPlan{}, err
		}
		plan.targets = targets
	}
	plan.text = review.String()
	return plan, nil
}

func editorDisplayValue(key, value string) string {
	if key == config.FieldBaseURL {
		return safeEndpointPreview(value)
	}
	return safePreviewText(value, 160)
}

func writeEditorPermissions(out io.Writer, validation settings.Validation) {
	for _, requirement := range validation.Confirmations {
		switch requirement {
		case ai.ConfirmationEndpointCredentials:
			fmt.Fprintln(out, "Permission: send credentials to", safeEndpointPreview(validation.ProviderConfig.BaseURL))
		case ai.ConfirmationInsecureEndpointCredentials:
			fmt.Fprintln(out, "Permission: use an unencrypted HTTP connection")
		case ai.ConfirmationDiffEgress:
			fmt.Fprintln(out, "Permission: send redacted repository changes to the selected provider")
		case ai.ConfirmationSubprocessExecution:
			fmt.Fprintln(out, "Permission: run the selected local provider")
		case ai.ConfirmationVerificationCommand:
			mode := validation.ResolvedHot[config.FieldIntentVerification]
			fmt.Fprintln(out, "Permission: run project verification:", safePreviewText(validation.ResolvedHot["verification."+mode+".command"], 4096))
		case ai.ConfirmationIntentRepair:
			fmt.Fprintln(out, "Permission: repair eligible recent ACD-owned commits within the configured limits")
		}
	}
}

func cloneEditorChanges(changes map[string]*string) map[string]*string {
	out := make(map[string]*string, len(changes))
	for key, value := range changes {
		if value == nil {
			out[key] = nil
		} else {
			copy := *value
			out[key] = &copy
		}
	}
	return out
}

func (e *configEditor) repositoryActivation(ctx context.Context, repo string, values map[string]string) (editorActivation, error) {
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		return editorActivation{}, err
	}
	target := editorActivation{repo: wt.Root, database: state.DBPathFromGitDir(wt.GitDir), values: values}
	db, err := state.OpenReadOnly(ctx, target.database)
	if errors.Is(err, os.ErrNotExist) {
		return target, nil
	}
	if err != nil {
		return target, err
	}
	defer db.Close()
	runtime, err := state.RuntimeConfigActivationState(ctx, db)
	if err != nil {
		return target, err
	}
	target.desired = runtime.DesiredRevisionID.Int64
	target.applied = runtime.AppliedRevisionID.Int64
	if target.desired != target.applied {
		return target, fmt.Errorf("%s already has a settings change waiting to apply; wait for it before saving", safeRepoPreview(repo))
	}
	return target, nil
}

func (e *configEditor) globalActivations(ctx context.Context, doc *config.Document, draft settingsui.EditorDraft, out io.Writer) ([]editorActivation, error) {
	registry, err := central.Load(e.roots)
	if err != nil {
		return nil, err
	}
	targets := []editorActivation{}
	updated := *doc
	updated.Settings = doc.Settings
	updated.Settings.Global = maps.Clone(doc.Settings.Global)
	for key, value := range draft.Changes {
		if value == nil {
			delete(updated.Settings.Global, key)
		} else {
			definition, _ := config.LookupField(key)
			raw, err := normalizedConfigRaw(definition, *value)
			if err != nil {
				return nil, err
			}
			updated.Settings.Global[key] = raw
		}
	}
	for _, record := range registry.Repos {
		if record.LifecycleDisabled() {
			continue
		}
		scoped := *e
		scoped.repo = record.Path
		before, err := scoped.projectDocument(doc, "repo", nil)
		if err != nil {
			return nil, err
		}
		after, err := scoped.projectDocument(&updated, "repo", nil)
		if err != nil {
			return nil, err
		}
		changes := map[string]string{}
		for key, value := range after.values {
			if before.values[key] != value {
				changes[key] = value
			}
		}
		keyChanged := draft.Credential != "" && after.values[config.FieldProvider] == "openai-compat"
		if len(changes) == 0 && !keyChanged {
			fmt.Fprintf(out, "Keeps repository/profile overrides: %s\n", safeRepoPreview(record.Path))
			continue
		}
		target, err := e.repositoryActivation(ctx, record.Path, nil)
		if err != nil {
			return nil, err
		}
		if target.applied == 0 {
			fmt.Fprintf(out, "Uses global defaults on next start: %s\n", safeRepoPreview(record.Path))
			continue
		}
		db, err := state.OpenReadOnly(ctx, target.database)
		if err != nil {
			return nil, err
		}
		revision, readErr := state.ConfigRevisionByID(ctx, db, target.applied)
		_ = db.Close()
		if readErr != nil {
			return nil, readErr
		}
		values, _, err := credentialRuntimeContract(revision.SnapshotJSON)
		if err != nil {
			return nil, err
		}
		restart := false
		for key, value := range changes {
			definition, _ := config.LookupField(key)
			if definition.Boundary == config.ApplyRestart {
				restart = true
				continue
			}
			values[key] = value
		}
		if restart {
			fmt.Fprintf(out, "Restart-required fields take effect on next start: %s\n", safeRepoPreview(record.Path))
		}
		target.values = values
		service, err := settings.NewValidationService(ctx, e.options(record.Path, draft.Credential))
		if err != nil {
			return nil, err
		}
		validation, validationErr := service.Validate(ctx, values, nil)
		_ = service.Close()
		if validationErr != nil {
			return nil, fmt.Errorf("%s: %w", safeRepoPreview(record.Path), validationErr)
		}
		target.confirmations = validation.Confirmations
		fmt.Fprintf(out, "Apply inherited changes: %s (%s / %s)\n", safeRepoPreview(record.Path), safeEndpointPreview(validation.ProviderConfig.BaseURL), safePreviewText(validation.ProviderConfig.Model, 100))
		writeEditorPermissions(out, validation)
		targets = append(targets, target)
	}
	return targets, nil
}

func (e *configEditor) save(ctx context.Context, plan editorPlan) (string, error) {
	fresh, err := e.prepare(ctx, plan.draft)
	if err != nil {
		return "", err
	}
	if fresh.validation.ProviderConfig.APIKey != plan.validation.ProviderConfig.APIKey || fresh.text != plan.text || !maps.Equal(fresh.values, plan.values) || !editorTargetsEqual(fresh.targets, plan.targets) {
		return "", errors.New("settings or affected repositories changed; review again before saving")
	}
	// Share successful synthetic probes across repositories using the same
	// connection. This cache exists only for this save and is never serialized.
	cached := *e
	probes := map[editorProbeConnection]ai.ProviderProbeResult{}
	probe := e.probe
	if probe == nil {
		probe = ai.ProbeProviderConfig
	}
	cached.probe = func(ctx context.Context, cfg ai.ProviderConfig) (ai.ProviderProbeResult, error) {
		key := editorProbeConnection{mode: cfg.Mode, endpoint: cfg.BaseURL, model: cfg.Model, credential: cfg.APIKey, ca: cfg.CAFile, timeout: cfg.Timeout.String(), format: string(cfg.CommitFormat)}
		if result, found := probes[key]; found {
			return result, nil
		}
		result, err := probe(ctx, cfg)
		if err == nil && result.Success {
			probes[key] = result
		}
		return result, err
	}
	e = &cached
	// All probes happen before authoring, credentials, or runtime state are written.
	service, err := e.validationService(ctx, plan.draft.Scope, plan.draft.Credential)
	if err != nil {
		return "", err
	}
	defer service.Close()
	tested, err := service.TestProvider(ctx, plan.values, plan.validation.Confirmations)
	if err != nil {
		return "", err
	}
	if !tested.Success {
		return "", errors.New("connection test failed; settings and API key were not saved")
	}
	for index := range plan.targets {
		target := &plan.targets[index]
		if plan.draft.Scope == "repo" {
			target.confirmations = plan.validation.Confirmations
			target.fingerprint = tested.Fingerprint
			continue
		}
		validator, err := settings.NewValidationService(ctx, e.options(target.repo, plan.draft.Credential))
		if err != nil {
			return "", err
		}
		result, probeErr := validator.TestProvider(ctx, target.values, target.confirmations)
		_ = validator.Close()
		if probeErr != nil {
			return "", probeErr
		}
		if !result.Success {
			return "", fmt.Errorf("connection test failed for %s; settings were not saved", safeRepoPreview(target.repo))
		}
		target.fingerprint = result.Fingerprint
	}
	fresh, err = e.prepare(ctx, plan.draft)
	if err != nil {
		return "", err
	}
	if fresh.validation.ProviderConfig.APIKey != plan.validation.ProviderConfig.APIKey || fresh.text != plan.text || !editorTargetsEqual(fresh.targets, plan.targets) {
		return "", errors.New("settings or affected repositories changed during testing; review again")
	}
	oldCredential, hadCredential := "", false
	if plan.draft.Credential != "" {
		oldCredential, err = configureCredentialRead(e.roots)
		if err == nil {
			hadCredential = true
		} else if !errors.Is(err, credentials.ErrNotFound) {
			return "", err
		}
		if err := configureCredentialWrite(e.roots, plan.draft.Credential); err != nil {
			return "", err
		}
	}
	generation := plan.draft.Generation
	if plan.draft.Scope == "global" {
		result, saveErr := service.SaveGlobalSetup(ctx, settings.SaveGlobalSetupRequest{Values: plan.values, Changes: plan.draft.Changes, TestedFingerprint: tested.Fingerprint, Confirmations: plan.validation.Confirmations, ExpectedGeneration: generation})
		err = saveErr
		generation = result.Generation
	} else {
		result, saveErr := service.Save(ctx, settings.SaveRequest{Scope: settings.ScopeRepository, Values: plan.draft.Changes, ExpectedGeneration: generation})
		err = saveErr
		generation = result.Generation
	}
	if err != nil {
		rollback := rollbackConfigureCredential(e.roots, plan.draft.Credential != "", hadCredential, oldCredential)
		return "", fmt.Errorf("settings were not saved: %w; credential rollback: %s", err, rollback)
	}
	var result strings.Builder
	fmt.Fprintln(&result, "Settings saved.")
	for _, target := range plan.targets {
		applied, applyErr := e.activate(ctx, target, generation, plan.draft.Credential)
		if applyErr != nil {
			fmt.Fprintf(&result, "Saved, but could not apply to %s: %s. Reopen acd config --repo %s and save to retry.\n", safeRepoPreview(target.repo), safePreviewText(applyErr.Error(), 500), productListShellQuote(target.repo))
			continue
		}
		fmt.Fprintf(&result, "Waiting to apply: %s (revision %d, next safe boundary or next start).\n", safeRepoPreview(target.repo), applied.RevisionID)
	}
	if len(plan.validation.RestartChanged) > 0 {
		fmt.Fprintln(&result, "Restart required for:", strings.Join(plan.validation.RestartChanged, ", "))
	}
	return strings.TrimSpace(result.String()), nil
}

func editorTargetsEqual(a, b []editorActivation) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index].repo != b[index].repo || a[index].database != b[index].database || a[index].desired != b[index].desired || a[index].applied != b[index].applied || !maps.Equal(a[index].values, b[index].values) {
			return false
		}
	}
	return true
}

func (e *configEditor) activate(ctx context.Context, target editorActivation, generation uint64, secret string) (settings.ApplyResult, error) {
	service, err := settings.NewService(ctx, e.options(target.repo, secret))
	if err != nil {
		return settings.ApplyResult{}, err
	}
	defer service.Close()
	request := settings.ApplyRequest{Values: target.values, TestedFingerprint: target.fingerprint, Confirmations: target.confirmations, ExpectedGeneration: generation, ExpectedDesiredRevision: target.desired}
	// Full verification retains the same approval gate as repository setup.
	if target.values[config.FieldIntentVerification] == "full" {
		validationTarget, err := resolveConfigureValidationTarget(ctx, target.repo)
		if err != nil {
			return settings.ApplyResult{}, err
		}
		request.SetupValidation = editorSetupValidation(validationTarget, target)
	}
	return service.Apply(ctx, request)
}

func editorSetupValidation(validationTarget configureValidationTarget, target editorActivation) *settings.SetupValidation {
	command := target.values[config.FieldVerificationFullCommand]
	digest := sha256.Sum256([]byte(command))
	return &settings.SetupValidation{BranchRef: validationTarget.BranchRef, BranchGeneration: validationTarget.BranchGeneration, ExpectedHead: validationTarget.ExpectedHead, Mode: "full", CommandSource: "settings editor", CommandDigest: fmt.Sprintf("%x", digest), ApprovalID: target.fingerprint}
}

// Validate individual connection inputs without requiring a key or making requests.
func validateEditorField(key, value string) error {
	cfg := ai.ProviderConfig{Mode: "openai-compat", BaseURL: ai.DefaultOpenAIBaseURL, Model: "validation", APIKey: "validation-only"}
	switch key {
	case config.FieldProvider:
		cfg.Mode = value
	case config.FieldModel:
		cfg.Model = value
	case config.FieldBaseURL:
		cfg.BaseURL = value
	default:
		return nil
	}
	_, err := ai.ValidateProviderConfig(cfg)
	return err
}

type editorProbeConnection struct {
	mode, endpoint, model, credential, ca, timeout, format string
}

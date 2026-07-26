package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/adapter"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

const configureReportVersion = 2

type configureOptions struct {
	Strategy        string
	Preset          string
	Accessible      bool
	CredentialStdin bool
	DryRun          bool
	JSON            bool
	Repo            string
}

type configureVerificationReport struct {
	Mode     string `json:"mode"`
	Command  string `json:"command,omitempty"`
	Timeout  string `json:"timeout,omitempty"`
	Approved bool   `json:"approved"`
	Status   string `json:"status"`
}

type configureReport struct {
	Version          int                         `json:"version"`
	DryRun           bool                        `json:"dry_run"`
	Repo             string                      `json:"repo"`
	Strategy         string                      `json:"strategy"`
	Preset           string                      `json:"preset"`
	PresetID         string                      `json:"preset_id"`
	PresetVersion    int                         `json:"preset_version"`
	Customized       bool                        `json:"customized"`
	CommitFormat     string                      `json:"commit_format"`
	Provider         string                      `json:"provider"`
	CredentialSource credentials.Source          `json:"credential_source"`
	DiffContext      string                      `json:"diff_context"`
	Verification     configureVerificationReport `json:"verification"`
	RepairEnabled    bool                        `json:"repair_enabled"`
	RepairHorizon    string                      `json:"repair_horizon"`
	RepairMaxCommits string                      `json:"repair_max_commits"`
	RuntimeRevision  int64                       `json:"runtime_revision,omitempty"`
	Daemon           string                      `json:"daemon"`
	HarnessGuidance  []string                    `json:"harness_guidance"`
	Risks            []string                    `json:"risks"`
	Operations       []string                    `json:"operations"`
}

type configureValidationService interface {
	AuthoringPreview() (settings.AuthoringPreview, error)
	Validate(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.Validation, error)
	Close() error
}

var (
	openConfigureSettingsService = func(ctx context.Context, opts settings.Options) (settingsCLIService, error) {
		return settings.NewService(ctx, opts)
	}
	openConfigureValidationService = func(ctx context.Context, opts settings.Options) (configureValidationService, error) {
		return settings.NewValidationService(ctx, opts)
	}
	runConfigureWizard        = settingsui.RunConfigureWizard
	confirmConfigurePreview   = settingsui.ConfirmConfigurePreview
	configureCredentialStatus = func(roots paths.Roots, lookup func(string) (string, bool)) (credentials.Source, bool, error) {
		store := credentials.NewStore(roots)
		_, source, err := credentials.Resolve(store, lookup)
		return source, source != credentials.SourceNone, err
	}
	configureCredentialWrite = func(roots paths.Roots, secret string) error {
		return credentials.NewStore(roots).Set(secret)
	}
	configureCredentialRead = func(roots paths.Roots) (string, error) {
		return credentials.NewStore(roots).Read()
	}
	configureCredentialRemove = func(roots paths.Roots) error {
		_, err := credentials.NewStore(roots).Remove()
		return err
	}
	configureRunVerification = func(ctx context.Context, repo, mode, command, timeout, commit string) (verification.Result, error) {
		duration, err := time.ParseDuration(timeout)
		if err != nil {
			return verification.Result{}, errors.New("acd configure: invalid verification timeout")
		}
		approved, err := verification.NewApprovedCommand(
			repo, "configure-preview", verification.Mode(mode), command, duration)
		if err != nil {
			return verification.Result{}, err
		}
		return (verification.Runner{}).Run(ctx, verification.Request{
			RepoPath: repo, CandidateID: "configure-preview", CommitOID: commit, Command: approved,
		})
	}
	configureEnable    = runControlOn
	configureHarnesses = adapter.DetectInstalled
)

func newConfigureCmd() *cobra.Command {
	var opts configureOptions
	cmd := &cobra.Command{
		Use:   "configure",
		Short: "Guided strategy, preset, provider, and safety setup",
		Long: `Configure ACD for everyday use through one reviewed transaction.

The wizard recommends Intent Balanced, tests the provider with synthetic
content, runs only an exact repository verification command you approve,
stores an optional credential in the protected XDG file, creates one immutable
runtime revision, and enables ACD. It never edits harness hook files.

--dry-run prints the final projection without provider calls, command
execution, credential/settings writes, daemon starts, or hook changes.`,
		Example: `  acd configure
  acd configure --accessible
  acd configure --strategy intent --preset balanced
  printf '%s\n' "$ACD_AI_API_KEY" | acd configure --credential-stdin
  acd configure --dry-run
  acd configure --dry-run --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts.Repo, _ = cmd.Flags().GetString("repo")
			opts.JSON, _ = cmd.Flags().GetBool("json")
			return runConfigure(cmd, opts)
		},
	}
	cmd.Flags().StringVar(&opts.Strategy, "strategy", "", "Preselect event or intent")
	cmd.Flags().StringVar(&opts.Preset, "preset", "", "Preselect fast, balanced, or quality")
	cmd.Flags().BoolVar(&opts.Accessible, "accessible", false, "Use linear screen-reader-friendly prompts")
	cmd.Flags().BoolVar(&opts.CredentialStdin, "credential-stdin", false, "Read the credential from the first standard-input line")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Preview without calls, commands, writes, starts, or hook changes")
	return cmd
}

func runConfigure(cmd *cobra.Command, opts configureOptions) error {
	if opts.JSON && !opts.DryRun {
		return errors.New("acd configure: --json requires --dry-run")
	}
	if opts.CredentialStdin && opts.DryRun {
		return errors.New("acd configure: --credential-stdin has no effect with --dry-run")
	}
	strategy, preset, err := normalizeConfigureMode(opts.Strategy, opts.Preset)
	if err != nil {
		return err
	}
	repo, err := resolveRepo(opts.Repo)
	if err != nil {
		return fmt.Errorf("acd configure: %w", err)
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd configure: resolve paths: %w", err)
	}
	suggested := detectVerificationCommand(repo)
	if opts.DryRun {
		selection := dryRunConfigureSelection(strategy, preset, suggested)
		report, err := buildConfigureReport(repo, selection, credentials.SourceNone, true)
		if err != nil {
			return err
		}
		return renderConfigureReport(cmd.OutOrStdout(), report, opts.JSON)
	}

	accessible := opts.Accessible || strings.EqualFold(os.Getenv("TERM"), "dumb")
	if !accessible && (!settingsInputTTY(cmd.InOrStdin()) || !settingsOutputTTY(cmd.OutOrStdout())) {
		return errors.New("acd configure: rich mode requires interactive stdin and stdout; use --accessible")
	}
	wizardInput := cmd.InOrStdin()
	var stagedCredential []byte
	if opts.CredentialStdin {
		stagedCredential, wizardInput, err = readConfigureCredentialLine(cmd.InOrStdin())
		if err != nil {
			return err
		}
		defer clearBytes(stagedCredential)
	}

	source, hasCredential, err := configureCredentialStatus(roots, os.LookupEnv)
	if err != nil {
		return fmt.Errorf("acd configure: credential status: %w", err)
	}
	lookup := configureLookupEnv(string(stagedCredential))
	previewService, err := openConfigureValidationService(cmd.Context(), settings.Options{
		Roots: roots, RepoPath: repo, LookupEnv: lookup,
	})
	if err != nil {
		return fmt.Errorf("acd configure: prepare read-only preview: %w", err)
	}
	defer previewService.Close()
	authoring, err := previewService.AuthoringPreview()
	if err != nil {
		return fmt.Errorf("acd configure: resolve authoring defaults: %w", err)
	}
	defaults := authoring.Values
	defaults[config.FieldCommitStrategy] = strategy
	defaults[config.FieldCommitPreset] = preset
	if strategy == "intent" && defaults[config.FieldProvider] == "deterministic" {
		defaults[config.FieldProvider] = "openai-compat"
	}
	selection, err := runConfigureWizard(cmd.Context(), settingsui.ConfigureWizardOptions{
		Input: wizardInput, Output: cmd.OutOrStdout(), Accessible: accessible,
		Defaults: defaults, SuggestedCommand: suggested, HasCredential: hasCredential || len(stagedCredential) > 0,
		CredentialFromStdin: opts.CredentialStdin,
	})
	if err != nil {
		return fmt.Errorf("acd configure: wizard: %w", err)
	}
	selection.Strategy, selection.Preset, err = normalizeConfigureMode(selection.Strategy, selection.Preset)
	if err != nil {
		return err
	}
	if len(stagedCredential) > 0 {
		selection.Credential = string(stagedCredential)
	}
	if err := validateConfigureSelection(selection, hasCredential || len(stagedCredential) > 0); err != nil {
		return err
	}
	if err := previewService.Close(); err != nil {
		return fmt.Errorf("acd configure: close initial preview: %w", err)
	}
	lookup = configureLookupEnv(selection.Credential)
	previewService, err = openConfigureValidationService(cmd.Context(), settings.Options{
		Roots: roots, RepoPath: repo, LookupEnv: lookup,
	})
	if err != nil {
		return fmt.Errorf("acd configure: resolve reviewed preview: %w", err)
	}
	defer previewService.Close()
	draft := selectionDraft(selection)
	providerConfirmations := selectionProviderConfirmations(selection)
	validation, err := previewService.Validate(cmd.Context(), draft, providerConfirmations)
	if err != nil {
		return fmt.Errorf("acd configure: resolve reviewed settings: %w", err)
	}
	report, err := buildResolvedConfigureReport(repo, selection, source, false, validation)
	if err != nil {
		return err
	}
	if selection.Credential != "" && source != credentials.SourceEnvironment {
		report.CredentialSource = credentials.SourceFile
	}
	if err := renderConfigureReport(cmd.OutOrStdout(), report, false); err != nil {
		return err
	}
	approval, err := confirmConfigurePreview(cmd.Context(), wizardInput, cmd.OutOrStdout(), accessible,
		settingsui.ConfigurePreviewApprovalOptions{
			VerificationMode: report.Verification.Mode, VerificationCommand: report.Verification.Command,
			RepairEnabled: report.RepairEnabled, RepairHorizon: report.RepairHorizon,
			RepairMaxCommits: report.RepairMaxCommits,
		})
	if err != nil {
		return fmt.Errorf("acd configure: final confirmation: %w", err)
	}
	if !approval.Apply {
		return errors.New("acd configure: final preview declined; no provider call, command, or write was made")
	}
	if report.Verification.Mode != "none" && !approval.Verification {
		return errors.New("acd configure: exact verification command declined; no provider call, command, or write was made")
	}
	if report.RepairEnabled && !approval.Repair {
		return errors.New("acd configure: automatic repair declined; no provider call, command, or write was made")
	}
	selection.VerificationApproved = approval.Verification
	selection.RepairApproved = approval.Repair
	confirmations := selectionConfirmations(selection)
	validation, err = previewService.Validate(cmd.Context(), draft, confirmations)
	if err != nil {
		return fmt.Errorf("acd configure: confirm reviewed settings: %w", err)
	}
	if len(validation.Missing) > 0 {
		return &settings.ConfirmationRequiredError{Missing: validation.Missing}
	}

	progress := configureProgress{}
	service, err := openConfigureSettingsService(cmd.Context(), settings.Options{
		Roots: roots, RepoPath: repo, LookupEnv: lookup,
	})
	if err != nil {
		return progress.fail("open settings service", err, "No changes were made; rerun acd configure.")
	}
	defer service.Close()
	snapshot, err := service.Snapshot(cmd.Context(), settings.ScopeRepository, "")
	if err != nil {
		return progress.fail("read runtime state", err, "No configuration was changed; rerun acd configure.")
	}
	if snapshot.SavedGeneration != authoring.Generation {
		return progress.fail("check authoring generation",
			errors.New("settings changed while the preview was open"),
			"No changes were made; rerun acd configure to review the latest values.")
	}
	liveValidation, err := service.Validate(cmd.Context(), draft, confirmations)
	if err != nil {
		return progress.fail("validate approved settings", err, "No configuration was changed; rerun acd configure.")
	}
	if liveValidation.Fingerprint != validation.Fingerprint ||
		liveValidation.Preset.Reference() != validation.Preset.Reference() ||
		liveValidation.Preset.Customized != validation.Preset.Customized {
		return progress.fail("check approved preview",
			errors.New("effective settings changed after confirmation"),
			"No changes were made; rerun acd configure to approve the current projection.")
	}
	if len(liveValidation.Missing) > 0 {
		return progress.fail("validate approval contract",
			&settings.ConfirmationRequiredError{Missing: liveValidation.Missing},
			"No changes were made; rerun acd configure and approve every displayed risk.")
	}
	tested, err := service.TestProvider(cmd.Context(), draft, confirmations)
	if err != nil {
		return progress.fail("test provider", err, "No configuration was changed; correct the provider and rerun acd configure.")
	}
	if !tested.Success {
		return progress.fail("test provider", errors.New("synthetic provider test did not pass"),
			"No configuration was changed; correct the provider and rerun acd configure.")
	}
	progress.complete("provider_test:passed")
	if report.Verification.Mode != "none" {
		head, err := gitpkg.RevParse(cmd.Context(), repo, "HEAD^{commit}")
		if err != nil {
			return progress.fail("resolve verification candidate", err,
				"No configuration was changed; restore an attached valid HEAD and rerun acd configure.")
		}
		verificationCommand := liveValidation.ResolvedHot[config.FieldVerificationFastCommand]
		verificationTimeout := liveValidation.ResolvedHot[config.FieldVerificationFastTimeout]
		if report.Verification.Mode == "full" {
			verificationCommand = liveValidation.ResolvedHot[config.FieldVerificationFullCommand]
			verificationTimeout = liveValidation.ResolvedHot[config.FieldVerificationFullTimeout]
		}
		result, err := configureRunVerification(cmd.Context(), repo, report.Verification.Mode,
			verificationCommand, verificationTimeout, head)
		if err != nil {
			return progress.fail("run approved verification", err,
				"No configuration was changed; fix the exact approved command or choose Intent Fast.")
		}
		if result.NeedsAttention || result.Status != verification.StatusPassed {
			return progress.fail("run approved verification",
				fmt.Errorf("%s verification did not pass (%s)", report.Verification.Mode, result.Status),
				"No configuration was changed; fix the command failure or choose Intent Fast.")
		}
		progress.complete("verification:passed")
	} else {
		progress.complete("verification:not_required")
	}
	var previousCredential string
	previousCredentialSet := false
	if selection.Credential != "" {
		previousCredential, err = configureCredentialRead(roots)
		if err == nil {
			previousCredentialSet = true
		} else if !errors.Is(err, credentials.ErrNotFound) {
			return progress.fail("inspect protected credential", err,
				"No configuration was changed; repair protected credential permissions and retry.")
		}
		if err := configureCredentialWrite(roots, selection.Credential); err != nil {
			return progress.fail("persist protected credential", err,
				"No settings or runtime revision were changed; repair protected credential permissions and retry.")
		}
		progress.complete("credential:persisted")
	}
	changes := configureSaveValues(selection)
	saved, err := service.Save(cmd.Context(), settings.SaveRequest{
		Scope: settings.ScopeRepository, Values: changes,
		ExpectedGeneration: authoring.Generation,
	})
	if err != nil {
		rollback := rollbackConfigureCredential(roots, selection.Credential != "",
			previousCredentialSet, previousCredential)
		return progress.failWithRollback("save settings", err, rollback,
			"Settings were not saved; rerun acd configure after resolving the reported conflict.")
	}
	progress.complete("settings:saved")
	applied, err := service.Apply(cmd.Context(), settings.ApplyRequest{
		Values: draft, TestedFingerprint: tested.Fingerprint, Confirmations: confirmations,
		ExpectedGeneration: saved.Generation, ExpectedDesiredRevision: snapshot.DesiredRevisionID,
	})
	if err != nil {
		rollback := rollbackConfigureCredential(roots, selection.Credential != "",
			previousCredentialSet, previousCredential)
		return progress.failWithRollback("create runtime revision", err, rollback,
			"Repository settings were saved but not activated; rerun acd configure to test and activate them.")
	}
	progress.complete(fmt.Sprintf("runtime_revision:%d", applied.RevisionID))
	if err := configureEnable(cmd.Context(), io.Discard, repo, false); err != nil {
		return progress.fail("enable daemon", err,
			fmt.Sprintf("Runtime revision %d is queued; run `acd on --repo %s` after resolving daemon health.",
				applied.RevisionID, safeRepoPreview(repo)))
	}
	progress.complete("daemon:enabled")
	report.RuntimeRevision = applied.RevisionID
	report.Daemon = "enabled"
	report.CredentialSource = source
	if selection.Credential != "" && source != credentials.SourceEnvironment {
		report.CredentialSource = credentials.SourceFile
	}
	report.Operations = append([]string(nil), progress.completed...)
	fmt.Fprintf(cmd.OutOrStdout(), "Configured %s.%s@%d in runtime revision %d\n",
		selection.Strategy, selection.Preset, config.PresetCatalogVersion, applied.RevisionID)
	for _, guidance := range report.HarnessGuidance {
		fmt.Fprintln(cmd.OutOrStdout(), guidance)
	}
	return nil
}

func normalizeConfigureMode(strategy, preset string) (string, string, error) {
	strategy = strings.ToLower(strings.TrimSpace(strategy))
	preset = strings.ToLower(strings.TrimSpace(preset))
	if strategy == "" {
		strategy = "intent"
	}
	if preset == "" {
		if strategy == "event" {
			preset = "fast"
		} else {
			preset = "balanced"
		}
	}
	if strategy != "event" && strategy != "intent" {
		return "", "", fmt.Errorf("acd configure: unsupported strategy %q", strategy)
	}
	if preset != "fast" && preset != "balanced" && preset != "quality" {
		return "", "", fmt.Errorf("acd configure: unsupported preset %q", preset)
	}
	return strategy, preset, nil
}

func dryRunConfigureSelection(strategy, preset, suggested string) settingsui.ConfigureSelection {
	provider := "openai-compat"
	if strategy == "event" && preset == "fast" {
		provider = "deterministic"
	}
	mode := configureSelectionVerificationMode(strategy, preset)
	return settingsui.ConfigureSelection{
		Strategy: strategy, Preset: preset, CommitFormat: "imperative",
		Provider: provider, Model: "gpt-5.4-mini", BaseURL: ai.DefaultOpenAIBaseURL,
		ProviderTimeout: "30s", VerificationMode: mode,
		VerificationCommand: suggested,
	}
}

func validateConfigureSelection(selection settingsui.ConfigureSelection, hasCredential bool) error {
	strategy, preset, err := normalizeConfigureMode(selection.Strategy, selection.Preset)
	if err != nil {
		return err
	}
	if selection.Provider == "deterministic" && (strategy != "event" || preset != "fast") {
		return errors.New("acd configure: deterministic provider is supported only by Event Fast")
	}
	if selection.CommitFormat != "imperative" && selection.CommitFormat != "conventional" {
		return fmt.Errorf("acd configure: unsupported commit format %q", selection.CommitFormat)
	}
	if selection.Provider == "openai-compat" && !hasCredential && strings.TrimSpace(selection.Credential) == "" {
		return errors.New("acd configure: OpenAI-compatible provider credential is required")
	}
	needsDiff := strategy == "intent" || preset != "fast"
	if needsDiff && !selection.DiffContextApproved {
		return errors.New("acd configure: regular selected preset requires explicit redacted diff-context approval")
	}
	if selection.VerificationMode != "none" && strings.TrimSpace(selection.VerificationCommand) == "" {
		return errors.New("acd configure: enter an exact verification command or choose Intent Fast")
	}
	return nil
}

func snapshotDraft(snapshot settings.Snapshot) map[string]string {
	out := make(map[string]string, len(snapshot.Fields))
	for _, field := range snapshot.Fields {
		if !field.Sensitive {
			out[field.Name] = field.DraftValue
		}
	}
	return out
}

func selectionDraft(selection settingsui.ConfigureSelection) map[string]string {
	out := make(map[string]string, 12)
	out[config.FieldCommitStrategy] = selection.Strategy
	out[config.FieldCommitPreset] = selection.Preset
	out[config.FieldCommitFormat] = selection.CommitFormat
	out[config.FieldProvider] = selection.Provider
	out[config.FieldModel] = selection.Model
	out[config.FieldBaseURL] = selection.BaseURL
	out[config.FieldTimeout] = selection.ProviderTimeout
	out[config.FieldDiffEgress] = fmt.Sprintf("%t", selection.DiffContextApproved)
	out[config.FieldIntentVerification] = selection.VerificationMode
	if selection.VerificationMode == "fast" {
		out[config.FieldVerificationFastCommand] = selection.VerificationCommand
	} else if selection.VerificationMode == "full" {
		out[config.FieldVerificationFullCommand] = selection.VerificationCommand
	}
	return out
}

func configureSaveValues(selection settingsui.ConfigureSelection) map[string]*string {
	values := map[string]string{
		config.FieldCommitStrategy:     selection.Strategy,
		config.FieldCommitPreset:       selection.Preset,
		config.FieldCommitFormat:       selection.CommitFormat,
		config.FieldProvider:           selection.Provider,
		config.FieldModel:              selection.Model,
		config.FieldBaseURL:            selection.BaseURL,
		config.FieldTimeout:            selection.ProviderTimeout,
		config.FieldDiffEgress:         fmt.Sprintf("%t", selection.DiffContextApproved),
		config.FieldIntentVerification: selection.VerificationMode,
	}
	if selection.VerificationMode == "fast" {
		values[config.FieldVerificationFastCommand] = selection.VerificationCommand
	} else if selection.VerificationMode == "full" {
		values[config.FieldVerificationFullCommand] = selection.VerificationCommand
	}
	out := make(map[string]*string, len(values))
	for key, value := range values {
		copyValue := value
		out[key] = &copyValue
	}
	return out
}

func selectionProviderConfirmations(selection settingsui.ConfigureSelection) []ai.ConfirmationRequirement {
	var out []ai.ConfirmationRequirement
	if selection.EndpointCredentialsApproved {
		out = append(out, ai.ConfirmationEndpointCredentials)
	}
	if selection.SubprocessApproved {
		out = append(out, ai.ConfirmationSubprocessExecution)
	}
	if selection.DiffContextApproved {
		out = append(out, ai.ConfirmationDiffEgress)
	}
	return out
}

func selectionConfirmations(selection settingsui.ConfigureSelection) []ai.ConfirmationRequirement {
	out := selectionProviderConfirmations(selection)
	if selection.VerificationApproved {
		out = append(out, ai.ConfirmationVerificationCommand)
	}
	if selection.RepairApproved {
		out = append(out, ai.ConfirmationIntentRepair)
	}
	return out
}

func buildConfigureReport(repo string, selection settingsui.ConfigureSelection, source credentials.Source, dryRun bool) (configureReport, error) {
	definition, ok := config.LookupPreset(config.CommitStrategy(selection.Strategy), config.PresetName(selection.Preset))
	if !ok {
		return configureReport{}, errors.New("acd configure: selected preset is unavailable")
	}
	verificationStatus := "not_required"
	if selection.VerificationMode != "none" {
		if dryRun {
			verificationStatus = "approval_required"
		} else {
			verificationStatus = "approved_pending_test"
		}
	}
	diff := "not_required"
	if definition.DiffContextRequired {
		diff = "approval_required"
		if selection.DiffContextApproved {
			diff = "approved_redacted"
		}
	}
	report := configureReport{
		Version: configureReportVersion, DryRun: dryRun, Repo: repo,
		Strategy: selection.Strategy, Preset: selection.Preset,
		PresetID: definition.ID(), PresetVersion: definition.Version,
		CommitFormat: selection.CommitFormat, Provider: selection.Provider,
		CredentialSource: source, DiffContext: diff,
		Verification: configureVerificationReport{
			Mode: selection.VerificationMode, Command: selection.VerificationCommand,
			Approved: selection.VerificationApproved, Status: verificationStatus,
		},
		RepairEnabled:    definition.Values[config.FieldIntentRepairEnabled] == "true",
		RepairHorizon:    definition.Values[config.FieldIntentRepairHorizon],
		RepairMaxCommits: definition.Values[config.FieldIntentRepairMaxCommits],
		Daemon:           "unchanged", HarnessGuidance: configureHarnessGuidance(),
		Risks: configureRisks(selection, definition),
		Operations: []string{
			"provider_test:planned", "verification:planned_if_required",
			"credential:persist_after_tests", "settings:save",
			"runtime_revision:create_one", "daemon:enable",
			"harness:report_only",
		},
	}
	if selection.VerificationMode == "fast" {
		report.Verification.Timeout = definition.Values[config.FieldVerificationFastTimeout]
	} else if selection.VerificationMode == "full" {
		report.Verification.Timeout = definition.Values[config.FieldVerificationFullTimeout]
	}
	return report, nil
}

func buildResolvedConfigureReport(repo string, selection settingsui.ConfigureSelection, source credentials.Source, dryRun bool, validation settings.Validation) (configureReport, error) {
	effective := selection
	values := validation.ResolvedHot
	effective.Strategy = values[config.FieldCommitStrategy]
	effective.Preset = values[config.FieldCommitPreset]
	effective.CommitFormat = values[config.FieldCommitFormat]
	effective.Provider = values[config.FieldProvider]
	effective.Model = values[config.FieldModel]
	effective.BaseURL = values[config.FieldBaseURL]
	effective.ProviderTimeout = values[config.FieldTimeout]
	effective.DiffContextApproved = values[config.FieldDiffEgress] == "true"
	effective.VerificationMode = values[config.FieldIntentVerification]
	switch effective.VerificationMode {
	case "fast":
		effective.VerificationCommand = values[config.FieldVerificationFastCommand]
	case "full":
		effective.VerificationCommand = values[config.FieldVerificationFullCommand]
	default:
		effective.VerificationCommand = ""
	}
	report, err := buildConfigureReport(repo, effective, source, dryRun)
	if err != nil {
		return configureReport{}, err
	}
	report.PresetID = validation.Preset.ID()
	report.PresetVersion = validation.Preset.Version()
	report.Customized = validation.Preset.Customized
	report.RepairEnabled = values[config.FieldIntentRepairEnabled] == "true"
	report.RepairHorizon = values[config.FieldIntentRepairHorizon]
	report.RepairMaxCommits = values[config.FieldIntentRepairMaxCommits]
	report.Verification.Command = safeCommandPreview(effective.VerificationCommand)
	if effective.VerificationMode == "fast" {
		report.Verification.Timeout = values[config.FieldVerificationFastTimeout]
	} else if effective.VerificationMode == "full" {
		report.Verification.Timeout = values[config.FieldVerificationFullTimeout]
	}
	report.Risks = configureRisksResolved(effective, report)
	return report, nil
}

func renderConfigureReport(out io.Writer, report configureReport, jsonOut bool) error {
	if jsonOut {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	customized := ""
	if report.Customized {
		customized = " (customized)"
	}
	suffix := ""
	if report.DryRun {
		suffix = " (dry run)"
	}
	fmt.Fprintf(out, "ACD CONFIGURE PREVIEW%s\n", suffix)
	fmt.Fprintf(out, "Repository: %s\n", safeRepoPreview(report.Repo))
	fmt.Fprintf(out, "Mode: %s %s%s [%s@%d]\n", displayConfigureWord(report.Strategy),
		displayConfigureWord(report.Preset), customized, report.PresetID, report.PresetVersion)
	fmt.Fprintf(out, "Provider: %s; credential source: %s\n",
		safePreviewText(report.Provider, 128), report.CredentialSource)
	fmt.Fprintf(out, "Diff context: %s\n", report.DiffContext)
	fmt.Fprintf(out, "Verification: %s", report.Verification.Mode)
	if report.Verification.Command != "" {
		fmt.Fprintf(out, " — exact command: %s", safeCommandPreview(report.Verification.Command))
	}
	fmt.Fprintln(out)
	for _, risk := range report.Risks {
		fmt.Fprintln(out, "Approval:", risk)
	}
	fmt.Fprintf(out, "Automatic repair: %t; horizon %s; maximum commits %s\n",
		report.RepairEnabled, report.RepairHorizon, report.RepairMaxCommits)
	fmt.Fprintln(out, "Apply order: provider test > verification > credential > settings > one runtime revision > acd on")
	fmt.Fprintln(out, "Harness hooks: report only; no external hook file will be edited")
	return nil
}

func detectVerificationCommand(repo string) string {
	makefile := filepath.Join(repo, "Makefile")
	if file, err := os.Open(makefile); err == nil {
		defer file.Close()
		body, readErr := io.ReadAll(io.LimitReader(file, 1024*1024+1))
		if readErr != nil || len(body) > 1024*1024 {
			return ""
		}
		text := "\n" + string(body)
		if strings.Contains(text, "\ntest:") {
			return "make test"
		}
	}
	if _, err := os.Stat(filepath.Join(repo, "go.mod")); err == nil {
		return "go test ./..."
	}
	return ""
}

func configureHarnessGuidance() []string {
	detected := configureHarnesses()
	if len(detected) > 0 {
		return []string{}
	}
	out := make([]string, 0, len(supportedHarnesses))
	for _, harness := range supportedHarnesses {
		out = append(out, "Harness integration not detected; inspect the existing snippet with `acd setup "+harness+"`.")
	}
	return out
}

func configureRisks(selection settingsui.ConfigureSelection, preset config.PresetDefinition) []string {
	risks := make([]string, 0, 4)
	if preset.DiffContextRequired {
		context := "redacted diff context remains local"
		if !strings.HasPrefix(selection.Provider, "subprocess:") {
			context = "redacted diff context may leave the machine through " + selection.Provider
		}
		risks = append(risks, context)
	}
	if selection.Provider == "openai-compat" &&
		strings.TrimRight(strings.TrimSpace(selection.BaseURL), "/") !=
			strings.TrimRight(ai.DefaultOpenAIBaseURL, "/") {
		risks = append(risks, "provider credentials may be sent to "+safeEndpointPreview(selection.BaseURL))
	}
	if strings.HasPrefix(selection.Provider, "subprocess:") {
		risks = append(risks, "local executable "+safePreviewText(selection.Provider, 128)+" will run")
	}
	if selection.VerificationMode != "none" {
		risks = append(risks, "repository command will run in an ephemeral worktree: "+
			safeCommandPreview(selection.VerificationCommand))
	}
	if preset.Values[config.FieldIntentRepairEnabled] == "true" {
		risks = append(risks, "eligible recent ACD-owned commits may be repaired automatically")
	}
	return risks
}

func configureRisksResolved(selection settingsui.ConfigureSelection, report configureReport) []string {
	definition, ok := config.LookupPreset(config.CommitStrategy(selection.Strategy), config.PresetName(selection.Preset))
	if !ok {
		return nil
	}
	definition.DiffContextRequired = selection.DiffContextApproved
	definition.Values[config.FieldIntentRepairEnabled] = fmt.Sprintf("%t", report.RepairEnabled)
	return configureRisks(selection, definition)
}

type configureProgress struct {
	completed []string
}

func (p *configureProgress) complete(stage string) {
	p.completed = append(p.completed, stage)
}

func (p configureProgress) fail(stage string, cause error, remediation string) error {
	return p.failWithRollback(stage, cause, "", remediation)
}

func (p configureProgress) failWithRollback(stage string, cause error, rollback, remediation string) error {
	completed := "none"
	if len(p.completed) > 0 {
		completed = strings.Join(p.completed, ", ")
	}
	message := fmt.Sprintf("acd configure: %s failed: %s; completed stages: %s",
		safePreviewText(stage, 64), safePreviewText(cause.Error(), 512), completed)
	if rollback != "" {
		message += "; rollback: " + rollback
	}
	if remediation != "" {
		message += "; remediation: " + safePreviewText(remediation, 512)
	}
	return errors.New(message)
}

func rollbackConfigureCredential(roots paths.Roots, changed, previousSet bool, previous string) string {
	if !changed {
		return "not needed"
	}
	var err error
	if previousSet {
		err = configureCredentialWrite(roots, previous)
	} else {
		err = configureCredentialRemove(roots)
	}
	clearString := []byte(previous)
	clearBytes(clearString)
	if err != nil {
		return "protected credential restore failed; use `acd auth status` and `acd auth set --stdin`"
	}
	return "protected credential restored"
}

func safePreviewText(value string, limit int) string {
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if limit > 0 && len(value) > limit {
		return value[:limit] + "..."
	}
	return value
}

func safeCommandPreview(command string) string {
	return safePreviewText(command, 256)
}

func safeRepoPreview(repo string) string {
	return safePreviewText(repo, 512)
}

func safeEndpointPreview(value string) string {
	value = strings.TrimSpace(value)
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return safePreviewText(value, 256)
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return safePreviewText(parsed.String(), 256)
}

func configureLookupEnv(secret string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, set := os.LookupEnv(name)
		if name == ai.EnvAPIKey {
			if set && strings.TrimSpace(value) != "" {
				return value, true
			}
			if strings.TrimSpace(secret) != "" {
				return secret, true
			}
		}
		return value, set
	}
}

func readConfigureCredentialLine(input io.Reader) ([]byte, io.Reader, error) {
	reader := bufio.NewReader(io.LimitReader(input, maxCredentialInput+1))
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, reader, fmt.Errorf("acd configure: read credential: %w", err)
	}
	if len(line) > maxCredentialInput {
		return nil, reader, errors.New("acd configure: credential input is too large")
	}
	value := []byte(strings.TrimSpace(line))
	if len(value) == 0 {
		return nil, reader, errors.New("acd configure: credential input is empty")
	}
	return value, reader, nil
}

func configureSelectionVerificationMode(strategy, preset string) string {
	if strategy != "intent" {
		return "none"
	}
	switch preset {
	case "balanced":
		return "fast"
	case "quality":
		return "full"
	default:
		return "none"
	}
}

func displayConfigureWord(value string) string {
	if value == "" {
		return ""
	}
	return strings.ToUpper(value[:1]) + value[1:]
}

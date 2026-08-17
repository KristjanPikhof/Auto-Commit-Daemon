package cli

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/adapter"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const configureReportVersion = 5

type configureOptions struct {
	Strategy        string
	Preset          string
	Accessible      bool
	CredentialStdin bool
	DryRun          bool
	Wait            bool
	JSON            bool
	Repo            string
	Replace         bool
	Inherit         bool
}

type configureVerificationReport struct {
	Mode             string `json:"mode"`
	Command          string `json:"command,omitempty"`
	CommandSource    string `json:"command_source,omitempty"`
	Timeout          string `json:"timeout,omitempty"`
	ExpectedDuration string `json:"expected_duration,omitempty"`
	Approved         bool   `json:"approved"`
	Status           string `json:"status"`
}

type configureReport struct {
	Version             int                         `json:"version"`
	DryRun              bool                        `json:"dry_run"`
	Scope               string                      `json:"scope"`
	Repo                string                      `json:"repo,omitempty"`
	Experience          string                      `json:"experience"`
	Strategy            string                      `json:"strategy"`
	Preset              string                      `json:"preset"`
	PresetID            string                      `json:"preset_id"`
	PresetVersion       int                         `json:"preset_version"`
	Customized          bool                        `json:"customized"`
	CommitFormat        string                      `json:"commit_format"`
	Provider            string                      `json:"provider"`
	Model               string                      `json:"model,omitempty"`
	Endpoint            string                      `json:"endpoint,omitempty"`
	ProviderTimeout     string                      `json:"provider_timeout,omitempty"`
	CredentialSource    credentials.Source          `json:"credential_source"`
	DiffContext         string                      `json:"diff_context"`
	Verification        configureVerificationReport `json:"verification"`
	RepairEnabled       bool                        `json:"repair_enabled"`
	RepairHorizon       string                      `json:"repair_horizon"`
	RepairMaxCommits    string                      `json:"repair_max_commits"`
	RuntimeRevision     int64                       `json:"runtime_revision,omitempty"`
	ExecutionMode       string                      `json:"execution_mode"`
	Readiness           string                      `json:"readiness"`
	Daemon              string                      `json:"daemon"`
	HarnessGuidance     []string                    `json:"harness_guidance"`
	Risks               []string                    `json:"risks"`
	Operations          []string                    `json:"operations"`
	ConfigurationAction string                      `json:"configuration_action"`
}

type configureValidationService interface {
	AuthoringPreview() (settings.AuthoringPreview, error)
	Validate(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.Validation, error)
	Close() error
}

type configureGlobalSettingsService interface {
	configureValidationService
	TestProvider(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.ProviderTestResult, error)
	SaveGlobalSetup(context.Context, settings.SaveGlobalSetupRequest) (settings.SaveGlobalSetupResult, error)
}

var (
	openConfigureSettingsService = func(ctx context.Context, opts settings.Options) (settingsCLIService, error) {
		return settings.NewService(ctx, opts)
	}
	openConfigureValidationService = func(ctx context.Context, opts settings.Options) (configureValidationService, error) {
		return settings.NewValidationService(ctx, opts)
	}
	openConfigureGlobalSettingsService = func(ctx context.Context, opts settings.Options) (configureGlobalSettingsService, error) {
		return settings.NewGlobalService(ctx, opts)
	}
	runConfigureWizard        = settingsui.RunConfigureWizard
	confirmConfigurePreview   = settingsui.ConfirmConfigurePreview
	chooseConfigureRecovery   = settingsui.ChooseConfigureRecovery
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
	configureEnable    = runControlOn
	configureHarnesses = adapter.DetectInstalled
)

func newConfigureCmd() *cobra.Command {
	var opts configureOptions
	cmd := &cobra.Command{
		Use:   "configure",
		Short: "Configure global defaults or one repository",
		Long: `Configure ACD for everyday use through one reviewed transaction.

Without --repo, configure global defaults that new repositories inherit.
Global setup offers Everyday work or Maximum speed, tests the provider before
writing, and never opens repository state, runs project commands, or starts a
daemon. Use --replace to discard saved global overrides before writing the
reviewed setup from current built-in defaults.

With an explicit --repo, configure that repository. Strict Review is available
only in repository setup and gates publishing on an approved full project
check. Everyday uses ACD's internal structural checks without running project
tests. Use --inherit to remove the repository override and activate the global
defaults. Use --wait only with repository Strict Review to follow validation.
ACD never edits harness hook files.

--dry-run prints the final projection without provider calls, command
execution, credential/settings writes, daemon starts, or hook changes.`,
		Example: `  acd config edit
  acd config edit --replace
  acd config edit --repo .
  acd config edit --repo . --inherit
  acd config edit --repo . --strategy intent --preset quality --wait
  acd config edit --accessible
  acd config edit --strategy intent --preset balanced
  printf '%s\n' "$ACD_AI_API_KEY" | acd config edit --credential-stdin
  acd config edit --dry-run
  acd config edit --dry-run --json`,
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
	cmd.Flags().BoolVar(&opts.Wait, "wait", false, "Wait for repository Strict Review validation")
	cmd.Flags().BoolVar(&opts.Replace, "replace", false, "Replace saved global overrides with the reviewed built-in defaults")
	cmd.Flags().BoolVar(&opts.Inherit, "inherit", false, "Remove a repository override and activate global defaults")
	return cmd
}

func runConfigure(cmd *cobra.Command, opts configureOptions) error {
	if opts.JSON && !opts.DryRun {
		return errors.New("acd config edit: --json requires --dry-run")
	}
	if opts.CredentialStdin && opts.DryRun {
		return errors.New("acd config edit: --credential-stdin has no effect with --dry-run")
	}
	if opts.Wait && opts.DryRun {
		return errors.New("acd config edit: --wait cannot be used with --dry-run")
	}
	repositoryScope := cmd.Flags().Changed("repo")
	if opts.Replace && repositoryScope {
		return errors.New("acd config edit: --replace configures global defaults and conflicts with --repo")
	}
	if opts.Inherit && !repositoryScope {
		return errors.New("acd config edit: --inherit requires an explicit --repo")
	}
	if opts.Replace && opts.Inherit {
		return errors.New("acd config edit: --replace conflicts with --inherit")
	}
	if opts.Inherit && (cmd.Flags().Changed("strategy") ||
		cmd.Flags().Changed("preset") || opts.CredentialStdin || opts.Wait) {
		return errors.New("acd config edit: --inherit conflicts with --strategy, --preset, --credential-stdin, and --wait")
	}
	if opts.Wait && !repositoryScope {
		return errors.New("acd config edit: --wait requires an explicit --repo and Strict Review")
	}
	if repositoryScope {
		return runRepositoryConfigure(cmd, opts)
	}
	return runGlobalConfigure(cmd, opts)
}

func runGlobalConfigure(cmd *cobra.Command, opts configureOptions) error {
	strategy, preset, err := normalizeConfigureMode(opts.Strategy, opts.Preset)
	if err != nil {
		return err
	}
	if err := validateGlobalConfigureMode(strategy, preset); err != nil {
		return err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd config edit: resolve paths: %w", err)
	}
	if opts.DryRun {
		selection := dryRunConfigureSelection(
			strategy, preset, configureVerificationDetection{},
		)
		report, reportErr := buildConfigureReport(
			"", selection, credentials.SourceNone, true,
		)
		if reportErr != nil {
			return reportErr
		}
		configureGlobalReport(&report)
		if opts.Replace {
			report.ConfigurationAction = "replace_global"
			report.Operations[2] = "global_settings:replace_with_approval"
		}
		return renderConfigureReport(cmd.OutOrStdout(), report, opts.JSON)
	}

	accessible := opts.Accessible ||
		strings.EqualFold(os.Getenv("TERM"), "dumb") ||
		configureTerminalTooShort(cmd.OutOrStdout())
	if !accessible &&
		(!settingsInputTTY(cmd.InOrStdin()) ||
			!settingsOutputTTY(cmd.OutOrStdout())) {
		return errors.New(
			"acd config edit: rich mode requires interactive stdin and stdout; use --accessible",
		)
	}
	wizardInput := cmd.InOrStdin()
	var stagedCredential []byte
	if opts.CredentialStdin {
		stagedCredential, wizardInput, err =
			readConfigureCredentialLine(cmd.InOrStdin())
		if err != nil {
			return err
		}
		defer clearBytes(stagedCredential)
	}
	source, hasCredential, err := configureCredentialStatus(
		roots, os.LookupEnv,
	)
	if err != nil {
		return fmt.Errorf("acd config edit: credential status: %w", err)
	}

	lookup := configureLookupEnv(string(stagedCredential))
	previewService, err := openConfigureGlobalSettingsService(
		cmd.Context(), settings.Options{Roots: roots, LookupEnv: lookup},
	)
	if err != nil {
		return fmt.Errorf(
			"acd config edit: prepare global preview: %w", err,
		)
	}
	authoring, err := previewService.AuthoringPreview()
	if err != nil {
		previewService.Close()
		return fmt.Errorf(
			"acd config edit: resolve global defaults: %w", err,
		)
	}
	defaults := authoring.Values
	if opts.Replace {
		defaults, err = builtInConfigureValues(strategy, preset)
		if err != nil {
			return err
		}
	}
	originalProvider := defaults[config.FieldProvider]
	defaults[config.FieldCommitStrategy] = strategy
	defaults[config.FieldCommitPreset] = preset
	defaults[config.FieldIntentVerification] =
		configureSelectionVerificationMode(strategy, preset)
	if strategy == "intent" &&
		defaults[config.FieldProvider] == "deterministic" {
		defaults[config.FieldProvider] = "openai-compat"
	}
	providerConfigured := !opts.Replace && configureSourceIsExplicit(
		authoring.Sources[config.FieldProvider],
	) && originalProvider == defaults[config.FieldProvider]
	openAIConfigured := !opts.Replace && configureSourceIsExplicit(
		authoring.Sources[config.FieldBaseURL],
	) && configureSourceIsExplicit(
		authoring.Sources[config.FieldModel],
	)
	if err := previewService.Close(); err != nil {
		return fmt.Errorf("acd config edit: close global preview: %w", err)
	}

	explicitMode := cmd.Flags().Changed("strategy") ||
		cmd.Flags().Changed("preset")
	selection, err := runConfigureWizard(
		cmd.Context(), settingsui.ConfigureWizardOptions{
			Input: wizardInput, Output: cmd.OutOrStdout(),
			Accessible: accessible, Defaults: defaults,
			ExplicitMode: explicitMode, HasCredential: hasCredential ||
				len(stagedCredential) > 0,
			CredentialFromStdin: opts.CredentialStdin,
			ProviderConfigured:  providerConfigured,
			OpenAIConfigured:    openAIConfigured,
			RepositoryScoped:    false,
		},
	)
	if err != nil {
		return fmt.Errorf("acd config edit: wizard: %w", err)
	}
	selection.Strategy, selection.Preset, err =
		normalizeConfigureMode(selection.Strategy, selection.Preset)
	if err != nil {
		return err
	}
	if err := validateGlobalConfigureMode(
		selection.Strategy, selection.Preset,
	); err != nil {
		return err
	}
	selection.VerificationMode = configureSelectionVerificationMode(
		selection.Strategy, selection.Preset,
	)
	selection.VerificationCommand = ""
	selection.VerificationSource = ""
	selection.ExecutionMode = "global defaults only"
	if len(stagedCredential) > 0 {
		selection.Credential = string(stagedCredential)
	}
	if err := validateConfigureSelection(
		selection, hasCredential || len(stagedCredential) > 0,
	); err != nil {
		return err
	}

	lookup = configureLookupEnv(selection.Credential)
	service, err := openConfigureGlobalSettingsService(
		cmd.Context(), settings.Options{Roots: roots, LookupEnv: lookup},
	)
	if err != nil {
		return fmt.Errorf(
			"acd config edit: open global settings service: %w", err,
		)
	}
	defer service.Close()
	draft := selectionDraft(selection)
	if opts.Replace {
		draft, err = builtInConfigureValues(
			selection.Strategy, selection.Preset,
		)
		if err != nil {
			return err
		}
		for name, value := range selectionDraft(selection) {
			draft[name] = value
		}
	}
	providerConfirmations := selectionProviderConfirmations(selection)
	validation, err := service.Validate(
		cmd.Context(), draft, providerConfirmations,
	)
	if err != nil {
		return fmt.Errorf(
			"acd config edit: resolve reviewed global settings: %w", err,
		)
	}
	report, err := buildResolvedConfigureReport(
		"", selection, source, false, validation,
	)
	if err != nil {
		return err
	}
	configureGlobalReport(&report)
	if opts.Replace {
		report.ConfigurationAction = "replace_global"
		report.Operations[2] = "global_settings:replace_with_approval"
	}
	if selection.Credential != "" &&
		source != credentials.SourceEnvironment {
		report.CredentialSource = credentials.SourceFile
	}
	if err := renderConfigureReport(
		cmd.OutOrStdout(), report, false,
	); err != nil {
		return err
	}
	approval, err := confirmConfigurePreview(
		cmd.Context(), wizardInput, cmd.OutOrStdout(), accessible,
		settingsui.ConfigurePreviewApprovalOptions{
			VerificationMode: report.Verification.Mode,
			RepairEnabled:    report.RepairEnabled,
			RepairHorizon:    report.RepairHorizon,
			RepairMaxCommits: report.RepairMaxCommits,
			Global:           true,
			Action:           report.ConfigurationAction,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"acd config edit: final confirmation: %w", err,
		)
	}
	if !approval.Apply {
		return errors.New(
			"acd config edit: final preview declined; no provider call or write was made",
		)
	}
	if report.RepairEnabled && !approval.Repair {
		return errors.New(
			"acd config edit: automatic repair declined; no provider call or write was made",
		)
	}
	selection.RepairApproved = approval.Repair
	confirmations := selectionConfirmations(selection)
	validation, err = service.Validate(
		cmd.Context(), draft, confirmations,
	)
	if err != nil {
		return fmt.Errorf(
			"acd config edit: confirm reviewed global settings: %w", err,
		)
	}
	if len(validation.Missing) > 0 {
		return &settings.ConfirmationRequiredError{
			Missing: validation.Missing,
		}
	}
	if validation.SourceGeneration != authoring.Generation {
		return errors.New(
			"acd config edit: global settings changed while the preview was open; rerun configure",
		)
	}

	fmt.Fprintln(
		cmd.ErrOrStderr(), "Applying reviewed global configuration...",
	)
	fmt.Fprintln(
		cmd.ErrOrStderr(),
		"[1/3] Testing provider with synthetic content...",
	)
	tested, err := service.TestProvider(
		cmd.Context(), draft, confirmations,
	)
	if err != nil {
		return fmt.Errorf(
			"acd config edit: test provider failed: %w; no configuration was changed",
			err,
		)
	}
	if !tested.Success {
		return errors.New(
			"acd config edit: provider test failed; no configuration was changed",
		)
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "[1/3] Provider test passed.")

	var previousCredential string
	previousCredentialSet := false
	if selection.Credential != "" {
		fmt.Fprintln(
			cmd.ErrOrStderr(),
			"[2/3] Storing protected credential...",
		)
		previousCredential, err = configureCredentialRead(roots)
		if err == nil {
			previousCredentialSet = true
		} else if !errors.Is(err, credentials.ErrNotFound) {
			return fmt.Errorf(
				"acd config edit: inspect protected credential: %w", err,
			)
		}
		if err := configureCredentialWrite(
			roots, selection.Credential,
		); err != nil {
			return fmt.Errorf(
				"acd config edit: persist protected credential: %w", err,
			)
		}
		fmt.Fprintln(
			cmd.ErrOrStderr(),
			"[2/3] Protected credential stored.",
		)
	} else {
		fmt.Fprintln(
			cmd.ErrOrStderr(),
			"[2/3] Existing credential retained.",
		)
	}

	fmt.Fprintln(
		cmd.ErrOrStderr(), "[3/3] Saving global defaults...",
	)
	saved, err := service.SaveGlobalSetup(
		cmd.Context(), settings.SaveGlobalSetupRequest{
			Values:             draft,
			TestedFingerprint:  tested.Fingerprint,
			Confirmations:      confirmations,
			ExpectedGeneration: authoring.Generation,
			Replace:            opts.Replace,
		},
	)
	if err != nil {
		rollback := rollbackConfigureCredential(
			roots, selection.Credential != "",
			previousCredentialSet, previousCredential,
		)
		return fmt.Errorf(
			"acd config edit: save global defaults failed: %w; rollback: %s",
			err, rollback,
		)
	}
	fmt.Fprintln(
		cmd.ErrOrStderr(), "[3/3] Global defaults saved.",
	)
	report.Operations = []string{
		"provider_test:passed",
		"credential:persisted_or_unchanged",
		fmt.Sprintf("global_settings:saved_generation_%d", saved.Generation),
		"repository_state:not_opened",
		"daemon:not_started",
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Global configuration saved.")
	if opts.Replace {
		fmt.Fprintln(
			cmd.OutOrStdout(),
			"Previous global overrides: replaced with the reviewed setup",
		)
	}
	fmt.Fprintln(
		cmd.OutOrStdout(),
		"Repositories: inherit these defaults unless they have an override",
	)
	if selection.VerificationMode == "structural" {
		fmt.Fprintln(
			cmd.OutOrStdout(),
			"Verification: ACD internal atomicity and materialization checks",
		)
	}
	fmt.Fprintln(
		cmd.OutOrStdout(),
		"Project tests: not configured; use `acd config edit --repo .` for Strict Review",
	)
	fmt.Fprintln(cmd.OutOrStdout(), "Daemon: not started")
	for _, guidance := range report.HarnessGuidance {
		fmt.Fprintln(cmd.OutOrStdout(), guidance)
	}
	return nil
}

func runRepositoryConfigure(cmd *cobra.Command, opts configureOptions) error {
	strategy, preset, err := normalizeConfigureMode(opts.Strategy, opts.Preset)
	if err != nil {
		return err
	}
	if err := validateRepositoryConfigureMode(strategy, preset); err != nil {
		return err
	}
	repo, err := resolveRepo(opts.Repo)
	if err != nil {
		return fmt.Errorf("acd config edit: %w", err)
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd config edit: resolve paths: %w", err)
	}
	detected := detectVerificationCommands(repo)
	if opts.DryRun {
		selection := dryRunConfigureSelection(strategy, preset, detected)
		if opts.Inherit {
			global, previewErr := loadGlobalConfigurePreview(
				cmd.Context(), roots, nil,
			)
			if previewErr != nil {
				return previewErr
			}
			selection = configureSelectionFromValues(global.Values)
		}
		report, err := buildConfigureReport(repo, selection, credentials.SourceNone, true)
		if err != nil {
			return err
		}
		if opts.Inherit {
			configureInheritedReport(&report)
		}
		return renderConfigureReport(cmd.OutOrStdout(), report, opts.JSON)
	}

	accessible := opts.Accessible ||
		strings.EqualFold(os.Getenv("TERM"), "dumb") ||
		configureTerminalTooShort(cmd.OutOrStdout())
	if !accessible && (!settingsInputTTY(cmd.InOrStdin()) || !settingsOutputTTY(cmd.OutOrStdout())) {
		return errors.New("acd config edit: rich mode requires interactive stdin and stdout; use --accessible")
	}
	explicitMode := cmd.Flags().Changed("strategy") ||
		cmd.Flags().Changed("preset")
	if !opts.Inherit && !explicitMode && !opts.CredentialStdin {
		existing, found, existingErr := existingConfigureValidation(
			cmd.Context(), repo,
		)
		if existingErr != nil {
			return fmt.Errorf(
				"acd config edit: inspect unfinished setup: %w", existingErr,
			)
		}
		if found {
			switch existing.Status {
			case state.ConfigValidationQueued, state.ConfigValidationRunning:
				fmt.Fprintf(cmd.OutOrStdout(),
					"Configuration validation is %s (attempt %d).\n",
					existing.Status, existing.Attempt)
				fmt.Fprintln(cmd.OutOrStdout(),
					"Capture remains active; commit publishing is waiting.")
				if opts.Wait {
					return waitForConfigureValidation(
						cmd.Context(), cmd.OutOrStdout(), repo, existing.ID,
					)
				}
				fmt.Fprintln(cmd.OutOrStdout(),
					"Safe to close this terminal. Use `acd config edit --wait` to follow it.")
				return nil
			case state.ConfigValidationFailed,
				state.ConfigValidationTimedOut,
				state.ConfigValidationCancelled:
				fmt.Fprintf(cmd.OutOrStdout(),
					"Configuration validation %s (attempt %d).\n",
					existing.Status, existing.Attempt)
				choice, choiceErr := chooseConfigureRecovery(
					cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(),
					accessible,
				)
				if choiceErr != nil {
					return fmt.Errorf(
						"acd config edit: choose recovery: %w", choiceErr,
					)
				}
				switch choice {
				case settingsui.ConfigureRecoveryRetry:
					retry, retried, retryErr := retryConfigureValidation(
						cmd.Context(), repo, existing.ActivationRequestID,
					)
					if retryErr != nil {
						return fmt.Errorf(
							"acd config edit: retry validation: %w", retryErr,
						)
					}
					if !retried {
						return errors.New(
							"acd config edit: validation is no longer retryable; rerun configure",
						)
					}
					if err := configureEnable(
						cmd.Context(), io.Discard, repo, false,
					); err != nil {
						return fmt.Errorf(
							"acd config edit: enable validation worker: %w", err,
						)
					}
					fmt.Fprintf(cmd.OutOrStdout(),
						"Validation retry queued (attempt %d).\n",
						retry.Attempt)
					if opts.Wait {
						return waitForConfigureValidation(
							cmd.Context(), cmd.OutOrStdout(), repo, retry.ID,
						)
					}
					fmt.Fprintln(cmd.OutOrStdout(),
						"Capture remains active; commit publishing is waiting.")
					return nil
				case settingsui.ConfigureRecoveryAdvanced:
					fmt.Fprintln(cmd.OutOrStdout(),
						"Run `acd settings` to edit advanced verification settings.")
					return nil
				case settingsui.ConfigureRecoveryLeave:
					fmt.Fprintln(cmd.OutOrStdout(),
						"Capture-only state left unchanged.")
					return nil
				case settingsui.ConfigureRecoverySwitch:
					// Continue into the normal experience selector.
				}
			}
		}
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
		return fmt.Errorf("acd config edit: credential status: %w", err)
	}
	lookup := configureLookupEnv(string(stagedCredential))
	previewService, err := openConfigureValidationService(cmd.Context(), settings.Options{
		Roots: roots, RepoPath: repo, LookupEnv: lookup,
	})
	if err != nil {
		return fmt.Errorf("acd config edit: prepare read-only preview: %w", err)
	}
	defer previewService.Close()
	authoring, err := previewService.AuthoringPreview()
	if err != nil {
		return fmt.Errorf("acd config edit: resolve authoring defaults: %w", err)
	}
	defaults := authoring.Values
	if opts.Inherit {
		global, previewErr := loadGlobalConfigurePreview(
			cmd.Context(), roots, lookup,
		)
		if previewErr != nil {
			return previewErr
		}
		defaults = global.Values
	}
	originalProvider := defaults[config.FieldProvider]
	if !opts.Inherit {
		defaults[config.FieldCommitStrategy] = strategy
		defaults[config.FieldCommitPreset] = preset
		if strategy == "intent" &&
			defaults[config.FieldProvider] == "deterministic" {
			defaults[config.FieldProvider] = "openai-compat"
		}
	}
	providerConfigured := configureSourceIsExplicit(
		authoring.Sources[config.FieldProvider],
	) && originalProvider == defaults[config.FieldProvider]
	openAIConfigured := configureSourceIsExplicit(
		authoring.Sources[config.FieldBaseURL],
	) && configureSourceIsExplicit(
		authoring.Sources[config.FieldModel],
	)
	var selection settingsui.ConfigureSelection
	if opts.Inherit {
		selection = configureSelectionFromValues(defaults)
	} else {
		selection, err = runConfigureWizard(cmd.Context(), settingsui.ConfigureWizardOptions{
			Input: wizardInput, Output: cmd.OutOrStdout(), Accessible: accessible,
			Defaults:             defaults,
			DetectedQuickCommand: detected.QuickCommand,
			DetectedQuickSource:  detected.QuickSource,
			DetectedFullCommand:  detected.FullCommand,
			DetectedFullSource:   detected.FullSource,
			ExplicitMode:         explicitMode,
			HasCredential:        hasCredential || len(stagedCredential) > 0,
			CredentialFromStdin:  opts.CredentialStdin,
			ProviderConfigured:   providerConfigured,
			OpenAIConfigured:     openAIConfigured,
			RepositoryScoped:     true,
		})
		if err != nil {
			return fmt.Errorf("acd config edit: wizard: %w", err)
		}
	}
	selection.Strategy, selection.Preset, err = normalizeConfigureMode(selection.Strategy, selection.Preset)
	if err != nil {
		return err
	}
	if err := validateRepositoryConfigureMode(
		selection.Strategy, selection.Preset,
	); err != nil {
		return err
	}
	if len(stagedCredential) > 0 {
		selection.Credential = string(stagedCredential)
	}
	if err := validateConfigureSelection(selection, hasCredential || len(stagedCredential) > 0); err != nil {
		return err
	}
	if opts.Wait && selection.VerificationMode != "full" {
		return errors.New(
			"acd config edit: --wait is available only for repository Strict Review",
		)
	}
	if err := previewService.Close(); err != nil {
		return fmt.Errorf("acd config edit: close initial preview: %w", err)
	}
	lookup = configureLookupEnv(selection.Credential)
	previewService, err = openConfigureValidationService(cmd.Context(), settings.Options{
		Roots: roots, RepoPath: repo, LookupEnv: lookup,
	})
	if err != nil {
		return fmt.Errorf("acd config edit: resolve reviewed preview: %w", err)
	}
	defer previewService.Close()
	draft := selectionDraft(selection)
	if opts.Inherit {
		draft = cloneConfigureValues(defaults)
	}
	providerConfirmations := selectionProviderConfirmations(selection)
	validation, err := previewService.Validate(cmd.Context(), draft, providerConfirmations)
	if err != nil {
		return fmt.Errorf("acd config edit: resolve reviewed settings: %w", err)
	}
	report, err := buildResolvedConfigureReport(repo, selection, source, false, validation)
	if err != nil {
		return err
	}
	if selection.Credential != "" && source != credentials.SourceEnvironment {
		report.CredentialSource = credentials.SourceFile
	}
	if opts.Inherit {
		configureInheritedReport(&report)
	}
	if err := renderConfigureReport(cmd.OutOrStdout(), report, false); err != nil {
		return err
	}
	approval, err := confirmConfigurePreview(cmd.Context(), wizardInput, cmd.OutOrStdout(), accessible,
		settingsui.ConfigurePreviewApprovalOptions{
			VerificationMode: report.Verification.Mode, VerificationCommand: report.Verification.Command,
			RepairEnabled: report.RepairEnabled, RepairHorizon: report.RepairHorizon,
			RepairMaxCommits: report.RepairMaxCommits,
			Action:           report.ConfigurationAction,
		})
	if err != nil {
		return fmt.Errorf("acd config edit: final confirmation: %w", err)
	}
	if !approval.Apply {
		return errors.New("acd config edit: final preview declined; no provider call, command, or write was made")
	}
	if (report.Verification.Mode == "fast" ||
		report.Verification.Mode == "full") &&
		!approval.Verification {
		return errors.New("acd config edit: exact verification command declined; no provider call, command, or write was made")
	}
	if report.RepairEnabled && !approval.Repair {
		return errors.New("acd config edit: automatic repair declined; no provider call, command, or write was made")
	}
	selection.VerificationApproved = approval.Verification
	selection.RepairApproved = approval.Repair
	progress := newConfigureProgress(cmd.ErrOrStderr())
	progress.start()
	confirmations := selectionConfirmations(selection)
	validation, err = previewService.Validate(cmd.Context(), draft, confirmations)
	if err != nil {
		return progress.fail("confirm reviewed settings", err,
			"No configuration was changed; rerun acd config edit.")
	}
	if len(validation.Missing) > 0 {
		return progress.fail("confirm reviewed settings",
			&settings.ConfirmationRequiredError{Missing: validation.Missing},
			"No configuration was changed; rerun acd config edit and approve every displayed risk.")
	}

	service, err := openConfigureSettingsService(cmd.Context(), settings.Options{
		Roots: roots, RepoPath: repo, LookupEnv: lookup,
	})
	if err != nil {
		return progress.fail("open settings service", err, "No changes were made; rerun acd config edit.")
	}
	defer service.Close()
	snapshot, err := service.Snapshot(cmd.Context(), settings.ScopeRepository, "")
	if err != nil {
		return progress.fail("read runtime state", err, "No configuration was changed; rerun acd config edit.")
	}
	if snapshot.SavedGeneration != authoring.Generation {
		return progress.fail("check authoring generation",
			errors.New("settings changed while the preview was open"),
			"No changes were made; rerun acd config edit to review the latest values.")
	}
	liveValidation, err := service.Validate(cmd.Context(), draft, confirmations)
	if err != nil {
		return progress.fail("validate approved settings", err, "No configuration was changed; rerun acd config edit.")
	}
	if liveValidation.Fingerprint != validation.Fingerprint ||
		liveValidation.Preset.Reference() != validation.Preset.Reference() ||
		liveValidation.Preset.Customized != validation.Preset.Customized {
		return progress.fail("check approved preview",
			errors.New("effective settings changed after confirmation"),
			"No changes were made; rerun acd config edit to approve the current projection.")
	}
	if len(liveValidation.Missing) > 0 {
		return progress.fail("validate approval contract",
			&settings.ConfirmationRequiredError{Missing: liveValidation.Missing},
			"No changes were made; rerun acd config edit and approve every displayed risk.")
	}
	progress.begin(1, "Testing provider with synthetic content...")
	tested, err := service.TestProvider(cmd.Context(), draft, confirmations)
	if err != nil {
		return progress.fail("test provider", err, "No configuration was changed; correct the provider and rerun acd config edit.")
	}
	if !tested.Success {
		return progress.fail("test provider", errors.New("synthetic provider test did not pass"),
			"No configuration was changed; correct the provider and rerun acd config edit.")
	}
	progress.complete("provider_test:passed")
	progress.success(1, "Provider test passed.")
	var setupValidation *settings.SetupValidation
	if report.Verification.Mode == "full" {
		target, targetErr := resolveConfigureValidationTarget(
			cmd.Context(), repo,
		)
		if targetErr != nil {
			return progress.fail("resolve validation target", targetErr,
				"No configuration was changed; restore an attached valid HEAD and rerun acd config edit.")
		}
		command := report.Verification.Command
		sum := sha256.Sum256([]byte(command))
		setupValidation = &settings.SetupValidation{
			BranchRef:        target.BranchRef,
			BranchGeneration: target.BranchGeneration,
			ExpectedHead:     target.ExpectedHead,
			Mode:             report.Verification.Mode,
			CommandSource:    report.Verification.CommandSource,
			CommandDigest:    fmt.Sprintf("%x", sum[:]),
			ApprovalID:       tested.Fingerprint,
		}
		progress.complete("validation:prepared")
		progress.success(2, "Background validation target prepared.")
	} else {
		progress.complete("validation:not_required")
		progress.success(2, "Project validation is not required.")
	}
	var previousCredential string
	previousCredentialSet := false
	if selection.Credential != "" {
		progress.begin(3, "Storing protected credential...")
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
		progress.success(3, "Protected credential stored.")
	} else {
		progress.complete("credential:unchanged")
		progress.success(3, "Existing credential retained; no credential write required.")
	}
	changes := configureSaveValues(selection)
	var repositoryProfile *string
	if opts.Inherit {
		changes = clearRepositoryValues()
		emptyProfile := ""
		repositoryProfile = &emptyProfile
		progress.begin(4, "Removing repository override...")
	} else {
		progress.begin(4, "Saving repository settings...")
	}
	saved, err := service.Save(cmd.Context(), settings.SaveRequest{
		Scope: settings.ScopeRepository, Values: changes,
		RepositoryProfile:  repositoryProfile,
		ExpectedGeneration: authoring.Generation,
	})
	if err != nil {
		rollback := rollbackConfigureCredential(roots, selection.Credential != "",
			previousCredentialSet, previousCredential)
		return progress.failWithRollback("save settings", err, rollback,
			"Settings were not saved; rerun acd config edit after resolving the reported conflict.")
	}
	if opts.Inherit {
		progress.complete("repository_override:removed")
		progress.success(4, "Repository override removed.")
	} else {
		progress.complete("settings:saved")
		progress.success(4, "Repository settings saved.")
	}
	progress.begin(5, "Creating immutable runtime revision...")
	applied, err := service.Apply(cmd.Context(), settings.ApplyRequest{
		Values: draft, TestedFingerprint: tested.Fingerprint, Confirmations: confirmations,
		ExpectedGeneration: saved.Generation, ExpectedDesiredRevision: snapshot.DesiredRevisionID,
		SetupValidation: setupValidation,
	})
	if err != nil {
		rollback := rollbackConfigureCredential(roots, selection.Credential != "",
			previousCredentialSet, previousCredential)
		return progress.failWithRollback("create runtime revision", err, rollback,
			"Repository settings were saved but not activated; rerun acd config edit to test and activate them.")
	}
	progress.complete(fmt.Sprintf("runtime_revision:%d", applied.RevisionID))
	if applied.ValidationRunID > 0 {
		progress.complete(fmt.Sprintf("validation_queued:%d", applied.ValidationRunID))
		progress.success(5, fmt.Sprintf(
			"Runtime revision %d created; validation job %d queued.",
			applied.RevisionID, applied.ValidationRunID))
	} else {
		progress.success(5, fmt.Sprintf("Runtime revision %d created.", applied.RevisionID))
	}
	progress.begin(6, "Enabling ACD...")
	if err := configureEnable(cmd.Context(), io.Discard, repo, false); err != nil {
		return progress.fail("enable daemon", err,
			fmt.Sprintf("Runtime revision %d is queued; run `acd on --repo %s` after resolving daemon health.",
				applied.RevisionID, safeRepoPreview(repo)))
	}
	progress.complete("daemon:enabled")
	progress.success(6, "ACD enabled.")
	report.RuntimeRevision = applied.RevisionID
	report.Daemon = "enabled"
	report.CredentialSource = source
	if selection.Credential != "" && source != credentials.SourceEnvironment {
		report.CredentialSource = credentials.SourceFile
	}
	report.Operations = append([]string(nil), progress.completed...)
	if applied.ValidationRunID > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "Configuration saved.")
		fmt.Fprintln(cmd.OutOrStdout(), "Capture: active")
		fmt.Fprintf(cmd.OutOrStdout(),
			"Commit publishing: waiting for %s validation\n",
			configureValidationLabel(report.Verification.Mode))
		fmt.Fprintln(cmd.OutOrStdout(), "Validation: running in background")
		if report.Verification.Command != "" {
			fmt.Fprintln(cmd.OutOrStdout(),
				"Command:", safeCommandPreview(report.Verification.Command))
		} else {
			fmt.Fprintln(cmd.OutOrStdout(),
				"Check: built-in structural and materialization gates")
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Safe to close this terminal.")
		if opts.Wait {
			if err := waitForConfigureValidation(
				cmd.Context(), cmd.OutOrStdout(), repo,
				applied.ValidationRunID,
			); err != nil {
				return err
			}
		}
	} else {
		fmt.Fprintf(cmd.OutOrStdout(),
			"Configuration ready: %s@%d; runtime revision %d; daemon enabled.\n",
			report.PresetID, report.PresetVersion, applied.RevisionID)
		if opts.Inherit {
			fmt.Fprintln(
				cmd.OutOrStdout(),
				"Repository settings: inheriting global defaults",
			)
		}
	}
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
		return "", "", fmt.Errorf("acd config edit: unsupported strategy %q", strategy)
	}
	if preset != "fast" && preset != "balanced" && preset != "quality" {
		return "", "", fmt.Errorf("acd config edit: unsupported preset %q", preset)
	}
	return strategy, preset, nil
}

func validateGlobalConfigureMode(strategy, preset string) error {
	if strategy == "intent" && preset == "quality" {
		return errors.New(
			"acd config edit: Strict Review requires an explicit --repo; global setup supports Everyday or Maximum Speed",
		)
	}
	if (strategy == "intent" && preset == "balanced") ||
		(strategy == "event" && preset == "fast") {
		return nil
	}
	return fmt.Errorf(
		"acd config edit: %s.%s is not a global experience; use intent.balanced (Everyday) or event.fast (Maximum Speed)",
		strategy, preset,
	)
}

func validateRepositoryConfigureMode(strategy, preset string) error {
	if (strategy == "intent" &&
		(preset == "balanced" || preset == "quality")) ||
		(strategy == "event" && preset == "fast") {
		return nil
	}
	return fmt.Errorf(
		"acd config edit: %s.%s is not a guided repository experience; use intent.balanced, event.fast, or intent.quality",
		strategy, preset,
	)
}

func configureTerminalTooShort(output io.Writer) bool {
	file, ok := output.(interface{ Fd() uintptr })
	if !ok || !term.IsTerminal(file.Fd()) {
		return false
	}
	_, rows, err := term.GetSize(file.Fd())
	return err == nil && rows > 0 && rows < 24
}

func dryRunConfigureSelection(
	strategy, preset string,
	detected configureVerificationDetection,
) settingsui.ConfigureSelection {
	provider := "openai-compat"
	if strategy == "event" && preset == "fast" {
		provider = "deterministic"
	}
	mode := configureSelectionVerificationMode(strategy, preset)
	var command, source string
	switch mode {
	case "fast":
		command, source = detected.QuickCommand, detected.QuickSource
	case "full":
		command, source = detected.FullCommand, detected.FullSource
	}
	if mode == "fast" && command == "" {
		mode = "structural"
		source = "built-in structural verification"
	}
	return settingsui.ConfigureSelection{
		Experience: configureExperienceName(strategy, preset),
		Strategy:   strategy, Preset: preset, CommitFormat: "imperative",
		Provider: provider, Model: "gpt-5.4-mini", BaseURL: ai.DefaultOpenAIBaseURL,
		ProviderTimeout: ai.DefaultProviderTimeout.String(), VerificationMode: mode,
		VerificationCommand: command, VerificationSource: source,
		ExecutionMode: configureExecutionMode(strategy, preset),
	}
}

func validateConfigureSelection(selection settingsui.ConfigureSelection, hasCredential bool) error {
	strategy, preset, err := normalizeConfigureMode(selection.Strategy, selection.Preset)
	if err != nil {
		return err
	}
	if selection.Provider == "deterministic" && strategy == "intent" && preset == "quality" {
		return errors.New("acd config edit: Strict Review requires a semantic provider")
	}
	if selection.CommitFormat != "imperative" && selection.CommitFormat != "conventional" {
		return fmt.Errorf("acd config edit: unsupported commit format %q", selection.CommitFormat)
	}
	if selection.Provider == "openai-compat" && !hasCredential && strings.TrimSpace(selection.Credential) == "" {
		return errors.New("acd config edit: OpenAI-compatible provider credential is required")
	}
	needsDiff := selection.Provider != "deterministic" &&
		(strategy == "intent" || preset != "fast")
	if needsDiff && !selection.DiffContextApproved {
		return errors.New("acd config edit: regular selected preset requires explicit redacted diff-context approval")
	}
	if (selection.VerificationMode == "fast" ||
		selection.VerificationMode == "full") &&
		strings.TrimSpace(selection.VerificationCommand) == "" {
		return errors.New("acd config edit: no project verification command is available for this experience")
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
	out[config.FieldCAFile] = selection.CAFile
	out[config.FieldDiffEgress] = fmt.Sprintf("%t", selection.DiffContextApproved)
	out[config.FieldIntentVerification] = selection.VerificationMode
	if selection.VerificationMode == "fast" {
		out[config.FieldVerificationFastCommand] = selection.VerificationCommand
	} else if selection.VerificationMode == "full" {
		out[config.FieldVerificationFullCommand] = selection.VerificationCommand
	}
	return out
}

func configureSelectionFromValues(values map[string]string) settingsui.ConfigureSelection {
	strategy := fallbackConfigureValue(values[config.FieldCommitStrategy], "intent")
	preset := fallbackConfigureValue(values[config.FieldCommitPreset], "balanced")
	provider := fallbackConfigureValue(values[config.FieldProvider], "openai-compat")
	baseURL := fallbackConfigureValue(values[config.FieldBaseURL], ai.DefaultOpenAIBaseURL)
	verification := fallbackConfigureValue(
		values[config.FieldIntentVerification],
		configureSelectionVerificationMode(strategy, preset),
	)
	selection := settingsui.ConfigureSelection{
		Experience:          configureExperienceName(strategy, preset),
		Strategy:            strategy,
		Preset:              preset,
		CommitFormat:        fallbackConfigureValue(values[config.FieldCommitFormat], "imperative"),
		Provider:            provider,
		Model:               fallbackConfigureValue(values[config.FieldModel], "gpt-5.4-mini"),
		BaseURL:             baseURL,
		ProviderTimeout:     fallbackConfigureValue(values[config.FieldTimeout], ai.DefaultProviderTimeout.String()),
		CAFile:              strings.TrimSpace(values[config.FieldCAFile]),
		VerificationMode:    verification,
		ExecutionMode:       "immediate",
		DiffContextApproved: values[config.FieldDiffEgress] == "true",
	}
	if verification == "fast" {
		selection.VerificationCommand =
			strings.TrimSpace(values[config.FieldVerificationFastCommand])
	} else if verification == "full" {
		selection.VerificationCommand =
			strings.TrimSpace(values[config.FieldVerificationFullCommand])
	}
	if selection.VerificationCommand != "" {
		selection.VerificationSource = "inherited global setting"
	}
	selection.EndpointCredentialsApproved = provider == "openai-compat" &&
		strings.TrimRight(strings.TrimSpace(baseURL), "/") !=
			strings.TrimRight(ai.DefaultOpenAIBaseURL, "/")
	selection.SubprocessApproved = strings.HasPrefix(provider, "subprocess:")
	return selection
}

func loadGlobalConfigurePreview(
	ctx context.Context,
	roots paths.Roots,
	lookup func(string) (string, bool),
) (settings.AuthoringPreview, error) {
	service, err := openConfigureGlobalSettingsService(
		ctx, settings.Options{Roots: roots, LookupEnv: lookup},
	)
	if err != nil {
		return settings.AuthoringPreview{}, fmt.Errorf(
			"acd config edit: read global defaults: %w", err,
		)
	}
	preview, previewErr := service.AuthoringPreview()
	closeErr := service.Close()
	if previewErr != nil {
		return settings.AuthoringPreview{}, fmt.Errorf(
			"acd config edit: resolve global defaults: %w", previewErr,
		)
	}
	if closeErr != nil {
		return settings.AuthoringPreview{}, fmt.Errorf(
			"acd config edit: close global preview: %w", closeErr,
		)
	}
	return preview, nil
}

func fallbackConfigureValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func cloneConfigureValues(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for name, value := range values {
		out[name] = value
	}
	return out
}

func builtInConfigureValues(strategy, preset string) (map[string]string, error) {
	selection := dryRunConfigureSelection(
		strategy, preset, configureVerificationDetection{},
	)
	overrides := config.Overrides{}
	for name, value := range selectionDraft(selection) {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf(
				"acd config edit: encode built-in field %q: %w", name, err,
			)
		}
		overrides[name] = raw
	}
	resolved, _, err := config.ResolveAll(
		config.ResolveInput{Experiment: overrides}, overrides,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"acd config edit: resolve built-in defaults: %w", err,
		)
	}
	out := make(map[string]string)
	for _, field := range config.Catalog() {
		if field.Boundary != config.ApplyHot ||
			!field.Persistable || field.Sensitive {
			continue
		}
		out[field.Name] = resolved[field.Name].EffectiveValue()
	}
	return out, nil
}

func configureSaveValues(selection settingsui.ConfigureSelection) map[string]*string {
	values := selectionDraft(selection)
	out := make(map[string]*string, len(values))
	for key, value := range values {
		copyValue := value
		out[key] = &copyValue
	}
	return out
}

func clearRepositoryValues() map[string]*string {
	out := make(map[string]*string)
	for _, field := range config.Catalog() {
		if field.Persistable && field.Name != config.FieldAPIKey {
			out[field.Name] = nil
		}
	}
	return out
}

func selectionProviderConfirmations(selection settingsui.ConfigureSelection) []ai.ConfirmationRequirement {
	var out []ai.ConfirmationRequirement
	if selection.EndpointCredentialsApproved {
		out = append(out, ai.ConfirmationEndpointCredentials)
	}
	if selection.Provider == "openai-compat" &&
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(selection.BaseURL)), "http://") {
		out = append(out, ai.ConfirmationInsecureEndpointCredentials)
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
		return configureReport{}, errors.New("acd config edit: selected preset is unavailable")
	}
	verificationStatus := "not_required"
	switch selection.VerificationMode {
	case "structural":
		verificationStatus = "internal_only"
	case "fast", "full":
		if dryRun {
			verificationStatus = "approval_required"
		} else {
			verificationStatus = "approved_pending_background_validation"
		}
	}
	diff := "not_required"
	if definition.DiffContextRequired {
		diff = "approval_required"
		if selection.DiffContextApproved {
			diff = "approved_redacted"
		}
	}
	scope := "repository"
	if repo == "" {
		scope = "global"
	}
	report := configureReport{
		Version: configureReportVersion, DryRun: dryRun,
		Scope: scope, Repo: repo,
		Experience: fallbackConfigureExperience(selection),
		Strategy:   selection.Strategy, Preset: selection.Preset,
		PresetID: definition.ID(), PresetVersion: definition.Version,
		CommitFormat: selection.CommitFormat, Provider: selection.Provider,
		Model: selection.Model, Endpoint: safeEndpointPreview(selection.BaseURL),
		ProviderTimeout:  selection.ProviderTimeout,
		CredentialSource: source, DiffContext: diff,
		Verification: configureVerificationReport{
			Mode: selection.VerificationMode, Command: selection.VerificationCommand,
			CommandSource: selection.VerificationSource,
			Approved:      selection.VerificationApproved, Status: verificationStatus,
		},
		RepairEnabled:    definition.Values[config.FieldIntentRepairEnabled] == "true",
		RepairHorizon:    definition.Values[config.FieldIntentRepairHorizon],
		RepairMaxCommits: definition.Values[config.FieldIntentRepairMaxCommits],
		ExecutionMode: configureExecutionMode(
			selection.Strategy, selection.Preset,
		),
		ConfigurationAction: "save_repository_override",
		Readiness: configureInitialReadiness(
			selection.Strategy, selection.Preset,
		),
		Daemon: "unchanged", HarnessGuidance: configureHarnessGuidance(),
		Risks: configureRisks(selection, definition),
		Operations: []string{
			"provider_test:planned", "credential:persist_after_provider_test",
			"settings:save", "runtime_revision:create_one",
			"validation:queue_if_required", "daemon:enable",
			"harness:report_only",
		},
	}
	if selection.VerificationMode == "full" {
		report.Operations[4] = "validation:queue"
	} else {
		report.Operations[4] = "validation:not_required"
	}
	if selection.VerificationMode == "fast" {
		report.Verification.Timeout = definition.Values[config.FieldVerificationFastTimeout]
		report.Verification.ExpectedDuration = "usually under 2 minutes"
	} else if selection.VerificationMode == "full" {
		report.Verification.Timeout = definition.Values[config.FieldVerificationFullTimeout]
		report.Verification.ExpectedDuration = "potentially several minutes"
	} else if selection.VerificationMode == "structural" {
		report.Verification.ExpectedDuration = "usually a few seconds"
	}
	return report, nil
}

func configureGlobalReport(report *configureReport) {
	if report == nil {
		return
	}
	report.Scope = "global"
	report.Repo = ""
	report.ConfigurationAction = "update_global"
	report.ExecutionMode = "global defaults only"
	report.Readiness = "saved after provider test"
	report.Daemon = "not_started"
	report.RuntimeRevision = 0
	report.Operations = []string{
		"provider_test:planned",
		"credential:persist_after_provider_test",
		"global_settings:save_with_approval",
		"repository_state:not_opened",
		"daemon:not_started",
		"harness:report_only",
	}
}

func configureInheritedReport(report *configureReport) {
	if report == nil {
		return
	}
	report.ConfigurationAction = "inherit_global"
	report.ExecutionMode = "remove repository override and activate inheritance"
	report.Operations = []string{
		"provider_test:planned",
		"credential:unchanged",
		"repository_override:remove",
		"runtime_revision:create_one",
		"validation:not_required",
		"daemon:enable",
		"harness:report_only",
	}
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
	effective.CAFile = values[config.FieldCAFile]
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
	if report.Scope == "global" {
		fmt.Fprintln(out, "Scope: Global defaults")
	} else {
		fmt.Fprintf(out, "Repository: %s\n", safeRepoPreview(report.Repo))
	}
	fmt.Fprintln(out, "Configuration action:",
		displayConfigureAction(report.ConfigurationAction))
	fmt.Fprintf(out, "Experience: %s\n", report.Experience)
	fmt.Fprintf(out, "Mode: %s %s%s [%s@%d]\n", displayConfigureWord(report.Strategy),
		displayConfigureWord(report.Preset), customized, report.PresetID, report.PresetVersion)
	fmt.Fprintf(out, "Provider: %s; credential source: %s\n",
		safePreviewText(report.Provider, 128), report.CredentialSource)
	if report.Provider == "openai-compat" {
		fmt.Fprintf(out, "Model: %s\n", safePreviewText(report.Model, 128))
		fmt.Fprintf(out, "Endpoint: %s\n", report.Endpoint)
		fmt.Fprintf(out, "Provider timeout: %s\n",
			safePreviewText(report.ProviderTimeout, 32))
	}
	fmt.Fprintf(out, "Diff context: %s\n", report.DiffContext)
	fmt.Fprintf(out, "Verification: %s", report.Verification.Mode)
	if report.Verification.Command != "" {
		fmt.Fprintf(out, ". Exact command: %s", safeCommandPreview(report.Verification.Command))
	}
	fmt.Fprintln(out)
	if report.Verification.CommandSource != "" {
		fmt.Fprintln(out, "Verification source:",
			safePreviewText(report.Verification.CommandSource, 128))
	}
	if report.Verification.ExpectedDuration != "" {
		fmt.Fprintln(out, "Expected duration:",
			report.Verification.ExpectedDuration)
	}
	for _, risk := range report.Risks {
		fmt.Fprintln(out, "Approval:", risk)
	}
	fmt.Fprintf(out, "Automatic repair: %t; horizon %s; maximum commits %s\n",
		report.RepairEnabled, report.RepairHorizon, report.RepairMaxCommits)
	fmt.Fprintln(out, "Execution:", report.ExecutionMode)
	fmt.Fprintln(out, "Readiness after save:", report.Readiness)
	if report.Scope == "global" {
		fmt.Fprintln(out,
			"Apply order: provider test > credential > global defaults")
	} else if report.Verification.Mode == "full" {
		fmt.Fprintln(out,
			"Apply order: provider test > credential > settings > one runtime revision + validation job > acd on")
	} else {
		fmt.Fprintln(out,
			"Apply order: provider test > credential > settings > one runtime revision > acd on")
	}
	if report.Verification.Mode == "full" {
		fmt.Fprintln(out, "Validation runs after setup returns; capture stays active while commit publishing waits.")
	}
	fmt.Fprintln(out, "Harness hooks: report only; no external hook file will be edited")
	return nil
}

func displayConfigureAction(action string) string {
	switch action {
	case "replace_global":
		return "replace saved global overrides"
	case "inherit_global":
		return "remove repository override and inherit global defaults"
	case "save_repository_override":
		return "save repository-specific override"
	default:
		return "update global defaults"
	}
}

type configureVerificationDetection struct {
	QuickCommand string
	QuickSource  string
	FullCommand  string
	FullSource   string
}

func detectVerificationCommands(repo string) configureVerificationDetection {
	if detected, ok := readVerificationManifest(repo); ok {
		return detected
	}
	var detected configureVerificationDetection
	makeTargets := readMakeTargets(filepath.Join(repo, "Makefile"))
	switch {
	case makeTargets["verify-fast"]:
		detected.QuickCommand = "make verify-fast"
		detected.QuickSource = "Makefile target verify-fast"
	case makeTargets["test-fast"]:
		detected.QuickCommand = "make test-fast"
		detected.QuickSource = "Makefile target test-fast"
	}
	if makeTargets["test"] {
		detected.FullCommand = "make test"
		detected.FullSource = "Makefile target test"
	}
	if _, err := os.Stat(filepath.Join(repo, "go.mod")); err == nil {
		if detected.QuickCommand == "" {
			detected.QuickCommand = "go test ./... -run '^$'"
			detected.QuickSource = "Go language default"
		}
		if detected.FullCommand == "" {
			detected.FullCommand = "go test ./..."
			detected.FullSource = "Go language default"
		}
	}
	if node := detectNodeVerification(repo); node.QuickCommand != "" ||
		node.FullCommand != "" {
		if detected.QuickCommand == "" {
			detected.QuickCommand, detected.QuickSource =
				node.QuickCommand, node.QuickSource
		}
		if detected.FullCommand == "" {
			detected.FullCommand, detected.FullSource =
				node.FullCommand, node.FullSource
		}
	}
	if _, err := os.Stat(filepath.Join(repo, "Cargo.toml")); err == nil {
		if detected.QuickCommand == "" {
			detected.QuickCommand = "cargo check --all-targets"
			detected.QuickSource = "Rust language default"
		}
		if detected.FullCommand == "" {
			detected.FullCommand = "cargo test"
			detected.FullSource = "Rust language default"
		}
	}
	if hasPythonTestConfiguration(repo) && detected.FullCommand == "" {
		detected.FullCommand = "python -m pytest"
		detected.FullSource = "Python test configuration"
	}
	return detected
}

func detectVerificationCommand(repo string) string {
	detected := detectVerificationCommands(repo)
	if detected.FullCommand != "" {
		return detected.FullCommand
	}
	return detected.QuickCommand
}

func readVerificationManifest(
	repo string,
) (configureVerificationDetection, bool) {
	for _, name := range []string{
		filepath.Join(".acd", "verification.json"),
		"acd.verification.json",
	} {
		body, err := readBoundedConfigureFile(filepath.Join(repo, name))
		if err != nil {
			continue
		}
		var manifest struct {
			Version int    `json:"version"`
			Quick   string `json:"quick"`
			Full    string `json:"full"`
		}
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&manifest); err != nil ||
			manifest.Version != 1 {
			continue
		}
		detected := configureVerificationDetection{
			QuickCommand: strings.TrimSpace(manifest.Quick),
			FullCommand:  strings.TrimSpace(manifest.Full),
		}
		if detected.QuickCommand != "" {
			detected.QuickSource = "repository ACD verification manifest"
		}
		if detected.FullCommand != "" {
			detected.FullSource = "repository ACD verification manifest"
		}
		return detected, true
	}
	return configureVerificationDetection{}, false
}

func readMakeTargets(path string) map[string]bool {
	body, err := readBoundedConfigureFile(path)
	if err != nil {
		return nil
	}
	targets := map[string]bool{}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || line[0] == '\t' || line[0] == ' ' {
			continue
		}
		name, _, ok := strings.Cut(line, ":")
		if !ok || strings.ContainsAny(name, " \t=$") {
			continue
		}
		targets[strings.TrimSpace(name)] = true
	}
	return targets
}

func detectNodeVerification(repo string) configureVerificationDetection {
	body, err := readBoundedConfigureFile(filepath.Join(repo, "package.json"))
	if err != nil {
		return configureVerificationDetection{}
	}
	var manifest struct {
		Scripts map[string]string `json:"scripts"`
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		return configureVerificationDetection{}
	}
	runner := "npm run"
	for _, candidate := range []struct {
		path   string
		runner string
	}{
		{"pnpm-lock.yaml", "pnpm"},
		{"yarn.lock", "yarn"},
		{"bun.lock", "bun run"},
		{"bun.lockb", "bun run"},
	} {
		if _, err := os.Stat(filepath.Join(repo, candidate.path)); err == nil {
			runner = candidate.runner
			break
		}
	}
	command := func(script string) string {
		if runner == "npm run" {
			return runner + " " + script
		}
		return runner + " " + script
	}
	var detected configureVerificationDetection
	switch {
	case strings.TrimSpace(manifest.Scripts["test:fast"]) != "":
		detected.QuickCommand = command("test:fast")
	case strings.TrimSpace(manifest.Scripts["check"]) != "":
		detected.QuickCommand = command("check")
	}
	if detected.QuickCommand != "" {
		detected.QuickSource = "package.json script"
	}
	if strings.TrimSpace(manifest.Scripts["test"]) != "" {
		detected.FullCommand = command("test")
		detected.FullSource = "package.json script"
	}
	return detected
}

func hasPythonTestConfiguration(repo string) bool {
	for _, name := range []string{
		"pytest.ini", "pyproject.toml", "setup.cfg", "tox.ini",
	} {
		if _, err := os.Stat(filepath.Join(repo, name)); err == nil {
			return true
		}
	}
	return false
}

func readBoundedConfigureFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, 1024*1024+1))
	if err != nil || len(body) > 1024*1024 {
		return nil, errors.New("configure detection file is too large")
	}
	return body, nil
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
	if selection.VerificationMode == "fast" ||
		selection.VerificationMode == "full" {
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
	out       io.Writer
	color     bool
}

func newConfigureProgress(out io.Writer) configureProgress {
	if out == nil {
		out = io.Discard
	}
	return configureProgress{
		out:   out,
		color: os.Getenv("NO_COLOR") == "" && rewriteProgressIsTerminal(out),
	}
}

func (p configureProgress) start() {
	p.writeLine("", "Applying reviewed configuration...")
}

func (p configureProgress) begin(step int, message string) {
	p.writeLine("\x1b[36m", fmt.Sprintf("[%d/6] %s", step, message))
}

func (p configureProgress) success(step int, message string) {
	p.writeLine("\x1b[32m", fmt.Sprintf("[%d/6] %s", step, message))
}

func (p configureProgress) writeLine(color, message string) {
	if p.color && color != "" {
		fmt.Fprintf(p.out, "%s%s\x1b[0m\n", color, message)
		return
	}
	fmt.Fprintln(p.out, message)
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
	message := fmt.Sprintf("acd config edit: %s failed: %s; completed stages: %s",
		safePreviewText(stage, 64), safePreviewText(cause.Error(), 512), completed)
	if rollback != "" {
		message += "; rollback: " + rollback
	}
	if remediation != "" {
		message += "; remediation: " + safePreviewText(remediation, 512)
	}
	return errors.New(message)
}

func safeConfigureVerificationOutput(value string, limit int) string {
	value = observabilityANSI.ReplaceAllString(value, "")
	value = strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\t':
			return r
		case '\r':
			return '\n'
		}
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, value)
	value = strings.TrimSpace(value)
	if limit > 0 && len(value) > limit {
		value = value[len(value)-limit:]
		for len(value) > 0 && value[0]&0xc0 == 0x80 {
			value = value[1:]
		}
		value = "[earlier output omitted]\n" + value
	}
	return value
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
		return nil, reader, fmt.Errorf("acd config edit: read credential: %w", err)
	}
	if len(line) > maxCredentialInput {
		return nil, reader, errors.New("acd config edit: credential input is too large")
	}
	value := []byte(strings.TrimSpace(line))
	if len(value) == 0 {
		return nil, reader, errors.New("acd config edit: credential input is empty")
	}
	return value, reader, nil
}

func configureSelectionVerificationMode(strategy, preset string) string {
	if strategy != "intent" {
		return "none"
	}
	switch preset {
	case "balanced":
		return "structural"
	case "quality":
		return "full"
	default:
		return "none"
	}
}

func configureSourceIsExplicit(source config.Source) bool {
	switch source {
	case config.SourceExperiment,
		config.SourceRepository,
		config.SourceProfile,
		config.SourceGlobal,
		config.SourceEnvironment:
		return true
	default:
		return false
	}
}

type configureValidationTarget struct {
	BranchRef        string
	BranchGeneration int64
	ExpectedHead     string
}

func resolveConfigureValidationTarget(
	ctx context.Context,
	repo string,
) (configureValidationTarget, error) {
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		return configureValidationTarget{}, err
	}
	branch, err := gitpkg.RunBranchRef(ctx, worktree.Root)
	if err != nil || branch == "" {
		return configureValidationTarget{},
			errors.New("configuration validation requires an attached branch")
	}
	head, err := gitpkg.RevParse(ctx, worktree.Root, "HEAD^{commit}")
	if err != nil {
		return configureValidationTarget{}, err
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(worktree.GitDir))
	if err != nil {
		return configureValidationTarget{}, err
	}
	defer db.Close()
	generation, err := daemon.LoadBranchGeneration(ctx, db)
	if err != nil {
		return configureValidationTarget{}, err
	}
	return configureValidationTarget{
		BranchRef: branch, BranchGeneration: generation, ExpectedHead: head,
	}, nil
}

func waitForConfigureValidation(
	ctx context.Context,
	out io.Writer,
	repo string,
	runID int64,
) error {
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		return err
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(worktree.GitDir))
	if err != nil {
		return err
	}
	defer db.Close()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	lastStatus := ""
	for {
		run, err := state.ConfigValidationByID(ctx, db, runID)
		if err != nil {
			return fmt.Errorf("acd config edit: read validation job: %w", err)
		}
		if run.Status != lastStatus {
			fmt.Fprintf(out, "Validation: %s (attempt %d)\n",
				run.Status, run.Attempt)
			lastStatus = run.Status
		}
		switch run.Status {
		case state.ConfigValidationPassed:
			fmt.Fprintln(out,
				"Configuration ready. Commit publishing is enabled.")
			return nil
		case state.ConfigValidationFailed,
			state.ConfigValidationTimedOut,
			state.ConfigValidationCancelled:
			fmt.Fprintln(out, "Configuration needs attention.")
			fmt.Fprintln(out,
				"Capture remains active; no commits were published.")
			if run.ExitCode.Valid {
				fmt.Fprintf(out, "Validation failed with exit code %d.\n",
					run.ExitCode.Int64)
			}
			if tail := safeConfigureVerificationOutput(
				run.SanitizedOutput, 8*1024,
			); tail != "" {
				fmt.Fprintln(out, "Sanitized validation output:")
				fmt.Fprintln(out, tail)
			}
			return errors.New(
				"acd config edit: validation needs attention; run `acd config edit` to retry or select another experience",
			)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func existingConfigureValidation(
	ctx context.Context,
	repo string,
) (state.ConfigValidationRun, bool, error) {
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		return state.ConfigValidationRun{}, false, err
	}
	dbPath := state.DBPathFromGitDir(worktree.GitDir)
	if _, err := os.Lstat(dbPath); errors.Is(err, os.ErrNotExist) {
		return state.ConfigValidationRun{}, false, nil
	} else if err != nil {
		return state.ConfigValidationRun{}, false, err
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return state.ConfigValidationRun{}, false, err
	}
	defer db.Close()
	return state.DesiredConfigValidation(ctx, db)
}

func retryConfigureValidation(
	ctx context.Context,
	repo string,
	requestID int64,
) (state.ConfigValidationRun, bool, error) {
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		return state.ConfigValidationRun{}, false, err
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(worktree.GitDir))
	if err != nil {
		return state.ConfigValidationRun{}, false, err
	}
	defer db.Close()
	return state.RetryConfigValidation(ctx, db, requestID)
}

func fallbackConfigureExperience(
	selection settingsui.ConfigureSelection,
) string {
	if selection.Experience != "" {
		return displayConfigureExperience(selection.Experience)
	}
	return configureExperienceName(selection.Strategy, selection.Preset)
}

func configureExperienceName(strategy, preset string) string {
	switch {
	case strategy == "event" && preset == "fast":
		return "Maximum Speed"
	case strategy == "intent" && preset == "quality":
		return "Strict Review"
	default:
		return "Everyday"
	}
}

func displayConfigureExperience(value string) string {
	switch value {
	case "speed", "Maximum Speed":
		return "Maximum Speed"
	case "strict", "Strict Review":
		return "Strict Review"
	default:
		return "Everyday"
	}
}

func configureExecutionMode(strategy, preset string) string {
	if strategy == "intent" && preset == "quality" {
		return "background activation gate"
	}
	return "immediate activation"
}

func configureInitialReadiness(strategy, preset string) string {
	if strategy == "intent" && preset == "quality" {
		return "validating"
	}
	return "ready"
}

func configureValidationLabel(mode string) string {
	switch mode {
	case "full":
		return "full"
	case "structural":
		return "structural"
	default:
		return "quick"
	}
}

func displayConfigureWord(value string) string {
	if value == "" {
		return ""
	}
	return strings.ToUpper(value[:1]) + value[1:]
}

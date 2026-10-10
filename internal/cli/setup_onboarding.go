package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
)

type setupOnboardingOptions struct {
	Experience                 string
	Strategy                   string
	CommitFormat               string
	Provider                   string
	BaseURL                    string
	Model                      string
	CAFile                     string
	CredentialStdin            bool
	Accessible                 bool
	ConfirmEndpointCredentials bool
	ConfirmInsecureHTTP        bool
	ConfirmDiffEgress          bool
	ConfirmIntentRepair        bool
}

type setupOnboardingState struct {
	Configuration installer.SetupConfiguration
	Selection     settingsui.ConfigureSelection
	Credential    string
	Draft         map[string]string
	Confirmations []ai.ConfirmationRequirement
	Roots         paths.Roots
	Interactive   bool
}

func prepareSetupOnboarding(cmd *cobra.Command, roots paths.Roots, opts setupOnboardingOptions, dryRun, nonInteractive bool) (*setupOnboardingState, error) {
	if dryRun && opts.CredentialStdin {
		return nil, invalidCommandError("acd setup: --credential-stdin cannot be used with --dry-run")
	}
	if nonInteractive && !dryRun && strings.TrimSpace(cmd.Flag("expect-plan").Value.String()) == "" {
		return nil, invalidCommandError("acd setup: non-interactive first setup requires --expect-plan from a dry-run preview")
	}
	strategy := strings.ToLower(strings.TrimSpace(opts.Strategy))
	if strategy == "" {
		switch strings.ToLower(strings.TrimSpace(opts.Experience)) {
		case "everyday":
			strategy = "intent"
		case "speed", "":
			strategy = "local"
		default:
			return nil, invalidCommandError("acd setup: legacy --experience must be everyday or speed")
		}
	}
	if strategy != "local" && strategy != "intent" {
		return nil, invalidCommandError("acd setup: --strategy must be local or intent")
	}
	if strategy == "local" && opts.CredentialStdin {
		return nil, invalidCommandError("acd setup: --credential-stdin requires --strategy intent")
	}
	format := fallbackConfigureValue(strings.ToLower(strings.TrimSpace(opts.CommitFormat)), "imperative")
	if format != "imperative" && format != "conventional" {
		return nil, invalidCommandError("acd setup: --commit-format must be imperative or conventional")
	}
	providerDefault := "deterministic"
	if strategy == "intent" {
		providerDefault = "openai-compat"
	}
	provider := fallbackConfigureValue(strings.ToLower(strings.TrimSpace(opts.Provider)), providerDefault)
	if provider != "deterministic" && provider != "openai-compat" {
		return nil, invalidCommandError("acd setup: --provider must be deterministic or openai-compat")
	}

	values := map[string]string{}
	for _, field := range config.Catalog() {
		if field.Boundary == config.ApplyHot && field.Persistable && !field.Sensitive {
			values[field.Name] = field.Default
		}
	}
	values[config.FieldCommitStrategy], values[config.FieldCommitFormat], values[config.FieldProvider] = strategy, format, provider
	values[config.FieldModel] = fallbackConfigureValue(strings.TrimSpace(opts.Model), config.DefaultOpenAIModel)
	values[config.FieldBaseURL] = fallbackConfigureValue(strings.TrimSpace(opts.BaseURL), ai.DefaultOpenAIBaseURL)
	values[config.FieldCAFile] = strings.TrimSpace(opts.CAFile)
	credentialSource, hasCredential := credentials.SourceNone, false
	credentialStatus := func() (bool, error) {
		var err error
		credentialSource, hasCredential, err = configureCredentialStatus(roots, os.LookupEnv)
		return hasCredential, err
	}
	if strategy == "intent" {
		if _, err := credentialStatus(); err != nil {
			return nil, fmt.Errorf("acd setup: inspect protected credential: %w", err)
		}
	}
	credential := ""
	if opts.CredentialStdin {
		body, _, readErr := readConfigureCredentialLine(cmd.InOrStdin())
		if readErr != nil {
			return nil, errors.New(strings.ReplaceAll(readErr.Error(), "acd config edit", "acd setup"))
		}
		credential = string(body)
		hasCredential = true
		if credentialSource != credentials.SourceEnvironment {
			credentialSource = credentials.SourceFile
		}
	}

	selection := configureSelectionFromValues(values)
	var err error
	selection.Experience = strategy
	selection.RepairApproved = false
	interactive := !dryRun && !nonInteractive
	if interactive {
		accessible := opts.Accessible ||
			!settingsInputTTY(cmd.InOrStdin()) ||
			!settingsOutputTTY(cmd.OutOrStdout()) ||
			strings.EqualFold(os.Getenv("TERM"), "dumb") ||
			configureTerminalTooShort(cmd.OutOrStdout())
		selection, err = runConfigureWizard(cmd.Context(), settingsui.ConfigureWizardOptions{
			Input: cmd.InOrStdin(), Output: cmd.OutOrStdout(), Accessible: accessible,
			Defaults: values, ExplicitMode: strings.TrimSpace(opts.Strategy) != "" || strings.TrimSpace(opts.Experience) != "",
			CredentialStatus: credentialStatus,
			RepositoryScoped: false, HasCredential: hasCredential,
			CredentialFromStdin: opts.CredentialStdin,
			ProviderConfigured:  strings.TrimSpace(opts.Provider) != "",
			OpenAIConfigured:    strings.TrimSpace(opts.BaseURL) != "" && strings.TrimSpace(opts.Model) != "",
			IncludeCommitFormat: true, IncludeCAFile: true,
		})
		if err != nil {
			return nil, fmt.Errorf("acd setup: preferences: %w", err)
		}
		if credential != "" {
			selection.Credential = credential
		}
		selection.RepairApproved = false
		values = selectionDraft(selection)
	} else {
		selection.Credential = credential
	}

	if selection.Strategy == "intent" && selection.Provider == "openai-compat" {
		normalized, normalizeErr := ai.NormalizeOpenAIBaseURL(selection.BaseURL)
		if normalizeErr != nil {
			return nil, fmt.Errorf("acd setup: endpoint: %w", normalizeErr)
		}
		selection.BaseURL = normalized
		values[config.FieldBaseURL] = normalized
		selection.EndpointCredentialsApproved = strings.TrimRight(normalized, "/") != strings.TrimRight(ai.DefaultOpenAIBaseURL, "/")
		if !hasCredential && strings.TrimSpace(selection.Credential) == "" && !dryRun {
			return nil, errors.New("acd setup: OpenAI-compatible setup needs a bearer token from ACD_AI_API_KEY or --credential-stdin")
		}
	} else {
		selection.Credential = ""
		credential = ""
		credentialSource = credentials.SourceNone
		values[config.FieldDiffEgress] = "false"
	}
	selection.DiffContextApproved = values[config.FieldDiffEgress] == "true"
	selection.RepairApproved = false
	confirmations := selectionConfirmations(selection)
	if nonInteractive && !dryRun {
		if err := requireSetupConfirmations(opts, confirmations); err != nil {
			return nil, err
		}
	}

	lookupSecret := selection.Credential
	if dryRun && selection.Provider == "openai-compat" && !hasCredential {
		lookupSecret = "dry-run-placeholder"
	}
	service, err := openConfigureGlobalSettingsService(cmd.Context(),
		configureSettingsOptions(roots, "", configureLookupEnv(lookupSecret)))
	if err != nil {
		return nil, fmt.Errorf("acd setup: prepare preferences: %w", err)
	}
	defer service.Close()
	validation, err := service.Validate(cmd.Context(), values, confirmations)
	if err != nil {
		return nil, fmt.Errorf("acd setup: validate preferences: %w", err)
	}
	if len(validation.Missing) > 0 {
		return nil, &settings.ConfirmationRequiredError{Missing: validation.Missing}
	}
	if selection.Strategy == "intent" && selection.Provider == "openai-compat" && credentialSource == credentials.SourceNone {
		credentialSource = credentials.SourceFile
	}
	storeCredential := selection.Strategy == "intent" && selection.Provider == "openai-compat" && selection.Credential != "" && credentialSource != credentials.SourceEnvironment
	return &setupOnboardingState{
		Configuration: installer.SetupConfiguration{
			Values: values, Fingerprint: validation.Fingerprint,
			Confirmations: confirmations, SourceGeneration: validation.SourceGeneration,
			CredentialSource: credentialSource, StoreCredential: storeCredential,
			ProviderTestStatus: "optional_connection_test",
		},
		Selection: selection, Credential: selection.Credential, Draft: values,
		Confirmations: confirmations, Roots: roots, Interactive: interactive,
	}, nil
}

func requireSetupConfirmations(opts setupOnboardingOptions, required []ai.ConfirmationRequirement) error {
	for _, requirement := range required {
		var confirmed bool
		var flag string
		switch requirement {
		case ai.ConfirmationEndpointCredentials:
			confirmed, flag = opts.ConfirmEndpointCredentials, "--confirm-endpoint-credentials"
		case ai.ConfirmationInsecureEndpointCredentials:
			confirmed, flag = opts.ConfirmInsecureHTTP, "--confirm-insecure-http"
		case ai.ConfirmationDiffEgress:
			confirmed, flag = opts.ConfirmDiffEgress, "--confirm-diff-egress"
		case ai.ConfirmationIntentRepair:
			confirmed, flag = opts.ConfirmIntentRepair, "--confirm-intent-repair"
		default:
			continue
		}
		if !confirmed {
			return invalidCommandError("acd setup: reviewed plan requires %s", flag)
		}
	}
	return nil
}

func testSetupProvider(cmd *cobra.Command, state *setupOnboardingState) error {
	if state == nil || state.Selection.Strategy == "local" {
		return nil
	}
	service, err := openConfigureGlobalSettingsService(cmd.Context(),
		configureSettingsOptions(state.Roots, "", configureLookupEnv(state.Credential)))
	if err != nil {
		return fmt.Errorf("acd setup: open provider test: %w", err)
	}
	defer service.Close()
	for {
		fmt.Fprintln(cmd.ErrOrStderr(), "Testing the provider with fixed synthetic content. No repository data is sent.")
		result, testErr := service.TestProvider(cmd.Context(), state.Draft, state.Confirmations)
		if testErr == nil && result.Success && result.Fingerprint == state.Configuration.Fingerprint {
			return nil
		}
		if !state.Interactive {
			if testErr == nil {
				testErr = errors.New("provider rejected the synthetic request")
			}
			return fmt.Errorf("acd setup: provider test failed; nothing was written: %w", testErr)
		}
		fmt.Fprintln(cmd.ErrOrStderr(), "The connection test failed. Nothing has been changed.")
		fmt.Fprint(cmd.ErrOrStderr(), "Retry, edit the connection, use the local provider, or exit? [r/e/l/q] ")
		var answer string
		if _, scanErr := fmt.Fscanln(cmd.InOrStdin(), &answer); scanErr != nil {
			return errors.New("acd setup: connection test stopped; nothing was written")
		}
		switch strings.ToLower(strings.TrimSpace(answer)) {
		case "r", "retry":
			continue
		case "e", "edit":
			return errSetupEditConnection
		case "l", "local":
			return errSetupUseLocalProvider
		default:
			return errors.New("acd setup: setup stopped; nothing was written")
		}
	}
}

func useLocalSetupProvider(cmd *cobra.Command, state *setupOnboardingState) (*setupOnboardingState, error) {
	values := cloneConfigureValues(state.Draft)
	values[config.FieldCommitStrategy] = "local"
	values[config.FieldProvider] = "deterministic"
	values[config.FieldDiffEgress] = "false"
	selection := configureSelectionFromValues(values)
	selection.Experience = state.Selection.Experience
	selection.Provider = "deterministic"
	selection.DiffContextApproved = false
	selection.RepairApproved = values[config.FieldIntentRepairEnabled] == "true"
	confirmations := selectionConfirmations(selection)
	service, err := openConfigureGlobalSettingsService(cmd.Context(), settings.Options{Roots: state.Roots})
	if err != nil {
		return nil, fmt.Errorf("acd setup: prepare local provider: %w", err)
	}
	defer service.Close()
	validation, err := service.Validate(cmd.Context(), values, confirmations)
	if err != nil {
		return nil, fmt.Errorf("acd setup: validate local provider: %w", err)
	}
	return &setupOnboardingState{
		Configuration: installer.SetupConfiguration{
			Values: values, Fingerprint: validation.Fingerprint,
			Confirmations: confirmations, SourceGeneration: validation.SourceGeneration,
			CredentialSource:   credentials.SourceNone,
			ProviderTestStatus: "optional_connection_test",
		},
		Selection: selection, Draft: values, Confirmations: confirmations,
		Roots: state.Roots, Interactive: true,
	}, nil
}

var (
	errSetupEditConnection   = errors.New("setup: edit connection")
	errSetupUseLocalProvider = errors.New("setup: use local provider")
)

func setupExperience(values map[string]string) string {
	if values[config.FieldCommitStrategy] == "intent" {
		return "Intent"
	}
	return "Local"
}

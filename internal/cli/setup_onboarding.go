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
		strategy = "local"
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
	interactive := !dryRun && !nonInteractive
	if interactive {
		accessible := opts.Accessible ||
			!settingsInputTTY(cmd.InOrStdin()) ||
			!settingsOutputTTY(cmd.OutOrStdout()) ||
			strings.EqualFold(os.Getenv("TERM"), "dumb") ||
			configureTerminalTooShort(cmd.OutOrStdout())
		selection, err = runConfigureWizard(cmd.Context(), settingsui.ConfigureWizardOptions{
			Input: cmd.InOrStdin(), Output: cmd.OutOrStdout(), Accessible: accessible,
			Defaults: values, ExplicitMode: strings.TrimSpace(opts.Strategy) != "",
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
	if selection.Strategy == "intent" && selection.Provider == "openai-compat" && credentialSource == credentials.SourceNone && selection.Credential != "" {
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
		default:
			continue
		}
		if !confirmed {
			return invalidCommandError("acd setup: reviewed plan requires %s", flag)
		}
	}
	return nil
}

func setupModeLabel(values map[string]string) string {
	if values[config.FieldCommitStrategy] == "intent" {
		return "Intent"
	}
	return "Local"
}

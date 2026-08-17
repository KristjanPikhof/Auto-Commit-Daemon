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

	experience := fallbackConfigureValue(strings.ToLower(strings.TrimSpace(opts.Experience)), "everyday")
	strategy, preset := "intent", "balanced"
	if experience == "speed" {
		strategy, preset = "event", "fast"
	} else if experience != "everyday" {
		return nil, invalidCommandError("acd setup: --experience must be everyday or speed")
	}
	format := fallbackConfigureValue(strings.ToLower(strings.TrimSpace(opts.CommitFormat)), "imperative")
	if format != "imperative" && format != "conventional" {
		return nil, invalidCommandError("acd setup: --commit-format must be imperative or conventional")
	}
	provider := fallbackConfigureValue(strings.ToLower(strings.TrimSpace(opts.Provider)), "deterministic")
	if provider != "deterministic" && provider != "openai-compat" {
		return nil, invalidCommandError("acd setup: --provider must be deterministic or openai-compat")
	}

	values, err := builtInConfigureValues(strategy, preset)
	if err != nil {
		return nil, err
	}
	values[config.FieldCommitStrategy] = strategy
	values[config.FieldCommitPreset] = preset
	values[config.FieldCommitFormat] = format
	values[config.FieldProvider] = provider
	values[config.FieldModel] = fallbackConfigureValue(strings.TrimSpace(opts.Model), "gpt-5.4-mini")
	values[config.FieldBaseURL] = fallbackConfigureValue(strings.TrimSpace(opts.BaseURL), ai.DefaultOpenAIBaseURL)
	values[config.FieldCAFile] = strings.TrimSpace(opts.CAFile)
	values[config.FieldTimeout] = ai.DefaultProviderTimeout.String()
	values[config.FieldIntentVerification] = configureSelectionVerificationMode(strategy, preset)
	values[config.FieldIntentRepairEnabled] = fmt.Sprintf("%t", experience == "everyday")
	values[config.FieldDiffEgress] = fmt.Sprintf("%t", provider == "openai-compat" && experience == "everyday")

	credentialSource, hasCredential, err := configureCredentialStatus(roots, os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("acd setup: inspect protected credential: %w", err)
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
	selection.Experience = experience
	selection.RepairApproved = experience == "everyday"
	interactive := !dryRun && !nonInteractive
	if interactive {
		accessible := opts.Accessible || strings.EqualFold(os.Getenv("TERM"), "dumb") || configureTerminalTooShort(cmd.OutOrStdout())
		selection, err = runConfigureWizard(cmd.Context(), settingsui.ConfigureWizardOptions{
			Input: cmd.InOrStdin(), Output: cmd.OutOrStdout(), Accessible: accessible,
			Defaults: values, ExplicitMode: strings.TrimSpace(opts.Experience) != "",
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
		selection.RepairApproved = selection.Strategy == "intent" && selection.Preset == "balanced"
		values = selectionDraft(selection)
		experience = fallbackConfigureExperience(selection)
	} else {
		selection.Credential = credential
	}

	if selection.Provider == "openai-compat" {
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
	selection.RepairApproved = selection.Strategy == "intent" && selection.Preset == "balanced"
	values[config.FieldIntentRepairEnabled] = fmt.Sprintf("%t", selection.RepairApproved)
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
	service, err := openConfigureGlobalSettingsService(cmd.Context(), settings.Options{
		Roots: roots, LookupEnv: configureLookupEnv(lookupSecret),
	})
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
	if selection.Provider == "openai-compat" && credentialSource == credentials.SourceNone {
		credentialSource = credentials.SourceFile
	}
	storeCredential := selection.Provider == "openai-compat" && selection.Credential != "" && credentialSource != credentials.SourceEnvironment
	return &setupOnboardingState{
		Configuration: installer.SetupConfiguration{
			Values: values, Fingerprint: validation.Fingerprint,
			Confirmations: confirmations, SourceGeneration: validation.SourceGeneration,
			CredentialSource: credentialSource, StoreCredential: storeCredential,
			ProviderTestStatus: "required_before_apply",
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
	if state == nil {
		return nil
	}
	service, err := openConfigureGlobalSettingsService(cmd.Context(), settings.Options{
		Roots: state.Roots, LookupEnv: configureLookupEnv(state.Credential),
	})
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
			ProviderTestStatus: "required_before_apply",
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
	if values[config.FieldCommitStrategy] == "event" {
		return "Maximum Speed"
	}
	return "Everyday"
}

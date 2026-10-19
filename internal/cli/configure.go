package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/charmbracelet/x/term"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
)

type configureValidationService interface {
	AuthoringPreview() (settings.AuthoringPreview, error)
	Validate(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.Validation, error)
	Close() error
}

type configureGlobalSettingsService interface {
	configureValidationService
	SaveGlobalSetup(context.Context, settings.SaveGlobalSetupRequest) (settings.SaveGlobalSetupResult, error)
}

var (
	openConfigureGlobalSettingsService = func(ctx context.Context, opts settings.Options) (configureGlobalSettingsService, error) {
		return settings.NewGlobalService(ctx, opts)
	}
	runConfigureWizard        = settingsui.RunConfigureWizard
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
)

func configureTerminalTooShort(output io.Writer) bool {
	file, ok := output.(interface{ Fd() uintptr })
	if !ok || !term.IsTerminal(file.Fd()) {
		return false
	}
	_, rows, err := term.GetSize(file.Fd())
	return err == nil && rows > 0 && rows < 24
}

func selectionDraft(selection settingsui.ConfigureSelection) map[string]string {
	out := make(map[string]string, 12)
	out[config.FieldCommitStrategy] = selection.Strategy
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
	strategy := fallbackConfigureValue(values[config.FieldCommitStrategy], "local")
	provider := fallbackConfigureValue(values[config.FieldProvider], "deterministic")
	baseURL := fallbackConfigureValue(values[config.FieldBaseURL], ai.DefaultOpenAIBaseURL)
	verification := fallbackConfigureValue(
		values[config.FieldIntentVerification],
		"none",
	)
	selection := settingsui.ConfigureSelection{
		Strategy:            strategy,
		CommitFormat:        fallbackConfigureValue(values[config.FieldCommitFormat], "imperative"),
		Provider:            provider,
		Model:               fallbackConfigureValue(values[config.FieldModel], config.DefaultOpenAIModel),
		BaseURL:             baseURL,
		ProviderTimeout:     fallbackConfigureValue(values[config.FieldTimeout], ai.DefaultProviderTimeout.String()),
		CAFile:              strings.TrimSpace(values[config.FieldCAFile]),
		VerificationMode:    verification,
		ExecutionMode:       "finish",
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

func fallbackConfigureValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
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
	return out
}

func rollbackConfigureCredential(roots paths.Roots, changed, previousSet bool, previous string) string {
	if !changed {
		return "No API key restore was needed."
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
		return "The previous API key could not be restored. Run acd config credentials status, then acd config credentials set --stdin."
	}
	return "The previous API key was restored."
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

func configureSettingsOptions(
	roots paths.Roots,
	repo string,
	credentialLookup func(string) (string, bool),
) settings.Options {
	return settings.Options{
		Roots: roots, RepoPath: repo, LookupEnv: os.LookupEnv,
		CredentialLookup: credentialLookup,
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

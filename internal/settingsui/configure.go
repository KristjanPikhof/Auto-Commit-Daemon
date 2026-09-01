package settingsui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"charm.land/huh/v2"
	"github.com/charmbracelet/x/term"
)

// ConfigureWizardOptions contains presentation-safe setup defaults. Credential
// values are accepted only in ConfigureSelection and are never rendered.
type ConfigureWizardOptions struct {
	Input                io.Reader
	Output               io.Writer
	Accessible           bool
	Defaults             map[string]string
	DetectedCommand      string
	DetectedQuickCommand string
	DetectedQuickSource  string
	DetectedFullCommand  string
	DetectedFullSource   string
	ExplicitMode         bool
	RepositoryScoped     bool
	HasCredential        bool
	CredentialFromStdin  bool
	ProviderConfigured   bool
	OpenAIConfigured     bool
	IncludeCommitFormat  bool
	IncludeCAFile        bool
}

// ConfigureSelection is an in-memory draft. Callers must discard Credential
// after the protected store has accepted it.
type ConfigureSelection struct {
	Experience                  string
	Strategy                    string
	Preset                      string
	CommitFormat                string
	Provider                    string
	Model                       string
	BaseURL                     string
	ProviderTimeout             string
	CAFile                      string
	Credential                  string
	DiffContextApproved         bool
	EndpointCredentialsApproved bool
	SubprocessApproved          bool
	VerificationMode            string
	VerificationCommand         string
	VerificationSource          string
	ExecutionMode               string
	VerificationApproved        bool
	RepairApproved              bool
	Confirmed                   bool
}

// RunConfigureWizard stages every setup choice in memory. It deliberately does
// not call a provider, run a command, or write configuration.
func RunConfigureWizard(ctx context.Context, opts ConfigureWizardOptions) (ConfigureSelection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Input == nil || opts.Output == nil {
		return ConfigureSelection{}, errors.New("configure wizard requires input and output")
	}
	defaults := opts.Defaults
	selection := ConfigureSelection{
		Strategy:        fallback(defaults["commit.strategy"], "intent"),
		Preset:          fallback(defaults["commit.preset"], "balanced"),
		CommitFormat:    fallback(defaults["commit.format"], "imperative"),
		Provider:        fallback(defaults["ai.provider"], "openai-compat"),
		Model:           fallback(defaults["ai.model"], "gpt-5.4-mini"),
		BaseURL:         fallback(defaults["ai.base_url"], "https://api.openai.com/v1"),
		ProviderTimeout: fallback(defaults["ai.timeout"], "30s"),
		CAFile:          defaults["ai.ca_file"],
	}
	selection.Experience = configureExperience(selection.Strategy, selection.Preset)
	if opts.ExplicitMode && !opts.RepositoryScoped &&
		selection.Strategy == "intent" && selection.Preset == "quality" {
		return ConfigureSelection{}, errors.New(
			"configure wizard: Strict Review is available only for repository-scoped setup",
		)
	}
	if !opts.ExplicitMode {
		options := []huh.Option[string]{
			huh.NewOption("Everyday work: semantic commits with ACD safety checks (recommended)", "everyday"),
			huh.NewOption("Maximum speed: immediate one-change commits, no project checks", "speed"),
		}
		description := "Everyday is the recommended balance. Project tests are not run."
		if opts.RepositoryScoped {
			options = append(options,
				huh.NewOption("Strict review: semantic commits gated by the full test suite (multi-minute)", "strict"),
			)
			description = "Everyday runs no project tests. Strict Review can take several minutes."
		} else if selection.Experience == "strict" {
			selection.Experience = "everyday"
		}
		experienceForm := huh.NewForm(huh.NewGroup(
			huh.NewSelect[string]().Key("experience").Title("How should ACD work?").
				Description(description).
				Options(options...).Value(&selection.Experience),
		))
		if err := runConfigureForm(ctx, experienceForm, opts); err != nil {
			return ConfigureSelection{}, err
		}
		selection.Strategy, selection.Preset = configureExperienceMode(selection.Experience)
	}
	if opts.IncludeCommitFormat {
		formatForm := huh.NewForm(huh.NewGroup(
			huh.NewSelect[string]().Key("commit-format").Title("How should commit messages look?").
				Description("Imperative: Fix setup retries. Conventional: fix: handle setup retries.").
				Options(
					huh.NewOption("Imperative (recommended)", "imperative"),
					huh.NewOption("Conventional Commits", "conventional"),
				).Value(&selection.CommitFormat),
		))
		if err := runConfigureForm(ctx, formatForm, opts); err != nil {
			return ConfigureSelection{}, err
		}
	}

	providerKind, subprocessName := configureProviderParts(selection.Provider)
	providerReady := opts.ProviderConfigured && (providerKind == "subprocess" ||
		(providerKind == "openai-compat" && opts.HasCredential) ||
		(providerKind == "deterministic" &&
			!(selection.Strategy == "intent" && selection.Preset == "quality")))
	providerSelected := false
	if !providerReady {
		options := []huh.Option[string]{
			huh.NewOption("OpenAI-compatible provider (network)", "openai-compat"),
			huh.NewOption("Local subprocess provider", "subprocess"),
		}
		if !(selection.Strategy == "intent" && selection.Preset == "quality") {
			options = append([]huh.Option[string]{
				huh.NewOption("Local rules (no AI or network)", "deterministic"),
			}, options...)
		}
		providerForm := huh.NewForm(huh.NewGroup(
			huh.NewSelect[string]().Key("provider").Title("Commit message provider").
				Description("Local rules create commits without AI. History rewrite needs an OpenAI-compatible or local subprocess provider.").
				Options(options...).Value(&providerKind),
		))
		if err := runConfigureForm(ctx, providerForm, opts); err != nil {
			return ConfigureSelection{}, err
		}
		providerSelected = true
	}
	switch providerKind {
	case "openai-compat":
		if providerSelected || !opts.OpenAIConfigured {
			providerForm := huh.NewForm(huh.NewGroup(
				huh.NewInput().Key("endpoint").
					Title("OpenAI-compatible endpoint").
					Description("Use the base URL ending in /v1.").
					Value(&selection.BaseURL),
				huh.NewInput().Key("model").
					Title("Model").
					Description("The model name supported by this endpoint.").
					Value(&selection.Model),
			))
			if err := runConfigureForm(ctx, providerForm, opts); err != nil {
				return ConfigureSelection{}, err
			}
			selection.BaseURL = strings.TrimSpace(selection.BaseURL)
			selection.Model = strings.TrimSpace(selection.Model)
		}
		if opts.IncludeCAFile {
			advanced := strings.TrimSpace(selection.CAFile) != ""
			advancedForm := huh.NewForm(huh.NewGroup(
				huh.NewConfirm().Key("custom-ca").Title("More connection options?").
					Description("Choose this only when the endpoint needs a custom CA certificate.").Value(&advanced),
			))
			if err := runConfigureForm(ctx, advancedForm, opts); err != nil {
				return ConfigureSelection{}, err
			}
			if advanced {
				caForm := huh.NewForm(huh.NewGroup(
					huh.NewInput().Key("ca-file").Title("CA certificate file").
						Description("Enter the PEM file used to trust this endpoint.").Value(&selection.CAFile),
				))
				if err := runConfigureForm(ctx, caForm, opts); err != nil {
					return ConfigureSelection{}, err
				}
				selection.CAFile = strings.TrimSpace(selection.CAFile)
			} else {
				selection.CAFile = ""
			}
		}
		if !opts.HasCredential && !opts.CredentialFromStdin {
			credential, readErr := readConfigureSecret(opts.Input, opts.Output)
			if readErr != nil {
				return ConfigureSelection{}, readErr
			}
			selection.Credential = credential
		}
		selection.Provider = "openai-compat"
	case "subprocess":
		subprocessName = fallback(subprocessName, "acd-ai")
		form := huh.NewForm(huh.NewGroup(
			huh.NewInput().Key("subprocess").Title("Subprocess plugin name").Description(
				"ACD executes acd-ai-<name> from PATH.").Value(&subprocessName),
		))
		if err := runConfigureForm(ctx, form, opts); err != nil {
			return ConfigureSelection{}, err
		}
		selection.Provider = "subprocess:" + strings.TrimSpace(subprocessName)
	case "deterministic":
		selection.Provider = "deterministic"
	default:
		return ConfigureSelection{}, fmt.Errorf("configure wizard: unsupported provider %q", safeText(providerKind))
	}

	selection.VerificationMode = configureVerificationMode(selection.Strategy, selection.Preset)
	if selection.VerificationMode == "full" {
		field := "verification." + selection.VerificationMode + ".command"
		selection.VerificationCommand = strings.TrimSpace(defaults[field])
		if selection.VerificationCommand != "" {
			selection.VerificationSource = "saved setting"
		}
		if selection.VerificationCommand == "" {
			selection.VerificationCommand = strings.TrimSpace(opts.DetectedFullCommand)
			selection.VerificationSource = strings.TrimSpace(opts.DetectedFullSource)
			if selection.VerificationCommand == "" {
				selection.VerificationCommand = strings.TrimSpace(opts.DetectedCommand)
				if selection.VerificationCommand != "" {
					selection.VerificationSource = "detected project command"
				}
			}
		}
		if selection.VerificationCommand == "" {
			return ConfigureSelection{}, errors.New(
				"configure wizard: Strict Review is unavailable because no full verification command was detected; " +
					"set one in acd settings",
			)
		}
	}
	selection.ExecutionMode = "background activation gate"
	if selection.VerificationMode != "full" {
		selection.ExecutionMode = "immediate"
	}

	needsDiff := selection.Strategy == "intent" ||
		(selection.Strategy == "event" && selection.Preset != "fast")
	diffContext := needsDiff && selection.Provider != "deterministic"
	customEndpoint := selection.Provider == "openai-compat" &&
		strings.TrimRight(strings.TrimSpace(selection.BaseURL), "/") != "https://api.openai.com/v1"
	selection.DiffContextApproved = diffContext
	selection.EndpointCredentialsApproved = customEndpoint
	selection.SubprocessApproved = strings.HasPrefix(selection.Provider, "subprocess:")
	return selection, nil
}

func readConfigureSecret(input io.Reader, output io.Writer) (string, error) {
	if file, ok := input.(*os.File); ok && term.IsTerminal(file.Fd()) {
		if _, err := fmt.Fprint(output, "API key (masked; stored only after tests pass): "); err != nil {
			return "", err
		}
		value, err := term.ReadPassword(file.Fd())
		_, _ = fmt.Fprintln(output)
		if err != nil {
			return "", errors.New("configure wizard: read masked API key")
		}
		return strings.TrimSpace(string(value)), nil
	}
	if _, err := fmt.Fprintln(output, "API key: [read without rendering; stored only after tests pass]"); err != nil {
		return "", err
	}
	line, err := readConfigureSecretLine(input, 32*1024)
	if err != nil {
		return "", errors.New("configure wizard: read API key")
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return "", errors.New("configure wizard: API key is required; use --credential-stdin for piped setup")
	}
	return line, nil
}

func readConfigureSecretLine(input io.Reader, limit int) (string, error) {
	var body strings.Builder
	body.Grow(min(limit, 256))
	var one [1]byte
	for body.Len() <= limit {
		count, err := input.Read(one[:])
		if count == 1 {
			if one[0] == '\n' {
				return body.String(), nil
			}
			body.WriteByte(one[0])
		}
		if err != nil {
			if errors.Is(err, io.EOF) && body.Len() > 0 {
				return body.String(), nil
			}
			return "", err
		}
	}
	return "", errors.New("credential exceeds safe input limit")
}

// ConfirmConfigurePreview is separate so callers can render the exact final
// projection before asking for the single apply confirmation.
type ConfigurePreviewApproval struct {
	Verification bool
	Repair       bool
	Apply        bool
}

type ConfigurePreviewApprovalOptions struct {
	VerificationMode    string
	VerificationCommand string
	RepairEnabled       bool
	RepairHorizon       string
	RepairMaxCommits    string
	Global              bool
	Action              string
}

type ConfigureRecoveryChoice string

const (
	ConfigureRecoveryRetry    ConfigureRecoveryChoice = "retry"
	ConfigureRecoverySwitch   ConfigureRecoveryChoice = "switch"
	ConfigureRecoveryAdvanced ConfigureRecoveryChoice = "advanced"
	ConfigureRecoveryLeave    ConfigureRecoveryChoice = "leave"
)

func ChooseConfigureRecovery(
	ctx context.Context,
	input io.Reader,
	output io.Writer,
	accessible bool,
) (ConfigureRecoveryChoice, error) {
	choice := ConfigureRecoveryRetry
	form := huh.NewForm(huh.NewGroup(
		huh.NewSelect[ConfigureRecoveryChoice]().
			Title("Previous configuration validation needs attention").
			Description("The reviewed setup is preserved; capture remains active and commit publishing is paused.").
			Options(
				huh.NewOption("Retry the same exact check", ConfigureRecoveryRetry),
				huh.NewOption("Switch experience", ConfigureRecoverySwitch),
				huh.NewOption("Open advanced settings", ConfigureRecoveryAdvanced),
				huh.NewOption("Leave capture-only state unchanged", ConfigureRecoveryLeave),
			).Value(&choice),
	))
	if accessible || os.Getenv("NO_COLOR") != "" {
		form = form.WithTheme(huh.ThemeFunc(huh.ThemeBase))
	}
	if err := form.WithAccessible(accessible).WithInput(input).
		WithOutput(output).WithShowHelp(true).RunWithContext(ctx); err != nil {
		return "", err
	}
	return choice, nil
}

// ConfirmConfigurePreview binds one consent to every permission shown in the
// exact final preview.
func ConfirmConfigurePreview(ctx context.Context, input io.Reader, output io.Writer, accessible bool, opts ConfigurePreviewApprovalOptions) (ConfigurePreviewApproval, error) {
	approval := ConfigurePreviewApproval{}
	title := "Approve these permissions, save, and enable ACD?"
	if opts.Global {
		title = "Approve these permissions and save the global defaults?"
		if opts.Action == "replace_global" {
			title = "Approve these permissions and replace the global defaults?"
		}
	} else if opts.Action == "inherit_global" {
		title = "Use every global setting for this repository and enable ACD?"
	}
	form := huh.NewForm(huh.NewGroup(
		huh.NewConfirm().Key("apply").Title(title).
			Value(&approval.Apply),
	))
	if accessible || os.Getenv("NO_COLOR") != "" {
		form = form.WithTheme(huh.ThemeFunc(huh.ThemeBase))
	}
	form = form.WithAccessible(accessible)
	if err := form.WithInput(input).WithOutput(output).
		WithShowHelp(true).RunWithContext(ctx); err != nil {
		return ConfigurePreviewApproval{}, err
	}
	approval.Verification = approval.Apply &&
		(opts.VerificationMode == "fast" || opts.VerificationMode == "full")
	approval.Repair = approval.Apply && opts.RepairEnabled
	return approval, nil
}

func runConfigureForm(ctx context.Context, form *huh.Form, opts ConfigureWizardOptions) error {
	if opts.Accessible || os.Getenv("NO_COLOR") != "" {
		form = form.WithTheme(huh.ThemeFunc(huh.ThemeBase))
	}
	return form.WithAccessible(opts.Accessible).WithInput(opts.Input).WithOutput(opts.Output).
		WithShowHelp(true).RunWithContext(ctx)
}

func configureProviderParts(value string) (string, string) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "subprocess:") {
		return "subprocess", strings.TrimSpace(strings.TrimPrefix(value, "subprocess:"))
	}
	switch value {
	case "deterministic", "openai-compat":
		return value, ""
	default:
		return "openai-compat", ""
	}
}

func configureVerificationMode(strategy, preset string) string {
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

func configureExperience(strategy, preset string) string {
	switch {
	case strategy == "event" && preset == "fast":
		return "speed"
	case strategy == "intent" && preset == "quality":
		return "strict"
	default:
		return "everyday"
	}
}

func configureExperienceMode(experience string) (string, string) {
	switch experience {
	case "speed":
		return "event", "fast"
	case "strict":
		return "intent", "quality"
	default:
		return "intent", "balanced"
	}
}

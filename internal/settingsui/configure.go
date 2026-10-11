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

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
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
	CredentialStatus     func() (bool, error)
	CredentialFromStdin  bool
	ProviderConfigured   bool
	OpenAIConfigured     bool
	IncludeCommitFormat  bool
	IncludeCAFile        bool
}

// ConfigureSelection is an in-memory draft. Callers must discard Credential
// after the protected store has accepted it.
type ConfigureSelection struct {
	Strategy                    string
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
		Strategy:        fallback(defaults["commit.strategy"], "local"),
		CommitFormat:    fallback(defaults["commit.format"], "imperative"),
		Provider:        fallback(defaults["ai.provider"], "deterministic"),
		Model:           fallback(defaults["ai.model"], config.DefaultOpenAIModel),
		BaseURL:         fallback(defaults["ai.base_url"], "https://api.openai.com/v1"),
		ProviderTimeout: fallback(defaults["ai.timeout"], "30s"),
		CAFile:          defaults["ai.ca_file"],
	}
	if selection.Strategy == "event" {
		selection.Strategy = "local"
	}
	if !opts.ExplicitMode {
		form := huh.NewForm(huh.NewGroup(huh.NewSelect[string]().Key("strategy").Title("How should finish create commits?").
			Description("Files are saved continuously. Branch commits are created only when you run acd finish.").
			Options(huh.NewOption("Local: one complete batch, offline (recommended)", "local"), huh.NewOption("Intent: AI groups completed goals", "intent")).Value(&selection.Strategy)))
		if err := runConfigureForm(ctx, form, opts); err != nil {
			return ConfigureSelection{}, err
		}
	}
	if opts.IncludeCommitFormat {
		formatForm := huh.NewForm(huh.NewGroup(
			huh.NewSelect[string]().Key("commit-format").Title("How should commit messages look?").
				Description("Imperative: Fix setup retries. Conventional: fix: handle setup retries.").
				Options(
					huh.NewOption("Imperative (recommended): Fix setup retries", "imperative"),
					huh.NewOption("Conventional: fix: handle setup retries", "conventional"),
				).Value(&selection.CommitFormat),
		))
		if err := runConfigureForm(ctx, formatForm, opts); err != nil {
			return ConfigureSelection{}, err
		}
	}

	if selection.Strategy == "local" {
		selection.Provider = "deterministic"
	}
	providerKind, subprocessName := configureProviderParts(selection.Provider)
	configuredProviderKind := providerKind
	if providerKind == "openai-compat" && opts.CredentialStatus != nil {
		var err error
		opts.HasCredential, err = opts.CredentialStatus()
		if err != nil {
			return ConfigureSelection{}, err
		}
	}
	providerReady := opts.ProviderConfigured && (providerKind == "subprocess" ||
		(providerKind == "openai-compat" && opts.HasCredential))
	providerSelected := false
	if selection.Strategy == "intent" && !providerReady {
		options := []huh.Option[string]{
			huh.NewOption("OpenAI-compatible connection", "openai-compat"),
			huh.NewOption("Local AI subprocess", "subprocess"),
		}
		if providerKind == "deterministic" {
			providerKind = "openai-compat"
		}
		providerForm := huh.NewForm(huh.NewGroup(
			huh.NewSelect[string]().Key("provider").Title("Which provider should Intent use?").
				Description("AI can group related code, tests, and documentation into one meaningful commit. Repository data is shared only with your approval.").
				Options(options...).Value(&providerKind),
		))
		if err := runConfigureForm(ctx, providerForm, opts); err != nil {
			return ConfigureSelection{}, err
		}
		providerSelected = true
	}
	switch providerKind {
	case "openai-compat":
		if configuredProviderKind != "openai-compat" && opts.CredentialStatus != nil {
			var err error
			opts.HasCredential, err = opts.CredentialStatus()
			if err != nil {
				return ConfigureSelection{}, err
			}
		}
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

	selection.VerificationMode = fallback(defaults["intent.verification"], "none")
	if selection.VerificationMode == "fast" || selection.VerificationMode == "full" {
		selection.VerificationCommand = strings.TrimSpace(defaults["verification."+selection.VerificationMode+".command"])
		selection.VerificationSource = "saved setting"
	}
	selection.ExecutionMode = "explicit finish"
	selection.DiffContextApproved = selection.Strategy == "intent" && defaults["ai.diff_egress"] == "true"
	selection.EndpointCredentialsApproved = selection.Provider == "openai-compat" && strings.TrimRight(strings.TrimSpace(selection.BaseURL), "/") != "https://api.openai.com/v1"
	selection.SubprocessApproved = strings.HasPrefix(selection.Provider, "subprocess:")
	return selection, nil
}

func readConfigureSecret(input io.Reader, output io.Writer) (string, error) {
	if file, ok := input.(*os.File); ok && term.IsTerminal(file.Fd()) {
		if _, err := fmt.Fprint(output, "API key (masked; stored after approval): "); err != nil {
			return "", err
		}
		value, err := term.ReadPassword(file.Fd())
		_, _ = fmt.Fprintln(output)
		if err != nil {
			return "", errors.New("configure wizard: read masked API key")
		}
		return strings.TrimSpace(string(value)), nil
	}
	if _, err := fmt.Fprintln(output, "API key: [read without rendering; stored after approval]"); err != nil {
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
	Apply        bool
}

type ConfigurePreviewApprovalOptions struct {
	VerificationMode    string
	VerificationCommand string
	Global              bool
	Action              string
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

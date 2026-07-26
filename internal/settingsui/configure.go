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
	Input               io.Reader
	Output              io.Writer
	Accessible          bool
	Defaults            map[string]string
	DetectedCommand     string
	HasCredential       bool
	CredentialFromStdin bool
}

// ConfigureSelection is an in-memory draft. Callers must discard Credential
// after the protected store has accepted it.
type ConfigureSelection struct {
	Strategy                    string
	Preset                      string
	CommitFormat                string
	Provider                    string
	Model                       string
	BaseURL                     string
	ProviderTimeout             string
	Credential                  string
	DiffContextApproved         bool
	EndpointCredentialsApproved bool
	SubprocessApproved          bool
	VerificationMode            string
	VerificationCommand         string
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
	}
	modeForm := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Key("strategy").Title("Commit strategy").Description(
			"Intent plans semantic atomic commits; Event publishes one capture per commit.").Options(
			huh.NewOption("Intent (recommended)", "intent"),
			huh.NewOption("Event", "event"),
		).Value(&selection.Strategy),
		huh.NewSelect[string]().Key("preset").Title("Preset").Description(
			"Balanced is the everyday default. Advanced settings can customize it later.").Options(
			huh.NewOption("Balanced (recommended)", "balanced"),
			huh.NewOption("Fast", "fast"),
			huh.NewOption("Quality", "quality"),
		).Value(&selection.Preset),
		huh.NewSelect[string]().Key("format").Title("Commit message format").Options(
			huh.NewOption("Imperative", "imperative"),
			huh.NewOption("Conventional", "conventional"),
		).Value(&selection.CommitFormat),
	))
	if err := runConfigureForm(ctx, modeForm, opts); err != nil {
		return ConfigureSelection{}, err
	}

	providerKind, subprocessName := configureProviderParts(selection.Provider)
	providerForm := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Key("provider").Title("Provider").Options(
			huh.NewOption("OpenAI-compatible network provider", "openai-compat"),
			huh.NewOption("Local subprocess provider", "subprocess"),
			huh.NewOption("Deterministic messages (Event Fast only)", "deterministic"),
		).Value(&providerKind),
	))
	if err := runConfigureForm(ctx, providerForm, opts); err != nil {
		return ConfigureSelection{}, err
	}
	switch providerKind {
	case "openai-compat":
		details := huh.NewForm(huh.NewGroup(
			huh.NewInput().Key("model").Title("Model").Value(&selection.Model),
			huh.NewInput().Key("base_url").Title("OpenAI-compatible base URL").Value(&selection.BaseURL),
			huh.NewInput().Key("timeout").Title("Provider timeout").Value(&selection.ProviderTimeout),
		))
		if err := runConfigureForm(ctx, details, opts); err != nil {
			return ConfigureSelection{}, err
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
	if selection.VerificationMode != "none" {
		field := "verification." + selection.VerificationMode + ".command"
		selection.VerificationCommand = strings.TrimSpace(defaults[field])
		if selection.VerificationCommand == "" {
			selection.VerificationCommand = strings.TrimSpace(opts.DetectedCommand)
		}
		if selection.VerificationCommand == "" {
			return ConfigureSelection{}, fmt.Errorf(
				"configure wizard: no %s verification command was detected; "+
					"choose Intent Fast or set an exact command in acd settings",
				safeText(selection.VerificationMode),
			)
		}
	}

	needsDiff := selection.Strategy == "intent" ||
		(selection.Strategy == "event" && selection.Preset != "fast")
	networkDiff := needsDiff && !strings.HasPrefix(selection.Provider, "subprocess:")
	customEndpoint := selection.Provider == "openai-compat" &&
		strings.TrimRight(strings.TrimSpace(selection.BaseURL), "/") != "https://api.openai.com/v1"
	var confirmations []huh.Field
	if networkDiff {
		confirmations = append(confirmations, huh.NewConfirm().Key("diff").Title(
			"Approve redacted repository diff context for this network provider?").
			Value(&selection.DiffContextApproved))
	} else if needsDiff {
		confirmations = append(confirmations, huh.NewConfirm().Key("diff").Title(
			"Approve redacted repository diff context for this local provider?").
			Value(&selection.DiffContextApproved))
	}
	if customEndpoint {
		confirmations = append(confirmations, huh.NewConfirm().Key("endpoint").Title(
			"Approve sending provider credentials to "+safeText(selection.BaseURL)+" ?").
			Value(&selection.EndpointCredentialsApproved))
	}
	if strings.HasPrefix(selection.Provider, "subprocess:") {
		confirmations = append(confirmations, huh.NewConfirm().Key("subprocess").Title(
			"Approve execution of local provider "+safeText(selection.Provider)+" ?").
			Value(&selection.SubprocessApproved))
	}
	if len(confirmations) > 0 {
		if err := runConfigureForm(ctx, huh.NewForm(huh.NewGroup(confirmations...)), opts); err != nil {
			return ConfigureSelection{}, err
		}
	}
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
}

// ConfirmConfigurePreview binds consent to the exact effective command and
// repair policy shown in the resolved final preview.
func ConfirmConfigurePreview(ctx context.Context, input io.Reader, output io.Writer, accessible bool, opts ConfigurePreviewApprovalOptions) (ConfigurePreviewApproval, error) {
	approval := ConfigurePreviewApproval{}
	fields := make([]huh.Field, 0, 3)
	if opts.VerificationMode != "none" {
		fields = append(fields, huh.NewConfirm().Key("verification").Title(
			"Approve exact "+safePreviewValue(opts.VerificationMode, 16)+
				" verification command: "+safePreviewValue(opts.VerificationCommand, 256)+" ?").
			Value(&approval.Verification))
	}
	if opts.RepairEnabled {
		fields = append(fields, huh.NewConfirm().Key("repair").Title(
			"Approve automatic repair of eligible ACD commits within "+
				safePreviewValue(opts.RepairHorizon, 32)+", up to "+
				safePreviewValue(opts.RepairMaxCommits, 8)+" commits?").
			Value(&approval.Repair))
	}
	fields = append(fields, huh.NewConfirm().Key("apply").Title(
		"Apply this reviewed configuration, create one runtime revision, and enable ACD?").
		Value(&approval.Apply))

	form := huh.NewForm(huh.NewGroup(fields...))
	if accessible || os.Getenv("NO_COLOR") != "" {
		form = form.WithTheme(huh.ThemeFunc(huh.ThemeBase))
	}
	form = form.WithAccessible(accessible)
	if err := form.WithInput(input).WithOutput(output).
		WithShowHelp(true).RunWithContext(ctx); err != nil {
		return ConfigurePreviewApproval{}, err
	}
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
		return "fast"
	case "quality":
		return "full"
	default:
		return "none"
	}
}

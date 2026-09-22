package settingsui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"charm.land/huh/v2"
)

// EditorDraft keeps credentials out of the ordinary settings map and any JSON output.
type EditorDraft struct {
	Scope      string
	Generation uint64
	Changes    map[string]*string
	Credential string `json:"-"`
}

type EditorField struct {
	Key, Value, Source, Inherited string
	Overridden                    bool
}

type EditorSnapshot struct {
	Generation                uint64
	Fields                    []EditorField
	Credential                string
	CredentialFromEnvironment bool
}

type EditorReview struct {
	Text string
	Save func(context.Context) (string, error)
}

type EditorBackend interface {
	Load(context.Context, string, map[string]*string) (EditorSnapshot, error)
	Review(context.Context, EditorDraft) (EditorReview, error)
}

type EditorOptions struct {
	Input               io.Reader
	Output              io.Writer
	Accessible, NoColor bool
	Scope, Repo         string
}

// RunEditor has one save action. Reviewing and saving use the same captured draft.
func RunEditor(ctx context.Context, backend EditorBackend, opts EditorOptions) error {
	if opts.Input == nil {
		opts.Input = os.Stdin
	}
	if opts.Output == nil {
		opts.Output = os.Stdout
	}
	if opts.Scope == "" {
		opts.Scope = "global"
	}
	draft := EditorDraft{Scope: opts.Scope, Changes: map[string]*string{}}
	advanced := false
	message := "Choose a setting to edit. Changes are saved together."
	initialized := false
	for {
		snapshot, err := backend.Load(ctx, draft.Scope, draft.Changes)
		if err != nil {
			return err
		}
		if !initialized {
			draft.Generation = snapshot.Generation
			initialized = true
		}
		if opts.Accessible {
			fmt.Fprintln(opts.Output, message)
		}
		action := ""
		fields := editorMenu(snapshot, draft, advanced, opts.Repo)
		form := huh.NewForm(huh.NewGroup(huh.NewSelect[string]().Title("ACD Settings · " + editorScopeLabel(draft.Scope, opts.Repo)).Description(message).Options(fields...).Value(&action).Height(12)))
		if err := runEditorForm(ctx, form, opts); err != nil {
			return editorExitError(err)
		}
		switch action {
		case "cancel":
			if editorDirty(draft) {
				discard, err := editorConfirm(ctx, "Discard unsaved changes?", opts)
				if err != nil {
					return editorExitError(err)
				}
				if !discard {
					continue
				}
			}
			return nil
		case "scope":
			scope := draft.Scope
			if err := runEditorForm(ctx, huh.NewForm(huh.NewGroup(huh.NewSelect[string]().Title("Where should these settings apply?").Options(huh.NewOption("Global defaults", "global"), huh.NewOption("This repository: "+safeText(opts.Repo), "repo")).Value(&scope))), opts); err != nil {
				return editorExitError(err)
			}
			if scope == draft.Scope {
				continue
			}
			if editorDirty(draft) {
				discard, err := editorConfirm(ctx, "Discard unsaved changes before switching scope?", opts)
				if err != nil {
					return editorExitError(err)
				}
				if !discard {
					continue
				}
			}
			draft = EditorDraft{Scope: scope, Changes: map[string]*string{}}
			initialized = false
			message = "Choose a setting to edit. Changes are saved together."
		case "commit.mode":
			strategy, preset := "", ""
			for _, field := range snapshot.Fields {
				if field.Key == "commit.strategy" {
					strategy = field.Value
				}
				if field.Key == "commit.preset" {
					preset = field.Value
				}
			}
			mode := configureExperience(strategy, preset)
			options := []huh.Option[string]{huh.NewOption("Everyday: semantic commits with safety checks", "everyday"), huh.NewOption("Maximum speed: immediate commits", "speed")}
			if draft.Scope == "repo" {
				options = append(options, huh.NewOption("Strict review: require project verification", "strict"))
			}
			if err := runEditorForm(ctx, huh.NewForm(huh.NewGroup(huh.NewSelect[string]().Title("Commit mode").Description("Advanced customizations are kept. Review the effective verification settings before saving.").Options(options...).Value(&mode))), opts); err != nil {
				return editorExitError(err)
			}
			strategy, preset = configureExperienceMode(mode)
			draft.Changes["commit.strategy"], draft.Changes["commit.preset"] = &strategy, &preset
			message = "Commit mode changed. Choose Save changes when ready."
		case "advanced":
			advanced = !advanced
		case "save":
			review, err := backend.Review(ctx, draft)
			if err != nil {
				message = "Cannot save: " + safeText(err.Error())
				continue
			}
			// The review is deliberately plain text so narrow and accessible terminals
			// show the entire change list, affected repositories, and permissions.
			if _, err := fmt.Fprintln(opts.Output, "\nReview changes\n"+review.Text); err != nil {
				return err
			}
			approved, err := editorConfirm(ctx, "Save these changes and apply them where possible?", opts)
			if err != nil {
				return editorExitError(err)
			}
			if !approved {
				continue
			}
			fmt.Fprintln(opts.Output, "Testing and saving settings...")
			result, err := review.Save(ctx)
			if err != nil {
				message = "Save did not finish: " + safeText(err.Error())
				fmt.Fprintln(opts.Output, message)
				// Keep edits so a failed connection can be corrected and retried.
				continue
			}
			fmt.Fprintln(opts.Output, result)
			return nil
		case "ai.api_key":
			if snapshot.CredentialFromEnvironment {
				message = "ACD_AI_API_KEY supplies the current key. Unset it in this terminal before replacing the stored key."
				continue
			}
			value, err := readConfigureSecret(opts.Input, opts.Output)
			if err != nil {
				message = safeText(err.Error())
				continue
			}
			draft.Credential = value
			message = "New API key entered. It will be tested and stored securely when you save."
		default:
			var field EditorField
			for _, candidate := range snapshot.Fields {
				if candidate.Key == action {
					field = candidate
					break
				}
			}
			if field.Key == "" {
				continue
			}
			validate := func(value string) error {
				changes := make(map[string]*string, len(draft.Changes)+1)
				for key, change := range draft.Changes {
					changes[key] = change
				}
				changes[field.Key] = &value
				_, err := backend.Load(ctx, draft.Scope, changes)
				return err
			}
			value, inherit, err := editEditorField(ctx, field, draft.Scope, opts, validate)
			if errors.Is(err, huh.ErrUserAborted) {
				continue
			}
			if err != nil {
				return err
			}
			if inherit {
				draft.Changes[field.Key] = nil
			} else {
				draft.Changes[field.Key] = &value
			}
			message = "Unsaved changes. Choose Save changes when ready."
		}
	}
}

func editorMenu(snapshot EditorSnapshot, draft EditorDraft, advanced bool, repo string) []huh.Option[string] {
	rows := []huh.Option[string]{}
	if repo != "" {
		rows = append(rows, huh.NewOption("Editing: "+editorScopeLabel(draft.Scope, repo)+" (change)", "scope"))
	}
	primary := []string{"ai.provider", "ai.model", "ai.base_url", "ai.api_key", "commit.format", "commit.mode"}
	byKey := map[string]EditorField{}
	for _, field := range snapshot.Fields {
		byKey[field.Key] = field
	}
	add := func(key string) {
		if key == "commit.mode" {
			strategy, preset := byKey["commit.strategy"].Value, byKey["commit.preset"].Value
			label := strategy + " / " + preset
			switch {
			case strategy == "intent" && preset == "balanced":
				label = "Everyday"
			case strategy == "event" && preset == "fast":
				label = "Maximum speed"
			case strategy == "intent" && preset == "quality":
				label = "Strict review"
			}
			rows = append(rows, huh.NewOption("Commit mode: "+label, key))
			return
		}
		if key == "ai.api_key" {
			status := snapshot.Credential
			if draft.Credential != "" {
				status = "New key entered (unsaved)"
			}
			rows = append(rows, huh.NewOption("API key: "+status, key))
			return
		}
		field, exists := byKey[key]
		if !exists {
			return
		}
		desc := descriptor(key)
		label := desc.Label + ": " + safePreviewValue(field.Value, 70)
		if !field.Overridden {
			label += " (from " + safeText(field.Source) + ")"
		}
		if _, changed := draft.Changes[key]; changed {
			label += " *"
		}
		rows = append(rows, huh.NewOption(label, key))
	}
	for _, key := range primary {
		add(key)
	}
	label := "Advanced settings"
	if advanced {
		label = "Hide advanced settings"
	}
	rows = append(rows, huh.NewOption(label, "advanced"))
	if advanced {
		for _, field := range snapshot.Fields {
			found := false
			for _, key := range primary {
				if field.Key == key {
					found = true
					break
				}
			}
			if !found {
				add(field.Key)
			}
		}
	}
	return append(rows, huh.NewOption("Save changes", "save"), huh.NewOption("Cancel", "cancel"))
}

func editEditorField(ctx context.Context, field EditorField, scope string, opts EditorOptions, validate func(string) error) (string, bool, error) {
	desc := descriptor(field.Key)
	value := field.Value
	if field.Overridden {
		action := "edit"
		label := "Use inherited value: " + safePreviewValue(field.Inherited, 80)
		if scope == "global" {
			label = "Reset to default: " + safePreviewValue(field.Inherited, 80)
		}
		err := runEditorForm(ctx, huh.NewForm(huh.NewGroup(huh.NewSelect[string]().Title(desc.Label).Options(huh.NewOption("Change value", "edit"), huh.NewOption(label, "inherit")).Value(&action))), opts)
		if err != nil || action == "inherit" {
			return "", action == "inherit", err
		}
	}
	if field.Key == "ai.provider" {
		provider := value
		if strings.HasPrefix(provider, "subprocess:") {
			provider = "subprocess"
		}
		err := runEditorForm(ctx, huh.NewForm(huh.NewGroup(huh.NewSelect[string]().Title("AI provider").Options(
			huh.NewOption("OpenAI-compatible connection", "openai-compat"),
			huh.NewOption("Local automatic commits (no API key)", "deterministic"),
			huh.NewOption("Local subprocess provider", "subprocess"),
		).Value(&provider))), opts)
		if err != nil {
			return "", false, err
		}
		if provider == "subprocess" {
			name := strings.TrimPrefix(value, "subprocess:")
			if !strings.HasPrefix(value, "subprocess:") {
				name = ""
			}
			err = runEditorForm(ctx, huh.NewForm(huh.NewGroup(huh.NewInput().Title("Local provider name").Value(&name).Validate(func(value string) error { return validate("subprocess:" + strings.TrimSpace(value)) }))), opts)
			provider = "subprocess:" + strings.TrimSpace(name)
		}
		return provider, false, err
	}
	var input huh.Field
	if len(desc.Choices) > 0 {
		choices := []huh.Option[string]{}
		for _, choice := range desc.Choices {
			label := choice
			if choice == "true" {
				label = "On"
			} else if choice == "false" {
				label = "Off"
			}
			choices = append(choices, huh.NewOption(label, choice))
		}
		input = huh.NewSelect[string]().Title(desc.Label).Description(desc.Description).Options(choices...).Value(&value).Validate(validate)
	} else {
		input = huh.NewInput().Title(desc.Label).Description(desc.Description).Value(&value).Validate(validate)
	}
	err := runEditorForm(ctx, huh.NewForm(huh.NewGroup(input)), opts)
	return strings.TrimSpace(value), false, err
}

func editorConfirm(ctx context.Context, title string, opts EditorOptions) (bool, error) {
	approved := false
	err := runEditorForm(ctx, huh.NewForm(huh.NewGroup(huh.NewConfirm().Title(title).Value(&approved))), opts)
	return approved, err
}
func runEditorForm(ctx context.Context, form *huh.Form, opts EditorOptions) error {
	if opts.NoColor {
		form = form.WithTheme(huh.ThemeFunc(huh.ThemeBase))
	}
	return form.WithAccessible(opts.Accessible).WithInput(opts.Input).WithOutput(opts.Output).WithShowHelp(true).RunWithContext(ctx)
}
func editorScopeLabel(scope, repo string) string {
	if scope == "repo" {
		return "This repository: " + safeText(repo)
	}
	return "Global defaults"
}
func editorDirty(draft EditorDraft) bool { return len(draft.Changes) > 0 || draft.Credential != "" }
func editorExitError(err error) error {
	if errors.Is(err, huh.ErrUserAborted) {
		return nil
	}
	return err
}

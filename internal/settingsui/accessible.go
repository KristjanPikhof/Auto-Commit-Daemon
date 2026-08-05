package settingsui

import (
	"context"
	"fmt"
	"io"
	"strings"

	"charm.land/huh/v2"
)

type AccessibleValues struct {
	Values           map[string]string
	Action           string
	Confirm          bool
	Profile          string
	ExperimentBudget string
	ExperimentExpiry string
	ExperimentPolicy string
	raw              map[string]*string
}

type AccessibleAction struct {
	Action string
}

var quickProviderFields = map[string]bool{
	"ai.provider": true,
	"ai.model":    true,
	"ai.base_url": true,
	"ai.timeout":  true,
	"ai.ca_file":  true,
}

func AccessibleActionForm(input io.Reader, output io.Writer) (*huh.Form, *AccessibleAction) {
	values := &AccessibleAction{Action: "test"}
	form := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Key("action").Title("What do you want to do?").Options(
			huh.NewOption("Test current settings (recommended)", "test"),
			huh.NewOption("Apply current settings at the next safe boundary", "apply"),
			huh.NewOption("Change strategy or preset", "mode"),
			huh.NewOption("Quick provider setup", "quick"),
			huh.NewOption("Advanced settings", "advanced"),
			huh.NewOption("Revert to last known good", "revert"),
			huh.NewOption("Select repository profile", "profile"),
			huh.NewOption("Start bounded experiment", "experiment"),
			huh.NewOption("Cancel active experiment and revert", "cancel_experiment"),
		).Value(&values.Action),
	)).WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	return form, values
}

func RunAccessibleAction(ctx context.Context, input io.Reader, output io.Writer) (string, error) {
	form, values := AccessibleActionForm(input, output)
	if err := form.RunWithContext(ctx); err != nil {
		return "", err
	}
	return safeText(strings.ToLower(values.Action)), nil
}

func ConfirmAccessibleAction(ctx context.Context, action string, input io.Reader, output io.Writer) (bool, error) {
	confirmed := false
	form := huh.NewForm(huh.NewGroup(huh.NewConfirm().Key("confirm").Title(accessibleActionConfirmation(action)).Value(&confirmed))).
		WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	if err := form.RunWithContext(ctx); err != nil {
		return false, err
	}
	return confirmed, nil
}

func ConfirmAccessibleRisks(ctx context.Context, requirements []ConfirmationRequirement, input io.Reader, output io.Writer) (bool, error) {
	confirmed := false
	title := "Required confirmation: " + confirmationRequirementLabels(requirements) + ". Continue?"
	form := huh.NewForm(huh.NewGroup(huh.NewConfirm().Key("confirm_risks").Title(title).Value(&confirmed))).
		WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	if err := form.RunWithContext(ctx); err != nil {
		return false, err
	}
	return confirmed, nil
}

func accessibleActionConfirmation(action string) string {
	switch action {
	case "test":
		return "Run one strict synthetic provider test? A network provider may charge for one request."
	case "apply":
		return "Run one strict synthetic provider test and apply current settings at the next safe boundary?"
	case "mode":
		return "Save the selected strategy and preset, run a strict synthetic test, and apply them?"
	case "revert":
		return "Revert to the last-known-good settings revision?"
	case "profile":
		return "Select this repository profile?"
	case "experiment":
		return "Run one strict synthetic provider test and start this bounded experiment?"
	case "cancel_experiment":
		return "Cancel the active experiment and queue its baseline revert?"
	case "save":
		return "Save this draft without testing or applying it?"
	default:
		return "Confirm selected action?"
	}
}

// RunAccessibleMode presents strategy and preset as one first-class product
// choice. Advanced intent knobs remain in Advanced settings.
func RunAccessibleMode(ctx context.Context, draft map[string]string, input io.Reader, output io.Writer) (AccessibleValues, error) {
	return runAccessibleMode(ctx, draft, nil, input, output)
}

func runAccessibleMode(ctx context.Context, draft map[string]string, fields []FieldValue, input io.Reader, output io.Writer) (AccessibleValues, error) {
	values := finalizeAccessibleValues(newAccessibleValues(draft))
	strategy := fallback(values.Values["commit.strategy"], "intent")
	preset := fallback(values.Values["commit.preset"], "balanced")
	form := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Key("commit.strategy").Title(
			accessibleRetainedValueTitle("Commit strategy", strategy)).Options(
			huh.NewOption("Intent — semantic atomic commits", "intent"),
			huh.NewOption("Event — one capture per commit", "event"),
		).Value(&strategy),
		huh.NewSelect[string]().Key("commit.preset").Title(
			accessibleRetainedValueTitle("Preset", preset)).Options(
			huh.NewOption("Balanced (recommended)", "balanced"),
			huh.NewOption("Fast", "fast"),
			huh.NewOption("Quality", "quality"),
		).Value(&preset),
	)).WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	if err := form.RunWithContext(ctx); err != nil {
		return AccessibleValues{}, err
	}
	strategy = safeText(strings.ToLower(strategy))
	preset = safeText(strings.ToLower(preset))
	applyAccessibleModeSelection(&values, fields, strategy, preset)
	values.Action = "apply"
	return values, nil
}

func applyAccessibleModeSelection(values *AccessibleValues, fields []FieldValue, strategy, preset string) {
	applyPresetSwitch(values.Values, fields, strategy, preset)
}

func RunAccessibleProfile(ctx context.Context, input io.Reader, output io.Writer) (string, error) {
	profile := ""
	form := huh.NewForm(huh.NewGroup(huh.NewInput().Key("profile").Title("Repository profile name").Value(&profile))).
		WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	if err := form.RunWithContext(ctx); err != nil {
		return "", err
	}
	return safeText(profile), nil
}

func RunAccessibleExperimentOptions(ctx context.Context, input io.Reader, output io.Writer) (string, string, string, error) {
	budget, expiry, policy := "10", "none", "continue"
	form := huh.NewForm(huh.NewGroup(
		huh.NewInput().Key("experiment_budget").Title(accessibleRetainedValueTitle(
			"Experiment window budget (1-1000)", budget)).Value(&budget),
		huh.NewInput().Key("experiment_expiry").Title(accessibleRetainedValueTitle(
			"Experiment expiry (none, 15m, or 1h)", expiry)).Value(&expiry),
		huh.NewSelect[string]().Key("experiment_policy").Title(accessibleRetainedValueTitle(
			"Experiment failure policy", policy)).Options(
			huh.NewOption("Continue until budget/expiry", "continue"),
			huh.NewOption("Revert on provider failure", "revert"),
		).Value(&policy),
	)).WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	if err := form.RunWithContext(ctx); err != nil {
		return "", "", "", err
	}
	return safeText(budget), safeText(strings.ToLower(expiry)), safeText(strings.ToLower(policy)), nil
}

func AccessibleQuickForm(draft map[string]string, input io.Reader, output io.Writer) (*huh.Form, *AccessibleValues) {
	values := newAccessibleValues(draft)
	values.Action = "test"
	fields := accessibleInputFields(values, func(desc FieldDescriptor) bool { return quickProviderFields[desc.Key] })
	fields = append(fields, huh.NewSelect[string]().Key("action").Title("After quick setup").Options(
		huh.NewOption("Run strict synthetic test (recommended)", "test"),
		huh.NewOption("Save draft only", "save"),
		huh.NewOption("Test and apply at the next safe boundary", "apply"),
	).Value(&values.Action))
	form := huh.NewForm(huh.NewGroup(fields...)).WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	return form, values
}

func RunAccessibleQuick(ctx context.Context, draft map[string]string, input io.Reader, output io.Writer) (AccessibleValues, error) {
	form, values := AccessibleQuickForm(draft, input, output)
	if err := form.RunWithContext(ctx); err != nil {
		return AccessibleValues{}, err
	}
	return finalizeAccessibleValues(values), nil
}

func AccessibleForm(draft map[string]string, input io.Reader, output io.Writer) (*huh.Form, *AccessibleValues) {
	values := newAccessibleValues(draft)
	var fields = accessibleInputFields(values, func(FieldDescriptor) bool { return true })
	fields = append(fields,
		huh.NewInput().Key("profile").Title("Repository profile to select (blank keeps current)").Value(&values.Profile),
		huh.NewInput().Key("experiment_budget").Title(accessibleRetainedValueTitle(
			"Experiment window budget (1-1000)", values.ExperimentBudget)).Value(&values.ExperimentBudget),
		huh.NewInput().Key("experiment_expiry").Title(accessibleRetainedValueTitle(
			"Experiment expiry (none, 15m, or 1h)", values.ExperimentExpiry)).Value(&values.ExperimentExpiry),
		huh.NewSelect[string]().Key("experiment_policy").Title(accessibleRetainedValueTitle(
			"Experiment failure policy", values.ExperimentPolicy)).Options(
			huh.NewOption("Continue until budget/expiry", "continue"), huh.NewOption("Revert on provider failure", "revert")).Value(&values.ExperimentPolicy),
		huh.NewSelect[string]().Key("action").Title(accessibleRetainedValueTitle("Next action", "save draft only")).Options(
			huh.NewOption("Save draft only", "save"),
			huh.NewOption("Run strict synthetic test", "test"),
			huh.NewOption("Apply tested draft at next safe boundary", "apply"),
			huh.NewOption("Revert to last known good", "revert"),
			huh.NewOption("Select repository profile", "profile"),
			huh.NewOption("Start bounded experiment", "experiment"),
			huh.NewOption("Cancel active experiment and revert", "cancel_experiment")).Value(&values.Action))
	form := huh.NewForm(huh.NewGroup(fields...)).WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	return form, values
}

func newAccessibleValues(draft map[string]string) *AccessibleValues {
	values := &AccessibleValues{Values: map[string]string{}, raw: map[string]*string{},
		Action: "save", ExperimentBudget: "10", ExperimentExpiry: "none", ExperimentPolicy: "continue"}
	for _, desc := range Fields() {
		if !desc.Sensitive {
			values.Values[desc.Key] = safeText(draft[desc.Key])
		}
	}
	return values
}

func accessibleInputFields(values *AccessibleValues, include func(FieldDescriptor) bool) []huh.Field {
	var fields []huh.Field
	for _, desc := range Fields() {
		if desc.Sensitive || !include(desc) {
			continue
		}
		value := values.Values[desc.Key]
		values.raw[desc.Key] = &value
		fields = append(fields, huh.NewInput().Key(desc.Key).Title(accessibleRetainedValueTitle(
			desc.Label+" ("+desc.Apply+")", value)).Value(&value))
	}
	return fields
}

func accessibleRetainedValueTitle(title, value string) string {
	return fmt.Sprintf("%s [current: %s; Enter keeps current]", safeText(title), fallback(safeText(value), "inherit"))
}

// AccessibleTranscript is the stable, redraw-free description screen readers
// receive before Huh begins its linear prompts.
func AccessibleTranscript(draft map[string]string) string {
	var lines []string
	lines = append(lines, "ACD SETTINGS - accessible mode")
	for _, desc := range Fields() {
		if desc.Sensitive {
			lines = append(lines, desc.Label+": [set/unset only; value never displayed]")
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: %s (apply: %s)", desc.Label, fallback(safeText(draft[desc.Key]), "inherit"), desc.Apply))
	}
	lines = append(lines,
		"Next action: save, run a strict synthetic test, apply at the next safe boundary, revert, select profile, start a bounded experiment, or cancel and revert it.",
		"Experiment controls: window budget 1-1000, optional expiry, failure policy, progress, cancellation, and descriptive comparison.",
		"Confirmation: a provider test may make one paid synthetic request and never sends repository content.",
	)
	return strings.Join(lines, "\n")
}

func AccessibleStartTranscript(draft map[string]string) string {
	lines := []string{"ACD SETTINGS - accessible mode", "Current provider setup:"}
	for _, desc := range Fields() {
		if desc.Sensitive {
			if desc.Key == "ai.api_key" {
				lines = append(lines, "API key: [set/unset only; value never displayed]")
			}
			continue
		}
		if quickProviderFields[desc.Key] {
			lines = append(lines, fmt.Sprintf("%s: %s", desc.Label, fallback(safeText(draft[desc.Key]), "inherit")))
		}
	}
	lines = append(lines,
		"Choose Test current settings to skip editing and run one strict synthetic probe.",
		"Change strategy or preset is the first-class mode setup. Quick provider follows it; Advanced settings opens the full catalog.",
		"Credentials come from ACD_AI_API_KEY or the protected file managed by acd auth; repository content is never included in the synthetic test.",
	)
	return strings.Join(lines, "\n")
}

func RunAccessible(ctx context.Context, draft map[string]string, input io.Reader, output io.Writer) (AccessibleValues, error) {
	form, values := AccessibleForm(draft, input, output)
	if err := form.RunWithContext(ctx); err != nil {
		return AccessibleValues{}, err
	}
	confirmed, err := ConfirmAccessibleAction(ctx, values.Action, input, output)
	if err != nil {
		return AccessibleValues{}, err
	}
	if !confirmed {
		return AccessibleValues{}, fmt.Errorf("action not confirmed")
	}
	values.Confirm = true
	return finalizeAccessibleValues(values), nil
}

func finalizeAccessibleValues(values *AccessibleValues) AccessibleValues {
	for key, value := range values.raw {
		values.Values[key] = *value
	}
	values.Action = safeText(strings.ToLower(values.Action))
	values.Profile = safeText(values.Profile)
	values.ExperimentBudget = safeText(values.ExperimentBudget)
	values.ExperimentExpiry = safeText(strings.ToLower(values.ExperimentExpiry))
	values.ExperimentPolicy = safeText(strings.ToLower(values.ExperimentPolicy))
	values.Values = sanitizedDraft(values.Values)
	return *values
}

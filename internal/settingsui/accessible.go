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

func AccessibleForm(draft map[string]string, input io.Reader, output io.Writer) (*huh.Form, *AccessibleValues) {
	values := &AccessibleValues{Values: map[string]string{}, raw: map[string]*string{},
		Action: "save", ExperimentBudget: "10", ExperimentExpiry: "none", ExperimentPolicy: "continue"}
	var fields []huh.Field
	for _, desc := range Fields() {
		if desc.Sensitive {
			continue
		}
		value := safeText(draft[desc.Key])
		values.Values[desc.Key] = value
		values.raw[desc.Key] = &value
		fields = append(fields, huh.NewInput().Key(desc.Key).Title(accessibleRetainedValueTitle(
			desc.Label+" ("+desc.Apply+")", value)).Value(&value))
	}
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
	fields = append(fields, huh.NewConfirm().Key("confirm").Title(
		"Confirm selected action? Provider tests may make one paid synthetic request. (Enter keeps no)").Value(&values.Confirm))
	form := huh.NewForm(huh.NewGroup(fields...)).WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	return form, values
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

func RunAccessible(ctx context.Context, draft map[string]string, input io.Reader, output io.Writer) (AccessibleValues, error) {
	form, values := AccessibleForm(draft, input, output)
	if err := form.RunWithContext(ctx); err != nil {
		return AccessibleValues{}, err
	}
	if !values.Confirm {
		return AccessibleValues{}, fmt.Errorf("action not confirmed")
	}
	for key, value := range values.raw {
		values.Values[key] = *value
	}
	values.Action = safeText(strings.ToLower(values.Action))
	values.Profile = safeText(values.Profile)
	values.ExperimentBudget = safeText(values.ExperimentBudget)
	values.ExperimentExpiry = safeText(strings.ToLower(values.ExperimentExpiry))
	values.ExperimentPolicy = safeText(strings.ToLower(values.ExperimentPolicy))
	values.Values = sanitizedDraft(values.Values)
	return *values, nil
}

package settingsui

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	tea "charm.land/bubbletea/v2"
)

type Options struct {
	Input      io.Reader
	Output     io.Writer
	Accessible bool
	NoColor    bool
}

func Run(backend Backend, opts Options) error {
	if opts.Accessible {
		return runAccessibleBackend(context.Background(), backend, opts)
	}
	m := New(backend)
	m.noColor = opts.NoColor
	var programOpts []tea.ProgramOption
	if opts.Input != nil {
		programOpts = append(programOpts, tea.WithInput(opts.Input))
	}
	if opts.Output != nil {
		programOpts = append(programOpts, tea.WithOutput(opts.Output))
	}
	_, err := tea.NewProgram(m, programOpts...).Run()
	return err
}

func runAccessibleBackend(ctx context.Context, backend Backend, opts Options) error {
	if backend == nil {
		return fmt.Errorf("settings backend unavailable")
	}
	in, out := opts.Input, opts.Output
	if in == nil {
		in = os.Stdin
	}
	if out == nil {
		out = os.Stdout
	}
	snapshot, err := backend.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("settings snapshot: %s", safeText(err.Error()))
	}
	draft := map[string]string{}
	for _, field := range snapshot.Fields {
		if !descriptor(field.Key).Sensitive {
			draft[field.Key] = field.Value
		}
	}
	if _, err := fmt.Fprintln(out, AccessibleTranscript(draft)); err != nil {
		return err
	}
	if snapshot.Experiment.Active {
		if _, err := fmt.Fprintf(out, "Experiment progress: %d/%d windows; expiry %s; policy %s\n", snapshot.Experiment.CompletedWindows,
			snapshot.Experiment.TotalWindows, snapshot.Experiment.ExpiresAt.Format(time.RFC3339), safeText(snapshot.Experiment.FailurePolicy)); err != nil {
			return err
		}
	}
	if snapshot.Comparison != "" {
		if _, err := fmt.Fprintln(out, "Descriptive comparison:", safeText(snapshot.Comparison)); err != nil {
			return err
		}
	}
	values, err := RunAccessible(ctx, draft, in, out)
	if err != nil {
		return fmt.Errorf("settings accessible: %s", safeText(err.Error()))
	}
	return dispatchAccessible(ctx, backend, values, out)
}

func dispatchAccessible(ctx context.Context, backend Backend, values AccessibleValues, out io.Writer) error {
	switch values.Action {
	case "save":
		result, err := backend.Save(ctx, values.Values)
		if err != nil {
			return fmt.Errorf("settings save: %s", safeText(err.Error()))
		}
		_, err = fmt.Fprintln(out, "SAVED:", safeText(result.Summary))
		return err
	case "test":
		result, err := backend.Test(ctx, values.Values)
		if err != nil {
			return fmt.Errorf("settings test: %s", safeText(err.Error()))
		}
		_, err = fmt.Fprintln(out, "TESTED:", safeText(result.Summary))
		return err
	case "apply":
		tested, err := backend.Test(ctx, values.Values)
		if err != nil {
			return fmt.Errorf("settings test: %s", safeText(err.Error()))
		}
		result, err := backend.Apply(ctx, values.Values, tested.Fingerprint)
		if err != nil {
			return fmt.Errorf("settings apply: %s", safeText(err.Error()))
		}
		label := "ACTIVE:"
		if result.Queued {
			label = "QUEUED:"
		}
		_, err = fmt.Fprintln(out, label, safeText(result.Summary))
		return err
	case "revert":
		result, err := backend.Revert(ctx, 0)
		if err != nil {
			return fmt.Errorf("settings revert: %s", safeText(err.Error()))
		}
		_, err = fmt.Fprintln(out, "QUEUED:", safeText(result.Summary))
		return err
	case "profile":
		result, err := backend.SelectProfile(ctx, values.Profile)
		if err != nil {
			return fmt.Errorf("settings profile: %s", safeText(err.Error()))
		}
		_, err = fmt.Fprintln(out, "SAVED:", safeText(result.Summary))
		return err
	case "experiment":
		budget, err := strconv.Atoi(values.ExperimentBudget)
		if err != nil {
			return fmt.Errorf("settings experiment: invalid window budget")
		}
		var expiry time.Duration
		switch values.ExperimentExpiry {
		case "", "none":
		case "15m":
			expiry = 15 * time.Minute
		case "1h":
			expiry = time.Hour
		default:
			return fmt.Errorf("settings experiment: invalid expiry")
		}
		result, err := backend.StartExperiment(ctx, values.Values, ExperimentOptions{WindowBudget: budget, ExpiresAfter: expiry, FailurePolicy: values.ExperimentPolicy})
		if err != nil {
			return fmt.Errorf("settings experiment: %s", safeText(err.Error()))
		}
		_, err = fmt.Fprintf(out, "QUEUED: experiment %d, 0/%d windows, policy %s\n", result.ID, result.TotalWindows, safeText(result.FailurePolicy))
		return err
	case "cancel_experiment":
		snapshot, err := backend.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("settings experiment: %s", safeText(err.Error()))
		}
		if !snapshot.Experiment.Active {
			return fmt.Errorf("settings experiment: no active experiment")
		}
		result, err := backend.CancelExperiment(ctx, snapshot.Experiment.ID)
		if err != nil {
			return fmt.Errorf("settings experiment: %s", safeText(err.Error()))
		}
		_, err = fmt.Fprintln(out, "QUEUED:", safeText(result.Summary), safeText(snapshot.Comparison))
		return err
	default:
		return fmt.Errorf("settings accessible: unsupported action")
	}
}

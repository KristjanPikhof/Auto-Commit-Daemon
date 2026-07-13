package settingsui

import (
	"context"
	"fmt"
	"io"
	"os"

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
	default:
		return fmt.Errorf("settings accessible: unsupported action")
	}
}

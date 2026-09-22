package cli

import (
	"context"
	"io"
	"os"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

type settingsCLIService interface {
	Snapshot(context.Context, settings.Scope, string) (settings.Snapshot, error)
	Save(context.Context, settings.SaveRequest) (settings.SaveResult, error)
	Validate(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.Validation, error)
	TestProvider(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.ProviderTestResult, error)
	Apply(context.Context, settings.ApplyRequest) (settings.ApplyResult, error)
	Close() error
}

var (
	settingsInputTTY = func(r io.Reader) bool {
		f, ok := r.(*os.File)
		return ok && (isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd()))
	}
	settingsOutputTTY = func(w io.Writer) bool {
		f, ok := w.(interface{ Fd() uintptr })
		return ok && (isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd()))
	}
)

func newSettingsCmd() *cobra.Command {
	cmd := newConfigEditorCmd()
	cmd.Use = "settings"
	return cmd
}

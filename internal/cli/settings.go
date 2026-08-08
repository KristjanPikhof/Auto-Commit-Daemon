package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
)

type settingsCLIService interface {
	settingsui.SettingsService
	Close() error
}

var (
	openSettingsCLIService = func(ctx context.Context, opts settings.Options) (settingsCLIService, error) {
		return settings.NewService(ctx, opts)
	}
	runSettingsUI    = settingsui.Run
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
	var (
		profile                    string
		globalScope                bool
		accessible                 bool
		confirmEndpointCredentials bool
		confirmSubprocess          bool
		confirmDiffEgress          bool
	)
	cmd := &cobra.Command{
		Use:   "settings",
		Short: "Inspect, test, and safely activate configuration",
		Long: `Open the Go-native settings lab for the current repository.

The lab shows active and draft values, their source and shadowed environment,
and whether each setting activates at the next safe work boundary or requires a
restart. Provider tests use synthetic content only. API keys come from the
environment or protected file and are displayed only by source, never value.

Repository scope is the default. --profile edits or creates one named profile;
--global saves global defaults without changing running repositories. Rich mode
requires an interactive terminal. --accessible, or TERM=dumb, uses linear
screen-reader-friendly prompts without full-screen redraws.`,
		Example: `  acd settings
  acd settings --repo /path/to/repo
  acd settings --profile fast
  acd settings --global
  acd settings --accessible
  acd settings --confirm-endpoint-credentials
  acd settings --confirm-subprocess --confirm-diff-egress`,
		Args: cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			jsonOut, _ := c.Flags().GetBool("json")
			if jsonOut {
				return errors.New("acd settings: interactive settings do not support --json")
			}
			if globalScope && c.Flags().Changed("repo") {
				return errors.New("acd settings: --global conflicts with explicit --repo")
			}
			if globalScope && strings.TrimSpace(profile) != "" {
				return errors.New("acd settings: --global conflicts with --profile")
			}
			if strings.TrimSpace(profile) != profile || strings.ContainsAny(profile, "\r\n\x1b") {
				return errors.New("acd settings: profile name contains unsafe text")
			}
			useAccessible := accessible || strings.EqualFold(os.Getenv("TERM"), "dumb")
			if !useAccessible && (!settingsInputTTY(c.InOrStdin()) || !settingsOutputTTY(c.OutOrStdout())) {
				return errors.New("acd settings: rich mode requires interactive stdin and stdout; use --accessible")
			}

			scope := settings.ScopeRepository
			if globalScope {
				scope = settings.ScopeGlobal
			} else if profile != "" {
				scope = settings.ScopeProfile
			}
			repo, err := resolveRepo(repoFlag)
			if err != nil {
				return fmt.Errorf("acd settings: %w", err)
			}
			roots, err := paths.Resolve()
			if err != nil {
				return fmt.Errorf("acd settings: resolve paths: %w", err)
			}
			service, err := openSettingsCLIService(c.Context(), settings.Options{Roots: roots, RepoPath: repo})
			if err != nil {
				return err
			}
			defer service.Close()
			var confirmations []ai.ConfirmationRequirement
			if confirmEndpointCredentials {
				confirmations = append(confirmations, ai.ConfirmationEndpointCredentials)
			}
			if confirmSubprocess {
				confirmations = append(confirmations, ai.ConfirmationSubprocessExecution)
			}
			if confirmDiffEgress {
				confirmations = append(confirmations, ai.ConfirmationDiffEgress)
			}
			backend := settingsui.NewServiceBackend(service, settingsui.BackendAdapterOptions{
				Scope: scope, Profile: profile, Confirmations: confirmations,
			})
			return runSettingsUI(backend, settingsui.Options{Input: c.InOrStdin(), Output: c.OutOrStdout(),
				Accessible: useAccessible, NoColor: os.Getenv("NO_COLOR") != ""})
		},
	}
	cmd.Flags().StringVar(&profile, "profile", "", "Edit or create a named profile")
	cmd.Flags().BoolVar(&globalScope, "global", false, "Edit global defaults without activating running repos")
	cmd.Flags().BoolVar(&accessible, "accessible", false, "Use linear screen-reader-friendly prompts")
	cmd.Flags().BoolVar(&confirmEndpointCredentials, "confirm-endpoint-credentials", false, "Confirm credentials may be sent to a non-default endpoint")
	cmd.Flags().BoolVar(&confirmSubprocess, "confirm-subprocess", false, "Confirm execution of the configured provider subprocess")
	cmd.Flags().BoolVar(&confirmDiffEgress, "confirm-diff-egress", false, "Confirm redacted repository diff egress")
	return cmd
}

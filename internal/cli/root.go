package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

// ErrNoCommand is retained for source compatibility. Bare acd now renders a
// read-only health summary and no longer returns this error.
var ErrNoCommand = errors.New("no command provided")

// Execute builds the root command tree and runs it.
func Execute() error {
	root := newRootCmd()
	var stdout *countingWriter
	if commandLineRequestsJSON(os.Args[1:]) {
		stdout = &countingWriter{Writer: os.Stdout}
		root.SetOut(stdout)
	} else {
		root.SetOut(os.Stdout)
	}
	root.SetErr(os.Stderr)
	_, err := root.ExecuteC()
	if err == nil {
		return nil
	}
	err = classifyCobraError(err)
	jsonOut, _ := root.Flags().GetBool("json")
	if !jsonOut || (stdout != nil && stdout.BytesWritten() != 0) {
		return err
	}
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		commandErr = &CommandError{Code: "internal_error", Message: err.Error(), Exit: ExitInternal}
		err = commandErr
	}
	envelope := productEnvelope{
		OK: false, State: productStateNeedsAction, Actions: []productAction{}, Data: map[string]any{},
		Error: &productError{Code: commandErr.Code, Message: commandErr.Message,
			Retryable: commandErr.Retryable, Details: commandErr.Details},
	}
	if renderErr := renderJSONEnvelope(os.Stdout, envelope); renderErr != nil {
		return commandError(renderErr, "output_error", ExitInternal, false)
	}
	commandErr.rendered = true
	return err
}

func commandLineRequestsJSON(args []string) bool {
	for _, arg := range args {
		if arg == "--json" || arg == "--json=true" {
			return true
		}
	}
	return false
}

type countingWriter struct {
	io.Writer
	written int64
}

func (w *countingWriter) Fd() uintptr {
	if file, ok := w.Writer.(interface{ Fd() uintptr }); ok {
		return file.Fd()
	}
	return ^uintptr(0)
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.written += int64(n)
	return n, err
}

func (w *countingWriter) BytesWritten() int64 { return w.written }

type invocationContextKey struct{}

type invocation struct {
	Repo     string
	JSON     bool
	Quiet    bool
	LogLevel slog.Level
}

func invocationFromContext(ctx context.Context) invocation {
	value, _ := ctx.Value(invocationContextKey{}).(invocation)
	return value
}

func newRootCmd() *cobra.Command {
	var (
		repoPath string
		jsonOut  bool
		quiet    bool
		logLevel string
	)

	cmd := &cobra.Command{
		Use:           "acd",
		Short:         "Durable checkpoints first, clean Git commits second",
		Version:       version.String(),
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(c *cobra.Command, args []string) error {
			return runControlStatus(c.Context(), c.OutOrStdout(), repoPath, jsonOut)
		},
		PersistentPreRunE: func(c *cobra.Command, _ []string) error {
			level, err := parseLogLevel(logLevel)
			if err != nil {
				return invalidCommandError("acd: %v", err)
			}
			if jsonOut && commandRejectsJSON(c) {
				return invalidCommandError("%s: --json is not supported for interactive commands", c.CommandPath())
			}
			if err := validateInvocationCapabilities(c); err != nil {
				return err
			}
			ctx := context.WithValue(c.Context(), invocationContextKey{}, invocation{
				Repo: repoPath, JSON: jsonOut, Quiet: quiet, LogLevel: level,
			})
			c.SetContext(ctx)
			slog.SetDefault(slog.New(slog.NewTextHandler(c.ErrOrStderr(), &slog.HandlerOptions{Level: level})))
			return nil
		},
	}
	cmd.CompletionOptions.HiddenDefaultCmd = true
	cmd.SetHelpTemplate(rootHelpTemplate)
	cmd.SetVersionTemplate("acd version {{.Version}}\n")

	pf := cmd.PersistentFlags()
	pf.StringVar(&repoPath, "repo", "", "Repo root (default: cwd)")
	pf.BoolVar(&jsonOut, "json", false, "Emit JSON output")
	pf.BoolVar(&quiet, "quiet", false, "Suppress non-essential output")
	pf.StringVar(&logLevel, "log-level", "info", "debug|info|warn|error")

	repo := newProductRepoNamespaceCmd()
	repo.Hidden = true
	cmd.AddCommand(
		newSetupCmd(),
		newProductStatusCmd(),
		newOnCmd(),
		newOffCmd(),
		newHistoryCmd(),
		newRestoreCmd(),
		newProductDoctorCmd(),
		newUninstallCmd(),
		newConfigNamespaceCmd(),
		newSupportNamespaceCmd(),
		repo,
		newInternalCmd(),
	)

	cmd.AddCommand(
		hideCompatibility(newSetupInitCompatCmd(), "acd setup", false),
		hideCompatibility(newConfigureCmd(), "acd config edit", false),
		hideCompatibility(newSettingsCmd(), "acd config edit", false),
		hideCompatibility(newAuthCmd(), "acd config credentials", false),
		hideCompatibility(newVersionCmd(), "acd --version", false),
		hideCompatibility(newCompatStartCmd(), "the managed ACD supervisor", true),
		hideCompatibility(newCompatStopCmd(), "the managed ACD supervisor", true),
		hideCompatibility(newCompatHintCmd("wake", "wake"), "the managed ACD supervisor", true),
		hideCompatibility(newCompatHintCmd("touch", "soft_boundary"), "the managed ACD supervisor", true),
		hideCompatibility(newCompatHintCmd("flush", "logical_boundary"), "the managed ACD supervisor", true),
		hideCompatibility(newEventsCmd(), "acd history --activity", false),
		hideCompatibility(newPromptCmd(), "acd support diagnose", false),
		hideCompatibility(newExplainCmd(), "acd history explain", false),
		hideCompatibility(newFixCmd(), "acd support repair", false),
		hideCompatibility(newLogsCmd(), "acd support logs", false),
		hideCompatibility(newListCmd(), "acd repo list", false),
		hideCompatibility(newStatsCmd(), "acd repo list", false),
		hideCompatibility(newDiagnoseCmd(), "acd support diagnose", false),
		hideCompatibility(newRecoverCmd(), "acd support repair", false),
		hideCompatibility(newPauseCmd(), "acd off", false),
		hideCompatibility(newResumeCmd(), "acd on", false),
		hideCompatibility(newPurgeEventsCmd(), "acd repo gc", false),
		hideCompatibility(newHookStdinExtractCmd(), "acd internal integration stdin-extract", true),
		hideCompatibility(newHookCursorExtractCmd(), "acd internal integration cursor-extract", true),
		hideCompatibility(newGCCmd(), "acd repo gc", false),
		hideCompatibility(newCommitAllCmd(), "acd on", false),
		hideCompatibility(newRewriteCommitsCmd(), "acd history rewrite", false),
		hideCompatibility(newCompatDaemonCmd(), "acd internal worker run", true),
	)

	return cmd
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("invalid --log-level %q (want debug, info, warn, or error)", value)
	}
}

func commandRejectsJSON(command *cobra.Command) bool {
	path := command.CommandPath()
	return path == "acd settings" || path == "acd config edit" || path == "acd config advanced"
}

func validateInvocationCapabilities(command *cobra.Command) error {
	path := command.CommandPath()
	if flagWasSet(command, "repo") {
		switch path {
		case "acd uninstall", "acd repo list", "acd repo gc", "acd config credentials":
			return invalidCommandError("%s: --repo is not supported by this global operation", path)
		}
	}
	return nil
}

func flagWasSet(command *cobra.Command, name string) bool {
	if flag := command.Flags().Lookup(name); flag != nil && flag.Changed {
		return true
	}
	if flag := command.InheritedFlags().Lookup(name); flag != nil && flag.Changed {
		return true
	}
	return false
}

func classifyCobraError(err error) error {
	var commandErr *CommandError
	if errors.As(err, &commandErr) {
		return err
	}
	message := err.Error()
	invalidFragments := []string{
		"unknown command", "unknown flag", "required flag", "accepts ",
		"requires at least", "requires exactly", "invalid argument",
	}
	for _, fragment := range invalidFragments {
		if strings.Contains(message, fragment) {
			return commandError(err, "invalid_command", ExitInvalid, false)
		}
	}
	return commandError(err, "internal_error", ExitInternal, false)
}

var _ io.Writer = (*countingWriter)(nil)

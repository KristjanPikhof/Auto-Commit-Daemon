package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
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
	requestedJSON := commandLineRequestsJSON(os.Args[1:])
	if requestedJSON {
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
	if !requestedJSON || (stdout != nil && stdout.BytesWritten() != 0) {
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
		if arg == "--" {
			return false
		}
		if arg == "--json" {
			return true
		}
		value, found := strings.CutPrefix(arg, "--json=")
		if found {
			enabled, err := strconv.ParseBool(value)
			if err == nil && enabled {
				return true
			}
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

type commandCapabilities struct {
	Repository      bool
	JSON            bool
	Quiet           bool
	Interactive     bool
	Streaming       bool
	JSONInteractive bool
	JSONStreaming   bool
}

const capabilityAnnotationPrefix = "acd.capability."

func withInvocationCapabilities(command *cobra.Command, capabilities commandCapabilities) *cobra.Command {
	if command.Annotations == nil {
		command.Annotations = map[string]string{}
	}
	values := map[string]bool{
		"repository": capabilities.Repository, "json": capabilities.JSON,
		"quiet": capabilities.Quiet, "interactive": capabilities.Interactive,
		"streaming": capabilities.Streaming, "json_interactive": capabilities.JSONInteractive,
		"json_streaming": capabilities.JSONStreaming,
	}
	for name, enabled := range values {
		command.Annotations[capabilityAnnotationPrefix+name] = strconv.FormatBool(enabled)
	}
	return command
}

func invocationCapabilities(command *cobra.Command) commandCapabilities {
	var annotations map[string]string
	for current := command; current != nil; current = current.Parent() {
		if current == command.Root() && current != command {
			break
		}
		if _, declared := current.Annotations[capabilityAnnotationPrefix+"json"]; declared {
			annotations = current.Annotations
			break
		}
	}
	if annotations == nil {
		return commandCapabilities{}
	}
	enabled := func(name string) bool {
		value, _ := strconv.ParseBool(annotations[capabilityAnnotationPrefix+name])
		return value
	}
	return commandCapabilities{
		Repository: enabled("repository"), JSON: enabled("json"), Quiet: enabled("quiet"),
		Interactive: enabled("interactive"), Streaming: enabled("streaming"),
		JSONInteractive: enabled("json_interactive"), JSONStreaming: enabled("json_streaming"),
	}
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
		Short:         "Protect your work and publish clear local Git commits",
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
	withInvocationCapabilities(cmd, commandCapabilities{Repository: true, JSON: true, Quiet: true})
	cmd.CompletionOptions.HiddenDefaultCmd = true
	cmd.SetHelpTemplate(rootHelpTemplate)
	cmd.SetVersionTemplate("acd version {{.Version}}\n")

	pf := cmd.PersistentFlags()
	pf.StringVar(&repoPath, "repo", "", "Repository to use (default: current directory)")
	pf.BoolVar(&jsonOut, "json", false, "Print machine-readable JSON")
	pf.BoolVar(&quiet, "quiet", false, "Hide progress and other optional output")
	pf.StringVar(&logLevel, "log-level", "info", "Log detail: debug, info, warn, or error")

	repo := newProductRepoNamespaceCmd()
	repo.Hidden = true
	cmd.AddCommand(
		withInvocationCapabilities(newSetupCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true, Interactive: true}),
		withInvocationCapabilities(newProductStatusCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newOnCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newOffCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newProductListCmd(), commandCapabilities{JSON: true, Quiet: true, Interactive: true, Streaming: true}),
		withInvocationCapabilities(newProductCommitAllCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newHistoryCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newRestoreCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newProductDoctorCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newUninstallCmd(), commandCapabilities{JSON: true, Quiet: true, Interactive: true}),
		newConfigNamespaceCmd(),
		newSupportNamespaceCmd(),
		repo,
		newInternalCmd(),
	)

	cmd.AddCommand(
		hideCompatibility(withInvocationCapabilities(newSetupInitCompatCmd(),
			commandCapabilities{Repository: true, JSON: true, Quiet: true}), "acd setup", false),
		hideCompatibility(withInvocationCapabilities(newConfigureCmd(),
			commandCapabilities{Repository: true, JSON: true, Quiet: true, Interactive: true}), "acd config edit", false),
		hideCompatibility(withInvocationCapabilities(newSettingsCmd(),
			commandCapabilities{Repository: true, Quiet: true, Interactive: true}), "acd config edit", false),
		hideCompatibility(renameCompatibility(newConfigCredentialsCmd(), "auth"), "acd config credentials", false),
		hideCompatibility(withInvocationCapabilities(newVersionCmd(), commandCapabilities{Quiet: true}), "acd --version", false),
		hideCompatibility(withInvocationCapabilities(newCompatStartCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "the managed ACD supervisor", true),
		hideCompatibility(withInvocationCapabilities(newCompatStopCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "the managed ACD supervisor", true),
		hideCompatibility(withInvocationCapabilities(newCompatHintCmd("wake", "wake"), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "the managed ACD supervisor", true),
		hideCompatibility(withInvocationCapabilities(newCompatHintCmd("touch", "soft_boundary"), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "the managed ACD supervisor", true),
		hideCompatibility(withInvocationCapabilities(newCompatHintCmd("flush", "logical_boundary"), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "the managed ACD supervisor", true),
		hideCompatibility(newEventsCompatibilityDelegate(), "acd history activity", false),
		hideCompatibility(renameCompatibility(newSupportPromptCmd(), "prompt"), "acd support prompt", false),
		hideCompatibility(renameCompatibility(newHistoryExplainCmd(), "explain"), "acd history explain", false),
		hideCompatibility(renameCompatibility(newSupportRecoverCmd(), "fix"), "acd support recover", false),
		hideCompatibility(newLogsCompatibilityDelegate(), "acd support logs", false),
		hideCompatibility(renameCompatibility(newRepoStatsCmd(), "stats"), "acd repo stats", false),
		hideCompatibility(renameCompatibility(newSupportDiagnoseCmd(), "diagnose"), "acd support diagnose", false),
		hideCompatibility(newRecoverCompatibilityDelegate(), "acd support recover", false),
		hideCompatibility(withInvocationCapabilities(newPauseCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "acd off", false),
		hideCompatibility(withInvocationCapabilities(newResumeCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "acd on", false),
		hideCompatibility(withInvocationCapabilities(newPurgeEventsCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "acd support recover", false),
		hideCompatibility(newHookStdinExtractCmd(), "acd internal integration stdin-extract", true),
		hideCompatibility(newHookCursorExtractCmd(), "acd internal integration cursor-extract", true),
		hideCompatibility(withInvocationCapabilities(newGCCmd(), commandCapabilities{JSON: true, Quiet: true, Interactive: true}), "acd repo gc", false),
		hideCompatibility(withInvocationCapabilities(newRewriteCommitsCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true, Interactive: true}), "acd history rewrite", false),
		newRewriteAllHintCmd(),
		hideCompatibility(withInvocationCapabilities(newCompatDaemonCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}), "acd internal worker run", true),
	)

	return cmd
}

func newRewriteAllHintCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "rewrite-all",
		Hidden: true,
		Short:  "Use acd history rewrite to improve commit messages",
		Long: `ACD does not rewrite all branch history automatically because that can
change commits you do not own or intend to edit.

Use acd history rewrite with an explicit selection. Start with --plan-only,
review the plan, preview the apply with --dry-run, and finish with --yes.`,
		Example: `  acd history rewrite --last 5 --plan-only
  acd history rewrite --apply <plan-id-or-file> --dry-run
  acd history rewrite --apply <plan-id-or-file> --yes`,
		Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return invalidCommandError("acd rewrite-all does not rewrite history automatically; run `acd history rewrite --help`")
		},
	}
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

func validateInvocationCapabilities(command *cobra.Command) error {
	capabilities := invocationCapabilities(command)
	path := command.CommandPath()
	jsonOut := boolFlagEnabled(command, "json")
	interactive := boolFlagEnabled(command, "interactive")
	streamingFlag := enabledFlag(command, "watch", "follow")
	streaming := streamingFlag != ""
	if flagWasSet(command, "repo") && !capabilities.Repository {
		return invalidCommandError("%s: --repo is not supported by this global operation", path)
	}
	if jsonOut && !capabilities.JSON {
		return invalidCommandError("%s: --json is not supported for this command", path)
	}
	if flagWasSet(command, "quiet") && !capabilities.Quiet {
		return invalidCommandError("%s: --quiet is not supported for this command", path)
	}
	if interactive && !capabilities.Interactive {
		return invalidCommandError("%s: --interactive is not supported for this command", path)
	}
	if streaming && !capabilities.Streaming {
		return invalidCommandError("%s: streaming output is not supported for this command", path)
	}
	if jsonOut && interactive && !capabilities.JSONInteractive {
		return invalidCommandError("%s: --json cannot be combined with interactive output", path)
	}
	if jsonOut && streaming && !capabilities.JSONStreaming {
		return invalidCommandError("%s: --%s does not support --json", path, streamingFlag)
	}
	return nil
}

func enabledFlag(command *cobra.Command, names ...string) string {
	for _, name := range names {
		if boolFlagEnabled(command, name) {
			return name
		}
	}
	return ""
}

func boolFlagEnabled(command *cobra.Command, name string) bool {
	flag := command.Flags().Lookup(name)
	if flag == nil {
		flag = command.InheritedFlags().Lookup(name)
	}
	if flag == nil {
		return false
	}
	value, err := strconv.ParseBool(flag.Value.String())
	return err == nil && value
}

func renameCompatibility(command *cobra.Command, use string) *cobra.Command {
	command.Use = use
	return command
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
		"unknown shorthand flag", "flag needs an argument", "requires an argument",
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

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type productDoctorSettings struct {
	Provider           string   `json:"provider"`
	ProviderSource     string   `json:"provider_source"`
	Strategy           string   `json:"strategy"`
	StrategySource     string   `json:"strategy_source"`
	Preset             string   `json:"preset"`
	Verification       string   `json:"verification"`
	RepairEnabled      bool     `json:"repair_enabled"`
	DiffEgressEnabled  bool     `json:"diff_egress_enabled"`
	ConfigurationNotes []string `json:"configuration_notes,omitempty"`
}

func newProductStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show whether this repository is protected and published",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runControlStatus(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
}

func newProductDoctorCmd() *cobra.Command {
	var bundle bool
	var output string
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Explain installation and protection problems",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			doctorCtx := withDoctorTarget(cmd.Context(), repo)
			if bundle {
				return runProductDoctorBundle(doctorCtx, cmd.OutOrStdout(), output, jsonOut)
			}
			status, err := inspectControl(doctorCtx, repo)
			if err != nil {
				return err
			}
			details := collectProductDoctorSettings(doctorCtx, status.Repo)
			envelope := envelopeFromControl(status)
			envelope.Data = map[string]any{"status": envelope.Data, "diagnostics": details}
			if jsonOut {
				if err := renderJSONEnvelope(cmd.OutOrStdout(), envelope); err != nil {
					return err
				}
			} else {
				if err := renderProductEnvelope(cmd.OutOrStdout(), envelopeFromControl(status), false); err != nil {
					return err
				}
				renderProductDoctorWorker(cmd.OutOrStdout(), status)
				renderProductDoctorSettings(cmd.OutOrStdout(), details)
			}
			if envelope.State == productStateNeedsAction {
				return &CommandError{Code: "needs_action", Message: status.Summary, Exit: ExitActionRequired, rendered: jsonOut}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&bundle, "bundle", false, "Write a sanitized support bundle")
	cmd.Flags().StringVar(&output, "output", "", "Override the support bundle output directory")
	return cmd
}

func renderProductDoctorWorker(out io.Writer, status controlResult) {
	if status.SupervisorWorkerState == "" {
		return
	}
	fmt.Fprintln(out, "\nWorker:")
	fmt.Fprintf(out, "  State: %s", status.SupervisorWorkerState)
	if status.SupervisorWorkerRestarts > 0 {
		fmt.Fprintf(out, " after %d restart attempt(s)", status.SupervisorWorkerRestarts)
	}
	fmt.Fprintln(out)
	if status.SupervisorWorkerError != "" {
		fmt.Fprintf(out, "  Cause: %s\n", status.SupervisorWorkerError)
	}
	if status.NextAction != "" && status.NextAction != "No action needed." {
		fmt.Fprintf(out, "  Fix: %s\n", status.NextAction)
	}
}

func collectProductDoctorSettings(ctx context.Context, repo string) productDoctorSettings {
	details := productDoctorSettings{ConfigurationNotes: []string{}}
	roots, err := paths.Resolve()
	if err != nil {
		details.ConfigurationNotes = append(details.ConfigurationNotes, err.Error())
		return details
	}
	service, err := settings.NewValidationService(ctx, settings.Options{Roots: roots, RepoPath: repo})
	if err != nil {
		details.ConfigurationNotes = append(details.ConfigurationNotes, err.Error())
		return details
	}
	defer service.Close()
	preview, err := service.AuthoringPreview()
	if err != nil {
		details.ConfigurationNotes = append(details.ConfigurationNotes, err.Error())
		return details
	}
	details.Provider = preview.Values[config.FieldProvider]
	details.ProviderSource = string(preview.Sources[config.FieldProvider])
	details.Strategy = preview.Values[config.FieldCommitStrategy]
	details.StrategySource = string(preview.Sources[config.FieldCommitStrategy])
	details.Preset = preview.Values[config.FieldCommitPreset]
	details.Verification = preview.Values[config.FieldIntentVerification]
	details.RepairEnabled = truthyConfig(preview.Values[config.FieldIntentRepairEnabled])
	details.DiffEgressEnabled = truthyConfig(preview.Values[config.FieldDiffEgress])
	return details
}

func renderProductDoctorSettings(out io.Writer, details productDoctorSettings) {
	fmt.Fprintln(out, "\nPublication settings:")
	fmt.Fprintf(out, "  Provider: %s (%s)\n", valueOrUnset(details.Provider), valueOrUnset(details.ProviderSource))
	fmt.Fprintf(out, "  Strategy: %s / %s (%s)\n", valueOrUnset(details.Strategy),
		valueOrUnset(details.Preset), valueOrUnset(details.StrategySource))
	fmt.Fprintf(out, "  Verification: %s; repair=%s; diff egress=%s\n",
		valueOrUnset(details.Verification), yesNo(details.RepairEnabled), yesNo(details.DiffEgressEnabled))
	for _, note := range details.ConfigurationNotes {
		fmt.Fprintf(out, "  Note: %s\n", note)
	}
	fmt.Fprintln(out, "More detail: acd support diagnose")
}

func runProductDiagnose(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	if !jsonOut {
		return runDiagnose(ctx, out, repo, false)
	}
	return renderAdvancedJSON(out, productStateProtected, func(raw io.Writer) error {
		return runDiagnose(ctx, raw, repo, true)
	})
}

func runProductDoctorBundle(ctx context.Context, out io.Writer, output string, jsonOut bool) error {
	if !jsonOut {
		return runDoctor(ctx, out, true, output, false)
	}
	return renderAdvancedJSON(out, productStateProtected, func(raw io.Writer) error {
		return runDoctor(ctx, raw, true, output, true)
	})
}

func runProductExplain(
	ctx context.Context,
	out io.Writer,
	repo, path, commit string,
	last bool,
	since int64,
	limit int,
	jsonOut bool,
) error {
	if !jsonOut {
		return runExplain(ctx, out, repo, path, commit, last, since, limit, false)
	}
	return renderAdvancedJSON(out, productStateProtected, func(raw io.Writer) error {
		return runExplain(ctx, raw, repo, path, commit, last, since, limit, true)
	})
}

func runProductRepoList(ctx context.Context, out io.Writer, jsonOut bool) error {
	if !jsonOut {
		return runRepoList(ctx, out, false)
	}
	return renderAdvancedJSON(out, productStateOff, func(raw io.Writer) error {
		return runRepoList(ctx, raw, true)
	})
}

func runProductEvents(
	ctx context.Context,
	out io.Writer,
	repo, path string,
	since int64,
	limit int,
	watch bool,
	interval time.Duration,
	jsonOut bool,
) error {
	if !jsonOut {
		return runEvents(ctx, out, repo, path, since, limit, watch, interval, false)
	}
	return renderAdvancedJSON(out, productStateProtected, func(raw io.Writer) error {
		return runEvents(ctx, raw, repo, path, since, limit, false, interval, true)
	})
}

func runProductPrompt(ctx context.Context, out io.Writer, repo string, last bool, seq int64, jsonOut bool) error {
	if !jsonOut {
		return runPrompt(ctx, out, repo, last, seq, false)
	}
	return renderAdvancedJSON(out, productStateProtected, func(raw io.Writer) error {
		return runPrompt(ctx, raw, repo, last, seq, true)
	})
}

func runProductStats(ctx context.Context, out io.Writer, since string, jsonOut bool) error {
	if !jsonOut {
		return runStats(ctx, out, since, false)
	}
	return renderAdvancedJSON(out, productStateOff, func(raw io.Writer) error {
		return runStats(ctx, raw, since, true)
	})
}

func runProductFix(
	ctx context.Context,
	out io.Writer,
	repo string,
	dryRun, yes, force, clearPause, jsonOut bool,
) error {
	if !jsonOut {
		return runFix(ctx, out, repo, dryRun, yes, force, clearPause, false)
	}
	return renderAdvancedJSON(out, productStateNeedsAction, func(raw io.Writer) error {
		return runFix(ctx, raw, repo, dryRun, yes, force, clearPause, true)
	})
}

func runProductLogs(ctx context.Context, out io.Writer, repo string, lines int, follow, jsonOut bool) error {
	if !jsonOut {
		return runLogs(ctx, out, repo, lines, follow)
	}
	var raw bytes.Buffer
	if err := runLogs(ctx, &raw, repo, lines, false); err != nil {
		return err
	}
	logLines := []string{}
	for _, line := range strings.Split(strings.TrimSuffix(raw.String(), "\n"), "\n") {
		if line != "" {
			logLines = append(logLines, line)
		}
	}
	return renderJSONEnvelope(out, productEnvelope{OK: true, State: productStateProtected,
		Actions: []productAction{}, Data: map[string]any{"lines": logLines}})
}

func renderAdvancedJSON(out io.Writer, stateName productState, render func(io.Writer) error) error {
	var raw bytes.Buffer
	if err := render(&raw); err != nil {
		return err
	}
	var data any = map[string]any{}
	if err := json.Unmarshal(raw.Bytes(), &data); err != nil {
		return fmt.Errorf("acd: decode advanced JSON result: %w", err)
	}
	return renderJSONEnvelope(out, productEnvelope{
		OK: true, State: stateName, Actions: []productAction{}, Data: data,
	})
}

type historyEntry struct {
	ID         string  `json:"id"`
	Sequence   int64   `json:"sequence"`
	Reason     string  `json:"reason"`
	Phase      string  `json:"phase"`
	CreatedTS  float64 `json:"created_ts"`
	CommitOID  string  `json:"checkpoint_commit_oid"`
	Published  bool    `json:"published"`
	EventCount int     `json:"event_count"`
}

func newHistoryCmd() *cobra.Command {
	var activity bool
	cmd := &cobra.Command{
		Use:   "history",
		Short: "Show protected checkpoints and their Git publication state",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			if activity {
				if !jsonOut {
					return runEvents(cmd.Context(), cmd.OutOrStdout(), repo, "", 0,
						defaultEventsLimit, false, defaultEventsWatchInterval, false)
				}
				var legacy bytes.Buffer
				if err := runEvents(cmd.Context(), &legacy, repo, "", 0,
					defaultEventsLimit, false, defaultEventsWatchInterval, true); err != nil {
					return err
				}
				var activityData any = map[string]any{}
				if err := json.Unmarshal(legacy.Bytes(), &activityData); err != nil {
					return fmt.Errorf("acd history --activity: decode activity: %w", err)
				}
				return renderJSONEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true,
					State: productStateProtected, Actions: []productAction{}, Data: activityData})
			}
			return runCheckpointHistory(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
	cmd.Flags().BoolVar(&activity, "activity", false, "Show capture and publication activity instead of checkpoints")
	explain := newHistoryExplainCmd()
	activityCommand := newHistoryActivityCmd()
	rewrite := newRewriteCommitsCmd()
	rewrite.Use = "rewrite"
	cmd.AddCommand(activityCommand, explain, withInvocationCapabilities(rewrite,
		commandCapabilities{Repository: true, JSON: true, Quiet: true, Interactive: true}))
	return cmd
}

func runCheckpointHistory(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	record, _, _, err := lookupRegisteredRepo("history", repo)
	if err != nil {
		return err
	}
	projection, err := state.ReadCheckpointProjection(ctx, record.StateDB, 100)
	if err != nil {
		return fmt.Errorf("acd history: %w", err)
	}
	if !projection.Available {
		return fmt.Errorf("acd history: checkpoint history is unavailable; run `acd setup` to cut over this repository")
	}
	db, err := openStateDBReadOnly(ctx, record.StateDB)
	if err != nil {
		return fmt.Errorf("acd history: open state.db read-only: %w", err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
SELECT cp.id, cp.seq, cp.reason, cp.phase, cp.created_ts, cp.commit_oid,
       COUNT(ce.event_seq),
       COALESCE(SUM(CASE WHEN e.state='published' THEN 1 ELSE 0 END), 0)
FROM checkpoints cp
LEFT JOIN checkpoint_events ce ON ce.checkpoint_id=cp.id
LEFT JOIN capture_events e ON e.seq=ce.event_seq
GROUP BY cp.id
ORDER BY cp.seq DESC
LIMIT 100`)
	if err != nil {
		return fmt.Errorf("acd history: query checkpoints: %w", err)
	}
	defer rows.Close()
	entries := make([]historyEntry, 0)
	for rows.Next() {
		var entry historyEntry
		var publishedEvents int
		if err := rows.Scan(&entry.ID, &entry.Sequence, &entry.Reason,
			&entry.Phase, &entry.CreatedTS, &entry.CommitOID,
			&entry.EventCount, &publishedEvents); err != nil {
			return fmt.Errorf("acd history: scan checkpoint: %w", err)
		}
		entry.Published = entry.Phase == state.CheckpointCompleted && publishedEvents == entry.EventCount
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("acd history: iterate checkpoints: %w", err)
	}
	if jsonOut {
		envelope := productEnvelope{
			OK: true, State: productStateProtected, Actions: []productAction{}, Data: entries,
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(envelope)
	}
	if len(entries) == 0 {
		fmt.Fprintln(out, "No checkpoints yet.")
		return nil
	}
	fmt.Fprintln(out, "CHECKPOINT\tWHEN\tREASON\tGIT")
	for _, entry := range entries {
		published := "waiting"
		if entry.Published {
			published = "published"
		}
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\n", entry.ID,
			time.Unix(int64(entry.CreatedTS), 0).Format(time.RFC3339), entry.Reason, published)
	}
	return nil
}

func newConfigNamespaceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "config", Short: "Manage ACD configuration", Hidden: true}
	edit := newConfigureCmd()
	edit.Use = "edit"
	cmd.AddCommand(
		withInvocationCapabilities(newConfigGetCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newConfigSetCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(edit, commandCapabilities{Repository: true, Quiet: true, Interactive: true}),
		withInvocationCapabilities(newConfigResetCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		newConfigCredentialsCmd(),
	)
	return cmd
}

func newConfigCredentialsCmd() *cobra.Command {
	credentials := newAuthCmd()
	credentials.Use = "credentials"
	credentials.Short = "Manage the protected provider credential"
	credentials.RunE = func(command *cobra.Command, _ []string) error {
		jsonOut, _ := command.Flags().GetBool("json")
		return runProductCredentialStatus(command.OutOrStdout(), jsonOut)
	}
	for _, child := range credentials.Commands() {
		child.Example = strings.ReplaceAll(child.Example, "acd auth", "acd config credentials")
		capabilities := commandCapabilities{Quiet: true, Interactive: true}
		if child.Name() == "status" {
			child.RunE = credentials.RunE
			capabilities.JSON = true
		}
		withInvocationCapabilities(child, capabilities)
	}
	return withInvocationCapabilities(credentials, commandCapabilities{JSON: true, Quiet: true})
}

func newSupportNamespaceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "support", Short: "Advanced diagnostics and repair", Hidden: true}
	diagnose := newSupportDiagnoseCmd()
	logs := newSupportLogsCmd()
	repair := withInvocationCapabilities(newProductRepairCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true})
	bundle := &cobra.Command{
		Use:   "bundle",
		Short: "Create a sanitized support bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			output, _ := cmd.Flags().GetString("output")
			return runProductDoctorBundle(withDoctorTarget(cmd.Context(), repo),
				cmd.OutOrStdout(), output, jsonOut)
		},
	}
	bundle.Flags().String("output", "", "Override the output directory")
	cmd.AddCommand(diagnose, logs, repair, newSupportRecoverCmd(), newSupportPromptCmd(),
		withInvocationCapabilities(bundle, commandCapabilities{Repository: true, JSON: true, Quiet: true}))
	return cmd
}

func newSupportDiagnoseCmd() *cobra.Command {
	command := newDiagnoseCmd()
	command.Use = "diagnose"
	command.Long = `Inspect replay blockers, pending depth, branch anchors, Git-operation
markers, and remediation hints for one repository without mutating state.

Use acd support recover --dry-run to preview exact-chain recovery when diagnose
reports a terminal publication barrier.`
	command.Example = `  acd support diagnose
  acd support diagnose --repo /path/to/repo
  acd support diagnose --json
  acd support recover --dry-run`
	return withInvocationCapabilities(command, commandCapabilities{Repository: true, JSON: true, Quiet: true})
}

func newSupportLogsCmd() *cobra.Command {
	command := newLogsCmd()
	command.Use = "logs"
	command.Long = `Print one repository's raw daemon JSONL log tail.

Use --lines to choose the initial tail or --follow to stream appended lines.
Use acd support bundle for a sanitized shareable archive and acd support prompt
for locally recorded AI prompt traces.`
	command.Example = `  acd support logs
  acd support logs --lines 200
  acd support logs --follow
  acd support logs --repo /path/to/repo --lines 50 --follow`
	return withInvocationCapabilities(command,
		commandCapabilities{Repository: true, JSON: true, Quiet: true, Streaming: true})
}

func newHistoryExplainCmd() *cobra.Command {
	command := newExplainCmd()
	command.Use = "explain"
	command.Example = `  acd history explain
  acd history explain --path internal/state/schema.go
  acd history explain --commit HEAD
  acd history explain --last
  acd history explain --since 42 --json`
	return withInvocationCapabilities(command, commandCapabilities{Repository: true, JSON: true, Quiet: true})
}

func newHistoryActivityCmd() *cobra.Command {
	command := newEventsCmd()
	command.Use = "activity"
	command.Short = "Show capture and publication activity"
	command.Long = `Show product-facing capture and publication decisions for one repository.

Use --path to focus on one path, --since to resume from a decision cursor, and
--watch to stream activity until interrupted.`
	command.Example = `  acd history activity
  acd history activity --path internal/state/schema.go
  acd history activity --since 42 --limit 100
  acd history activity --watch`
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		jsonOut, _ := cmd.Flags().GetBool("json")
		path, _ := cmd.Flags().GetString("path")
		since, _ := cmd.Flags().GetInt64("since")
		limit, _ := cmd.Flags().GetInt("limit")
		watch, _ := cmd.Flags().GetBool("watch")
		interval, _ := cmd.Flags().GetDuration("interval")
		ctx := cmd.Context()
		stop := func() {}
		if watch {
			ctx, stop = signal.NotifyContext(ctx, os.Interrupt)
		}
		defer stop()
		return runProductEvents(ctx, cmd.OutOrStdout(), repo, path,
			since, limit, watch, interval, jsonOut)
	}
	return withInvocationCapabilities(command,
		commandCapabilities{Repository: true, JSON: true, Quiet: true, Streaming: true})
}

func newSupportPromptCmd() *cobra.Command {
	command := newPromptCmd()
	command.Use = "prompt"
	command.Short = "Inspect locally recorded AI prompt traces"
	command.Long = `Inspect locally recorded AI prompt traces for one repository.

Prompt traces exist only when ACD_AI_PROMPT_TRACE is enabled. With no selector,
the newest trace is shown; use --seq for one captured event or intent sequence.`
	command.Example = `  acd support prompt
  acd support prompt --last
  acd support prompt --seq 42
  acd support prompt --seq 42 --json`
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		jsonOut, _ := cmd.Flags().GetBool("json")
		last, _ := cmd.Flags().GetBool("last")
		seq, _ := cmd.Flags().GetInt64("seq")
		return runProductPrompt(cmd.Context(), cmd.OutOrStdout(), repo, last, seq, jsonOut)
	}
	return withInvocationCapabilities(command, commandCapabilities{Repository: true, JSON: true, Quiet: true})
}

func newSupportRecoverCmd() *cobra.Command {
	command := newFixCmd()
	command.Use = "recover"
	command.Short = "Preview or apply exact-chain recovery"
	command.Long = `Preview or apply exact-chain recovery for one repository.

Without --yes, recovery is read-only. --yes applies only proved recovery;
--force selects archive-only protection for otherwise unresolved pairs.`
	command.Example = `  acd support recover --dry-run
  acd support recover --yes
  acd support recover --force --dry-run
  acd support recover --force --yes`
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		jsonOut, _ := cmd.Flags().GetBool("json")
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		yes, _ := cmd.Flags().GetBool("yes")
		force, _ := cmd.Flags().GetBool("force")
		clearPause, _ := cmd.Flags().GetBool("clear-pause")
		return runProductFix(cmd.Context(), cmd.OutOrStdout(), repo,
			dryRun, yes, force, clearPause, jsonOut)
	}
	return withInvocationCapabilities(command, commandCapabilities{Repository: true, JSON: true, Quiet: true})
}

func newProductRepoNamespaceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "repo", Short: "Manage protected repositories", Hidden: true, Args: cobra.NoArgs}
	initCompat := &cobra.Command{
		Use:    "init",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			fmt.Fprintln(command.ErrOrStderr(), "warning: acd repo init is a compatibility alias; use acd on")
			repo, _ := command.Flags().GetString("repo")
			jsonOut, _ := command.Flags().GetBool("json")
			return runControlOn(command.Context(), command.OutOrStdout(), repo, jsonOut)
		},
	}
	cmd.AddCommand(
		withInvocationCapabilities(newRepoListCmd(), commandCapabilities{JSON: true, Quiet: true}),
		withInvocationCapabilities(newRepoRemoveCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true, Interactive: true}),
		withInvocationCapabilities(newGCCmd(), commandCapabilities{JSON: true, Quiet: true, Interactive: true}),
		newRepoStatsCmd(),
		withInvocationCapabilities(initCompat, commandCapabilities{Repository: true, JSON: true, Quiet: true}),
	)
	return cmd
}

func newRepoStatsCmd() *cobra.Command {
	command := newStatsCmd()
	command.Use = "stats"
	command.Short = "Show aggregate activity across repositories"
	command.Long = `Show aggregate commit, event, file, byte, and error counts from the
central statistics database across all registered repositories.`
	command.Example = `  acd repo stats
  acd repo stats --since 30d
  acd repo stats --since 1y --json`
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		since, _ := cmd.Flags().GetString("since")
		jsonOut, _ := cmd.Flags().GetBool("json")
		return runProductStats(cmd.Context(), cmd.OutOrStdout(), since, jsonOut)
	}
	return withInvocationCapabilities(command, commandCapabilities{JSON: true, Quiet: true})
}

func newRecoverCompatibilityDelegate() *cobra.Command {
	var auto, dryRun, yes, clearPause bool
	command := &cobra.Command{
		Use: "recover", Hidden: true, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !auto && !dryRun {
				return invalidCommandError("acd recover: pass --auto to derive a recovery plan, or --dry-run to inspect first")
			}
			if !dryRun && !yes {
				return invalidCommandError("acd recover: refusing to mutate state without --yes")
			}
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runProductFix(cmd.Context(), cmd.OutOrStdout(), repo,
				dryRun, yes, false, clearPause, jsonOut)
		},
	}
	command.Flags().BoolVar(&auto, "auto", false, "Derive recovery from current HEAD")
	command.Flags().BoolVar(&dryRun, "dry-run", false, "Show the recovery plan without mutation")
	command.Flags().BoolVar(&yes, "yes", false, "Apply proved recovery")
	command.Flags().BoolVar(&clearPause, "clear-pause", false, "Also remove the manual pause marker")
	return withInvocationCapabilities(command, commandCapabilities{Repository: true, JSON: true, Quiet: true})
}

func newEventsCompatibilityDelegate() *cobra.Command {
	command := renameCompatibility(newHistoryActivityCmd(), "events")
	command.Short = "Show product decisions for the current repo"
	command.Long = `Show product-facing ACD decisions for the current repo.

The events command reads the durable decision ledger instead of raw daemon
JSONL logs. Use --path to focus on one path, --since with a decision cursor to
resume polling, and --watch to stream appended decisions until interrupted.
With --watch and no --since, events prints only decisions appended after watch starts.`
	command.Example = `  acd events
  acd events --path internal/state/schema.go
  acd events --since 42 --limit 100
  acd events --watch
  acd events --json`
	return command
}

func newLogsCompatibilityDelegate() *cobra.Command {
	command := renameCompatibility(newSupportLogsCmd(), "logs")
	command.Short = "Print the current repo daemon log tail"
	command.Long = `Print the current repo daemon log tail as raw JSONL.

The default repo is the current working directory. By default acd logs prints
the last 100 raw log lines and exits. Use --lines to choose the initial tail
length, or --follow to keep streaming appended lines until interrupted. For
bundled diagnostics and sanitized tails, use acd doctor or acd doctor --bundle.
Full AI prompt traces are stored separately and require ACD_AI_PROMPT_TRACE=1
plus acd prompt.`
	command.Example = `  acd logs
  acd logs --lines 200
  acd logs --follow
  acd logs --repo /path/to/repo --lines 50 --follow`
	return command
}

func hideCompatibility(command *cobra.Command, replacement string, protocol bool) *cobra.Command {
	command.Hidden = true
	if command.Annotations == nil {
		command.Annotations = map[string]string{}
	}
	command.Annotations["acd.compatibility.replacement"] = replacement
	if command.RunE == nil {
		return command
	}
	run := command.RunE
	command.RunE = func(cmd *cobra.Command, args []string) error {
		if !protocol {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: acd %s is a compatibility alias; use %s\n",
				command.Name(), replacement)
		}
		return run(cmd, args)
	}
	return command
}

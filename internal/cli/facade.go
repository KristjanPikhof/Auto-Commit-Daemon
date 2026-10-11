package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type productDoctorSettings struct {
	Style              string   `json:"style"`
	Provider           string   `json:"provider"`
	ProviderSource     string   `json:"provider_source"`
	Strategy           string   `json:"strategy"`
	StrategySource     string   `json:"strategy_source"`
	Verification       string   `json:"verification"`
	RepairEnabled      bool     `json:"repair_enabled"`
	DiffEgressEnabled  bool     `json:"diff_egress_enabled"`
	ConfigurationNotes []string `json:"configuration_notes,omitempty"`
}

func newProductStatusCmd() *cobra.Command {
	var verbose bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show saved work, commit status, and the next step",
		Long: `Show whether ACD is running, whether current changes are protected, and
whether those changes have been published to local Git.

The command does not change anything. If action is needed, the last line gives
the exact next command to run.`,
		Example: `  acd status
  acd status --repo /path/to/repo
  acd status --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			if !verbose || jsonOut {
				return runControlStatus(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
			}
			result, err := inspectControl(cmd.Context(), repo)
			if err != nil {
				return err
			}
			if err := renderControl(cmd.OutOrStdout(), result, false); err != nil {
				return err
			}
			renderProductPublicationProgress(cmd.OutOrStdout(), result.PublicationProgress)
			if detail := maintenanceDetails(result.CheckpointMaintenance); detail != "" {
				fmt.Fprintln(cmd.OutOrStdout(), consoleProse(detail))
			}
			renderProductDoctorWorker(cmd.OutOrStdout(), result)
			if !result.OK {
				return &CommandError{Code: "needs_action", Message: result.Summary, Exit: ExitActionRequired, rendered: true}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Include finish progress, protection, and worker details")
	return cmd
}

func newProductDoctorCmd() *cobra.Command {
	var bundle bool
	var output string
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Explain ACD problems and show how to fix them",
		Long: `Check the installation, background worker, protection state, and active
configuration for one repository.

Doctor does not change anything. It explains the current problem and prints a
safe next step. Use --bundle only when you need a sanitized archive for
support.`,
		Example: `  acd doctor
  acd doctor --repo /path/to/repo
  acd doctor --bundle
  acd doctor --bundle --output /path/to/folder
  acd doctor --json`,
		Args: cobra.NoArgs,
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
	cmd.Flags().BoolVar(&bundle, "bundle", false, "Create a sanitized support archive")
	cmd.Flags().StringVar(&output, "output", "", "Folder for the support archive")
	return cmd
}

func renderProductDoctorWorker(out io.Writer, status controlResult) {
	if status.SupervisorWorkerState == "" {
		return
	}
	fmt.Fprintln(out, "Background protection:")
	fmt.Fprintf(out, "  Status: %s", strings.ReplaceAll(status.SupervisorWorkerState, "_", " "))
	if status.SupervisorWorkerRestarts > 0 {
		fmt.Fprintf(out, " after %d attempts to restart", status.SupervisorWorkerRestarts)
	}
	fmt.Fprintln(out)
	if status.SupervisorWorkerError != "" {
		fmt.Fprintf(out, "  Details: %s\n", consoleProse(status.SupervisorWorkerError))
	}
}

func collectProductDoctorSettings(ctx context.Context, repo string) productDoctorSettings {
	details := productDoctorSettings{ConfigurationNotes: []string{}}
	roots, err := paths.Resolve()
	if err != nil {
		details.ConfigurationNotes = append(details.ConfigurationNotes, err.Error())
		return details
	}
	values, source, err := finishReadOnlyConfigValues(ctx, roots, repo)
	if err != nil {
		details.ConfigurationNotes = append(details.ConfigurationNotes, err.Error())
		return details
	}
	details.Provider = values[config.FieldProvider]
	details.ProviderSource = source
	details.Strategy = values[config.FieldCommitStrategy]
	details.StrategySource = source
	if details.Strategy == "local" {
		details.Provider = "local"
	}
	details.Style = values[config.FieldCommitFormat]
	details.Verification = values[config.FieldIntentVerification]
	details.DiffEgressEnabled = truthyConfig(values[config.FieldDiffEgress])
	return details
}

func renderProductDoctorSettings(out io.Writer, details productDoctorSettings) {
	fmt.Fprintln(out, "Commit settings:")
	fmt.Fprintf(out, "  AI provider: %s (from %s)\n", valueOrUnset(details.Provider), valueOrUnset(details.ProviderSource))
	fmt.Fprintf(out, "  Finish mode: %s (from %s)\n", valueOrUnset(details.Strategy), valueOrUnset(details.StrategySource))
	fmt.Fprintf(out, "  Message style: %s\n", valueOrUnset(details.Style))
	fmt.Fprintf(out, "  Project checks: %s\n", valueOrUnset(details.Verification))
	fmt.Fprintf(out, "  Share file differences with AI: %s\n", yesNo(details.DiffEgressEnabled))
	for _, note := range details.ConfigurationNotes {
		fmt.Fprintf(out, "  Note: %s\n", note)
	}
	fmt.Fprintln(out, "More detail: Run `acd support diagnose`.")
}

func runProductDiagnose(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	if !jsonOut {
		return runDiagnose(ctx, out, repo, false)
	}
	report, err := collectDiagnose(ctx, repo)
	if err != nil {
		return err
	}
	return renderProductDiagnoseReport(out, report)
}

func renderProductDiagnoseReport(out io.Writer, report diagnoseReport) error {
	if report.OperationalState == "paused" {
		next := "Run `acd status` to review the pause reason."
		return renderJSONEnvelope(out, productEnvelope{
			OK: true, State: productStateNeedsAction,
			Actions: []productAction{}, NextAction: &next, Data: report,
		})
	}
	if report.BackpressurePaused {
		next := "Run `acd doctor` before clearing backpressure."
		return renderJSONEnvelope(out, productEnvelope{
			OK: true, State: productStateNeedsAction,
			Actions: []productAction{}, NextAction: &next, Data: report,
		})
	}
	if report.OperationalState == "needs_attention" {
		next := "Review the blocked drain and run `acd support logs --lines 100` for the failure context."
		return renderJSONEnvelope(out, productEnvelope{
			OK: true, State: productStateNeedsAction,
			Actions: []productAction{}, NextAction: &next, Data: report,
		})
	}
	if report.CaptureHealth.Error != "" {
		state := productStateNeedsAction
		if report.CaptureHealth.State == "retrying" {
			state = productStateWaiting
		}
		next := "Inspect the affected paths with `acd doctor`; ACD retries capture automatically."
		return renderJSONEnvelope(out, productEnvelope{OK: true, State: state, Actions: []productAction{}, NextAction: &next, Data: report})
	}
	return renderAdvancedResult(out, productStateProtected, report)
}

func runProductDoctorBundle(ctx context.Context, out io.Writer, output string, jsonOut bool) error {
	if !jsonOut {
		return runDoctor(ctx, out, true, output, false)
	}
	report, err := collectDoctorReport(ctx)
	if err != nil {
		return fmt.Errorf("acd doctor: collect\nDetails: %w", err)
	}
	bundle, err := writeDoctorBundle(ctx, report, output)
	if err != nil {
		return fmt.Errorf("acd doctor: bundle\nDetails: %w", err)
	}
	return renderAdvancedResult(out, productStateProtected, bundle)
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
	report, err := collectExplain(ctx, repo, path, commit, last, since, limit)
	if err != nil {
		return err
	}
	return renderAdvancedResult(out, productStateProtected, report)
}

func runProductRepoList(ctx context.Context, out io.Writer, jsonOut bool) error {
	if !jsonOut {
		return runRepoList(ctx, out, false)
	}
	entries, err := collectRepoList(ctx)
	if err != nil {
		return err
	}
	return renderAdvancedResult(out, productStateOff, struct {
		Repos []repoListEntry `json:"repos"`
	}{Repos: entries})
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
	report, err := collectEvents(ctx, repo, path, since, limit)
	if err != nil {
		return err
	}
	return renderAdvancedResult(out, productStateProtected, report)
}

func runProductStats(ctx context.Context, out io.Writer, since string, jsonOut bool) error {
	if !jsonOut {
		return runStats(ctx, out, since, false)
	}
	report, err := collectStats(ctx, since)
	if err != nil {
		return err
	}
	return renderAdvancedResult(out, productStateOff, report)
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
	plan, err := executeFix(ctx, repo, dryRun, yes, force, clearPause)
	if plan == nil {
		return err
	}
	changed := !plan.DryRun && (plan.RowsChanged > 0 || plan.ManualPauseRemoved)
	if err == nil {
		stateName := productStateProtected
		if plan.CaptureHealth.State == "retrying" {
			stateName = productStateWaiting
		} else if plan.CaptureHealth.Error != "" || len(plan.Unsafe) > 0 || plan.DryRun && len(plan.Actions) > 0 {
			stateName = productStateNeedsAction
		}
		return renderJSONEnvelope(out, productEnvelope{
			OK: true, State: stateName, Changed: changed, Actions: []productAction{}, Data: plan,
		})
	}
	commandErr := &CommandError{Code: "recovery_failed", Message: err.Error(), Exit: ExitCode(err)}
	var existing *CommandError
	if errors.As(err, &existing) {
		*commandErr = *existing
	}
	if renderErr := renderJSONEnvelope(out, productEnvelope{
		OK: false, State: productStateNeedsAction, Changed: changed, Actions: []productAction{}, Data: plan,
		Error: &productError{Code: commandErr.Code, Message: commandErr.Message,
			Retryable: commandErr.Retryable, Details: commandErr.Details},
	}); renderErr != nil {
		return renderErr
	}
	commandErr.rendered = true
	return commandErr
}

func runProductLogs(ctx context.Context, out io.Writer, repo string, lines int, follow, jsonOut bool) error {
	if !jsonOut {
		return runLogs(ctx, out, repo, lines, follow)
	}
	tail, _, _, err := collectLogTail(repo, lines)
	if err != nil {
		return err
	}
	logLines := make([]string, 0, len(tail))
	for _, line := range tail {
		if line != "" {
			logLines = append(logLines, line)
		}
	}
	return renderJSONEnvelope(out, productEnvelope{OK: true, State: productStateProtected,
		Actions: []productAction{}, Data: map[string]any{"lines": logLines}})
}

func renderAdvancedResult(out io.Writer, stateName productState, data any) error {
	return renderJSONEnvelope(out, productEnvelope{OK: true, State: stateName, Actions: []productAction{}, Data: data})
}

type historyEntry struct {
	PublicationOutcome publicationOutcome `json:"publication_outcome"`
	Retained           bool               `json:"retained"`
	Outcome            string             `json:"outcome"`
	RecoveredEvents    int                `json:"recovered_events"`
	ID                 string             `json:"id"`
	Sequence           int64              `json:"sequence"`
	Reason             string             `json:"reason"`
	Phase              string             `json:"phase"`
	CreatedTS          float64            `json:"created_ts"`
	CommitOID          string             `json:"checkpoint_commit_oid"`
	Published          bool               `json:"published"`
	EventCount         int                `json:"event_count"`
}

func newHistoryCmd() *cobra.Command {
	var activity bool
	cmd := &cobra.Command{
		Use:   "history",
		Short: "Show checkpoints and whether Git contains them",
		Long: `Show recent protected checkpoints for this repository and whether each
checkpoint has been published to local Git.

Use the subcommands to inspect activity, explain a decision, or safely rewrite
commit messages. History commands do not rewrite commits unless you explicitly
apply a reviewed rewrite plan.`,
		Example: `  acd history
  acd history --activity
  acd history explain --last
  acd history rewrite --help`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			if activity {
				if !jsonOut {
					return runEvents(cmd.Context(), cmd.OutOrStdout(), repo, "", 0,
						defaultEventsLimit, false, defaultEventsWatchInterval, false)
				}
				report, err := collectEvents(cmd.Context(), repo, "", 0, defaultEventsLimit)
				if err != nil {
					return err
				}
				return renderAdvancedResult(cmd.OutOrStdout(), productStateProtected, report)
			}
			return runCheckpointHistory(cmd.Context(), cmd.OutOrStdout(), repo, jsonOut)
		},
	}
	cmd.Flags().BoolVar(&activity, "activity", false, "Show capture and publication activity instead of checkpoints")
	explain := newHistoryExplainCmd()
	activityCommand := newHistoryActivityCmd()
	rewrite := newRewriteCommitsCmd()
	rewrite.Use = "rewrite [selection flags]"
	rewrite.Aliases = nil
	cmd.AddCommand(activityCommand, explain, withInvocationCapabilities(rewrite,
		commandCapabilities{Repository: true, JSON: true, Quiet: true, Interactive: true}))
	return cmd
}

func loadCheckpointHistory(ctx context.Context, repo string) ([]historyEntry, error) {
	record, _, _, err := lookupRegisteredRepo("history", repo)
	if err != nil {
		return nil, err
	}
	projection, err := state.ReadCheckpointProjection(ctx, record.StateDB, 100)
	if err != nil {
		return nil, fmt.Errorf("acd history\nDetails: %w", err)
	}
	if !projection.Available {
		return nil, fmt.Errorf("acd history: checkpoint history is unavailable; run `acd setup` to cut over this repository")
	}
	db, err := openStateDBReadOnly(ctx, record.StateDB)
	if err != nil {
		return nil, fmt.Errorf("acd history: open state.db read-only: %w", err)
	}
	defer db.Close()
	coverage := "cp.coverage_complete"
	if projection.SchemaVersion < 29 {
		coverage = "1"
	}
	hasCaptures, err := state.LegacyTableExists(ctx, db, "capture_events")
	if err != nil {
		return nil, fmt.Errorf("acd history: inspect legacy evidence\nDetails: %w", err)
	}
	membership := "COUNT(ce.event_seq), 0, 0"
	legacyJoin := ""
	if hasCaptures {
		membership = "COUNT(ce.event_seq), COALESCE(SUM(e.state='published'),0), COALESCE(SUM(e.state='recovered'),0)"
		legacyJoin = "LEFT JOIN capture_events e ON e.seq=ce.event_seq"
	}
	rows, err := db.QueryContext(ctx, `
SELECT cp.id, cp.seq, cp.reason, cp.phase, cp.created_ts, cp.commit_oid, cp.retained, `+coverage+`,
       `+membership+`
FROM checkpoints cp
LEFT JOIN checkpoint_events ce ON ce.checkpoint_id=cp.id
`+legacyJoin+`
GROUP BY cp.id
ORDER BY cp.seq DESC
LIMIT 100`)
	if err != nil {
		return nil, fmt.Errorf("acd history: query checkpoints\nDetails: %w", err)
	}
	defer rows.Close()
	entries := make([]historyEntry, 0)
	for rows.Next() {
		var entry historyEntry
		var publishedEvents int
		var complete bool
		if err := rows.Scan(&entry.ID, &entry.Sequence, &entry.Reason,
			&entry.Phase, &entry.CreatedTS, &entry.CommitOID, &entry.Retained, &complete,
			&entry.EventCount, &publishedEvents, &entry.RecoveredEvents); err != nil {
			return nil, fmt.Errorf("acd history: scan checkpoint\nDetails: %w", err)
		}
		entry.Published = entry.Phase == state.CheckpointCompleted && entry.EventCount > 0 && publishedEvents+entry.RecoveredEvents == entry.EventCount
		entry.Outcome = checkpointOutcome(entry.Phase, entry.EventCount, publishedEvents, entry.RecoveredEvents)
		entry.PublicationOutcome = publicationOutcome{CheckpointAvailable: true, Protected: entry.Retained && complete && entry.Phase == state.CheckpointCompleted, Resolved: entry.Published, RecoveredChanges: entry.RecoveredEvents}
		if !complete {
			entry.Published = false
			entry.Outcome = "partial"
			entry.PublicationOutcome.Resolved = false
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("acd history: iterate checkpoints\nDetails: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range entries {
		entry := &entries[i]
		finish, err := readFinishStatusForCheckpoint(ctx, db, checkpoint.WorktreeID(record.Path), entry.ID)
		if err != nil {
			return nil, err
		}
		committed := entry.Outcome == "published"
		if finish != nil && finish.Published {
			committed = true
			entry.Published = true
			entry.Outcome = "published"
		}
		if finish != nil && finish.NoChanges && !finish.DryRun {
			entry.Published = true
			entry.Outcome = "up_to_date"
		}
		entry.PublicationOutcome.Resolved = entry.Published
		entry.PublicationOutcome.Finish = finish
		entry.PublicationOutcome.BranchCommitted = &committed
	}
	return entries, nil
}

func runCheckpointHistory(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	entries, err := loadCheckpointHistory(ctx, repo)
	if err != nil {
		return err
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
		fmt.Fprintln(out, "Status: No checkpoints yet.")
		fmt.Fprintln(out, "Next: No action needed. A checkpoint will appear after ACD protects a change.")
		return nil
	}
	fmt.Fprintln(out, "CHECKPOINT\tWHEN\tREASON\tGIT")
	for _, entry := range entries {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\n", entry.ID,
			time.Unix(int64(entry.CreatedTS), 0).Format(time.RFC3339), entry.Reason, entry.Outcome)
	}
	return nil
}

func newConfigNamespaceCmd() *cobra.Command {
	cmd := newConfigEditorCmd()
	cmd.Use = "config"
	cmd.Short = "Edit global or repository settings"
	withInvocationCapabilities(cmd, commandCapabilities{Repository: true, Quiet: true, Interactive: true})
	edit := newConfigEditorCmd()
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
	credentials.Short = "Manage the saved AI API key"
	credentials.Example = `  acd config credentials status
  acd config credentials set
  acd config credentials remove`
	credentials.RunE = func(command *cobra.Command, _ []string) error {
		jsonOut, _ := command.Flags().GetBool("json")
		return runProductCredentialStatus(command.OutOrStdout(), jsonOut)
	}
	for _, child := range credentials.Commands() {
		child.Example = strings.ReplaceAll(child.Example, "acd auth", "acd config credentials")
		capabilities := commandCapabilities{Quiet: true, Interactive: true, Repository: child.Name() == "set"}
		if child.Name() == "status" {
			child.RunE = credentials.RunE
			capabilities.JSON = true
		}
		withInvocationCapabilities(child, capabilities)
	}
	return withInvocationCapabilities(credentials, commandCapabilities{JSON: true, Quiet: true})
}

func newSupportNamespaceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "support", Short: "Inspect and safely repair ACD problems", Hidden: true,
		Long: `Use read-only diagnostics first. Recovery and repair commands show a preview
before they change anything and require explicit confirmation.`,
		Example: `  acd support diagnose
  acd support logs --lines 100
  acd support recover --dry-run`}
	diagnose := newSupportDiagnoseCmd()
	logs := newSupportLogsCmd()
	repair := withInvocationCapabilities(newProductRepairCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true})
	bundle := &cobra.Command{
		Use:   "bundle",
		Short: "Create a sanitized support archive",
		Long: `Create a sanitized archive with ACD diagnostics for one repository.

The archive removes known home-directory paths and secrets. Review it before
sharing it. This command does not change repository state.`,
		Example: `  acd support bundle
  acd support bundle --repo /path/to/repo
  acd support bundle --output /path/to/folder`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			output, _ := cmd.Flags().GetString("output")
			return runProductDoctorBundle(withDoctorTarget(cmd.Context(), repo),
				cmd.OutOrStdout(), output, jsonOut)
		},
	}
	bundle.Flags().String("output", "", "Folder for the support archive")
	cmd.AddCommand(diagnose, logs, repair, newSupportRecoverCmd(),
		withInvocationCapabilities(bundle, commandCapabilities{Repository: true, JSON: true, Quiet: true}))
	return cmd
}

func newSupportDiagnoseCmd() *cobra.Command {
	command := newDiagnoseCmd()
	command.Use = "diagnose"
	command.Short = "Inspect why protection or Git publication is stuck"
	command.Long = `Inspect one repository without changing anything. The report
shows whether ACD is protecting changes, what is delaying Git publication, and
the safest next step.

If the report finds a safety block, use acd support recover --dry-run to preview
recovery before applying it.`
	command.Example = `  acd support diagnose
  acd support diagnose --repo /path/to/repo
  acd support diagnose --json
  acd support recover --dry-run`
	return withInvocationCapabilities(command, commandCapabilities{Repository: true, JSON: true, Quiet: true})
}

func newSupportLogsCmd() *cobra.Command {
	command := newLogsCmd()
	command.Use = "logs"
	command.Short = "Show recent background-worker log entries"
	command.Long = `Print recent background-worker log entries for one repository.

Use --lines to choose the initial tail or --follow to stream appended lines.
The log is raw JSONL and may contain local paths, so review it before sharing.
Use acd support bundle for a sanitized archive.`
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
	command.Long = `Show when ACD protected a change, published a commit, waited,
or stopped at a safety check.

Use --path to focus on one file, --since to continue after an activity number,
or --watch to keep showing new activity until interrupted.`
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
	command.Flags().Lookup("since").Usage = "Show activity after this activity number"
	command.Flags().Lookup("watch").Usage = "Keep showing new activity until interrupted"
	return withInvocationCapabilities(command,
		commandCapabilities{Repository: true, JSON: true, Quiet: true, Streaming: true})
}

func newSupportRecoverCmd() *cobra.Command {
	command := newFixCmd()
	command.Use = "recover"
	command.Short = "Preview or recover protected work that cannot publish"
	command.Long = "Preview recovery of older saved work that ACD cannot add to Git safely.\n\nReview the plan before using --yes to apply it. Recovery does not start a new\nfinish request or rewrite current Git history. Use --force only to preserve\nunresolved work in a private recovery archive. Run acd status for current work."
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
	command.Flags().Lookup("dry-run").Usage = "Show the recovery plan without changing anything"
	command.Flags().Lookup("yes").Usage = "Apply the displayed recovery plan"
	command.Flags().Lookup("force").Usage = "Preserve unresolved protected work in a private recovery archive"
	command.Flags().Lookup("clear-pause").Usage = "Also remove a manual pause after recovery"
	return withInvocationCapabilities(command, commandCapabilities{Repository: true, JSON: true, Quiet: true})
}

func newProductRepoNamespaceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "repo", Short: "Manage registered repositories", Hidden: true, Args: cobra.NoArgs,
		Long: `List every registered repository, remove an old registration, clean up missing
entries, or view activity totals.

Use acd list for the everyday protection dashboard. These commands are for
registration and storage maintenance.`,
		Example: `  acd repo list
  acd repo remove --dry-run
  acd repo gc`}
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

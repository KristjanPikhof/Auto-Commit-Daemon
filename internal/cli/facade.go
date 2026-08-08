package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

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
			status, err := inspectControl(cmd.Context(), repo)
			if err != nil {
				return err
			}
			var diagnostic bytes.Buffer
			if err := runDoctor(cmd.Context(), &diagnostic, bundle, output, true); err != nil {
				return err
			}
			var details any = map[string]any{}
			if err := json.Unmarshal(diagnostic.Bytes(), &details); err != nil {
				return fmt.Errorf("acd doctor: decode diagnostic report: %w", err)
			}
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
				fmt.Fprintln(cmd.OutOrStdout(), "\nDiagnostics:")
				if err := runDoctor(cmd.Context(), cmd.OutOrStdout(), bundle, output, false); err != nil {
					return err
				}
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
	explain := newExplainCmd()
	explain.Use = "explain"
	rewrite := newRewriteCommitsCmd()
	rewrite.Use = "rewrite"
	cmd.AddCommand(explain, rewrite)
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
	credentials := newAuthCmd()
	credentials.Use = "credentials"
	credentials.RunE = func(command *cobra.Command, _ []string) error {
		jsonOut, _ := command.Flags().GetBool("json")
		return runProductCredentialStatus(command.OutOrStdout(), jsonOut)
	}
	cmd.AddCommand(newConfigGetCmd(), newConfigSetCmd(), edit, newConfigResetCmd(), credentials)
	return cmd
}

func newSupportNamespaceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "support", Short: "Advanced diagnostics and repair", Hidden: true}
	diagnose := newDiagnoseCmd()
	diagnose.Use = "diagnose"
	logs := newLogsCmd()
	logs.Use = "logs"
	repair := newProductRepairCmd()
	bundle := &cobra.Command{
		Use:   "bundle",
		Short: "Create a sanitized support bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			jsonOut, _ := cmd.Flags().GetBool("json")
			output, _ := cmd.Flags().GetString("output")
			return runDoctor(cmd.Context(), cmd.OutOrStdout(), true, output, jsonOut)
		},
	}
	bundle.Flags().String("output", "", "Override the output directory")
	cmd.AddCommand(diagnose, logs, repair, bundle)
	return cmd
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
	cmd.AddCommand(newRepoListCmd(), newRepoRemoveCmd(), newGCCmd(), initCompat)
	return cmd
}

func hideCompatibility(command *cobra.Command, replacement string, protocol bool) *cobra.Command {
	command.Hidden = true
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

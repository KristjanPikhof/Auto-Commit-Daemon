package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	restorepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/restore"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func newRestoreCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "restore ID",
		Short: "Preview or restore a checkpoint into the working tree",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runRestore(cmd.Context(), cmd.OutOrStdout(), repo, args[0], yes, jsonOut)
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Apply the exact preview after revalidation")
	return cmd
}

func runRestore(ctx context.Context, out io.Writer, repo, id string, apply, jsonOut bool) error {
	record, roots, _, err := lookupRegisteredRepo("restore", repo)
	if err != nil {
		return err
	}
	if record.RepositoryID == "" || record.WorktreeID == "" {
		return fmt.Errorf("acd restore: repository requires `acd setup` checkpoint cutover")
	}
	params, _ := json.Marshal(map[string]string{"id": id})
	request := supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("restore-%d", time.Now().UnixNano()),
		Method: "restore_plan", RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID,
		DeadlineMS: time.Now().Add(5 * time.Minute).UnixMilli(), Params: params,
	}
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 5 * time.Minute}
	response, err := client.Do(ctx, request)
	if err != nil {
		return fmt.Errorf("acd restore: %w", err)
	}
	if response.Error != nil {
		return fmt.Errorf("acd restore: %s", response.Error.Message)
	}
	plan, err := decodeProductData[restorepkg.Plan](response.Data)
	if err != nil {
		return fmt.Errorf("acd restore: decode preview: %w", err)
	}
	if !apply {
		return renderRestorePlan(out, plan, jsonOut)
	}
	if !plan.CanApply || plan.Refusal != "" {
		return fmt.Errorf("acd restore: %s", plan.Refusal)
	}
	params, _ = json.Marshal(map[string]string{"id": plan.CheckpointID, "plan_digest": plan.PlanDigest})
	request.ID = fmt.Sprintf("restore-apply-%d", time.Now().UnixNano())
	request.Method = "restore_apply"
	request.Params = params
	response, err = client.Do(ctx, request)
	if err != nil {
		return fmt.Errorf("acd restore: %w", err)
	}
	if response.Error != nil {
		return fmt.Errorf("acd restore: %s", response.Error.Message)
	}
	result, err := decodeProductData[restorepkg.Result](response.Data)
	if err != nil {
		return fmt.Errorf("acd restore: decode result: %w", err)
	}
	if jsonOut {
		return renderAnyProductEnvelope(out, productEnvelope{OK: true, State: productStateWaiting,
			Changed: true, Actions: []productAction{{Kind: "restore", Status: "completed", Target: plan.CheckpointID}}, Data: result}, true)
	}
	fmt.Fprintf(out, "Restored checkpoint %s into the working tree.\n", result.RestoredCheckpoint)
	fmt.Fprintf(out, "Undo with: acd restore %s\n", result.UndoCheckpoint)
	fmt.Fprintln(out, "Git HEAD and the index were not changed.")
	return nil
}

func renderRestorePlan(out io.Writer, plan restorepkg.Plan, jsonOut bool) error {
	stateName := productStateProtected
	var next *string
	if !plan.CanApply || plan.Refusal != "" {
		stateName = productStateNeedsAction
		message := plan.Refusal
		next = &message
	} else {
		message := "acd restore " + plan.CheckpointID + " --yes"
		next = &message
	}
	if jsonOut {
		return renderAnyProductEnvelope(out, productEnvelope{OK: true, State: stateName,
			Actions: []productAction{}, NextAction: next, Data: plan}, true)
	}
	fmt.Fprintf(out, "Restore preview for %s\n", plan.CheckpointID)
	fmt.Fprintf(out, "Create: %d  Modify: %d  Delete: %d  Mode: %d  Symlink: %d\n",
		plan.Counts.Created, plan.Counts.Modified, plan.Counts.Deleted,
		plan.Counts.ModeChanged, plan.Counts.Symlinks)
	fmt.Fprintf(out, "Untracked overwrites: %d  Staged overlaps: %d\n",
		plan.Counts.UntrackedOverwrite, plan.Counts.StagedOverlap)
	if plan.Refusal != "" {
		fmt.Fprintf(out, "Cannot apply: %s\n", plan.Refusal)
	} else {
		fmt.Fprintf(out, "Apply: acd restore %s --yes\n", plan.CheckpointID)
	}
	return nil
}

func newProductRepairCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "repair",
		Short: "Complete a safely provable interrupted operation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runProductRepair(cmd.Context(), cmd.OutOrStdout(), repo, yes, jsonOut)
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Apply the exact repair preview after revalidation")
	return cmd
}

func runProductRepair(ctx context.Context, out io.Writer, repo string, apply, jsonOut bool) error {
	record, roots, _, err := lookupRegisteredRepo("support repair", repo)
	if err != nil {
		return err
	}
	params, _ := json.Marshal(map[string]any{"apply": false})
	request := supervisor.Request{Version: supervisor.ProtocolVersion,
		ID: fmt.Sprintf("repair-%d", time.Now().UnixNano()), Method: "repair",
		RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID,
		DeadlineMS: time.Now().Add(5 * time.Minute).UnixMilli(), Params: params}
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 5 * time.Minute}
	response, err := client.Do(ctx, request)
	if err != nil {
		return unavailableError(fmt.Sprintf("acd support repair: supervisor unavailable: %v", err))
	}
	if response.Error != nil {
		return actionRequiredError("repair_unavailable", response.Error.Message)
	}
	plan, err := decodeProductData[restorepkg.RepairPlan](response.Data)
	if err != nil {
		return fmt.Errorf("acd support repair: decode preview: %w", err)
	}
	if !apply {
		if jsonOut {
			next := "acd support repair --yes"
			return renderAnyProductEnvelope(out, productEnvelope{OK: true, State: productStateNeedsAction,
				Actions: []productAction{}, NextAction: &next, Data: plan}, true)
		}
		fmt.Fprintf(out, "Repair interrupted restore %s.\n", plan.OperationID)
		fmt.Fprintln(out, "Apply: acd support repair --yes")
		return nil
	}
	params, _ = json.Marshal(map[string]any{"apply": true, "operation_id": plan.OperationID})
	request.ID = fmt.Sprintf("repair-apply-%d", time.Now().UnixNano())
	request.Params = params
	response, err = client.Do(ctx, request)
	if err != nil {
		return unavailableError(fmt.Sprintf("acd support repair: supervisor unavailable: %v", err))
	}
	if response.Error != nil {
		return actionRequiredError("repair_failed", response.Error.Message)
	}
	result, err := decodeProductData[restorepkg.Result](response.Data)
	if err != nil {
		return err
	}
	if jsonOut {
		return renderAnyProductEnvelope(out, productEnvelope{OK: true, State: productStateWaiting,
			Changed: true, Actions: []productAction{{Kind: "repair", Status: "completed", Target: plan.OperationID}}, Data: result}, true)
	}
	fmt.Fprintf(out, "Completed interrupted restore %s.\n", plan.OperationID)
	fmt.Fprintf(out, "Undo with: acd restore %s\n", result.UndoCheckpoint)
	return nil
}

func decodeProductData[T any](value any) (T, error) {
	var target T
	body, err := json.Marshal(value)
	if err != nil {
		return target, err
	}
	if err := json.Unmarshal(body, &target); err != nil {
		return target, err
	}
	return target, nil
}

func renderAnyProductEnvelope(out io.Writer, envelope productEnvelope, jsonOut bool) error {
	if !jsonOut {
		return errors.New("acd: internal renderer requires JSON")
	}
	if envelope.Actions == nil {
		envelope.Actions = []productAction{}
	}
	return renderJSONEnvelope(out, envelope)
}

func newUninstallCmd() *cobra.Command {
	var dryRun, yes, purgeData, confirmPurge, nonInteractive bool
	var expectedPlan string
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove ACD while preserving protected repository data",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			jsonOut, _ := cmd.Flags().GetBool("json")
			if dryRun && yes {
				return invalidCommandError("acd uninstall: --dry-run cannot be combined with --yes")
			}
			if nonInteractive && !yes && !dryRun {
				return invalidCommandError("acd uninstall: --non-interactive apply requires --yes")
			}
			roots, err := paths.Resolve()
			if err != nil {
				return err
			}
			plan, err := installer.BuildUninstallPlan(cmd.Context(), roots, purgeData)
			if err != nil {
				return fmt.Errorf("acd uninstall: %w", err)
			}
			if expectedPlan != "" && expectedPlan != plan.Digest {
				return invalidCommandError("acd uninstall: plan digest changed: got %s, expected %s", plan.Digest, expectedPlan)
			}
			if dryRun || !yes {
				if jsonOut {
					return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateOff, Actions: []productAction{}, Data: plan}, true)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Uninstall plan %s\n", plan.Digest)
				for index, action := range plan.Actions {
					fmt.Fprintf(cmd.OutOrStdout(), "%d. %s: %s (%s)\n", index+1, action.Kind, action.Target, action.Detail)
				}
				if purgeData && !confirmPurge {
					fmt.Fprintln(cmd.OutOrStdout(), "Protected data purge requires --confirm-purge-data when applying.")
				}
				return nil
			}
			if nonInteractive && expectedPlan == "" {
				return invalidCommandError("acd uninstall: --expect-plan %s is required non-interactively", plan.Digest)
			}
			if purgeData && !confirmPurge {
				return invalidCommandError("acd uninstall: --purge-data requires the second --confirm-purge-data confirmation")
			}
			if !nonInteractive {
				fmt.Fprint(cmd.ErrOrStderr(), "Apply this uninstall plan? [y/N] ")
				answer, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				answer = strings.ToLower(strings.TrimSpace(answer))
				if answer != "y" && answer != "yes" {
					return nil
				}
				if purgeData {
					fmt.Fprint(cmd.ErrOrStderr(), "Permanently delete all listed ACD data? [y/N] ")
					answer, _ = bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
					answer = strings.ToLower(strings.TrimSpace(answer))
					if answer != "y" && answer != "yes" {
						return nil
					}
				}
			}
			result, err := installer.ApplyUninstall(cmd.Context(), roots, plan, nil)
			if err != nil {
				return fmt.Errorf("acd uninstall: %w", err)
			}
			if jsonOut {
				return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateOff, Changed: true, Actions: []productAction{{Kind: "uninstall", Status: "completed"}}, Data: result}, true)
			}
			if purgeData {
				fmt.Fprintln(cmd.OutOrStdout(), "ACD uninstalled. All verified ACD data and private refs were permanently removed.")
			} else {
				fmt.Fprintln(cmd.OutOrStdout(), "ACD uninstalled. Repository checkpoints and databases were preserved.")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show the exact uninstall plan without writes or service actions")
	cmd.Flags().BoolVar(&yes, "yes", false, "Apply the reviewed uninstall plan")
	cmd.Flags().BoolVar(&purgeData, "purge-data", false, "Also plan removal of checkpoint data after a second confirmation")
	cmd.Flags().BoolVar(&confirmPurge, "confirm-purge-data", false, "Second explicit confirmation for permanent protected-data deletion")
	cmd.Flags().BoolVar(&nonInteractive, "non-interactive", false, "Disable prompts; requires --yes and --expect-plan")
	cmd.Flags().StringVar(&expectedPlan, "expect-plan", "", "Require this exact sha256 plan digest")
	return cmd
}

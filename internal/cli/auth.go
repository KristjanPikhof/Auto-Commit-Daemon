package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

const maxCredentialInput = 64 * 1024

type authStatusReport struct {
	Path             string             `json:"path"`
	Source           credentials.Source `json:"source"`
	EnvironmentSet   bool               `json:"environment_set"`
	ProtectedFileSet bool               `json:"protected_file_set"`
}

func newAuthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage the protected provider credential",
		Long: `Manage the optional OpenAI-compatible API key stored in the protected
XDG credential file. ACD_AI_API_KEY remains higher priority. Secret values are
never printed by these commands.`,
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(newAuthSetCmd(), newAuthStatusCmd(), newAuthRemoveCmd())
	return cmd
}

func newAuthSetCmd() *cobra.Command {
	var fromStdin bool
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Store a protected OpenAI-compatible API key",
		Long: `Read an API key from a masked terminal prompt or, with --stdin, from
standard input. No command-line flag accepts a literal secret.`,
		Example: "  acd auth set\n  printf '%s\\n' \"$ACD_AI_API_KEY\" | acd auth set --stdin",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			roots, err := paths.Resolve()
			if err != nil {
				return fmt.Errorf("acd auth set: resolve paths: %w", err)
			}
			secret, err := readAuthSecret(cmd, fromStdin)
			if err != nil {
				return err
			}
			defer clearBytes(secret)
			store := credentials.NewStore(roots)
			if err := store.Set(string(secret)); err != nil {
				return fmt.Errorf("acd auth set: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Stored provider credential in %s\n", store.Path())
			return nil
		},
	}
	cmd.Flags().BoolVar(&fromStdin, "stdin", false, "Read the API key from standard input")
	return cmd
}

func newAuthStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the active credential source without its value",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			roots, err := paths.Resolve()
			if err != nil {
				return fmt.Errorf("acd auth status: resolve paths: %w", err)
			}
			store := credentials.NewStore(roots)
			status, err := store.Status()
			if err != nil {
				return fmt.Errorf("acd auth status: %w", err)
			}
			_, source, err := credentials.Resolve(store, os.LookupEnv)
			if err != nil {
				return fmt.Errorf("acd auth status: %w", err)
			}
			environmentSet := source == credentials.SourceEnvironment
			report := authStatusReport{
				Path: status.Path, Source: source, EnvironmentSet: environmentSet,
				ProtectedFileSet: status.ProtectedFileSet,
			}
			jsonOut, _ := cmd.Flags().GetBool("json")
			if jsonOut {
				encoder := json.NewEncoder(cmd.OutOrStdout())
				encoder.SetIndent("", "  ")
				return encoder.Encode(report)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Credential source: %s\n", report.Source)
			fmt.Fprintf(cmd.OutOrStdout(), "Environment override: %t\n", report.EnvironmentSet)
			fmt.Fprintf(cmd.OutOrStdout(), "Protected file configured: %t\n", report.ProtectedFileSet)
			fmt.Fprintf(cmd.OutOrStdout(), "Protected file: %s\n", report.Path)
			return nil
		},
	}
}

func newAuthRemoveCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove the protected provider credential",
		Long:  "Remove only the protected credential file. ACD_AI_API_KEY is unchanged.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !yes {
				return errors.New("acd auth remove: refusing to remove the protected credential without --yes")
			}
			roots, err := paths.Resolve()
			if err != nil {
				return fmt.Errorf("acd auth remove: resolve paths: %w", err)
			}
			store := credentials.NewStore(roots)
			removed, err := store.Remove()
			if err != nil {
				return fmt.Errorf("acd auth remove: %w", err)
			}
			if removed {
				fmt.Fprintln(cmd.OutOrStdout(), "Removed protected provider credential")
			} else {
				fmt.Fprintln(cmd.OutOrStdout(), "Protected provider credential was not configured")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm removal of the protected credential")
	return cmd
}

func readAuthSecret(cmd *cobra.Command, fromStdin bool) ([]byte, error) {
	if fromStdin {
		body, err := io.ReadAll(io.LimitReader(cmd.InOrStdin(), maxCredentialInput+1))
		if err != nil {
			return nil, fmt.Errorf("acd auth set: read standard input: %w", err)
		}
		if len(body) > maxCredentialInput {
			clearBytes(body)
			return nil, errors.New("acd auth set: standard input is too large")
		}
		return []byte(strings.TrimSpace(string(body))), nil
	}
	input, ok := cmd.InOrStdin().(*os.File)
	if !ok || !term.IsTerminal(input.Fd()) {
		return nil, errors.New("acd auth set: terminal input is required; use --stdin for piped input")
	}
	fmt.Fprint(cmd.ErrOrStderr(), "API key: ")
	secret, err := term.ReadPassword(input.Fd())
	fmt.Fprintln(cmd.ErrOrStderr())
	if err != nil {
		return nil, errors.New("acd auth set: read masked terminal input")
	}
	return secret, nil
}

func clearBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

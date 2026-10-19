package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/adapter"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

type setupAgentsOptions struct {
	Integrations, InstructionsFile string
	DryRun, Manual, Yes, Protect   bool
}

type setupAgentsPlan struct {
	Setup        installer.Plan        `json:"setup"`
	Instructions agentInstructionsPlan `json:"instructions"`
	Guidance     string                `json:"recommended_agent_instructions"`
	Commands     []string              `json:"manual_commands"`
	ProtectRepo  string                `json:"protect_repository,omitempty"`
}

// Tests replace the installer boundary, not the guide's decisions or file work.
var applySetupAgentsHooks = func(ctx context.Context, roots paths.Roots, plan installer.Plan) (installer.Result, error) {
	return installer.Apply(ctx, roots, plan, installer.ApplyOptions{
		Quiesce: func(ctx context.Context) error {
			return runStopRegistry(ctx, io.Discard, true, false, plan.Registry)
		},
	})
}

func newSetupAgentsCmd() *cobra.Command {
	var options setupAgentsOptions
	cmd := &cobra.Command{
		Use: "agents", Short: "Set up coding-tool hooks and agent instructions",
		Long: `Choose coding tools, review hook changes, and copy the ACD agent instructions.
You can also add the instructions to a file you choose and enable protection
for the current repository. Every change is previewed before confirmation.

In a terminal, the guide asks which tools and instruction file to use. Outside
a terminal it prints a preview; --yes applies the selected options. --manual
and --dry-run always leave files and protection unchanged.

Hooks use the existing setup transaction, which can install or update the ACD
runtime. Those changes appear in the preview too.`,
		Example: `  acd setup agents
  acd setup agents --manual
  acd setup agents --integrations=codex --dry-run
  acd setup agents --integrations=codex --instructions-file=AGENTS.md --yes
  acd setup agents --integrations=none --instructions-file=~/.codex/AGENTS.md --dry-run`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSetupAgents(cmd, options)
		},
	}
	cmd.Flags().StringVar(&options.Integrations, "integrations", "auto", "Tools: auto, none, or comma-separated names")
	cmd.Flags().StringVar(&options.InstructionsFile, "instructions-file", "", "Instruction file to update; otherwise print the copy only")
	cmd.Flags().BoolVar(&options.DryRun, "dry-run", false, "Preview without changing anything")
	cmd.Flags().BoolVar(&options.Manual, "manual", false, "Print instructions and commands without changing anything")
	cmd.Flags().BoolVar(&options.Yes, "yes", false, "Apply the displayed choices without a confirmation prompt")
	cmd.Flags().BoolVar(&options.Protect, "protect", false, "Also enable protection for the selected repository")
	return cmd
}

func agentSetupAnswer(reader *bufio.Reader, out io.Writer, question, fallback string) (string, error) {
	fmt.Fprintf(out, "%s [%s]: ", question, fallback)
	line, err := reader.ReadString('\n')
	if errors.Is(err, io.EOF) && line == "" {
		return "", io.EOF
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return fallback, nil
	}
	return line, nil
}

func detectedAgentTools() []string {
	var names []string
	for _, name := range adapter.Names() {
		harness, _ := adapter.Lookup(name)
		path := harness.ConfigPath()
		if _, err := os.Stat(path); err == nil {
			names = append(names, name)
		} else if name != "shell" {
			if info, err := os.Stat(filepath.Dir(path)); err == nil && info.IsDir() {
				names = append(names, name)
			}
		}
	}
	return names
}

func chooseAgentSetup(cmd *cobra.Command, reader *bufio.Reader, options *setupAgentsOptions, repo string) error {
	out := cmd.ErrOrStderr()
	if !cmd.Flags().Changed("integrations") {
		fmt.Fprintf(out, "1. Coding tools\nAvailable: %s\n", strings.Join(adapter.Names(), ", "))
		defaults := strings.Join(detectedAgentTools(), ",")
		if defaults == "" {
			defaults = "none"
		}
		answer, err := agentSetupAnswer(reader, out, "Tools to configure (comma-separated names, or none)", defaults)
		if err != nil {
			return err
		}
		options.Integrations = strings.ToLower(answer)
	}
	if !cmd.Flags().Changed("instructions-file") {
		fmt.Fprintln(out, "2. Agent instructions\nChoose copy, repo (AGENTS.md), or file (a global or custom instruction file).")
		choice, err := agentSetupAnswer(reader, out, "Where should the instructions go?", "copy")
		if err != nil {
			return err
		}
		switch strings.ToLower(choice) {
		case "copy":
		case "repo":
			worktree, err := git.ResolveWorktree(cmd.Context(), repo)
			if err != nil {
				return err
			}
			options.InstructionsFile = filepath.Join(worktree.Root, "AGENTS.md")
		case "file":
			options.InstructionsFile, err = agentSetupAnswer(reader, out, "Instruction file (for example ~/.codex/AGENTS.md)", "")
			if err != nil {
				return err
			}
			if options.InstructionsFile == "" {
				return invalidCommandError("Choose an instruction file or use the copy option.")
			}
		default:
			return invalidCommandError("Choose copy, repo, or file.")
		}
	}
	if !cmd.Flags().Changed("protect") {
		if worktree, err := git.ResolveWorktree(cmd.Context(), repo); err == nil {
			answer, err := agentSetupAnswer(reader, out, "3. Protect "+worktree.Root+" after setup?", "N")
			if err != nil {
				return err
			}
			options.Protect = strings.EqualFold(answer, "y") || strings.EqualFold(answer, "yes")
		}
	}
	return nil
}

func runSetupAgents(cmd *cobra.Command, options setupAgentsOptions) error {
	jsonOut, _ := cmd.Flags().GetBool("json")
	quiet, _ := cmd.Flags().GetBool("quiet")
	repo, _ := cmd.Flags().GetString("repo")
	repoSelected := repo != ""
	if repo == "" {
		repo = "."
	}
	reader := bufio.NewReader(cmd.InOrStdin())
	interactive := !jsonOut && !quiet && !options.Manual && !options.DryRun && !options.Yes &&
		settingsInputTTY(cmd.InOrStdin()) && settingsOutputTTY(cmd.OutOrStdout())
	if interactive {
		if err := chooseAgentSetup(cmd, reader, &options, repo); err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Fprintln(cmd.OutOrStdout(), "Agent setup cancelled. No changes applied.")
				return nil
			}
			return err
		}
	}
	roots, err := paths.Resolve()
	if err != nil {
		return err
	}
	if options.Integrations == "auto" {
		options.Integrations = strings.Join(detectedAgentTools(), ",")
		if options.Integrations == "" {
			options.Integrations = "none"
		}
	}
	guidance, err := fs.ReadFile(templatesFS, "agent-guidance.md")
	if err != nil {
		return err
	}
	plan := setupAgentsPlan{Guidance: string(guidance)}
	instructionPath := options.InstructionsFile
	if repoSelected && instructionPath != "" && !filepath.IsAbs(instructionPath) && !strings.HasPrefix(instructionPath, "~/") {
		worktree, err := git.ResolveWorktree(cmd.Context(), repo)
		if err != nil {
			return err
		}
		instructionPath = filepath.Join(worktree.Root, instructionPath)
	}
	plan.Instructions, err = planAgentInstructions(instructionPath, plan.Guidance)
	if err != nil {
		return err
	}
	if options.Protect {
		worktree, err := git.ResolveWorktree(cmd.Context(), repo)
		if err != nil {
			return err
		}
		plan.ProtectRepo = worktree.Root
	}
	plan.Setup, err = installer.BuildPlan(cmd.Context(), roots, installer.Options{Integrations: options.Integrations})
	if err != nil {
		return err
	}
	baseCommand := "acd setup agents --integrations=" + productListShellQuote(options.Integrations)
	if options.InstructionsFile != "" {
		baseCommand += " --instructions-file=" + productListShellQuote(plan.Instructions.Path)
	}
	if plan.ProtectRepo != "" {
		baseCommand += " --protect --repo " + productListShellQuote(plan.ProtectRepo)
	}
	plan.Commands = []string{baseCommand + " --dry-run", baseCommand + " --yes"}
	for _, name := range plan.Setup.Integrations {
		plan.Commands = append(plan.Commands, "acd setup "+name+" --raw")
	}
	if !jsonOut {
		if err := renderSetupAgentsPlan(cmd, plan); err != nil {
			return err
		}
	}
	apply := options.Yes && !options.DryRun && !options.Manual
	if interactive {
		answer, err := agentSetupAnswer(reader, cmd.ErrOrStderr(), "Apply this preview?", "N")
		if errors.Is(err, io.EOF) {
			fmt.Fprintln(cmd.OutOrStdout(), "Preview only. No changes applied.")
			return nil
		}
		if err != nil {
			return err
		}
		apply = strings.EqualFold(answer, "y") || strings.EqualFold(answer, "yes")
	}
	if !apply {
		if jsonOut {
			return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateReady, Actions: []productAction{}, Data: plan}, true)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Preview only. No changes applied.")
		return nil
	}
	// Recheck the instruction preimage before the hook transaction too. The
	// instruction writer checks it again after hooks complete.
	if err := plan.Instructions.revalidate(); err != nil {
		return err
	}
	result, err := applySetupAgentsHooks(cmd.Context(), roots, plan.Setup)
	if err != nil {
		return fmt.Errorf("acd setup agents: hook setup failed: %w", err)
	}
	backup, err := plan.Instructions.apply(roots)
	if err != nil {
		return fmt.Errorf("acd setup agents: hooks were configured, but instructions were not updated: %w", err)
	}
	if plan.ProtectRepo != "" {
		if err := runControlOn(cmd.Context(), io.Discard, plan.ProtectRepo, false); err != nil {
			return fmt.Errorf("acd setup agents: hooks and instructions are ready, but protection could not be confirmed: %w", err)
		}
	}
	if jsonOut {
		return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateReady, Changed: result.Changed || plan.Instructions.Changed || plan.ProtectRepo != "",
			Actions: []productAction{}, Data: map[string]any{"plan": plan, "instructions_backup": backup, "protection_enabled": plan.ProtectRepo != ""}}, true)
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Agent setup complete. Hook configuration was checked by setup.")
	if plan.Instructions.Path == "" {
		fmt.Fprintln(cmd.OutOrStdout(), "Instructions: copy the section above into AGENTS.md or your tool's instruction file.")
	} else if plan.Instructions.Changed {
		fmt.Fprintf(cmd.OutOrStdout(), "Instructions saved: %s\n", plan.Instructions.Path)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "Instructions: %s (%s).\n", plan.Instructions.Path, plan.Instructions.Status)
	}
	if backup != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Instruction backup: %s\n", backup)
	}
	if plan.ProtectRepo != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Protection confirmed: %s\n", plan.ProtectRepo)
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Next: Restart or reload your coding tool if it requires that for hook changes. Run `acd doctor` inside a repository to check protection.")
	return nil
}

func renderSetupAgentsPlan(cmd *cobra.Command, plan setupAgentsPlan) error {
	out := cmd.OutOrStdout()
	fmt.Fprintln(out, "Agent setup preview\n\n1. Hooks and runtime")
	for _, hook := range plan.Setup.IntegrationPlans {
		fmt.Fprintf(out, "  %s: %s (change: %s)\n", hook.Name, hook.Target, yesNo(hook.Changed))
	}
	if len(plan.Setup.IntegrationPlans) == 0 {
		fmt.Fprintln(out, "  No coding-tool hooks selected.")
	}
	if err := renderSetupPlan(cmd, plan.Setup, false); err != nil {
		return err
	}
	fmt.Fprintln(out, "\n2. Agent instructions")
	if plan.Instructions.Path == "" {
		fmt.Fprintln(out, "Copy only. Instruction files stay unchanged.")
	} else {
		fmt.Fprintf(out, "File: %s\nAction: %s\n", plan.Instructions.Path, plan.Instructions.Status)
	}
	fmt.Fprintf(out, "\nCopy-ready AGENTS.md section:\n\n%s\n", plan.Guidance)
	fmt.Fprintln(out, "3. Repository protection")
	if plan.ProtectRepo == "" {
		fmt.Fprintln(out, "Leave protection settings unchanged. To enable a repository, run: acd on --repo /path/to/repository")
	} else {
		fmt.Fprintf(out, "Enable protection: %s\n", plan.ProtectRepo)
	}
	fmt.Fprintln(out, "\nManual setup commands (raw hook snippets must be merged, not redirected over existing settings):")
	for _, command := range plan.Commands {
		fmt.Fprintln(out, command)
	}
	return nil
}

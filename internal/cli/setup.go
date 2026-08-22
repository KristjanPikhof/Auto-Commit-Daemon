package cli

// Checkpoint-first transactional setup plus the hidden legacy snippet route.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/adapter"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

// supportedHarnesses is the canonical ordered list of harness identifiers.
var supportedHarnesses = adapter.Names()

// harnessSnippet describes which file inside templates/<harness>/ is the
// primary snippet and what comment prefix to use for the header/footer line.
type harnessSnippet struct {
	file          string // relative path inside templates/
	commentPrefix string // language-appropriate comment marker
}

var harnessSnippets = map[string]harnessSnippet{
	"claude-code": {"claude-code/settings.snippet.json", "//"},
	// codex points at `codex/hooks.json` (not `*.snippet.json`) by design:
	// `~/.codex/hooks.json` is consumed verbatim by Codex and must be strict
	// JSON, so the template ships the full file content rather than a
	// `.snippet.*` excerpt. `acd setup codex --raw > ~/.codex/hooks.json`
	// is the canonical install path. Keep this entry's filename in sync
	// with `templates/codex/hooks.json` rather than renaming it to a
	// `.snippet.json` form, which would imply a partial fragment.
	"codex": {"codex/hooks.json", "//"},
	// cursor ships the full ~/.cursor/hooks.json body (strict JSON), same pattern
	// as codex: `acd setup cursor --raw > ~/.cursor/hooks.json`.
	"cursor":   {"cursor/hooks.json", "//"},
	"opencode": {"opencode/hooks.snippet.yaml", "#"},
	"pi":       {"pi/hooks.snippet.yaml", "#"},
	// shell prints both snippet files separated by a divider.
	"shell": {"shell/direnv.envrc.snippet", "#"},
}

// shellExtra is the second snippet for the shell harness (zshrc).
const shellZshrcSnippet = "shell/zshrc.snippet.sh"

// templatesFS is the embedded FS used by runSetup. Tests overlay it via
// setTemplatesFSForTest to inject malformed JSON without touching the
// production templates package. Production callers always use templates.FS.
var templatesFS fs.FS = templates.FS

// readmeFile returns the README path for a harness.
func readmeFile(harness string) string {
	return harness + "/README.md"
}

func newSetupCmd() *cobra.Command {
	return newSetupCommand(false)
}

func newSetupInitCompatCmd() *cobra.Command {
	return newSetupCommand(true)
}

func newSetupCommand(initCompat bool) *cobra.Command {
	var dryRun, yes, nonInteractive, rawFlag bool
	var expectedPlan, integrations string
	var onboarding setupOnboardingOptions
	use := "setup"
	if initCompat {
		use = "init [harness]"
	}

	cmd := &cobra.Command{
		Use:   use,
		Short: "Safely install or upgrade ACD",
		Long: `Inspect the current installation and show the exact setup plan before
changing anything. The plan installs or upgrades ACD, starts its background
service, and updates user-level integrations as one rollback-safe operation.
It does not enable the current repository. Run acd on once in each repository
that ACD should protect.

Start with --dry-run when you want a preview. The default setup works without
an API key. Fresh setup asks how to group and format commits, then lets you use
the local provider or test an OpenAI-compatible endpoint. On macOS, run setup
from the terminal or coding tool that should own the ACD service.
Full Disk Access is not required.`,
		Example: `  acd setup
  acd setup --dry-run
  acd setup --yes --non-interactive --expect-plan sha256:...`,
		Args:         cobra.RangeArgs(0, 1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if initCompat {
				fmt.Fprintln(cmd.ErrOrStderr(), "warning: acd init is a compatibility alias; use acd setup")
			}
			// Two-release compatibility for `setup <harness> --raw`; it prints
			// only and never enters the installer transaction.
			if len(args) == 1 {
				for _, name := range []string{
					"dry-run", "yes", "non-interactive", "expect-plan", "integrations", "repo",
					"experience", "commit-format", "provider", "base-url", "model", "ca-file",
					"credential-stdin", "accessible", "confirm-endpoint-credentials",
					"confirm-insecure-http", "confirm-diff-egress", "confirm-intent-repair",
				} {
					if flagWasSet(cmd, name) {
						return invalidCommandError("acd setup %s: --%s is not supported by the compatibility print route", args[0], name)
					}
				}
				jsonOut, _ := cmd.Flags().GetBool("json")
				if jsonOut {
					return invalidCommandError("acd setup integration compatibility output does not support --json")
				}
				fmt.Fprintln(cmd.ErrOrStderr(), "warning: setup snippet printing is a compatibility route; use setup --integrations="+args[0])
				return runSetup(cmd, args[0], rawFlag)
			}
			if rawFlag {
				return invalidCommandError("acd setup: --raw requires a compatibility harness argument")
			}
			return runTransactionalSetup(cmd, dryRun, yes, nonInteractive, expectedPlan, integrations, onboarding)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show the setup plan without changing anything")
	cmd.Flags().BoolVar(&yes, "yes", false, "Apply the reviewed setup plan")
	cmd.Flags().BoolVar(&nonInteractive, "non-interactive", false, "Disable prompts; requires --yes")
	cmd.Flags().StringVar(&expectedPlan, "expect-plan", "", "Apply only this exact plan ID")
	cmd.Flags().StringVar(&integrations, "integrations", "auto", "Coding-tool integrations: auto, none, or a comma-separated list")
	cmd.Flags().BoolVar(&rawFlag, "raw", false, "Compatibility: print a harness snippet without instructions")
	cmd.Flags().StringVar(&onboarding.Experience, "experience", "", "First setup: everyday or speed")
	cmd.Flags().StringVar(&onboarding.CommitFormat, "commit-format", "", "First setup: imperative or conventional")
	cmd.Flags().StringVar(&onboarding.Provider, "provider", "", "First setup: deterministic or openai-compat")
	cmd.Flags().StringVar(&onboarding.BaseURL, "base-url", "", "OpenAI-compatible base URL")
	cmd.Flags().StringVar(&onboarding.Model, "model", "", "OpenAI-compatible model name")
	cmd.Flags().StringVar(&onboarding.CAFile, "ca-file", "", "Optional PEM CA certificate file")
	cmd.Flags().BoolVar(&onboarding.CredentialStdin, "credential-stdin", false, "Read the bearer token from the first input line")
	cmd.Flags().BoolVar(&onboarding.Accessible, "accessible", false, "Use linear screen-reader-friendly prompts")
	cmd.Flags().BoolVar(&onboarding.ConfirmEndpointCredentials, "confirm-endpoint-credentials", false, "Allow the bearer token to be sent to the reviewed endpoint")
	cmd.Flags().BoolVar(&onboarding.ConfirmInsecureHTTP, "confirm-insecure-http", false, "Allow unencrypted HTTP after reviewing the warning")
	cmd.Flags().BoolVar(&onboarding.ConfirmDiffEgress, "confirm-diff-egress", false, "Allow redacted repository changes to be sent later")
	cmd.Flags().BoolVar(&onboarding.ConfirmIntentRepair, "confirm-intent-repair", false, "Allow bounded repair of private ACD-owned commits")
	_ = cmd.Flags().MarkHidden("raw")
	return cmd
}

func runTransactionalSetup(cmd *cobra.Command, dryRun, yes, nonInteractive bool, expectedPlan, integrations string, onboarding setupOnboardingOptions) error {
	repo, _ := cmd.Flags().GetString("repo")
	jsonOut, _ := cmd.Flags().GetBool("json")
	quiet, _ := cmd.Flags().GetBool("quiet")
	input := bufio.NewReader(cmd.InOrStdin())
	if dryRun && yes {
		return invalidCommandError("acd setup: --dry-run cannot be combined with --yes")
	}
	if nonInteractive && !yes && !dryRun {
		return invalidCommandError("acd setup: --non-interactive apply requires --yes")
	}
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd setup: resolve paths: %w", err)
	}
	planningProgress := newSetupProgress(cmd.ErrOrStderr(), jsonOut || quiet, 10*time.Second)
	planningProgress.Update(installer.Progress{
		Phase: "plan", Detail: "Inspecting the ACD installation and preparing the exact setup plan",
	})
	plan, err := installer.BuildPlan(cmd.Context(), roots, installer.Options{Repo: repo, Integrations: integrations, NonInteractive: nonInteractive, ExpectedPlan: expectedPlan})
	planningProgress.Close()
	if err != nil {
		return fmt.Errorf("acd setup: %w", err)
	}
	var onboardingState *setupOnboardingState
	if plan.FreshDefaults {
		onboardingState, err = prepareSetupOnboarding(cmd, roots, onboarding, dryRun, nonInteractive)
		if err != nil {
			return err
		}
		plan, err = installer.BuildPlan(cmd.Context(), roots, installer.Options{
			Repo: repo, Integrations: integrations, NonInteractive: nonInteractive,
			ExpectedPlan: expectedPlan, Configuration: &onboardingState.Configuration,
		})
		if err != nil {
			return fmt.Errorf("acd setup: rebuild reviewed plan: %w", err)
		}
	}
	if expectedPlan != "" && expectedPlan != plan.Digest {
		return invalidCommandError("acd setup: plan digest changed: got %s, expected %s", plan.Digest, expectedPlan)
	}
	if dryRun {
		return renderSetupPlan(cmd, plan, jsonOut)
	}
	if len(plan.Actions) == 1 && plan.Actions[0].Kind == "verify_compatible_runtime" {
		if jsonOut {
			return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateReady,
				Changed: false, Actions: []productAction{}, Data: plan}, true)
		}
		if err := renderSetupPlan(cmd, plan, false); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Status: ACD installation is ready.")
		renderSetupNextAction(cmd.OutOrStdout(), plan)
		return nil
	}
	if !jsonOut {
		if err := renderSetupPlan(cmd, plan, false); err != nil {
			return err
		}
	}
	if nonInteractive {
		if plan.RequiresExpected && expectedPlan == "" {
			return invalidCommandError("acd setup: --expect-plan %s is required for non-interactive upgrade", plan.Digest)
		}
	} else if !yes {
		fmt.Fprint(cmd.ErrOrStderr(), "Apply this setup plan? [y/N] ")
		answer, _ := input.ReadString('\n')
		answer = strings.ToLower(strings.TrimSpace(answer))
		if answer != "y" && answer != "yes" {
			return nil
		}
	}
	for onboardingState != nil {
		testErr := testSetupProvider(cmd, onboardingState)
		if testErr == nil {
			break
		}
		if errors.Is(testErr, errSetupUseLocalProvider) {
			onboardingState, err = useLocalSetupProvider(cmd, onboardingState)
		} else if errors.Is(testErr, errSetupEditConnection) {
			onboardingState, err = prepareSetupOnboarding(cmd, roots, onboarding, false, false)
		} else {
			return testErr
		}
		if err != nil {
			return err
		}
		plan, err = installer.BuildPlan(cmd.Context(), roots, installer.Options{
			Repo: repo, Integrations: integrations, Configuration: &onboardingState.Configuration,
		})
		if err != nil {
			return fmt.Errorf("acd setup: rebuild changed provider plan: %w", err)
		}
		if err := renderSetupPlan(cmd, plan, false); err != nil {
			return err
		}
		fmt.Fprint(cmd.ErrOrStderr(), "Apply this changed setup plan? [y/N] ")
		answer, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
		answer = strings.ToLower(strings.TrimSpace(answer))
		if answer != "y" && answer != "yes" {
			return errors.New("acd setup: setup stopped; nothing was written")
		}
	}
	progress := newSetupProgress(cmd.ErrOrStderr(), jsonOut || quiet, 10*time.Second)
	defer progress.Close()
	applyOptions := installer.ApplyOptions{
		Quiesce: func(ctx context.Context) error {
			return runStopRegistry(ctx, io.Discard, true, false, plan.Registry)
		},
		Progress: progress.Update,
		Credential: func() string {
			if onboardingState == nil {
				return ""
			}
			return onboardingState.Credential
		}(),
	}
	result, err := installer.Apply(cmd.Context(), roots, plan, applyOptions)
	progress.Close()
	if err != nil {
		return fmt.Errorf("acd setup: %w", err)
	}
	if jsonOut {
		return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateReady, Changed: result.Changed,
			Actions: []productAction{{Kind: "setup", Status: "completed", Target: plan.ManagedBinary}}, Data: struct {
				Result installer.Result `json:"result"`
				Plan   installer.Plan   `json:"plan"`
			}{Result: result, Plan: plan}}, true)
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Setup complete.")
	fmt.Fprintln(cmd.OutOrStdout(), "Status: ACD installation is ready.")
	fmt.Fprintln(cmd.OutOrStdout(), "Repositories: Existing enablement was preserved; no repository was added.")
	if onboardingState != nil {
		fmt.Fprintf(cmd.OutOrStdout(), "Preferences: %s, %s commits, %s provider.\n",
			setupExperience(onboardingState.Draft),
			onboardingState.Selection.CommitFormat,
			onboardingState.Selection.Provider)
		fmt.Fprintln(cmd.OutOrStdout(), "Provider test: passed with synthetic content.")
	}
	renderSetupNextAction(cmd.OutOrStdout(), plan)
	return nil
}

func renderSetupNextAction(out io.Writer, plan installer.Plan) {
	if len(plan.Warnings) > 0 {
		for _, warning := range plan.Warnings {
			fmt.Fprintf(out, "Warning: %s\n", warning)
		}
		return
	}
	if plan.FreshDefaults {
		fmt.Fprintln(out, "Next: Run `acd on` inside each repository you want ACD to protect.")
		return
	}
	fmt.Fprintln(out, "Next: No action needed. Run `acd on` inside any new repository you want to protect.")
}

type setupProgress struct {
	out      io.Writer
	interval time.Duration
	silent   bool

	mu       sync.Mutex
	phase    string
	detail   string
	started  time.Time
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

func newSetupProgress(out io.Writer, silent bool, interval time.Duration) *setupProgress {
	p := &setupProgress{out: out, interval: interval, silent: silent, stop: make(chan struct{}), done: make(chan struct{})}
	if silent || interval <= 0 {
		close(p.done)
		return p
	}
	go p.heartbeat()
	return p
}

func (p *setupProgress) Update(update installer.Progress) {
	if p == nil || p.silent {
		return
	}
	p.mu.Lock()
	if update.Phase != p.phase {
		p.phase = update.Phase
		p.started = time.Now()
	}
	p.detail = update.Detail
	fmt.Fprintf(p.out, "Setup: %s\n", update.Detail)
	p.mu.Unlock()
}

func (p *setupProgress) heartbeat() {
	ticker := time.NewTicker(p.interval)
	defer func() {
		ticker.Stop()
		close(p.done)
	}()
	for {
		select {
		case <-ticker.C:
			p.mu.Lock()
			elapsed := time.Since(p.started)
			if p.detail != "" && elapsed >= p.interval {
				display := elapsed.Round(time.Second).String()
				if elapsed < time.Second {
					display = "<1s"
				}
				fmt.Fprintf(p.out, "Setup: still working on %s (%s elapsed)\n", strings.ToLower(p.detail), display)
			}
			p.mu.Unlock()
		case <-p.stop:
			return
		}
	}
}

func (p *setupProgress) Close() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		if !p.silent && p.interval > 0 {
			close(p.stop)
		}
		<-p.done
	})
}

func renderSetupPlan(cmd *cobra.Command, plan installer.Plan, jsonOut bool) error {
	if jsonOut {
		return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateReady,
			Actions: []productAction{}, Data: plan}, true)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Setup plan: %s\n", plan.Digest)
	if plan.Configuration != nil {
		values := plan.Configuration.Values
		fmt.Fprintf(cmd.OutOrStdout(), "Preferences: %s, %s commit messages.\n",
			setupExperience(values), values["commit.format"])
		fmt.Fprintf(cmd.OutOrStdout(), "Provider: %s", values["ai.provider"])
		if values["ai.provider"] == "openai-compat" {
			fmt.Fprintf(cmd.OutOrStdout(), " at %s using %s", values["ai.base_url"], values["ai.model"])
		}
		fmt.Fprintln(cmd.OutOrStdout(), ".")
		if strings.HasPrefix(strings.ToLower(values["ai.base_url"]), "http://") {
			fmt.Fprintln(cmd.OutOrStdout(), "Warning: HTTP is not encrypted. The bearer token and later requests can be read or changed in transit.")
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Connection test: fixed synthetic text only. No repository content is included.")
		if values["ai.diff_egress"] == "true" {
			fmt.Fprintln(cmd.OutOrStdout(), "Later requests: redacted change context may be sent to this provider.")
		} else {
			fmt.Fprintln(cmd.OutOrStdout(), "Later requests: repository content stays on this machine.")
		}
		if values["intent.repair.enabled"] == "true" {
			fmt.Fprintln(cmd.OutOrStdout(), "Repair: limited to recent, private, ACD-owned commits. Pushed and user-owned history is never rewritten.")
		}
		if plan.Configuration.CredentialSource == "environment" {
			fmt.Fprintln(cmd.OutOrStdout(), "Credential: ACD_AI_API_KEY from the environment. No credential file will be written.")
		} else if plan.Configuration.StoreCredential {
			fmt.Fprintln(cmd.OutOrStdout(), "Credential: stored in the protected user credential file with mode 0600.")
		} else {
			fmt.Fprintln(cmd.OutOrStdout(), "Credential: not needed for the local provider.")
		}
	}
	for _, warning := range plan.Warnings {
		fmt.Fprintf(cmd.OutOrStdout(), "Warning: %s\n", warning)
	}
	for index, action := range plan.Actions {
		fmt.Fprintf(cmd.OutOrStdout(), "%d. %s\n", index+1, setupActionDescription(action))
		if action.Target != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "   Target: %s\n", action.Target)
		}
	}
	fmt.Fprintln(cmd.OutOrStdout(), "No changes have been applied yet. Review this global setup plan before applying it.")
	fmt.Fprintf(cmd.OutOrStdout(), "Repository impact: %d enabled repository database(s) will be migrated; %d disabled repository database(s) will be left unchanged.\n",
		len(plan.Repositories), plan.DeferredRepositories)
	return nil
}

func setupActionDescription(action installer.Action) string {
	switch action.Kind {
	case "backup":
		return "Back up the current ACD installation and repository protection data"
	case "install_binary":
		return "Install the new ACD program safely"
	case "migrate":
		return "Upgrade enabled repositories to the current protection format"
	case "defer_disabled_migrations":
		return "Leave disabled repository protection data unchanged until its next `acd on`"
	case "start_supervisor", "start_session_supervisor", "restart_session_supervisor", "install_service":
		return "Start ACD's background service"
	case "write_registry":
		return "Update the list of protected repositories"
	case "self_test":
		return "Verify checkpoint protection, Git publication, and restore in an isolated test"
	case "import_recovery_checkpoints":
		return "Keep recovery checkpoints created by an earlier upgrade"
	case "disable_missing_repository":
		return "Keep a missing repository registration disabled without deleting its record"
	case "merge_integration":
		return "Update the coding-tool integration without changing unrelated settings"
	case "verify_compatible_runtime":
		return "Check that ACD and its coding-tool integrations are current"
	case "checkpoint":
		return "Protect current changes before the upgrade"
	case "save_preferences":
		return "Save the reviewed preferences as user defaults for current and future repositories"
	default:
		return action.Detail
	}
}

func runSetup(cmd *cobra.Command, harness string, raw bool) error {
	if harness == "" {
		detected := adapter.DetectInstalled()
		switch len(detected) {
		case 0:
			fmt.Fprintf(cmd.ErrOrStderr(),
				"acd setup: no harness specified and no acd-managed harness install was detected\nSupported harnesses: %s\n",
				strings.Join(supportedHarnesses, ", "))
			return fmt.Errorf("acd setup: no harness specified")
		case 1:
			harness = detected[0].Name()
		default:
			var names []string
			for _, h := range detected {
				names = append(names, h.Name())
			}
			fmt.Fprintf(cmd.ErrOrStderr(),
				"acd setup: multiple acd-managed harness installs detected: %s\nRun acd setup <harness> with one of the detected harnesses.\n",
				strings.Join(names, ", "))
			return fmt.Errorf("acd setup: multiple harnesses detected")
		}
	}

	if _, known := adapter.Lookup(harness); !known {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"acd setup: unknown harness %q\nSupported harnesses: %s\n",
			harness, strings.Join(supportedHarnesses, ", "))
		return fmt.Errorf("acd setup: unknown harness %q", harness)
	}

	meta := harnessSnippets[harness]
	cp := meta.commentPrefix
	embeddedFS := templatesFS

	out := cmd.OutOrStdout()

	if raw {
		// Raw mode: emit just the snippet body so the output can be redirected
		// directly into a strict-JSON config (no comment syntax allowed).
		if harness == "shell" {
			if err := printSnippet(out, embeddedFS, meta.file); err != nil {
				return err
			}
			// Guarantee a blank line between the direnv and zshrc snippets so
			// the concatenated body parses as bash even if either snippet's
			// last line lacks a trailing newline. bash -n must succeed on the
			// joined body when redirected into a startup file.
			if _, err := out.Write([]byte("\n")); err != nil {
				return err
			}
			if err := printSnippet(out, embeddedFS, shellZshrcSnippet); err != nil {
				return err
			}
			return nil
		}
		// Strict-JSON validation guard: when the canonical install path is a
		// `.json` config (e.g. `~/.codex/hooks.json`), users redirect the
		// raw output directly into the file. If a future template edit ships
		// invalid JSON (trailing comma, unescaped quote, schema drift), the
		// harness silently ignores the file. Validate before emitting so the
		// regression surfaces as a non-zero exit with an actionable error
		// instead of a silently broken install.
		if filepath.Ext(meta.file) == ".json" {
			body, err := fs.ReadFile(embeddedFS, meta.file)
			if err != nil {
				return fmt.Errorf("acd setup: read template %s: %w", meta.file, err)
			}
			if !json.Valid(body) {
				offset := jsonInvalidOffset(body)
				fmt.Fprintf(cmd.ErrOrStderr(),
					"acd setup: template %s is not valid JSON (parse error near byte offset %d); refusing to emit --raw output that would corrupt %s. Please file a bug at github.com/KristjanPikhof/Auto-Commit-Daemon.\n",
					meta.file, offset, meta.file)
				return fmt.Errorf("acd setup: template %s contains invalid JSON", meta.file)
			}
			if _, err := out.Write(body); err != nil {
				return err
			}
			return nil
		}
		return printSnippet(out, embeddedFS, meta.file)
	}

	// Header.
	fmt.Fprintf(out, "%s acd setup %s: copy the snippet below into your harness config\n", cp, harness)
	fmt.Fprintf(out, "%s -------------------------------------------------------------\n", cp)

	if harness == "shell" {
		// Shell harness: print direnv snippet first, then zshrc snippet.
		if err := printSnippet(out, embeddedFS, meta.file); err != nil {
			return err
		}

		fmt.Fprintf(out, "\n%s zshrc variant\n", cp)

		if err := printSnippet(out, embeddedFS, shellZshrcSnippet); err != nil {
			return err
		}
	} else {
		if err := printSnippet(out, embeddedFS, meta.file); err != nil {
			return err
		}
	}

	// Footer from README.
	readmePath := readmeFile(harness)
	footer, err := fs.ReadFile(embeddedFS, readmePath)
	if err != nil {
		// Fallback generic footer if README is somehow missing.
		fmt.Fprintf(out, "\n%s Copy the snippet above into your %s config and restart the harness.\n", cp, harness)
	} else {
		fmt.Fprintf(out, "\n%s install instructions\n", cp)
		// Re-format each README line as a comment so the whole output
		// can be pasted as a single block without confusing the host config
		// parser.
		for _, line := range strings.Split(strings.TrimRight(string(footer), "\n"), "\n") {
			if line == "" {
				fmt.Fprintf(out, "%s\n", cp)
			} else {
				fmt.Fprintf(out, "%s %s\n", cp, line)
			}
		}
	}

	fmt.Fprintf(out, "%s -------------------------------------------------------------\n", cp)
	return nil
}

// jsonInvalidOffset returns the byte offset of the first JSON parse error in
// body, or len(body) if json.Decoder reports no offset (e.g. trailing
// content). Used to render an actionable error message when the embedded
// JSON template fails json.Valid.
func jsonInvalidOffset(body []byte) int64 {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	for {
		_, err := dec.Token()
		if err == nil {
			continue
		}
		if se, ok := err.(*json.SyntaxError); ok {
			return se.Offset
		}
		return dec.InputOffset()
	}
}

// printSnippet reads a file from the embedded FS and writes it verbatim.
func printSnippet(out interface{ Write([]byte) (int, error) }, embeddedFS fs.FS, path string) error {
	body, err := fs.ReadFile(embeddedFS, path)
	if err != nil {
		return fmt.Errorf("acd setup: read template %s: %w", path, err)
	}
	_, err = out.Write(body)
	return err
}

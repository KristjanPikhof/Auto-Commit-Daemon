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
	use := "setup"
	if initCompat {
		use = "init [harness]"
	}

	cmd := &cobra.Command{
		Use:   use,
		Short: "Install or upgrade ACD transactionally",
		Long: `Inspect the current installation, show one exact plan, and install or
upgrade the user supervisor and current repository as one rollback-safe
transaction. The default deterministic provider needs no API key.`,
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
				for _, name := range []string{"dry-run", "yes", "non-interactive", "expect-plan", "integrations", "repo"} {
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
			return runTransactionalSetup(cmd, dryRun, yes, nonInteractive, expectedPlan, integrations)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show the exact setup plan without any writes or service actions")
	cmd.Flags().BoolVar(&yes, "yes", false, "Apply the reviewed setup plan")
	cmd.Flags().BoolVar(&nonInteractive, "non-interactive", false, "Disable prompts; requires --yes")
	cmd.Flags().StringVar(&expectedPlan, "expect-plan", "", "Require this exact sha256 plan digest")
	cmd.Flags().StringVar(&integrations, "integrations", "auto", "auto, none, or comma-separated integration names")
	cmd.Flags().BoolVar(&rawFlag, "raw", false, "Compatibility: print a harness snippet without instructions")
	_ = cmd.Flags().MarkHidden("raw")
	return cmd
}

func runTransactionalSetup(cmd *cobra.Command, dryRun, yes, nonInteractive bool, expectedPlan, integrations string) error {
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
	plan, err := installer.BuildPlan(cmd.Context(), roots, installer.Options{Repo: repo, Integrations: integrations, NonInteractive: nonInteractive, ExpectedPlan: expectedPlan})
	if err != nil {
		return fmt.Errorf("acd setup: %w", err)
	}
	if expectedPlan != "" && expectedPlan != plan.Digest {
		return invalidCommandError("acd setup: plan digest changed: got %s, expected %s", plan.Digest, expectedPlan)
	}
	if dryRun {
		return renderSetupPlan(cmd, plan, jsonOut)
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
	progress := newSetupProgress(cmd.ErrOrStderr(), jsonOut || quiet, 10*time.Second)
	defer progress.Close()
	applyOptions := installer.ApplyOptions{
		Quiesce: func(ctx context.Context) error {
			return runStopRegistry(ctx, io.Discard, true, false, plan.Registry)
		},
		Progress: progress.Update,
	}
	if !nonInteractive && !jsonOut {
		applyOptions.ServiceAccessRetry = func(ctx context.Context, accessErr *installer.ServiceAccessError) error {
			return promptSetupServiceAccess(ctx, cmd.ErrOrStderr(), input, accessErr)
		}
	}
	result, err := installer.Apply(cmd.Context(), roots, plan, applyOptions)
	progress.Close()
	if err != nil {
		return classifySetupApplyError(err)
	}
	if jsonOut {
		return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateProtected, Changed: true,
			Actions: []productAction{{Kind: "setup", Status: "completed", Target: plan.Repo}}, Data: result}, true)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Setup complete. Repository protected: %s\n", plan.Repo)
	return nil
}

func promptSetupServiceAccess(
	ctx context.Context,
	out io.Writer,
	input *bufio.Reader,
	accessErr *installer.ServiceAccessError,
) error {
	fmt.Fprintf(out, `
macOS needs your permission before ACD can protect repositories in Desktop,
Documents, or other privacy-controlled folders.

1. Open System Settings > Privacy & Security > Full Disk Access.
2. Click +, press Command-Shift-G, and enter:
   %s
3. Add and enable that binary, then return here.

Press Enter to check again, or type "cancel" to roll back: `, accessErr.ManagedBinary)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		answer, err := input.ReadString('\n')
		if err != nil && strings.TrimSpace(answer) == "" {
			return err
		}
		switch strings.ToLower(strings.TrimSpace(answer)) {
		case "", "retry", "r":
			return nil
		case "cancel", "c", "no", "n", "q", "quit":
			return errors.New("macOS Full Disk Access was not granted")
		default:
			fmt.Fprint(out, "Press Enter to retry, or type \"cancel\" to roll back: ")
		}
	}
}

func classifySetupApplyError(err error) error {
	var accessErr *installer.ServiceAccessError
	if errors.As(err, &accessErr) {
		return actionRequiredError("service_access_required", "acd setup: "+accessErr.Error())
	}
	return fmt.Errorf("acd setup: %w", err)
}

type setupProgress struct {
	out      io.Writer
	interval time.Duration
	silent   bool

	mu       sync.Mutex
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
	p.detail = update.Detail
	p.started = time.Now()
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
			if p.detail != "" {
				fmt.Fprintf(p.out, "Setup: still working on %s (%s elapsed)\n", strings.ToLower(p.detail), time.Since(p.started).Round(time.Second))
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
		return renderAnyProductEnvelope(cmd.OutOrStdout(), productEnvelope{OK: true, State: productStateOff,
			Actions: []productAction{}, Data: plan}, true)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Setup plan %s\n", plan.Digest)
	for index, action := range plan.Actions {
		fmt.Fprintf(cmd.OutOrStdout(), "%d. %s: %s (%s)\n", index+1, action.Kind, action.Target, action.Detail)
	}
	return nil
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
	fmt.Fprintf(out, "%s acd setup %s — copy the snippet below into your harness config\n", cp, harness)
	fmt.Fprintf(out, "%s ─────────────────────────────────────────────────────────────\n", cp)

	if harness == "shell" {
		// Shell harness: print direnv snippet first, then zshrc snippet.
		if err := printSnippet(out, embeddedFS, meta.file); err != nil {
			return err
		}

		fmt.Fprintf(out, "\n%s ── zshrc variant ─────────────────────────────────────────────\n", cp)

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
		fmt.Fprintf(out, "\n%s ── install instructions ───────────────────────────────────────\n", cp)
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

	fmt.Fprintf(out, "%s ─────────────────────────────────────────────────────────────\n", cp)
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

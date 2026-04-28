package cli

// §7.9 — `acd init <harness>` print-only command.
//
// Reads embedded templates/<harness>/* via the templates package's FS and
// emits the canonical snippet body plus a copy-paste instructions footer.
// --apply is accepted for forward-compat but deferred to v0.2.

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

// supportedHarnesses is the canonical ordered list of harness identifiers.
var supportedHarnesses = []string{"claude-code", "codex", "opencode", "pi", "shell"}

// harnessSnippet describes which file inside templates/<harness>/ is the
// primary snippet and what comment prefix to use for the header/footer line.
type harnessSnippet struct {
	file          string // relative path inside templates/
	commentPrefix string // language-appropriate comment marker
}

var harnessSnippets = map[string]harnessSnippet{
	"claude-code": {"claude-code/settings.snippet.json", "//"},
	"codex":       {"codex/config.snippet.toml", "#"},
	"opencode":    {"opencode/hooks.snippet.yaml", "#"},
	"pi":          {"pi/hooks.snippet.yaml", "#"},
	// shell prints both snippet files separated by a divider.
	"shell": {"shell/direnv.envrc.snippet", "#"},
}

// shellExtra is the second snippet for the shell harness (zshrc).
const shellZshrcSnippet = "shell/zshrc.snippet.sh"

// readmeFile returns the README path for a harness.
func readmeFile(harness string) string {
	return harness + "/README.md"
}

func newInitCmd() *cobra.Command {
	var applyFlag bool

	cmd := &cobra.Command{
		Use:       "init <harness>",
		Short:     "Print install snippet for a harness adapter",
		Args:      cobra.ExactArgs(1),
		ValidArgs: supportedHarnesses,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInit(cmd, args[0], applyFlag)
		},
	}
	cmd.Flags().BoolVar(&applyFlag, "apply", false, "Automatically apply snippet (deferred to v0.2)")
	return cmd
}

func runInit(cmd *cobra.Command, harness string, apply bool) error {
	// Validate harness.
	known := false
	for _, h := range supportedHarnesses {
		if h == harness {
			known = true
			break
		}
	}
	if !known {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"acd init: unknown harness %q\nSupported harnesses: %s\n",
			harness, strings.Join(supportedHarnesses, ", "))
		return fmt.Errorf("acd init: unknown harness %q", harness)
	}

	// --apply deferred to v0.2.
	if apply {
		fmt.Fprintf(cmd.OutOrStdout(),
			"acd init: --apply is not implemented in v0.1; copy the snippet below manually.\n\n")
	}

	meta := harnessSnippets[harness]
	cp := meta.commentPrefix
	embeddedFS := templates.FS

	out := cmd.OutOrStdout()

	// Header.
	fmt.Fprintf(out, "%s acd init %s — copy the snippet below into your harness config\n", cp, harness)
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

// printSnippet reads a file from the embedded FS and writes it verbatim.
func printSnippet(out interface{ Write([]byte) (int, error) }, embeddedFS fs.FS, path string) error {
	body, err := fs.ReadFile(embeddedFS, path)
	if err != nil {
		return fmt.Errorf("acd init: read template %s: %w", path, err)
	}
	_, err = out.Write(body)
	return err
}

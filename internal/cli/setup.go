package cli

// §7.9 — `acd setup <harness>` print-only command.
//
// Reads embedded templates/<harness>/* via the templates package's FS and
// emits the canonical snippet body plus a copy-paste instructions footer.
// --apply is accepted for forward-compat but deferred to v0.2.

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/adapter"
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
	var applyFlag bool
	var helperFlag bool
	var rawFlag bool

	cmd := &cobra.Command{
		Use:     "setup [harness]",
		Aliases: []string{"init"},
		Short:   "Print install snippet for a harness adapter",
		Long: `Print the install snippet for a supported harness adapter.

When no harness is provided, acd tries to detect one installed acd-managed harness. Otherwise pass a harness name explicitly. This command prints snippets only; --apply is reserved for a future version and is hidden.

Use --raw to emit only the snippet body (no comment-wrapped header, footer, or README). This is required when the snippet is strict JSON (e.g. acd setup codex --raw > ~/.codex/hooks.json) because JSON has no comment syntax.

Use --helper with cursor to emit the embedded lifecycle helper script.

Supported harnesses include claude-code, codex, cursor, opencode, pi, and shell.`,
		Example: `  acd setup codex --raw > ~/.codex/hooks.json
  acd setup cursor --raw > ~/.cursor/hooks.json
  acd setup cursor --helper > ~/.cursor/hooks/acd-lifecycle.sh
  acd setup claude-code
  acd setup opencode
  acd setup shell`,
		Args:         cobra.RangeArgs(0, 1),
		ValidArgs:    supportedHarnesses,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.CalledAs() == "init" {
				fmt.Fprintln(cmd.ErrOrStderr(), "warning: acd init is deprecated and will be removed in a future release; use acd setup")
			}
			if applyFlag {
				fmt.Fprintln(cmd.ErrOrStderr(), "acd setup: --apply is not implemented")
				return fmt.Errorf("acd setup: --apply is not implemented")
			}
			harness := ""
			if len(args) == 1 {
				harness = args[0]
			}
			return runSetup(cmd, harness, rawFlag, helperFlag)
		},
	}
	cmd.Flags().BoolVar(&applyFlag, "apply", false, "Automatically apply snippet (deferred to v0.2)")
	cmd.Flags().BoolVar(&helperFlag, "helper", false, "Emit the Cursor lifecycle helper script")
	cmd.Flags().BoolVar(&rawFlag, "raw", false, "Emit only the snippet body (no comment-wrapped instructions); required for strict-JSON targets like ~/.codex/hooks.json")
	_ = cmd.Flags().MarkHidden("apply")
	return cmd
}

func runSetup(cmd *cobra.Command, harness string, raw bool, helper bool) error {
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

	if helper {
		if harness != "cursor" {
			return fmt.Errorf("acd setup: --helper is only supported for cursor")
		}
		return printSnippet(out, embeddedFS, "cursor/hooks/acd-lifecycle.sh")
	}

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

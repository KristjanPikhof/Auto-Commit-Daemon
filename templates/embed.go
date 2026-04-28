// Package templates exposes the embedded harness adapter snippets.
//
// Each subdirectory holds a single harness's drop-in config plus README and
// uninstall docs. The CLI's `acd init <harness>` reads from FS at runtime.
package templates

import "embed"

//go:embed claude-code codex opencode pi shell
var FS embed.FS

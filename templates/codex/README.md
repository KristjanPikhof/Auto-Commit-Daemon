# Codex integration

Install or update transactionally:

~~~bash
acd setup --integrations=codex
~~~

ACD merges schema-clean entries into `~/.codex/hooks.json`. The file keeps only
Codex-supported top-level keys; ownership metadata is stored in ACD's separate
integration registry. Unrelated hooks remain untouched.

The five events provide session start, prompt/tool activity, and a soft
publication boundary at Stop. They are semantic hints only. Filesystem watching
and complete polling protect changes even when hooks are absent, rejected, or
temporarily fail.

Codex loads user-level hooks in every workspace. The ACD hook checks the nearest
Git root before it contacts the supervisor. It exits quietly when the repository
is not registered, is disabled, or has not been enabled with `acd on`.

If Codex requests hook approval after an update, review and approve the exact
merged commands in `/hooks`. Use `acd doctor` for drift and
`acd uninstall --dry-run` for safe removal.

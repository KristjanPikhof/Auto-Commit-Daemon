# Codex integration

Install or update:

~~~bash
acd setup --integrations=codex
~~~

ACD merges schema-clean entries into `~/.codex/hooks.json`. The file keeps only
Codex-supported top-level keys; ownership metadata is stored in ACD's separate
integration registry. Unrelated hooks remain untouched.

Session, tool, prompt-submit, and Stop hooks only wake checkpoint protection.
Ending a turn never publishes commits. Filesystem watching and bounded polling
protect changes when hooks are absent or temporarily fail.

Codex loads user-level hooks in every workspace. The ACD hook checks the nearest
Git root before it contacts the supervisor. It exits quietly when the repository
is not registered, is disabled, or has not been enabled with `acd on`.

If Codex requests hook approval after an update, review and approve the exact
merged commands in `/hooks`. Use `acd doctor` for drift and
`acd uninstall --dry-run` for safe removal.

## Recommended agent instructions

Add the [shared ACD guidance](../agent-guidance.md) to your global or repository
`AGENTS.md`, or your tool's equivalent instruction file. Run `acd setup agents`
for the guided preview, or `acd setup agents --manual` for commands and the
instruction copy. Adding the section to a file requires an explicit choice.

## Agent task completion

Once the requested job and relevant checks are complete, call `acd finish` in
that repository/worktree only if existing user and repository instructions
authorize committing:

~~~bash
acd finish --repo /path/to/worktree 'Complete the settings update'
~~~

Use a brief technical purpose. Keep prompt transcripts, secrets, and customer
data out of it. Hooks do not send or store prompt text.

Local mode creates one complete batch without AI and is the default. Intent
uses the approved provider to group completed goals. Either mode supports
imperative messages (`Fix setup retries`) or Conventional Commits
(`fix: handle setup retries`).

Let ACD choose the commit boundary. Do not stage files, run manual Git commits,
poll for background commits, or retry finish automatically. Report the finish
result; a failed request needs an explicit retry after its reported cause is
resolved. Protection continues independently.

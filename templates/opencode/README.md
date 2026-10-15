# OpenCode integration

Install or update:

~~~bash
acd setup --integrations=opencode
~~~

ACD merges one bounded, versioned owned block into
`~/.config/opencode/hook/hooks.yaml`. Unrelated YAML remains unchanged, and a
modified owned block requires a newly approved plan before replacement or
removal.

Session, tool, idle, and deletion events only wake checkpoint protection.
Ending a session never publishes commits. Filesystem watching and bounded
polling protect changes without hooks.

Each hook sends one normalized event. ACD first checks the nearest Git root and
quietly ignores repositories where protection is not active.

Use `acd doctor` for drift and `acd uninstall --dry-run` for safe removal.

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

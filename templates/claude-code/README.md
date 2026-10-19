# Claude Code integration

Install or update:

~~~bash
acd setup --integrations=claude-code
~~~

ACD structurally merges its owned hook entries into
`~/.claude/settings.json` and records their signatures separately. Unrelated
settings and hooks remain untouched. A modified owned entry blocks a later
overwrite or removal until a new plan is explicitly approved.

Session, tool, Stop, and SessionEnd hooks only wake checkpoint protection.
Ending a turn or session never publishes commits. Filesystem watching and
bounded polling protect changes without hooks.

Claude can run these hooks in repositories where ACD is not enabled. The hook
checks the nearest Git root first and exits quietly when protection is off. An
active hook sends one event to ACD instead of separate session and wake calls.

Use `acd doctor` to inspect integration drift. Use `acd uninstall --dry-run`
before removing verified owned entries.

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

Describe the problem, approach, and constraints in the purpose so Intent can
explain decisions and choose useful steps within the task. Use `--message` only
for an intentional single batch; it disables splitting. Review the actual commit
boundaries and reasons before handoff. Keep prompt transcripts, secrets, and
customer data out of the context. Hooks do not send or store prompt text.

Local mode creates one complete batch without AI and is the default. Intent
uses the approved provider to group completed goals. Either mode supports
imperative messages (`Fix setup retries`) or Conventional Commits
(`fix: handle setup retries`).

Let ACD choose the commit boundary. Do not stage files, run manual Git commits,
poll for background commits, or retry finish automatically. Report the finish
result; a failed request needs an explicit retry after its reported cause is
resolved. Protection continues independently.

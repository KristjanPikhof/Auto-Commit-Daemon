# Cursor integration

Install or update:

~~~bash
acd setup --integrations=cursor
~~~

ACD merges its five owned events into the user-level
`~/.cursor/hooks.json`: `sessionStart`, `postToolUse`, `afterFileEdit`, `stop`,
and `sessionEnd`. Repo-local `.cursor/hooks.json` is not owned or changed.
Unrelated user-level hooks remain untouched.

Cursor payloads are resolved to a canonical Git worktree before a hint is
accepted. Hooks only wake checkpoint protection; Stop and SessionEnd never publish
commits. Watching and bounded polling protect changes without hooks.

The integration ignores repositories where ACD protection is off. This check is
read-only and happens before ACD opens repository state or contacts a worker.

Review the merged hooks in Cursor Settings after setup. Use `acd doctor` for
drift and `acd uninstall --dry-run` for safe removal.

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

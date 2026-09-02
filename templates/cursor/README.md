# Cursor integration

Install or update transactionally:

~~~bash
acd setup --integrations=cursor
~~~

ACD merges its five owned events into the user-level
`~/.cursor/hooks.json`: `sessionStart`, `postToolUse`, `afterFileEdit`, `stop`,
and `sessionEnd`. Repo-local `.cursor/hooks.json` is not owned or changed.
Unrelated user-level hooks remain untouched.

Cursor payloads are resolved to a canonical Git worktree before a hint is
accepted. The hooks provide semantic and boundary hints only; watcher and poll
coverage does not depend on them.

The integration ignores repositories where ACD protection is off. This check is
read-only and happens before ACD opens repository state or contacts a worker.

Review the merged hooks in Cursor Settings after setup. Use `acd doctor` for
drift and `acd uninstall --dry-run` for safe removal.

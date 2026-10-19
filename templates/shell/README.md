# Shell integration

Install or update:

~~~bash
acd setup --integrations=shell
~~~

ACD merges a bounded, versioned block into the supported user shell file. The
integration wakes protection only for registered, enabled repositories.
Filesystem watching and bounded polling provide protection without it.

Enable or disable repositories with `acd on` and `acd off`. Use
`acd uninstall --dry-run` to preview removal of only the verified owned block.

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

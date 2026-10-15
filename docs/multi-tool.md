# Multiple tools in one repository

ACD protection observes filesystem changes from editors, terminals, agents,
and other local tools. Optional integrations send wake hints to the same worker;
they do not create a second protection engine or commit boundaries.

Hooks contact only registered, enabled repositories through a matching runtime.
Unknown or disabled repositories are skipped without registration or state
creation. Tool stop, idle, and session-close events are activity hints. They do
not mean the task is complete and never start Finish, retry AI, clear staging,
or rewrite history. Templates do not forward prompts or session identifiers.

One worker owns a Git common directory. Linked worktrees retain distinct
identities and checkpoints. You can enable each explicitly with `acd on`
inside that worktree or with `--repo PATH`.

An agent may run `acd finish` for completed work only when the user authorized
publication. Merely saving a file or sending a hook is not that authorization.
Review staging and use `--include-staged` only for an approved exact selection.
Never make manual Git commits to bypass an active ACD owner.

Run `acd setup agents` after installing ACD. The guide helps you choose tools,
preview hook changes, and copy the [shared ACD guidance](../templates/agent-guidance.md)
into `AGENTS.md` or your coding tool's instruction file. Regular `acd setup`
points to this guide instead of printing the instruction copy.

~~~bash
acd setup agents
acd setup agents --manual
acd setup agents --integrations=codex --instructions-file=AGENTS.md --dry-run
acd setup agents --integrations=codex --instructions-file=AGENTS.md --yes
~~~

The guide prints copy-ready commands at every preview. `--manual` and
`--dry-run` change nothing, even with `--yes`. Outside a terminal, the default
is also a preview. Use `--integrations=none` when you only want the instruction
step. Hook changes reuse the existing setup transaction, including any runtime
update shown in the preview.

Instructions default to copy only. In the guide, choose `repo` for the current
repository's `AGENTS.md`, or `file` for a global or custom path such as
`~/.codex/AGENTS.md`. `--instructions-file PATH` selects a file directly.
With `--repo`, relative instruction paths use that worktree's root; otherwise
they use the current directory.
Applying keeps unrelated content and symlinks, backs up existing content, and
updates only the section between ACD's markers. Repeated runs do not add another
section. Existing ACD guidance without those markers stays untouched for manual
review. A changed file or symlink requires a fresh preview.

Repository protection is a separate choice. The guide asks before including it;
scripts must explicitly pass `--protect --repo PATH`. Configured hooks do not
prove your coding tool has loaded them. Follow any reload instructions from
the tool and use `acd doctor` in the repository to check protection.

JSON agent previews include the copy as `recommended_agent_instructions`, plus
the setup plan, instruction destination, and manual commands. Existing user
instruction content is omitted from JSON.

~~~bash
acd status
acd finish "Complete the approved checkout change"
acd history --activity
acd doctor
~~~

If another tool moves the branch or changes the index during a finish, ACD
requires the frozen publication proof to match. Later file edits stay protected,
but they do not expand the request. Inspect a failed request and explicitly
retry or start a fresh finish; a hook does not resolve stale proof.

Source and installed runtime can differ. An existing older runtime may still
publish automatically until an explicit `acd setup` upgrade. Integrations refuse
an incompatible runtime rather than replacing it during normal activity.

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

After installation, `acd setup` recommends a copy-ready section for your global
or repository `AGENTS.md`, or your coding tool's equivalent instruction file.
The [shared guidance](../templates/agent-guidance.md) tells agents to check ACD
before editing and finish authorized work after the relevant checks. Setup
prints the recommendation without editing your instruction files. JSON setup
results include it as `recommended_agent_instructions`.

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

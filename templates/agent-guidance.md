## Commits: ACD (Auto-Commit-Daemon)

ACD protects changes in private checkpoints. `acd finish` creates branch commits.

- Check `acd status` once before editing. When active, use ACD instead of
  `git add` or `git commit`. An error does not prove ACD is inactive. Use manual
  commits only after confirming it is inactive and repository rules allow them.
- When committing is authorized, run `acd finish [PURPOSE]` after the task and
  relevant checks are complete, before handoff or a branch switch. Finish
  complete, verified milestones separately during longer tasks.
- PURPOSE is optional context for the committing AI, not a commit message.
  Pass the problem, chosen approach, and constraints, including decisions and
  rejected alternatives the diff cannot explain. A quoted argument may span
  paragraphs and multiple completed changes. Name independent problems and
  distinguish results from assumptions. Omit transcripts, secrets and customer
  data.
- Intent examines the code and supplied context, chooses useful boundaries and
  writes messages explaining why. Let it keep coherent features complete and
  separate unrelated fixes even in one file. If the evidence cannot support a
  safe split, keep the work protected and report the missing evidence.
- Local creates one offline batch with optional context; it does not infer
  motivation. Use advanced `--message` only for a user-supplied exact message or
  an explicitly requested single batch; it disables splitting. Review the actual
  commit boundaries and reasons before claiming success.
- Do not use an exact message to force a mixed batch after Intent refuses a
  plan. Improve the evidence or report the limit; keep the work protected.
- Review the whole diff first: finish includes all eligible pending changes in
  the worktree. Preserve unrelated work and use an isolated worktree when needed.
  Preserve user staging; use `--include-staged` only for an authorized selection.
- Questions and reviews do not authorize commits. Honor requests to leave work
  uncommitted and stricter repository instructions.
- Enable new worktrees with `acd on --repo PATH` only when authorized. Do not
  install, upgrade, restart, rewrite history, release, or push without permission.
- Report whether work is protected, committed, or needs attention. Follow the
  installed runtime's commands. Do not force `commit-all`, poll for commits,
  retry finish automatically, or switch modes to bypass a provider failure.

For example, pass decisions that would be lost when this chat ends:

~~~bash
acd finish "Settings reloads reset saved choices. Preserve authored values ahead
of defaults so reopening settings keeps the selection.

Keep the separate preview allocation fix independently reviewable. Reuse the
existing Metal context because preview needs textures, not effect pipelines."
~~~

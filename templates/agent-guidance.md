## Commits: ACD (Auto-Commit-Daemon)

ACD protects changes in private checkpoints. `acd finish` creates branch commits.

- Check `acd status` once before editing. When active, use ACD instead of
  `git add` or `git commit`. An error does not prove ACD is inactive. Use manual
  commits only after confirming it is inactive and repository rules allow them.
- When committing is authorized, run `acd finish "Complete the task goal"`
  after the task and relevant checks are complete, before handoff or a branch
  switch. During longer tasks, finish complete, verified milestones separately.
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

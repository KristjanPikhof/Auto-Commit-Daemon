## Commits: ACD (Auto-Commit-Daemon)

ACD protects changes in private checkpoints. `acd finish` creates branch commits.

- Check `acd status` once before editing. When active, use ACD instead of
  `git add` or `git commit`. An error does not prove ACD is inactive. Use manual
  commits only after confirming it is inactive and repository rules allow them.
- When committing is authorized, run `acd finish` with the completed change
  and its reason, for example `acd finish "Preserve saved choices on reload"`,
  after the task and relevant checks are complete, before handoff or a branch
  switch. During longer tasks, finish complete, verified milestones separately.
- Supply context about the problem, chosen approach, and constraints so Intent
  can explain decisions and identify useful steps within the task. Use an exact
  `--message` only when the user supplies the text or explicitly requests one
  batch; it disables splitting. Let Intent write and split messages otherwise.
  Review the actual commit boundaries and reasons before claiming success.
- Name independent problems in the completion context. Explain the previous
  failure or constraint and the chosen mechanism, not just the files edited.
  Keep a coherent feature together; separate unrelated fixes even in one file.
  If recorded versions cannot support that split, keep the work protected and
  report the missing evidence instead of forcing one mixed batch.
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

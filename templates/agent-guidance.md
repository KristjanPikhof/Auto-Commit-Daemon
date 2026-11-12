## When and how to use ACD for commits

ACD (Auto-Commit-Daemon) continuously saves changes in private Git checkpoints.
Its Intent mode uses AI to group completed work into commits and explain why
each change was needed.

- At session start, run `acd status`. If ACD is active for the current Git
  working directory, follow the rules below. Otherwise, use the normal repository
  workflow. If status fails, resolve the uncertainty before committing.
- Do not enable or reconfigure ACD unless asked. While active, use it instead of
  `git add` and `git commit`.
- When committing is allowed, run `acd finish` after each completed feature,
  fix, or independently useful step has passed its relevant checks. Then continue
  working. Checkpoints alone do not create branch commits.
- Supply context as a quoted argument: `acd finish "Problem, reason for the change, and decisions the diff cannot explain"`.
  ACD uses this context to choose commit boundaries and messages. Omit secrets
  and customer data.
- Review all pending changes first. Finish includes all changes allowed by
  ACD's ignore and safety rules, excluding ignored files, sensitive paths,
  generated caches, and submodule contents. It does not select only your task's
  files.
- Preserve unrelated work and user staging. Coordinate with other agents sharing
  the working directory before finishing. Ordinary finish refuses staged changes;
  do not include them without authorization.
- Verify the result before reporting that work is committed. If finish fails,
  report it. Do not bypass ACD with manual commits or retry automatically.

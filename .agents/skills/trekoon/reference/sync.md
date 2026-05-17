# Sync Reference

Trekoon uses one live SQLite database per repository at
`<sharedStorageRoot>/.trekoon/trekoon.db`. Linked worktrees share it. `git
checkout` and `git switch` do not roll back tracker state. Sync imports tracker
events between branches; never copy or commit the `.db` file.

Same-branch sync is a no-op. Cross-branch sync matters before merging a feature
branch back.

## Before Merge

Pull tracker events from the base branch:

```bash
trekoon --toon sync pull --from main
```

If conflicts exist:

```bash
trekoon --toon sync conflicts list
trekoon --toon sync conflicts show <conflict-id>
trekoon --toon sync resolve <conflict-id> --use theirs --dry-run
trekoon --toon sync resolve <conflict-id> --use ours
```

## Conflict Rules

Conflicts are field-level, not whole-record. Each conflict targets one field on
one entity.

- `--use ours`: keep the current shared DB field value; mark conflict resolved.
- `--use theirs`: write the source-branch field value into the shared DB; mark
  conflict resolved.
- `--dry-run`: preview without mutation. Returns `oursValue`, `theirsValue`,
  `wouldWrite`, and `dryRun: true`.

Always inspect with `sync conflicts show` before resolving. Choosing `theirs`
without inspection can overwrite in-progress shared DB work.

Typical choices:

| Scenario | Usually use | Why |
|---|---|---|
| Completed work vs stale main | ours | Your branch has latest progress |
| Enriched descriptions vs original | ours | Your descriptions are more detailed |
| Upstream updates from another agent | theirs | Accept newer upstream state |
| User-intentional reset | theirs | Respect explicit user action |

When unsure, ask the user.

## Batch Resolve

```bash
trekoon --toon sync resolve --all --use ours --dry-run
trekoon --toon sync resolve --all --use ours
trekoon --toon sync resolve --all --use ours --field status
trekoon --toon sync resolve --all --use theirs --entity <id>
trekoon --toon sync resolve --all --use ours --entity <id> --field description
```

Use batch resolve only when the conflict pattern is uniform. Otherwise inspect
and resolve individually or narrow by `--entity` / `--field`.

## Worktree Scope

Inspect machine-readable storage fields when debugging worktrees:
`storageMode`, `repoCommonDir`, `worktreeRoot`, `sharedStorageRoot`, and
`databaseFile`.

Conflicts are scoped per worktree and branch. `sync conflicts list` and
`sync resolve` only act on rows for the current worktree/branch, so resolving a
conflict does not erase a peer worktree's conflict on the same entity.

## Destructive Recovery

`sharedStorageRoot` is the repo-scoped source of truth for `.trekoon` in git
worktrees. If the user explicitly requests `trekoon wipe --yes --toon`, warn
that it deletes shared storage for the whole repository and every linked
worktree. Wipe is destructive recovery only; it is never the fix for a tracked
DB or gitignore mistake.

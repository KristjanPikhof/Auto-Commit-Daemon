# acd: overview

`acd` (Auto-Commit-Daemon) watches a git worktree, captures meaningful file
changes as durable snapshot events, and replays those events as commits on the
current branch. The default replay strategy publishes one captured event per
commit. The optional intent strategy lets an AI planner group related pending
captures into one reviewable commit without changing capture durability.

## How it works

```
  Harness (AI tool)
       │ acd start / acd wake
       ▼
  Daemon  ←─── fsnotify / poll
       │
       ├─ Capture  → SQLite (event metadata + blob OIDs) + git objects (file contents)
       │
       └─ Replay   → isolated scratch index → git commit-tree → git update-ref
```

1. **Capture.** When `fsnotify` fires (or the poll timer fires, or `acd wake`
   nudges the daemon), the daemon walks the worktree, hashes changed files into
   the git object store via `git hash-object -w`, and writes event rows to
   `<repo>/.git/acd/state.db`. Each event records the file path, operation
   (`create`/`modify`/`delete`/`rename`/`mode`), and the `before_oid` /
   `after_oid` blob OIDs.

2. **Replay.** A background loop drains `pending` events. In `event` strategy it
   publishes one event at a time. In `intent` strategy it offers a bounded
   pending window to the AI planner, then publishes the selected captures as one
   commit. Both paths apply ops against an isolated scratch index, write a tree,
   create a commit, and advance the branch ref atomically with `git update-ref`.
   Unsafe events become `blocked_conflict`; replay-build failures become
   `failed`. Both are terminal barriers when later pending work is waiting.

3. **AI messages and planning.** Commit subjects, and optional intent grouping,
   come from the configured AI provider or deterministic fallback. Diff context
   is reconstructed from captured blobs, never from the live worktree, and only
   leaves the machine when `ACD_AI_DIFF_EGRESS=1` is set.

For a detailed walkthrough of the storage model, replay index semantics,
branch-generation safety, and conflict resolution, see
[capture-replay.md](capture-replay.md).

For day-to-day troubleshooting with `acd status`, `acd events`,
`acd explain`, `acd fix`, `acd diagnose`, and `acd doctor --bundle`, see
[user-workflows.md](user-workflows.md).

For AI provider configuration and the subprocess plugin protocol, see
[ai-providers.md](ai-providers.md).

For quick-start usage and CLI reference, see the top-level
[README](../README.md).

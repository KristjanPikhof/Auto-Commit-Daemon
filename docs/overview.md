# acd: overview

`acd` (Auto-Commit-Daemon) watches a git worktree, captures every meaningful
file change as a snapshot event, and replays those events as atomic commits on
the current branch. It is designed to run alongside AI coding assistants
(Claude Code, Codex, OpenCode, Pi, or any shell-hook capable harness) and
produce a faithful, chronological commit history without operator intervention.

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

2. **Replay.** A background loop drains `pending` events one at a time. For
   each event the daemon applies ops against an isolated scratch index (seeded
   from `BaseHead`), writes a tree with `git write-tree`, creates a commit with
   `git commit-tree`, and advances the branch ref atomically with
   `git update-ref`. Events that fail conflict checks become `blocked_conflict`
   (terminal — operator action required) and halt the batch.

3. **AI messages.** Commit subjects come from the configured AI provider (or the
   deterministic rule-based fallback). The diff handed to AI providers is
   reconstructed from captured blobs — never from the live worktree.

For a detailed walkthrough of the storage model, replay index semantics,
branch-generation safety, and conflict resolution, see
[capture-replay.md](capture-replay.md).

For AI provider configuration and the subprocess plugin protocol, see
[ai-providers.md](ai-providers.md).

For quick-start usage and CLI reference, see the top-level
[README](../README.md).

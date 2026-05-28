# acd overview

`acd` captures worktree changes, stores durable metadata in the repo, and
replays those captures as Git commits. The default strategy commits one capture
at a time. Intent mode lets an AI planner group pending captures first.

~~~mermaid
flowchart LR
  subgraph Tool["AI tool or shell"]
    Hook["hooks call<br/>acd start / wake / flush"]
  end

  subgraph Repo["Git repo"]
    Daemon["acd daemon"]
    State[("state.db")]
    Objects[("git object database")]
    Scratch["scratch index"]
    Branch["branch ref"]
  end

  Hook --> Daemon
  Daemon -->|capture metadata| State
  Daemon -->|hash file contents| Objects
  State --> Scratch
  Objects --> Scratch
  Scratch -->|commit-tree| Branch

  classDef outside fill:#243447,stroke:#7aa2f7,color:#e6edf3
  classDef process fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef data fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  class Hook outside
  class Daemon,Scratch process
  class State,Objects,Branch data
~~~

## How it works

| Step | What ACD does | Where to look |
|---|---|---|
| Capture | Walks the worktree, filters ignored or sensitive paths, writes file contents as Git blobs, and records ops in `<gitDir>/acd/state.db`. | `acd events`, `acd explain --path FILE` |
| Replay | Applies pending ops to a private scratch index, creates commits with `git commit-tree`, and advances the branch with `git update-ref`. | `acd status`, `acd logs --follow` |
| Settle | Marks queued work as published when another committer already landed the same final state. | `acd explain --commit HEAD` |
| Block | Stops behind `blocked_conflict` or `failed` terminal rows when replay cannot prove the next commit is safe. | `acd diagnose --json`, `acd fix --dry-run` |

## Commit strategies

| Strategy | Behavior |
|---|---|
| `event` | FIFO replay. One capture can produce one commit. This is the default. |
| `intent` | A bounded window of pending captures goes to the planner. Selected captures replay as one commit; deferred captures stay pending. |

Both strategies use the same capture rows and replay safety checks. Intent mode
changes grouping, not durability.

## Data boundaries

| Data | Stored in | Leaves the machine by default? |
|---|---|---|
| File contents | Git object database | No |
| Capture metadata | `<gitDir>/acd/state.db` | No |
| Commit-message metadata | AI provider request | Only when a non-deterministic provider is configured |
| Captured diffs | Rebuilt from captured blobs | Only when the provider needs diffs and `ACD_AI_DIFF_EGRESS=1` is set |
| Prompt traces | `<gitDir>/acd/prompt-trace/` | No automatic upload, but the files may contain source text |

## Start here

| Task | Doc |
|---|---|
| Install and set up hooks | [README](../README.md) |
| Recover from a stuck queue | [user-workflows.md](user-workflows.md) |
| Understand replay safety | [capture-replay.md](capture-replay.md) |
| Configure AI providers | [ai-providers.md](ai-providers.md) |
| Use intent grouping | [intent-commit-flow.md](intent-commit-flow.md) |
| Rewrite local commit messages | [intent-commit-rewrite-flow.md](intent-commit-rewrite-flow.md) |

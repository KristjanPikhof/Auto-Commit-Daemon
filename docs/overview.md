# acd overview

`acd` captures worktree changes, stores durable metadata in the repo, and
replays those captures as Git commits. Event mode commits one capture at a
time. Intent v2 maintains semantic candidates across evaluations and publishes
them only after dependency, atomicity, materialization, and preset verification
checks pass.

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
| Reconcile | Proves a complete unpublished chain already landed, or preserves its reconstructed tree at a hidden recovery ref before reseeding and recapturing. | `acd events`, `acd diagnose` |
| Block | Stops behind `blocked_conflict` or `failed` only when ACD cannot complete an all-or-none proof or archive safely. | `acd diagnose --json`, `acd fix --dry-run` |

## Commit strategies

| Strategy | Behavior |
|---|---|
| `event` | FIFO replay. One capture can produce one commit. This is the default. |
| `intent` | Bounded evaluations revise durable semantic candidates. Ready candidates publish in topological order after the atomicity gate. |

Both strategies use the same capture rows and replay safety checks. Intent mode
changes grouping, not durability.

Intent mode also has a persisted provider circuit breaker. Fast and Balanced
apply bounded dependency-aware fallback policies during a provider outage.
Quality retains candidates and reports `needs_attention`. Capture continues in
all three cases.

## Data boundaries

| Data | Stored in | Leaves the machine by default? |
|---|---|---|
| File contents | Git object database | No |
| Capture metadata | `<gitDir>/acd/state.db` | No |
| Commit-message metadata | AI provider request | Only when a non-deterministic provider is configured |
| Captured diffs | Rebuilt from captured blobs | Only when the provider needs diffs and `ACD_AI_DIFF_EGRESS=1` is set |
| Prompt traces | `<gitDir>/acd/prompt-trace/` | No automatic upload, but the files may contain source text |
| Provider credential | `${XDG_CONFIG_HOME:-$HOME/.config}/acd/credentials.json` | Only to the configured provider; environment wins over this file |
| Candidate verification | Ephemeral detached worktree | No, unless the approved command itself performs network access |

## Start here

| Task | Doc |
|---|---|
| Install and set up hooks | [README](../README.md) |
| Find the right command | [commands.md](commands.md) |
| Recover from a stuck queue | [user-workflows.md](user-workflows.md) |
| Understand replay safety | [capture-replay.md](capture-replay.md) |
| Configure AI providers | [ai-providers.md](ai-providers.md) |
| Use intent grouping | [intent-commit-flow.md](intent-commit-flow.md) |
| Migrate an existing Intent repo | [intent-v2-migration.md](intent-v2-migration.md) |
| Rewrite local commit messages | [intent-commit-rewrite-flow.md](intent-commit-rewrite-flow.md) |

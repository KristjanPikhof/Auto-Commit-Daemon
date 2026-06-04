# Capture, replay, and conflict resolution

This page explains why ACD can keep committing while you work, and why it
sometimes stops. For day-to-day commands, start with
[user-workflows.md](user-workflows.md).

## Storage model

ACD stores metadata and content separately.

| Store | Contents |
|---|---|
| `<repo>/.git/acd/state.db` | Capture events, ops, blob OIDs, branch generation, daemon state, clients, decisions. |
| `.git/objects/` | File contents written with `git hash-object -w` at capture time. |

SQLite does not store file contents. It stores `before_oid` and `after_oid`.
That keeps captures durable even when the live worktree changes again.

## Event states

~~~mermaid
stateDiagram-v2
  [*] --> pending
  pending --> published: commit written or HEAD already matches
  pending --> blocked_conflict: replay not safe
  pending --> failed: replay data or git operation failed
  published --> [*]
  blocked_conflict --> [*]
  failed --> [*]
~~~

| State | Meaning |
|---|---|
| `pending` | Replay may publish it. |
| `published` | ACD wrote a commit or proved the current `HEAD` already has the captured after-state. |
| `blocked_conflict` | ACD cannot safely apply the event. Later pending rows for the same branch generation wait behind it. |
| `failed` | Replay could not build or apply the event. It is also a terminal barrier when later pending rows exist. |

ACD never retries terminal rows automatically. Use `acd fix --dry-run` before
mutating state.

## Product decisions

`acd status`, `acd events`, and `acd explain` read the decision ledger, not raw
daemon logs.

| Decision | User meaning |
|---|---|
| `captured` | A change was queued for replay. |
| `committed` | ACD published the queued change as a commit. |
| `skipped` | ACD intentionally left a path uncommitted. |
| `protected` | ACD protected a sensitive or generated path. |
| `handled_external` | Another commit already contains the captured after-state. |
| `handled_external_after_block` | A blocked row was promoted after an external commit landed the captured change. |
| `superseded_external` | External history made the queued work obsolete. |
| `blocked` | Replay stopped because the next event was not provably safe. |
| `paused` / `resumed` | Capture or replay pause state changed. |

## Replay: how a pending event becomes a commit

~~~mermaid
flowchart TB
  A["Read visible pending rows"] --> B["Seed scratch index<br/>from BaseHead"]
  B --> C["Check branch generation"]
  C --> D["Probe before-state<br/>in scratch index"]
  D --> E{"HEAD already<br/>has after-state?"}
  E -->|yes| F["Mark published<br/>at HEAD"]
  E -->|no| G{"Before-state<br/>matches?"}
  G -->|no| H["blocked_conflict<br/>halt pass"]
  G -->|yes| I["Apply ops to<br/>scratch index"]
  I --> J["write-tree<br/>commit-tree"]
  J --> K{"update-ref CAS<br/>succeeds?"}
  K -->|no| H
  K -->|yes| L["Mark published<br/>with commit OID"]
  F --> M["Next event"]
  L --> M

  classDef normal fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef data fill:#243447,stroke:#7aa2f7,color:#e6edf3
  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef block fill:#402b2b,stroke:#f7768e,color:#ffe8ee
  class A,B,C,D,I,J,M data
  class E,G,K decision
  class F,L normal
  class H block
~~~

The scratch index is private to the replay pass. ACD does not run broad
`git reset`, `git checkout`, or `git read-tree` commands against your live
index.

After a publish, ACD may repair the live index for the event paths only. It
skips anything that looks like user staging, conflict stages, intent-to-add, or
malformed index state.

## Branch-generation safety

Every capture records `(branch_ref, branch_generation)`. This protects queued
work from replaying on top of the wrong branch history.

| HEAD movement | Classification | Effect |
|---|---|---|
| New HEAD descends from the previous HEAD on the same branch | Fast-forward | Generation stays the same. |
| Rebase, reset, branch switch, or same-SHA ref switch | Diverged | Generation bumps. Stale pending rows from the old generation are dropped. |
| Detached HEAD | Paused/refused | `acd start` refuses and capture/replay stay disabled until HEAD is attached. |
| Deleted old branch ref | Dead branch | Startup cleanup can prune stale unpublished rows for that ref. |

Same-branch rewinds set a short grace pause:

| Setting | Default | Meaning |
|---|---:|---|
| `ACD_REWIND_GRACE_SECONDS` | `60` | Capture and replay pause after a same-branch rewind. |
| `ACD_KEEP_DEAD_BRANCH_BARRIERS` | off | Truthy keeps stale deleted-branch barriers for inspection. |
| `ACD_SHADOW_RETENTION_GENERATIONS` | `1` | Prior shadow generations retained after reseed. |

Manual pause wins over rewind grace:

~~~bash
acd pause --repo . --reason "branch surgery" --yes
# reset, rebase, switch, or inspect
acd resume --repo . --yes
~~~

## AI diffs

Provider diffs come from captured blobs, not from the live worktree.

| Condition | Result |
|---|---|
| Provider is `deterministic` | No diff is built. |
| Provider can use diffs but `ACD_AI_DIFF_EGRESS` is off | Metadata only. |
| Provider can use diffs and `ACD_AI_DIFF_EGRESS=1` | Redacted captured diff is sent. |

Commit-message diffs are capped at 4000 bytes. Intent planner and message
rewrite diffs are capped at 16000 bytes.

## Commit strategies

| Strategy | Replay behavior |
|---|---|
| `event` | FIFO replay. One capture can become one commit. |
| `intent` | A bounded pending window goes to the planner. Selected seqs replay as one commit; deferred seqs stay pending. |

Intent waits for one of these:

| Trigger | Meaning |
|---|---|
| `ACD_INTENT_MIN_PENDING` | Enough visible pending captures exist. |
| `ACD_INTENT_MAX_PENDING_AGE` | Oldest visible capture reached the age escape hatch. |
| `acd flush --logical --session-id <active-session>` | A registered harness session asks to drain the visible batch now. |
| Forced aging | A repeatedly deferred capture reached `ACD_INTENT_DEFER_LIMIT`. |

Plain `acd wake` nudges capture and replay. It does not bypass intent batch
gates.

## One-shot capture with `commit-all`

`acd commit-all` is for a dirty worktree when the daemon was off.

~~~bash
acd commit-all --dry-run
acd commit-all --yes
acd commit-all --yes --json
~~~

It reseeds shadow state from `HEAD`, drops stale pending rows for the active
branch generation, captures the live diff, sorts paths lexicographically, and
replays with the configured strategy.

Refusals:

| Refusal | Fix |
|---|---|
| Detached HEAD | Check out a branch. |
| Rebase, merge, cherry-pick, or bisect in progress | Finish the Git operation. |
| Manual pause marker | `acd resume --yes` |
| Per-repo daemon is running | `acd stop` first. |
| No initial commit | Create the first commit yourself. |

## Blocked conflicts

`blocked_conflict` means replay stopped before it could prove the next commit
was safe.

Common causes:

| Cause | Example |
|---|---|
| Generation mismatch | The branch was rebased after capture. |
| Before-state mismatch | Another tool changed the path before ACD replayed it. |
| `update-ref` CAS failure | The branch moved while ACD was creating the commit. |
| Ancestry failure | The replay parent is no longer reachable. |
| Mode or symlink mismatch | File content matches, but mode or symlink target does not. |

Recovery ladder:

~~~bash
acd status
acd events
acd explain --path path/from/status
acd diagnose --json
acd fix --dry-run
acd fix --yes
acd fix --force --dry-run
acd fix --force --yes
~~~

Safe apply handles verifiable cleanup. Force apply is only for terminal barriers
with pending successors after you check the blocked changes.

## Operator commands

| Task | Command |
|---|---|
| Current repo health | `acd status` |
| Live status refresh | `acd status --watch` |
| Decision ledger | `acd events` |
| Stream new decisions | `acd events --watch` |
| Explain one path | `acd explain --path FILE` |
| Explain one commit | `acd explain --commit HEAD` |
| All registered repos | `acd list` |
| Wide repo table | `acd list --verbose` |
| Machine-readable repo table | `acd list --json` |
| Interactive repo lifecycle manager | `acd list --interactive` |
| Raw daemon log | `acd logs --follow` |
| Recovery report | `acd diagnose --json` |
| Safe recovery plan | `acd fix --dry-run` |
| Apply safe recovery plan | `acd fix --yes` |
| Support zip | `acd doctor --bundle` |

`acd list` compact status tokens:

| Token | Meaning |
|---|---|
| `OK` | Running with no queued or blocked work. |
| `wait` | Queued work remains. In intent mode this may be a normal batch wait. |
| `blk` | Terminal barrier needs operator action. |
| `pause` | Manual pause or rewind grace is active. |
| `miss` | Repo or state DB is missing. |
| `bad` | State DB exists but cannot be read. |

Disabled repos are hidden from normal `acd list` snapshots. A disabled registry
row makes hook-driven `start`, `wake`, `touch`, and `flush` skip with
`repo_disabled` before capture or replay state is opened. Re-enable with
`acd repo enable --repo <path>`, or use `acd repo manage` /
`acd list --interactive` to toggle from the manager.

## Safe-ignore and sensitive paths

ACD prunes generated dependency and cache trees before capture. Defaults include
`node_modules/`, `target/`, `.venv/`, `venv/`, `__pycache__/`,
`.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, and `.gradle/`.

~~~bash
acd explain --path .env
acd events --path node_modules/pkg/index.js
acd doctor
~~~

| Setting | Meaning |
|---|---|
| `ACD_SAFE_IGNORE=0` | Disable generated-tree pruning. |
| `ACD_SAFE_IGNORE_EXTRA=dist/,build/` | Add generated trees. |
| `ACD_SENSITIVE_GLOBS=...` | Replace or extend sensitive path handling. Empty keeps defaults. |

Restart the daemon after changing these values.

Tracked generated files are different from new generated files. If a cache tree
such as `.derivedData-provider-core/` was already committed to Git, deleting it
can leave pending delete rows from an older ACD version or a stale queue. New
capture passes protect those safe-ignore deletes instead of queuing them, and
`acd diagnose --json` reports grouped `generated_pending` roots. Use
`acd fix --dry-run` and `acd fix --yes` to clean ACD state, then review and
commit the Git cleanup separately:

~~~bash
git status -- .derivedData-provider-core
git add -u -- .derivedData-provider-core
git commit -m "Remove tracked generated cache files"
~~~

## Revert and rebase workflows

Pause before planned branch surgery:

~~~bash
acd pause --repo . --reason "branch surgery" --yes
# git revert, reset, rebase, or inspect
acd resume --repo . --yes
acd wake --repo . --session-id "$ACD_SESSION_ID"
~~~

| Operation | What ACD sees |
|---|---|
| `git revert` | Fast-forward commit. Queued matching work may settle as already handled at `HEAD`. |
| `git reset --soft` or `--mixed` | Same-branch rewind. Capture and replay pause for rewind grace. |
| `git reset --hard` | Same rewind behavior, with worktree overwritten by Git. |
| `git rebase -i` | Git operation marker pauses capture and replay. After rebase, generation bumps and stale pending rows are dropped. |

## Trace event classes

Enable trace logging:

~~~bash
ACD_TRACE=1 acd start
~~~

Trace files live under `<gitDir>/acd/trace/` and avoid full prompts and diffs.

| Class | When it appears |
|---|---|
| `bootstrap_shadow.reseed` | Shadow state reseeded after startup or divergence. |
| `capture.classify` | Live worktree was compared with shadow state. |
| `capture.event` | A capture event was appended or dropped at queue cap. |
| `capture.pause` | Capture skipped because replay is paused. |
| `replay.commit` | A queued event published or settled at `HEAD`. |
| `replay.self_heal` | A blocked row was promoted because `HEAD` now matches the after-state. |
| `replay.conflict` | Replay produced `blocked_conflict`. |
| `replay.failed` | Replay produced `failed`. |
| `replay.update_ref` | A `git update-ref` attempt ran. |
| `replay.live_index` | Path-scoped live-index repair ran or skipped. |
| `replay.pause` | Replay skipped because paused. |
| `intent.batch_wait` | Intent mode waited for count, age, or flush. |
| `intent.planner.input` | A planner window was built. |
| `intent.planner.output` | A valid planner result returned. |
| `intent.planner.validation_failed` | Planner output failed validation. |
| `intent.forced_aging` | A deferred capture was forced into a one-capture window. |
| `branch_token.transition` | HEAD movement was classified. |
| `daemon.pause` | Git operation marker paused or resumed the daemon. |

Prompt traces are separate. Enable them with `ACD_AI_PROMPT_TRACE=1` and inspect
with `acd prompt --last` or `acd prompt --seq <seq>`. Treat those files as
sensitive because they can contain source text.

## Multi-tool coexistence

If another committer lands the same captured state first, ACD marks its queued
event as published at the external `HEAD` instead of making a duplicate commit.
Real mismatches still become `blocked_conflict`.

See [multi-tool.md](multi-tool.md).

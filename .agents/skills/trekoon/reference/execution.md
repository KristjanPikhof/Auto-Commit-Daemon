# Execution Reference

You are an orchestrator. Execute work from Trekoon, not markdown plan files.
Execution is complete only when the epic is `done`, all remaining work is
`blocked` with recorded reasons, or real user input is required.

Use subagents by default for meaningful independent work. Keep small or tightly
coupled tasks in the parent. Use your context for orchestration, dependency
decisions, user communication, and synthesis.

If the plan has unclear requirements or meaningful tradeoffs, ask before
starting. Do not stop at status reporting when ready work exists.

The atomic-claim, append-progress, and `task done` recipes live in
`reference/harness-primitives.md`. This file references them rather than
restating.

## Choose Shape

- Direct work: one ready task, tiny change, narrow scope, or tightly coupled
  work. Use the direct work loop below.
- Orchestrated work: multiple ready tasks across separable lanes. Build the
  graph, group by lane, delegate meaningful independent lanes, and coordinate
  completion from the parent session.

If a higher-priority harness policy blocks subagent use without explicit user
wording, tell the user immediately after the lane graph is known:

```text
I found <n> independent Trekoon lanes. This harness requires explicit
permission before I can spawn subagents. Should I delegate those lanes and keep
coordinating from the parent session?
```

## Build The Graph

Use scheduler reads before broad tree reads:

```bash
trekoon --toon task ready --epic <epic-id> --limit 50
trekoon --toon dep reverse <task-or-subtask-id>
trekoon --toon epic show <epic-id> --all
```

Group ready tasks by lane:

- Same directory prefix.
- Same subsystem/domain.
- Same owner.
- Same implementation context.

Keep each lane to about 3-4 tasks. Split larger lanes. Parallel lanes touch
different subsystems and have no dependency edge. Sequential lanes have hard
dependencies.

Mark the epic in progress once, before dispatching:

```bash
trekoon --toon epic update <epic-id> --status in_progress
```

## Delegate Lanes

For each meaningful independent lane, spawn a subagent when the harness exposes
subagents. Keep local todo/task tools as a live progress display only; Trekoon
remains the durable source of truth.

Prompt shape:

```text
Spawn or act as a write-capable subagent for this Trekoon execution lane.

Epic: <epic-id>
Lane owner: <lane-name>
Execute these Trekoon tasks IN ORDER unless task descriptions say parallel
subtasks are safe:
- Task <id>: <title>
- Task <id>: <title>

Scope:
- Target files: <paths from task descriptions>
- Read first: <paths/patterns to inspect before editing>
- Do not touch: <paths owned by other lanes>

For each task use the atomic-claim + append-progress recipe in
reference/harness-primitives.md:
- claim with --owner <lane-name>
- append progress, verification, and blockers; do not rewrite descriptions
- task done on completion; for subtasks move through in_progress first
- on block: append reason + dependency id + failing command output, then
  --status blocked

Assume other agents may edit unrelated files. Do not revert unrelated changes.

Claude Code parallel tool calls:
- Parallel Trekoon Bash calls must stay read-only (session, progress, ready,
  suggest, targeted show).
- Do not issue multiple status-changing Bash commands in one parallel batch.
  Use task/subtask claim for atomic races, then serialize updates and
  completion commands.
- If a parallel batch reports sibling cancellation, re-read the affected task
  before retrying; recover from current Trekoon state.

Use --compact in noisy Trekoon reads. Do not create branches, commits, pushes,
or PRs unless the user explicitly asked and harness policy allows it.

Final report: tasks completed, files changed, checks, review result/gap,
task done response, blockers.
```

## Use `task done` Responses

`task done` returns:

- `unblocked`: downstream tasks that became ready.
- `warning`/`openSubtaskIds`: incomplete subtasks to consciously handle.
- `next`: next ready candidate.

After every completion:

1. Dispatch newly unblocked meaningful independent work to subagents when safe.
2. If no unblocked tasks, inspect `next`.
3. If neither exists, run `trekoon --toon suggest --epic <epic-id>`.

## Direct Work Loop

Use this loop for small, tightly coupled work, or after meaningful independent
lanes are already delegated.

1. Orient:
   ```bash
   trekoon --toon session
   trekoon --toon session --epic <epic-id>
   ```
2. If diagnostics show `recoveryRequired`, stop and run `trekoon --toon init`.
3. If behind or conflicts exist, resolve sync before claiming work. Load
   `reference/sync.md` for conflict handling.
4. Claim, append, finish per the recipe in `reference/harness-primitives.md`.
5. Update important subtasks explicitly:
   ```bash
   trekoon --toon subtask claim <subtask-id> --owner <name>
   trekoon --toon subtask update <subtask-id> --append "Verified with fixture set" --status done
   ```
6. Repeat from the returned `unblocked`/`next` data. A fresh `session` is not
   needed mid-loop unless you need updated diagnostics or switch epics.

If `task done` warns about open subtasks, decide whether the task is genuinely
complete before moving on.

## Recovery

`status_transition_invalid`:

1. `trekoon --toon --compact task show <id>`
2. Append the attempted transition and current status.
3. Load `reference/status-machine.md`.
4. Apply the valid intermediate transition, then retry.
5. Continue only after the target task reaches the intended status or is marked
   `blocked` with reason.

`dependency_blocked`:

1. `trekoon --toon --compact task show <id>`
2. Append the unmet dependency.
3. `trekoon --toon task ready --epic <epic-id>`
4. Continue with a ready candidate. Do not retry the blocked task until its
   dependency is complete.

## Verify Before Closing

All applicable checks must pass before marking the epic done.

### Review

For non-trivial implementation, run a separate review pass before closing the
task or epic. Prefer a specialized review agent/skill. Review the diff for
correctness, regressions, missing tests, security, reliability, performance,
and integration risks. Tiny docs/mechanical changes may skip separate review
but record that decision.

### Tests And Manual Checks

- Run relevant automated tests for touched scope.
- Run broader tests when shared behavior or cross-module contracts changed.
- Exercise CLI/API/parser/integration changes with realistic inputs.
- Record gaps when credentials or external services are unavailable.
- Fix confusing errors, noisy output, inconsistent behavior, or rough DX.

Append evidence with the `task update --append` recipe in
`reference/harness-primitives.md`.

## Close The Epic

Before closing:

```bash
trekoon --toon epic progress <epic-id>
trekoon --toon suggest --epic <epic-id>
```

Verify all tasks are `done`, or all remaining work is `blocked` with recorded
reasons. Then mark the epic done:

```bash
trekoon --toon epic update <epic-id> --status done
```

Return final summary: tasks completed, files changed, verification, review,
remaining blockers, and dependency state.

## Update And Read Policies

Descriptions are the durable work log. Append progress instead of rewriting
descriptions unless the plan itself is wrong.

Preferred commands:

| Need | Command |
|---|---|
| Session diagnostics + next task | `trekoon --toon session` |
| Scoped session | `trekoon --toon session --epic <epic-id>` |
| Suggestions | `trekoon --toon suggest --epic <epic-id>` |
| Progress dashboard | `trekoon --toon epic progress <epic-id>` |
| Next task only | `trekoon --toon task next` |
| Ready set | `trekoon --toon task ready --epic <epic-id> --limit 50` |
| Direct blockers | `trekoon --toon dep list <task-id>` |
| What an item unblocks | `trekoon --toon dep reverse <task-or-subtask-id>` |
| One task | `trekoon --toon task show <task-id> --all` |
| One epic tree | `trekoon --toon epic show <epic-id> --all` |
| Reduce output | Add `--compact` |

Bulk updates:

- Use `--ids <csv>` or `--all`; prefer `--ids`.
- Bulk mode supports only `--append` and/or `--status`.
- Do not pass a positional ID in bulk mode.
- `--append` and `--description` are mutually exclusive.
- Use `--all` only for clear maintenance sweeps or when the user explicitly
  wants broad updates.

## Claude Agent Teams (Appendix)

Use this section only when the user explicitly asks for Claude Code Agent
Teams and `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=true`. Otherwise use the
standard subagent flow above.

Team-specific surface:

| Purpose | Tool |
|---|---|
| Create team | `TeamCreate` |
| Manage shared tasks | `TaskCreate` / `TaskList` / `TaskUpdate` / `TaskGet` |
| Spawn teammates | `Agent` with `team_name` |
| Communicate | `SendMessage` |
| Clean up | `TeamDelete` |

Flow:

1. Build the graph and mark the epic in progress per the standard flow.
2. `TeamCreate` with name `<epic-slug>` and a description naming the epic.
3. One `TaskCreate` per lane. Reuse the standard subagent prompt body
   (claim, append, task done, blocker handling). Use `blockedBy` for
   sequential lanes.
4. `Agent` with `team_name` to spawn one teammate per parallel lane.
   Use `general-purpose`; reserve `Explore`/`Plan` for read-only research.
   Use 3-5 teammates for most epics.
5. Coordinate via `SendMessage`. Update Trekoon owners with
   `task update <task-id> --owner <teammate-name>`. When all teammates
   block, run `suggest --epic <epic-id>`.
6. Close the epic per the standard flow. Then `shutdown_request` each
   teammate and `TeamDelete`.

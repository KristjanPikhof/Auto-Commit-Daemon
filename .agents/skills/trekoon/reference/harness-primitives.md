# Harness Primitives

Use these intent-level primitives across Codex, Claude Code, OpenCode, Pi, and
similar harnesses. Trekoon is durable state; harness-local todo/task tools are
only live session display.

| Need | Instruction |
|---|---|
| Orient | Read Trekoon session/progress/suggest for ready work and blockers. |
| Display progress | Use local todo/task tools for the current execution shape and lane status. |
| Ask | Use the harness question tool when available; otherwise ask one concise plain-text question. |
| Delegate | When executing an epic, use subagents by default for meaningful work that can run independently. |
| Explore | Use read-only/explorer subagents for noisy codebase lookup, logs, or research. |
| Execute | Use write-capable worker/general subagents for bounded implementation lanes. |
| Test | Run the required automated or manual checks for touched scope. |
| Review | Use a review agent/skill for non-trivial code changes when available. |
| Record | Append progress, blockers, tests, review, and completion evidence to Trekoon. |

## Delegation Default

When executing an epic, use subagents by default for any meaningful work that
can run independently. Keep small or tightly coupled tasks in the parent agent.
Use the parent session to coordinate the epic, make dependency decisions, keep
the user oriented, and synthesize results.

Treat "execute this epic", "work through this Trekoon plan", "use agents",
"spawn subagents", "delegate independent lanes", "execute with subagents",
"parallelize this", and "team execute" as requests to orchestrate work to
completion. If safe independent lanes exist and the harness supports subagents,
delegate those lanes by default.

If a higher-priority harness policy blocks subagents without explicit user
wording, tell the user immediately:

```text
I found <n> independent Trekoon lanes. This harness requires explicit
permission before I can spawn subagents. Should I delegate those lanes and keep
coordinating from the parent session?
```

## Runtime Notes

- Codex: use subagents by default when exposed. If Codex policy requires
  explicit user wording, ask immediately before broad execution. Do not silently
  do broad work in the parent. When available, use `spawn_agent`, `send_input`,
  `wait_agent`, `resume_agent`, and `close_agent`.
- Claude Code: use normal subagents for bounded side work. Use Agent Teams only
  when the user explicitly asks for team execution and the environment supports
  it. Treat parallel `Bash` tool calls as read-only unless every command is a
  safe atomic claim. Do not batch multiple Trekoon status-changing commands in
  one parallel tool turn; run them sequentially so one failed transition does
  not cancel sibling mutations.
- OpenCode: use `@explore` for read-only discovery and `@general` or native
  Task for write-capable lane work. Use `question` when available.
- Pi/other harnesses: use the same intent and native task/subagent/question
  tools when available.

## Local Task Tools

Use local todo/task tools to show only current-session coordination:

1. Execution shape and lane list.
2. Lane status: pending, in progress, blocked, review, done.
3. Short enough to stay readable.

If local state and Trekoon disagree, Trekoon wins.

## Review

For non-trivial implementation, run a separate review pass before closing the
task or epic. Prefer a specialized review agent/skill. Review the actual diff
for correctness, regressions, missing tests, security, reliability, performance,
and integration risk.

Tiny docs/mechanical changes may skip separate review. Still run relevant
checks and record the review gap or decision:

```bash
trekoon --toon task update <task-id> --append "Review: <summary or accepted gap>"
```

## Atomic Claim And Append-Progress Recipe

Canonical recipe for owning a Trekoon item and recording progress. Other
references point here instead of restating these commands.

Atomic claim before editing:

```bash
trekoon --toon task claim <task-id> --owner <name>
trekoon --toon subtask claim <subtask-id> --owner <name>
```

`task claim` races safely across parallel subagents; the loser sees the
existing owner. `task update --status in_progress` does not race; prefer
`claim` for ownership transitions.

Append progress, verification, blocker notes:

```bash
trekoon --toon task update <task-id> --append "Started implementation"
trekoon --toon task update <task-id> --append "Verified: <commands/results>"
trekoon --toon task update <task-id> --append "Review: <result or accepted gap>"
trekoon --toon task update <task-id> --append "Blocked by <reason>" --status blocked
```

Append, do not rewrite descriptions. Use `--ids <csv>` for bulk append; bulk
mode supports only `--append` and/or `--status`.

Finish:

```bash
trekoon --toon task done <task-id>
```

`task done` auto-walks `todo` or `blocked` through `in_progress`. For subtasks,
move through `in_progress` (claim or status) before `done`.

## Compact Spec Hazards

Any batch creation command (`epic create`, `epic expand`, `task create-many`,
`subtask create-many`, `dep add-many`) splits `--task`/`--subtask`/`--dep`
values on raw `|`. Three recurring footguns — applies in plan and execute
modes (e.g. mid-execution `epic expand`):

1. **Single mid-value `|`** with no explicit `|<status>` field: trailing
   text silently lands in the status slot. Creation succeeds, fails on next
   transition.
2. **`||`** (JS logical-OR, shell OR): adds two extra fields per occurrence,
   overshoots the field-count gate. Rephrase as "or" or escape as `\|\|`.
3. **Trailing `|`** is not a terminator: creates an empty final field; on a
   4-field subtask shape that becomes an empty description and the parser
   rejects with "is missing a description".

Escape literal `|` as `\|`. Pre-flight specs:

```bash
grep -nE '(^|[^\\])\|\||\|$' specs.txt
```

Full rules and the silent/loud failure matrix live in
`reference/planning.md` "CAUTION — bare-pipe footguns".

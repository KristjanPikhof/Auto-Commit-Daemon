# Planning Reference

Write plans directly into Trekoon as epics with task/subtask DAGs. A plan is
done only when the epic exists, the graph is validated, and the user can run
`trekoon <epic-id> execute` without reinterpretation.

Ask planning-critical questions before writing. Use the harness question tool
when available (`question`, `AskUserQuestion`, or native equivalent); otherwise
ask one concise plain-text question. Use multi-select only for independent
features that can combine. Use single-select for mutually exclusive choices.

## Before You Create Records

Synthesize existing context instead of rediscovering it:

1. Goal and user outcome.
2. Decisions already made and rejected options.
3. Constraints: architecture, compatibility, safety, timelines, libraries.
4. Affected areas: subsystems, paths, interfaces, workflows.
5. Verification evidence needed for completion.
6. Unknowns that block graph creation.

If the brief is sufficient, plan. If not, do targeted research or ask one narrow
question. Preserve prior user decisions unless they explicitly reopen them.
Expand existing Trekoon items when they already represent the work.

## Data Model

- Epic: full outcome, scope, constraints, and verification gates.
- Task: one complete subsystem/domain work unit that one agent can own.
- Subtask: concrete implementation/test/verification step under a task.
- Dependency: hard prerequisite only, not "nice to have" ordering.

All new entities start as `todo`. Do not create records as `in_progress`,
`blocked`, or `done`.

## Write Detailed, Executable Records

### Epic

Title format:

`<Product/Area>: <deliverable> to <user/system outcome> (<key constraint>)`

Description must include:

- Goal and why now.
- In scope and out of scope.
- Success criteria.
- Risks and constraints.
- Verification gates.
- Key prior decisions or research findings that affect implementation.

### Task

Title format:

`[<Subsystem>] <verb> <artifact/interface> to <observable outcome>`

Description must include:

- Target files: files to create or modify.
- Read first: files, functions, or patterns to inspect before editing.
- Do not touch: paths owned by other lanes.
- Acceptance criteria with observable behavior.
- Required tests or manual verification commands.
- Integration constraints and compatibility requirements.
- Owner/lane when clear.
- Parallelism note: "can run in parallel with ..." or "blocked by ...".
- Relevant prior findings so execution agents do not rediscover context.

Example:

```text
Implement refresh-token rotation endpoint.

Target files: src/auth/refresh.ts (new), src/auth/refresh.test.ts (new)
Read first: src/auth/login.ts (follow handler pattern)
Do not touch: src/billing/* (owned by billing-lane)

Follow login.ts: schema validation -> service call -> response mapping.
Acceptance: POST /auth/refresh returns a new token pair and invalidates old token.
Verify: bun test src/auth/refresh.test.ts
Owner: auth-lane
Can run in parallel with: billing-lane. Blocked by: task-types.
```

### Subtask

- Use imperative, specific titles.
- State exact artifact and completion signal.
- Use subtasks for real execution units, not filler checklist noise.
- Inherit parent task file scope unless a subtask differs.

## Design For Delegated Execution

- Tasks with no dependency edge are parallel candidates.
- Different subsystems usually become different lanes.
- Same subsystem work should usually be grouped or sequenced.
- Keep each active lane to about 3-4 tasks.
- Add owners after creation:
  ```bash
  trekoon --toon task update <task-id> --owner <lane-name>
  ```

Use dependencies for hard prerequisites only. Prefer task-to-task; use subtask
dependencies only when task-level ordering is too coarse.

## Create Records Efficiently

Use `--toon` on every planning command for stable structured responses.

Prefer one transactional command over repeated single-item creates. If the
epic, tasks, subtasks, and dependencies are known, one-shot with
`epic create --task ... --subtask ... --dep ...`. This saves tool calls and
keeps the graph internally consistent.

| Situation | Command |
|---|---|
| New epic with known graph | Prefer `trekoon --toon epic create ... --task ... --subtask ... --dep ...` |
| Existing epic needs linked additions | `trekoon --toon epic expand <epic-id> ...` |
| Many sibling tasks | `trekoon --toon task create-many --epic <epic-id> --task ...` |
| Many sibling subtasks | `trekoon --toon subtask create-many <task-id> --subtask ...` |
| Many dependency edges across existing IDs | `trekoon --toon dep add-many --dep ...` |
| One record only | `epic create`, `task create`, or `subtask create` |

Compact specs use pipe-delimited values. `\` is the escape character:

| Sequence | Produces |
|---|---|
| `\|` | literal `|` |
| `\\` | literal `\` |
| `\n` | newline |
| `\r` | carriage return |
| `\t` | tab |

Any other `\X` is invalid. Do not paste regex/shell-escaped patterns into
compact specs: `\?`, `\.`, `\{`, `\(`, `\[` etc. fail before records are
created. Prefer prose over exact regex in descriptions. Rephrase operators
like `!=` as words to avoid escaping confusion.

Bare `|` inside field values is a field separator and will silently corrupt
records when the spec omits an explicit `|<status>` field. A single shell
pipe (`cmd a | cmd b`) in a Verify line on a 3-field task spec or 4-field
subtask spec (epic create/expand) splits into one extra field, and the parser
treats that trailing fragment as the status — but does not validate it.
Creation succeeds and the record only breaks on the next status transition.
Concrete failure: `--task "task-x|API|Verify: bun test foo | tail -20"`
splits to `[task-x, API, "Verify: bun test foo ", " tail -20"]`; ` tail -20`
becomes the status, and the bad status only surfaces on the next update.

Escape literal `|` as `\|` or rephrase to avoid the character. `||` fallbacks
(`cmd a || cmd b`) and other multi-pipe constructs are caught loudly by the
parser: every unescaped `|` adds a field, so multi-pipe input overshoots the
field-count gate and fails before any record is written. The empty-field gate
fires on a different shape — a single bare middle pipe like
`key|title||desc`. Either way, the silent failure mode is specifically the
single-pipe / no-explicit-status case. Specs that already pass `|<status>`
fail loudly even on a single unescaped `|`.

A bare `|` at the very end of a spec (trailing pipe) is **not** a terminator.
It produces an empty final field. On a `<...>|<title>|<description>` shape
that empty field becomes the description and the parser rejects the spec with
"is missing a description". Drop trailing `|`; never use it as a "done" marker.

Spec shape (status optional, defaults to `todo`):

- `--task <temp-key>|<title>|<description>` or `<temp-key>|<title>|<description>|<status>`
- `--subtask <temp-key>|<title>|<description>` or `<temp-key>|<title>|<description>|<status>` (subtask create-many)
- `--subtask <parent-ref>|<temp-key>|<title>|<description>` or `<parent-ref>|<temp-key>|<title>|<description>|<status>` (epic create/expand)

Prefer the shorter form. Pass an explicit `|<status>` only when seeding a
non-`todo` status.

The three bare-pipe footguns (`||`, single mid-value `|`, trailing `|`) and
the pre-flight `grep -nE '(^|[^\\])\|\||\|$'` recipe live in
`reference/harness-primitives.md` "Compact Spec Hazards". The paragraphs
above add the silent-vs-loud detail that the quick-ref points at. When in
doubt, build descriptions as plain prose without operator characters.

One-shot rules:

- Declare tasks/subtasks with plain temp keys, e.g. `task-api`, `sub-api-tests`.
- Temp keys form a flat namespace per command. Every `--task` and `--subtask`
  key must be unique across the whole `epic create` / `epic expand` call, not
  per parent task. Prefix subtask keys with the parent task key
  (`sub-api-tests`, `sub-ui-states`) to stay safe across re-runs.
- Refer to records created in the same command as `@task-api` or
  `@sub-api-tests`.
- Use `@task-key` as the subtask parent ref and in `--dep` specs.
- `--dep <source>|<depends-on>` points from blocked item to prerequisite.
- `epic create` returns `result.mappings` and counts. Use those real UUIDs in
  handoff summaries and follow-up updates. Never show temp keys as real IDs.
- `dep add-many` does not resolve temp keys from earlier commands. Use real IDs.

One-shot example (status omitted, defaults to `todo`):

```bash
trekoon --toon epic create \
  --title "Checkout: add idempotent payment capture" \
  --description "Goal: prevent duplicate charges.\nVerification: bun test tests/payments" \
  --task "task-api|[API] add capture endpoint|Target files: src/payments/capture.ts\nRead first: src/payments/service.ts\nDo not touch: src/ui/*\nAcceptance: duplicate request returns prior result.\nVerify: bun test tests/payments/capture.test.ts\nOwner: api-lane\nCan run in parallel with: task-ui." \
  --task "task-ui|[UI] show capture states|Target files: src/ui/checkout.tsx\nRead first: src/ui/form.tsx\nDo not touch: src/payments/*\nAcceptance: loading and retry states render.\nVerify: bun test tests/ui/checkout.test.ts\nOwner: ui-lane\nBlocked by: task-api." \
  --subtask "@task-api|sub-api-tests|Write capture tests|Cover idempotent retry and conflict responses." \
  --subtask "@task-ui|sub-ui-states|Write UI state tests|Cover loading, success, retry, and error states." \
  --dep "@task-ui|@task-api"
```

## Append Efficiently

Use the append-progress recipe from `reference/harness-primitives.md` for
planning notes, shared findings, verification, or refinements. Appending is
cheaper and safer than rewriting full descriptions.

Bulk append uses IDs from `result.mappings`, `epic show --all`, or
`task ready`:

```bash
trekoon --toon task update --ids id1,id2,id3 --append "Shared verification: bun test tests/payments"
```

Important:

- `epic update <epic-id> --append ...` appends only to the epic description.
- There is no scoped "append to all tasks in this epic" command.
- Do not use `task update --all --append ...` unless you truly mean every task
  in the database.
- `epic update <epic-id> --all` is cascade status mode only and rejects
  `--append`.
- Bulk append is per-row, not atomic.

## Validate Before Handoff

After creating or expanding the epic:

```bash
trekoon --toon epic progress <epic-id>
trekoon --toon suggest --epic <epic-id>
trekoon --toon task ready --epic <epic-id> --limit 50
```

Verify:

- Counts match expectations.
- All new tasks are `todo`.
- Ready count equals first-wave tasks with no dependencies.
- Blocked tasks show the expected dependencies.
- `suggest` returns a sensible first execution candidate and no recovery/sync
  issue that blocks execution.

## Handoff Summary

Return the actual Trekoon epic and first execution wave, not prose-only design.

- Full UUIDs in summaries; never temp keys outside create commands.
- Render IDs in code formatting.
- Group tasks by wave with title, owner/lane, dependencies.
- Include final verification gate.

Format:

```text
Epic: <full-uuid>
Title: <epic title>

Wave 1 (parallel)
| ID | Task | Owner |
|---|---|---|
| <uuid> | [API] Payment capture endpoint | api-lane |

Wave 2
| ID | Task | Depends on |
|---|---|---|
| <uuid> | [API] Retry logic | <wave-1-uuid> |

Verification: bun run build && bun run test
```

## Search And Replace

Use scoped search before manual tree reads for repeated paths, labels, owners,
or migration targets. Narrowest scope first:
`subtask search`/`replace`, then `task`, then `epic`.

```bash
trekoon --toon epic search <epic-id> "path/to/somewhere"
trekoon --toon epic replace <epic-id> --search "path/to/somewhere" --replace "path/to/new-path"
trekoon --toon epic replace <epic-id> --search "path/to/somewhere" --replace "path/to/new-path" --apply
```

Guardrails:

- Use literal, explicit search text.
- Narrow fields when useful: `--fields title`, `--fields description`, or
  `--fields title,description`.
- Preview before `--apply`.
- Prefer scoped replace over manually editing many records one by one.

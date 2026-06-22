# Intent commit flow

Intent mode groups related captures before replay writes a commit. Capture
durability stays the same: every edit is still stored first, then planning
decides which pending seqs can publish together.

Use `event` mode when you want one capture per commit. Use `intent` when local
history should read like review-ready changes.

## Setup

Sparse repo:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
export ACD_AI_DIFF_EGRESS=1
export ACD_INTENT_WINDOW=5
export ACD_INTENT_MIN_PENDING=3
export ACD_INTENT_SETTLE_WINDOW=10s
export ACD_INTENT_MAX_PENDING_AGE=60s
export ACD_INTENT_DEFER_LIMIT=1
~~~

Busy repo:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
export ACD_AI_DIFF_EGRESS=1
export ACD_INTENT_WINDOW=10
export ACD_INTENT_MIN_PENDING=8
export ACD_INTENT_SETTLE_WINDOW=10s
export ACD_INTENT_MAX_PENDING_AGE=5m
export ACD_INTENT_DEFER_LIMIT=1
export ACD_INTENT_RETRY_ON_INVALID=1
~~~

Prompt-end flush from a registered harness session:

~~~bash
acd flush --repo . --session-id "$ACD_SESSION_ID" --logical
~~~

Logical flush bypasses only the batch wait. It does not bypass validation,
terminal barriers, or replay safety checks.

## Flow

~~~mermaid
flowchart TB
  A["Capture rows<br/>state=pending"] --> B{"Batch ready?"}
  B -->|no| C["Wait for count,<br/>age, or logical flush"]
  C --> B
  B -->|yes| D{"Burst settled?"}
  D -->|no| C
  D -->|yes| E["Build planner window"]
  E --> F["AI planner"]
  F --> G{"Plan valid?"}
  G -->|no| H["Deterministic<br/>one-capture fallback"]
  G -->|yes| I{"Message OK?"}
  I -->|needs rewrite| J["Locked message rewrite"]
  J --> K{"Rewrite valid?"}
  K -->|no| H
  I -->|yes| L["Replay selected group(s)"]
  K -->|yes| L
  H --> L
  L --> M[("decision ledger")]

  classDef queue fill:#243447,stroke:#7aa2f7,color:#e6edf3
  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef provider fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef fallback fill:#402b2b,stroke:#f7768e,color:#ffe8ee
  class A,M queue
  class B,D,G,I,K decision
  class F,J provider
  class H fallback
~~~

## Planner input

| Field | Meaning |
|---|---|
| `offered_captures` | Candidate seqs with path, op, timestamp, defer count, fidelity, and optional captured diff. |
| `recent_commits` | Compact branch context. |
| `path_commit_context` | Recent commits for paths touched by the offered captures. |
| `forced_aging` | True when a repeatedly deferred capture is forced into a one-capture window. |
| `path_recent_commits` | Optional hint that an offered path recently changed at HEAD. It is not an amend feature. |

By default, consecutive captures that touch the same path remain separate
planner-visible seqs. This lets the planner split independent edits inside one
file when replay can apply them safely. `ACD_INTENT_PATH_COALESCE=1` restores
the legacy behavior that folds consecutive same-path captures into one planner
offer; `hidden_seqs` then records the original seqs covered by that folded
offer.

The planner must put every offered seq in either `selected_seqs` or
`deferred_seqs`. It may select one seq or a larger related subset. Deferred
seqs need reasons.

For a mixed window, the planner may return `commit_groups`. Groups publish in
the returned order, each with its own selected seqs, message, and grouping
reason. Top-level `selected_seqs` remains the union of all group selections for
legacy compatibility.

## Message rules

`ACD_COMMIT_FORMAT=imperative` is the default and keeps the existing rules.
Small single-file commits can use only a subject:

~~~text
Refine prompt validation
~~~

Multi-file, larger, mixed code/test/docs/config, CLI, migration, recovery,
template, workflow, installer, and public contract changes need body bullets:

~~~text
Surface rewrite diagnostics

- Show recent rewrite counts in status output
- Preserve fallback reasons for diagnose and events inspection
~~~

Rejected examples:

| Subject | Why it is rejected |
|---|---|
| `Update file` | Generic. |
| `Update parsed` | Token-only. |
| `Update effort.ts` | Filename-only. |

When only the message is weak, ACD sends a locked rewrite request. The provider
may change `subject` and `body`, but not selected seqs, deferred seqs, or
grouping rationale.

`ACD_COMMIT_FORMAT=conventional` opts into scope-less Conventional Commit
subjects:

~~~text
feat: add intent format reporting
~~~

Accepted types are `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`,
`chore`, `perf`, `style`, and `revert`. Scopes and breaking markers are not
accepted. Body bullets keep the same format as imperative commits. If a planner
or locked rewrite returns the wrong format, ACD records the validation issue,
uses configured correction retries when available, and then falls back to a
deterministic one-capture message in the selected format.

## Deferrals

Deferral is normal. It means the planner decided a capture did not belong in the
current commit.

After `ACD_INTENT_MIN_PENDING` is reached, ACD waits for
`ACD_INTENT_SETTLE_WINDOW` with no newer visible capture before planning. A full
`ACD_INTENT_WINDOW`, `ACD_INTENT_MAX_PENDING_AGE`, forced aging, or
`acd flush --logical` bypasses that settle wait.

When a capture reaches `ACD_INTENT_DEFER_LIMIT`, ACD forces it through a
one-capture planning window unless an earlier related-path capture must land
first. If the provider fails there, deterministic fallback publishes the
capture safely.

## Observability

| Question | Command |
|---|---|
| What strategy and batch gate are active? | `acd status --json` |
| Why is intent waiting? | `acd diagnose --json` or `acd doctor` |
| Which seqs were grouped or deferred? | `acd events --json` |
| What did the provider see? | `ACD_AI_PROMPT_TRACE=1` then `acd prompt --seq <seq>` |
| Where are rejected planner responses? | `<gitDir>/acd/planner-rejects.jsonl` |

`intent_strategy` reports `commit_format`, window settings, batch wait state,
settle countdowns, deferred counts, forced-aging readiness, planner error rate,
singleton commit rate, message-quality rewrite or fallback counts, and the most
recent privacy-safe `last_planner_window` summary.

`acd events --json` adds `planner_window` to decisions with a known planner
window. The summary shows `offered_seqs`, `visible_original_seqs`,
`hidden_seqs`, `selected_groups`, `deferred_seqs`, forced/fallback metadata,
and per-event participation flags. It never stores raw diffs; use prompt traces
only when the exact provider payload is needed.

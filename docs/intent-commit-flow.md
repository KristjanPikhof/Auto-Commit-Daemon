# Intent commit flow

Intent mode lets ACD group related captured edits into semantic commits while
preserving replay safety. The default `event` strategy still commits one capture
at a time; use intent mode only when you want the AI planner to decide which
pending captures belong together.

## Quick setup

Sparse repos should keep the batch small and the age trigger short:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
export ACD_AI_DIFF_EGRESS=1
export ACD_INTENT_WINDOW=5
export ACD_INTENT_MIN_PENDING=3
export ACD_INTENT_MAX_PENDING_AGE=60s
export ACD_INTENT_DEFER_LIMIT=1
~~~

Busy repos can wait for larger groups:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
export ACD_AI_DIFF_EGRESS=1
export ACD_INTENT_WINDOW=10
export ACD_INTENT_MIN_PENDING=8
export ACD_INTENT_MAX_PENDING_AGE=5m
export ACD_INTENT_DEFER_LIMIT=1
~~~

Enable prompt tracing only while debugging:

~~~bash
export ACD_AI_PROMPT_TRACE=1
acd prompt --last
acd prompt --seq <capture-seq>
~~~

Prompt traces are local diagnostics under `<gitDir>/acd/prompt-trace/`. They
are redacted and truncated before writing, but can still contain source text and
private paths.

## Flow

~~~mermaid
flowchart TB
  A[Capture event] --> B[(state.db pending)]
  B --> C{Batch gate met?}
  C -- no --> D[Wait for min count<br/>or max age]
  D --> C
  C -- yes --> E[Build planner request]
  E --> F[AI intent planner]
  F --> G{Valid grouping?}
  G -- no --> H[Deterministic fallback]
  G -- yes --> I{Message quality OK?}
  I -- rewrite --> J[Message-only rewrite]
  J --> K{Rewrite OK?}
  K -- no --> H
  I -- yes --> L[Publish selected seqs]
  K -- yes --> L
  H --> L
  L --> M[(decision_records)]
  M --> N[status / diagnose / events]

  classDef queue fill:#243447,stroke:#7aa2f7,color:#e6edf3
  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef provider fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef fallback fill:#402b2b,stroke:#f7768e,color:#ffe8ee
  class B,M queue
  class C,G,I,K decision
  class F,J provider
  class H fallback
~~~

## What the planner receives

ACD offers a bounded pending window to the provider as structured
`capture_intent_plan` input. The request includes:

| Field | Purpose |
|---|---|
| `offered_captures` | Candidate seqs with path, op, timestamp, fidelity, defer count, and optional captured diff. |
| `latest_commit` | Recent HEAD context for the branch. |
| `path_commit_context` | Recent commits for paths touched by the offered captures. |
| `forced_aging` | True when a repeatedly deferred capture is forced into a one-capture window. |
| `path_recent_commits` | Hint that an offered path recently changed at HEAD. It does not amend commits. |

The planner must classify every offered seq as selected or deferred. It may
select exactly one capture or a larger related subset. Deferred seqs require one
reason each.

## Message quality gate

Grouping validation and message quality are separate. A valid grouping can still
be rejected for a weak commit message.

Small single-file commits can use a semantic subject without a body:

~~~text
Refine prompt validation
~~~

Body bullets are required for multi-file changes, larger diffs, mixed
code/test/docs/config changes, and high-impact paths such as CLI, config,
migrations, recovery, public API, templates, workflows, and installer scripts:

~~~text
Surface rewrite diagnostics

- Show recent message-quality rewrite counts in status output
- Preserve fallback reasons for diagnose and events inspection
~~~

ACD rejects generic, token-only, and filename-only subjects such as:

| Rejected subject | Reason |
|---|---|
| `Update file` | Generic subject. |
| `Update parsed` | Token-only subject. |
| `Update effort.ts` | Filename-only subject. |

When only the message is weak, ACD sends a locked message-only rewrite request.
The provider can replace `subject` and `body`, but cannot change
`selected_seqs`, `deferred_seqs`, `grouping_reason`, or `deferred_reasons`.

## Forced aging

Repeated deferrals eventually force the overdue capture into a one-capture
window. Non-deterministic providers still receive that locked request so the
message quality gate and rewrite path can run. If the provider is unavailable,
times out, or returns an unsafe rewrite, ACD falls back to the bounded
deterministic forced-aging path and still publishes the capture safely.

## Observability

Use these commands to inspect intent behavior:

~~~bash
acd status --json
acd diagnose --json
acd events --json
acd prompt --seq <capture-seq>
~~~

`intent_strategy` reports active window settings, batch wait state, deferred
counts, forced-aging readiness, planner error rate, singleton commit rate, and
message-quality rewrite/fallback fields. `decision_records` include
`intent_deferred`, `intent_forced`, `intent_planner_error`,
`message_quality_rewrite`, and `message_quality_fallback` rows when those paths
run.

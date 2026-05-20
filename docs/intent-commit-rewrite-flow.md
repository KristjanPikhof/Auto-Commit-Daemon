# Intent commit rewrite flow

`acd rewrite-commits` is the operator-driven flow for improving messages on
existing commits with the same AI intent planner family used by intent mode. It
is for local, reviewable history cleanup before sharing a branch.

Important boundaries:

- **Plan generation requires working AI intent mode.** Set
  `ACD_COMMIT_STRATEGY=intent` and configure a usable non-deterministic planner
  provider (`openai-compat` with `ACD_AI_API_KEY`, or `subprocess:<name>`).
  Deterministic fallback is not enough.
- **The daemon never rewrites commits automatically.** ACD may capture and replay
  new work in the background, but history rewrite is started, reviewed, and
  applied only by an explicit `acd rewrite-commits` CLI invocation.
- Saved plan display and saved plan apply can be used without the AI provider
  gate because they do not ask the provider to generate a new plan.
- v1 rewrite scope is the current branch and linear commit ranges. Merge commit
  rewrites are refused.

## Generate a plan

Choose commits by SHA, 1-based position from `HEAD`, contiguous position range,
newest-count, or advanced git revset:

~~~bash
# From a commit-ish through HEAD.
acd rewrite-commits --from 8f4c2a1 --plan-out rewrite.json

# From the 5th newest commit through HEAD; position 1 is HEAD.
acd rewrite-commits --from 5 --plan-out rewrite.json

# Rewrite positions 5 through 12, recreating newer commits unchanged as needed.
acd rewrite-commits --range 5-12 --plan-out rewrite.json

# Rewrite the newest n commits.
acd rewrite-commits --last 4 --plan-out rewrite.json

# Advanced explicit git range: exclusive base, inclusive head.
acd rewrite-commits --git-range main~12..main~4 --plan-out rewrite.json
~~~

~~~mermaid
flowchart TB
  A[Operator chooses commits] --> B{Selector type}
  B -->|--from sha| C[Resolve commit-ish through HEAD]
  B -->|--from 5| D[Resolve 1-based position 5 through HEAD]
  B -->|--range 5-12| E[Resolve contiguous position range]
  B -->|--last n| F[Resolve newest n commits]
  B -->|--git-range base..head| G[Resolve linear git revset]
  C --> H[Validate current branch safety]
  D --> H
  E --> H
  F --> H
  G --> H
  H --> I{Working AI intent mode?}
  I -- no --> J[Refuse plan generation]
  I -- yes --> K[Send selected commit messages/context to planner]
  K --> L[Validate proposed message-only plan]
  L --> M[Save plan for review or apply]

  classDef operator fill:#243447,stroke:#7aa2f7,color:#e6edf3
  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef provider fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef reject fill:#402b2b,stroke:#f7768e,color:#ffe8ee
  class A,M operator
  class B,I decision
  class K provider
  class J reject
~~~

## Review and optionally edit

Always review the saved plan before applying. The plan is the safety boundary:
it records the original commits, proposed messages, validation status, and apply
status so a later command can reuse the same decision without asking the AI
provider again.

~~~bash
acd rewrite-commits --show-plan rewrite.json
acd rewrite-commits --edit <plan-id-or-file> --format text --plan-only
acd rewrite-commits --edit <plan-id-or-file> --dry-run
~~~

~~~mermaid
sequenceDiagram
  participant O as Operator
  participant CLI as acd rewrite-commits
  participant Plan as Saved plan
  participant AI as Intent planner

  O->>CLI: generate with --plan-out rewrite.json
  CLI->>AI: request message rewrite plan
  AI-->>CLI: proposed subjects/bodies
  CLI->>Plan: persist draft/valid plan
  O->>CLI: --show-plan rewrite.json
  CLI-->>O: display original and proposed messages
  O->>CLI: --edit plan-id-or-file
  CLI->>Plan: load saved plan without AI call
  CLI-->>O: open EDITOR with text/json plan
  CLI->>Plan: validate and save edited revision when changed
  CLI-->>O: unchanged/edited status and apply prompt
~~~

## Apply a saved plan

Apply is explicit and confirmation-gated. Use `--dry-run` first to verify the
plan and branch still match; use `--yes` only after reviewing the dry-run.

~~~bash
acd rewrite-commits --apply-plan rewrite.json --dry-run
acd rewrite-commits --apply-plan rewrite.json --yes
~~~

The rewrite operation is message-only: it recreates selected commits with their
planned messages and recreates any newer commits unchanged so the branch remains
linear. It should create a pre-apply backup before moving the branch ref. Keep
that backup until the rewritten branch has passed review.

~~~mermaid
flowchart TB
  A[Operator applies saved plan] --> B[Load saved plan]
  B --> C{Plan valid and branch still matches?}
  C -- no --> D[Refuse without moving branch]
  C -- yes --> E[Create pre-apply backup]
  E --> F[Recreate selected commits with planned messages]
  F --> G[Recreate newer commits unchanged]
  G --> H[Move current branch ref]
  H --> I[Mark plan applied]

  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef safe fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef reject fill:#402b2b,stroke:#f7768e,color:#ffe8ee
  class C decision
  class E,H,I safe
  class D reject
~~~

## Backup recovery

If review shows the rewrite was wrong, recover before doing more work on top of
the rewritten branch. Prefer the backup ref or backup SHA printed by the apply
command; fall back to `git reflog` only when the explicit backup is unavailable.

~~~bash
# Example shape; use the exact backup ref or SHA printed by apply.
git reset --hard refs/acd/rewrite-backups/<plan-id>

# If only a SHA was printed:
git reset --hard <backup-sha>

# Last-resort manual inspection:
git reflog --date=iso
~~~

~~~mermaid
flowchart TB
  A[Bad rewrite discovered] --> B{Have apply backup ref/SHA?}
  B -- yes --> C[git reset --hard backup]
  B -- no --> D[Inspect git reflog]
  D --> E[Find pre-rewrite HEAD]
  E --> F[git reset --hard pre-rewrite HEAD]
  C --> G[Re-run status/tests]
  F --> G
  G --> H{Need another rewrite?}
  H -- yes --> I[Generate or edit a new plan]
  H -- no --> J[Continue work]

  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef recover fill:#203a31,stroke:#9ece6a,color:#eaffdf
  class B,H decision
  class C,F,G recover
~~~

## Reuse a saved plan

Saved plans make review reproducible. A plan generated on one run can be shown,
dry-run applied, or applied later without a working AI provider, as long as the
repository state still matches the plan's original commit IDs.

~~~bash
# No provider needed for display/apply of an existing plan.
ACD_COMMIT_STRATEGY=event ACD_AI_PROVIDER=deterministic acd rewrite-commits --show-plan rewrite.json
ACD_COMMIT_STRATEGY=event ACD_AI_PROVIDER=deterministic acd rewrite-commits --apply-plan rewrite.json --dry-run
~~~

~~~mermaid
sequenceDiagram
  participant O as Operator
  participant CLI as acd rewrite-commits
  participant Plan as Saved plan
  participant Git as Git branch

  O->>CLI: --show-plan rewrite.json
  CLI->>Plan: load without provider gate
  CLI-->>O: display plan
  O->>CLI: --apply-plan rewrite.json --dry-run
  CLI->>Plan: load without provider gate
  CLI->>Git: verify original commit IDs still match
  Git-->>CLI: match or mismatch
  CLI-->>O: dry-run result
  O->>CLI: --apply-plan rewrite.json --yes
  CLI->>Git: backup, recreate commits, move branch
~~~

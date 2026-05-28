# Intent commit rewrite flow

`acd rewrite-commits` is for local history cleanup before you share a branch. It
asks the same intent planner family to improve commit messages, saves a plan,
lets you review or edit that plan, and applies it only when you say yes.

The daemon never rewrites commits automatically.

## Boundaries

| Rule | Meaning |
|---|---|
| Plan generation needs working intent mode | Set `ACD_COMMIT_STRATEGY=intent` and a non-deterministic provider. |
| Saved plan show/edit/apply does not need the provider | No new AI call is made. |
| v1 rewrites linear ranges on the current branch | Merge commit rewrites are refused. |
| Apply is message-only | File contents stay the same. |
| Apply requires a quiet ACD queue | Stop the daemon and clear pending captures first. |

## Generate a plan

~~~bash
# Commit-ish through HEAD.
acd rewrite-commits --from 8f4c2a1 --plan-out rewrite.json

# 1-based position through HEAD. Position 1 is HEAD.
acd rewrite-commits --from 5 --plan-out rewrite.json

# Positions 5 through 12.
acd rewrite-commits --range 5-12 --plan-out rewrite.json

# Newest four commits.
acd rewrite-commits --last 4 --plan-out rewrite.json

# Advanced explicit range. Base is exclusive, head is inclusive.
acd rewrite-commits --git-range main~12..main~4 --plan-out rewrite.json
~~~

~~~mermaid
flowchart TB
  A["Choose commits"] --> B{"Selector"}
  B --> C["Resolve linear range"]
  C --> D{"Intent provider usable?"}
  D -->|no| E["Refuse plan generation"]
  D -->|yes| F["Ask planner for<br/>message-only plan"]
  F --> G{"Plan valid?"}
  G -->|no| H["Save invalid plan<br/>apply blocked"]
  G -->|yes| I["Save plan"]

  classDef work fill:#243447,stroke:#7aa2f7,color:#e6edf3
  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef provider fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef stop fill:#402b2b,stroke:#f7768e,color:#ffe8ee
  class A,C,I work
  class B,D,G decision
  class F provider
  class E,H stop
~~~

## Review and edit

Review before apply:

~~~bash
acd rewrite-commits --show-plan rewrite.json
acd rewrite-commits --edit rewrite.json --format text --plan-only
acd rewrite-commits --edit rewrite.json --dry-run
~~~

~~~mermaid
sequenceDiagram
  participant O as Operator
  participant CLI as rewrite-commits
  participant Plan as Saved plan

  O->>CLI: --show-plan rewrite.json
  CLI->>Plan: load
  CLI-->>O: display old and proposed messages
  O->>CLI: --edit rewrite.json
  CLI-->>O: open EDITOR
  O->>CLI: save edits
  CLI->>Plan: validate and save
  CLI-->>O: print next apply commands
~~~

Use `--format text` for manual edits and `--format json` for automation.

## Apply a plan

Always dry-run first:

~~~bash
acd rewrite-commits --apply-plan rewrite.json --dry-run
acd rewrite-commits --apply-plan rewrite.json --yes
~~~

Apply verifies that the original commit IDs still match the branch, creates a
backup, recreates selected commits with planned messages, recreates newer
commits unchanged when needed, moves the current branch ref, and marks the plan
applied.

~~~mermaid
flowchart TB
  A["Apply saved plan"] --> B["Load plan"]
  B --> C{"Plan valid and<br/>branch matches?"}
  C -->|no| D["Refuse without<br/>moving branch"]
  C -->|yes| E["Create backup ref/SHA"]
  E --> F["Recreate commits<br/>with planned messages"]
  F --> G["Recreate newer commits<br/>unchanged"]
  G --> H["Move branch ref"]
  H --> I["Mark plan applied"]

  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef safe fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef stop fill:#402b2b,stroke:#f7768e,color:#ffe8ee
  class C decision
  class E,F,G,H,I safe
  class D stop
~~~

## Recover a bad rewrite

Use the backup ref or SHA printed by apply:

~~~bash
git reset --hard refs/acd/rewrite-backups/<plan-id>
git reset --hard <backup-sha>
~~~

If that is unavailable:

~~~bash
git reflog --date=iso
git reset --hard <pre-rewrite-head>
~~~

Then rerun your normal checks.

## Reuse a saved plan

Saved plans make review reproducible. You can show, dry-run, or apply a plan
later without a working AI provider as long as the repository still matches the
plan.

~~~bash
ACD_COMMIT_STRATEGY=event ACD_AI_PROVIDER=deterministic \
  acd rewrite-commits --show-plan rewrite.json

ACD_COMMIT_STRATEGY=event ACD_AI_PROVIDER=deterministic \
  acd rewrite-commits --apply-plan rewrite.json --dry-run
~~~

See [rewrite-commits.md](rewrite-commits.md) for the full command grammar.

# `acd rewrite-commits` command contract

`acd rewrite-commits` creates, reviews, and applies AI-generated commit-message
rewrite plans for local branch cleanup. It rewrites messages only. It does not
change file contents.

The daemon never starts this command automatically.

## Safe path

~~~bash
ACD_COMMIT_STRATEGY=intent ACD_AI_PROVIDER=openai-compat ACD_AI_API_KEY=... \
  acd rewrite-commits --from 5 --plan-out rewrite.json --plan-only

acd rewrite-commits --show-plan rewrite.json
acd rewrite-commits --apply-plan rewrite.json --dry-run
acd rewrite-commits --apply-plan rewrite.json --yes
~~~

If apply prints a backup ref or SHA and the rewrite is wrong:

~~~bash
git reset --hard refs/acd/rewrite-backups/<plan-id>
git reset --hard <backup-sha>
~~~

## Scope

| Supported | Refused |
|---|---|
| Current branch | Merge commit rewrites |
| Linear ranges | Detached HEAD |
| Message-only rewrites | Dirty branch mismatch |
| Saved-plan show, edit, dry-run, and apply | Applying while the daemon is running |
| Saved-plan reuse without an AI provider | Applying while ACD has pending captures |

Plan generation requires working intent mode with a non-deterministic planner:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
~~~

`deterministic` fallback is not enough to generate a new rewrite plan. It is
fine for showing or applying an existing saved plan.

## Select commits

| Selector | Meaning |
|---|---|
| `--from <sha>` | Rewrite from commit-ish through `HEAD`. |
| `--from <position>` | Rewrite from a 1-based position through `HEAD`; position `1` is `HEAD`. |
| `--range <start-end>` | Rewrite a 1-based contiguous range. Newer commits are recreated unchanged when needed. |
| `--last <n>` | Rewrite the newest `n` commits. |
| `--git-range <base>..<head>` | Advanced revset. Base is exclusive, head is inclusive. |
| `--base <base> --head <head>` | Deprecated alias for the explicit git range. |

Examples:

~~~bash
acd rewrite-commits --from 8f4c2a1 --plan-out rewrite.json
acd rewrite-commits --from 5 --plan-only
acd rewrite-commits --range 5-12 --review --format text
acd rewrite-commits --last 4 --no-review --yes
acd rewrite-commits --git-range main~12..main~4 --format json
~~~

## Work with plans

| Command | What it does |
|---|---|
| `--plan-out FILE` | Saves the generated plan to a file. |
| `--plan-only` | Saves the plan and prints next commands. Git history is unchanged. |
| `--show-plan FILE` | Displays a saved plan without calling the provider. |
| `--edit <plan-id-or-file>` | Opens `$EDITOR`, validates edits, and saves an edited revision. |
| `--format text` | Human-editable format. Default. |
| `--format json` | Automation-friendly editor format. |
| `--apply-plan FILE --dry-run` | Validates that the saved plan still matches the branch. |
| `--apply-plan FILE --yes` | Applies the saved plan after validation. |
| `--apply <plan-id-or-file>` | Applies a saved plan by id or by file path. |

Noninteractive apply requires either `--dry-run` or `--yes`. Declining an
interactive apply prompt prints `No rewrite performed.`

## Command grammar

~~~text
acd rewrite-commits --from <sha|position> [--plan-out FILE] [--review|--no-review] [--format text|json] [--plan-only|--yes]
acd rewrite-commits --range <start-end> [--plan-out FILE] [--review|--no-review] [--format text|json] [--plan-only|--yes]
acd rewrite-commits --last <n> [--plan-out FILE] [--review|--no-review] [--format text|json] [--plan-only|--yes]
acd rewrite-commits --git-range <base>..<head> [--plan-out FILE] [--review|--no-review] [--format text|json] [--plan-only|--yes]
acd rewrite-commits --base <base> [--head <head>] [--plan-out FILE] [--review|--no-review] [--format text|json] [--plan-only|--yes]
acd rewrite-commits --show-plan FILE
acd rewrite-commits --edit <plan-id-or-file> [--format text|json] [--plan-only|--yes|--dry-run]
acd rewrite-commits --apply-plan FILE (--yes | --dry-run)
acd rewrite-commits --apply <plan-id-or-file> (--yes | --dry-run)
~~~

For the end-to-end workflow and diagrams, see
[intent-commit-rewrite-flow.md](intent-commit-rewrite-flow.md).

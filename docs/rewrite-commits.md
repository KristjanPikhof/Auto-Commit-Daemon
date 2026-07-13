# `acd rewrite-commits` command contract

`acd rewrite-commits` creates, reviews, and applies AI-generated commit-message
rewrite plans for local branch cleanup. It rewrites messages only. It does not
change file contents.

The daemon never starts this command automatically.

## Safe path

~~~bash
ACD_COMMIT_STRATEGY=intent ACD_AI_PROVIDER=openai-compat ACD_AI_API_KEY=... \
  acd rewrite-commits --from-nr 5 --plan-out rewrite.json --plan-only

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

Rewrite plans use the active `ACD_COMMIT_FORMAT` when they are generated. The
default is `imperative`. Set
`ACD_COMMIT_FORMAT=conventional` to request scope-less Conventional Commit
subjects such as `fix: preserve rewrite plan format`. Accepted conventional
types are `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`, `chore`,
`perf`, `style`, and `revert`; scopes and breaking markers are rejected. Body
bullets keep the same `- ` prefix and wrapping rules in both modes.

## Select commits

| Selector | Meaning |
|---|---|
| `--from-sha <sha>` | Rewrite from commit-ish through `HEAD`; numeric-looking values are treated as commits. |
| `--from-nr <position>` | Rewrite from a 1-based position through `HEAD`; position `1` is `HEAD`. |
| `--range-nr <start-end>` | Rewrite a 1-based contiguous range. Newer commits are recreated unchanged when needed. |
| `--range-sha <base>..<head>` | Rewrite a simple git range. Base is exclusive, head is inclusive. |
| `--last <n>` | Rewrite the newest `n` commits. |

Examples:

~~~bash
acd rewrite-commits --from-sha 8f4c2a1 --plan-out rewrite.json
acd rewrite-commits --from-nr 5 --plan-only
acd rewrite-commits --range-nr 5-12 --review --format text
acd rewrite-commits --range-sha main~12..main~4 --format json
acd rewrite-commits --last 4 --no-review --yes
~~~

The CLI also accepts compatibility selectors. Use `acd rewrite-commits --help`
when you need to open a plan created with one of those forms.

## Progress output

| Mode | Behavior |
|---|---|
| `--progress auto` | Default. Uses plain progress on a TTY and stays silent for non-TTY output. |
| `--progress plain` | Writes stable line progress to stderr. |
| `--progress json` | Writes JSONL progress events to stderr. |
| `--progress off` | Disables progress. |
| `--quiet` | Suppresses progress output. |

Progress never goes to stdout. Selection and command results remain parseable
when `--json` is set. Generation progress reports selection, provider, proposal,
save, validation, and next-step phases. Apply progress reports validation,
backup, selected recreation, unchanged descendant recreation, ref update, and
state reconciliation.

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

Saved plans store their commit format. `--show-plan` prints it, `--edit`
preserves it, and edited text or JSON must still match the stored format. When a
provider proposes a wrong-format conventional rewrite, ACD saves the plan as
invalid with the original messages retained so you can inspect or regenerate it
without accidentally applying malformed subjects.

## Command grammar

`--progress auto|plain|json|off` is accepted by generate, edit, and apply modes
that run through the rewrite command.

For fresh plan generation, `--json` prints the resolved selection and exits
before calling the provider. Use normal text output to generate and save a new
plan.

~~~text
acd rewrite-commits --from-sha <sha> [--plan-out FILE] [--review|--no-review] [--format text|json] [--progress auto|plain|json|off] [--plan-only|--yes]
acd rewrite-commits --from-nr <position> [--plan-out FILE] [--review|--no-review] [--format text|json] [--progress auto|plain|json|off] [--plan-only|--yes]
acd rewrite-commits --range-nr <start-end> [--plan-out FILE] [--review|--no-review] [--format text|json] [--progress auto|plain|json|off] [--plan-only|--yes]
acd rewrite-commits --range-sha <base>..<head> [--plan-out FILE] [--review|--no-review] [--format text|json] [--progress auto|plain|json|off] [--plan-only|--yes]
acd rewrite-commits --last <n> [--plan-out FILE] [--review|--no-review] [--format text|json] [--plan-only|--yes]
acd rewrite-commits --show-plan FILE
acd rewrite-commits --edit <plan-id-or-file> [--format text|json] [--plan-only|--yes|--dry-run]
acd rewrite-commits --apply-plan FILE (--yes | --dry-run)
acd rewrite-commits --apply <plan-id-or-file> (--yes | --dry-run)
~~~

For the end-to-end workflow and diagrams, see
[intent-commit-rewrite-flow.md](intent-commit-rewrite-flow.md).

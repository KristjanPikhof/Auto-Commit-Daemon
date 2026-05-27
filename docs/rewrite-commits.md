# `acd rewrite-commits` command contract (v1)

`acd rewrite-commits` is the interactive entrypoint for previewing and applying
an AI-generated commit-message rewrite plan. Use it for local branch cleanup
before sharing history.

## Quick start

Generate a plan, preview apply, then apply when ready:

~~~bash
# 1. Plan only — AI proposes messages; git history stays untouched.
ACD_COMMIT_STRATEGY=intent ACD_AI_PROVIDER=openai-compat ACD_AI_API_KEY=... \
  acd rewrite-commits --from 5 --plan-out rewrite.json --plan-only
# Ends with: Plan saved. Git history unchanged.
# Next: --show-plan, --apply-plan --dry-run, --apply-plan --yes

# 2. Dry-run apply — validate the saved plan without rewriting.
acd rewrite-commits --apply-plan rewrite.json --dry-run

# 3. Apply — rewrite commits after you accept the plan.
acd rewrite-commits --apply-plan rewrite.json --yes
~~~

Declining an interactive apply prompt prints `No rewrite performed.`

For prerequisites, recovery, Mermaid diagrams, and intent-mode detail, see
[intent-commit-rewrite-flow.md](intent-commit-rewrite-flow.md).

## Scope

- Current branch only.
- Linear commit ranges only.
- Merge commit rewrites are out of scope and must be refused.
- No daemon automation: the daemon does not initiate, approve, or apply rewrites.
- Plan generation requires working AI intent mode.
- Saved plan display/edit/apply can run without an AI provider because no new plan is
  generated.

## Plan-generation gate

Generating a new plan requires both:

1. `ACD_COMMIT_STRATEGY=intent` (after normal config resolution), and
2. a usable non-deterministic planner provider, currently:
   - `ACD_AI_PROVIDER=openai-compat` with `ACD_AI_API_KEY` set, or
   - `ACD_AI_PROVIDER=subprocess:<name>`.

The deterministic fallback planner is not sufficient for rewrite planning. If a
configured provider degrades to deterministic (for example, `openai-compat`
without an API key), `rewrite-commits` refuses plan generation.

## Grammar

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

Flags:

- `--from`: select from a commit-ish or 1-based position through `HEAD`.
- `--range`: 1-based position range to rewrite (`start-end`, where `1` is
  `HEAD`).
- `--last`: select the newest `n` commits.
- `--git-range`: advanced git rev-list revset; selected commits must be
  contiguous on the current branch.
- `--base`: deprecated alias for `--git-range <base>..<head>`; exclusive base
  revision.
- `--head`: inclusive head revision; defaults to `HEAD` when `--base` is set.
- `--plan-out`: destination for a generated plan.
- `--review`: open `$EDITOR` to review/edit proposed messages before the apply prompt.
- `--no-review`: skip the review prompt and leave proposed messages unchanged.
- `--plan-only`: generate/save/print the plan summary without any interactive prompts or apply. On success, prints `Plan saved. Git history unchanged.` plus a `Next:` block with `--show-plan`, `--apply-plan --dry-run`, and `--apply-plan --yes` using the saved plan id or `--plan-out` path.
- `--format`: editor format, `text` by default or `json` for automation-oriented edits.
- `--show-plan`: display an existing saved plan; bypasses the provider gate.
- `--edit`: load an existing saved plan by id or file path, open `$EDITOR` using
  `--format`, validate the edited content, and save a new edited plan revision
  when a saved plan id changes or write changed standalone file plans back to the
  same file. It never calls the AI provider. After editing,
  ACD prompts to apply the edited plan unless `--plan-only` suppresses apply,
  `--yes` applies immediately, or `--dry-run` validates/previews apply without rewriting.
- `--apply-plan`: apply an existing saved plan file; bypasses the plan-generation
  provider gate.
- `--apply`: apply an existing saved plan by plan id, or by file path when the
  argument names an existing file. This is compatible with `--apply-plan FILE`.
- `--dry-run`: validate/preview without rewriting commits.
- `--yes`: answer yes to the apply prompt and skip confirmation in noninteractive runs.

Examples:

~~~bash
ACD_COMMIT_STRATEGY=intent ACD_AI_PROVIDER=openai-compat ACD_AI_API_KEY=... \
  acd rewrite-commits --from 8f4c2a1 --plan-out rewrite.json
acd rewrite-commits --from 5 --plan-only
acd rewrite-commits --range 5-12 --review --format text
acd rewrite-commits --last 4 --no-review --yes
acd rewrite-commits --git-range main~12..main~4 --format json
acd rewrite-commits --show-plan rewrite.json
acd rewrite-commits --edit rewrite-plan-abc123 --format text --plan-only
acd rewrite-commits --edit rewrite-plan-abc123 --dry-run
acd rewrite-commits --apply-plan rewrite.json --dry-run
acd rewrite-commits --apply rewrite-plan-abc123 --yes
~~~

Backup recovery is part of the apply workflow: apply should create and print a
pre-apply backup ref or SHA before moving the current branch. If the rewrite is
wrong, recover with the printed backup target, for example:

~~~bash
git reset --hard refs/acd/rewrite-backups/<plan-id>
# or: git reset --hard <backup-sha>
~~~

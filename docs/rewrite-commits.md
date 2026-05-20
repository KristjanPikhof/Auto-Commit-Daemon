# `acd rewrite-commits` command contract (v1)

`acd rewrite-commits` is the interactive entrypoint for previewing a commit-history rewrite plan. In v1 the command contract is intentionally narrow so rewrite selection, plan storage, and git apply mechanics can be implemented by later lanes without changing user-facing flags.

## Scope

- Current branch only.
- Linear commit ranges only: `--range <base>..<head>` or `--base <base> [--head <head>]`.
- Merge commit rewrites are out of scope and must be refused by implementation lanes.
- No daemon automation: the daemon does not initiate, approve, or apply rewrites.
- Saved plan display/apply can run without an AI provider because no new plan is generated.

## Plan-generation gate

Generating a new plan requires both:

1. `ACD_COMMIT_STRATEGY=intent` (after normal config resolution), and
2. a usable non-deterministic planner provider, currently:
   - `ACD_AI_PROVIDER=openai-compat` with `ACD_AI_API_KEY` set, or
   - `ACD_AI_PROVIDER=subprocess:<name>`.

The deterministic fallback planner is not sufficient for rewrite planning. If a configured provider degrades to deterministic (for example, `openai-compat` without an API key), `rewrite-commits` refuses plan generation.

## Grammar

```text
acd rewrite-commits --range <base>..<head> [--plan-out FILE] [--dry-run]
acd rewrite-commits --base <base> [--head <head>] [--plan-out FILE] [--dry-run]
acd rewrite-commits --show-plan FILE
acd rewrite-commits --apply-plan FILE (--yes | --dry-run)
```

Flags:

- `--range`: linear range to consider, exclusive base and inclusive head.
- `--base`: exclusive base revision when not using `--range`.
- `--head`: inclusive head revision; defaults to `HEAD` when `--base` is set.
- `--plan-out`: destination for a generated plan.
- `--show-plan`: display an existing saved plan; bypasses the provider gate.
- `--apply-plan`: apply an existing saved plan; bypasses the provider gate.
- `--dry-run`: validate/preview without rewriting commits.
- `--yes`: skip confirmation for saved-plan apply.

Current implementation state: the CLI exposes and tests the contract/gates, but plan selection, plan persistence, and git rewrite application are intentionally left to dedicated implementation lanes.

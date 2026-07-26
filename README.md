# acd: Auto-Commit-Daemon

`acd` watches a Git worktree and turns file edits into local commits while you
keep working. It is built for AI coding tools such as Claude Code, Codex,
Cursor, OpenCode, and Pi, but the daemon itself is just a static Go binary.

## How ACD turns AI edits into clean commits

ACD saves each edit as it happens, then decides which changes belong in the
same commit.

~~~mermaid
flowchart TB
  Work(["Your coding agent<br/>edits files"])
  Capture["ACD saves every edit as it happens<br/>Your work is safe before planning starts"]
  Strategy{"How should these edits<br/>be committed?"}

  Event["Event mode - default<br/>Each saved edit gets its own commit"]
  Intent["Intent mode<br/>Related edits become logical commits<br/>10 saved edits might become 3 commits"]

  Replay["ACD builds each commit safely<br/>using a temporary Git index"]
  Safe{"Is the branch still<br/>safe to update?"}

  History(["Your branch gets normal Git commits<br/>ready to review or undo"])
  Heal["ACD checks all pending work<br/>It finds what landed and saves the rest"]
  Preserve["ACD cannot verify the result<br/>It keeps the captured work and leaves your files alone"]

  Work --> Capture --> Strategy
  Strategy -->|Keep every edit separate| Event
  Strategy -->|Group edits by task| Intent
  Event --> Replay
  Intent --> Replay
  Replay --> Safe
  Safe -->|Yes| History
  Safe -->|No| Heal
  Heal -.->|Continue when safe| Capture
  Heal -->|Still uncertain| Preserve

  classDef external fill:#1e3a5f,stroke:#60a5fa,color:#f8fafc,stroke-width:2px
  classDef process fill:#164e3b,stroke:#34d399,color:#ecfdf5,stroke-width:2px
  classDef decision fill:#78350f,stroke:#fbbf24,color:#fffbeb,stroke-width:2px
  classDef intent fill:#4c1d95,stroke:#c084fc,color:#faf5ff,stroke-width:3px
  classDef guard fill:#3f3f46,stroke:#a1a1aa,color:#fafafa,stroke-width:2px

  class Work external
  class Capture,Replay,History,Heal process
  class Strategy,Safe decision
  class Intent intent
  class Event,Preserve guard
~~~

## Install

Building from source or using `go install` requires Go 1.26.5 or newer.

~~~bash
brew install KristjanPikhof/tap/acd
~~~

Other options:

~~~bash
curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh

go install github.com/KristjanPikhof/Auto-Commit-Daemon/cmd/acd@latest
~~~

## Set up your AI tool

Run the setup command for the tool you use, then paste the printed snippet into
the target config file. Do not redirect with `>` when you already have custom
hooks, because that replaces the whole file.

~~~bash
acd setup claude-code
acd setup codex
acd setup cursor
acd setup opencode
acd setup pi
acd setup shell
~~~

| Tool | Config file | Notes |
|---|---|---|
| Claude Code | `~/.claude/settings.json` | Native hook support. |
| Codex | `~/.codex/hooks.json` | Run `/hooks` after changing the file so Codex re-approves it. |
| Cursor | `~/.cursor/hooks.json` | User-global hooks only. Approve in Settings -> Hooks. |
| OpenCode | `~/.config/opencode/hook/hooks.yaml` | Uses the [OpenCode-Hooks](https://github.com/KristjanPikhof/OpenCode-Hooks) adapter. |
| Pi | `~/.pi/agent/hook/hooks.yaml` | Uses the [Pi-YAML-Hooks](https://github.com/KristjanPikhof/Pi-YAML-Hooks) adapter. |
| Shell | `.envrc` or shell rc | Fallback when no native harness exists. |

Fresh Cursor install with no existing hooks:

~~~bash
mkdir -p ~/.cursor
acd setup cursor --raw > ~/.cursor/hooks.json
~~~

If `~/.codex/config.toml` pins feature flags, keep hooks enabled:

~~~toml
[features]
hooks = true
~~~

After setup:

~~~bash
acd doctor
~~~

`doctor` checks that the installed snippet still matches the current template.

## Configure commit behavior

Use the guided setup for a regular repository:

~~~bash
acd configure
~~~

The wizard recommends Intent Balanced. It previews the exact provider, diff
context, verification command, and automatic repair policy before changing
anything. Apply creates one immutable runtime revision and enables ACD. It does
not edit hook files, so run the reported `acd setup <harness>` command
separately when needed.

Preview the default or preselect a mode without making calls or writes:

~~~bash
acd configure --dry-run
acd configure --strategy intent --preset balanced
acd configure --accessible
~~~

Use `acd settings` after onboarding for profiles, experiments, and advanced
field overrides. See the [settings guide](docs/settings.md) for authoring
scopes, preset customization, testing, and activation.

| Strategy | What happens | Best for |
|---|---|---|
| `event` | One captured edit becomes one commit. New Event repositories use Fast unless another preset is selected. | Offline use, CI, shared branches, strict traceability. |
| `intent` | Durable semantic candidates group related captures, validate atomicity, and publish in dependency order. | Local work where reviewable, reversible commits matter more than one-edit history. |

Fast offline setup:

~~~bash
acd configure --strategy event --preset fast
~~~

Everyday semantic commits:

~~~bash
acd configure --strategy intent --preset balanced
~~~

All regular Intent presets use redacted captured diffs. A network provider
needs explicit diff-egress approval. A local subprocess may receive the same
bounded context without network egress. Metadata-only Intent v2 is not a
supported regular configuration.

Intent candidates survive planner windows. Hard dependencies keep same-path,
rename, create/modify, and before/after chains ordered. Soft evidence connects
likely code, tests, documentation, configuration, and migrations. An atomicity
gate then checks cohesion, completeness, separation, dependencies,
materialization, verification, and revertibility before publication.

Fallback depends on the preset. Fast publishes the smallest materializable
hard-dependency component. Balanced reuses a valid or deterministic dependency
partition and runs the approved fast verification. Quality keeps the candidate
pending and reports `needs_attention`. No preset bypasses hard dependencies,
scratch materialization, or required verification.

Balanced and Quality may repair a small, recent ACD-only commit chain when a
late companion capture arrives. Repair runs only while the commits remain
private to the current HEAD chain and all safety and verification checks pass.
Otherwise ACD records the skip and publishes a new commit.

Message format:

| Format | Example subject | Notes |
|---|---|---|
| `imperative` | `Add commit format selection` | Default. Subjects start with an imperative verb. |
| `conventional` | `feat: add commit format selection` | Optional scope-less Conventional Commit style. |

~~~bash
export ACD_COMMIT_FORMAT=conventional
~~~

Conventional mode accepts only `feat`, `fix`, `docs`, `refactor`, `test`,
`build`, `ci`, `chore`, `perf`, `style`, and `revert`. Scopes and breaking
markers are not supported. Body bullets keep the same `- ` prefix and wrapping
rules as the default format.

## Daily commands

After setup, leave ACD running in the background. Use the three short commands
below for normal health and lifecycle control; the hook protocol stays
automatic.

| Need | Command |
|---|---|
| Check this repo and get one recommended next action | `acd` |
| Enable ACD and ensure the daemon is running | `acd on` |
| Disable ACD and stop the daemon without deleting state | `acd off` |
| Watch enabled repos | `acd list` |
| Show this repo state | `acd status` |
| Follow capture, group, publish, and block decisions | `acd events --watch` |
| Ask why a path behaved a certain way | `acd explain --path FILE` |
| Ask what ACD did for a commit | `acd explain --commit HEAD` |
| Tail the daemon log | `acd logs --follow` |
| Create a support bundle | `acd doctor --bundle` |

Bare `acd` is read-only. `acd on` and `acd off` are idempotent, work from a
subdirectory, and preserve `.git/acd/state.db`. Harness integrations continue
to use the lower-level `start`, `wake`, `touch`, `flush`, and `stop` commands.
See the [command reference](docs/commands.md) for every public command and its
read or write behavior.

## When commits stop

ACD first tries to heal the queue itself. It reconciles the complete unpublished
chain for one branch generation, never an isolated blocker:

- If stable `HEAD` already has the chain's exact final touched-path state, ACD
  marks the chain published and keeps a hidden proof ref.
- Otherwise ACD writes the reconstructed tree to a hidden recovery ref, marks
  the chain recovered, reseeds from `HEAD`, and captures the still-dirty
  worktree again.
- If objects are missing, the branch changes during proof, or a recovery ref
  collides, ACD leaves the queue, live `HEAD`, index, and worktree unchanged.
  A hidden evidence ref may remain so a safe retry can reuse the same tree.

Start with the short read-only path:

~~~bash
acd
acd diagnose
acd events --watch
~~~

Use `fix` when you want to preview or run the same recovery manually. Preview
first, turn ACD off so hooks cannot restart the daemon during repair, then turn
it on again:

~~~bash
acd fix --dry-run
acd off
acd fix --yes
acd on
acd
~~~

`--force` means archive-only recovery. It does not purge, retarget, or discard
captured events:

~~~bash
acd fix --force --dry-run
acd off
acd fix --force --yes
acd on
~~~

`acd fix` creates a SQLite-consistent backup before it mutates state and refuses
to run while a live daemon owns the database. If the problem is only a manual
pause marker, run:

~~~bash
acd resume --yes
~~~

To inspect or restore an archived chain, use the `/archive` ref printed by
`acd events`; see [Inspect or restore archived work](docs/user-workflows.md#inspect-or-restore-archived-work).

If `acd diagnose --json` reports generated pending deletes under a tracked
cache directory such as `.derivedData-provider-core`, `acd fix --yes` cleans
only ACD's queue. Record the Git cleanup separately after review:

~~~bash
git status -- .derivedData-provider-core
git add -u -- .derivedData-provider-core
git commit -m "Remove tracked generated cache files"
~~~

## Dirty worktree after the daemon was off

Use `commit-all` when files changed while no daemon was running:

~~~bash
acd commit-all --dry-run
acd commit-all --yes
acd commit-all --yes --json
~~~

Detached HEAD, in-progress Git operations, and manual pause markers are refused
for previews and apply. If an authorized run reaches its mutation phase, it
also refuses while the per-repo daemon is alive. Dry-run, a declined
confirmation, and a clean no-op do not acquire `daemon.lock`; the first two are
read-only and do not capture files, start the AI provider, create recovery refs,
or write ACD state. An incomplete drain exits non-zero and leaves the captured
queue protected for diagnosis.

## Repo registration

Most repos need no manual setup. Harness hooks call `acd start`, which creates
`<gitDir>/acd/state.db` and registers the repo.

For normal use, prefer `acd on` and `acd off`. Use the explicit lifecycle
commands below for bulk administration, registry cleanup, or when global
autodiscovery is disabled:

~~~bash
acd repo init
acd repo disable --repo /path/to/repo
acd repo enable --repo /path/to/repo
acd repo manage
acd list --interactive
acd repo list
acd repo remove --dry-run
acd repo remove --yes
acd repo remove --yes --purge-state
~~~

`acd repo disable` stops a live repo daemon, clears start caches, preserves
`.git/acd/state.db`, and records disabled lifecycle state in the central
registry. Hook-driven `start`, `wake`, `touch`, and `flush` calls then skip
cleanly with `repo_disabled`; manual `acd start` tells you to run
`acd repo enable --repo <path>`. Normal `acd list` snapshots hide disabled
repos; use `acd repo list`, `acd repo manage`, or `acd list --interactive` to
inspect and re-enable them. `acd repo enable` only clears that disabled state;
it does not start the daemon. Use `repo remove` when you want to unregister a
row instead of temporarily disabling it.

`acd repo manage` and `acd list --interactive` open the same line-oriented
manager. Compact mode is the default; `--verbose` starts with state DB, last
seen, harness, and status details. Inside the manager: `t N` toggles a repo,
`e N` enables, `d N` disables, `r` refreshes, `v` switches compact/verbose,
and `q` exits.

Disable autodiscovery in `~/.config/acd/config.json`:

~~~json
{
  "repo_lifecycle": {
    "autodiscovery": false
  }
}
~~~

Override it for one process:

~~~bash
ACD_REPO_AUTODISCOVERY=disabled acd start
ACD_REPO_AUTODISCOVERY=enabled acd start
~~~

## Rewrite commit messages

The daemon never rewrites history on its own. Use `rewrite-commits` only for an
explicit local cleanup before sharing a branch:

~~~bash
acd rewrite-commits --from-nr 5 --plan-out rewrite.json --plan-only
acd rewrite-commits --show-plan rewrite.json
acd off
acd rewrite-commits --apply-plan rewrite.json --dry-run
acd rewrite-commits --apply-plan rewrite.json --yes
acd on
~~~

Use `--from-sha <sha>` when you want a commit-ish selector, `--range-nr 5-12`
for positions, or `--range-sha base..head` for a simple git range. Progress goes
to stderr; stdout remains safe for command output and `--json`. Use
`--progress json` for JSONL progress events or `--progress off` to disable them.

If apply prints a backup ref or SHA and review fails:

~~~bash
git reset --hard <backup-ref-or-sha>
~~~

## Environment you will actually touch

| Variable | Default | Use |
|---|---:|---|
| `ACD_COMMIT_STRATEGY` | `event` | `event` for one capture per commit, `intent` for AI grouping. |
| `ACD_COMMIT_PRESET` | strategy default | `fast`, `balanced`, or `quality`. Intent defaults to Balanced; Event defaults to Fast. |
| `ACD_COMMIT_FORMAT` | `imperative` | `imperative` keeps the current subject rules; `conventional` opts into scope-less Conventional Commit subjects. |
| `ACD_AI_PROVIDER` | `deterministic` | `deterministic`, `openai-compat`, or `subprocess:<name>`. |
| `ACD_AI_API_KEY` | unset | Overrides the protected credential file for `openai-compat`. |
| `ACD_AI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible endpoint. |
| `ACD_AI_MODEL` | `gpt-5.4-mini` | Model passed to the provider. |
| `ACD_AI_DIFF_EGRESS` | off | Truthy sends redacted captured diffs to providers that ask for diffs. |
| `ACD_INTENT_WINDOW` | `10` | Max captures offered to one planner pass. |
| `ACD_INTENT_MIN_PENDING` | `10` | Preferred count before planning. Lower it for sparse repos. |
| `ACD_INTENT_SETTLE_WINDOW` | `10s` | Burst settle delay after the count gate. `0` disables it. |
| `ACD_INTENT_MAX_PENDING_AGE` | `5m` | Age escape hatch for sparse queues. |
| `ACD_INTENT_DEFER_LIMIT` | `1` | Deferrals before ACD forces a one-capture window. |
| `ACD_INTENT_RETRY_ON_INVALID` | `2` | Max correction retries after invalid planner output. |
| `ACD_INTENT_VERIFICATION` | `none` | `none`, `fast`, or `full`; presets provide the regular defaults. |
| `ACD_VERIFICATION_FAST_COMMAND` | unset | Approved repository command for Balanced verification. |
| `ACD_VERIFICATION_FULL_COMMAND` | unset | Approved repository command for Quality verification. |
| `ACD_INTENT_REPAIR_ENABLED` | off | Enables bounded repair of eligible recent ACD commits. |
| `ACD_INTENT_REPAIR_HORIZON` | `10m` | Maximum age of the repair chain. |
| `ACD_INTENT_REPAIR_MAX_COMMITS` | `3` | Maximum repair chain, capped at five. |
| `ACD_SAFE_IGNORE` | enabled | Set false-like value to stop pruning generated trees. |
| `ACD_SAFE_IGNORE_EXTRA` | unset | Extra generated trees, such as `dist/,build/`. |
| `ACD_SENSITIVE_GLOBS` | built in | Non-empty values replace the protected path globs. Unset or empty uses the defaults. |
| `ACD_TRACE` | off | Writes daemon decision summaries under `<gitDir>/acd/trace/`. |
| `ACD_AI_PROMPT_TRACE` | off | Writes local AI request diagnostics. Treat as sensitive. |

Restart a running daemon after changing daemon runtime environment.

## Docs

| Doc | Use it for |
|---|---|
| [docs/overview.md](docs/overview.md) | A short system map. |
| [docs/commands.md](docs/commands.md) | Every public command, its side effects, and safe examples. |
| [docs/user-workflows.md](docs/user-workflows.md) | Daily status, recovery, support bundles, and `commit-all`. |
| [docs/capture-replay.md](docs/capture-replay.md) | Storage, replay, branch safety, blockers, and trace classes. |
| [docs/intent-commit-flow.md](docs/intent-commit-flow.md) | Intent grouping behavior and planner observability. |
| [docs/intent-v2-migration.md](docs/intent-v2-migration.md) | Upgrade rules and remediation for existing repositories. |
| [docs/intent-commit-rewrite-flow.md](docs/intent-commit-rewrite-flow.md) | Safe history rewrite workflow. |
| [docs/rewrite-commits.md](docs/rewrite-commits.md) | `rewrite-commits` command grammar. |
| [docs/ai-providers.md](docs/ai-providers.md) | Provider setup, diff privacy, prompt tracing, and plugin protocol. |
| [docs/multi-tool.md](docs/multi-tool.md) | Running ACD next to another auto-committer. |

## License

MIT. See [LICENSE](LICENSE).

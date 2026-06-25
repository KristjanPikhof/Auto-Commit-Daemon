# acd: Auto-Commit-Daemon

`acd` watches a Git worktree and turns file edits into local commits while you
keep working. It is built for AI coding tools such as Claude Code, Codex,
Cursor, OpenCode, and Pi, but the daemon itself is just a static Go binary.

The flow is: your AI tool tells `acd` that work happened, the daemon records a
durable capture, then replay turns that capture into a normal Git commit.

~~~mermaid
flowchart TB
  Hook["AI tool hook<br/>Claude Code, Codex, Cursor, OpenCode, Pi"]

  subgraph Capture["1. Notice and capture edits"]
    CLI["acd start / wake / flush<br/>refreshes the repo session"]
    Daemon["acd daemon<br/>watches files and records events"]
  end

  subgraph State["2. Keep the work durable"]
    DB[("state.db<br/>event ledger")]
    Blobs[("git blobs<br/>captured file content")]
  end

  subgraph Replay["3. Publish a safe commit"]
    Scratch["scratch index replay<br/>rebuilds the intended tree"]
    Commit["git commit + update-ref<br/>moves the branch only after checks pass"]
  end

  Hook --> CLI --> Daemon --> DB
  Daemon --> Blobs
  DB --> Scratch
  Blobs --> Scratch
  Scratch --> Commit

  classDef external fill:#233142,stroke:#7aa2f7,color:#e6edf3
  classDef capture fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef durable fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef publish fill:#332b46,stroke:#bb9af7,color:#f4edff
  class Hook external
  class CLI,Daemon capture
  class DB,Blobs durable
  class Scratch,Commit publish
~~~

## Install

Building from source or using `go install` requires Go 1.26.4 or newer.

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
| OpenCode | `~/.config/opencode/hook/hooks.yaml` | Uses the OpenCode-Hooks adapter. |
| Pi | `~/.pi/agent/hook/hooks.yaml` | Uses the Pi-YAML-Hooks adapter. |
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

## Choose commit behavior

| Strategy | What happens | Best for |
|---|---|---|
| `event` | One captured edit becomes one commit. This is the default. | Offline use, CI, shared branches, strict traceability. |
| `intent` | An AI planner groups related captures before replay writes a commit. | Local work where reviewable commits matter more than one-edit history. |

Offline default:

~~~bash
export ACD_AI_PROVIDER=deterministic
export ACD_COMMIT_STRATEGY=event
~~~

AI grouping:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=$YOUR_API_KEY
export ACD_AI_BASE_URL=https://your-endpoint/v1
export ACD_AI_MODEL=gpt-5.4-mini
export ACD_AI_DIFF_EGRESS=1

export ACD_COMMIT_STRATEGY=intent
export ACD_INTENT_WINDOW=10
export ACD_INTENT_MIN_PENDING=4
export ACD_INTENT_SETTLE_WINDOW=10s
export ACD_INTENT_MAX_PENDING_AGE=5m
export ACD_INTENT_DEFER_LIMIT=1
~~~

`ACD_AI_DIFF_EGRESS=1` lets the planner see redacted captured diffs. Leave it
unset when the endpoint should receive metadata only.

Intent mode waits briefly after the pending-count gate before planning. This
settle window lets a burst of related edits reach one planner-visible window,
while `acd flush --logical` still drains the current visible batch from an
active harness session.

When one visible window contains separate intents, the planner prompt asks for
ordered `commit_groups` so replay can publish atomic commits instead of forcing
the whole window into one commit.

Message format:

| Format | Example subject | Notes |
|---|---|---|
| `imperative` | `Add commit format selection` | Default and recommended. Existing behavior is unchanged. |
| `conventional` | `feat: add commit format selection` | Optional scope-less Conventional Commit style. |

~~~bash
export ACD_COMMIT_FORMAT=conventional
~~~

Conventional mode accepts only `feat`, `fix`, `docs`, `refactor`, `test`,
`build`, `ci`, `chore`, `perf`, `style`, and `revert`. Scopes and breaking
markers are not supported. Body bullets keep the same `- ` prefix and wrapping
rules as the default format.

## Daily commands

| Need | Command |
|---|---|
| Start or refresh the current repo daemon | `acd start` |
| Watch all registered repos | `acd list` |
| Show this repo state | `acd status` |
| Follow capture, group, publish, and block decisions | `acd events --watch` |
| Ask why a path behaved a certain way | `acd explain --path FILE` |
| Ask what ACD did for a commit | `acd explain --commit HEAD` |
| Flush the current visible intent batch from an active harness session | `acd flush --session-id "$ACD_SESSION_ID" --logical` |
| Nudge capture and replay without bypassing intent batch gates | `acd wake --session-id "$ACD_SESSION_ID"` |
| Stop this repo daemon | `acd stop` |
| Stop every registered daemon | `acd stop --all` |
| Tail the daemon log | `acd logs --follow` |
| Create a support bundle | `acd doctor --bundle` |

`acd start` resolves your current directory to the canonical Git worktree root,
so calling it from a subdirectory refreshes the same daemon.

## When commits stop

Use the same ladder every time:

~~~bash
acd status
acd events --watch
acd explain --path path/to/file
acd diagnose --json
acd fix --dry-run
acd fix --yes
acd status
~~~

Only use the force path after the dry-run shows terminal barriers with pending
successors and you have checked that the blocked changes are already in `HEAD`
or should be discarded:

~~~bash
acd fix --force --dry-run
acd fix --force --yes
~~~

`acd fix` backs up `state.db` before it mutates state and refuses to run while a
live daemon owns the database. If the problem is only a manual pause marker,
run:

~~~bash
acd resume --yes
~~~

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

It refuses on detached HEAD, in-progress Git operations, manual pause markers,
or while the per-repo daemon is alive.

## Repo registration

Most repos need no manual setup. Harness hooks call `acd start`, which creates
`<gitDir>/acd/state.db` and registers the repo.

Use explicit lifecycle commands when autodiscovery is disabled or when old rows
need cleanup, or when you want to keep global autodiscovery on but exclude one
repo:

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
acd rewrite-commits --apply-plan rewrite.json --dry-run
acd rewrite-commits --apply-plan rewrite.json --yes
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
| `ACD_COMMIT_FORMAT` | `imperative` | `imperative` keeps the current subject rules; `conventional` opts into scope-less Conventional Commit subjects. |
| `ACD_AI_PROVIDER` | `deterministic` | `deterministic`, `openai-compat`, or `subprocess:<name>`. |
| `ACD_AI_API_KEY` | unset | Required by `openai-compat`. |
| `ACD_AI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible endpoint. |
| `ACD_AI_MODEL` | `gpt-5.4-mini` | Model passed to the provider. |
| `ACD_AI_DIFF_EGRESS` | off | Truthy sends redacted captured diffs to providers that ask for diffs. |
| `ACD_INTENT_WINDOW` | `10` | Max captures offered to one planner pass. |
| `ACD_INTENT_MIN_PENDING` | `10` | Preferred count before planning. Lower it for sparse repos. |
| `ACD_INTENT_SETTLE_WINDOW` | `10s` | Burst settle delay after the count gate. `0` disables it. |
| `ACD_INTENT_MAX_PENDING_AGE` | `5m` | Age escape hatch for sparse queues. |
| `ACD_INTENT_DEFER_LIMIT` | `1` | Deferrals before ACD forces a one-capture window. |
| `ACD_INTENT_RETRY_ON_INVALID` | `2` | Max correction retries after invalid planner output. |
| `ACD_SAFE_IGNORE` | enabled | Set false-like value to stop pruning generated trees. |
| `ACD_SAFE_IGNORE_EXTRA` | unset | Extra generated trees, such as `dist/,build/`. |
| `ACD_SENSITIVE_GLOBS` | built in | Extra sensitive path globs. Empty keeps defaults. |
| `ACD_TRACE` | off | Writes daemon decision summaries under `<gitDir>/acd/trace/`. |
| `ACD_AI_PROMPT_TRACE` | off | Writes local AI request diagnostics. Treat as sensitive. |

Restart a running daemon after changing daemon runtime environment.

## Docs

| Doc | Use it for |
|---|---|
| [docs/overview.md](docs/overview.md) | A short system map. |
| [docs/user-workflows.md](docs/user-workflows.md) | Daily status, recovery, support bundles, and `commit-all`. |
| [docs/capture-replay.md](docs/capture-replay.md) | Storage, replay, branch safety, blockers, and trace classes. |
| [docs/intent-commit-flow.md](docs/intent-commit-flow.md) | Intent grouping behavior and planner observability. |
| [docs/intent-commit-rewrite-flow.md](docs/intent-commit-rewrite-flow.md) | Safe history rewrite workflow. |
| [docs/rewrite-commits.md](docs/rewrite-commits.md) | `rewrite-commits` command grammar. |
| [docs/ai-providers.md](docs/ai-providers.md) | Provider setup, diff privacy, prompt tracing, and plugin protocol. |
| [docs/multi-tool.md](docs/multi-tool.md) | Running ACD next to another auto-committer. |

## License

MIT. See [LICENSE](LICENSE).

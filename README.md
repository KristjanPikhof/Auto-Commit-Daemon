# acd — Auto-Commit-Daemon

One static binary. Watches your git worktree. Captures meaningful changes, then
publishes them as chronological commits. By default each captured event becomes
one commit; with `ACD_COMMIT_STRATEGY=intent`, AI can group related pending
captures into one reviewable commit.

## Install

~~~bash
curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh
~~~

Or:

~~~bash
brew tap KristjanPikhof/tap
brew install acd
~~~

Or:

~~~bash
go install github.com/KristjanPikhof/Auto-Commit-Daemon/cmd/acd@latest
~~~

## Wire your harness

~~~bash
acd setup claude-code   # paste output into ~/.claude/settings.json
acd setup codex         # paste output into ~/.codex/hooks.json
acd setup opencode      # paste output into ~/.config/opencode/hook/hooks.yaml
acd setup pi            # paste output into ~/.pi/agent/hook/hooks.yaml
acd setup shell         # universal direnv / zshrc fallback
~~~

Codex also needs lifecycle hooks enabled in `~/.codex/config.toml`:

~~~toml
[features]
codex_hooks = true
~~~

> **Overwrite warning:** each `acd setup` command prints a snippet to stdout
> for you to merge into the target config. Redirecting with `>` (e.g. `acd
> setup codex --raw > ~/.codex/hooks.json`) replaces the entire file and will
> destroy any custom (non-acd) hooks or settings you already have there. Back
> up the file first if you have made manual edits, then merge the acd block
> in by hand. See the per-harness README for details:
> [claude-code](templates/claude-code/README.md),
> [codex](templates/codex/README.md),
> [opencode](templates/opencode/README.md),
> [pi](templates/pi/README.md).

### Supported harnesses

| Harness | Hook support | Install location | Source |
|---|---|---|---|
| `claude-code` | Native hooks (default, no extra engine required) | `~/.claude/settings.json` — managed block via `acd setup claude-code` | [Anthropic Claude Code](https://docs.claude.com/en/docs/claude-code/hooks) |
| `codex` | Native hooks (default, no extra engine required) | `~/.codex/hooks.json` plus `[features].codex_hooks = true` in `~/.codex/config.toml`. Run `/hooks` inside Codex after every install to approve entries — Codex re-flags all hooks as review-required after any `hooks.json` change. See [templates/codex/README.md](templates/codex/README.md). | OpenAI Codex |
| `opencode` | External engine: [OpenCode-Hooks](https://github.com/KristjanPikhof/OpenCode-Hooks) (default) | `~/.config/opencode/hook/hooks.yaml` | [KristjanPikhof/OpenCode-Hooks](https://github.com/KristjanPikhof/OpenCode-Hooks) |
| `pi` | External engine: [Pi-YAML-Hooks](https://github.com/KristjanPikhof/Pi-YAML-Hooks) (default) | `~/.pi/agent/hook/hooks.yaml` | [KristjanPikhof/Pi-YAML-Hooks](https://github.com/KristjanPikhof/Pi-YAML-Hooks) |
| `shell` | Plain shell — `direnv` `.envrc` or `~/.zshrc` / `~/.bashrc` fallback | `~/.zshrc` / `~/.bashrc` + direnv snippet | n/a (no harness required) |

ACD supports Claude Code and Codex via their native hook systems by default. OpenCode and Pi are supported via the external hook engines [OpenCode-Hooks](https://github.com/KristjanPikhof/OpenCode-Hooks) and [Pi-YAML-Hooks](https://github.com/KristjanPikhof/Pi-YAML-Hooks) respectively; install the engine first, then run the matching `acd setup …` to wire `acd` into it.

Run `acd doctor` to check whether your installed snippet is current. It warns when active hooks are missing `acd start` or `acd wake`, and shows the remediation command per harness.

**Hook file detected but `acd doctor` still reports detection as `no`?** The hook
file is missing the `# acd-managed: true` marker on its first line, so `acd
doctor` cannot recognise it as an acd-managed file. To fix this, either:

- **Prepend the marker manually** — open the hook file in an editor and add
  `# acd-managed: true` as the very first line.
- **Re-run setup and merge** — run `acd setup <harness>` (no `>`), copy the
  printed snippet, and merge the acd block into your existing hook file.

  **Do not use `>` to redirect** when you have custom hooks — redirecting with
  `>` overwrites the entire file and destroys any existing entries.

## Use it

Open your harness. Edit files. Commits land automatically.

~~~bash
acd start               # start or refresh the current repo daemon
acd list                # daemons running across all your repos
                        # columns: REPO  DAEMON  CLIENTS  PENDING  BLOCKED  LAST_COMMIT  STATUS
acd list --watch        # refresh the repo table until Ctrl-C
acd list --watch --interval 5s
acd status              # current repo daemon, queue, pause, and recent decisions
acd status --watch      # refresh the same repo until Ctrl-C
acd events              # show the durable product decision ledger
acd events --watch      # stream decisions appended after watch starts
acd explain --path FILE # explain why a path was captured, skipped, or blocked
acd explain --commit HEAD # explain decisions linked to a commit
acd prompt              # inspect the last opt-in AI prompt trace
acd prompt --seq 42 --json # inspect an event or offered intent seq as JSON
acd fix --dry-run       # plan safe remediation for a stuck repo
acd fix --yes           # apply the safe plan after reading it
acd fix --force --yes   # also purge blocked barriers with pending successors
acd logs                # tail the current repo daemon log as raw JSONL
acd logs --lines 200    # choose the initial tail length
acd logs --follow       # stream appended raw JSONL lines until Ctrl-C
acd stats --since 7d    # last week's commits
acd doctor              # health/support diagnostics, including queue blockers
acd doctor --bundle     # write a diagnostics zip for issue reports
acd diagnose            # read-only branch anchor + blocked_conflict report
acd pause --reason "resetting branch" --yes   # durable manual replay pause
acd resume --yes          # remove the manual pause marker
acd wake --session-id X # heartbeat refresh + nudge daemon for low-latency replay
acd flush --session-id X --logical # heartbeat + drain pending captures NOW (intent gate bypassed)
acd flush --session-id X           # heartbeat-only (alias for acd touch)
acd gc                  # prune stale central-registry entries
acd stop                # graceful stop for the current repo daemon
acd stop --session-id X # harness/refcount stop; exits only when no peers remain
acd stop --all          # stop every daemon
acd commit-all          # one-shot: commit every uncommitted file (daemon must be off)
acd commit-all --dry-run           # plan and show summary; no commits written
acd commit-all --yes               # skip interactive confirmation
acd commit-all --yes --json        # machine-readable JSON output
acd commit-all --repo /path/to/repo --yes   # target a repo other than $PWD
~~~

Use the no-flag lifecycle commands when you are driving ACD from a terminal.
Manual `acd start` resolves the current directory to its canonical Git worktree
root, registers that root in the central registry, and creates a stable human
client for it. Starting from a subdirectory of the same worktree refreshes the
same root entry instead of creating a second daemon or registry row. Manual
`acd stop` stops the current repo daemon directly.

ACD refuses lifecycle and lookup commands outside a Git worktree. If `--repo` or
`$PWD` is not inside a worktree, `start`, `status`, and `diagnose` fail with a
clear non-Git error instead of registering an arbitrary directory.

Harness integrations should keep passing `--session-id` (and usually
`--harness`). That path is refcount-aware: `acd stop --session-id X` removes one
client and stops the shared daemon only after the final harness client exits.

| Situation | Command |
|---|---|
| Start or refresh ACD while working in a repo | `acd start` |
| Stop the daemon for the repo you are in | `acd stop` |
| Stop one harness session and respect peer sessions | `acd stop --session-id X` |
| Stop every registered repo daemon | `acd stop --all` |

If commits stop appearing, see [docs/capture-replay.md](docs/capture-replay.md)
for a step-by-step troubleshooting checklist.

For daily "what happened?" questions, start with
[docs/user-workflows.md](docs/user-workflows.md). The usual loop is
`acd status`, `acd events`, and `acd explain --path FILE`; use
`acd fix --dry-run` before applying any safe remediation. `acd logs` reads the
daemon's per-repo JSONL log directly. It does not pretty print, summarize, or
sanitize the stream, and it does not include full AI prompt traces. Use
`acd prompt` for opt-in prompt-trace inspection, and use `acd doctor --bundle`
when you want bundled diagnostics with tail snippets and safe metadata for
reports.

See [docs/capture-replay.md#revert-workflows](docs/capture-replay.md#revert-workflows)
for how `acd` handles `git revert`, `git reset --soft/--mixed/--hard`, and
interactive rebase while the daemon is running, including the rewind grace window
that pauses both capture and replay automatically.

See [docs/multi-tool.md](docs/multi-tool.md) for guidance on running `acd`
alongside another auto-committer such as the Claude Code Automatic Atomic
Commits hook or the Codex ACD hook. The short version: if an external tool
lands a commit before `acd`'s replay tick, the daemon detects the match and
settles the queued event as `published` with no duplicate commit. Real content
mismatches still produce `blocked_conflict`.

## Workflows and recovery

Use the daily workflow commands first:

~~~bash
acd status
acd events --watch
acd explain --path path/to/file
acd explain --commit HEAD
acd fix --dry-run
~~~

`status` resolves `--repo` or `$PWD` the same way `start` does: subdirectories
look up the canonical worktree root that was registered by `acd start`. It shows
daemon health, queue counts, pause state, failed terminal barriers, recent
decision counts, and the active commit strategy. `status --json` includes the
decision cursor plus recent decision records, and includes
`failed_events` / `failed_blocking_pending` when failed rows are holding pending
replay behind a terminal barrier. With intent grouping enabled, it also reports
planner deferrals, forced-aging readiness, and the latest planner error.
`events --watch` starts at the current decision-ledger tail unless `--since` is
provided, so it prints only decisions appended after watch starts. `explain`
answers why ACD captured, skipped, committed, deferred, grouped, treated work as
externally handled, or blocked a path or commit. `fix --dry-run` plans
conservative cleanup for common stuck states; apply only after reading the plan:

~~~bash
acd fix --yes
~~~

Use `acd diagnose` when replay stalls or `fix` needs more context:

~~~bash
acd diagnose --repo .
acd diagnose --repo . --json
~~~

It reports the current git `HEAD` branch, the daemon's persisted branch anchor,
blocked-conflict counts by `error_class`, failed terminal barriers, the five
most recent blocked or failed events, and the most recent dead-branch prune
(`dead_branch_prune_last_run_ts`, `dead_branch_prune_last_count`,
`dead_branch_prune_last_refs`). ACD prunes stale `pending`,
`blocked_conflict`, and `failed` rows for deleted branch refs, so phantom
blocked counts no longer linger after a merged feature branch is removed.
Paused repos are left untouched. Set `ACD_KEEP_DEAD_BRANCH_BARRIERS=1` to keep
dead-branch rows for forensic inspection. The two int fields render as `0` when
the daemon has never recorded a non-empty prune; the refs slice is omitted from
JSON when empty. If the daemon is stopped and `fix` reports a safe plan, apply
it:

~~~bash
acd fix --yes
~~~

`fix --yes` backs up `state.db` before any mutation and refuses to run while a
live daemon owns the database. It resolves already-landed barriers, retargets
stale anchors, clears obsolete terminal barriers, marks externally-published
rows, clears expired manual pauses, and clears drained backpressure. Add
`--force` to also plan (and with `--yes`, apply) the purge of blocked barriers
that still have pending successors. A manual pause marker is preserved unless
you pass `--clear-pause`; use `acd resume --yes` when the marker itself is the
only problem.

ACD uses an isolated scratch index for replay correctness, then performs a
guarded path-scoped live-index reconciliation so IDEs see the committed state
for ACD-owned paths. It will not run broad `git reset`, `git checkout`, or
`git read-tree` against your live index, and it skips same-path staged work
that no longer matches the captured before-state.

### Cold start: committing a dirty worktree

Use `acd commit-all` when the daemon was off and files accumulated without
being committed. It captures all uncommitted changes, sorts them by path for
coherent sibling clustering, replays them with the configured strategy, and
exits without starting the persistent daemon.

~~~bash
# Preview what would happen — no commits written
acd commit-all --dry-run

# Interactive flow (default): shows a confirmation prompt
acd commit-all
~~~

Expected confirmation output:

~~~
Repo: /path/to/repo (refs/heads/main @ abc123456789)
Pending events: 42
Strategy: event (provider deterministic)
Estimated passes: 42
Proceed? [y/N]: y
commit-all complete for /path/to/repo (refs/heads/main)
Strategy: event (provider deterministic)
Pending: before=42 after=0
Commits: 42 (drained=42)
HEAD: abc123456789 -> def456789012
- shadow reseeded from HEAD
Duration: 3.2s
~~~

Skip the prompt with `--yes`, or combine with `--json` for scripting:

~~~bash
acd commit-all --yes
acd commit-all --yes --json | jq '.commits'
~~~

`commit-all` refuses to run on detached HEAD, while a git operation is in
progress (rebase, merge, cherry-pick, bisect), while a manual pause marker is
present, or while the per-repo daemon is alive. After it finishes, start the
live daemon normally with `acd start`.

For full flag reference and intent-strategy behavior, see the
[cold start commit cleanup](docs/user-workflows.md#cold-start-commit-cleanup)
workflow in the docs.

Use a manual pause when you want to reset, rebase, inspect, or stage branch
changes without replay racing you:

~~~bash
acd pause --repo . --reason "manual reset" --yes
# ...do the branch work...
acd resume --repo . --yes
acd wake --repo . --session-id "$ACD_SESSION_ID"
~~~

`--reason` defaults to `manual` when omitted. The marker is stored at `<gitDir>/acd/paused` and survives daemon restarts.
`acd status` and `acd list` show the pause source and remaining TTL when one is
active.

If a parallel committer already landed the captured edits, first check the
decision ledger:

~~~bash
acd events --path path/to/file
acd explain --commit HEAD
acd fix --dry-run
~~~

You should usually see `handled_external` or `superseded_external`. If a
blocked barrier remains after the external commit, run `acd fix --dry-run` to
see the auto-resolvable plan, or `acd fix --force --dry-run` when the barrier
has pending successors.

Enable local decision tracing when you need a replay/capture audit trail:

~~~bash
ACD_TRACE=1 acd start
ACD_TRACE=1 ACD_TRACE_DIR=/tmp/acd-trace acd daemon run --repo .
~~~

Trace files are daily JSONL logs under `<gitDir>/acd/trace/` unless
`ACD_TRACE_DIR` is set. Each record includes `ts`, `repo`, `branch_ref`,
`head_sha`, `event_class`, `decision`, `reason`, `input`, `output`, `error`,
`seq`, and `generation`. See [docs/capture-replay.md](docs/capture-replay.md#trace-event-classes)
for the full `event_class` enumeration. These decision traces summarize daemon
behavior; they do not store full AI prompts or provider request envelopes.

Enable AI prompt tracing only when you need to inspect exactly what an AI
provider or intent planner saw. The default deterministic provider does not
send an AI request, so `ACD_AI_PROMPT_TRACE=1` alone will not create prompt
records. Use a non-deterministic provider when you expect prompt traces:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=event acd start
acd prompt --last

export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=intent acd start
acd prompt --seq 42 --json
~~~

Prompt traces are local JSONL diagnostics under `<gitDir>/acd/prompt-trace/`.
They are written after ACD's redaction and truncation steps, but they may still
contain source code, paths, request envelopes, provider responses, and fallback
metadata. Files are grouped by UTC day and are not pruned automatically; the
async writer buffers up to 256 pending records and drops the oldest buffered
record if it falls behind. Treat the directory as sensitive and delete it when
the investigation is complete.

## Environment

| Variable | Default | Effect |
|---|---:|---|
| `ACD_TRACE` | unset | Truthy values `1`, `true`, `yes` enable best-effort JSONL trace logging. |
| `ACD_TRACE_DIR` | `<gitDir>/acd/trace` | Overrides trace output location. |
| `ACD_AI_PROMPT_TRACE` | unset | Truthy values `1`, `true`, `yes` persist local AI request/response diagnostics under `<gitDir>/acd/prompt-trace/` when a non-deterministic provider sends a request; sensitive even after redaction/truncation. |
| `ACD_SENSITIVE_GLOBS` | built-in defaults | Empty string keeps the default deny-list. |
| `ACD_SAFE_IGNORE` | enabled | Set to `0`, `false`, `no`, or `off` to disable ACD's internal generated-tree pruning. |
| `ACD_SAFE_IGNORE_EXTRA` | unset | Comma-separated patterns appended to the safe-ignore defaults, for example `dist/,build/`. |
| `ACD_SHADOW_RETENTION_GENERATIONS` | `1` | Prior shadow generations retained after Diverged reseed. |
| `ACD_REWIND_GRACE_SECONDS` | `60` | Seconds to pause replay after a same-branch rewind. `0` disables the grace. |
| `ACD_COMMIT_STRATEGY` | `event` | `event` preserves one captured event per commit. `intent` asks the AI planner to select one or more pending captures for the next commit. |
| `ACD_INTENT_WINDOW` | `10` | Maximum pending captures offered to the intent planner in one normal planning pass. |
| `ACD_INTENT_MIN_PENDING` | `10` | Preferred pending-count gate before a normal intent planning pass starts. |
| `ACD_INTENT_MAX_PENDING_AGE` | `5m` | Bounded wait escape hatch for sparse pending queues that have not reached `ACD_INTENT_MIN_PENDING`. |
| `ACD_INTENT_RECENT_COMMITS` | `5` | Recent branch/path commits included as compact planner context. |
| `ACD_INTENT_DEFER_LIMIT` | `1` | Deferrals allowed before ACD forces the overdue capture into a one-item planning window. Lowered from `2` in the Wave 2 planner-atomicity epic — the validation retry loop in `composed.PlanIntent` plus the `<gitDir>/acd/planner-rejects.jsonl` forensic surface make a single deferral overwhelmingly more likely to be planner churn than legitimate "wait for related work". Set `=2` (or higher) to restore the prior tolerance. |
| `ACD_INTENT_PATH_COALESCE` | `1` (on) | Same-path coalesce: folds runs of consecutive single-path captures into one planner offer; the resulting commit settles every original capture seq. Stops at branch-token transitions, multi-path interleave, and terminal barriers. Set `0`/`false`/`no`/`off` to restore one-offer-per-capture behavior. Restart the daemon to apply. |
| `ACD_INTENT_RETRY_ON_INVALID` | `1` (on) | Composed planner retries the primary provider once when the response fails typed validation, quoting the validator error verbatim in the next prompt. Skips retry on transport errors and on validator codes the normalizer already heals. Set `0`/`false`/`no`/`off` to disable retry and fall back immediately on any validation error. |
| `ACD_INTENT_REJECTS_RAW` | unset (off) | **Security-relevant.** When unset/`0`/`false`/`no`/`off`, validator-rejected planner responses are persisted to `<gitDir>/acd/planner-rejects.jsonl` with the raw model output redacted; only the validation code, message, offered seqs, response size, sha256, and parsed-plan summary are kept. Truthy (`1`/`true`/`yes`/`on`) opts into persisting the verbatim response — useful for offline debugging but the file then contains echoed prompt fragments and can leak via repo handoff or backups. The daemon emits a one-shot `slog.Warn` at startup when verbatim mode is on. |
| `ACD_PATH_QUIESCENCE_SECONDS` | `0` (off) | When `>0`, defers offering pending captures for path P to the planner until P has been quiet for that many seconds. Capture rows still persist immediately for durability — only the planner-offer is gated. FIFO-preserving (a gated head blocks the whole batch) and multi-op aware. `acd status` reports `path_quiescence_gated_events`. Restart the daemon to apply. |
| `ACD_RECENT_COMMIT_AFFINITY_SECONDS` | `0` (off, was `120`) | When `>0` AND the most recent HEAD commit touched an offered path within that window, the planner receives a `path_recent_commits` hint suggesting "extend or wait". Hint-only — no amend implemented. Default flipped to `0` because the lookup costs N `git log` per replay pass; opt back in for small repos or when prototyping amend flows. |
| `ACD_AI_DIFF_EGRESS` | unset | Truthy (`1`/`true`/`yes`) opts in to sending reconstructed diffs to network AI providers. Off by default; metadata-only payload otherwise. See [docs/ai-providers.md](docs/ai-providers.md). |

ACD also skips common generated dependency/cache trees even when a project has
not gitignored them: `node_modules/`, `target/`, `.venv/`, `venv/`,
`__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, and
`.gradle/`. This does not edit `.gitignore`; it only prunes ACD capture and
watcher work. Use `acd doctor` to inspect the active safe-ignore pattern list.
`ACD_SAFE_IGNORE` and `ACD_SAFE_IGNORE_EXTRA` are read when the daemon starts;
stop and restart an existing daemon before expecting those changes to apply.

### Commit strategy profiles

Two strategies. Pick one. Both can use AI for commit messages; only `intent`
calls the planner to group captures.

#### Event-driven (one commit per change)

Every captured edit becomes its own commit. Planner is never called. AI is used
only to write the commit message for the single change. Best for CI smoke runs,
shared branches with strict review, or when you want one-to-one traceability
between an edit and a commit.

~~~bash
# AI provider — used for commit message generation only
export ACD_AI_PROVIDER=openai-compat            # openai-compat | deterministic | subprocess:<name>
export ACD_AI_API_KEY=$YOUR_API_KEY             # provider key; omit for deterministic
export ACD_AI_BASE_URL=https://ai.example.internal/v1  # provider endpoint
export ACD_AI_MODEL="gpt-5.4-mini"              # model id passed to provider
export ACD_AI_TIMEOUT=30s                       # per-request budget; replay backs off on timeout
export ACD_AI_DIFF_EGRESS=1                     # opt-in: send redacted diffs to provider for richer messages

# Strategy
export ACD_COMMIT_STRATEGY=event                # one captured event = one commit; planner not called
~~~

Notes:
- Intent knobs (`ACD_INTENT_*`) are inert in this mode.
- Leave `ACD_AI_DIFF_EGRESS` unset to send metadata only when the endpoint is
  not trusted; messages will be coarser but no diff bytes leave the machine.
- For a fully offline setup use `ACD_AI_PROVIDER=deterministic` and drop the
  network knobs.

#### Intent-driven (AI groups related changes)

Pending captures are offered to an AI planner, which selects one or more
related captures to land in the next commit. Other captures stay pending or get
deferred. Best for local development where reviewer-friendly, semantically
grouped commits matter.

~~~bash
# AI provider — used for both planner decisions and commit messages
export ACD_AI_PROVIDER=openai-compat            # network providers required; deterministic does not plan
export ACD_AI_API_KEY=$YOUR_API_KEY
export ACD_AI_BASE_URL=https://ai.example.internal/v1
export ACD_AI_MODEL="gpt-5.4-mini"
export ACD_AI_TIMEOUT=30s
export ACD_AI_DIFF_EGRESS=1                     # strongly recommended for intent: planner groups better with diff context

# Strategy
export ACD_COMMIT_STRATEGY=intent               # planner picks one or more captures per commit
export ACD_INTENT_WINDOW=10                     # max pending captures offered to one planner pass
export ACD_INTENT_MIN_PENDING=4                 # preferred count gate before a normal planner pass starts (lower for sparse repos)
export ACD_INTENT_MAX_PENDING_AGE=5m            # age escape hatch when pending count never reaches MIN_PENDING
export ACD_INTENT_RECENT_COMMITS=5              # recent branch/path commits sent to planner as context
export ACD_INTENT_DEFER_LIMIT=1                 # deferrals allowed before the overdue capture is forced into a one-item commit (Wave 2 default)
~~~

Notes:
- Without `ACD_AI_DIFF_EGRESS=1` the planner sees metadata only; grouping
  quality drops. Use only when the endpoint is trusted.
- `acd wake` bypasses the batch wait and plans currently visible captures
  immediately.
- Over-deferred captures are forced through a one-item planning window so they
  cannot starve. Same-path and nested-path ordering barriers still land first.
- For sparse repos lower `ACD_INTENT_MIN_PENDING` (e.g. `2`-`4`); for heavy
  edit bursts raise `ACD_INTENT_WINDOW` so the planner sees more candidates
  per pass at the cost of more tokens per call.

## Docs

- [docs/capture-replay.md](docs/capture-replay.md) — storage model, replay index, `blocked_conflict`, branch-generation safety, revert workflows, AI diff from captured blobs, operator troubleshooting, pause JSON shapes, trace event classes
- [docs/user-workflows.md](docs/user-workflows.md) — daily user workflows for status, events, explain, fix, skipped files, external commits, branch resets, conflicts, and support bundles
- [docs/multi-tool.md](docs/multi-tool.md) — running `acd` alongside Claude Code auto-commit, Codex ACD hook, or any parallel committer
- [docs/ai-providers.md](docs/ai-providers.md) — AI provider configuration, env vars, subprocess plugin protocol
- [docs/overview.md](docs/overview.md) — high-level overview

## Migrating from prior releases

Re-run `acd setup <harness>` after upgrading. The Stop / `session.idle` hook for
Claude Code, OpenCode, and Pi changed from `acd touch` to `acd flush --logical`
so partial work commits at session-end instead of waiting up to 5 minutes for
the age trigger. Existing snippets keep working — they just lose the new
prompt-end commit boundary. `acd doctor` flags the drift.

`acd flush --logical` is the new explicit drain entrypoint:

- Refreshes the session heartbeat (same as `acd touch`).
- Enqueues a labeled `flush_logical` request.
- Signals the daemon to drain immediately, bypassing `ACD_INTENT_MIN_PENDING`
  and `ACD_INTENT_MAX_PENDING_AGE`.
- Refuses on detached HEAD, while a git operation is in progress, or with a
  manual pause marker present (heartbeat still runs).
- **Requires an existing registered session** when `--logical` is set; without
  `--logical` the command falls back to `acd touch`-style lazy registration.

The Wave 2 planner-atomicity epic also added forensic surfaces operators
should know about:

- `<gitDir>/acd/planner-rejects.jsonl` — rotating JSONL of validator-rejected
  planner responses, 5 MiB per file, 2 files retained. **Raw model output is
  redacted by default**; set `ACD_INTENT_REJECTS_RAW=1` to opt into verbatim
  capture for debugging. The daemon emits a one-shot `slog.Warn` at startup
  when verbatim mode is on; treat the file as sensitive in either case.
- `acd status` JSON adds `planner_error_rate_recent`,
  `singleton_commit_rate_recent`, `intent_stage_diff_cap`, and
  `path_quiescence_gated_events` under the `intent_strategy` block.
- `acd diagnose` warns when `planner_error_rate_recent` exceeds 5% over the
  last 100 decisions and points at the rejects log.

## Status

Active development.
- First tag: `v2026-04-28` - Last tag: `v2026-05-10`

## License

MIT. See [LICENSE](LICENSE).

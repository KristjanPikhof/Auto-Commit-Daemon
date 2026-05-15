# acd — Auto-Commit-Daemon

One static binary that watches your git worktree, captures meaningful changes, and lands them as chronological commits while you keep coding. Pair it with Claude Code, Codex, OpenCode, or Pi and your AI tool's edits ship as real commits without you stopping to type `git commit`.

Two ways to commit:

- **`event`** (default): one captured edit becomes one commit. Fast, predictable, no LLM grouping.
- **`intent`**: an AI planner groups related captures into one reviewable commit per logical change.

## Install

~~~bash
curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh
~~~

Or:

~~~bash
brew tap KristjanPikhof/tap
brew install acd

# or
go install github.com/KristjanPikhof/Auto-Commit-Daemon/cmd/acd@latest
~~~

## Wire your harness

Pick the one matching your AI tool, run the setup, paste the printed snippet into the target file. Don't redirect with `>` if you already have custom hooks — it overwrites the file.

~~~bash
acd setup claude-code   # → ~/.claude/settings.json
acd setup codex         # → ~/.codex/hooks.json
acd setup opencode      # → ~/.config/opencode/hook/hooks.yaml
acd setup pi            # → ~/.pi/agent/hook/hooks.yaml
acd setup shell         # universal direnv / zshrc fallback
~~~

Codex needs one extra line in `~/.codex/config.toml`:

~~~toml
[features]
codex_hooks = true
~~~

After wiring, sanity-check with `acd doctor`. It checks the snippet matches the current template and tells you what to fix.

| Harness | Hook engine | Source |
|---|---|---|
| `claude-code` | Native | [Anthropic Claude Code](https://docs.claude.com/en/docs/claude-code/hooks) |
| `codex` | Native (run `/hooks` after each install to re-approve) | [OpenAI Codex](https://developers.openai.com/codex/hooks) |
| `opencode` | External: [OpenCode-Hooks](https://github.com/KristjanPikhof/OpenCode-Hooks) | KristjanPikhof |
| `pi` | External: [Pi-YAML-Hooks](https://github.com/KristjanPikhof/Pi-YAML-Hooks) | KristjanPikhof |
| `shell` | direnv `.envrc` or shell rc | n/a |

## Pick a strategy

Both strategies use AI for commit messages. Only `intent` calls the planner to group captures.

### Recommended: deterministic event strategy (offline, no provider)

Best when you want one-edit-equals-one-commit, no network calls, no API key. CI smoke runs, shared branches with strict review, and most users start here.

~~~bash
export ACD_AI_PROVIDER=deterministic
export ACD_COMMIT_STRATEGY=event
~~~

That's the whole config. Subjects come from a built-in symbol extractor (Go func name, TS class, Python def, Markdown heading, basename fallback).

### Recommended: AI intent strategy (gpt-5.4-mini openai-compat)

Best for local development where you want reviewer-friendly commits. The planner sees a window of pending captures and picks one or more related ones to ship as a single commit; the rest stay pending until the next pass.

~~~bash
# Provider
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=$YOUR_API_KEY
export ACD_AI_BASE_URL=https://your-endpoint/v1
export ACD_AI_MODEL=gpt-5.4-mini
export ACD_AI_TIMEOUT=30s
export ACD_AI_DIFF_EGRESS=1            # required: planner groups poorly without diff context

# Strategy
export ACD_COMMIT_STRATEGY=intent
export ACD_INTENT_WINDOW=10
export ACD_INTENT_MIN_PENDING=4        # 4 for sparse repos, 10 for heavy edit bursts
export ACD_INTENT_MAX_PENDING_AGE=5m
export ACD_INTENT_RECENT_COMMITS=5
export ACD_INTENT_DEFER_LIMIT=1        # Wave 2 default
~~~

Why these defaults? `MIN_PENDING=4` keeps sparse repos from waiting on a 10-edit batch that never arrives. `DEFER_LIMIT=1` matches the Wave 2 retry+normalize stack: a single deferral is more often planner churn than a real "wait for related work" signal. Raise to `2` if you see legit grouping decisions getting forced into singletons.

`ACD_AI_DIFF_EGRESS=1` is the one knob that materially improves grouping. Without it the planner sees metadata only and groups poorly. Only leave it off if your endpoint is untrusted.

### When to skip intent

- You don't trust the network endpoint with diff bytes (use `event` + `deterministic`).
- Your repo is huge and replay latency matters more than message quality.
- You want strict one-to-one traceability between an edit and a commit.

## Use it

Open your AI tool, edit files, commits land. The daemon starts on first hook fire and refreshes itself per session.

~~~bash
acd start                          # start or refresh the current repo daemon
acd list                           # daemons across all your repos
acd list --watch                   # refresh until Ctrl-C

acd status                         # health, queue depth, recent decisions, intent metrics
acd status --watch                 # live refresh
acd events                         # decision ledger (capture, group, defer, publish, block)
acd events --watch                 # stream new decisions
acd explain --path FILE            # why was this captured / skipped / blocked?
acd explain --commit HEAD          # why did these captures land in this commit?

acd flush --session-id X --logical # drain pending captures NOW (bypasses MIN_PENDING / age)
acd wake --session-id X            # heartbeat refresh + low-latency replay nudge
acd stop --session-id X            # refcount-aware stop (stays alive while peers are connected)
acd stop                           # stop the current repo daemon
acd stop --all                     # stop every registered daemon

acd commit-all                     # one-shot: commit every uncommitted file (daemon must be off)
acd pause --reason "rebasing" --yes
acd resume --yes

acd logs --follow                  # tail the daemon JSONL log
acd doctor                         # health diagnostics + harness-snippet drift
acd doctor --bundle                # zip diagnostics for issue reports
acd diagnose --json                # branch-anchor + blocked-conflict report
~~~

`acd start` resolves your `$PWD` to its canonical Git worktree root, so calling it from a subdirectory refreshes the same daemon instead of spawning a duplicate.

ACD refuses lifecycle commands outside a Git worktree. `start`, `status`, `diagnose` all fail with a clear non-Git error rather than registering an arbitrary directory.

## When commits stop appearing

In order:

~~~bash
acd status                         # any pause? failed barriers? blocked conflicts?
acd events --watch                 # what is the daemon deciding right now?
acd explain --path path/to/file    # why is this file stuck?
acd diagnose --json                # branch anchor + barrier report
acd fix --dry-run                  # plan safe remediation; read it before applying
acd fix --yes                      # apply the safe plan
acd fix --force --yes              # also purge blocked barriers with pending successors
~~~

`acd fix` is the single recovery entrypoint. It backs up `state.db` before mutating, refuses to run while a live daemon owns the database, and won't lift a manual pause unless you pass `--clear-pause`. Use `acd resume --yes` when the only problem is a stale pause marker.

If a parallel committer (Claude Code's atomic-commit hook, Codex ACD hook, your own script) lands the change before ACD's replay tick, you'll see `handled_external` or `superseded_external` in `acd events`. That's normal. Real content mismatches still surface as `blocked_conflict`.

## Cold start: dirty worktree, daemon was off

When files accumulated without ACD running:

~~~bash
acd commit-all --dry-run           # preview, no writes
acd commit-all --yes               # apply
acd commit-all --yes --json        # machine-readable
~~~

Refuses on detached HEAD, in-progress git operations (rebase, merge, cherry-pick, bisect), manual pause markers, or while the per-repo daemon is alive. Start the daemon normally with `acd start` afterward.

## Migrating from prior releases

Re-run `acd setup <harness>` after upgrading. The Stop / `session.idle` hook for Claude Code, OpenCode, and Pi changed from `acd touch` to `acd flush --logical` so partial work commits when the AI session ends instead of waiting up to 5 minutes for the age trigger. Existing snippets keep working but lose the new prompt-end commit boundary. `acd doctor` flags the drift.

`acd flush --logical` is the new explicit drain entrypoint:

- Refreshes the session heartbeat (same as `acd touch`).
- Enqueues a labeled flush request and signals the daemon.
- Bypasses `ACD_INTENT_MIN_PENDING` and `ACD_INTENT_MAX_PENDING_AGE` for the next pass.
- Refuses on detached HEAD, in-progress git operations, or active pause markers (heartbeat still runs).
- Requires an existing registered session when `--logical` is set. Without `--logical` the command lazy-registers like `acd touch`.

The Wave 2 planner-atomicity epic added two operator-facing surfaces:

- **`<gitDir>/acd/planner-rejects.jsonl`** — rotating JSONL of validator-rejected planner responses (5 MiB per file, 2 files retained). **Raw model output is redacted by default**: only the validation code, message, offered seqs, response size, sha256, and parsed-plan summary are kept. Set `ACD_INTENT_REJECTS_RAW=1` to opt into verbatim capture for offline debugging — the daemon emits a one-shot startup warning when verbatim mode is on. Treat the file as sensitive either way; it can leak via repo handoff or backups.
- **New `acd status` JSON fields** under `intent_strategy`: `planner_error_rate_recent`, `singleton_commit_rate_recent`, `intent_stage_diff_cap`, `path_quiescence_gated_events`. `acd diagnose` warns when `planner_error_rate_recent` exceeds 5% over the last 100 decisions and points at the rejects log.

## Environment variables

| Variable | Default | What it does |
|---|---:|---|
| `ACD_COMMIT_STRATEGY` | `event` | `event` = one captured edit per commit. `intent` = AI planner groups related captures. |
| `ACD_AI_PROVIDER` | `deterministic` | `deterministic` (offline), `openai-compat` (network), `subprocess:<name>` (plugin). |
| `ACD_AI_API_KEY` | unset | Required for network providers. |
| `ACD_AI_BASE_URL` | unset | Required for network providers. |
| `ACD_AI_MODEL` | provider default | Model id passed to the provider (e.g. `gpt-5.4-mini`). |
| `ACD_AI_TIMEOUT` | `30s` | Per-request budget; replay backs off on timeout. |
| `ACD_AI_DIFF_EGRESS` | unset (off) | Truthy opts in to sending redacted diffs to network providers. Off = metadata-only. Strongly recommended on for `intent`. |
| `ACD_INTENT_WINDOW` | `10` | Max pending captures offered to one planner pass. |
| `ACD_INTENT_MIN_PENDING` | `10` | Preferred batch gate before a normal planner pass starts. Lower for sparse repos. |
| `ACD_INTENT_MAX_PENDING_AGE` | `5m` | Age escape hatch for sparse queues that never reach `MIN_PENDING`. |
| `ACD_INTENT_RECENT_COMMITS` | `5` | Recent commits sent to the planner as context. |
| `ACD_INTENT_DEFER_LIMIT` | `1` | Deferrals allowed before ACD forces the overdue capture into a one-item commit. Lowered from `2` in Wave 2. Raise to `2` to restore prior tolerance. |
| `ACD_INTENT_PATH_COALESCE` | `1` (on) | Folds consecutive same-path captures into one planner offer. Stops at branch transitions, multi-path interleave, and barriers. Set `0`/`false`/`no`/`off` to disable. Restart daemon to apply. |
| `ACD_INTENT_RETRY_ON_INVALID` | `1` (on) | Composed planner retries the primary provider once on typed validation error, quoting the validator message. Skips retry on transport errors and on already-healed validator codes. Set `0`/`false`/`no`/`off` to disable. |
| `ACD_INTENT_REJECTS_RAW` | unset (off) | Off = redact raw model output before persisting to `planner-rejects.jsonl` (recommended). Truthy = persist verbatim (debugging only; sensitive). |
| `ACD_PATH_QUIESCENCE_SECONDS` | `0` (off) | When `>0`, defers planner-offer for path P until P has been quiet that long. Capture rows still persist immediately. FIFO-preserving and multi-op aware. Restart daemon to apply. |
| `ACD_RECENT_COMMIT_AFFINITY_SECONDS` | `0` (off, was `120`) | When `>0` and the most recent HEAD commit touched an offered path within that window, the planner gets a "extend or wait" hint. Hint-only; no amend implemented. Default flipped to `0` because the lookup costs N `git log` per pass. |
| `ACD_TRACE` | unset | Truthy enables JSONL decision-trace logging under `<gitDir>/acd/trace/`. |
| `ACD_TRACE_DIR` | `<gitDir>/acd/trace` | Override trace output location. |
| `ACD_AI_PROMPT_TRACE` | unset | Truthy persists AI request/response diagnostics under `<gitDir>/acd/prompt-trace/`. Sensitive even after redaction; delete when done. |
| `ACD_SAFE_IGNORE` | enabled | Set `0`/`false`/`no`/`off` to disable ACD's built-in generated-tree pruning (`node_modules/`, `target/`, `.venv/`, etc.). |
| `ACD_SAFE_IGNORE_EXTRA` | unset | Comma-separated extra patterns: `dist/,build/`. |
| `ACD_SENSITIVE_GLOBS` | built-in defaults | Empty string keeps defaults; never disables them. |
| `ACD_SHADOW_RETENTION_GENERATIONS` | `1` | Prior shadow generations retained after Diverged reseed. |
| `ACD_REWIND_GRACE_SECONDS` | `60` | Pause replay this long after a same-branch rewind. `0` disables. |

Env changes are read at daemon start. Restart an existing daemon to pick them up.

## Docs

- [docs/capture-replay.md](docs/capture-replay.md) — storage model, replay index, blocked-conflict states, branch-generation safety, revert workflows, trace event classes
- [docs/user-workflows.md](docs/user-workflows.md) — daily user workflows
- [docs/multi-tool.md](docs/multi-tool.md) — running ACD alongside another auto-committer
- [docs/ai-providers.md](docs/ai-providers.md) — provider configuration and subprocess plugin protocol
- [docs/overview.md](docs/overview.md) — high-level overview

## Status

Active development. First tag: `v2026-04-28`. Last tag: `v2026-05-10`.

## License

MIT. See [LICENSE](LICENSE).

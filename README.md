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
acd init claude-code   # paste output into ~/.claude/settings.json
acd init codex         # paste output into ~/.config/codex/config.toml
acd init opencode      # paste output into your OpenCode-Hooks hooks.yaml
acd init pi            # paste output into your .pi/hook/hooks.yaml
acd init shell         # universal direnv / zshrc fallback
~~~

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
acd fix --dry-run       # plan safe remediation for a stuck repo
acd fix --yes           # apply the safe plan after reading it
acd logs                # tail the current repo daemon log as raw JSONL
acd logs --lines 200    # choose the initial tail length
acd logs --follow       # stream appended raw JSONL lines until Ctrl-C
acd stats --since 7d    # last week's commits
acd doctor              # health/support diagnostics, including queue blockers
acd doctor --bundle     # write a diagnostics zip for issue reports
acd diagnose            # read-only branch anchor + blocked_conflict report
acd recover --auto --dry-run  # preview stale-anchor recovery without mutation
acd pause --reason "resetting branch" --yes   # durable manual replay pause
acd resume --yes          # remove the manual pause marker
acd wake --session-id X # heartbeat refresh + nudge daemon for low-latency replay
acd gc                  # prune stale central-registry entries
acd stop                # graceful stop for the current repo daemon
acd stop --session-id X # harness/refcount stop; exits only when no peers remain
acd stop --all          # stop every daemon
~~~

Use the no-flag lifecycle commands when you are driving ACD from a terminal.
Manual `acd start` registers a stable human client for the current repo, so
running it again refreshes the same client instead of creating a new session.
Manual `acd stop` stops the current repo daemon directly.

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
sanitize the stream; use `acd doctor --bundle` when you want bundled
diagnostics with tail snippets and safe metadata for reports.

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

`status` shows daemon health, queue counts, pause state, failed terminal
barriers, recent decision counts, and the active commit strategy. `status
--json` includes the decision cursor plus recent decision records, and includes
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
blocked-conflict counts by `error_class`, failed terminal barriers, and the
five most recent blocked or failed events. If the daemon is stopped and the
plan looks right, recover a stale anchor with an automatic backup:

~~~bash
acd recover --repo . --auto --dry-run
acd recover --repo . --auto --yes
~~~

`recover` refuses to run while the daemon PID is alive. Applying a plan copies
`.git/acd/state.db` to `.git/acd/state.db.recover-<timestamp>`, retargets stale
pending/blocked rows to the current attached branch, resets `blocked_conflict`
rows to `pending`, clears stale replay metadata, and repairs ACD-owned stale
live-index entries when the current `HEAD` and worktree still match the
published event. `acd doctor` also reports live-index repair candidates and
points at the recover dry-run command. A manual pause marker is preserved unless
you pass `--clear-pause`; use `acd resume --yes` when you only need to lift a
manual pause.

ACD uses an isolated scratch index for replay correctness, then performs a
guarded path-scoped live-index reconciliation so IDEs see the committed state
for ACD-owned paths. It will not run broad `git reset`, `git checkout`, or
`git read-tree` against your live index, and it skips same-path staged work
that no longer matches the captured before-state.

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

You should usually see `handled_external` or `superseded_external`. Use
`purge-events` only as an advanced fallback when `diagnose` or `fix` points at
obsolete terminal barriers that must be deleted.

Enable local decision tracing when you need a replay/capture audit trail:

~~~bash
ACD_TRACE=1 acd start
ACD_TRACE=1 ACD_TRACE_DIR=/tmp/acd-trace acd daemon run --repo .
~~~

Trace files are daily JSONL logs under `<gitDir>/acd/trace/` unless
`ACD_TRACE_DIR` is set. Each record includes `ts`, `repo`, `branch_ref`,
`head_sha`, `event_class`, `decision`, `reason`, `input`, `output`, `error`,
`seq`, and `generation`. See [docs/capture-replay.md](docs/capture-replay.md#trace-event-classes)
for the full `event_class` enumeration.

## Environment

| Variable | Default | Effect |
|---|---:|---|
| `ACD_TRACE` | unset | Truthy values `1`, `true`, `yes` enable best-effort JSONL trace logging. |
| `ACD_TRACE_DIR` | `<gitDir>/acd/trace` | Overrides trace output location. |
| `ACD_SENSITIVE_GLOBS` | built-in defaults | Empty string keeps the default deny-list. |
| `ACD_SAFE_IGNORE` | enabled | Set to `0`, `false`, `no`, or `off` to disable ACD's internal generated-tree pruning. |
| `ACD_SAFE_IGNORE_EXTRA` | unset | Comma-separated patterns appended to the safe-ignore defaults, for example `dist/,build/`. |
| `ACD_SHADOW_RETENTION_GENERATIONS` | `1` | Prior shadow generations retained after Diverged reseed. |
| `ACD_REWIND_GRACE_SECONDS` | `60` | Seconds to pause replay after a same-branch rewind. `0` disables the grace. |
| `ACD_COMMIT_STRATEGY` | `event` | `event` preserves one captured event per commit. `intent` asks the AI planner to select one or more pending captures for the next commit. |
| `ACD_INTENT_WINDOW` | `10` | Maximum pending captures offered to the intent planner in one normal planning pass. |
| `ACD_INTENT_RECENT_COMMITS` | `5` | Recent branch/path commits included as compact planner context. |
| `ACD_INTENT_DEFER_LIMIT` | `2` | Deferrals allowed before ACD forces the overdue capture into a one-item planning window. |
| `ACD_AI_DIFF_EGRESS` | unset | Truthy (`1`/`true`/`yes`) opts in to sending reconstructed diffs to network AI providers. Off by default; metadata-only payload otherwise. See [docs/ai-providers.md](docs/ai-providers.md). |

ACD also skips common generated dependency/cache trees even when a project has
not gitignored them: `node_modules/`, `target/`, `.venv/`, `venv/`,
`__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, and
`.gradle/`. This does not edit `.gitignore`; it only prunes ACD capture and
watcher work. Use `acd doctor` to inspect the active safe-ignore pattern list.
`ACD_SAFE_IGNORE` and `ACD_SAFE_IGNORE_EXTRA` are read when the daemon starts;
stop and restart an existing daemon before expecting those changes to apply.

### Commit strategy profiles

Compatibility default:

~~~bash
export ACD_COMMIT_STRATEGY=event
~~~

Reviewer-friendly local work:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
~~~

Private-code metadata-only setup:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
# Leave ACD_AI_DIFF_EGRESS unset.
~~~

Self-hosted AI with explicit diff egress:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
export ACD_AI_BASE_URL=https://ai.example.internal/v1
export ACD_AI_DIFF_EGRESS=1
~~~

Use `event` for CI smoke runs and compatibility-sensitive shared branches.
Use `intent` when review quality matters and the AI endpoint is trusted. Intent
planning may group related captures, choose exactly one capture, or defer
unrelated captures. Over-deferred captures are forced through a one-capture
planner window so they cannot starve.

## Docs

- [docs/capture-replay.md](docs/capture-replay.md) — storage model, replay index, `blocked_conflict`, branch-generation safety, revert workflows, AI diff from captured blobs, operator troubleshooting, pause JSON shapes, trace event classes
- [docs/user-workflows.md](docs/user-workflows.md) — daily user workflows for status, events, explain, fix, skipped files, external commits, branch resets, conflicts, and support bundles
- [docs/multi-tool.md](docs/multi-tool.md) — running `acd` alongside Claude Code auto-commit, Codex ACD hook, or any parallel committer
- [docs/ai-providers.md](docs/ai-providers.md) — AI provider configuration, env vars, subprocess plugin protocol
- [docs/overview.md](docs/overview.md) — high-level overview

## Status

Active development. First tag: `v2026-04-28`.

## License

MIT. See [LICENSE](LICENSE).

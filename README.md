# acd — Auto-Commit-Daemon

One static binary. Watches your git worktree. Captures every meaningful change as an atomic commit. Plays nicely with Claude Code, Codex, OpenCode, Pi, and any tool that runs commands at session start.

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
acd list                # daemons running across all your repos
                        # columns: REPO  DAEMON  CLIENTS  PENDING  BLOCKED  LAST_COMMIT  STATUS
acd status              # current repo's daemon (shows pending_events + blocked_conflicts)
acd stats --since 7d    # last week's commits
acd doctor              # pending : N, blocked : N, last conflict path + age + error
acd doctor --bundle     # diagnostics zip for issue reports
acd diagnose            # read-only branch anchor + blocked_conflict report
acd recover --auto --dry-run  # preview stale-anchor recovery without mutation
acd pause --reason "resetting branch" --yes   # durable manual replay pause
acd resume --yes          # remove the manual pause marker
acd wake --session-id X # heartbeat refresh + nudge daemon for low-latency replay
acd gc                  # prune stale central-registry entries
acd stop --repo X       # graceful stop, refcount-aware
acd stop --all          # stop every daemon
~~~

If commits stop appearing, see [docs/capture-replay.md](docs/capture-replay.md)
for a step-by-step troubleshooting checklist.

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

## Trace and recovery

Use `acd diagnose` first when replay stalls:

~~~bash
acd diagnose --repo .
acd diagnose --repo . --json
~~~

It reports the current git `HEAD` branch, the daemon's persisted branch anchor,
blocked-conflict counts by `error_class`, and the five most recent blocked
events. If the daemon is stopped and the plan looks right, recover a stale
anchor with an automatic backup:

~~~bash
acd recover --repo . --auto --dry-run
acd recover --repo . --auto --yes
~~~

`recover` refuses to run while the daemon PID is alive. Applying a plan copies
`.git/acd/state.db` to `.git/acd/state.db.recover-<timestamp>`, retargets stale
pending/blocked rows to the current attached branch, resets `blocked_conflict`
rows to `pending`, and clears stale replay metadata.

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

If a parallel committer already landed the captured edits, do not requeue those
rows with `recover`: they will usually hit the same before-state mismatch. Use
`purge-events` to delete the terminal barrier and, when that tail is obsolete,
the pending rows behind it:

~~~bash
acd purge-events --repo . --blocked --pending --dry-run
acd purge-events --repo . --blocked --pending --yes
~~~

Use `--blocked` alone when you only want to lift the barrier and keep later
pending rows for replay.

Enable local decision tracing when you need a replay/capture audit trail:

~~~bash
ACD_TRACE=1 acd start --repo . --session-id debug --harness shell
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
| `ACD_AI_SEND_DIFF` | unset | Sends redacted captured diffs to AI providers when truthy. |
| `ACD_SENSITIVE_GLOBS` | built-in defaults | Empty string keeps the default deny-list. |
| `ACD_SHADOW_RETENTION_GENERATIONS` | `1` | Prior shadow generations retained after Diverged reseed. |
| `ACD_REWIND_GRACE_SECONDS` | `60` | Seconds to pause replay after a same-branch rewind. `0` disables the grace. |

## Docs

- [docs/capture-replay.md](docs/capture-replay.md) — storage model, replay index, `blocked_conflict`, branch-generation safety, revert workflows, AI diff from captured blobs, operator troubleshooting, pause JSON shapes, trace event classes
- [docs/multi-tool.md](docs/multi-tool.md) — running `acd` alongside Claude Code auto-commit, Codex ACD hook, or any parallel committer
- [docs/ai-providers.md](docs/ai-providers.md) — AI provider configuration, env vars, subprocess plugin protocol
- [docs/overview.md](docs/overview.md) — high-level overview

## Status

Active development. First tag: `v2026-04-28`.

## License

MIT. See [LICENSE](LICENSE).

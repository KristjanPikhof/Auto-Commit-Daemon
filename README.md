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
acd wake --session-id X # heartbeat refresh + nudge daemon for low-latency replay
acd gc                  # prune stale central-registry entries
acd stop --repo X       # graceful stop, refcount-aware
acd stop --all          # stop every daemon
~~~

If commits stop appearing, see [docs/capture-replay.md](docs/capture-replay.md)
for a step-by-step troubleshooting checklist.

## Docs

- [docs/capture-replay.md](docs/capture-replay.md) — storage model, replay index, `blocked_conflict`, branch-generation safety, AI diff from captured blobs, operator troubleshooting
- [docs/ai-providers.md](docs/ai-providers.md) — AI provider configuration, env vars, subprocess plugin protocol
- [docs/overview.md](docs/overview.md) — high-level overview

## Status

Active development. First tag: `v2026-04-28`.

## License

MIT. See [LICENSE](LICENSE).

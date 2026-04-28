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
acd status              # current repo's daemon
acd stats --since 7d    # last week's commits
acd doctor --bundle     # diagnostics zip for issue reports
acd stop --repo X       # graceful stop, refcount-aware
acd stop --all          # nuke every daemon
~~~

## Status

Active development. First tag: `v2026-04-28`.

## License

MIT. See [LICENSE](LICENSE).

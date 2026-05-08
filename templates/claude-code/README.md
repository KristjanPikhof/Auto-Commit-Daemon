# acd adapter: claude-code

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd setup claude-code`
3. Merge the printed JSON into `~/.claude/settings.json` under the `hooks` key
4. Restart Claude Code

If you previously installed a snippet that predates the v2026-05-08 release,
re-run `acd setup claude-code` and replace the `hooks` block in
`~/.claude/settings.json` so the new self-heal hooks take effect. Run
`acd doctor` to check whether your installed snippet is current; it warns when
active hooks are missing `acd start` or `acd wake` and shows the remediation
command.

## Verify

- Open Claude Code in any git repo
- From another shell, run `acd status`
- The output should show one client with `harness=claude-code`

`PreToolUse` and `PostToolUse` run idempotent `acd start` before `acd wake`,
so later tool activity can recover if you manually ran `acd stop` while the
Claude Code session stayed open. `SessionEnd` still deregisters the session
with `acd stop --session-id`.

## Uninstall

See [uninstall.md](uninstall.md).

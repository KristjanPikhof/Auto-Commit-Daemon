# acd adapter: claude-code

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Install jq (required): `brew install jq` or `apt install jq`
3. Generate snippet: `acd init claude-code`
4. Merge the printed JSON into `~/.claude/settings.json` under the `hooks` key
5. Restart Claude Code

## Verify

- Open Claude Code in any git repo
- From another shell, run `acd status`
- The output should show one client with `harness=claude-code`

## Uninstall

See [uninstall.md](uninstall.md).
